"""End-to-end verification of the coordination service.

Runs entirely over real HTTP (through the nginx gateway and, for concurrency
checks, directly against the two API instances) plus a direct database read to
confirm persistence. Exits 0 only if every check passes.
"""

from __future__ import annotations

import hashlib
import os
import sys
import threading
import time
import uuid

import psycopg

from .client import Api

G = Api(os.environ.get("API_BASE_URL", "http://gateway"))
A = Api(os.environ.get("API_A_URL", "http://api-a:8000"))
B = Api(os.environ.get("API_B_URL", "http://api-b:8000"))
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://scheduler:scheduler@db:5432/scheduler"
)
SCALE = int(os.environ.get("VERIFY_SCALE", "2000"))

FAILURES: list[str] = []
CHECKS = [0]


def check(name: str, cond: bool, detail: str = "") -> None:
    CHECKS[0] += 1
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAILURES.append(f"{name} {detail}")


def expect_err(name: str, resp: tuple[int, dict], status: int, code: str) -> dict:
    st, body = resp
    ok = (
        st == status
        and isinstance(body, dict)
        and body.get("error", {}).get("code") == code
    )
    check(name, ok, f"want {status}/{code}, got {st}/{body}")
    return body


def expect_err_any(name: str, resp: tuple[int, dict], status: int, codes: set[str]) -> dict:
    """For races with the background sweeper: several codes are equally valid,
    the guarantee under test is that the stale request changed nothing."""
    st, body = resp
    ok = (
        st == status
        and isinstance(body, dict)
        and body.get("error", {}).get("code") in codes
    )
    check(name, ok, f"want {status}/{sorted(codes)}, got {st}/{body}")
    return body


def expect_ok(name: str, resp: tuple[int, dict], statuses=(200, 201)) -> dict:
    st, body = resp
    check(name, st in statuses and isinstance(body, dict) and "error" not in body,
          f"want {statuses}, got {st}/{body}")
    return body


def digest_of(tasks: list[str], edges: list[tuple[str, str]]) -> str:
    h = hashlib.sha256()
    for t in sorted(tasks):
        h.update(b"T:" + t.encode() + b"\n")
    for s, d in sorted(edges):
        h.update(b"E:" + s.encode() + b"->" + d.encode() + b"\n")
    return h.hexdigest()


def new_pipeline(api: Api = G) -> str:
    body = expect_ok("create pipeline", api.post("/pipelines"))
    return body["id"]


def mutate(api: Api, pid: str, op_id: str, version: int, ops: list[dict]):
    return api.post(f"/pipelines/{pid}/draft/mutations",
                    {"op_id": op_id, "draft_version": version, "ops": ops})


def wait_ready() -> None:
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            st, body = G.get("/health")
            if st == 200 and body.get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit("API did not become healthy in time")


# --------------------------------------------------------------------------
# sections
# --------------------------------------------------------------------------

def t_health() -> None:
    print("== health ==")
    for name, api in (("gateway", G), ("api-a", A), ("api-b", B)):
        st, body = api.get("/health")
        check(f"health {name}", st == 200 and body.get("status") == "ok", f"{st} {body}")


def t_draft_mutations() -> str:
    print("== draft mutations, idempotency, versioning ==")
    pid = new_pipeline()
    ops = (
        [{"type": "add_task", "task_id": t} for t in ("a", "b", "c", "d")]
        + [{"type": "add_edge", "src": s, "dst": d}
           for s, d in (("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"))]
    )
    r1 = expect_ok("batch mutation", mutate(G, pid, "m1", 1, ops))
    check("version bumped", r1.get("draft_version") == 2, str(r1))

    r2 = expect_ok("op_id replay", mutate(G, pid, "m1", 1, ops))
    check("replay returns first result", r2 == r1, f"{r2} != {r1}")

    expect_err("op_id reuse with other params", mutate(G, pid, "m1", 1,
               [{"type": "add_task", "task_id": "zz"}]), 409, "OP_CONFLICT")

    stale = expect_err("stale version rejected", mutate(G, pid, "m2", 1,
                       [{"type": "add_task", "task_id": "zz"}]), 409, "STALE_VERSION")
    check("stale returns current version",
          stale.get("error", {}).get("details", {}).get("current_draft_version") == 2,
          str(stale))
    draft = expect_ok("draft readable", G.get(f"/pipelines/{pid}/draft"))
    check("no partial write from stale op",
          "zz" not in draft["tasks"] and draft["draft_version"] == 2, str(draft)[:200])

    expect_err("invalid task id", mutate(G, pid, "m3", 2,
               [{"type": "add_task", "task_id": "bad id!"}]), 400, "INVALID_TASK_ID")
    expect_err("self loop", mutate(G, pid, "m4", 2,
               [{"type": "add_edge", "src": "a", "dst": "a"}]), 400, "SELF_LOOP")
    expect_err("duplicate edge", mutate(G, pid, "m5", 2,
               [{"type": "add_edge", "src": "a", "dst": "b"}]), 409, "EDGE_EXISTS")
    expect_err("edge to missing task", mutate(G, pid, "m6", 2,
               [{"type": "add_edge", "src": "a", "dst": "ghost"}]), 404, "TASK_NOT_FOUND")
    expect_err("duplicate task", mutate(G, pid, "m7", 2,
               [{"type": "add_task", "task_id": "a"}]), 409, "TASK_EXISTS")

    # remove_task cascades its edges; remove_edge works; both bump the version.
    expect_ok("remove edge", mutate(G, pid, "m8", 2,
              [{"type": "remove_edge", "src": "a", "dst": "c"}]))
    expect_ok("re-add removed edge", mutate(G, pid, "m9", 3,
              [{"type": "add_edge", "src": "a", "dst": "c"}]))
    draft = expect_ok("draft after re-add", G.get(f"/pipelines/{pid}/draft"))
    check("draft version 4", draft["draft_version"] == 4, str(draft["draft_version"]))
    return pid


def t_cycle_witness() -> None:
    print("== cycle detection with deterministic witness ==")
    pid = new_pipeline()
    expect_ok("build cycle", mutate(G, pid, "c1", 1, [
        {"type": "add_task", "task_id": t} for t in ("x", "y", "z")
    ] + [
        {"type": "add_edge", "src": s, "dst": d}
        for s, d in (("x", "y"), ("y", "z"), ("z", "x"))
    ]))
    resp = expect_err("seal cyclic graph rejected",
                      G.post(f"/pipelines/{pid}/seals",
                             {"expected_draft_version": 2, "idempotency_key": "s"}),
                      409, "CYCLE_DETECTED")
    witness = resp.get("error", {}).get("details", {}).get("cycle")
    check("witness is x->y->z->x", witness == ["x", "y", "z", "x"], str(witness))

    seals = expect_ok("list seals", G.get(f"/pipelines/{pid}/seals"))["seals"]
    check("no seal persisted for cyclic graph", seals == [], str(seals))

    # Cycle not reachable from the smallest task id: isolated 'a' first.
    pid2 = new_pipeline()
    expect_ok("build second cycle", mutate(G, pid2, "c1", 1, [
        {"type": "add_task", "task_id": t} for t in ("a", "m", "n")
    ] + [
        {"type": "add_edge", "src": s, "dst": d} for s, d in (("m", "n"), ("n", "m"))
    ]))
    resp = expect_err("second cycle rejected",
                      G.post(f"/pipelines/{pid2}/seals",
                             {"expected_draft_version": 2, "idempotency_key": "s"}),
                      409, "CYCLE_DETECTED")
    check("witness is m->n->m",
          resp.get("error", {}).get("details", {}).get("cycle") == ["m", "n", "m"],
          str(resp))


def t_seal(pid: str) -> tuple[str, str]:
    print("== sealing: digest, topo order, idempotency, concurrency ==")
    expected_digest = digest_of(
        ["a", "b", "c", "d"], [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]
    )
    seal = expect_ok("seal v4", G.post(f"/pipelines/{pid}/seals",
                     {"expected_draft_version": 4, "idempotency_key": "seal-1"}), (201,))
    check("lexicographic topo order", seal["topo_order"] == ["a", "b", "c", "d"],
          str(seal["topo_order"]))
    check("canonical digest", seal["digest"] == expected_digest,
          f"{seal['digest']} != {expected_digest}")

    again = expect_ok("seal replay", G.post(f"/pipelines/{pid}/seals",
                      {"expected_draft_version": 4, "idempotency_key": "seal-1"}), (200, 201))
    check("replay returns same seal", again["id"] == seal["id"], str(again))

    expect_err("seal key reuse with other version",
               G.post(f"/pipelines/{pid}/seals",
                      {"expected_draft_version": 3, "idempotency_key": "seal-1"}),
               409, "OP_CONFLICT")
    expect_err("same version sealed under other key",
               G.post(f"/pipelines/{pid}/seals",
                      {"expected_draft_version": 4, "idempotency_key": "seal-2"}),
               409, "SEAL_CONFLICT")
    expect_err("stale seal version",
               G.post(f"/pipelines/{pid}/seals",
                      {"expected_draft_version": 2, "idempotency_key": "seal-3"}),
               409, "STALE_VERSION")

    # Two instances race to seal the same draft version with different keys.
    pid2 = new_pipeline()
    expect_ok("prep race pipeline", mutate(G, pid2, "r1", 1,
              [{"type": "add_task", "task_id": "solo"}]))
    results: list[tuple[int, dict]] = [None, None]  # type: ignore

    def seal_on(api: Api, key: str, slot: int) -> None:
        results[slot] = api.post(f"/pipelines/{pid2}/seals",
                                 {"expected_draft_version": 2, "idempotency_key": key})

    t1 = threading.Thread(target=seal_on, args=(A, "race-a", 0))
    t2 = threading.Thread(target=seal_on, args=(B, "race-b", 1))
    t1.start(); t2.start(); t1.join(); t2.join()
    codes = sorted(r[0] for r in results)
    check("concurrent seals: exactly one wins", codes == [201, 409], str(codes))
    loser = results[0] if results[0][0] == 409 else results[1]
    check("loser gets SEAL_CONFLICT",
          loser[1].get("error", {}).get("code") == "SEAL_CONFLICT", str(loser))
    seals = expect_ok("seals listed", G.get(f"/pipelines/{pid2}/seals"))["seals"]
    check("exactly one seal for the version", len(seals) == 1, str(seals))

    # Same key raced on both instances: both calls return the same seal.
    pid3 = new_pipeline()
    expect_ok("prep race pipeline 2", mutate(G, pid3, "r1", 1,
              [{"type": "add_task", "task_id": "solo"}]))

    def seal3(api: Api, slot: int) -> None:
        results[slot] = api.post(f"/pipelines/{pid3}/seals",
                                 {"expected_draft_version": 2, "idempotency_key": "same-key"})

    t1 = threading.Thread(target=seal3, args=(A, 0))
    t2 = threading.Thread(target=seal3, args=(B, 1))
    t1.start(); t2.start(); t1.join(); t2.join()
    check("same-key race both succeed",
          results[0][0] in (200, 201) and results[1][0] in (200, 201), str(results))
    check("same-key race same seal id",
          results[0][1].get("id") == results[1][1].get("id"), str(results))

    # Draft keeps evolving after a seal; the old seal is immutable.
    expect_ok("mutate after seal", mutate(G, pid, "m10", 4, [
        {"type": "add_task", "task_id": "e"},
        {"type": "add_edge", "src": "d", "dst": "e"},
    ]))
    seal2 = expect_ok("seal v5", G.post(f"/pipelines/{pid}/seals",
                      {"expected_draft_version": 5, "idempotency_key": "seal-4"}), (201,))
    check("new topo order", seal2["topo_order"] == ["a", "b", "c", "d", "e"],
          str(seal2["topo_order"]))
    old = expect_ok("old seal fetched", G.get(f"/pipelines/{pid}/seals/{seal['id']}"))
    check("old seal immutable",
          old["digest"] == expected_digest and old["topo_order"] == ["a", "b", "c", "d"],
          str(old))
    return seal["id"], seal2["id"]


def t_run_creation() -> None:
    print("== run creation: validation, idempotency, empty run ==")
    # Zero-task seal -> run succeeds immediately.
    pid = new_pipeline()
    seal = expect_ok("seal empty draft", G.post(f"/pipelines/{pid}/seals",
                     {"expected_draft_version": 1, "idempotency_key": "z"}), (201,))
    check("empty topo", seal["topo_order"] == [] and seal["task_count"] == 0, str(seal))
    run = expect_ok("run of empty seal", G.post(f"/pipelines/{pid}/runs",
                    {"sealed_version_id": seal["id"], "idempotency_key": "r",
                     "max_attempts": 1, "lease_seconds": 2}), (201,))
    check("empty run succeeded", run["status"] == "succeeded" and run["total_tasks"] == 0,
          str(run))

    expect_err("max_attempts too small",
               G.post(f"/pipelines/{pid}/runs",
                      {"sealed_version_id": seal["id"], "idempotency_key": "r2",
                       "max_attempts": 0, "lease_seconds": 2}), 400, "INVALID_PARAMS")
    expect_err("max_attempts too large",
               G.post(f"/pipelines/{pid}/runs",
                      {"sealed_version_id": seal["id"], "idempotency_key": "r3",
                       "max_attempts": 11, "lease_seconds": 2}), 400, "INVALID_PARAMS")
    expect_err("lease too small",
               G.post(f"/pipelines/{pid}/runs",
                      {"sealed_version_id": seal["id"], "idempotency_key": "r4",
                       "max_attempts": 1, "lease_seconds": 1}), 400, "INVALID_PARAMS")
    expect_err("lease too large",
               G.post(f"/pipelines/{pid}/runs",
                      {"sealed_version_id": seal["id"], "idempotency_key": "r5",
                       "max_attempts": 1, "lease_seconds": 31}), 400, "INVALID_PARAMS")
    expect_err("unknown seal",
               G.post(f"/pipelines/{pid}/runs",
                      {"sealed_version_id": str(uuid.uuid4()), "idempotency_key": "r6",
                       "max_attempts": 1, "lease_seconds": 2}), 404, "SEAL_NOT_FOUND")


def t_diamond_run(seal_id: str, pid: str) -> str:
    print("== claim/complete lifecycle on a diamond DAG ==")
    body = {"sealed_version_id": seal_id, "idempotency_key": "run-1",
            "max_attempts": 2, "lease_seconds": 10}
    run = expect_ok("create run", G.post(f"/pipelines/{pid}/runs", body), (201,))
    rid = run["id"]
    check("run running with 4 tasks",
          run["status"] == "running" and run["total_tasks"] == 4, str(run))

    replay = expect_ok("run idempotent replay", G.post(f"/pipelines/{pid}/runs", body), (200, 201))
    check("same run id", replay["id"] == rid, str(replay))
    expect_err("run key reuse with other params",
               G.post(f"/pipelines/{pid}/runs",
                      {"sealed_version_id": seal_id, "idempotency_key": "run-1",
                       "max_attempts": 3, "lease_seconds": 10}), 409, "OP_CONFLICT")

    expect_err("successor not claimable yet",
               G.post(f"/runs/{rid}/claims", {"task_id": "d"}), 409, "TASK_NOT_READY")
    expect_err("unknown task claim",
               G.post(f"/runs/{rid}/claims", {"task_id": "ghost"}), 404, "TASK_NOT_FOUND")

    c1 = expect_ok("claim picks lexicographically first ready",
                   G.post(f"/runs/{rid}/claims"), (201,))
    check("first claim is task a attempt 1 fencing 1",
          c1["task_id"] == "a" and c1["attempt"] == 1 and c1["fencing_token"] == 1, str(c1))
    check("claim has token and deadline",
          bool(c1["claim_token"]) and bool(c1["lease_expires_at"]), str(c1))
    expect_err("nothing else ready", G.post(f"/runs/{rid}/claims"), 409, "NO_READY_TASK")

    expect_err("bad digest rejected",
               G.post(f"/runs/{rid}/tasks/a/complete",
                      {"attempt": 1, "fencing_token": 1, "claim_token": c1["claim_token"],
                       "output_digest": "XYZ"}), 400, "INVALID_DIGEST")

    d1 = hashlib.sha256(b"output-a").hexdigest()
    done = expect_ok("complete a", G.post(f"/runs/{rid}/tasks/a/complete",
                     {"attempt": 1, "fencing_token": 1, "claim_token": c1["claim_token"],
                      "output_digest": d1}))
    check("b and c released", done["released"] == ["b", "c"], str(done))

    replay = expect_ok("complete replay same content",
                       G.post(f"/runs/{rid}/tasks/a/complete",
                              {"attempt": 1, "fencing_token": 1,
                               "claim_token": c1["claim_token"], "output_digest": d1}))
    check("replay equals first result", replay == done, f"{replay} != {done}")
    expect_err("complete replay different content",
               G.post(f"/runs/{rid}/tasks/a/complete",
                      {"attempt": 1, "fencing_token": 1, "claim_token": c1["claim_token"],
                       "output_digest": hashlib.sha256(b"other").hexdigest()}),
               409, "CONTENT_CONFLICT")

    cb = expect_ok("claim b", G.post(f"/runs/{rid}/claims", {"task_id": "b"}), (201,))
    cc = expect_ok("claim c", G.post(f"/runs/{rid}/claims", {"task_id": "c"}), (201,))

    expect_err("stale fencing rejected",
               G.post(f"/runs/{rid}/tasks/b/complete",
                      {"attempt": 1, "fencing_token": 0, "claim_token": cb["claim_token"],
                       "output_digest": d1}), 409, "FENCING_STALE")
    expect_err("wrong token rejected",
               G.post(f"/runs/{rid}/tasks/b/complete",
                      {"attempt": 1, "fencing_token": 1, "claim_token": "f" * 64,
                       "output_digest": d1}), 409, "CLAIM_INVALID")
    expect_err("wrong attempt rejected",
               G.post(f"/runs/{rid}/tasks/b/complete",
                      {"attempt": 2, "fencing_token": 1, "claim_token": cb["claim_token"],
                       "output_digest": d1}), 409, "CLAIM_INVALID")

    hb = expect_ok("heartbeat", G.post(f"/runs/{rid}/tasks/b/heartbeat",
                   {"attempt": 1, "fencing_token": 1, "claim_token": cb["claim_token"]}))
    check("heartbeat extends lease", hb["lease_expires_at"] > cb["lease_expires_at"],
          f"{hb} vs {cb}")

    d_b = hashlib.sha256(b"output-b").hexdigest()
    d_c = hashlib.sha256(b"output-c").hexdigest()
    done_b = expect_ok("complete b", G.post(f"/runs/{rid}/tasks/b/complete",
                       {"attempt": 1, "fencing_token": 1, "claim_token": cb["claim_token"],
                        "output_digest": d_b}))
    check("d not released before c", done_b["released"] == [], str(done_b))
    expect_err("d still not claimable",
               G.post(f"/runs/{rid}/claims", {"task_id": "d"}), 409, "TASK_NOT_READY")
    done_c = expect_ok("complete c", G.post(f"/runs/{rid}/tasks/c/complete",
                       {"attempt": 1, "fencing_token": 1, "claim_token": cc["claim_token"],
                        "output_digest": d_c}))
    check("d released after both preds", done_c["released"] == ["d"], str(done_c))

    cd = expect_ok("claim d", G.post(f"/runs/{rid}/claims"), (201,))
    check("d attempt 1 fencing 1", cd["task_id"] == "d" and cd["fencing_token"] == 1, str(cd))
    done_d = expect_ok("complete d", G.post(f"/runs/{rid}/tasks/d/complete",
                       {"attempt": 1, "fencing_token": 1, "claim_token": cd["claim_token"],
                        "output_digest": hashlib.sha256(b"output-d").hexdigest()}))
    check("run succeeded", done_d["run_status"] == "succeeded", str(done_d))

    final = expect_ok("run query", G.get(f"/runs/{rid}"))
    check("run terminal succeeded with counts",
          final["status"] == "succeeded" and final["succeeded_count"] == 4
          and final["finished_at"] is not None, str(final))
    expect_err("no claims after terminal", G.post(f"/runs/{rid}/claims"),
               409, "RUN_NOT_RUNNING")
    return rid


def t_fencing_and_expiry() -> None:
    print("== lease expiry, fencing, stale worker ==")
    pid = new_pipeline()
    expect_ok("prep", mutate(G, pid, "f1", 1, [{"type": "add_task", "task_id": "t1"}]))
    seal = expect_ok("seal", G.post(f"/pipelines/{pid}/seals",
                     {"expected_draft_version": 2, "idempotency_key": "s"}), (201,))
    run = expect_ok("run", G.post(f"/pipelines/{pid}/runs",
                    {"sealed_version_id": seal["id"], "idempotency_key": "r",
                     "max_attempts": 2, "lease_seconds": 2}), (201,))
    rid = run["id"]

    k1 = expect_ok("claim attempt 1", G.post(f"/runs/{rid}/claims"), (201,))
    check("attempt 1 fencing 1", k1["attempt"] == 1 and k1["fencing_token"] == 1, str(k1))
    time.sleep(3)  # lease (2s, database time) expires

    expect_err_any("expired complete rejected",
                   G.post(f"/runs/{rid}/tasks/t1/complete",
                          {"attempt": 1, "fencing_token": 1, "claim_token": k1["claim_token"],
                           "output_digest": hashlib.sha256(b"x").hexdigest()}),
                   409, {"LEASE_EXPIRED", "CLAIM_INVALID", "FENCING_STALE"})
    expect_err_any("expired heartbeat rejected",
                   G.post(f"/runs/{rid}/tasks/t1/heartbeat",
                          {"attempt": 1, "fencing_token": 1, "claim_token": k1["claim_token"]}),
                   409, {"LEASE_EXPIRED", "CLAIM_INVALID", "FENCING_STALE"})

    k2 = expect_ok("reclaim after expiry", G.post(f"/runs/{rid}/claims"), (201,))
    check("attempt 2 fencing 2, token changed",
          k2["attempt"] == 2 and k2["fencing_token"] == 2
          and k2["claim_token"] != k1["claim_token"], str(k2))

    expect_err("stale worker complete rejected",
               G.post(f"/runs/{rid}/tasks/t1/complete",
                      {"attempt": 1, "fencing_token": 1, "claim_token": k1["claim_token"],
                       "output_digest": hashlib.sha256(b"x").hexdigest()}),
               409, "FENCING_STALE")
    expect_err("stale worker heartbeat rejected",
               G.post(f"/runs/{rid}/tasks/t1/heartbeat",
                      {"attempt": 1, "fencing_token": 1, "claim_token": k1["claim_token"]}),
               409, "FENCING_STALE")
    task = expect_ok("task state unchanged", G.get(f"/runs/{rid}/tasks/t1"))
    check("still claimed at attempt 2",
          task["state"] == "claimed" and task["attempt"] == 2
          and task["fencing_token"] == 2, str(task))

    done = expect_ok("current worker completes",
                     G.post(f"/runs/{rid}/tasks/t1/complete",
                            {"attempt": 2, "fencing_token": 2,
                             "claim_token": k2["claim_token"],
                             "output_digest": hashlib.sha256(b"real").hexdigest()}))
    check("run succeeded", done["run_status"] == "succeeded", str(done))


def t_failure_propagation() -> None:
    print("== failure, retries, blocked propagation, run failure ==")
    pid = new_pipeline()
    expect_ok("prep", mutate(G, pid, "p1", 1,
              [{"type": "add_task", "task_id": t} for t in ("f1", "f2", "f3", "g1", "g2")]
              + [{"type": "add_edge", "src": s, "dst": d} for s, d in
                 (("f1", "f2"), ("f2", "f3"), ("g1", "g2"))]))
    seal = expect_ok("seal", G.post(f"/pipelines/{pid}/seals",
                     {"expected_draft_version": 2, "idempotency_key": "s"}), (201,))
    run = expect_ok("run max_attempts=1", G.post(f"/pipelines/{pid}/runs",
                    {"sealed_version_id": seal["id"], "idempotency_key": "r",
                     "max_attempts": 1, "lease_seconds": 10}), (201,))
    rid = run["id"]

    c = expect_ok("claim f1", G.post(f"/runs/{rid}/claims", {"task_id": "f1"}), (201,))
    f = expect_ok("fail f1", G.post(f"/runs/{rid}/tasks/f1/fail",
                  {"attempt": 1, "fencing_token": 1, "claim_token": c["claim_token"],
                   "error": "boom"}))
    check("f1 permanently failed at attempt cap", f["state"] == "failed", str(f))

    freplay = expect_ok("fail replay", G.post(f"/runs/{rid}/tasks/f1/fail",
                        {"attempt": 1, "fencing_token": 1, "claim_token": c["claim_token"],
                         "error": "boom"}))
    check("fail replay equals first", freplay == f, f"{freplay} != {f}")

    states = {t["task_id"]: t["state"]
              for t in expect_ok("tasks", G.get(f"/runs/{rid}/tasks?limit=100"))["tasks"]}
    check("f2,f3 blocked; g1,g2 unaffected",
          states.get("f2") == "blocked" and states.get("f3") == "blocked"
          and states.get("g1") == "ready" and states.get("g2") == "waiting", str(states))

    run_mid = expect_ok("run still running", G.get(f"/runs/{rid}"))
    check("run not failed while work remains", run_mid["status"] == "running", str(run_mid))

    for t in ("g1", "g2"):
        cg = expect_ok(f"claim {t}", G.post(f"/runs/{rid}/claims"), (201,))
        check(f"claimed {t}", cg["task_id"] == t, str(cg))
        expect_ok(f"complete {t}", G.post(f"/runs/{rid}/tasks/{t}/complete",
                  {"attempt": cg["attempt"], "fencing_token": cg["fencing_token"],
                   "claim_token": cg["claim_token"],
                   "output_digest": hashlib.sha256(t.encode()).hexdigest()}))

    final = expect_ok("final run", G.get(f"/runs/{rid}"))
    check("run failed with counts",
          final["status"] == "failed" and final["failed_count"] == 1
          and final["blocked_count"] == 2 and final["succeeded_count"] == 2, str(final))


def t_attempt_exhaustion_via_expiry() -> None:
    print("== attempt exhaustion via lease expiry ==")
    pid = new_pipeline()
    expect_ok("prep", mutate(G, pid, "e1", 1, [{"type": "add_task", "task_id": "solo"}]))
    seal = expect_ok("seal", G.post(f"/pipelines/{pid}/seals",
                     {"expected_draft_version": 2, "idempotency_key": "s"}), (201,))
    run = expect_ok("run", G.post(f"/pipelines/{pid}/runs",
                    {"sealed_version_id": seal["id"], "idempotency_key": "r",
                     "max_attempts": 1, "lease_seconds": 2}), (201,))
    rid = run["id"]
    expect_ok("claim", G.post(f"/runs/{rid}/claims"), (201,))
    time.sleep(3)
    expect_err_any("no reclaim after final attempt expired",
                   G.post(f"/runs/{rid}/claims"), 409, {"NO_READY_TASK", "RUN_NOT_RUNNING"})
    final = expect_ok("run failed", G.get(f"/runs/{rid}"))
    check("run failed after exhaustion",
          final["status"] == "failed" and final["failed_count"] == 1, str(final))


def t_concurrency() -> None:
    print("== concurrent claims across instances ==")
    n = 20
    pid = new_pipeline()
    expect_ok("prep", mutate(G, pid, "cc1", 1,
              [{"type": "add_task", "task_id": f"t{i:02d}"} for i in range(n)]))
    seal = expect_ok("seal", G.post(f"/pipelines/{pid}/seals",
                     {"expected_draft_version": 2, "idempotency_key": "s"}), (201,))
    run = expect_ok("run", G.post(f"/pipelines/{pid}/runs",
                    {"sealed_version_id": seal["id"], "idempotency_key": "r",
                     "max_attempts": 3, "lease_seconds": 30}), (201,))
    rid = run["id"]

    claims: list[tuple[int, dict]] = [None] * n  # type: ignore

    def auto_claim(i: int) -> None:
        api = (A, B, G)[i % 3]
        claims[i] = api.post(f"/runs/{rid}/claims")

    threads = [threading.Thread(target=auto_claim, args=(i,)) for i in range(n)]
    for t in threads: t.start()
    for t in threads: t.join()
    got = sorted(c[1].get("task_id") for c in claims if c[0] == 201)
    check("all parallel claims granted exactly once",
          got == sorted(f"t{i:02d}" for i in range(n)), str(got))
    check("all attempt 1 / fencing 1",
          all(c[1].get("attempt") == 1 and c[1].get("fencing_token") == 1
              for c in claims if c[0] == 201), str(claims[:3]))

    # Eight workers race for the same task on a fresh run: exactly one wins.
    pid2 = new_pipeline()
    expect_ok("prep2", mutate(G, pid2, "cc1", 1, [{"type": "add_task", "task_id": "hot"}]))
    seal2 = expect_ok("seal2", G.post(f"/pipelines/{pid2}/seals",
                      {"expected_draft_version": 2, "idempotency_key": "s"}), (201,))
    run2 = expect_ok("run2", G.post(f"/pipelines/{pid2}/runs",
                     {"sealed_version_id": seal2["id"], "idempotency_key": "r",
                      "max_attempts": 3, "lease_seconds": 30}), (201,))
    rid2 = run2["id"]
    results: list[tuple[int, dict]] = [None] * 8  # type: ignore

    def claim_hot(i: int) -> None:
        api = (A, B)[i % 2]
        results[i] = api.post(f"/runs/{rid2}/claims", {"task_id": "hot"})

    threads = [threading.Thread(target=claim_hot, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    wins = [r for r in results if r[0] == 201]
    losses = [r for r in results if r[0] == 409]
    check("same task claimed exactly once under race",
          len(wins) == 1 and len(losses) == 7, str([(r[0], r[1]) for r in results]))
    check("losers see TASK_NOT_READY",
          all(r[1].get("error", {}).get("code") == "TASK_NOT_READY" for r in losses),
          str(losses[:2]))

    # Finish the first run to prove mass completion works.
    for c in claims:
        if c[0] != 201:
            continue
        body = c[1]
        expect_ok(f"complete {body['task_id']}",
                  G.post(f"/runs/{rid}/tasks/{body['task_id']}/complete",
                         {"attempt": body["attempt"],
                          "fencing_token": body["fencing_token"],
                          "claim_token": body["claim_token"],
                          "output_digest": hashlib.sha256(body["task_id"].encode()).hexdigest()}))
    final = expect_ok("mass run succeeded", G.get(f"/runs/{rid}"))
    check("run succeeded after 20 completions",
          final["status"] == "succeeded" and final["succeeded_count"] == n, str(final))


def t_scale() -> None:
    print(f"== scale smoke: chain of {SCALE} tasks ==")
    pid = new_pipeline()
    expect_ok("add tasks", mutate(G, pid, "sc1", 1,
              [{"type": "add_task", "task_id": f"t{i:06d}"} for i in range(SCALE)]))
    expect_ok("add edges", mutate(G, pid, "sc2", 2,
              [{"type": "add_edge", "src": f"t{i:06d}", "dst": f"t{i+1:06d}"}
               for i in range(SCALE - 1)]))
    start = time.time()
    seal = expect_ok("seal large chain", G.post(f"/pipelines/{pid}/seals",
                     {"expected_draft_version": 3, "idempotency_key": "s"}), (201,))
    elapsed = time.time() - start
    check("seal is fast", elapsed < 30, f"{elapsed:.1f}s")
    check("topo order is the chain",
          seal["topo_order"] == [f"t{i:06d}" for i in range(SCALE)], "mismatch")
    expected = digest_of([f"t{i:06d}" for i in range(SCALE)],
                         [(f"t{i:06d}", f"t{i+1:06d}") for i in range(SCALE - 1)])
    check("digest matches independent computation", seal["digest"] == expected,
          seal["digest"])

    run = expect_ok("run large chain", G.post(f"/pipelines/{pid}/runs",
                    {"sealed_version_id": seal["id"], "idempotency_key": "r",
                     "max_attempts": 1, "lease_seconds": 30}), (201,))
    rid = run["id"]
    c = expect_ok("claim chain head", G.post(f"/runs/{rid}/claims"), (201,))
    check("head is t000000", c["task_id"] == "t000000", str(c))
    expect_ok("complete head", G.post(f"/runs/{rid}/tasks/t000000/complete",
              {"attempt": 1, "fencing_token": 1, "claim_token": c["claim_token"],
               "output_digest": hashlib.sha256(b"head").hexdigest()}))
    nxt = expect_ok("next task ready", G.post(f"/runs/{rid}/claims"), (201,))
    check("chain advances to t000001", nxt["task_id"] == "t000001", str(nxt))


def t_db_persistence(rid: str) -> None:
    print("== direct database persistence check ==")
    with psycopg.connect(DATABASE_URL) as conn:
        row = conn.execute(
            "SELECT status, succeeded_count, total_tasks FROM runs WHERE id = %s", (rid,)
        ).fetchone()
        check("run persisted as succeeded",
              row is not None and row[0] == "succeeded" and row[1] == row[2] == 4, str(row))
        cnt = conn.execute(
            "SELECT COUNT(*) FROM run_tasks WHERE run_id = %s AND state = 'succeeded'",
            (rid,),
        ).fetchone()[0]
        check("all run_tasks persisted succeeded", cnt == 4, str(cnt))
        tok = conn.execute(
            "SELECT COUNT(*) FROM run_tasks WHERE run_id = %s AND claim_token IS NOT NULL",
            (rid,),
        ).fetchone()[0]
        check("no dangling claim tokens", tok == 0, str(tok))


def main() -> int:
    wait_ready()
    t_health()
    pid = t_draft_mutations()
    t_cycle_witness()
    seal_v4, _seal_v5 = t_seal(pid)
    t_run_creation()
    rid = t_diamond_run(seal_v4, pid)
    t_fencing_and_expiry()
    t_failure_propagation()
    t_attempt_exhaustion_via_expiry()
    t_concurrency()
    t_scale()
    t_db_persistence(rid)

    print(f"\n{CHECKS[0]} checks, {len(FAILURES)} failures")
    if FAILURES:
        for f in FAILURES:
            print("FAILED:", f)
        return 1
    print("VERIFY OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
