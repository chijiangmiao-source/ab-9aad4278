"""One-shot acceptance verifier.

Exercises the coordinator's key semantics over real HTTP (through the
gateway, hence across both API instances) and checks invariants directly
against the database. Exits 0 on success, 1 on any failure.
"""

from __future__ import annotations

import hashlib
import os
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
import psycopg

API = os.environ.get("API_BASE", "http://localhost:8000")
DB = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/coordinator")

_checks = 0
_failures: List[str] = []


def check(name: str, cond: bool, extra: str = "") -> None:
    global _checks
    _checks += 1
    if cond:
        print(f"  ok   {name}")
    else:
        msg = f"  FAIL {name}" + (f" — {extra}" if extra else "")
        print(msg)
        _failures.append(msg)


def section(title: str) -> None:
    print(f"\n== {title}")


# ---------------------------------------------------------------------------
# independent re-implementations (cross-check the service's canonical forms)
# ---------------------------------------------------------------------------
def independent_digest(tasks: List[str], edges: List[List[str]]) -> str:
    tl = sorted(tasks)
    el = sorted((e[0], e[1]) for e in edges)
    parts = ["cryoem-pipeline-digest/v1", f"tasks:{len(tl)}"]
    parts += [f"T {t}" for t in tl]
    parts.append(f"edges:{len(el)}")
    parts += [f"E {s} {d}" for s, d in el]
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def independent_topo(tasks: List[str], edges: List[List[str]]) -> List[str]:
    import heapq
    adj: Dict[str, List[str]] = {t: [] for t in tasks}
    indeg: Dict[str, int] = {t: 0 for t in tasks}
    for s, d in edges:
        adj[s].append(d)
        indeg[d] += 1
    heap = [t for t in tasks if indeg[t] == 0]
    heapq.heapify(heap)
    out = []
    while heap:
        n = heapq.heappop(heap)
        out.append(n)
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                heapq.heappush(heap, m)
    return out


# ---------------------------------------------------------------------------
# http helpers
# ---------------------------------------------------------------------------
def client() -> httpx.Client:
    return httpx.Client(base_url=API, timeout=30)


def uid() -> str:
    return uuid.uuid4().hex


def make_pipeline(c: httpx.Client) -> str:
    r = c.post("/pipelines", json={"name": "verify"})
    assert r.status_code == 201, r.text
    return r.json()["pipeline_id"]


def mutate(c: httpx.Client, pid: str, op_id: str, version: int,
           ops: List[Dict[str, Any]]) -> httpx.Response:
    return c.post(f"/pipelines/{pid}/draft/mutations",
                  json={"op_id": op_id, "draft_version": version,
                        "operations": ops})


def add_graph(c: httpx.Client, pid: str, tasks: List[str],
              edges: List[List[str]], op_prefix: str = "") -> int:
    """Add tasks then edges; returns the resulting draft_version."""
    r = mutate(c, pid, op_prefix + uid(), 0,
               [{"type": "add_task", "task_id": t} for t in tasks])
    assert r.status_code == 200, r.text
    v = r.json()["draft_version"]
    if edges:
        r = mutate(c, pid, op_prefix + uid(), v,
                   [{"type": "add_edge", "src": s, "dst": d} for s, d in edges])
        assert r.status_code == 200, r.text
        v = r.json()["draft_version"]
    return v


def seal(c: httpx.Client, pid: str, version: int, key: str) -> httpx.Response:
    return c.post(f"/pipelines/{pid}/seals",
                  json={"expected_draft_version": version,
                        "idempotency_key": key})


def make_run(c: httpx.Client, pid: str, seal_id: str, key: str,
             max_attempts: int = 3, lease_seconds: int = 5) -> httpx.Response:
    return c.post("/runs", json={
        "pipeline_id": pid, "seal_id": seal_id, "idempotency_key": key,
        "max_attempts": max_attempts, "lease_seconds": lease_seconds})


def claim(c: httpx.Client, run_id: str) -> Dict[str, Any]:
    r = c.post(f"/runs/{run_id}/claims")
    assert r.status_code == 200, r.text
    return r.json()


def complete(c: httpx.Client, run_id: str, cl: Dict[str, Any],
             digest: Optional[str] = None) -> httpx.Response:
    return c.post(f"/runs/{run_id}/claims/complete", json={
        "claim_token": cl["claim_token"], "attempt": cl["attempt"],
        "fencing_token": cl["fencing_token"],
        "output_digest": digest or hashlib.sha256(
            f"out-{cl['task_id']}-{cl['attempt']}".encode()).hexdigest()})


def fail(c: httpx.Client, run_id: str, cl: Dict[str, Any],
         reason: str = "boom") -> httpx.Response:
    return c.post(f"/runs/{run_id}/claims/fail", json={
        "claim_token": cl["claim_token"], "attempt": cl["attempt"],
        "fencing_token": cl["fencing_token"], "reason": reason})


def run_status(c: httpx.Client, run_id: str) -> Dict[str, Any]:
    r = c.get(f"/runs/{run_id}")
    assert r.status_code == 200, r.text
    return r.json()


def drain_run(c: httpx.Client, run_id: str, max_rounds: int = 1000) -> str:
    """Claim+complete everything until the run reaches a terminal state."""
    for _ in range(max_rounds):
        st = run_status(c, run_id)
        if st["status"] != "running":
            return st["status"]
        cl = claim(c, run_id)
        if not cl["claimed"]:
            time.sleep(0.05)
            continue
        r = complete(c, run_id, cl)
        assert r.status_code == 200, r.text
    raise AssertionError("run did not terminate")


# ---------------------------------------------------------------------------
# test sections
# ---------------------------------------------------------------------------
def t_draft_semantics(c: httpx.Client) -> None:
    section("draft revisions: versioning, op_id idempotency, atomicity")
    pid = make_pipeline(c)

    body_ops = [{"type": "add_task", "task_id": "denoise"},
                {"type": "add_task", "task_id": "extract"}]
    op = uid()
    r1 = mutate(c, pid, op, 0, body_ops)
    check("mutation accepted", r1.status_code == 200, r1.text)
    v1 = r1.json()["draft_version"]
    check("version starts at 1", v1 == 1)

    # same op_id + same params -> first result, no new version
    r2 = mutate(c, pid, op, 0, body_ops)
    check("op_id replay returns first result",
          r2.status_code == 200 and r2.json() == r1.json(), r2.text)

    # same op_id + different params -> conflict
    r3 = mutate(c, pid, op, 0, [{"type": "add_task", "task_id": "other"}])
    check("op_id reuse with different params conflicts",
          r3.status_code == 409 and r3.json()["error"]["code"] == "OP_CONFLICT",
          r3.text)

    # stale version -> current version returned, nothing written
    r4 = mutate(c, pid, uid(), 0, [{"type": "add_task", "task_id": "classify"}])
    j = r4.json()
    check("stale version rejected with current version",
          r4.status_code == 409
          and j["error"]["code"] == "VERSION_CONFLICT"
          and j["error"]["details"]["current_draft_version"] == v1, r4.text)
    tasks = c.get(f"/pipelines/{pid}/draft/tasks").json()
    check("no partial write on stale version",
          tasks["total"] == 2 and "classify" not in tasks["tasks"])

    # atomic batch: one bad op aborts the whole request
    r5 = mutate(c, pid, uid(), v1, [
        {"type": "add_task", "task_id": "refine"},
        {"type": "add_task", "task_id": "denoise"},  # duplicate -> abort all
    ])
    check("conflicting batch rejected",
          r5.status_code == 409 and r5.json()["error"]["code"] == "TASK_EXISTS",
          r5.text)
    tasks = c.get(f"/pipelines/{pid}/draft/tasks").json()
    check("batch is atomic (no partial write)", tasks["total"] == 2)

    # validation
    r6 = mutate(c, pid, uid(), v1, [{"type": "add_task", "task_id": "bad id!"}])
    check("invalid task id rejected", r6.status_code == 400, r6.text)
    r7 = mutate(c, pid, uid(), v1, [{"type": "add_task", "task_id": "denoise"}])
    check("duplicate task rejected",
          r7.status_code == 409 and r7.json()["error"]["code"] == "TASK_EXISTS")

    # edges: self loop, missing endpoint, duplicate
    r8 = mutate(c, pid, uid(), v1,
                [{"type": "add_edge", "src": "denoise", "dst": "denoise"}])
    check("self loop rejected",
          r8.status_code == 409 and r8.json()["error"]["code"] == "SELF_LOOP")
    r9 = mutate(c, pid, uid(), v1,
                [{"type": "add_edge", "src": "denoise", "dst": "ghost"}])
    check("missing endpoint rejected",
          r9.status_code == 409
          and r9.json()["error"]["code"] == "EDGE_ENDPOINT_MISSING")
    r10 = mutate(c, pid, uid(), v1,
                 [{"type": "add_edge", "src": "denoise", "dst": "extract"}])
    check("edge added", r10.status_code == 200, r10.text)
    v2 = r10.json()["draft_version"]
    r11 = mutate(c, pid, uid(), v2,
                 [{"type": "add_edge", "src": "denoise", "dst": "extract"}])
    check("duplicate edge rejected",
          r11.status_code == 409 and r11.json()["error"]["code"] == "EDGE_EXISTS")

    # remove_task cascades its edges
    r12 = mutate(c, pid, uid(), v2, [{"type": "remove_task", "task_id": "extract"}])
    check("remove_task cascades edges",
          r12.status_code == 200
          and r12.json()["removed_edges"] == [{"src": "denoise", "dst": "extract"}],
          r12.text)
    edges = c.get(f"/pipelines/{pid}/draft/edges").json()
    check("cascaded edge gone", edges["total"] == 0)


def t_sealing(c: httpx.Client, db) -> None:
    section("sealing: digest, topo order, cycle witness, concurrency")
    # --- deterministic cycle witness
    pid = make_pipeline(c)
    v = add_graph(c, pid, ["m", "a", "z", "b"],
                  [["m", "a"], ["m", "z"], ["a", "b"], ["b", "m"]])
    r = seal(c, pid, v, uid())
    j = r.json()
    check("cyclic draft cannot be sealed",
          r.status_code == 409 and j["error"]["code"] == "CYCLE_DETECTED", r.text)
    check("cycle witness is the deterministic DFS path",
          j["error"]["details"]["cycle"] == ["a", "b", "m", "a"],
          str(j["error"]["details"]))
    check("no seal persisted for cyclic draft",
          c.get(f"/pipelines/{pid}").json()["seals"] == [])

    # --- successful seal with canonical digest/topo
    pid2 = make_pipeline(c)
    tasks = ["extract", "denoise", "classify", "refine", "motion"]
    edges = [["denoise", "motion"], ["motion", "extract"],
             ["extract", "classify"], ["classify", "refine"]]
    v2 = add_graph(c, pid2, tasks, edges)
    key = uid()
    r = seal(c, pid2, v2, key)
    check("seal succeeds", r.status_code == 201, r.text)
    s1 = r.json()
    check("topo order is the lexicographic canonical order",
          s1["topo_order"] == independent_topo(tasks, edges),
          str(s1["topo_order"]))
    check("digest matches independently computed canonical digest",
          s1["digest_sha256"] == independent_digest(tasks, edges))

    # idempotent replay + conflict
    r = seal(c, pid2, v2, key)
    check("seal idempotent replay returns identical result",
          r.status_code in (200, 201) and r.json() == s1, r.text)
    r = seal(c, pid2, v2 + 1, key)
    check("seal key reuse with different params conflicts",
          r.status_code == 409
          and r.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT", r.text)
    r = seal(c, pid2, v2, uid())
    check("second seal of same draft version rejected",
          r.status_code == 409
          and r.json()["error"]["code"] == "SEAL_ALREADY_EXISTS", r.text)
    r = seal(c, pid2, v2 + 5, uid())
    check("seal with wrong expected version rejected",
          r.status_code == 409
          and r.json()["error"]["code"] == "SEAL_VERSION_CONFLICT", r.text)

    # seal immutability: fetched seal matches
    got = c.get(f"/pipelines/{pid2}/seals/{s1['seal_id']}").json()
    check("sealed content is immutable and queryable",
          got["digest_sha256"] == s1["digest_sha256"]
          and got["topo_order"] == s1["topo_order"])

    # --- concurrent seals from two "instances" (through the gateway):
    # exactly one seal per draft version
    pid3 = make_pipeline(c)
    v3 = add_graph(c, pid3, ["t1", "t2"], [["t1", "t2"]])
    results: List[httpx.Response] = []
    barrier = threading.Barrier(2)

    def do_seal():
        with client() as cc:
            barrier.wait()
            results.append(seal(cc, pid3, v3, uid()))

    th = [threading.Thread(target=do_seal) for _ in range(2)]
    [t.start() for t in th]
    [t.join() for t in th]
    codes = sorted(r.status_code for r in results)
    check("concurrent seals: exactly one wins",
          codes == [201, 409], str([(r.status_code, r.text) for r in results]))
    rows = db.execute(
        "SELECT COUNT(*) FROM seals WHERE pipeline_id=%s AND draft_version=%s",
        (pid3, v3)).fetchall()
    check("exactly one seal row for the draft version", rows[0][0] == 1)

    # --- seal vs concurrent revision: definite atomic order
    pid4 = make_pipeline(c)
    v4 = add_graph(c, pid4, ["a"], [])
    r = mutate(c, pid4, uid(), v4, [{"type": "add_task", "task_id": "b"}])
    v5 = r.json()["draft_version"]
    r = seal(c, pid4, v4, uid())  # the mutation won the race earlier
    check("seal of superseded version rejected atomically",
          r.status_code == 409
          and r.json()["error"]["code"] == "SEAL_VERSION_CONFLICT", r.text)
    r = seal(c, pid4, v5, uid())
    check("seal of current version succeeds after concurrent revision",
          r.status_code == 201 and r.json()["task_count"] == 2, r.text)


def t_run_creation(c: httpx.Client) -> None:
    section("run creation: validation, idempotency, zero-task run")
    pid = make_pipeline(c)
    v = add_graph(c, pid, ["a", "b"], [["a", "b"]])
    sid = seal(c, pid, v, uid()).json()["seal_id"]

    r = make_run(c, pid, sid, uid(), max_attempts=0)
    check("max_attempts below range rejected", r.status_code == 400)
    r = make_run(c, pid, sid, uid(), max_attempts=11)
    check("max_attempts above range rejected", r.status_code == 400)
    r = make_run(c, pid, sid, uid(), lease_seconds=1)
    check("lease below range rejected", r.status_code == 400)
    r = make_run(c, pid, sid, uid(), lease_seconds=31)
    check("lease above range rejected", r.status_code == 400)
    r = make_run(c, pid, uid(), uid())
    check("run from unknown seal rejected",
          r.status_code == 404 and r.json()["error"]["code"] == "SEAL_NOT_FOUND")

    key = uid()
    r1 = make_run(c, pid, sid, key)
    check("run created", r1.status_code == 201, r1.text)
    r2 = make_run(c, pid, sid, key)
    check("run creation idempotent replay", r2.json() == r1.json(), r2.text)
    r3 = make_run(c, pid, sid, key, max_attempts=4)
    check("run key reuse with different params conflicts",
          r3.status_code == 409
          and r3.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT", r3.text)

    # zero-task sealed run succeeds immediately
    pid0 = make_pipeline(c)
    s0 = seal(c, pid0, 0, uid()).json()
    check("empty draft seals", s0["task_count"] == 0 and s0["topo_order"] == [])
    r0 = make_run(c, pid0, s0["seal_id"], uid())
    check("zero-task run is immediately succeeded",
          r0.status_code == 201 and r0.json()["status"] == "succeeded", r0.text)


def t_claiming_and_completion(c: httpx.Client, db) -> None:
    section("claims: gating, concurrency, fencing, completion idempotency")
    pid = make_pipeline(c)
    tasks = [f"t{i}" for i in range(8)]
    v = add_graph(c, pid, tasks, [])
    sid = seal(c, pid, v, uid()).json()["seal_id"]
    run = make_run(c, pid, sid, uid(), max_attempts=3, lease_seconds=5).json()
    rid = run["run_id"]

    # concurrent claims across both API instances: each task granted once
    granted: List[Dict[str, Any]] = []
    lock = threading.Lock()

    def do_claim():
        with client() as cc:
            cl = claim(cc, rid)
            if cl["claimed"]:
                with lock:
                    granted.append(cl)

    th = [threading.Thread(target=do_claim) for _ in range(24)]
    [t.start() for t in th]
    [t.join() for t in th]
    check("every ready task claimed exactly once under concurrency",
          len(granted) == 8
          and len({g["task_id"] for g in granted}) == 8
          and all(g["attempt"] == 1 for g in granted)
          and len({g["claim_token"] for g in granted}) == 8,
          str(granted))
    check("no further claims while all tasks leased",
          claim(c, rid)["claimed"] is False)

    # complete idempotency: same digest replays, different digest conflicts
    cl = granted[0]
    d1 = hashlib.sha256(b"output-1").hexdigest()
    r1 = complete(c, rid, cl, d1)
    check("complete accepted", r1.status_code == 200, r1.text)
    r2 = complete(c, rid, cl, d1)
    check("same-content completion retry returns first result",
          r2.status_code == 200 and r2.json() == r1.json(), r2.text)
    r3 = complete(c, rid, cl, hashlib.sha256(b"output-2").hexdigest())
    check("different-content completion retry conflicts",
          r3.status_code == 409
          and r3.json()["error"]["code"] == "DIGEST_CONFLICT", r3.text)
    r4 = complete(c, rid, cl, "ABCD")
    check("non-hex/uppercase digest rejected", r4.status_code == 400)

    # finish the rest
    for g in granted[1:]:
        r = complete(c, rid, g)
        assert r.status_code == 200, r.text
    st = run_status(c, rid)
    check("run succeeds when all tasks succeed", st["status"] == "succeeded",
          str(st))
    check("claim on terminal run grants nothing",
          claim(c, rid)["claimed"] is False)

    # fencing/attempt staleness
    pid2 = make_pipeline(c)
    v2 = add_graph(c, pid2, ["solo"], [])
    sid2 = seal(c, pid2, v2, uid()).json()["seal_id"]
    rid2 = make_run(c, pid2, sid2, uid(), max_attempts=3, lease_seconds=5).json()["run_id"]
    cl1 = claim(c, rid2)
    check("first claim attempt=1 fencing=1",
          cl1["claimed"] and cl1["attempt"] == 1 and cl1["fencing_token"] == 1,
          str(cl1))
    r = fail(c, rid2, cl1)
    check("active failure requeues while under max_attempts",
          r.status_code == 200 and r.json()["status"] == "ready", r.text)
    r = fail(c, rid2, cl1)
    check("fail retry on same claim replays first result",
          r.status_code == 200 and r.json()["status"] == "ready", r.text)
    cl2 = claim(c, rid2)
    check("re-claim gets attempt=2 with strictly larger fencing token",
          cl2["claimed"] and cl2["attempt"] == 2 and cl2["fencing_token"] == 2,
          str(cl2))
    r = complete(c, rid2, cl1)
    check("stale claim token cannot complete after requeue",
          r.status_code == 409
          and r.json()["error"]["code"] in ("CLAIM_NOT_ACTIVE", "CLAIM_STALE",
                                            "CLAIM_EXPIRED"), r.text)
    st = c.get(f"/runs/{rid2}/tasks/solo").json()
    check("stale completion changed nothing",
          st["status"] == "leased" and st["attempts"] == 2, str(st))
    r = complete(c, rid2, cl2)
    check("current claim completes", r.status_code == 200, r.text)

    # fencing tokens strictly increasing in the DB
    rows = db.execute(
        "SELECT 1 FROM leases l1 JOIN leases l2 "
        "ON l1.run_id=l2.run_id AND l1.task_id=l2.task_id "
        "AND l1.attempt < l2.attempt AND l1.fencing_token >= l2.fencing_token "
        "LIMIT 1").fetchall()
    check("fencing tokens strictly increasing per task (DB check)", not rows)


def t_dependencies_and_release(c: httpx.Client, db) -> None:
    section("dependency gating and atomic successor release")
    pid = make_pipeline(c)
    tasks = ["a", "b", "c", "d"]
    edges = [["a", "b"], ["a", "c"], ["b", "d"], ["c", "d"]]
    v = add_graph(c, pid, tasks, edges)
    sid = seal(c, pid, v, uid()).json()["seal_id"]
    rid = make_run(c, pid, sid, uid()).json()["run_id"]

    cl = claim(c, rid)
    check("only source task claimable initially",
          cl["claimed"] and cl["task_id"] == "a", str(cl))
    check("dependent tasks not claimable before predecessors succeed",
          claim(c, rid)["claimed"] is False)
    r = complete(c, rid, cl)
    check("completion releases exactly the direct successors",
          sorted(r.json()["released"]) == ["b", "c"], r.text)

    claimed = [claim(c, rid), claim(c, rid)]
    check("both released successors claimable",
          sorted(x["task_id"] for x in claimed) == ["b", "c"], str(claimed))
    check("join task still gated", claim(c, rid)["claimed"] is False)
    complete(c, rid, claimed[0])
    check("join task gated until ALL predecessors succeed",
          claim(c, rid)["claimed"] is False)
    complete(c, rid, claimed[1])
    cl = claim(c, rid)
    check("join task released after all predecessors succeed",
          cl["claimed"] and cl["task_id"] == "d", str(cl))
    complete(c, rid, cl)
    check("run succeeded", run_status(c, rid)["status"] == "succeeded")

    # DB-level atomicity invariant: no leased/succeeded task may ever be
    # observed with an unsucceeded predecessor.
    rows = db.execute(
        """
        SELECT 1 FROM run_tasks rt
        JOIN runs r ON r.id = rt.run_id
        JOIN seal_edges se ON se.seal_id = r.seal_id
                          AND se.dst_task_id = rt.task_id
        JOIN run_tasks p ON p.run_id = rt.run_id
                        AND p.task_id = se.src_task_id
        WHERE rt.run_id = %s AND rt.status IN ('leased','succeeded')
          AND p.status <> 'succeeded'
        LIMIT 1
        """, (rid,)).fetchall()
    check("invariant: never leased/succeeded with unsucceeded predecessor",
          not rows)


def t_leases_and_expiry(c: httpx.Client) -> None:
    section("leases: renew, DB-time expiry, lazy reaping")
    pid = make_pipeline(c)
    v = add_graph(c, pid, ["x"], [])
    sid = seal(c, pid, v, uid()).json()["seal_id"]
    rid = make_run(c, pid, sid, uid(), max_attempts=2, lease_seconds=2).json()["run_id"]

    cl = claim(c, rid)
    r = c.post(f"/runs/{rid}/claims/renew", json={
        "claim_token": cl["claim_token"], "attempt": cl["attempt"],
        "fencing_token": cl["fencing_token"]})
    check("renew extends the lease",
          r.status_code == 200
          and r.json()["lease_expires_at"] > cl["lease_expires_at"], r.text)
    r = c.post(f"/runs/{rid}/claims/renew", json={
        "claim_token": cl["claim_token"], "attempt": cl["attempt"],
        "fencing_token": cl["fencing_token"] + 1})
    check("renew with wrong fencing token rejected",
          r.status_code == 409 and r.json()["error"]["code"] == "CLAIM_STALE",
          r.text)
    r = c.post(f"/runs/{rid}/claims/renew", json={
        "claim_token": "0" * 64, "attempt": 1, "fencing_token": 1})
    check("unknown claim token rejected",
          r.status_code == 404 and r.json()["error"]["code"] == "CLAIM_NOT_FOUND")

    time.sleep(2.6)  # lease (2s, DB time) is now expired
    r = complete(c, rid, cl)
    check("expired lease cannot complete",
          r.status_code == 409 and r.json()["error"]["code"] == "CLAIM_EXPIRED",
          r.text)
    cl2 = claim(c, rid)
    check("expired task re-claimable via lazy reaping (no sweeper needed)",
          cl2["claimed"] and cl2["attempt"] == 2 and cl2["fencing_token"] == 2,
          str(cl2))
    r = complete(c, rid, cl2)
    check("new attempt completes", r.status_code == 200, r.text)
    check("run succeeded", run_status(c, rid)["status"] == "succeeded")


def t_failure_propagation(c: httpx.Client, db) -> None:
    section("permanent failure: blocked propagation and run terminal state")
    pid = make_pipeline(c)
    # x -> y -> y2 (chain that will break), z independent
    tasks = ["x", "y", "y2", "z"]
    edges = [["x", "y"], ["y", "y2"]]
    v = add_graph(c, pid, tasks, edges)
    sid = seal(c, pid, v, uid()).json()["seal_id"]
    rid = make_run(c, pid, sid, uid(), max_attempts=2, lease_seconds=2).json()["run_id"]

    cl = claim(c, rid)
    first = cl["task_id"]
    # fail whichever of x/z got claimed first; keep failing x until permanent
    if first != "x":
        complete(c, rid, cl)
        cl = claim(c, rid)
    check("claiming x", cl["claimed"] and cl["task_id"] == "x", str(cl))
    r = fail(c, rid, cl)
    check("first failure requeues", r.json()["status"] == "ready", r.text)
    cl = claim(c, rid)
    check("requeued x claimable with attempt 2",
          cl["claimed"] and cl["task_id"] == "x" and cl["attempt"] == 2, str(cl))
    r = fail(c, rid, cl)
    check("failure at max_attempts is permanent",
          r.json()["status"] == "failed", r.text)

    st = run_status(c, rid)
    check("transitive successors blocked, unrelated branch unaffected",
          st["counts"]["failed"] == 1 and st["counts"]["blocked"] == 2
          and st["status"] == "running", str(st))
    t_y2 = c.get(f"/runs/{rid}/tasks/y2").json()
    check("transitive successor is blocked", t_y2["status"] == "blocked")

    # finish the independent branch -> run must turn failed
    cl = claim(c, rid)
    check("unrelated task still claimable",
          cl["claimed"] and cl["task_id"] == "z", str(cl))
    complete(c, rid, cl)
    st = run_status(c, rid)
    check("run failed once nothing runnable remains",
          st["status"] == "failed", str(st))
    check("claim on failed run grants nothing",
          claim(c, rid)["claimed"] is False)

    # terminal state must not regress
    rows = db.execute("SELECT status FROM runs WHERE id=%s", (rid,)).fetchall()
    check("terminal state persisted", rows[0][0] == "failed")

    # expiry-driven permanent failure (no active fail call)
    pid2 = make_pipeline(c)
    v2 = add_graph(c, pid2, ["p", "q"], [["p", "q"]])
    sid2 = seal(c, pid2, v2, uid()).json()["seal_id"]
    rid2 = make_run(c, pid2, sid2, uid(), max_attempts=1,
                    lease_seconds=2).json()["run_id"]
    cl = claim(c, rid2)
    check("claimed p", cl["claimed"] and cl["task_id"] == "p")
    time.sleep(2.6)  # lease expires; max_attempts=1 -> permanent failure
    st = run_status(c, rid2)  # GET lazily reaps
    check("expired last-attempt lease permanently fails task and run",
          st["status"] == "failed" and st["counts"]["failed"] == 1
          and st["counts"]["blocked"] == 1, str(st))


def t_concurrent_workload_invariants(c: httpx.Client, db) -> None:
    section("concurrent workload: invariants hold throughout")
    pid = make_pipeline(c)
    # layered graph: 4 sources -> 4 middles -> 2 joins
    tasks = [f"s{i}" for i in range(4)] + [f"m{i}" for i in range(4)] + ["j0", "j1"]
    edges = [[f"s{i}", f"m{i}"] for i in range(4)]
    edges += [[f"m{i}", "j0"] for i in range(4)]
    edges += [[f"m{i}", "j1"] for i in range(2)]
    v = add_graph(c, pid, tasks, edges)
    sid = seal(c, pid, v, uid()).json()["seal_id"]
    rid = make_run(c, pid, sid, uid(), max_attempts=3, lease_seconds=5).json()["run_id"]

    violations: List[str] = []
    done = threading.Event()

    def watcher():
        # observe the run concurrently; the invariant must hold at all times
        with client() as cc:
            while not done.is_set():
                st = run_status(cc, rid)
                if st["status"] not in ("running", "succeeded", "failed"):
                    violations.append(f"bad status {st['status']}")
                time.sleep(0.02)

    def worker():
        with client() as cc:
            while not done.is_set():
                cl = claim(cc, rid)
                if not cl["claimed"]:
                    if cl.get("run_status") != "running":
                        return
                    time.sleep(0.02)
                    continue
                complete(cc, rid, cl)

    th = [threading.Thread(target=worker) for _ in range(6)]
    th.append(threading.Thread(target=watcher))
    [t.start() for t in th]
    deadline = time.time() + 60
    while time.time() < deadline:
        if run_status(c, rid)["status"] != "running":
            break
        time.sleep(0.2)
    done.set()
    [t.join() for t in th]

    st = run_status(c, rid)
    check("concurrent run reaches succeeded", st["status"] == "succeeded", str(st))
    check("no watcher violations", not violations, str(violations[:3]))
    rows = db.execute(
        """
        SELECT 1 FROM run_tasks rt
        JOIN runs r ON r.id = rt.run_id
        JOIN seal_edges se ON se.seal_id = r.seal_id
                          AND se.dst_task_id = rt.task_id
        JOIN run_tasks p ON p.run_id = rt.run_id
                        AND p.task_id = se.src_task_id
        WHERE rt.run_id = %s AND rt.status IN ('leased','succeeded')
          AND p.status <> 'succeeded'
        LIMIT 1
        """, (rid,)).fetchall()
    check("dependency invariant held throughout concurrent execution", not rows)
    rows = db.execute(
        "SELECT COUNT(*), COUNT(DISTINCT (task_id, attempt)) FROM leases "
        "WHERE run_id=%s", (rid,)).fetchall()
    check("each (task, attempt) granted exactly once",
          rows[0][0] == rows[0][1])


def t_scale_smoke(c: httpx.Client) -> None:
    section("scale smoke: 2000-task pipeline seals and runs")
    pid = make_pipeline(c)
    n = 2000
    tasks = [f"t{i:05d}" for i in range(n)]
    # chain layers of 50: layer i -> layer i+1 (all-to-all between layers)
    ops = [{"type": "add_task", "task_id": t} for t in tasks]
    for i in range(0, n, 500):
        r = mutate(c, pid, uid(), c.get(f"/pipelines/{pid}").json()["draft_version"],
                   ops[i:i + 500])
        assert r.status_code == 200, r.text
    v = c.get(f"/pipelines/{pid}").json()["draft_version"]
    edge_ops = []
    for layer in range(0, n - 50, 50):
        for s in tasks[layer:layer + 50]:
            for d in tasks[layer + 50:layer + 100]:
                edge_ops.append({"type": "add_edge", "src": s, "dst": d})
    # add edges in batches of 1000 (request limit)
    for i in range(0, len(edge_ops), 1000):
        r = mutate(c, pid, uid(), v, edge_ops[i:i + 1000])
        assert r.status_code == 200, r.text
        v = r.json()["draft_version"]
    check("large draft built", len(edge_ops) > 0)

    t0 = time.time()
    r = seal(c, pid, v, uid())
    check("large seal succeeds", r.status_code == 201, r.text)
    s = r.json()
    check("seal counts match", s["task_count"] == n
          and s["edge_count"] == len(edge_ops))
    check("seal digest matches independent digest",
          s["digest_sha256"] == independent_digest(
              tasks, [[o["src"], o["dst"]] for o in edge_ops]))
    check("seal topo order matches independent topo",
          s["topo_order"] == independent_topo(
              tasks, [[o["src"], o["dst"]] for o in edge_ops]))
    print(f"  (seal of {n} tasks / {len(edge_ops)} edges took "
          f"{time.time() - t0:.2f}s)")

    rid = make_run(c, pid, s["seal_id"], uid(), max_attempts=1,
                   lease_seconds=30).json()["run_id"]
    st = run_status(c, rid)
    check("large run starts with first layer ready",
          st["counts"]["ready"] == 50 and st["counts"]["pending"] == n - 50,
          str(st["counts"]))


def main() -> int:
    print(f"verify: API={API}")
    # wait for the stack
    deadline = time.time() + 120
    while True:
        try:
            with client() as c:
                if c.get("/healthz").status_code == 200:
                    break
        except Exception:
            pass
        if time.time() > deadline:
            print("API did not become healthy in time")
            return 1
        time.sleep(1)

    db = psycopg.connect(DB)
    db.autocommit = True

    with client() as c:
        t_draft_semantics(c)
        t_sealing(c, db)
        t_run_creation(c)
        t_claiming_and_completion(c, db)
        t_dependencies_and_release(c, db)
        t_leases_and_expiry(c)
        t_failure_propagation(c, db)
        t_concurrent_workload_invariants(c, db)
        t_scale_smoke(c)

    print(f"\nverify: {_checks - len(_failures)}/{_checks} checks passed")
    if _failures:
        print(f"verify: {len(_failures)} FAILURES")
        return 1
    print("verify: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
