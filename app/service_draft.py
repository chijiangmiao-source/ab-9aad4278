"""Pipeline drafts: creation, queries, and idempotent, version-checked mutations."""

from __future__ import annotations

import hashlib
import json
import re
import uuid

from psycopg import Connection

from . import errors

TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
MAX_OPS_PER_REQUEST = 10_000

OP_ADD_TASK = "add_task"
OP_REMOVE_TASK = "remove_task"
OP_ADD_EDGE = "add_edge"
OP_REMOVE_EDGE = "remove_edge"
_OP_TYPES = {OP_ADD_TASK, OP_REMOVE_TASK, OP_ADD_EDGE, OP_REMOVE_EDGE}


def _check_task_id(task_id: object) -> str:
    if not isinstance(task_id, str) or not TASK_ID_RE.match(task_id):
        raise errors.invalid_task_id(task_id)
    return task_id


def _check_key(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 200:
        raise errors.invalid_params(f"{field} must be a non-empty string of at most 200 chars")
    return value


def create_pipeline(conn: Connection) -> dict:
    pid = uuid.uuid4()
    row = conn.execute(
        "INSERT INTO pipelines (id) VALUES (%s) RETURNING id, draft_version, created_at",
        (pid,),
    ).fetchone()
    return {
        "id": str(row["id"]),
        "draft_version": row["draft_version"],
        "task_count": 0,
        "edge_count": 0,
        "created_at": row["created_at"].isoformat(),
    }


def _pipeline_row(conn: Connection, pid: uuid.UUID, lock: bool = False) -> dict:
    sql = "SELECT id, draft_version, created_at FROM pipelines WHERE id = %s"
    if lock:
        sql += " FOR UPDATE"
    row = conn.execute(sql, (pid,)).fetchone()
    if row is None:
        raise errors.pipeline_not_found()
    return row


def get_pipeline(conn: Connection, pid: uuid.UUID) -> dict:
    row = _pipeline_row(conn, pid)
    counts = conn.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM draft_tasks WHERE pipeline_id = %s) AS task_count,
          (SELECT COUNT(*) FROM draft_edges WHERE pipeline_id = %s) AS edge_count
        """,
        (pid, pid),
    ).fetchone()
    seals = conn.execute(
        """
        SELECT id, draft_version, digest, task_count, edge_count, created_at
        FROM seals WHERE pipeline_id = %s ORDER BY draft_version
        """,
        (pid,),
    ).fetchall()
    return {
        "id": str(row["id"]),
        "draft_version": row["draft_version"],
        "task_count": counts["task_count"],
        "edge_count": counts["edge_count"],
        "created_at": row["created_at"].isoformat(),
        "seals": [
            {
                "id": str(s["id"]),
                "draft_version": s["draft_version"],
                "digest": s["digest"],
                "task_count": s["task_count"],
                "edge_count": s["edge_count"],
                "created_at": s["created_at"].isoformat(),
            }
            for s in seals
        ],
    }


def get_draft(conn: Connection, pid: uuid.UUID) -> dict:
    row = _pipeline_row(conn, pid)
    tasks = [
        r["task_id"]
        for r in conn.execute(
            "SELECT task_id FROM draft_tasks WHERE pipeline_id = %s ORDER BY task_id", (pid,)
        )
    ]
    edges = [
        [r["src"], r["dst"]]
        for r in conn.execute(
            "SELECT src, dst FROM draft_edges WHERE pipeline_id = %s ORDER BY src, dst", (pid,)
        )
    ]
    return {
        "id": str(row["id"]),
        "draft_version": row["draft_version"],
        "tasks": tasks,
        "edges": edges,
    }


def _canonical_ops(ops: list[dict]) -> list[dict]:
    """Validate op shapes and normalize to a canonical form used both for the
    idempotency hash and for application."""
    if not isinstance(ops, list) or not ops:
        raise errors.invalid_op("ops must be a non-empty list")
    if len(ops) > MAX_OPS_PER_REQUEST:
        raise errors.invalid_op(f"too many ops in one request (max {MAX_OPS_PER_REQUEST})")
    canonical = []
    for i, op in enumerate(ops):
        if not isinstance(op, dict):
            raise errors.invalid_op(f"op #{i} must be an object")
        otype = op.get("type")
        if otype not in _OP_TYPES:
            raise errors.invalid_op(f"op #{i}: unknown type {otype!r}", {"index": i})
        if otype in (OP_ADD_TASK, OP_REMOVE_TASK):
            tid = _check_task_id(op.get("task_id"))
            canonical.append({"type": otype, "task_id": tid})
        else:
            src = _check_task_id(op.get("src"))
            dst = _check_task_id(op.get("dst"))
            if otype == OP_ADD_EDGE and src == dst:
                raise errors.self_loop(src)
            canonical.append({"type": otype, "src": src, "dst": dst})
    return canonical


def _request_hash(draft_version: int, canonical_ops: list[dict]) -> str:
    payload = json.dumps(
        {"draft_version": draft_version, "ops": canonical_ops},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def apply_mutations(
    conn: Connection, pid: uuid.UUID, op_id: object, draft_version: object, ops: object
) -> dict:
    """Apply a batch of draft mutations atomically.

    Idempotency: the first result for (pipeline, op_id) is persisted and
    replayed for an identical request; reusing op_id with different parameters
    conflicts. A stale draft_version aborts the whole batch without writes.
    """
    op_id = _check_key(op_id, "op_id")
    if not isinstance(draft_version, int) or isinstance(draft_version, bool) or draft_version < 1:
        raise errors.invalid_params("draft_version must be a positive integer")
    canonical_ops = _canonical_ops(ops)
    req_hash = _request_hash(draft_version, canonical_ops)

    with conn.transaction():
        pipeline = _pipeline_row(conn, pid, lock=True)

        prior = conn.execute(
            "SELECT request_hash, response_json FROM draft_ops WHERE pipeline_id = %s AND op_id = %s",
            (pid, op_id),
        ).fetchone()
        if prior is not None:
            if prior["request_hash"] != req_hash:
                raise errors.op_conflict(op_id)
            return prior["response_json"]

        current = pipeline["draft_version"]
        if draft_version != current:
            raise errors.stale_version(current)

        _validate_against_db(conn, pid, canonical_ops)
        _apply(conn, pid, canonical_ops)

        new_version = current + 1
        conn.execute(
            "UPDATE pipelines SET draft_version = %s WHERE id = %s", (new_version, pid)
        )
        response = {
            "pipeline_id": str(pid),
            "draft_version": new_version,
            "applied": len(canonical_ops),
        }
        conn.execute(
            "INSERT INTO draft_ops (pipeline_id, op_id, request_hash, response_json)"
            " VALUES (%s, %s, %s, %s)",
            (pid, op_id, req_hash, json.dumps(response)),
        )
        return response


def _validate_against_db(conn: Connection, pid: uuid.UUID, ops: list[dict]) -> None:
    add_tasks = [o["task_id"] for o in ops if o["type"] == OP_ADD_TASK]
    del_tasks = [o["task_id"] for o in ops if o["type"] == OP_REMOVE_TASK]
    add_edges = [(o["src"], o["dst"]) for o in ops if o["type"] == OP_ADD_EDGE]
    del_edges = [(o["src"], o["dst"]) for o in ops if o["type"] == OP_REMOVE_EDGE]

    # --- within-batch consistency -------------------------------------
    seen: set[str] = set()
    for t in add_tasks:
        if t in seen:
            raise errors.task_exists(t)
        seen.add(t)
    removed: set[str] = set()
    for t in del_tasks:
        if t in removed:
            raise errors.task_not_found(t)
        removed.add(t)
    both = removed & seen
    if both:
        raise errors.invalid_op(
            "task both added and removed in one batch", {"task_id": sorted(both)[0]}
        )
    seen_e: set[tuple[str, str]] = set()
    for e in add_edges:
        if e in seen_e:
            raise errors.edge_exists(*e)
        seen_e.add(e)
    removed_e: set[tuple[str, str]] = set()
    for e in del_edges:
        if e in removed_e:
            raise errors.edge_not_found(*e)
        removed_e.add(e)
    both_e = removed_e & seen_e
    if both_e:
        e = sorted(both_e)[0]
        raise errors.invalid_op("edge both added and removed in one batch", {"src": e[0], "dst": e[1]})

    # --- against persisted state --------------------------------------
    existing_tasks: set[str] = set()
    candidates = list(set(add_tasks) | removed | {s for s, _ in add_edges} | {d for _, d in add_edges})
    if candidates:
        existing_tasks = {
            r["task_id"]
            for r in conn.execute(
                "SELECT task_id FROM draft_tasks WHERE pipeline_id = %s AND task_id = ANY(%s)",
                (pid, candidates),
            )
        }
    for t in add_tasks:
        if t in existing_tasks:
            raise errors.task_exists(t)
    for t in del_tasks:
        if t not in existing_tasks:
            raise errors.task_not_found(t)

    all_edges = list(set(add_edges) | removed_e)
    existing_edges: set[tuple[str, str]] = set()
    if all_edges:
        srcs = [e[0] for e in all_edges]
        dsts = [e[1] for e in all_edges]
        existing_edges = {
            (r["src"], r["dst"])
            for r in conn.execute(
                """
                SELECT src, dst FROM draft_edges
                WHERE pipeline_id = %s
                  AND (src, dst) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
                """,
                (pid, srcs, dsts),
            )
        }
    for e in add_edges:
        if e in existing_edges:
            raise errors.edge_exists(*e)
    for e in del_edges:
        if e not in existing_edges:
            raise errors.edge_not_found(*e)

    final_tasks = (existing_tasks | set(add_tasks)) - removed
    for src, dst in add_edges:
        if src not in final_tasks:
            raise errors.task_not_found(src)
        if dst not in final_tasks:
            raise errors.task_not_found(dst)


def _apply(conn: Connection, pid: uuid.UUID, ops: list[dict]) -> None:
    del_edges = [(o["src"], o["dst"]) for o in ops if o["type"] == OP_REMOVE_EDGE]
    del_tasks = [o["task_id"] for o in ops if o["type"] == OP_REMOVE_TASK]
    add_tasks = [o["task_id"] for o in ops if o["type"] == OP_ADD_TASK]
    add_edges = [(o["src"], o["dst"]) for o in ops if o["type"] == OP_ADD_EDGE]

    if del_edges:
        srcs = [e[0] for e in del_edges]
        dsts = [e[1] for e in del_edges]
        conn.execute(
            """
            DELETE FROM draft_edges
            WHERE pipeline_id = %s
              AND (src, dst) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
            """,
            (pid, srcs, dsts),
        )
    if del_tasks:
        # Incident edges are removed by ON DELETE CASCADE.
        conn.execute(
            "DELETE FROM draft_tasks WHERE pipeline_id = %s AND task_id = ANY(%s)",
            (pid, del_tasks),
        )
    if add_tasks:
        conn.execute(
            "INSERT INTO draft_tasks (pipeline_id, task_id)"
            " SELECT %s, t FROM unnest(%s::text[]) AS t",
            (pid, add_tasks),
        )
    if add_edges:
        srcs = [e[0] for e in add_edges]
        dsts = [e[1] for e in add_edges]
        conn.execute(
            "INSERT INTO draft_edges (pipeline_id, src, dst)"
            " SELECT %s, s, d FROM unnest(%s::text[], %s::text[]) AS e(s, d)",
            (pid, srcs, dsts),
        )
