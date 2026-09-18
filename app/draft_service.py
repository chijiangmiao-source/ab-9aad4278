"""Pipeline and draft-revision logic.

Concurrency model: every mutation transaction takes a row lock on the
pipeline row (`SELECT ... FOR UPDATE`), which serializes all draft
revisions and seal operations for that pipeline into a definite atomic
order. op_id idempotency is checked before the version check so a retried
request always replays its first result even when the version has moved on.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Dict, List

from psycopg import Connection
from psycopg.errors import UniqueViolation

from . import errors
from .schemas import MAX_EDGES, MAX_TASKS


def canonical_hash(payload: Dict[str, Any]) -> str:
    """Stable fingerprint of a request body (used for idempotency checks)."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def create_pipeline(conn: Connection, name: str | None) -> Dict[str, Any]:
    pid = uuid.uuid4().hex
    with conn.transaction():
        conn.execute(
            "INSERT INTO pipelines (id, name) VALUES (%s, %s)", (pid, name))
    return {"pipeline_id": pid, "name": name, "draft_version": 0}


def get_pipeline(conn: Connection, pipeline_id: str) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT id, name, draft_version, created_at FROM pipelines WHERE id=%s",
        (pipeline_id,)).fetchone()
    if row is None:
        raise errors.not_found("PIPELINE", pipeline_id)
    counts = conn.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM draft_tasks WHERE pipeline_id=%s) AS tasks,
          (SELECT COUNT(*) FROM draft_edges WHERE pipeline_id=%s) AS edges
        """, (pipeline_id, pipeline_id)).fetchone()
    seals = conn.execute(
        """
        SELECT id, draft_version, digest_sha256, task_count, edge_count, created_at
        FROM seals WHERE pipeline_id=%s ORDER BY draft_version
        """, (pipeline_id,)).fetchall()
    return {
        "pipeline_id": row["id"],
        "name": row["name"],
        "draft_version": row["draft_version"],
        "task_count": counts["tasks"],
        "edge_count": counts["edges"],
        "created_at": row["created_at"].isoformat(),
        "seals": [{
            "seal_id": s["id"],
            "draft_version": s["draft_version"],
            "digest_sha256": s["digest_sha256"],
            "task_count": s["task_count"],
            "edge_count": s["edge_count"],
            "created_at": s["created_at"].isoformat(),
        } for s in seals],
    }


def get_draft_tasks(conn: Connection, pipeline_id: str,
                    offset: int, limit: int) -> Dict[str, Any]:
    _require_pipeline(conn, pipeline_id)
    total = conn.execute(
        "SELECT COUNT(*) AS c FROM draft_tasks WHERE pipeline_id=%s",
        (pipeline_id,)).fetchone()["c"]
    rows = conn.execute(
        "SELECT task_id FROM draft_tasks WHERE pipeline_id=%s "
        "ORDER BY task_id LIMIT %s OFFSET %s",
        (pipeline_id, limit, offset)).fetchall()
    return {"total": total, "offset": offset, "limit": limit,
            "tasks": [r["task_id"] for r in rows]}


def get_draft_edges(conn: Connection, pipeline_id: str,
                    offset: int, limit: int) -> Dict[str, Any]:
    _require_pipeline(conn, pipeline_id)
    total = conn.execute(
        "SELECT COUNT(*) AS c FROM draft_edges WHERE pipeline_id=%s",
        (pipeline_id,)).fetchone()["c"]
    rows = conn.execute(
        "SELECT src_task_id, dst_task_id FROM draft_edges WHERE pipeline_id=%s "
        "ORDER BY src_task_id, dst_task_id LIMIT %s OFFSET %s",
        (pipeline_id, limit, offset)).fetchall()
    return {"total": total, "offset": offset, "limit": limit,
            "edges": [{"src": r["src_task_id"], "dst": r["dst_task_id"]}
                      for r in rows]}


def _require_pipeline(conn: Connection, pipeline_id: str) -> None:
    row = conn.execute("SELECT 1 FROM pipelines WHERE id=%s",
                       (pipeline_id,)).fetchone()
    if row is None:
        raise errors.not_found("PIPELINE", pipeline_id)


def _lock_pipeline(conn: Connection, pipeline_id: str) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT id, draft_version FROM pipelines WHERE id=%s FOR UPDATE",
        (pipeline_id,)).fetchone()
    if row is None:
        raise errors.not_found("PIPELINE", pipeline_id)
    return row


def mutate_draft(conn: Connection, pipeline_id: str, op_id: str,
                 draft_version: int,
                 operations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply a batch of draft operations atomically.

    Idempotent on op_id; version-checked; all-or-nothing.
    """
    try:
        return _mutate_draft_tx(conn, pipeline_id, op_id, draft_version,
                                operations)
    except UniqueViolation:
        # Another pipeline concurrently inserted the same globally-unique
        # op_id (a client-side reuse bug). Our transaction rolled back
        # cleanly; report the conflict.
        raise errors.op_conflict(op_id)


def _mutate_draft_tx(conn: Connection, pipeline_id: str, op_id: str,
                     draft_version: int,
                     operations: List[Dict[str, Any]]) -> Dict[str, Any]:
    fingerprint = canonical_hash({
        "pipeline_id": pipeline_id,
        "draft_version": draft_version,
        "operations": operations,
    })

    with conn.transaction():
        pipe = _lock_pipeline(conn, pipeline_id)

        # 1) op_id replay / conflict — checked before the version check so a
        #    retried request replays its first result even after the version
        #    has advanced.
        existing = conn.execute(
            "SELECT request_hash, response_json FROM draft_ops WHERE op_id=%s",
            (op_id,)).fetchone()
        if existing is not None:
            if existing["request_hash"] != fingerprint:
                raise errors.op_conflict(op_id)
            return existing["response_json"]

        # 2) version check — stale callers get the current version, nothing
        #    is written, and the op is not recorded (it never happened).
        current = pipe["draft_version"]
        if draft_version != current:
            raise errors.version_conflict(current)

        # 3) apply operations sequentially inside this transaction
        removed_edges: List[Dict[str, str]] = []
        task_count = conn.execute(
            "SELECT COUNT(*) AS c FROM draft_tasks WHERE pipeline_id=%s",
            (pipeline_id,)).fetchone()["c"]
        edge_count = conn.execute(
            "SELECT COUNT(*) AS c FROM draft_edges WHERE pipeline_id=%s",
            (pipeline_id,)).fetchone()["c"]

        for op in operations:
            kind = op["type"]
            if kind == "add_task":
                if task_count + 1 > MAX_TASKS:
                    raise errors.ApiError(
                        409, "LIMIT_EXCEEDED",
                        f"pipeline may not exceed {MAX_TASKS} tasks")
                cur = conn.execute(
                    "INSERT INTO draft_tasks (pipeline_id, task_id) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (pipeline_id, op["task_id"]))
                if cur.rowcount == 0:
                    raise errors.ApiError(
                        409, "TASK_EXISTS",
                        f"task {op['task_id']!r} already exists",
                        {"task_id": op["task_id"]})
                task_count += 1
            elif kind == "remove_task":
                cur = conn.execute(
                    "DELETE FROM draft_tasks WHERE pipeline_id=%s AND task_id=%s",
                    (pipeline_id, op["task_id"]))
                if cur.rowcount == 0:
                    raise errors.ApiError(
                        409, "TASK_NOT_FOUND",
                        f"task {op['task_id']!r} does not exist",
                        {"task_id": op["task_id"]})
                task_count -= 1
                cascaded = conn.execute(
                    "DELETE FROM draft_edges WHERE pipeline_id=%s "
                    "AND (src_task_id=%s OR dst_task_id=%s) "
                    "RETURNING src_task_id, dst_task_id",
                    (pipeline_id, op["task_id"], op["task_id"])).fetchall()
                edge_count -= len(cascaded)
                for r in sorted(cascaded,
                                key=lambda r: (r["src_task_id"], r["dst_task_id"])):
                    removed_edges.append(
                        {"src": r["src_task_id"], "dst": r["dst_task_id"]})
            elif kind == "add_edge":
                src, dst = op["src"], op["dst"]
                if src == dst:
                    raise errors.ApiError(
                        409, "SELF_LOOP", "self loops are not allowed",
                        {"task_id": src})
                missing = conn.execute(
                    """
                    SELECT t.task_id FROM (VALUES (%s), (%s)) AS t(task_id)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM draft_tasks d
                        WHERE d.pipeline_id=%s AND d.task_id=t.task_id)
                    """, (src, dst, pipeline_id)).fetchall()
                if missing:
                    raise errors.ApiError(
                        409, "EDGE_ENDPOINT_MISSING",
                        "edge endpoint task does not exist",
                        {"missing": sorted(m["task_id"] for m in missing)})
                if edge_count + 1 > MAX_EDGES:
                    raise errors.ApiError(
                        409, "LIMIT_EXCEEDED",
                        f"pipeline may not exceed {MAX_EDGES} edges")
                cur = conn.execute(
                    "INSERT INTO draft_edges (pipeline_id, src_task_id, dst_task_id) "
                    "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (pipeline_id, src, dst))
                if cur.rowcount == 0:
                    raise errors.ApiError(
                        409, "EDGE_EXISTS",
                        f"edge {src!r} -> {dst!r} already exists",
                        {"src": src, "dst": dst})
                edge_count += 1
            elif kind == "remove_edge":
                cur = conn.execute(
                    "DELETE FROM draft_edges WHERE pipeline_id=%s "
                    "AND src_task_id=%s AND dst_task_id=%s",
                    (pipeline_id, op["src"], op["dst"]))
                if cur.rowcount == 0:
                    raise errors.ApiError(
                        409, "EDGE_NOT_FOUND",
                        f"edge {op['src']!r} -> {op['dst']!r} does not exist",
                        {"src": op["src"], "dst": op["dst"]})
                edge_count -= 1
            else:  # pragma: no cover - pydantic validates the union
                raise errors.invalid_request(f"unknown operation {kind!r}")

        # 4) bump version and record the op for replay
        new_version = current + 1
        conn.execute(
            "UPDATE pipelines SET draft_version=%s, updated_at=now() WHERE id=%s",
            (new_version, pipeline_id))
        response = {
            "pipeline_id": pipeline_id,
            "draft_version": new_version,
            "applied": len(operations),
            "removed_edges": removed_edges,
            "task_count": task_count,
            "edge_count": edge_count,
        }
        conn.execute(
            "INSERT INTO draft_ops (op_id, pipeline_id, request_hash, response_json) "
            "VALUES (%s, %s, %s, %s)",
            (op_id, pipeline_id, fingerprint, json.dumps(response)))
        return response
