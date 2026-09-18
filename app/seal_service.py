"""Sealing: freeze a draft version into an immutable, content-addressed seal.

The seal transaction holds the pipeline row lock, so a seal and any draft
mutation form a definite atomic order: a successful seal snapshots exactly
one draft version, and concurrent mutations land in later draft versions.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Tuple

from psycopg import Connection

from . import errors
from .draft_service import canonical_hash
from .graph import canonical_digest, find_cycle_witness, lexicographic_topo_order


def _load_draft(conn: Connection, pipeline_id: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    tasks = [r["task_id"] for r in conn.execute(
        "SELECT task_id FROM draft_tasks WHERE pipeline_id=%s",
        (pipeline_id,)).fetchall()]
    edges = [(r["src_task_id"], r["dst_task_id"]) for r in conn.execute(
        "SELECT src_task_id, dst_task_id FROM draft_edges WHERE pipeline_id=%s",
        (pipeline_id,)).fetchall()]
    return tasks, edges


def seal_pipeline(conn: Connection, pipeline_id: str,
                  expected_draft_version: int,
                  idempotency_key: str) -> Dict[str, Any]:
    fingerprint = canonical_hash({
        "pipeline_id": pipeline_id,
        "expected_draft_version": expected_draft_version,
    })

    with conn.transaction():
        pipe = conn.execute(
            "SELECT id, draft_version FROM pipelines WHERE id=%s FOR UPDATE",
            (pipeline_id,)).fetchone()
        if pipe is None:
            raise errors.not_found("PIPELINE", pipeline_id)

        # 1) idempotent replay of a previous successful seal with this key
        prev = conn.execute(
            "SELECT request_hash, response_json FROM seal_requests "
            "WHERE pipeline_id=%s AND idempotency_key=%s",
            (pipeline_id, idempotency_key)).fetchone()
        if prev is not None:
            if prev["request_hash"] != fingerprint:
                raise errors.idempotency_conflict(idempotency_key)
            return prev["response_json"]

        # 2) version check — nothing is recorded on failure
        current = pipe["draft_version"]
        if expected_draft_version != current:
            raise errors.seal_version_conflict(current)

        # 3) at most one seal per draft version, even across service instances
        existing = conn.execute(
            "SELECT id, digest_sha256 FROM seals "
            "WHERE pipeline_id=%s AND draft_version=%s",
            (pipeline_id, current)).fetchone()
        if existing is not None:
            raise errors.seal_already_exists(existing["id"],
                                             existing["digest_sha256"])

        # 4) snapshot the draft (pipeline lock makes this consistent)
        tasks, edges = _load_draft(conn, pipeline_id)

        # 5) deterministic cycle check — a cyclic draft can never be sealed
        witness = find_cycle_witness(tasks, edges)
        if witness is not None:
            raise errors.cycle_detected(witness)

        topo = lexicographic_topo_order(tasks, edges)
        digest = canonical_digest(tasks, edges)
        seal_id = uuid.uuid4().hex

        row = conn.execute(
            "INSERT INTO seals (id, pipeline_id, draft_version, task_count, "
            "                    edge_count, topo_order, digest_sha256, idempotency_key) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING created_at",
            (seal_id, pipeline_id, current, len(tasks), len(edges),
             json.dumps(topo), digest, idempotency_key)).fetchone()
        conn.execute(
            "INSERT INTO seal_tasks (seal_id, task_id) "
            "SELECT %s, task_id FROM draft_tasks WHERE pipeline_id=%s",
            (seal_id, pipeline_id))
        conn.execute(
            "INSERT INTO seal_edges (seal_id, src_task_id, dst_task_id) "
            "SELECT %s, src_task_id, dst_task_id FROM draft_edges WHERE pipeline_id=%s",
            (seal_id, pipeline_id))

        response = {
            "seal_id": seal_id,
            "pipeline_id": pipeline_id,
            "draft_version": current,
            "task_count": len(tasks),
            "edge_count": len(edges),
            "topo_order": topo,
            "digest_sha256": digest,
            "created_at": row["created_at"].isoformat(),
        }
        conn.execute(
            "INSERT INTO seal_requests (pipeline_id, idempotency_key, "
            "                          request_hash, response_json) "
            "VALUES (%s, %s, %s, %s)",
            (pipeline_id, idempotency_key, fingerprint, json.dumps(response)))
        return response


def get_seal(conn: Connection, pipeline_id: str, seal_id: str) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT id, pipeline_id, draft_version, task_count, edge_count, "
        "       topo_order, digest_sha256, created_at "
        "FROM seals WHERE id=%s AND pipeline_id=%s",
        (seal_id, pipeline_id)).fetchone()
    if row is None:
        raise errors.not_found("SEAL", seal_id)
    return {
        "seal_id": row["id"],
        "pipeline_id": row["pipeline_id"],
        "draft_version": row["draft_version"],
        "task_count": row["task_count"],
        "edge_count": row["edge_count"],
        "topo_order": row["topo_order"],
        "digest_sha256": row["digest_sha256"],
        "created_at": row["created_at"].isoformat(),
    }
