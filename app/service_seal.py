"""Sealing: freeze a draft version into an immutable, content-addressed DAG."""

from __future__ import annotations

import json
import uuid

from psycopg import Connection

from . import errors
from .graph import canonical_digest, find_cycle, topo_order
from .service_draft import _check_key, _pipeline_row


def _seal_json(row: dict) -> dict:
    return {
        "id": str(row["id"]),
        "pipeline_id": str(row["pipeline_id"]),
        "draft_version": row["draft_version"],
        "topo_order": row["topo_order"],
        "digest": row["digest"],
        "task_count": row["task_count"],
        "edge_count": row["edge_count"],
        "created_at": row["created_at"].isoformat(),
    }


def create_seal(
    conn: Connection, pid: uuid.UUID, expected_draft_version: object, idempotency_key: object
) -> dict:
    """Seal the current draft atomically against concurrent mutations.

    The pipeline row lock serializes sealing with draft mutations and with
    seals from other API instances: a successful seal captures exactly one
    draft version, later mutations land in subsequent draft versions, and at
    most one seal exists per draft version.
    """
    if (
        not isinstance(expected_draft_version, int)
        or isinstance(expected_draft_version, bool)
        or expected_draft_version < 1
    ):
        raise errors.invalid_params("expected_draft_version must be a positive integer")
    idempotency_key = _check_key(idempotency_key, "idempotency_key")

    with conn.transaction():
        pipeline = _pipeline_row(conn, pid, lock=True)

        prior = conn.execute(
            "SELECT * FROM seals WHERE pipeline_id = %s AND idempotency_key = %s",
            (pid, idempotency_key),
        ).fetchone()
        if prior is not None:
            if prior["draft_version"] != expected_draft_version:
                raise errors.op_conflict(idempotency_key)
            return _seal_json(prior)

        current = pipeline["draft_version"]
        if expected_draft_version != current:
            raise errors.stale_version(current)

        clash = conn.execute(
            "SELECT id FROM seals WHERE pipeline_id = %s AND draft_version = %s",
            (pid, current),
        ).fetchone()
        if clash is not None:
            raise errors.seal_conflict(current, str(clash["id"]))

        tasks = [
            r["task_id"]
            for r in conn.execute(
                "SELECT task_id FROM draft_tasks WHERE pipeline_id = %s", (pid,)
            )
        ]
        edges = [
            (r["src"], r["dst"])
            for r in conn.execute(
                "SELECT src, dst FROM draft_edges WHERE pipeline_id = %s", (pid,)
            )
        ]

        witness = find_cycle(tasks, edges)
        if witness is not None:
            raise errors.cycle_detected(witness)
        order = topo_order(tasks, edges)
        assert order is not None  # acyclic was just established
        digest = canonical_digest(tasks, edges)

        seal_id = uuid.uuid4()
        row = conn.execute(
            """
            INSERT INTO seals (id, pipeline_id, draft_version, idempotency_key,
                               topo_order, digest, task_count, edge_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                seal_id,
                pid,
                current,
                idempotency_key,
                json.dumps(order),
                digest,
                len(tasks),
                len(edges),
            ),
        ).fetchone()
        conn.execute(
            "INSERT INTO seal_tasks (seal_id, task_id)"
            " SELECT %s, task_id FROM draft_tasks WHERE pipeline_id = %s",
            (seal_id, pid),
        )
        conn.execute(
            "INSERT INTO seal_edges (seal_id, src, dst)"
            " SELECT %s, src, dst FROM draft_edges WHERE pipeline_id = %s",
            (seal_id, pid),
        )
        return _seal_json(row)


def get_seal(conn: Connection, pid: uuid.UUID, seal_id: uuid.UUID) -> dict:
    row = conn.execute(
        "SELECT * FROM seals WHERE id = %s AND pipeline_id = %s", (seal_id, pid)
    ).fetchone()
    if row is None:
        raise errors.seal_not_found()
    return _seal_json(row)


def list_seals(conn: Connection, pid: uuid.UUID) -> list[dict]:
    _pipeline_row(conn, pid)
    rows = conn.execute(
        "SELECT * FROM seals WHERE pipeline_id = %s ORDER BY draft_version", (pid,)
    ).fetchall()
    return [_seal_json(r) for r in rows]
