"""Run lifecycle: creation, leasing, completion, failure, reaping.

Correctness rules implemented here:

* A task is claimable only when every direct predecessor has succeeded
  (it is in ``ready`` with ``pending_preds = 0``).
* A claim grants ``attempt``/``fencing_token`` exactly once
  (``SELECT ... FOR UPDATE SKIP LOCKED`` on the task row).
* Fencing tokens are strictly increasing per task and never reused.
* Renew/complete/fail validate the claim token, attempt, fencing token and
  the lease deadline against database time; stale or expired requests are
  rejected without any state change.
* Expired leases are reaped lazily inside claim/status transactions and by
  the background sweeper; correctness never depends on the sweeper running.
* Successor release, failure propagation (transitive ``blocked``) and run
  terminal transitions happen in the same transaction as the task update,
  so they are atomically observable. Terminal states never regress.

Serialization: every run-scoped mutating transaction first takes a
per-run PostgreSQL advisory transaction lock. This gives a single,
deadlock-free lock order (advisory lock -> leases/run_tasks -> runs) and
makes terminal-state checks observe every committed task transition.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from typing import Any, Dict, List, Optional

from psycopg import Connection

from . import errors
from .draft_service import canonical_hash

TASK_STATUSES = ("pending", "ready", "leased", "succeeded", "failed", "blocked")


def _run_lock_key(run_id: str) -> int:
    return int.from_bytes(hashlib.sha256(run_id.encode()).digest()[:8],
                          "big", signed=True)


def _lock_run(conn: Connection, run_id: str) -> None:
    """Serialize all run-scoped mutations for this run (transaction-scoped)."""
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (_run_lock_key(run_id),))


# ---------------------------------------------------------------------------
# run creation
# ---------------------------------------------------------------------------
def create_run(conn: Connection, pipeline_id: str, seal_id: str,
               idempotency_key: str, max_attempts: int,
               lease_seconds: int) -> Dict[str, Any]:
    fingerprint = canonical_hash({
        "pipeline_id": pipeline_id,
        "seal_id": seal_id,
        "max_attempts": max_attempts,
        "lease_seconds": lease_seconds,
    })

    with conn.transaction():
        # Serialize run creations per pipeline so the idempotency
        # check-then-insert below is race-free across instances.
        pipe = conn.execute(
            "SELECT id FROM pipelines WHERE id=%s FOR UPDATE",
            (pipeline_id,)).fetchone()
        if pipe is None:
            raise errors.not_found("PIPELINE", pipeline_id)

        prev = conn.execute(
            "SELECT request_hash, response_json FROM run_requests "
            "WHERE pipeline_id=%s AND idempotency_key=%s",
            (pipeline_id, idempotency_key)).fetchone()
        if prev is not None:
            if prev["request_hash"] != fingerprint:
                raise errors.idempotency_conflict(idempotency_key)
            return prev["response_json"]

        seal = conn.execute(
            "SELECT id, task_count FROM seals WHERE id=%s AND pipeline_id=%s",
            (seal_id, pipeline_id)).fetchone()
        if seal is None:
            raise errors.not_found("SEAL", seal_id)

        run_id = uuid.uuid4().hex
        status = "running" if seal["task_count"] > 0 else "succeeded"
        row = conn.execute(
            "INSERT INTO runs (id, pipeline_id, seal_id, status, max_attempts, "
            "                  lease_seconds, idempotency_key) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING created_at",
            (run_id, pipeline_id, seal_id, status, max_attempts, lease_seconds,
             idempotency_key)).fetchone()

        # Materialize per-run task state from the immutable seal. Tasks with
        # no predecessors start ready; everyone else waits on pending_preds.
        conn.execute(
            """
            INSERT INTO run_tasks (run_id, task_id, status, pending_preds)
            SELECT %s, st.task_id,
                   CASE WHEN COALESCE(deg.c, 0) = 0 THEN 'ready' ELSE 'pending' END,
                   COALESCE(deg.c, 0)
            FROM seal_tasks st
            LEFT JOIN (
                SELECT dst_task_id, COUNT(*) AS c
                FROM seal_edges WHERE seal_id = %s GROUP BY dst_task_id
            ) deg ON deg.dst_task_id = st.task_id
            WHERE st.seal_id = %s
            """, (run_id, seal_id, seal_id))

        response = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "seal_id": seal_id,
            "status": status,
            "max_attempts": max_attempts,
            "lease_seconds": lease_seconds,
            "created_at": row["created_at"].isoformat(),
        }
        conn.execute(
            "INSERT INTO run_requests (pipeline_id, idempotency_key, "
            "                         request_hash, response_json) "
            "VALUES (%s, %s, %s, %s)",
            (pipeline_id, idempotency_key, fingerprint, json.dumps(response)))
        return response


# ---------------------------------------------------------------------------
# lease reaping (lazy + sweeper share this exact logic)
# ---------------------------------------------------------------------------
def _propagate_blocked(conn: Connection, run_id: str, seal_id: str,
                       failed_task_ids: List[str]) -> None:
    """Mark every transitive successor of the failed tasks as blocked."""
    if not failed_task_ids:
        return
    conn.execute(
        """
        WITH RECURSIVE succ AS (
            SELECT e.dst_task_id AS tid
            FROM seal_edges e
            WHERE e.seal_id = %s AND e.src_task_id = ANY(%s)
            UNION
            SELECT e.dst_task_id
            FROM seal_edges e JOIN succ s ON e.src_task_id = s.tid
        )
        UPDATE run_tasks rt
        SET status = 'blocked', updated_at = now()
        WHERE rt.run_id = %s AND rt.status = 'pending'
          AND rt.task_id IN (SELECT tid FROM succ)
        """, (seal_id, failed_task_ids, run_id))


def _refresh_run_terminal(conn: Connection, run_id: str) -> None:
    """Terminal transitions, guarded so a terminal state never regresses."""
    conn.execute(
        """
        UPDATE runs SET status='failed', updated_at=now()
        WHERE id=%s AND status='running'
          AND EXISTS (SELECT 1 FROM run_tasks
                      WHERE run_id=%s AND status='failed')
          AND NOT EXISTS (SELECT 1 FROM run_tasks
                          WHERE run_id=%s AND status IN ('pending','ready','leased'))
        """, (run_id, run_id, run_id))
    conn.execute(
        """
        UPDATE runs SET status='succeeded', updated_at=now()
        WHERE id=%s AND status='running'
          AND NOT EXISTS (SELECT 1 FROM run_tasks
                          WHERE run_id=%s AND status <> 'succeeded')
        """, (run_id, run_id))


def _reap_expired_locked(conn: Connection, run_id: str) -> int:
    """Reap expired active leases of one run (caller holds the run lock).

    Idempotent; safe to call any number of times. Returns the number reaped.
    """
    run = conn.execute(
        "SELECT id, seal_id, max_attempts, status FROM runs WHERE id=%s",
        (run_id,)).fetchone()
    if run is None:
        raise errors.not_found("RUN", run_id)
    if run["status"] != "running":
        return 0

    expired = conn.execute(
        """
        UPDATE leases SET state='expired'
        WHERE run_id=%s AND state='active' AND lease_expires_at <= now()
        RETURNING task_id, attempt
        """, (run_id,)).fetchall()
    if not expired:
        return 0

    newly_failed: List[str] = []
    for row in expired:
        # The attempt guard ensures we only reap the lease that is still the
        # task's current attempt; a newer attempt can never exist here because
        # the lease was active until the UPDATE above.
        updated = conn.execute(
            """
            UPDATE run_tasks rt
            SET status = CASE WHEN rt.attempts < %s THEN 'ready' ELSE 'failed' END,
                updated_at = now()
            WHERE rt.run_id=%s AND rt.task_id=%s AND rt.status='leased'
              AND rt.attempts=%s
            RETURNING rt.status
            """, (run["max_attempts"], run_id, row["task_id"],
                  row["attempt"])).fetchone()
        if updated and updated["status"] == "failed":
            newly_failed.append(row["task_id"])

    _propagate_blocked(conn, run_id, run["seal_id"], newly_failed)
    _refresh_run_terminal(conn, run_id)
    return len(expired)


def reap_expired(conn: Connection, run_id: str) -> int:
    """Public reaper used by the sweeper: takes the run lock itself."""
    _lock_run(conn, run_id)
    return _reap_expired_locked(conn, run_id)


def runs_with_expired_leases(conn: Connection, limit: int = 64) -> List[str]:
    rows = conn.execute(
        "SELECT DISTINCT run_id FROM leases "
        "WHERE state='active' AND lease_expires_at <= now() LIMIT %s",
        (limit,)).fetchall()
    return [r["run_id"] for r in rows]


# ---------------------------------------------------------------------------
# claiming
# ---------------------------------------------------------------------------
def claim(conn: Connection, run_id: str) -> Dict[str, Any]:
    with conn.transaction():
        _lock_run(conn, run_id)
        _reap_expired_locked(conn, run_id)  # lazy reaping: no sweeper needed
        run = conn.execute(
            "SELECT id, status, lease_seconds FROM runs WHERE id=%s",
            (run_id,)).fetchone()
        if run["status"] != "running":
            return {"claimed": False, "run_status": run["status"]}

        task = conn.execute(
            """
            SELECT task_id FROM run_tasks
            WHERE run_id=%s AND status='ready'
            ORDER BY task_id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """, (run_id,)).fetchone()
        if task is None:
            return {"claimed": False, "run_status": "running"}

        updated = conn.execute(
            """
            UPDATE run_tasks
            SET attempts = attempts + 1,
                fencing_token = fencing_token + 1,
                status = 'leased',
                updated_at = now()
            WHERE run_id=%s AND task_id=%s AND status='ready'
            RETURNING attempts, fencing_token
            """, (run_id, task["task_id"])).fetchone()

        claim_token = secrets.token_hex(32)
        token_hash = hashlib.sha256(claim_token.encode("ascii")).hexdigest()
        lease = conn.execute(
            """
            INSERT INTO leases (run_id, task_id, attempt, fencing_token,
                                claim_token_hash, lease_expires_at)
            VALUES (%s, %s, %s, %s, %s, now() + %s * interval '1 second')
            RETURNING lease_expires_at
            """, (run_id, task["task_id"], updated["attempts"],
                  updated["fencing_token"], token_hash,
                  run["lease_seconds"])).fetchone()

        return {
            "claimed": True,
            "run_id": run_id,
            "task_id": task["task_id"],
            "attempt": updated["attempts"],
            "fencing_token": updated["fencing_token"],
            "lease_expires_at": lease["lease_expires_at"].isoformat(),
            "claim_token": claim_token,
        }


# ---------------------------------------------------------------------------
# shared claim validation
# ---------------------------------------------------------------------------
def _load_and_validate_claim(conn: Connection, run_id: str, claim_token: str,
                             attempt: int, fencing_token: int) -> Dict[str, Any]:
    """Load the lease and task row (both locked) and validate freshness.

    Raises on any staleness/expiry without changing state. Returns the lease
    row when the claim is valid and active.
    """
    token_hash = hashlib.sha256(claim_token.encode("ascii")).hexdigest()
    lease = conn.execute(
        "SELECT * FROM leases WHERE claim_token_hash=%s FOR UPDATE",
        (token_hash,)).fetchone()
    if lease is None or lease["run_id"] != run_id:
        raise errors.claim_not_found()

    if lease["state"] == "expired":
        raise errors.claim_expired()
    if lease["state"] != "active":
        raise errors.claim_not_active()

    task = conn.execute(
        "SELECT * FROM run_tasks WHERE run_id=%s AND task_id=%s FOR UPDATE",
        (run_id, lease["task_id"])).fetchone()

    if (attempt != lease["attempt"] or fencing_token != lease["fencing_token"]
            or lease["attempt"] != task["attempts"]
            or lease["fencing_token"] != task["fencing_token"]):
        raise errors.claim_stale()
    if task["status"] != "leased":
        raise errors.claim_not_active()

    expired = conn.execute(
        "SELECT (%s::timestamptz <= now()) AS expired",
        (lease["lease_expires_at"],)).fetchone()["expired"]
    if expired:
        raise errors.claim_expired()
    return lease


def _get_run(conn: Connection, run_id: str) -> Dict[str, Any]:
    run = conn.execute(
        "SELECT id, seal_id, status, max_attempts, lease_seconds "
        "FROM runs WHERE id=%s", (run_id,)).fetchone()
    if run is None:
        raise errors.not_found("RUN", run_id)
    return run


# ---------------------------------------------------------------------------
# renew / complete / fail
# ---------------------------------------------------------------------------
def renew(conn: Connection, run_id: str, claim_token: str, attempt: int,
          fencing_token: int) -> Dict[str, Any]:
    with conn.transaction():
        _lock_run(conn, run_id)
        run = _get_run(conn, run_id)
        lease = _load_and_validate_claim(conn, run_id, claim_token, attempt,
                                         fencing_token)
        row = conn.execute(
            """
            UPDATE leases
            SET lease_expires_at = now() + %s * interval '1 second'
            WHERE run_id=%s AND task_id=%s AND attempt=%s
            RETURNING lease_expires_at
            """, (run["lease_seconds"], run_id, lease["task_id"],
                  lease["attempt"])).fetchone()
        return {"run_id": run_id, "task_id": lease["task_id"],
                "attempt": lease["attempt"],
                "lease_expires_at": row["lease_expires_at"].isoformat()}


def complete(conn: Connection, run_id: str, claim_token: str, attempt: int,
             fencing_token: int, output_digest: str) -> Dict[str, Any]:
    token_hash = hashlib.sha256(claim_token.encode("ascii")).hexdigest()
    with conn.transaction():
        _lock_run(conn, run_id)
        run = _get_run(conn, run_id)

        # Idempotent replay of a previous successful completion.
        prev = conn.execute(
            "SELECT * FROM leases WHERE claim_token_hash=%s",
            (token_hash,)).fetchone()
        if prev is not None and prev["run_id"] == run_id \
                and prev["state"] == "completed":
            if prev["completed_digest"] == output_digest:
                return prev["result_json"]
            raise errors.digest_conflict()

        lease = _load_and_validate_claim(conn, run_id, claim_token, attempt,
                                         fencing_token)
        task_id = lease["task_id"]

        conn.execute(
            "UPDATE leases SET state='completed', completed_digest=%s "
            "WHERE run_id=%s AND task_id=%s AND attempt=%s",
            (output_digest, run_id, task_id, lease["attempt"]))
        conn.execute(
            "UPDATE run_tasks SET status='succeeded', output_digest=%s, "
            "            updated_at=now() "
            "WHERE run_id=%s AND task_id=%s",
            (output_digest, run_id, task_id))

        # Release direct successors in the same transaction.
        released_rows = conn.execute(
            """
            UPDATE run_tasks rt
            SET pending_preds = rt.pending_preds - 1,
                status = CASE WHEN rt.pending_preds - 1 = 0
                              THEN 'ready' ELSE rt.status END,
                updated_at = now()
            WHERE rt.run_id=%s AND rt.status='pending'
              AND rt.task_id IN (
                    SELECT dst_task_id FROM seal_edges
                    WHERE seal_id=%s AND src_task_id=%s)
            RETURNING rt.task_id, rt.status
            """, (run_id, run["seal_id"], task_id)).fetchall()
        released = sorted(r["task_id"] for r in released_rows
                          if r["status"] == "ready")

        _refresh_run_terminal(conn, run_id)
        run_status = conn.execute(
            "SELECT status FROM runs WHERE id=%s", (run_id,)).fetchone()["status"]

        response = {"run_id": run_id, "task_id": task_id,
                    "status": "succeeded", "run_status": run_status,
                    "released": released}
        conn.execute(
            "UPDATE leases SET result_json=%s "
            "WHERE run_id=%s AND task_id=%s AND attempt=%s",
            (json.dumps(response), run_id, task_id, lease["attempt"]))
        return response


def fail(conn: Connection, run_id: str, claim_token: str, attempt: int,
         fencing_token: int, reason: Optional[str]) -> Dict[str, Any]:
    token_hash = hashlib.sha256(claim_token.encode("ascii")).hexdigest()
    with conn.transaction():
        _lock_run(conn, run_id)
        run = _get_run(conn, run_id)

        # Idempotent replay of a previous failure report on this claim.
        prev = conn.execute(
            "SELECT * FROM leases WHERE claim_token_hash=%s",
            (token_hash,)).fetchone()
        if prev is not None and prev["run_id"] == run_id \
                and prev["state"] == "failed":
            return prev["result_json"]

        lease = _load_and_validate_claim(conn, run_id, claim_token, attempt,
                                         fencing_token)
        task_id = lease["task_id"]

        permanently_failed = lease["attempt"] >= run["max_attempts"]
        new_status = "failed" if permanently_failed else "ready"

        conn.execute(
            "UPDATE leases SET state='failed', fail_reason=%s "
            "WHERE run_id=%s AND task_id=%s AND attempt=%s",
            (reason, run_id, task_id, lease["attempt"]))
        conn.execute(
            "UPDATE run_tasks SET status=%s, updated_at=now() "
            "WHERE run_id=%s AND task_id=%s",
            (new_status, run_id, task_id))

        if permanently_failed:
            _propagate_blocked(conn, run_id, run["seal_id"], [task_id])
        _refresh_run_terminal(conn, run_id)
        run_status = conn.execute(
            "SELECT status FROM runs WHERE id=%s", (run_id,)).fetchone()["status"]

        response = {"run_id": run_id, "task_id": task_id,
                    "status": new_status, "run_status": run_status,
                    "attempts_used": lease["attempt"],
                    "max_attempts": run["max_attempts"]}
        conn.execute(
            "UPDATE leases SET result_json=%s "
            "WHERE run_id=%s AND task_id=%s AND attempt=%s",
            (json.dumps(response), run_id, task_id, lease["attempt"]))
        return response


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------
def get_run(conn: Connection, run_id: str) -> Dict[str, Any]:
    with conn.transaction():
        _lock_run(conn, run_id)
        _reap_expired_locked(conn, run_id)  # lazy reaping keeps views truthful
        run = conn.execute(
            "SELECT id, pipeline_id, seal_id, status, max_attempts, "
            "       lease_seconds, created_at, updated_at "
            "FROM runs WHERE id=%s", (run_id,)).fetchone()
        counts = {s: 0 for s in TASK_STATUSES}
        for row in conn.execute(
                "SELECT status, COUNT(*) AS c FROM run_tasks "
                "WHERE run_id=%s GROUP BY status", (run_id,)).fetchall():
            counts[row["status"]] = row["c"]
        return {
            "run_id": run["id"],
            "pipeline_id": run["pipeline_id"],
            "seal_id": run["seal_id"],
            "status": run["status"],
            "max_attempts": run["max_attempts"],
            "lease_seconds": run["lease_seconds"],
            "counts": counts,
            "created_at": run["created_at"].isoformat(),
            "updated_at": run["updated_at"].isoformat(),
        }


def get_run_task(conn: Connection, run_id: str, task_id: str) -> Dict[str, Any]:
    with conn.transaction():
        _lock_run(conn, run_id)
        _reap_expired_locked(conn, run_id)
        row = conn.execute(
            "SELECT task_id, status, attempts, fencing_token, pending_preds, "
            "       output_digest, updated_at "
            "FROM run_tasks WHERE run_id=%s AND task_id=%s",
            (run_id, task_id)).fetchone()
        if row is None:
            raise errors.not_found("TASK", task_id)
        return {
            "run_id": run_id,
            "task_id": row["task_id"],
            "status": row["status"],
            "attempts": row["attempts"],
            "fencing_token": row["fencing_token"],
            "pending_preds": row["pending_preds"],
            "output_digest": row["output_digest"],
            "updated_at": row["updated_at"].isoformat(),
        }


def list_run_tasks(conn: Connection, run_id: str, status: Optional[str],
                   offset: int, limit: int) -> Dict[str, Any]:
    with conn.transaction():
        _lock_run(conn, run_id)
        _reap_expired_locked(conn, run_id)
        if status is not None and status not in TASK_STATUSES:
            raise errors.invalid_request(
                f"status must be one of {sorted(TASK_STATUSES)}")
        if status is None:
            total = conn.execute(
                "SELECT COUNT(*) AS c FROM run_tasks WHERE run_id=%s",
                (run_id,)).fetchone()["c"]
            rows = conn.execute(
                "SELECT task_id, status, attempts, fencing_token, output_digest "
                "FROM run_tasks WHERE run_id=%s "
                "ORDER BY task_id LIMIT %s OFFSET %s",
                (run_id, limit, offset)).fetchall()
        else:
            total = conn.execute(
                "SELECT COUNT(*) AS c FROM run_tasks WHERE run_id=%s AND status=%s",
                (run_id, status)).fetchone()["c"]
            rows = conn.execute(
                "SELECT task_id, status, attempts, fencing_token, output_digest "
                "FROM run_tasks WHERE run_id=%s AND status=%s "
                "ORDER BY task_id LIMIT %s OFFSET %s",
                (run_id, status, limit, offset)).fetchall()
        return {"run_id": run_id, "total": total, "offset": offset,
                "limit": limit, "tasks": rows}
