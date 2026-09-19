"""Runs: creation, claiming, heartbeats, completion, failure, and sweeping.

Correctness invariants (all enforced in the database, never in-process):

* A task is claimable only in state 'ready'; 'ready' is reached only in the
  same transaction that marked its last direct predecessor succeeded.
* A claim atomically increments attempts_used and fencing_token under a row
  lock, so a (task, attempt) pair and a fencing token are each granted once.
* Terminal claim outcomes are validated against attempt + fencing_token +
  claim_token + database-time lease expiry; stale grants can never mutate
  state, even if an old worker resurfaces later.
* Successor release, failure propagation, counter updates, and run terminal
  transitions commit in the same transaction as the task outcome.

Lock ordering: transactions that perform multi-row updates (complete, fail,
sweep-with-exhaustion) first take a per-run advisory transaction lock, then
row locks. Claims, heartbeats, and the requeue-branch of the sweep only ever
touch single rows or use SKIP LOCKED, so they never wait on the advisory
lock and cannot participate in a deadlock cycle.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import uuid

from psycopg import Connection

from . import errors
from .service_draft import _check_key, _pipeline_row

DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


def _lock_run(conn: Connection, run_id: uuid.UUID) -> None:
    """Per-run advisory transaction lock serializing outcome transactions."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (str(run_id),))


# --------------------------------------------------------------------------
# creation & queries
# --------------------------------------------------------------------------

def _run_json(row: dict) -> dict:
    return {
        "id": str(row["id"]),
        "pipeline_id": str(row["pipeline_id"]),
        "seal_id": str(row["seal_id"]),
        "status": row["status"],
        "max_attempts": row["max_attempts"],
        "lease_seconds": row["lease_seconds"],
        "total_tasks": row["total_tasks"],
        "succeeded_count": row["succeeded_count"],
        "failed_count": row["failed_count"],
        "blocked_count": row["blocked_count"],
        "created_at": row["created_at"].isoformat(),
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
    }


def create_run(
    conn: Connection,
    pid: uuid.UUID,
    sealed_version_id: object,
    idempotency_key: object,
    max_attempts: object,
    lease_seconds: object,
) -> dict:
    idempotency_key = _check_key(idempotency_key, "idempotency_key")
    try:
        seal_id = uuid.UUID(str(sealed_version_id))
    except (ValueError, AttributeError, TypeError):
        raise errors.invalid_params("sealed_version_id must be a UUID")
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or not 1 <= max_attempts <= 10:
        raise errors.invalid_params("max_attempts must be an integer in [1, 10]")
    if not isinstance(lease_seconds, int) or isinstance(lease_seconds, bool) or not 2 <= lease_seconds <= 30:
        raise errors.invalid_params("lease_seconds must be an integer in [2, 30]")

    with conn.transaction():
        # Serializes run creation per pipeline, making the idempotency check
        # and insert atomic.
        _pipeline_row(conn, pid, lock=True)

        prior = conn.execute(
            "SELECT * FROM runs WHERE pipeline_id = %s AND idempotency_key = %s",
            (pid, idempotency_key),
        ).fetchone()
        if prior is not None:
            same = (
                prior["seal_id"] == seal_id
                and prior["max_attempts"] == max_attempts
                and prior["lease_seconds"] == lease_seconds
            )
            if not same:
                raise errors.op_conflict(idempotency_key)
            return _run_json(prior)

        seal = conn.execute(
            "SELECT * FROM seals WHERE id = %s AND pipeline_id = %s", (seal_id, pid)
        ).fetchone()
        if seal is None:
            raise errors.seal_not_found()

        run_id = uuid.uuid4()
        total = seal["task_count"]
        status = "succeeded" if total == 0 else "running"
        row = conn.execute(
            """
            INSERT INTO runs (id, pipeline_id, seal_id, idempotency_key, max_attempts,
                              lease_seconds, status, total_tasks, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                    CASE WHEN %s THEN clock_timestamp() ELSE NULL END)
            RETURNING *
            """,
            (run_id, pid, seal_id, idempotency_key, max_attempts, lease_seconds,
             status, total, total == 0),
        ).fetchone()

        # Materialize per-run task/edge state from the immutable seal.
        conn.execute(
            """
            INSERT INTO run_tasks (run_id, task_id, state, remaining_preds)
            SELECT %s, t.task_id,
                   CASE WHEN COALESCE(c.cnt, 0) = 0 THEN 'ready' ELSE 'waiting' END,
                   COALESCE(c.cnt, 0)
            FROM seal_tasks t
            LEFT JOIN (
                SELECT dst, COUNT(*) AS cnt FROM seal_edges WHERE seal_id = %s GROUP BY dst
            ) c ON c.dst = t.task_id
            WHERE t.seal_id = %s
            """,
            (run_id, seal_id, seal_id),
        )
        conn.execute(
            "INSERT INTO run_edges (run_id, src, dst)"
            " SELECT %s, src, dst FROM seal_edges WHERE seal_id = %s",
            (run_id, seal_id),
        )
        return _run_json(row)


def _fetch_run(conn: Connection, run_id: uuid.UUID) -> dict:
    row = conn.execute("SELECT * FROM runs WHERE id = %s", (run_id,)).fetchone()
    if row is None:
        raise errors.run_not_found()
    return row


def get_run(conn: Connection, run_id: uuid.UUID) -> dict:
    return _run_json(_fetch_run(conn, run_id))


def get_run_task(conn: Connection, run_id: uuid.UUID, task_id: str) -> dict:
    _fetch_run(conn, run_id)
    row = conn.execute(
        """
        SELECT task_id, state, remaining_preds, attempts_used, fencing_token,
               attempt, lease_expires_at, output_digest
        FROM run_tasks WHERE run_id = %s AND task_id = %s
        """,
        (run_id, task_id),
    ).fetchone()
    if row is None:
        raise errors.task_not_found(task_id)
    return _task_json(row)


def list_run_tasks(
    conn: Connection, run_id: uuid.UUID, state: str | None, limit: int, offset: int
) -> dict:
    _fetch_run(conn, run_id)
    valid_states = {"waiting", "ready", "claimed", "succeeded", "failed", "blocked"}
    if state is not None and state not in valid_states:
        raise errors.invalid_params(f"state must be one of {sorted(valid_states)}")
    where = "WHERE run_id = %s" + (" AND state = %s" if state else "")
    params: list = [run_id, state] if state else [run_id]
    total = conn.execute(
        f"SELECT COUNT(*) AS c FROM run_tasks {where}", params
    ).fetchone()["c"]
    rows = conn.execute(
        f"""
        SELECT task_id, state, remaining_preds, attempts_used, fencing_token,
               attempt, lease_expires_at, output_digest
        FROM run_tasks {where} ORDER BY task_id LIMIT %s OFFSET %s
        """,
        params + [limit, offset],
    ).fetchall()
    return {"total": total, "tasks": [_task_json(r) for r in rows]}


def _task_json(row: dict) -> dict:
    # claim_token is a bearer capability and is only ever returned by the
    # claim endpoint itself.
    return {
        "task_id": row["task_id"],
        "state": row["state"],
        "remaining_preds": row["remaining_preds"],
        "attempts_used": row["attempts_used"],
        "fencing_token": row["fencing_token"],
        "attempt": row["attempt"],
        "lease_expires_at": (
            row["lease_expires_at"].isoformat() if row["lease_expires_at"] else None
        ),
        "output_digest": row["output_digest"],
    }


# --------------------------------------------------------------------------
# lease sweeping (lazy + background; correctness never depends on it)
# --------------------------------------------------------------------------

def _propagate_failures(conn: Connection, run_id: uuid.UUID, failed_ids: list[str]) -> None:
    """Block every transitive successor of newly failed tasks and update the
    run counters and terminal status. Caller must hold the per-run advisory
    lock; everything commits in the caller's transaction."""
    if not failed_ids:
        return
    cur = conn.execute(
        """
        WITH RECURSIVE reach(dst) AS (
            SELECT e.dst FROM run_edges e
            WHERE e.run_id = %s AND e.src = ANY(%s)
            UNION
            SELECT e.dst FROM run_edges e
            JOIN reach r ON e.src = r.dst
            WHERE e.run_id = %s
        )
        UPDATE run_tasks t SET state = 'blocked'
        FROM reach r
        WHERE t.run_id = %s AND t.task_id = r.dst AND t.state = 'waiting'
        """,
        (run_id, failed_ids, run_id, run_id),
    )
    blocked = cur.rowcount
    conn.execute(
        "UPDATE runs SET failed_count = failed_count + %s, blocked_count = blocked_count + %s"
        " WHERE id = %s",
        (len(failed_ids), blocked, run_id),
    )
    conn.execute(
        """
        UPDATE runs SET status = 'failed', finished_at = clock_timestamp()
        WHERE id = %s AND status = 'running' AND failed_count > 0
          AND succeeded_count + failed_count + blocked_count = total_tasks
        """,
        (run_id,),
    )


def _sweep_run(conn: Connection, run_id: uuid.UUID, max_attempts: int) -> None:
    """Requeue or permanently fail tasks whose lease expired (database time).

    Rows are locked with SELECT ... FOR UPDATE SKIP LOCKED and then updated
    by primary key: concurrent sweeps/claims never block each other, rows
    currently owned by another transaction are skipped (that transaction owns
    their outcome), and the update itself can never wait or misbehave. The
    exhaustion branch additionally holds the per-run advisory lock before its
    multi-row failure propagation.
    """
    requeue = conn.execute(
        """
        SELECT task_id FROM run_tasks
        WHERE run_id = %s AND state = 'claimed'
          AND lease_expires_at <= clock_timestamp()
          AND attempts_used < %s
        FOR UPDATE SKIP LOCKED
        """,
        (run_id, max_attempts),
    ).fetchall()
    if requeue:
        conn.execute(
            """
            UPDATE run_tasks
            SET state = 'ready', attempt = NULL, claim_token = NULL, lease_expires_at = NULL
            WHERE run_id = %s AND task_id = ANY(%s)
            """,
            (run_id, [r["task_id"] for r in requeue]),
        )

    # Cheap existence check first so the common case never touches the
    # advisory lock. Taking the advisory lock before any row locks keeps the
    # global lock order (advisory -> rows) shared with complete()/fail().
    pending = conn.execute(
        """
        SELECT 1 FROM run_tasks
        WHERE run_id = %s AND state = 'claimed'
          AND lease_expires_at <= clock_timestamp()
          AND attempts_used >= %s
        LIMIT 1
        """,
        (run_id, max_attempts),
    ).fetchone()
    if pending is None:
        return
    _lock_run(conn, run_id)
    rows = conn.execute(
        """
        SELECT task_id FROM run_tasks
        WHERE run_id = %s AND state = 'claimed'
          AND lease_expires_at <= clock_timestamp()
          AND attempts_used >= %s
        FOR UPDATE SKIP LOCKED
        """,
        (run_id, max_attempts),
    ).fetchall()
    if not rows:
        return
    failed = conn.execute(
        """
        UPDATE run_tasks
        SET state = 'failed', attempt = NULL, claim_token = NULL, lease_expires_at = NULL
        WHERE run_id = %s AND task_id = ANY(%s)
        RETURNING task_id
        """,
        (run_id, [r["task_id"] for r in rows]),
    ).fetchall()
    _propagate_failures(conn, run_id, [r["task_id"] for r in failed])


def sweep_expired(conn: Connection, limit: int = 200) -> int:
    """Best-effort background sweep across runs. Each run is swept and
    committed independently so one failing run cannot undo the others; the
    lazy sweep in claim() provides the correctness floor regardless."""
    rows = conn.execute(
        """
        SELECT DISTINCT run_id FROM run_tasks
        WHERE state = 'claimed' AND lease_expires_at <= clock_timestamp()
        LIMIT %s
        """,
        (limit,),
    ).fetchall()
    conn.commit()
    swept = 0
    for r in rows:
        try:
            run = conn.execute(
                "SELECT max_attempts, status FROM runs WHERE id = %s", (r["run_id"],)
            ).fetchone()
            if run is not None and run["status"] == "running":
                _sweep_run(conn, r["run_id"], run["max_attempts"])
                swept += 1
            conn.commit()
        except Exception:
            conn.rollback()
    return swept


# --------------------------------------------------------------------------
# claiming
# --------------------------------------------------------------------------

def claim(conn: Connection, run_id: uuid.UUID, task_id: str | None) -> dict:
    token = secrets.token_hex(32)
    with conn.transaction():
        run = _fetch_run(conn, run_id)
        if run["status"] != "running":
            raise errors.run_not_running(run["status"])
        # Lazy sweep: expired leases become claimable (or terminally failed)
        # in the same transaction, so correctness never needs the background
        # sweeper to have run.
        _sweep_run(conn, run_id, run["max_attempts"])

        if task_id is not None:
            row = conn.execute(
                """
                UPDATE run_tasks
                SET state = 'claimed',
                    attempts_used = attempts_used + 1,
                    fencing_token = fencing_token + 1,
                    attempt = attempts_used + 1,
                    claim_token = %s,
                    lease_expires_at = clock_timestamp() + make_interval(secs => %s)
                WHERE run_id = %s AND task_id = %s AND state = 'ready'
                RETURNING task_id, attempt, fencing_token, lease_expires_at
                """,
                (token, run["lease_seconds"], run_id, task_id),
            ).fetchone()
            if row is None:
                state = conn.execute(
                    "SELECT state FROM run_tasks WHERE run_id = %s AND task_id = %s",
                    (run_id, task_id),
                ).fetchone()
                if state is None:
                    raise errors.task_not_found(task_id)
                raise errors.task_not_ready(task_id, state["state"])
        else:
            # Two-step queue pop: lock one ready row with SKIP LOCKED, then
            # update it by primary key. (An UPDATE ... FROM (SELECT ... FOR
            # UPDATE SKIP LOCKED) one-liner is not safe: inlined into the
            # outer query it can update multiple rows under EPQ re-checks.)
            pick = conn.execute(
                """
                SELECT task_id FROM run_tasks
                WHERE run_id = %s AND state = 'ready'
                ORDER BY task_id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (run_id,),
            ).fetchone()
            if pick is None:
                raise errors.no_ready_task()
            row = conn.execute(
                """
                UPDATE run_tasks
                SET state = 'claimed',
                    attempts_used = attempts_used + 1,
                    fencing_token = fencing_token + 1,
                    attempt = attempts_used + 1,
                    claim_token = %s,
                    lease_expires_at = clock_timestamp() + make_interval(secs => %s)
                WHERE run_id = %s AND task_id = %s
                RETURNING task_id, attempt, fencing_token, lease_expires_at
                """,
                (token, run["lease_seconds"], run_id, pick["task_id"]),
            ).fetchone()

        return {
            "run_id": str(run_id),
            "task_id": row["task_id"],
            "attempt": row["attempt"],
            "fencing_token": row["fencing_token"],
            "claim_token": token,
            "lease_expires_at": row["lease_expires_at"].isoformat(),
            "lease_seconds": run["lease_seconds"],
        }


# --------------------------------------------------------------------------
# claim actions (heartbeat / complete / fail)
# --------------------------------------------------------------------------

def _fetch_task_for_update(conn: Connection, run_id: uuid.UUID, task_id: str) -> dict:
    row = conn.execute(
        """
        SELECT state, attempt, fencing_token, claim_token, attempts_used,
               (lease_expires_at > clock_timestamp()) AS lease_alive
        FROM run_tasks WHERE run_id = %s AND task_id = %s
        FOR UPDATE
        """,
        (run_id, task_id),
    ).fetchone()
    if row is None:
        raise errors.task_not_found(task_id)
    return row


def _validate_claim_row(row: dict, attempt: int, fencing_token: int, claim_token: str) -> None:
    """Validate the presented grant against the persisted one. Any mismatch
    raises without mutating state."""
    if fencing_token < row["fencing_token"]:
        raise errors.fencing_stale()
    if fencing_token > row["fencing_token"]:
        raise errors.claim_invalid("unknown fencing_token")
    if row["state"] != "claimed":
        raise errors.claim_invalid("task is not currently claimed")
    if attempt != row["attempt"]:
        raise errors.claim_invalid("attempt mismatch")
    if claim_token != row["claim_token"]:
        raise errors.claim_invalid("claim_token mismatch")
    if not row["lease_alive"]:
        raise errors.lease_expired()


def heartbeat(
    conn: Connection, run_id: uuid.UUID, task_id: str,
    attempt: int, fencing_token: int, claim_token: str,
) -> dict:
    with conn.transaction():
        run = _fetch_run(conn, run_id)
        row = _fetch_task_for_update(conn, run_id, task_id)
        _validate_claim_row(row, attempt, fencing_token, claim_token)
        row = conn.execute(
            """
            UPDATE run_tasks
            SET lease_expires_at = clock_timestamp() + make_interval(secs => %s)
            WHERE run_id = %s AND task_id = %s
            RETURNING lease_expires_at
            """,
            (run["lease_seconds"], run_id, task_id),
        ).fetchone()
        return {
            "run_id": str(run_id),
            "task_id": task_id,
            "attempt": attempt,
            "lease_expires_at": row["lease_expires_at"].isoformat(),
        }


def _prior_result(
    conn: Connection, run_id: uuid.UUID, task_id: str,
    claim_token: str, kind: str, content_hash: str,
) -> dict | None:
    prior = conn.execute(
        "SELECT run_id, task_id, kind, content_hash, response_json"
        " FROM claim_results WHERE claim_token = %s",
        (claim_token,),
    ).fetchone()
    if prior is None:
        return None
    if str(prior["run_id"]) != str(run_id) or prior["task_id"] != task_id:
        raise errors.claim_invalid("claim_token does not belong to this task")
    if prior["kind"] != kind or prior["content_hash"] != content_hash:
        raise errors.content_conflict()
    return prior["response_json"]


def complete(
    conn: Connection, run_id: uuid.UUID, task_id: str, attempt: int,
    fencing_token: int, claim_token: str, output_digest: object,
) -> dict:
    if not isinstance(output_digest, str) or not DIGEST_RE.match(output_digest):
        raise errors.invalid_digest()
    content_hash = hashlib.sha256(f"complete:{output_digest}".encode()).hexdigest()

    with conn.transaction():
        # Fast path: a retry of an already-committed outcome.
        replay = _prior_result(conn, run_id, task_id, claim_token, "complete", content_hash)
        if replay is not None:
            return replay

        _lock_run(conn, run_id)
        _fetch_run(conn, run_id)
        row = _fetch_task_for_update(conn, run_id, task_id)
        # A concurrent duplicate may have committed while we waited on locks.
        replay = _prior_result(conn, run_id, task_id, claim_token, "complete", content_hash)
        if replay is not None:
            return replay
        _validate_claim_row(row, attempt, fencing_token, claim_token)

        conn.execute(
            """
            UPDATE run_tasks
            SET state = 'succeeded', output_digest = %s,
                attempt = NULL, claim_token = NULL, lease_expires_at = NULL
            WHERE run_id = %s AND task_id = %s
            """,
            (output_digest, run_id, task_id),
        )
        conn.execute(
            "UPDATE runs SET succeeded_count = succeeded_count + 1 WHERE id = %s",
            (run_id,),
        )
        # Atomically release direct successors whose last predecessor this was.
        rows = conn.execute(
            """
            UPDATE run_tasks t
            SET remaining_preds = t.remaining_preds - 1,
                state = CASE WHEN t.remaining_preds - 1 = 0 THEN 'ready' ELSE t.state END
            FROM run_edges e
            WHERE e.run_id = %s AND e.src = %s
              AND t.run_id = %s AND t.task_id = e.dst AND t.state = 'waiting'
            RETURNING t.task_id, (t.state = 'ready') AS released
            """,
            (run_id, task_id, run_id),
        ).fetchall()
        released = sorted(r["task_id"] for r in rows if r["released"])

        conn.execute(
            """
            UPDATE runs SET status = 'succeeded', finished_at = clock_timestamp()
            WHERE id = %s AND status = 'running' AND succeeded_count = total_tasks
            """,
            (run_id,),
        )
        # A success can also be the event that leaves nothing runnable: if a
        # permanent failure exists and every task is now terminal, the run
        # fails. Both terminal checks commit in this same transaction.
        conn.execute(
            """
            UPDATE runs SET status = 'failed', finished_at = clock_timestamp()
            WHERE id = %s AND status = 'running' AND failed_count > 0
              AND succeeded_count + failed_count + blocked_count = total_tasks
            """,
            (run_id,),
        )
        run_status = conn.execute(
            "SELECT status FROM runs WHERE id = %s", (run_id,)
        ).fetchone()["status"]

        response = {
            "run_id": str(run_id),
            "task_id": task_id,
            "attempt": attempt,
            "state": "succeeded",
            "released": released,
            "run_status": run_status,
        }
        conn.execute(
            """
            INSERT INTO claim_results
                (claim_token, run_id, task_id, attempt, kind, content_hash, response_json)
            VALUES (%s, %s, %s, %s, 'complete', %s, %s)
            """,
            (claim_token, run_id, task_id, attempt, content_hash, json.dumps(response)),
        )
        return response


def fail(
    conn: Connection, run_id: uuid.UUID, task_id: str, attempt: int,
    fencing_token: int, claim_token: str, error: object = None,
) -> dict:
    if error is not None and not isinstance(error, str):
        raise errors.invalid_params("error must be a string")
    content_hash = hashlib.sha256(f"fail:{error or ''}".encode()).hexdigest()

    with conn.transaction():
        replay = _prior_result(conn, run_id, task_id, claim_token, "fail", content_hash)
        if replay is not None:
            return replay

        _lock_run(conn, run_id)
        run = _fetch_run(conn, run_id)
        row = _fetch_task_for_update(conn, run_id, task_id)
        replay = _prior_result(conn, run_id, task_id, claim_token, "fail", content_hash)
        if replay is not None:
            return replay
        _validate_claim_row(row, attempt, fencing_token, claim_token)

        attempts_used = row["attempts_used"]
        exhausted = attempts_used >= run["max_attempts"]

        if exhausted:
            conn.execute(
                """
                UPDATE run_tasks
                SET state = 'failed', attempt = NULL, claim_token = NULL, lease_expires_at = NULL
                WHERE run_id = %s AND task_id = %s
                """,
                (run_id, task_id),
            )
            _propagate_failures(conn, run_id, [task_id])
            new_state = "failed"
        else:
            conn.execute(
                """
                UPDATE run_tasks
                SET state = 'ready', attempt = NULL, claim_token = NULL, lease_expires_at = NULL
                WHERE run_id = %s AND task_id = %s
                """,
                (run_id, task_id),
            )
            new_state = "ready"

        run_status = conn.execute(
            "SELECT status FROM runs WHERE id = %s", (run_id,)
        ).fetchone()["status"]
        response = {
            "run_id": str(run_id),
            "task_id": task_id,
            "attempt": attempt,
            "state": new_state,
            "attempts_used": attempts_used,
            "max_attempts": run["max_attempts"],
            "run_status": run_status,
        }
        conn.execute(
            """
            INSERT INTO claim_results
                (claim_token, run_id, task_id, attempt, kind, content_hash, response_json)
            VALUES (%s, %s, %s, %s, 'fail', %s, %s)
            """,
            (claim_token, run_id, task_id, attempt, content_hash, json.dumps(response)),
        )
        return response
