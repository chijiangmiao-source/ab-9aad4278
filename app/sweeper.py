"""Background lease sweeper.

This process only *prompts* expired leases back into the claimable set.
Correctness never depends on it: claim and status endpoints reap expired
leases lazily inside their own transactions, and every expiry judgement
uses database time. The sweeper may run zero, one, or many times, on any
instance, concurrently — reaping is idempotent and lock-guarded.
"""

from __future__ import annotations

import asyncio
import logging
import os

from .db import get_pool
from .run_service import reap_expired, runs_with_expired_leases

log = logging.getLogger("sweeper")

SWEEP_INTERVAL_SECONDS = float(os.environ.get("SWEEP_INTERVAL_SECONDS", "0.5"))


def _sweep_once() -> int:
    pool = get_pool()
    with pool.connection() as conn:
        run_ids = runs_with_expired_leases(conn)
        conn.commit()
    total = 0
    for run_id in run_ids:
        try:
            with pool.connection() as conn:
                with conn.transaction():
                    total += reap_expired(conn, run_id)
        except Exception:  # pragma: no cover - defensive; next pass retries
            log.exception("sweep failed for run %s", run_id)
    return total


async def sweeper_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.to_thread(_sweep_once)
        except Exception:  # pragma: no cover - defensive
            log.exception("sweeper iteration failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=SWEEP_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
