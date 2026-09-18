"""One-shot migration runner: `python -m app.migrate`."""

from __future__ import annotations

import logging
import os
import time

import psycopg

from .db import DATABASE_URL, run_migrations

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("migrate")


def main() -> None:
    deadline = time.time() + 120
    while True:
        try:
            applied = run_migrations()
            log.info("migrations applied: %s", applied or "none (up to date)")
            return
        except psycopg.OperationalError as exc:
            if time.time() > deadline:
                raise
            log.info("waiting for database (%s)", exc)
            time.sleep(1)


if __name__ == "__main__":
    main()
