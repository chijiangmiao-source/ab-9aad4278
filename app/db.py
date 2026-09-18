"""Database access: connection pool, migration runner, small helpers.

All time-based decisions (lease expiry, deadlines) are made with database
time (`now()` inside SQL), never with application-server clocks.
"""

from __future__ import annotations

import os
import re
import threading
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/coordinator",
)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def get_pool() -> ConnectionPool:
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ConnectionPool(
                DATABASE_URL,
                min_size=1,
                max_size=int(os.environ.get("DB_POOL_SIZE", "16")),
                kwargs={"row_factory": dict_row, "autocommit": False},
                open=True,
            )
        return _pool


def close_pool() -> None:
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.close()
            _pool = None


_MIGRATION_FILE_RE = re.compile(r"^(\d+)_.*\.sql$")


def run_migrations(db_url: str | None = None) -> list[int]:
    """Apply pending migrations in filename order. Idempotent.

    Uses a PostgreSQL advisory lock so concurrent starters (two API
    instances booting at once) serialize safely.
    """
    url = db_url or DATABASE_URL
    applied: list[int] = []
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(72727272)")
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    INTEGER PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
            done = {r[0] for r in rows}
            files = sorted(MIGRATIONS_DIR.glob("*.sql"))
            for path in files:
                m = _MIGRATION_FILE_RE.match(path.name)
                if not m:
                    continue
                version = int(m.group(1))
                if version in done:
                    continue
                sql = path.read_text(encoding="utf-8")
                with conn.transaction():
                    conn.execute(sql)
                    conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)",
                        (version,),
                    )
                applied.append(version)
        finally:
            conn.execute("SELECT pg_advisory_unlock(72727272)")
    return applied
