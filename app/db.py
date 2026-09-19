"""Database access: connection pool and race-safe migration runner."""

from __future__ import annotations

import os
import re
import threading
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/scheduler"
)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

# Serializes migration runs across API instances sharing one database.
_MIGRATION_LOCK_KEY = 727_272

pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def get_pool() -> ConnectionPool:
    global pool
    with _pool_lock:
        if pool is None:
            pool = ConnectionPool(
                DATABASE_URL,
                min_size=1,
                max_size=int(os.environ.get("DB_POOL_SIZE", "20")),
                kwargs={"row_factory": dict_row, "autocommit": False},
                open=False,
            )
            pool.open(wait=True)
        return pool


def close_pool() -> None:
    global pool
    with _pool_lock:
        if pool is not None:
            pool.close()
            pool = None


def run_migrations() -> None:
    """Apply pending migrations exactly once, safely under concurrent startup."""
    files = sorted(
        p for p in MIGRATIONS_DIR.glob("*.sql") if re.match(r"^\d+_.*\.sql$", p.name)
    )
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_MIGRATION_LOCK_KEY,))
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    name text PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
                )
                """
            )
            applied = {
                r[0] for r in conn.execute("SELECT name FROM schema_migrations").fetchall()
            }
            for path in files:
                if path.name in applied:
                    continue
                sql = path.read_text(encoding="utf-8")
                with conn.transaction():
                    conn.execute(sql)
                    conn.execute(
                        "INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,)
                    )
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_MIGRATION_LOCK_KEY,))
