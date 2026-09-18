-- 001_init.sql — core schema for the cryo-EM pipeline coordination service.
-- All state lives here; the service keeps no correctness-critical state in memory.

BEGIN;

CREATE TABLE IF NOT EXISTS pipelines (
    id              TEXT PRIMARY KEY,
    name            TEXT,
    draft_version   BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency log for draft mutations. op_id is globally unique by contract.
CREATE TABLE IF NOT EXISTS draft_ops (
    op_id           TEXT PRIMARY KEY,
    pipeline_id     TEXT NOT NULL REFERENCES pipelines(id),
    request_hash    TEXT NOT NULL,          -- sha256 of canonical request body
    response_json   JSONB NOT NULL,         -- stored first response, replayed verbatim
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS draft_tasks (
    pipeline_id     TEXT NOT NULL REFERENCES pipelines(id),
    task_id         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, task_id)
);

CREATE TABLE IF NOT EXISTS draft_edges (
    pipeline_id     TEXT NOT NULL REFERENCES pipelines(id),
    src_task_id     TEXT NOT NULL,
    dst_task_id     TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, src_task_id, dst_task_id)
);
CREATE INDEX IF NOT EXISTS draft_edges_dst_idx ON draft_edges (pipeline_id, dst_task_id);

-- Sealed (immutable) versions. At most one seal per (pipeline, draft_version).
CREATE TABLE IF NOT EXISTS seals (
    id              TEXT PRIMARY KEY,
    pipeline_id     TEXT NOT NULL REFERENCES pipelines(id),
    draft_version   BIGINT NOT NULL,
    task_count      INTEGER NOT NULL,
    edge_count      INTEGER NOT NULL,
    topo_order      JSONB NOT NULL,         -- canonical lexicographic topo order
    digest_sha256   TEXT NOT NULL,          -- sha256 over canonical task/edge sets
    idempotency_key TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, draft_version)
);

-- Idempotency log for seal requests (successful seals only).
CREATE TABLE IF NOT EXISTS seal_requests (
    pipeline_id     TEXT NOT NULL REFERENCES pipelines(id),
    idempotency_key TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    response_json   JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS seal_tasks (
    seal_id         TEXT NOT NULL REFERENCES seals(id),
    task_id         TEXT NOT NULL,
    PRIMARY KEY (seal_id, task_id)
);

CREATE TABLE IF NOT EXISTS seal_edges (
    seal_id         TEXT NOT NULL REFERENCES seals(id),
    src_task_id     TEXT NOT NULL,
    dst_task_id     TEXT NOT NULL,
    PRIMARY KEY (seal_id, src_task_id, dst_task_id)
);
CREATE INDEX IF NOT EXISTS seal_edges_dst_idx ON seal_edges (seal_id, dst_task_id);

CREATE TABLE IF NOT EXISTS runs (
    id              TEXT PRIMARY KEY,
    pipeline_id     TEXT NOT NULL REFERENCES pipelines(id),
    seal_id         TEXT NOT NULL REFERENCES seals(id),
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running','succeeded','failed')),
    max_attempts    INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    lease_seconds   INTEGER NOT NULL CHECK (lease_seconds BETWEEN 2 AND 30),
    idempotency_key TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS runs_pipeline_idx ON runs (pipeline_id);

-- Idempotency log for run creation (successful creations only).
CREATE TABLE IF NOT EXISTS run_requests (
    pipeline_id     TEXT NOT NULL REFERENCES pipelines(id),
    idempotency_key TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    response_json   JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, idempotency_key)
);

-- Per-run task state. fencing_token is strictly increasing per task and never reused.
CREATE TABLE IF NOT EXISTS run_tasks (
    run_id          TEXT NOT NULL REFERENCES runs(id),
    task_id         TEXT NOT NULL,
    status          TEXT NOT NULL
                    CHECK (status IN ('pending','ready','leased','succeeded','failed','blocked')),
    pending_preds   INTEGER NOT NULL DEFAULT 0,   -- direct predecessors not yet succeeded
    attempts        INTEGER NOT NULL DEFAULT 0,   -- attempts granted so far
    fencing_token   BIGINT NOT NULL DEFAULT 0,    -- last fencing token issued
    output_digest   TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, task_id)
);
CREATE INDEX IF NOT EXISTS run_tasks_ready_idx ON run_tasks (run_id, status, task_id);

-- Leases / claims. claim_token is stored only as a sha256 hash.
CREATE TABLE IF NOT EXISTS leases (
    run_id              TEXT NOT NULL,
    task_id             TEXT NOT NULL,
    attempt             INTEGER NOT NULL,
    fencing_token       BIGINT NOT NULL,
    claim_token_hash    TEXT NOT NULL UNIQUE,
    state               TEXT NOT NULL DEFAULT 'active'
                        CHECK (state IN ('active','completed','failed','expired')),
    lease_expires_at    TIMESTAMPTZ NOT NULL,
    completed_digest    TEXT,
    fail_reason         TEXT,
    result_json         JSONB,              -- stored first response for claim-scoped replay
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, task_id, attempt),
    FOREIGN KEY (run_id, task_id) REFERENCES run_tasks(run_id, task_id)
);
CREATE INDEX IF NOT EXISTS leases_expiry_idx ON leases (run_id, state, lease_expires_at);

COMMIT;
