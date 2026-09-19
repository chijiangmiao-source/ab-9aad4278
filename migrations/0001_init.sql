-- 0001_init.sql — initial schema for the pipeline coordination service.
-- All lease/deadline arithmetic is done with database time (clock_timestamp()).

CREATE TABLE IF NOT EXISTS pipelines (
    id              uuid PRIMARY KEY,
    draft_version   bigint NOT NULL DEFAULT 1,
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS draft_tasks (
    pipeline_id     uuid NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    task_id         text NOT NULL,
    PRIMARY KEY (pipeline_id, task_id)
);

CREATE TABLE IF NOT EXISTS draft_edges (
    pipeline_id     uuid NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    src             text NOT NULL,
    dst             text NOT NULL,
    PRIMARY KEY (pipeline_id, src, dst),
    FOREIGN KEY (pipeline_id, src) REFERENCES draft_tasks(pipeline_id, task_id) ON DELETE CASCADE,
    FOREIGN KEY (pipeline_id, dst) REFERENCES draft_tasks(pipeline_id, task_id) ON DELETE CASCADE
);

-- Idempotency ledger for draft mutations: one row per (pipeline, op_id).
CREATE TABLE IF NOT EXISTS draft_ops (
    pipeline_id     uuid NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    op_id           text NOT NULL,
    request_hash    text NOT NULL,          -- sha256 of the canonical request payload
    response_json   jsonb NOT NULL,         -- first response, replayed on same-params retry
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline_id, op_id)
);

CREATE TABLE IF NOT EXISTS seals (
    id              uuid PRIMARY KEY,
    pipeline_id     uuid NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    draft_version   bigint NOT NULL,
    idempotency_key text NOT NULL,
    topo_order      jsonb NOT NULL,         -- lexicographically-tie-broken topological order
    digest          text NOT NULL,          -- sha256 over canonical task/edge sets
    task_count      integer NOT NULL,
    edge_count      integer NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (pipeline_id, draft_version),    -- at most one seal per draft version
    UNIQUE (pipeline_id, idempotency_key)   -- idempotent replay per key
);

CREATE TABLE IF NOT EXISTS seal_tasks (
    seal_id         uuid NOT NULL REFERENCES seals(id) ON DELETE CASCADE,
    task_id         text NOT NULL,
    PRIMARY KEY (seal_id, task_id)
);

CREATE TABLE IF NOT EXISTS seal_edges (
    seal_id         uuid NOT NULL REFERENCES seals(id) ON DELETE CASCADE,
    src             text NOT NULL,
    dst             text NOT NULL,
    PRIMARY KEY (seal_id, src, dst)
);

CREATE TABLE IF NOT EXISTS runs (
    id               uuid PRIMARY KEY,
    pipeline_id      uuid NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    seal_id          uuid NOT NULL REFERENCES seals(id),
    idempotency_key  text NOT NULL,
    max_attempts     integer NOT NULL,
    lease_seconds    integer NOT NULL,
    status           text NOT NULL DEFAULT 'running',   -- running | succeeded | failed
    total_tasks      integer NOT NULL,
    succeeded_count  integer NOT NULL DEFAULT 0,
    failed_count     integer NOT NULL DEFAULT 0,
    blocked_count    integer NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at      timestamptz,
    UNIQUE (pipeline_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS run_tasks (
    run_id           uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_id          text NOT NULL,
    state            text NOT NULL,         -- waiting | ready | claimed | succeeded | failed | blocked
    remaining_preds  integer NOT NULL DEFAULT 0,
    attempts_used    integer NOT NULL DEFAULT 0,
    fencing_token    bigint NOT NULL DEFAULT 0,   -- strictly increasing, never reused
    attempt          integer,               -- current attempt while claimed
    claim_token      text,                  -- unforgeable, set only while claimed
    lease_expires_at timestamptz,           -- database-time deadline
    output_digest    text,
    PRIMARY KEY (run_id, task_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_tasks_claim_token ON run_tasks(claim_token) WHERE claim_token IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_run_tasks_state ON run_tasks(run_id, state);
CREATE INDEX IF NOT EXISTS idx_run_tasks_expired ON run_tasks(run_id, lease_expires_at) WHERE state = 'claimed';

CREATE TABLE IF NOT EXISTS run_edges (
    run_id           uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    src              text NOT NULL,
    dst              text NOT NULL,
    PRIMARY KEY (run_id, src, dst)
);
CREATE INDEX IF NOT EXISTS idx_run_edges_src ON run_edges(run_id, src);

-- Idempotency ledger for terminal claim outcomes (complete / fail).
CREATE TABLE IF NOT EXISTS claim_results (
    claim_token      text PRIMARY KEY,
    run_id           uuid NOT NULL,
    task_id          text NOT NULL,
    attempt          integer NOT NULL,
    kind             text NOT NULL,         -- complete | fail
    content_hash     text NOT NULL,         -- sha256 of canonical outcome payload
    response_json    jsonb NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);
