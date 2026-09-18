# Cryo-EM Pipeline Coordinator

Pure-backend coordination service for cryo-EM reconstruction pipelines
(denoise → motion correction → particle extraction → classification →
refinement …). It manages **draft revisions** of a task DAG, **seals**
immutable, content-addressed versions of a draft, and executes **runs**
with leased, fenced task claims. There is no frontend.

## Guarantees

* **One run uses exactly one sealed DAG.** Runs are created only from
  sealed versions; sealed tasks/edges/digest never change.
* **A task is claimable only after all direct predecessors succeeded.**
  Successor release happens in the same transaction as the predecessor's
  success, so the invariant is atomically observable.
* **Stale executors cannot overwrite newer retries.** Every claim carries
  a strictly increasing, never-reused `fencing_token`; renew/complete/fail
  validate token + attempt + fencing + lease deadline (database time) and
  reject stale or expired requests without any state change.
* **Idempotency everywhere retries can happen.** Draft mutations
  (`op_id`), seals and run creations (idempotency keys), and
  completions/failures (claim tokens) all replay their first result;
  reusing a key with different parameters is a conflict.
* **No in-process correctness state.** All state lives in PostgreSQL; all
  time judgements use database time. Any API instance can die at any
  point; retries converge. The background sweeper only *prompts* expired
  leases back — claim and status endpoints reap expired leases lazily in
  their own transactions, so correctness never depends on the sweeper
  running (exactly once or at all).

## Layout

```
app/            FastAPI service (routes, draft/seal/run services, sweeper)
app/graph.py    deterministic cycle witness, lexicographic topo, digest
migrations/     SQL migrations (applied idempotently, advisory-locked)
verify/         one-shot acceptance service (real HTTP + DB checks)
tests/          pytest: unit tests + optional API integration tests
Dockerfile      single image for api / migrate / verify
docker-compose.yml  db + migrate + api1 + api2 + gateway(nginx) + verify
nginx.conf      unified entry load-balancing the two API instances
```

## Run

```bash
docker compose up --build -d          # db, migrate, api1, api2, gateway
docker compose up verify              # one-shot acceptance (exits non-zero on failure)
docker compose logs verify
```

The unified API entry is exposed on the host port from `API_PORT`
(default `8000`): `http://localhost:${API_PORT}`.

Health: `GET /healthz` on each API instance; compose healthchecks gate the
gateway and the verifier.

Unit tests (no DB needed) plus optional API integration tests:

```bash
docker compose run --rm --no-deps --entrypoint pytest api1 tests/test_graph.py
API_BASE=http://localhost:${API_PORT:-8000} pytest tests/   # against a running stack
```

## Data model & concurrency design

* `pipelines(draft_version)` — the pipeline row is locked
  (`SELECT ... FOR UPDATE`) by every draft mutation and every seal, so
  revisions and seals form a definite atomic order per pipeline.
* `draft_ops(op_id PK)` — global op-idempotency log storing the request
  fingerprint and the first response. Replay is checked **before** the
  version check, so a retried mutation returns its first result even
  after the version moved on. Failed (version-conflict) requests are not
  recorded — they never happened.
* `seals` — `UNIQUE(pipeline_id, draft_version)` guarantees a single seal
  per draft version even across concurrent instances. `seal_tasks` /
  `seal_edges` snapshot the draft inside the same transaction.
* `runs`, `run_tasks(status, pending_preds, attempts, fencing_token)` —
  per-run task state. `pending_preds` is decremented transactionally when
  a predecessor succeeds; hitting 0 flips `pending → ready`.
* `leases(run_id, task_id, attempt, fencing_token, claim_token_hash,
  state, lease_expires_at, result_json)` — one row per granted attempt;
  `claim_token` is 256-bit random, stored only as SHA-256.
* Every run-scoped mutation first takes a per-run
  `pg_advisory_xact_lock`, giving one deadlock-free lock order and making
  terminal-state checks observe all committed task transitions.
* Run terminal transitions are guarded (`WHERE status='running' …`), so a
  terminal state never regresses. A run is `succeeded` iff every task
  succeeded; `failed` iff a permanent failure exists and no task is
  runnable (`pending`/`ready`/`leased`). A zero-task run is `succeeded`
  at creation.
* Permanent failure (active fail or expired lease at `max_attempts`)
  marks all transitive successors `blocked` via a recursive CTE in the
  same transaction; independent branches keep running.

## Sealing specifics

* **Cycle check** — deterministic iterative DFS: start nodes ascending by
  task id, adjacency ascending; the witness is the path from the first
  on-stack target to the current node, closed by repeating the target
  (`[v, …, u, v]`). O(V+E), no path enumeration, no per-edge rescans.
* **Topological order** — Kahn with a min-heap on task id: the unique
  lexicographically smallest topological order.
* **Digest** — SHA-256 over the canonical serialization (task ids cannot
  contain spaces, so the format is unambiguous):

  ```
  cryoem-pipeline-digest/v1
  tasks:<N>
  T <task_id>            (N lines, ascending)
  edges:<M>
  E <src> <dst>          (M lines, lexicographic by (src, dst))
  ```

  Identical task/edge sets always produce the same digest regardless of
  submission order.

## HTTP API

All errors are `{"error": {"code", "message", "details"}}` with stable
machine-readable codes.

| Method & path | Purpose |
| --- | --- |
| `POST /pipelines` | create pipeline (revisable draft, `draft_version=0`) |
| `GET /pipelines/{pid}` | draft version, counts, seals |
| `GET /pipelines/{pid}/draft/tasks?offset&limit` | draft tasks (paginated) |
| `GET /pipelines/{pid}/draft/edges?offset&limit` | draft edges (paginated) |
| `POST /pipelines/{pid}/draft/mutations` | atomic batch of `add_task`/`remove_task`/`add_edge`/`remove_edge` with `op_id` + `draft_version` |
| `POST /pipelines/{pid}/seals` | seal `expected_draft_version` (idempotency key) → topo order + digest, or `CYCLE_DETECTED` witness |
| `GET /pipelines/{pid}/seals/{sid}` | sealed content |
| `POST /runs` | create run from a seal (`max_attempts` 1–10, `lease_seconds` 2–30, idempotency key) |
| `GET /runs/{rid}` | run status + per-state task counts |
| `GET /runs/{rid}/tasks?status&offset&limit` | task states |
| `GET /runs/{rid}/tasks/{tid}` | single task state |
| `POST /runs/{rid}/claims` | claim one ready task → `task_id`, `attempt` (from 1), `fencing_token`, `lease_expires_at`, `claim_token` |
| `POST /runs/{rid}/claims/renew` | extend the lease (`claim_token`, `attempt`, `fencing_token`) |
| `POST /runs/{rid}/claims/complete` | submit lowercase 64-hex SHA-256 output digest |
| `POST /runs/{rid}/claims/fail` | report failure (requeue or permanent at `max_attempts`) |

### Error codes

`INVALID_REQUEST`, `PIPELINE_NOT_FOUND`, `SEAL_NOT_FOUND`, `RUN_NOT_FOUND`,
`TASK_NOT_FOUND`, `OP_CONFLICT`, `VERSION_CONFLICT`,
`SEAL_VERSION_CONFLICT`, `IDEMPOTENCY_CONFLICT`, `SEAL_ALREADY_EXISTS`,
`CYCLE_DETECTED`, `TASK_EXISTS`, `EDGE_EXISTS`, `EDGE_NOT_FOUND`,
`SELF_LOOP`, `EDGE_ENDPOINT_MISSING`, `LIMIT_EXCEEDED`, `CLAIM_NOT_FOUND`,
`CLAIM_EXPIRED`, `CLAIM_STALE`, `CLAIM_NOT_ACTIVE`, `DIGEST_CONFLICT`,
`RUN_NOT_RUNNING`, `INTERNAL`.

## Limits

* 1–50 000 tasks and 0–200 000 directed edges per pipeline.
* Task id: `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`, unique per pipeline; no
  self-loops, no duplicate edges.
* `max_attempts` 1–10, `lease_seconds` 2–30, fixed at run creation.
* Up to 1000 operations per mutation request.

## Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `API_PORT` | `8000` | host port of the unified gateway entry |
| `DATABASE_URL` | `postgresql://postgres:postgres@db:5432/coordinator` | service DSN |
| `DB_POOL_SIZE` | `16` | per-instance connection pool size |
