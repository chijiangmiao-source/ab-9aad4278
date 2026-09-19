# Pipeline Coordinator

A pure-backend coordination service for dependency-graph (DAG) task
pipelines — the kind of reconstruction pipeline a cryo-EM center splits into
denoising, motion-correction, particle-picking, classification and
refinement stages. It provides:

* **Revisable pipeline drafts** with optimistic versioning and idempotent
  mutations (`op_id`).
* **Sealing**: an atomic, immutable, content-addressed snapshot of one draft
  version, with cycle detection (deterministic witness), a unique
  lexicographic topological order, and a SHA-256 digest over the canonical
  task/edge sets.
* **Runs** created only from sealed versions, with fixed `max_attempts` and
  lease duration.
* **Work claiming** with exactly-once grant per `(task, attempt)`, strictly
  increasing fencing tokens, unforgeable claim tokens, and database-time
  leases — safe across multiple API instances and workers.
* **Atomic lifecycle transitions**: successor release, failure propagation
  (`blocked`), and run terminal states commit in the same transaction as the
  task outcome. All state lives in PostgreSQL; a restart simply continues.

## Quick start (Docker only)

```bash
docker compose up --build -d        # db + api-a + api-b + gateway (nginx)
docker compose up verify            # one-shot acceptance suite -> exits 0 on success
```

The unified API entry point is `http://localhost:${API_PORT}` (default
`API_PORT=8080`), load-balanced across the two independent API instances.

```bash
API_PORT=9000 docker compose up --build -d   # choose the host port
```

Unit tests (pure graph algorithms, no database needed):

```bash
docker compose run --rm --entrypoint pytest api-a tests/ -q
# or locally: pip install -r requirements.txt && pytest tests/ -q
```

Everything is built, run, tested and accepted with Docker alone; no online
service is contacted at runtime.

## Repository layout

```
app/
  main.py           FastAPI routes (one DB transaction per mutating handler)
  service_draft.py  drafts + idempotent, version-checked mutations
  service_seal.py   sealing (atomic vs. concurrent mutations)
  service_run.py    runs, claims, heartbeats, complete/fail, sweeping
  graph.py          pure topo-sort / cycle-witness / digest algorithms
  db.py             connection pool + race-safe migration runner
  errors.py         stable machine-readable error codes
migrations/0001_init.sql
verify/verify.py    end-to-end acceptance suite (real HTTP + DB reads)
tests/test_graph.py unit tests
gateway/nginx.conf  unified entry -> api-a/api-b
docker-compose.yml  db, api-a, api-b, gateway, verify
```

## API overview

All errors are `{"error": {"code", "message", "details"}}` with a stable
machine-readable `code`.

| Method & path | Purpose |
| --- | --- |
| `POST /pipelines` | create pipeline; returns revisable draft at `draft_version=1` |
| `GET /pipelines/{pid}` | pipeline summary (version, counts, seals) |
| `GET /pipelines/{pid}/draft` | full draft (tasks, edges, version) |
| `POST /pipelines/{pid}/draft/mutations` | atomic batch of add/remove task/edge ops |
| `POST /pipelines/{pid}/seals` | seal `expected_draft_version` (idempotency key) |
| `GET /pipelines/{pid}/seals` · `GET /pipelines/{pid}/seals/{sid}` | seal queries |
| `POST /pipelines/{pid}/runs` | create run from a seal (idempotency key, `max_attempts` 1–10, `lease_seconds` 2–30) |
| `GET /runs/{rid}` | run status + counters |
| `GET /runs/{rid}/tasks?state=&limit=&offset=` · `GET /runs/{rid}/tasks/{tid}` | task queries |
| `POST /runs/{rid}/claims` | claim a ready task (optionally `{"task_id": ...}`) |
| `POST /runs/{rid}/tasks/{tid}/heartbeat` | extend the lease |
| `POST /runs/{rid}/tasks/{tid}/complete` | report success with `output_digest` |
| `POST /runs/{rid}/tasks/{tid}/fail` | report failure |
| `GET /health` | health check |

### Draft mutations

```json
POST /pipelines/{pid}/draft/mutations
{
  "op_id": "client-unique-op-id",
  "draft_version": 3,
  "ops": [
    {"type": "add_task", "task_id": "denoise"},
    {"type": "add_edge", "src": "denoise", "dst": "pick"}
  ]
}
```

* The batch is validated and applied atomically; `draft_version` is
  incremented by one and returned.
* **Idempotency**: `(pipeline, op_id)` is recorded with a hash of the full
  request. An identical retry returns the stored first response; reusing the
  same `op_id` with different parameters yields `409 OP_CONFLICT`.
* **Versioning**: a stale `draft_version` yields `409 STALE_VERSION` with the
  current version in `details.current_draft_version`; nothing is written.
* Task IDs must match `[A-Za-z0-9][A-Za-z0-9_-]{0,63}` and are unique per
  pipeline. Self-loops and duplicate edges are rejected. Removing a task
  removes its incident edges.

### Sealing

```json
POST /pipelines/{pid}/seals
{"expected_draft_version": 4, "idempotency_key": "release-1"}
```

Sealing and draft mutations serialize on the same database row lock, so a
seal and any concurrent mutation have a definite atomic order: a successful
seal captures exactly one draft version forever, and any mutation not
included in it lands in a later draft version. At most one seal exists per
draft version (`SEAL_CONFLICT` for a second key on the same version), even
when two API instances race.

A successful seal returns:

* `topo_order` — the unique topological order with lexicographic
  tie-breaking (Kahn's algorithm with a min-heap on task IDs);
* `digest` — SHA-256 over the canonical graph encoding, independent of
  submission order:

  ```
  for each task id t in sorted order:        "T:" t "\n"
  for each edge (s, d) in sorted order:      "E:" s "->" d "\n"
  digest = sha256(concatenation).hexdigest()
  ```

If the graph contains a cycle, no seal is created and the response is
`409 CYCLE_DETECTED` with a deterministic witness in `details.cycle`: a
depth-first traversal visits start nodes in ascending task-ID order and
expands successors in ascending order; the witness is the first edge
`(u, v)` with `v` on the current recursion stack, returned as the stack path
`v … u` closed by `v`. Cycle detection is a single O(V+E) pass — no path
enumeration, no per-edge rescans — and handles 50k tasks / 200k edges.

### Runs and claiming

```json
POST /pipelines/{pid}/runs
{"sealed_version_id": "...", "idempotency_key": "run-1",
 "max_attempts": 3, "lease_seconds": 10}
```

`max_attempts` and `lease_seconds` are frozen into the run. A run of a
zero-task seal is `succeeded` immediately.

```json
POST /runs/{rid}/claims            ->  201
{"run_id": "...", "task_id": "denoise", "attempt": 1, "fencing_token": 1,
 "claim_token": "9f2c...", "lease_expires_at": "2026-01-01T00:00:10+00:00",
 "lease_seconds": 10}
```

* A task is claimable only when all its direct predecessors have succeeded.
  The grant atomically increments `attempts_used` and the per-task
  `fencing_token` (strictly increasing, never reused) under a row lock, so
  one `(task, attempt)` is granted exactly once across all API instances and
  workers. Without a `task_id`, the lexicographically smallest ready task is
  granted (`SKIP LOCKED` queue-pop).
* `claim_token` is a 256-bit random bearer capability required for
  heartbeat/complete/fail. It is never exposed by query endpoints.
* All lease arithmetic and expiry checks use database time
  (`clock_timestamp()`).

Heartbeat/complete/fail all present `{attempt, fencing_token, claim_token}`
and are validated against the persisted grant. Any of: expired lease
(`LEASE_EXPIRED`), attempt/token mismatch (`CLAIM_INVALID`), or a lagging
fencing token (`FENCING_STALE`) is rejected **without any state change** —
including when an old worker resurfaces after its lease expired and the task
was re-claimed.

`complete` requires `output_digest` matching `^[0-9a-f]{64}$`. Terminal
outcomes are recorded idempotently: a retry of the same claim with the same
content returns the first response; the same claim with different content
yields `409 CONTENT_CONFLICT`. This also covers the crash window between
"state committed" and "response sent".

### Failure, retries, and run terminal states

* On a reported failure or an expired lease, a task with attempts remaining
  becomes `ready` again (its next claim carries a larger fencing token);
  at `max_attempts` it becomes permanently `failed`.
* When a task fails permanently, **all transitive successors** become
  `blocked` (one recursive set-based statement); independent branches keep
  running.
* A run is `succeeded` exactly when every task succeeded; it is `failed`
  when a permanent failure exists and no task remains runnable. Terminal
  states never regress.
* Successor release, failure propagation, counters, and the run terminal
  transition all commit in the **same transaction** as the task outcome, so
  observers never see "successor claimed while predecessor not succeeded"
  or terminal-state rollback.

### Lease sweeping

Correctness never depends on a cleaner: every `claim` call first performs a
lazy, in-transaction sweep of expired leases for that run. A best-effort
background sweeper in each API instance (`SWEEP_INTERVAL_SECONDS`, default
1s) additionally requeues expired leases so they become claimable promptly;
it is idempotent and safe to run any number of times (or never).

## Error codes

| Code | HTTP | Meaning |
| --- | --- | --- |
| `INVALID_PARAMS` | 400 | schema/range validation failure |
| `INVALID_TASK_ID` | 400 | task ID violates the required pattern |
| `INVALID_OP` | 400 | malformed or ambiguous mutation batch |
| `SELF_LOOP` | 400 | edge from a task to itself |
| `INVALID_DIGEST` | 400 | output digest is not lowercase 64-hex |
| `PIPELINE_NOT_FOUND` / `SEAL_NOT_FOUND` / `RUN_NOT_FOUND` / `TASK_NOT_FOUND` | 404 | missing resource |
| `TASK_EXISTS` / `EDGE_EXISTS` | 409 | duplicate task / edge |
| `EDGE_NOT_FOUND` | 409 | removing a non-existent edge |
| `STALE_VERSION` | 409 | draft version is stale; current version in details |
| `OP_CONFLICT` | 409 | `op_id` / idempotency key reused with different parameters |
| `SEAL_CONFLICT` | 409 | draft version already sealed under another key |
| `CYCLE_DETECTED` | 409 | graph has a cycle; deterministic witness in details |
| `RUN_NOT_RUNNING` | 409 | run is already terminal |
| `NO_READY_TASK` | 409 | nothing claimable right now |
| `TASK_NOT_READY` | 409 | the named task is not claimable (state in details) |
| `CLAIM_INVALID` | 409 | attempt / claim_token mismatch, or task not claimed |
| `FENCING_STALE` | 409 | fencing token behind the current grant |
| `LEASE_EXPIRED` | 409 | lease deadline (database time) has passed |
| `CONTENT_CONFLICT` | 409 | same claim completed with different content |
| `INTERNAL` | 500 | unexpected error |

## Concurrency design notes

* **Drafts/seals/runs** serialize per pipeline on the pipeline row lock;
  idempotency ledgers (`draft_ops`, `seals.idempotency_key`,
  `runs.idempotency_key`, `claim_results`) make every mutating endpoint
  safe to retry after a crash anywhere between commit and response.
* **Claims** use `SELECT ... FOR UPDATE SKIP LOCKED` followed by an update
  by primary key (a one-statement `UPDATE ... FROM (SELECT ... FOR UPDATE
  SKIP LOCKED)` is deliberately *not* used: inlined into the outer query it
  can update multiple rows under EvalPlanQual re-checks).
* **Outcome transactions** (`complete`, `fail`, and the exhaustion branch of
  the sweep) take a per-run advisory transaction lock before any row locks,
  giving a global lock order (advisory → rows) that keeps multi-row updates
  (successor release, blocked propagation) deadlock-free without
  serializing claims or heartbeats.
* Run counters (`succeeded/failed/blocked_count`) are maintained in the
  same transactions as task transitions, so terminal-state checks are O(1)
  even at 50k tasks.

## Scale

Verified locally against the maximum shape: 50,000 tasks and 199,990 edges
are drafted in ~4s (batched mutations), sealed in ~1.5s (single-pass cycle
check + heap-based topo sort + streaming digest), and instantiated as a run
in ~1.2s (set-based `INSERT ... SELECT`); claim/complete stay
constant-time. The `verify` service runs a 2,000-task chain smoke test by
default (`VERIFY_SCALE` env var).
