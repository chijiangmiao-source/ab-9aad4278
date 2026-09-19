"""HTTP API surface. All handlers are synchronous (psycopg) and run in the
FastAPI threadpool; every mutating handler is a single database transaction."""

from __future__ import annotations

import os
import threading
import time
import uuid

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import errors, service_draft, service_run, service_seal
from .db import close_pool, get_pool, run_migrations

app = FastAPI(title="pipeline-coordinator")
app.add_exception_handler(errors.ApiError, errors.api_error_handler)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content=errors.error_body(
            "INVALID_PARAMS", "request failed schema validation",
            {"issues": exc.errors(include_url=False)},
        ),
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content=errors.error_body("INTERNAL", "unexpected internal error"),
    )


# --------------------------------------------------------------------------
# models
# --------------------------------------------------------------------------

class MutationOp(BaseModel):
    type: str
    task_id: str | None = None
    src: str | None = None
    dst: str | None = None


class MutationsRequest(BaseModel):
    op_id: str
    draft_version: int
    ops: list[MutationOp]


class SealRequest(BaseModel):
    expected_draft_version: int
    idempotency_key: str


class RunCreateRequest(BaseModel):
    sealed_version_id: str
    idempotency_key: str
    max_attempts: int
    lease_seconds: int


class ClaimRequest(BaseModel):
    task_id: str | None = None


class ClaimActionRequest(BaseModel):
    attempt: int
    fencing_token: int
    claim_token: str


class CompleteRequest(ClaimActionRequest):
    output_digest: str


class FailRequest(ClaimActionRequest):
    error: str | None = None


# --------------------------------------------------------------------------
# lifecycle
# --------------------------------------------------------------------------

def _sweeper_loop() -> None:
    interval = float(os.environ.get("SWEEP_INTERVAL_SECONDS", "1"))
    while True:
        try:
            with get_pool().connection() as conn:
                service_run.sweep_expired(conn)
        except Exception:
            pass  # best-effort only; the lazy sweep in claim() is the floor
        time.sleep(interval)


@app.on_event("startup")
def startup() -> None:
    run_migrations()
    get_pool()
    threading.Thread(target=_sweeper_loop, daemon=True).start()


@app.on_event("shutdown")
def shutdown() -> None:
    close_pool()


@app.get("/health")
def health() -> dict:
    with get_pool().connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


# --------------------------------------------------------------------------
# pipelines & drafts
# --------------------------------------------------------------------------

@app.post("/pipelines", status_code=201)
def create_pipeline() -> dict:
    with get_pool().connection() as conn, conn.transaction():
        return service_draft.create_pipeline(conn)


@app.get("/pipelines/{pipeline_id}")
def get_pipeline(pipeline_id: uuid.UUID) -> dict:
    with get_pool().connection() as conn:
        return service_draft.get_pipeline(conn, pipeline_id)


@app.get("/pipelines/{pipeline_id}/draft")
def get_draft(pipeline_id: uuid.UUID) -> dict:
    with get_pool().connection() as conn:
        return service_draft.get_draft(conn, pipeline_id)


@app.post("/pipelines/{pipeline_id}/draft/mutations")
def mutate_draft(pipeline_id: uuid.UUID, body: MutationsRequest) -> dict:
    ops = [op.model_dump(exclude_none=True) for op in body.ops]
    with get_pool().connection() as conn:
        return service_draft.apply_mutations(
            conn, pipeline_id, body.op_id, body.draft_version, ops
        )


# --------------------------------------------------------------------------
# seals
# --------------------------------------------------------------------------

@app.post("/pipelines/{pipeline_id}/seals", status_code=201)
def create_seal(pipeline_id: uuid.UUID, body: SealRequest) -> dict:
    with get_pool().connection() as conn:
        return service_seal.create_seal(
            conn, pipeline_id, body.expected_draft_version, body.idempotency_key
        )


@app.get("/pipelines/{pipeline_id}/seals")
def list_seals(pipeline_id: uuid.UUID) -> dict:
    with get_pool().connection() as conn:
        return {"seals": service_seal.list_seals(conn, pipeline_id)}


@app.get("/pipelines/{pipeline_id}/seals/{seal_id}")
def get_seal(pipeline_id: uuid.UUID, seal_id: uuid.UUID) -> dict:
    with get_pool().connection() as conn:
        return service_seal.get_seal(conn, pipeline_id, seal_id)


# --------------------------------------------------------------------------
# runs
# --------------------------------------------------------------------------

@app.post("/pipelines/{pipeline_id}/runs", status_code=201)
def create_run(pipeline_id: uuid.UUID, body: RunCreateRequest) -> dict:
    with get_pool().connection() as conn:
        return service_run.create_run(
            conn, pipeline_id, body.sealed_version_id, body.idempotency_key,
            body.max_attempts, body.lease_seconds,
        )


@app.get("/runs/{run_id}")
def get_run(run_id: uuid.UUID) -> dict:
    with get_pool().connection() as conn:
        return service_run.get_run(conn, run_id)


@app.get("/runs/{run_id}/tasks")
def list_run_tasks(
    run_id: uuid.UUID,
    state: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    with get_pool().connection() as conn:
        return service_run.list_run_tasks(conn, run_id, state, limit, offset)


@app.get("/runs/{run_id}/tasks/{task_id}")
def get_run_task(run_id: uuid.UUID, task_id: str) -> dict:
    with get_pool().connection() as conn:
        return service_run.get_run_task(conn, run_id, task_id)


@app.post("/runs/{run_id}/claims", status_code=201)
def claim(run_id: uuid.UUID, body: ClaimRequest | None = None) -> dict:
    task_id = body.task_id if body else None
    with get_pool().connection() as conn:
        return service_run.claim(conn, run_id, task_id)


@app.post("/runs/{run_id}/tasks/{task_id}/heartbeat")
def heartbeat(run_id: uuid.UUID, task_id: str, body: ClaimActionRequest) -> dict:
    with get_pool().connection() as conn:
        return service_run.heartbeat(
            conn, run_id, task_id, body.attempt, body.fencing_token, body.claim_token
        )


@app.post("/runs/{run_id}/tasks/{task_id}/complete")
def complete(run_id: uuid.UUID, task_id: str, body: CompleteRequest) -> dict:
    with get_pool().connection() as conn:
        return service_run.complete(
            conn, run_id, task_id, body.attempt, body.fencing_token,
            body.claim_token, body.output_digest,
        )


@app.post("/runs/{run_id}/tasks/{task_id}/fail")
def fail(run_id: uuid.UUID, task_id: str, body: FailRequest) -> dict:
    with get_pool().connection() as conn:
        return service_run.fail(
            conn, run_id, task_id, body.attempt, body.fencing_token,
            body.claim_token, body.error,
        )
