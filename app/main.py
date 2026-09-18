"""HTTP API surface (FastAPI).

Routes:
    GET  /healthz
    POST /pipelines
    GET  /pipelines/{pipeline_id}
    GET  /pipelines/{pipeline_id}/draft/tasks
    GET  /pipelines/{pipeline_id}/draft/edges
    POST /pipelines/{pipeline_id}/draft/mutations
    POST /pipelines/{pipeline_id}/seals
    GET  /pipelines/{pipeline_id}/seals/{seal_id}
    POST /runs
    GET  /runs/{run_id}
    GET  /runs/{run_id}/tasks
    GET  /runs/{run_id}/tasks/{task_id}
    POST /runs/{run_id}/claims
    POST /runs/{run_id}/claims/renew
    POST /runs/{run_id}/claims/complete
    POST /runs/{run_id}/claims/fail
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import draft_service, run_service, seal_service
from .db import close_pool, get_pool, run_migrations
from .errors import ApiError
from .schemas import (ClaimActionRequest, CompleteRequest, CreatePipelineRequest,
                      CreateRunRequest, FailRequest, MutateDraftRequest,
                      SealRequest)
from .sweeper import sweeper_loop

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Idempotent, advisory-locked; safe when both instances start together.
    await asyncio.to_thread(run_migrations)
    get_pool()
    stop = asyncio.Event()
    sweeper = asyncio.create_task(sweeper_loop(stop))
    yield
    stop.set()
    sweeper.cancel()
    close_pool()


app = FastAPI(title="cryoem-pipeline-coordinator", lifespan=lifespan)


@app.exception_handler(ApiError)
async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content=exc.body())


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request,
                                   exc: RequestValidationError) -> JSONResponse:
    safe = [{"type": e.get("type"), "loc": list(e.get("loc", ())),
             "msg": e.get("msg")} for e in exc.errors()]
    return JSONResponse(status_code=400, content={
        "error": {"code": "INVALID_REQUEST",
                  "message": "request validation failed",
                  "details": {"errors": safe}}})


@app.exception_handler(Exception)
async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={
        "error": {"code": "INTERNAL", "message": "internal error",
                  "details": {}}})


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------
@app.get("/healthz")
def healthz():
    pool = get_pool()
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# pipelines & drafts
# ---------------------------------------------------------------------------
@app.post("/pipelines", status_code=201)
def create_pipeline(req: CreatePipelineRequest):
    with get_pool().connection() as conn:
        return draft_service.create_pipeline(conn, req.name)


@app.get("/pipelines/{pipeline_id}")
def get_pipeline(pipeline_id: str):
    with get_pool().connection() as conn:
        return draft_service.get_pipeline(conn, pipeline_id)


@app.get("/pipelines/{pipeline_id}/draft/tasks")
def get_draft_tasks(pipeline_id: str,
                    offset: int = Query(0, ge=0),
                    limit: int = Query(1000, ge=1, le=10000)):
    with get_pool().connection() as conn:
        return draft_service.get_draft_tasks(conn, pipeline_id, offset, limit)


@app.get("/pipelines/{pipeline_id}/draft/edges")
def get_draft_edges(pipeline_id: str,
                    offset: int = Query(0, ge=0),
                    limit: int = Query(1000, ge=1, le=10000)):
    with get_pool().connection() as conn:
        return draft_service.get_draft_edges(conn, pipeline_id, offset, limit)


@app.post("/pipelines/{pipeline_id}/draft/mutations")
def mutate_draft(pipeline_id: str, req: MutateDraftRequest):
    operations = [op.model_dump() for op in req.operations]
    with get_pool().connection() as conn:
        return draft_service.mutate_draft(
            conn, pipeline_id, req.op_id, req.draft_version, operations)


# ---------------------------------------------------------------------------
# seals
# ---------------------------------------------------------------------------
@app.post("/pipelines/{pipeline_id}/seals", status_code=201)
def seal_pipeline(pipeline_id: str, req: SealRequest):
    with get_pool().connection() as conn:
        return seal_service.seal_pipeline(
            conn, pipeline_id, req.expected_draft_version, req.idempotency_key)


@app.get("/pipelines/{pipeline_id}/seals/{seal_id}")
def get_seal(pipeline_id: str, seal_id: str):
    with get_pool().connection() as conn:
        return seal_service.get_seal(conn, pipeline_id, seal_id)


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------
@app.post("/runs", status_code=201)
def create_run(req: CreateRunRequest):
    with get_pool().connection() as conn:
        return run_service.create_run(
            conn, req.pipeline_id, req.seal_id, req.idempotency_key,
            req.max_attempts, req.lease_seconds)


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    with get_pool().connection() as conn:
        return run_service.get_run(conn, run_id)


@app.get("/runs/{run_id}/tasks")
def list_run_tasks(run_id: str,
                   status: Optional[str] = Query(None),
                   offset: int = Query(0, ge=0),
                   limit: int = Query(1000, ge=1, le=10000)):
    with get_pool().connection() as conn:
        return run_service.list_run_tasks(conn, run_id, status, offset, limit)


@app.get("/runs/{run_id}/tasks/{task_id}")
def get_run_task(run_id: str, task_id: str):
    with get_pool().connection() as conn:
        return run_service.get_run_task(conn, run_id, task_id)


@app.post("/runs/{run_id}/claims")
def claim(run_id: str):
    with get_pool().connection() as conn:
        return run_service.claim(conn, run_id)


@app.post("/runs/{run_id}/claims/renew")
def renew(run_id: str, req: ClaimActionRequest):
    with get_pool().connection() as conn:
        return run_service.renew(conn, run_id, req.claim_token, req.attempt,
                                 req.fencing_token)


@app.post("/runs/{run_id}/claims/complete")
def complete(run_id: str, req: CompleteRequest):
    with get_pool().connection() as conn:
        return run_service.complete(conn, run_id, req.claim_token, req.attempt,
                                    req.fencing_token, req.output_digest)


@app.post("/runs/{run_id}/claims/fail")
def fail(run_id: str, req: FailRequest):
    with get_pool().connection() as conn:
        return run_service.fail(conn, run_id, req.claim_token, req.attempt,
                                req.fencing_token, req.reason)
