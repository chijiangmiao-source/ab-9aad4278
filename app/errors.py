"""Stable machine-readable error codes and the API error envelope."""

from fastapi import Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    """Domain error rendered as {"error": {"code", "message", "details"}}."""

    def __init__(self, status: int, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


def error_body(code: str, message: str, details: dict | None = None) -> dict:
    return {"error": {"code": code, "message": message, "details": details or {}}}


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status,
        content=error_body(exc.code, exc.message, exc.details),
    )


# --- 400 validation -------------------------------------------------------

def invalid_params(message: str, details: dict | None = None) -> ApiError:
    return ApiError(400, "INVALID_PARAMS", message, details)


def invalid_task_id(task_id: object) -> ApiError:
    return ApiError(400, "INVALID_TASK_ID", f"invalid task id: {task_id!r}", {"task_id": task_id})


def invalid_op(message: str, details: dict | None = None) -> ApiError:
    return ApiError(400, "INVALID_OP", message, details)


def self_loop(task_id: str) -> ApiError:
    return ApiError(400, "SELF_LOOP", f"self-loop is not allowed: {task_id!r}", {"task_id": task_id})


def invalid_digest() -> ApiError:
    return ApiError(400, "INVALID_DIGEST", "output_digest must be a lowercase 64-char hex sha256")


# --- 404 ------------------------------------------------------------------

def pipeline_not_found() -> ApiError:
    return ApiError(404, "PIPELINE_NOT_FOUND", "pipeline not found")


def seal_not_found() -> ApiError:
    return ApiError(404, "SEAL_NOT_FOUND", "seal not found")


def run_not_found() -> ApiError:
    return ApiError(404, "RUN_NOT_FOUND", "run not found")


def task_not_found(task_id: str) -> ApiError:
    return ApiError(404, "TASK_NOT_FOUND", f"task not found: {task_id!r}", {"task_id": task_id})


# --- 409 conflicts --------------------------------------------------------

def task_exists(task_id: str) -> ApiError:
    return ApiError(409, "TASK_EXISTS", f"task already exists: {task_id!r}", {"task_id": task_id})


def edge_exists(src: str, dst: str) -> ApiError:
    return ApiError(409, "EDGE_EXISTS", f"edge already exists: {src!r} -> {dst!r}",
                    {"src": src, "dst": dst})


def edge_not_found(src: str, dst: str) -> ApiError:
    return ApiError(409, "EDGE_NOT_FOUND", f"edge not found: {src!r} -> {dst!r}",
                    {"src": src, "dst": dst})


def stale_version(current: int) -> ApiError:
    return ApiError(409, "STALE_VERSION", "draft_version is stale; no changes applied",
                    {"current_draft_version": current})


def op_conflict(op_id: str) -> ApiError:
    return ApiError(409, "OP_CONFLICT",
                    f"op_id {op_id!r} was already used with different parameters", {"op_id": op_id})


def seal_conflict(draft_version: int, seal_id: str) -> ApiError:
    return ApiError(409, "SEAL_CONFLICT",
                    "draft version already sealed under a different idempotency key",
                    {"draft_version": draft_version, "existing_seal_id": seal_id})


def cycle_detected(witness: list[str]) -> ApiError:
    return ApiError(409, "CYCLE_DETECTED", "graph contains a cycle; no seal created",
                    {"cycle": witness})


def run_not_running(status: str) -> ApiError:
    return ApiError(409, "RUN_NOT_RUNNING", f"run is not running (status={status})",
                    {"status": status})


def no_ready_task() -> ApiError:
    return ApiError(409, "NO_READY_TASK", "no task is currently claimable")


def task_not_ready(task_id: str, state: str) -> ApiError:
    return ApiError(409, "TASK_NOT_READY", f"task {task_id!r} is not claimable (state={state})",
                    {"task_id": task_id, "state": state})


def claim_invalid(message: str = "claim does not match the current grant") -> ApiError:
    return ApiError(409, "CLAIM_INVALID", message)


def fencing_stale() -> ApiError:
    return ApiError(409, "FENCING_STALE", "fencing_token is behind the current grant")


def lease_expired() -> ApiError:
    return ApiError(409, "LEASE_EXPIRED", "lease has expired")


def content_conflict() -> ApiError:
    return ApiError(409, "CONTENT_CONFLICT",
                    "claim already completed with different content")
