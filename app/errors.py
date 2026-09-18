"""Stable machine-readable error codes and the shared error response shape.

Every error response has the form:
    {"error": {"code": "<CODE>", "message": "...", "details": {...}}}
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class ApiError(Exception):
    """Application error carrying a stable code, HTTP status and details."""

    def __init__(self, status: int, code: str, message: str,
                 details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}

    def body(self) -> Dict[str, Any]:
        return {"error": {"code": self.code, "message": self.message,
                          "details": self.details}}


# --- generic ---------------------------------------------------------------
def invalid_request(message: str, details: Optional[Dict[str, Any]] = None) -> ApiError:
    return ApiError(400, "INVALID_REQUEST", message, details)


def not_found(resource: str, ident: str) -> ApiError:
    return ApiError(404, f"{resource}_NOT_FOUND",
                    f"{resource.lower()} {ident!r} does not exist",
                    {resource.lower(): ident})


# --- draft mutations -------------------------------------------------------
def op_conflict(op_id: str) -> ApiError:
    return ApiError(409, "OP_CONFLICT",
                    "op_id was already used with different parameters",
                    {"op_id": op_id})


def version_conflict(current: int) -> ApiError:
    return ApiError(409, "VERSION_CONFLICT",
                    "draft_version is stale; no changes were applied",
                    {"current_draft_version": current})


def seal_version_conflict(current: int) -> ApiError:
    return ApiError(409, "SEAL_VERSION_CONFLICT",
                    "expected_draft_version does not match the current draft version",
                    {"current_draft_version": current})


def idempotency_conflict(key: str) -> ApiError:
    return ApiError(409, "IDEMPOTENCY_CONFLICT",
                    "idempotency key was already used with different parameters",
                    {"idempotency_key": key})


def seal_already_exists(seal_id: str, digest: str) -> ApiError:
    return ApiError(409, "SEAL_ALREADY_EXISTS",
                    "this draft version is already sealed",
                    {"seal_id": seal_id, "digest_sha256": digest})


def cycle_detected(witness: list) -> ApiError:
    return ApiError(409, "CYCLE_DETECTED",
                    "dependency graph contains a cycle; no seal was created",
                    {"cycle": witness})


# --- claims / leases -------------------------------------------------------
def claim_not_found() -> ApiError:
    return ApiError(404, "CLAIM_NOT_FOUND", "claim_token is unknown")


def claim_expired() -> ApiError:
    return ApiError(409, "CLAIM_EXPIRED", "the lease has expired")


def claim_stale() -> ApiError:
    return ApiError(409, "CLAIM_STALE",
                    "attempt or fencing_token is not the current one")


def claim_not_active() -> ApiError:
    return ApiError(409, "CLAIM_NOT_ACTIVE",
                    "the claim was already consumed by a different operation")


def digest_conflict() -> ApiError:
    return ApiError(409, "DIGEST_CONFLICT",
                    "the claim was already completed with a different output digest")


def run_not_running(status: str) -> ApiError:
    return ApiError(409, "RUN_NOT_RUNNING",
                    "the run is already in a terminal state",
                    {"run_status": status})
