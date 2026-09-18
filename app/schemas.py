"""Request/response schemas (pydantic)."""

from __future__ import annotations

import re
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator

TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")

MAX_TASKS = 50_000
MAX_EDGES = 200_000


def _check_task_id(v: str) -> str:
    if not TASK_ID_RE.match(v):
        raise ValueError("task_id must match [A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    return v


# --- draft mutations -------------------------------------------------------
class AddTaskOp(BaseModel):
    type: Literal["add_task"]
    task_id: str
    _tid = field_validator("task_id")(_check_task_id)


class RemoveTaskOp(BaseModel):
    type: Literal["remove_task"]
    task_id: str
    _tid = field_validator("task_id")(_check_task_id)


class AddEdgeOp(BaseModel):
    type: Literal["add_edge"]
    src: str
    dst: str
    _s = field_validator("src")(_check_task_id)
    _d = field_validator("dst")(_check_task_id)


class RemoveEdgeOp(BaseModel):
    type: Literal["remove_edge"]
    src: str
    dst: str
    _s = field_validator("src")(_check_task_id)
    _d = field_validator("dst")(_check_task_id)


Operation = Union[AddTaskOp, RemoveTaskOp, AddEdgeOp, RemoveEdgeOp]


class MutateDraftRequest(BaseModel):
    op_id: str = Field(min_length=1, max_length=128)
    draft_version: int = Field(ge=0)
    operations: List[Operation] = Field(min_length=1, max_length=1000)


class CreatePipelineRequest(BaseModel):
    name: Optional[str] = Field(default=None, max_length=256)


# --- sealing ---------------------------------------------------------------
class SealRequest(BaseModel):
    expected_draft_version: int = Field(ge=0)
    idempotency_key: str = Field(min_length=1, max_length=128)


# --- runs ------------------------------------------------------------------
class CreateRunRequest(BaseModel):
    pipeline_id: str = Field(min_length=1, max_length=64)
    seal_id: str = Field(min_length=1, max_length=64)
    idempotency_key: str = Field(min_length=1, max_length=128)
    max_attempts: int = Field(ge=1, le=10)
    lease_seconds: int = Field(ge=2, le=30)


class ClaimActionRequest(BaseModel):
    claim_token: str = Field(min_length=1, max_length=128)
    attempt: int = Field(ge=1)
    fencing_token: int = Field(ge=1)


class CompleteRequest(ClaimActionRequest):
    output_digest: str

    @field_validator("output_digest")
    @classmethod
    def _digest(cls, v: str) -> str:
        if not DIGEST_RE.match(v):
            raise ValueError(
                "output_digest must be a lowercase 64-char hex SHA-256")
        return v


class FailRequest(ClaimActionRequest):
    reason: Optional[str] = Field(default=None, max_length=1024)
