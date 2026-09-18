"""Integration smoke tests against a running API (skipped when unreachable).

Run with: API_BASE=http://localhost:8000 pytest tests/test_api.py
The full acceptance suite lives in `verify/verify.py` and runs as the
`verify` compose service.
"""

from __future__ import annotations

import hashlib
import os
import uuid

import httpx
import pytest

API = os.environ.get("API_BASE", "http://localhost:8000")


def _api_up() -> bool:
    try:
        return httpx.get(f"{API}/healthz", timeout=2).status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _api_up(), reason="API not reachable")


def test_end_to_end_happy_path():
    c = httpx.Client(base_url=API, timeout=30)
    pid = c.post("/pipelines", json={}).json()["pipeline_id"]

    r = c.post(f"/pipelines/{pid}/draft/mutations", json={
        "op_id": uuid.uuid4().hex, "draft_version": 0,
        "operations": [{"type": "add_task", "task_id": "a"},
                       {"type": "add_task", "task_id": "b"}]})
    assert r.status_code == 200
    v = r.json()["draft_version"]
    r = c.post(f"/pipelines/{pid}/draft/mutations", json={
        "op_id": uuid.uuid4().hex, "draft_version": v,
        "operations": [{"type": "add_edge", "src": "a", "dst": "b"}]})
    v = r.json()["draft_version"]

    r = c.post(f"/pipelines/{pid}/seals", json={
        "expected_draft_version": v, "idempotency_key": uuid.uuid4().hex})
    assert r.status_code == 201
    seal = r.json()
    assert seal["topo_order"] == ["a", "b"]

    r = c.post("/runs", json={
        "pipeline_id": pid, "seal_id": seal["seal_id"],
        "idempotency_key": uuid.uuid4().hex,
        "max_attempts": 2, "lease_seconds": 5})
    assert r.status_code == 201
    rid = r.json()["run_id"]

    cl = c.post(f"/runs/{rid}/claims").json()
    assert cl["claimed"] and cl["task_id"] == "a"
    digest = hashlib.sha256(b"a-out").hexdigest()
    r = c.post(f"/runs/{rid}/claims/complete", json={
        "claim_token": cl["claim_token"], "attempt": cl["attempt"],
        "fencing_token": cl["fencing_token"], "output_digest": digest})
    assert r.status_code == 200 and r.json()["released"] == ["b"]

    cl = c.post(f"/runs/{rid}/claims").json()
    assert cl["claimed"] and cl["task_id"] == "b"
    r = c.post(f"/runs/{rid}/claims/complete", json={
        "claim_token": cl["claim_token"], "attempt": cl["attempt"],
        "fencing_token": cl["fencing_token"],
        "output_digest": hashlib.sha256(b"b-out").hexdigest()})
    assert r.status_code == 200
    assert c.get(f"/runs/{rid}").json()["status"] == "succeeded"


def test_error_shape_is_stable():
    c = httpx.Client(base_url=API, timeout=10)
    r = c.get("/pipelines/nope")
    assert r.status_code == 404
    body = r.json()
    assert body["error"]["code"] == "PIPELINE_NOT_FOUND"
    assert "message" in body["error"] and "details" in body["error"]
