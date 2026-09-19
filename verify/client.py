"""Minimal HTTP helper for the verification suite."""

from __future__ import annotations

import json

import httpx


class Api:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self.client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0))

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        resp = self.client.request(method, self.base + path, json=body)
        try:
            payload = resp.json()
        except json.JSONDecodeError:
            payload = {"_raw": resp.text}
        return resp.status_code, payload

    def get(self, path: str) -> tuple[int, dict]:
        return self.request("GET", path)

    def post(self, path: str, body: dict | None = None) -> tuple[int, dict]:
        return self.request("POST", path, body)
