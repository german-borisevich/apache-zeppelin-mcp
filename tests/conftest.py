"""Shared test helpers: a request()-level FakeZeppelin, a minimal MCP ctx,
and fixtures that isolate module-level state (caches, retry backoff)."""

import asyncio
import re
from types import SimpleNamespace

import pytest

import server
from server import ZeppelinAPIError


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """Zero out retry sleeps and clear module-level caches around every test."""
    monkeypatch.setattr(server, "REQUEST_RETRY_BACKOFF_SECONDS", (0.0, 0.0))
    server._notebook_path_cache.clear()
    server._backup_notebook_id_cache.clear()
    yield
    server._notebook_path_cache.clear()
    server._backup_notebook_id_cache.clear()


# ---------------------------------------------------------------------------
# FakeZeppelin — request()-level stand-in for tool tests
# ---------------------------------------------------------------------------

class FakeZeppelin:
    """Routes ZeppelinClient.request(method, path) calls to in-memory state."""

    def __init__(self, path="Users/german/Project/NB", paragraphs=None, run_codes=None):
        self.path = path
        self.paragraphs = paragraphs or {}
        self.run_codes = run_codes or {}  # pid -> results code ("SUCCESS"/"ERROR")
        self.path_fetches = 0
        self.calls: list[tuple[str, str]] = []
        self.run_param_bodies: list[tuple[str, dict | None]] = []  # (pid, POST json body)

    async def request(self, method, path, json=None, params=None, timeout=None):
        self.calls.append((method, path))

        if method == "GET" and re.fullmatch(r"/api/notebook/[^/]+", path):
            self.path_fetches += 1
            return {"status": "OK", "body": {"name": self.path}}

        m = re.fullmatch(r"/api/notebook/[^/]+/paragraph/([^/]+)", path)
        if m:
            pid = m.group(1)
            if pid not in self.paragraphs:
                raise ZeppelinAPIError(f"paragraph {pid} not found")
            if method == "GET":
                return {"status": "OK", "body": self.paragraphs[pid]}
            if method == "PUT":
                self.paragraphs[pid]["text"] = json["text"]
                return {"status": "OK", "body": {}}

        if method == "PUT" and path.endswith("/config"):
            return {"status": "OK", "body": {}}

        m = re.fullmatch(r"/api/notebook/job/[^/]+/([^/]+)", path)
        if m:
            pid = m.group(1)
            if method == "POST":
                self.run_param_bodies.append((pid, json))
                return {"status": "OK", "body": {}}
            if method == "GET":
                status = "FINISHED" if self.run_codes.get(pid, "SUCCESS") == "SUCCESS" else "ERROR"
                return {"status": "OK", "body": {"status": status}}

        raise AssertionError(f"FakeZeppelin: unexpected {method} {path}")


def _ctx(zeppelin: FakeZeppelin):
    ctx = SimpleNamespace(
        request_context=SimpleNamespace(
            lifespan_context=SimpleNamespace(zeppelin=zeppelin)
        )
    )

    async def report_progress(*args, **kwargs):
        pass

    ctx.report_progress = report_progress
    return ctx


def _para(pid: str, text: str = "%sql select 1", code: str = "SUCCESS") -> dict:
    return {
        "id": pid,
        "text": text,
        "config": {},
        "results": {"code": code, "msg": [{"type": "TEXT", "data": f"out-{pid}"}]},
    }


@pytest.fixture
def no_backup(monkeypatch):
    """Replace the backup flow with a recorder — not under test here."""
    backed_up: list[str] = []

    async def fake_backup(zeppelin, notebook_id, notebook_path, paragraph_id, data, op="EDIT"):
        backed_up.append(paragraph_id)

    monkeypatch.setattr(server, "_backup_paragraph", fake_backup)
    return backed_up
