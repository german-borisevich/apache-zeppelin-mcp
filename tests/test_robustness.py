"""Robustness tests: HTTP retry/backoff, batch partial-failure semantics,
~Backups protection (including path-cache staleness), and chart-config
column auto-fill.

Network is faked at two seams:
- ZeppelinClient tests use httpx.MockTransport (real client code, fake wire).
- Tool-level tests use FakeZeppelin, a stand-in for ZeppelinClient that routes
  request(method, path, ...) against an in-memory notebook.
"""

import asyncio
import re
from types import SimpleNamespace

import httpx
import pytest
from mcp.server.fastmcp.exceptions import ToolError

import server
from server import ParagraphRun, ParagraphUpdate, ZeppelinAPIError


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
# ZeppelinClient retry / redirect behavior (httpx.MockTransport)
# ---------------------------------------------------------------------------

def _client_with(handler) -> server.ZeppelinClient:
    zc = server.ZeppelinClient("http://zeppelin.test", "user", "pass")
    zc.client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=1.0)
    return zc


def test_503_is_retried_then_succeeds():
    calls = {"api": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            return httpx.Response(200, json={"status": "OK"})
        calls["api"] += 1
        if calls["api"] < 3:
            return httpx.Response(503, text="overloaded")
        return httpx.Response(200, json={"status": "OK", "body": {"ok": True}})

    data = run(_client_with(handler).request("GET", "/api/notebook"))
    assert data["body"] == {"ok": True}
    assert calls["api"] == 3


def test_persistent_503_raises_http_status_error():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            return httpx.Response(200, json={"status": "OK"})
        return httpx.Response(503, text="overloaded")

    with pytest.raises(httpx.HTTPStatusError):
        run(_client_with(handler).request("GET", "/api/notebook"))


def test_read_timeout_is_retried():
    calls = {"api": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            return httpx.Response(200, json={"status": "OK"})
        calls["api"] += 1
        if calls["api"] == 1:
            raise httpx.ReadTimeout("read timed out")
        return httpx.Response(200, json={"status": "OK", "body": {"ok": True}})

    data = run(_client_with(handler).request("GET", "/api/notebook"))
    assert data["body"] == {"ok": True}
    assert calls["api"] == 2


def test_persistent_connect_error_raises_after_retries():
    calls = {"api": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            return httpx.Response(200, json={"status": "OK"})
        calls["api"] += 1
        raise httpx.ConnectError("connection refused")

    with pytest.raises(httpx.ConnectError):
        run(_client_with(handler).request("GET", "/api/notebook"))
    assert calls["api"] == server.REQUEST_RETRY_ATTEMPTS


def test_persistent_redirect_raises_clear_error_not_json_decode():
    logins = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            logins["n"] += 1
            return httpx.Response(200, json={"status": "OK"})
        return httpx.Response(
            302, headers={"Location": "http://zeppelin.test/#/login"}, text=""
        )

    with pytest.raises(ZeppelinAPIError, match="redirecting to the login page"):
        run(_client_with(handler).request("GET", "/api/notebook"))
    assert logins["n"] == 2  # initial login + re-auth attempt


def test_redirect_then_success_after_reauth():
    calls = {"api": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            return httpx.Response(200, json={"status": "OK"})
        calls["api"] += 1
        if calls["api"] == 1:  # expired session -> redirect to login page
            return httpx.Response(
                302, headers={"Location": "http://zeppelin.test/#/login"}, text=""
            )
        return httpx.Response(200, json={"status": "OK", "body": {"ok": True}})

    data = run(_client_with(handler).request("GET", "/api/notebook"))
    assert data["body"] == {"ok": True}


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


# ---------------------------------------------------------------------------
# Batch partial-failure semantics
# ---------------------------------------------------------------------------

def test_batch_update_paragraph_continues_past_failure(no_backup):
    fake = FakeZeppelin(paragraphs={"p1": _para("p1"), "p3": _para("p3")})
    updates = [
        ParagraphUpdate(paragraph_id="p1", text="new1"),
        ParagraphUpdate(paragraph_id="p2", text="new2"),  # missing -> fails
        ParagraphUpdate(paragraph_id="p3", text="new3"),
    ]
    result = run(server.batch_update_paragraph(_ctx(fake), "NB1", updates))

    assert "Updated 2/3 paragraphs." in result
    assert "Failed:" in result and "p2" in result
    assert fake.paragraphs["p1"]["text"] == "new1"
    assert fake.paragraphs["p3"]["text"] == "new3"  # p3 processed despite p2 failing
    assert no_backup == ["p1", "p3"]


def test_batch_run_paragraph_stops_on_error():
    fake = FakeZeppelin(
        paragraphs={
            "ok1": _para("ok1"),
            "bad": _para("bad", code="ERROR"),
            "ok2": _para("ok2"),
        },
        run_codes={"bad": "ERROR"},
    )
    result = run(server.batch_run_paragraph(
        _ctx(fake), "NB1",
        [ParagraphRun(paragraph_id="ok1"), ParagraphRun(paragraph_id="bad"), ParagraphRun(paragraph_id="ok2")],
    ))

    assert "Ran 2/3 paragraphs (stopped on error)." in result
    assert "Skipped: ok2" in result
    # ok2 was never submitted
    assert ("POST", "/api/notebook/job/NB1/ok2") not in fake.calls


def test_batch_run_paragraph_continues_when_stop_on_error_false():
    fake = FakeZeppelin(
        paragraphs={
            "ok1": _para("ok1"),
            "bad": _para("bad", code="ERROR"),
            "ok2": _para("ok2"),
        },
        run_codes={"bad": "ERROR"},
    )
    result = run(server.batch_run_paragraph(
        _ctx(fake), "NB1",
        [ParagraphRun(paragraph_id="ok1"), ParagraphRun(paragraph_id="bad"), ParagraphRun(paragraph_id="ok2")],
        stop_on_error=False,
    ))

    assert "Ran 3/3 paragraphs." in result
    assert "Skipped" not in result
    assert ("POST", "/api/notebook/job/NB1/ok2") in fake.calls


def test_batch_run_paragraph_merges_shared_and_per_item_params():
    fake = FakeZeppelin(paragraphs={"p1": _para("p1"), "p2": _para("p2")})
    runs = [
        ParagraphRun(paragraph_id="p1"),                                            # shared only
        ParagraphRun(paragraph_id="p2", params={"city": "Seoul", "env": "stage"}),  # overrides shared key
        ParagraphRun(paragraph_id="p2", params={"city": "Tokyo"}),                  # sweep: same pid again
    ]
    result = run(server.batch_run_paragraph(_ctx(fake), "NB1", runs, params={"env": "prod"}))

    assert "Ran 3/3 paragraphs." in result
    assert fake.run_param_bodies == [
        ("p1", {"params": {"env": "prod"}}),
        ("p2", {"params": {"env": "stage", "city": "Seoul"}}),
        ("p2", {"params": {"env": "prod", "city": "Tokyo"}}),
    ]


def test_batch_run_paragraph_without_any_params_posts_none():
    fake = FakeZeppelin(paragraphs={"p1": _para("p1")})
    run(server.batch_run_paragraph(_ctx(fake), "NB1", [ParagraphRun(paragraph_id="p1")]))
    assert fake.run_param_bodies == [("p1", None)]


# ---------------------------------------------------------------------------
# ~Backups protection & path-cache staleness
# ---------------------------------------------------------------------------

def test_mutation_blocked_inside_backups(no_backup):
    fake = FakeZeppelin(
        path="Users/german/~Backups/Project/NB_2ABC_backup",
        paragraphs={"p1": _para("p1")},
    )
    with pytest.raises(ToolError, match="~Backups"):
        run(server.update_paragraph(_ctx(fake), "NB1", "p1", "new text"))
    assert fake.paragraphs["p1"]["text"] != "new text"


def test_backup_guard_after_notebook_moved_into_backups(no_backup):
    """Path cached as non-backup, notebook then moved into ~Backups: once the
    TTL expires the guard must re-fetch the path and block the mutation."""
    fake = FakeZeppelin(path="Users/german/Project/NB", paragraphs={"p1": _para("p1")})
    ctx = _ctx(fake)

    run(server.update_paragraph(ctx, "NB1", "p1", "v2"))
    assert fake.path_fetches == 1

    # Fresh cache entry: second call is a cache hit (no re-fetch).
    run(server.update_paragraph(ctx, "NB1", "p1", "v3"))
    assert fake.path_fetches == 1

    # Notebook is moved into ~Backups outside this server; expire the entry.
    fake.path = "Users/german/~Backups/Project/NB"
    value, cached_at = server._notebook_path_cache["NB1"]
    server._notebook_path_cache["NB1"] = (value, cached_at - server._CACHE_TTL_SECONDS)

    with pytest.raises(ToolError, match="~Backups"):
        run(server.update_paragraph(ctx, "NB1", "p1", "v4"))
    assert fake.path_fetches == 2
    assert fake.paragraphs["p1"]["text"] == "v3"


def test_path_cache_size_cap(monkeypatch, no_backup):
    monkeypatch.setattr(server, "_CACHE_MAX_ENTRIES", 2)
    fake = FakeZeppelin(paragraphs={"p1": _para("p1")})
    ctx = _ctx(fake)
    for nb in ("NB1", "NB2", "NB3"):
        run(server.update_paragraph(ctx, nb, "p1", "x"))
    assert len(server._notebook_path_cache) <= 2


# ---------------------------------------------------------------------------
# Chart-config column auto-fill
# ---------------------------------------------------------------------------

def test_config_autofill_with_saved_results():
    para = {
        "id": "p1",
        "text": "%sql select ...",
        "config": {},
        "results": {
            "code": "SUCCESS",
            "msg": [{"type": "TABLE", "data": "event_date\twin_rate\n2026-01-01\t0.5"}],
        },
    }
    fake = FakeZeppelin(paragraphs={"p1": para})
    config = {
        "graph": {
            "mode": "lineChart",
            "keys": [{"name": "event_date"}],
            "values": [{"name": "win_rate"}],
        }
    }
    merged = run(server._apply_paragraph_config(fake, "NB1", "p1", config))

    graph = merged["graph"]
    assert graph["keys"][0]["index"] == 0
    assert graph["values"][0]["index"] == 1
    assert graph["values"][0]["aggr"] == "sum"


def test_config_autofill_skipped_without_results():
    """Documented behavior: a never-run paragraph has no output headers, so
    index/aggr cannot be auto-filled — column names pass through as given."""
    para = {"id": "p1", "text": "%sql select ...", "config": {}}
    fake = FakeZeppelin(paragraphs={"p1": para})
    config = {
        "graph": {
            "mode": "lineChart",
            "keys": [{"name": "event_date"}],
            "values": [{"name": "win_rate"}],
        }
    }
    merged = run(server._apply_paragraph_config(fake, "NB1", "p1", config))

    graph = merged["graph"]
    assert graph["keys"][0] == {"name": "event_date"}  # no index
    assert graph["values"][0] == {"name": "win_rate"}  # no index, no aggr
    # Config was still PUT to the server
    assert any(m == "PUT" and p.endswith("/config") for m, p in fake.calls)
