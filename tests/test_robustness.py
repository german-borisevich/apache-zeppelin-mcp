"""Robustness tests: HTTP retry/backoff, batch partial-failure semantics,
~Backups protection (including path-cache staleness), and chart-config
column auto-fill.

Network is faked at two seams:
- ZeppelinClient tests use httpx.MockTransport (real client code, fake wire).
- Tool-level tests use FakeZeppelin, a stand-in for ZeppelinClient that routes
  request(method, path, ...) against an in-memory notebook (see conftest.py).
"""

import httpx
import pytest
from mcp.server.fastmcp.exceptions import ToolError

import server
from conftest import FakeZeppelin, _ctx, _para, run
from server import ParagraphRun, ParagraphUpdate, ZeppelinAPIError


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
