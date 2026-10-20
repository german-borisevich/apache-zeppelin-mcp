"""Startup without (valid) credentials: the server must stay up and report
missing / rejected credentials as tool errors instead of exiting during init."""

import httpx
import pytest
from mcp.server.fastmcp.exceptions import ToolError

import server
from conftest import _ctx, run
from server import ZeppelinAPIError


def _client_with(handler, base_url="http://zeppelin.test", username="user", password="pass"):
    zc = server.ZeppelinClient(base_url, username, password)
    zc.client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=1.0)
    return zc


def _unreachable(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"unexpected request to {request.url}")


def test_lifespan_starts_without_env(monkeypatch):
    monkeypatch.setattr(server, "ZEPPELIN_BASE_URL", "")
    monkeypatch.setattr(server, "ZEPPELIN_USERNAME", "")
    monkeypatch.setattr(server, "ZEPPELIN_PASSWORD", "")

    async def enter():
        async with server.app_lifespan(server.mcp) as app:
            return app.zeppelin.missing_config()

    assert run(enter()) == ["ZEPPELIN_BASE_URL", "ZEPPELIN_USERNAME", "ZEPPELIN_PASSWORD"]


def test_lifespan_does_not_contact_zeppelin(monkeypatch):
    monkeypatch.setattr(server, "ZEPPELIN_BASE_URL", "http://zeppelin.test")
    monkeypatch.setattr(server, "ZEPPELIN_USERNAME", "user")
    monkeypatch.setattr(server, "ZEPPELIN_PASSWORD", "wrong")

    async def no_login(self):
        raise AssertionError("login at startup")

    monkeypatch.setattr(server.ZeppelinClient, "login", no_login)

    async def enter():
        async with server.app_lifespan(server.mcp):
            pass

    run(enter())


def test_missing_username_is_a_clear_error_without_network():
    zc = _client_with(_unreachable, username="")
    with pytest.raises(ZeppelinAPIError, match=r"ZEPPELIN_USERNAME not set.*/mcp"):
        run(zc.request("GET", "/api/notebook"))


def test_missing_config_surfaces_as_tool_error():
    zc = _client_with(_unreachable, base_url="", password="")
    with pytest.raises(ToolError, match="ZEPPELIN_BASE_URL, ZEPPELIN_PASSWORD not set"):
        run(server.list_notebooks(_ctx(zc)))


@pytest.mark.parametrize("status", [401, 403])
def test_rejected_login_is_clear_and_not_retried(status):
    logins = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            logins["n"] += 1
            return httpx.Response(status, text="")
        raise AssertionError("API must not be called after a rejected login")

    zc = _client_with(handler)
    for _ in range(3):
        with pytest.raises(ZeppelinAPIError, match=rf"rejected the login for user 'user' \(HTTP {status}\)"):
            run(zc.request("GET", "/api/notebook"))
    assert logins["n"] == 1


def test_rejected_reauth_mid_session_is_clear():
    logins = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/login":
            logins["n"] += 1
            return httpx.Response(200 if logins["n"] == 1 else 403, text="")
        return httpx.Response(403, text="")  # session expired

    with pytest.raises(ZeppelinAPIError, match="rejected the login"):
        run(_client_with(handler).request("GET", "/api/notebook"))
    assert logins["n"] == 2
