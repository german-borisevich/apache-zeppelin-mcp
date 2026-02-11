import asyncio
import functools
import os
import logging
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("zeppelin-mcp")

ZEPPELIN_BASE_URL = os.environ.get("ZEPPELIN_BASE_URL")
ZEPPELIN_USERNAME = os.environ.get("ZEPPELIN_USERNAME")
ZEPPELIN_PASSWORD = os.environ.get("ZEPPELIN_PASSWORD")

if not ZEPPELIN_BASE_URL:
    raise ValueError("ZEPPELIN_BASE_URL environment variable is required")
if not ZEPPELIN_USERNAME:
    raise ValueError("ZEPPELIN_USERNAME environment variable is required")
if not ZEPPELIN_PASSWORD:
    raise ValueError("ZEPPELIN_PASSWORD environment variable is required")

MAX_OUTPUT_CHARS = int(os.environ.get("ZEPPELIN_MAX_OUTPUT_CHARS", "50000"))

mcp = FastMCP("zeppelin")


# ---------------------------------------------------------------------------
# Exception & helpers
# ---------------------------------------------------------------------------

class ZeppelinAPIError(Exception):
    pass


def _check_status(data: dict) -> dict:
    if data.get("status") != "OK":
        raise ZeppelinAPIError(data.get("message", "Unknown error"))
    return data


def _tool_error_handler(operation: str):
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except ZeppelinAPIError as e:
                return f"Error: {e}"
            except Exception as e:
                return f"Error {operation}: {e}"
        return wrapper
    return decorator


def _truncate(text: str, limit: int = MAX_OUTPUT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n... Output truncated ({len(text)} chars, limit {limit})"


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.splitlines())


def _format_messages(msgs: list[dict], indent: int = 0, prefix: str = "", label: str = "Output") -> list[str]:
    lines = []
    for msg in msgs:
        msg_data = msg.get("data", "").strip()
        if msg_data:
            text = _indent(msg_data, indent) if indent else msg_data
            lines.append(f"{prefix}{label} ({msg.get('type', 'TEXT')}):\n{text}")
    return lines


def _format_forms(paragraph: dict) -> list[str]:
    """Extract dynamic form definitions and current values from a paragraph."""
    settings = paragraph.get("settings", {})
    forms = settings.get("forms", {})
    params = settings.get("params", {})
    if not forms and not params:
        return []
    lines: list[str] = []
    if forms:
        lines.append("Dynamic forms:")
        for name, form in forms.items():
            form_type = form.get("type", "unknown")
            default = form.get("defaultValue", "")
            current = params.get(name, default)
            entry = f"  - {name} (type: {form_type}, default: {default!r}, current: {current!r})"
            options = form.get("options", [])
            if options:
                option_strs = []
                for o in options:
                    val = o.get("value", "")
                    display = o.get("displayName", val)
                    option_strs.append(val if display == val else f"{val} ({display})")
                entry += f" options: [{', '.join(option_strs)}]"
            lines.append(entry)
    elif params:
        lines.append("Form parameters:")
        for name, value in params.items():
            lines.append(f"  - {name}: {value!r}")
    return lines


def _build_params_body(params: Optional[dict[str, Any]]) -> dict[str, Any] | None:
    if params:
        return {"params": params}
    return None


async def _save_paragraph_state(notebook_id: str, paragraph_id: str) -> dict | None:
    """Fetch paragraph data needed for config save/restore."""
    try:
        data = _check_status(await zeppelin.request("GET", f"/api/notebook/{notebook_id}"))
        for p in data.get("body", {}).get("paragraphs", []):
            if p.get("id") == paragraph_id:
                logger.debug(
                    "Saved state for paragraph %s, config keys: %s",
                    paragraph_id, list(p.get("config", {}).keys()),
                )
                return p
        return None
    except Exception:
        return None


async def _restore_paragraph_config(
    notebook_id: str, paragraph_id: str, saved: dict
) -> None:
    """Restore paragraph config (chart/visualization settings)."""
    try:
        config = saved.get("config")
        if not config:
            return
        await zeppelin.request(
            "PUT",
            f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}/config",
            json=config,
        )
        logger.debug("Restored config for paragraph %s", paragraph_id)
    except Exception:
        logger.warning("Failed to restore config for paragraph %s", paragraph_id)


async def _wait_for_notebook_completion(
    notebook_id: str, timeout: float = 600.0, poll_interval: float = 2.0
) -> bool:
    """Poll notebook job status until all paragraphs finish or timeout."""
    elapsed = 0.0
    while elapsed < timeout:
        try:
            data = _check_status(await zeppelin.request(
                "GET", f"/api/notebook/job/{notebook_id}"
            ))
            paragraphs = data.get("body", [])
            if not paragraphs or not any(
                p.get("status") in ("RUNNING", "PENDING", "READY")
                for p in paragraphs
            ):
                return True
        except Exception:
            pass
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    logger.warning("Timeout waiting for notebook %s after %.0fs", notebook_id, timeout)
    return False


# ---------------------------------------------------------------------------
# Zeppelin client
# ---------------------------------------------------------------------------

class ZeppelinClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.client = httpx.AsyncClient(timeout=30.0)
        self._authenticated = False

    async def login(self) -> None:
        resp = await self.client.post(
            f"{self.base_url}/api/login",
            data=f"userName={self.username}&password={self.password}",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        self._authenticated = True
        logger.info("Authenticated with Zeppelin")

    async def request(
        self, method: str, path: str, json: Any = None
    ) -> dict:
        if not self._authenticated:
            await self.login()

        url = f"{self.base_url}{path}"
        resp = await self.client.request(method, url, json=json)

        if resp.status_code in (401, 403):
            logger.info("Session expired, re-authenticating")
            await self.login()
            resp = await self.client.request(method, url, json=json)

        resp.raise_for_status()
        return resp.json()


zeppelin = ZeppelinClient(ZEPPELIN_BASE_URL, ZEPPELIN_USERNAME, ZEPPELIN_PASSWORD)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
@_tool_error_handler("listing notebooks")
async def list_notebooks(name_filter: Optional[str] = None) -> str:
    """List notebooks on the Zeppelin server.

    Args:
        name_filter: Optional substring filter (case-insensitive) matched against the full path.
    """
    data = _check_status(await zeppelin.request("GET", "/api/notebook"))
    notebooks = data.get("body", [])
    if name_filter:
        lower_filter = name_filter.lower()
        notebooks = [
            nb for nb in notebooks
            if lower_filter in nb.get("path", nb.get("name", "")).lower()
        ]
    if not notebooks:
        if name_filter:
            return f"No notebooks matching '{name_filter}'."
        return "No notebooks found."
    lines = [f"- {nb.get('id', 'N/A')}: {nb.get('path', nb.get('name', 'N/A'))}" for nb in notebooks]
    return f"Found {len(notebooks)} notebooks:\n" + "\n".join(lines)


@mcp.tool()
@_tool_error_handler("searching notebooks")
async def search_notebooks(query: str) -> str:
    """Full-text search across all notebook paragraphs.

    Args:
        query: Search query string
    """
    data = _check_status(await zeppelin.request("GET", f"/api/notebook/search?q={query}"))
    results = data.get("body", [])
    if not results:
        return f"No results found for '{query}'."
    lines = []
    for r in results:
        raw_id = r.get("id", "")
        parts = raw_id.split("/paragraph/")
        notebook_id = parts[0] if parts else "N/A"
        paragraph_id = parts[1] if len(parts) > 1 else "N/A"

        notebook_name = r.get("name", r.get("notebookName", "N/A"))
        header = r.get("header", "")
        snippet = r.get("snippet", "").replace("\n", " ")[:200]
        lines.append(
            f"- Notebook: {notebook_name} (id: {notebook_id}) | "
            f"Paragraph: {paragraph_id} | "
            f"Header: {header} | "
            f"Snippet: {snippet}"
        )
    return f"Found {len(results)} results for '{query}':\n" + "\n".join(lines)


@mcp.tool()
@_tool_error_handler("getting notebook")
async def get_notebook(notebook_id: str) -> str:
    """Get full notebook details including all paragraphs, code, and output.
    Can be very large — prefer list_paragraphs + get_paragraph when possible.

    Args:
        notebook_id: The notebook ID to retrieve
    """
    data = _check_status(await zeppelin.request("GET", f"/api/notebook/{notebook_id}"))
    nb = data.get("body", {})
    paragraphs = nb.get("paragraphs", [])
    lines = [f"Notebook: {nb.get('name', 'N/A')} (id: {nb.get('id', notebook_id)})"]
    lines.append(f"Paragraphs: {len(paragraphs)}")
    lines.append("")
    for i, p in enumerate(paragraphs):
        title = p.get("title", "")
        text = p.get("text", "")
        status = p.get("status", "UNKNOWN")
        title_str = f" — {title}" if title else ""
        lines.append(f"[{i}] Paragraph {p.get('id', 'N/A')}{title_str} (status: {status})")
        if text:
            lines.append(f"  Code:\n{_indent(text, 2)}")
        form_lines = _format_forms(p)
        if form_lines:
            for fl in form_lines:
                lines.append(f"  {fl}")
        results = p.get("results", {})
        if results and results.get("msg"):
            lines.extend(f"  {l}" for l in _format_messages(results["msg"], indent=2))
        lines.append("")
    return _truncate("\n".join(lines))


@mcp.tool()
@_tool_error_handler("listing paragraphs")
async def list_paragraphs(notebook_id: str) -> str:
    """List paragraph metadata (index, id, title, status) without code or output.

    Args:
        notebook_id: The notebook ID to list paragraphs for
    """
    data = _check_status(await zeppelin.request("GET", f"/api/notebook/{notebook_id}"))
    nb = data.get("body", {})
    paragraphs = nb.get("paragraphs", [])
    lines = [f"Notebook: {nb.get('name', 'N/A')} (id: {nb.get('id', notebook_id)})"]
    lines.append(f"Total paragraphs: {len(paragraphs)}")
    lines.append("")
    for i, p in enumerate(paragraphs):
        title = p.get("title", "")
        status = p.get("status", "UNKNOWN")
        pid = p.get("id", "N/A")
        if title:
            label = title
        else:
            text = p.get("text", "")
            first_line = text.split("\n", 1)[0] if text else ""
            if len(first_line) > 60:
                first_line = first_line[:60] + "..."
            label = f'"{first_line}"' if first_line else "(empty)"
        lines.append(f"[{i}] {pid} - {label} (status: {status})")
    return "\n".join(lines)


@mcp.tool()
@_tool_error_handler("getting paragraph")
async def get_paragraph(notebook_id: str, paragraph_id: str) -> str:
    """Get full content of a single paragraph (code, output, and dynamic forms).

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to retrieve
    """
    data = _check_status(await zeppelin.request(
        "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
    ))
    p = data.get("body", {})
    title = p.get("title", "")
    text = p.get("text", "")
    status = p.get("status", "UNKNOWN")
    pid = p.get("id", paragraph_id)

    lines = [f"Paragraph: {pid}"]
    if title:
        lines.append(f"Title: {title}")
    lines.append(f"Status: {status}")
    if text:
        lines.append(f"Code:\n{_indent(text, 2)}")
    lines.extend(_format_forms(p))
    results = p.get("results", {})
    if results and results.get("msg"):
        lines.extend(_format_messages(results["msg"], indent=2))
    return _truncate("\n".join(lines))


@mcp.tool()
@_tool_error_handler("getting paragraph forms")
async def get_paragraph_forms(notebook_id: str, paragraph_id: str) -> str:
    """Get dynamic form definitions and current parameter values for a paragraph.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to inspect
    """
    data = _check_status(await zeppelin.request(
        "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
    ))
    p = data.get("body", {})
    form_lines = _format_forms(p)
    if not form_lines:
        return f"Paragraph {paragraph_id} has no dynamic forms."
    return "\n".join(form_lines)


@mcp.tool()
@_tool_error_handler("updating paragraph forms")
async def update_paragraph_forms(
    notebook_id: str, paragraph_id: str, params: dict[str, Any]
) -> str:
    """Update dynamic form values without re-executing the paragraph.
    Safest way to change form parameters when chart settings must be preserved.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to update
        params: Dict of form values to set, e.g. {"city": "Seoul", "limit": "10"}.
    """
    saved = await _save_paragraph_state(notebook_id, paragraph_id)
    if saved is None:
        return f"Error: could not fetch paragraph {paragraph_id}"

    settings = saved.get("settings", {})
    existing_params = settings.get("params", {})
    existing_params.update(params)
    settings["params"] = existing_params

    body: dict[str, Any] = {
        "text": saved.get("text", ""),
        "config": saved.get("config", {}),
        "settings": settings,
    }
    title = saved.get("title")
    if title:
        body["title"] = title

    await zeppelin.request(
        "PUT",
        f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}",
        json=body,
    )
    return (
        f"Updated form values for paragraph {paragraph_id}: "
        + ", ".join(f"{k}={v!r}" for k, v in params.items())
        + ". Paragraph was NOT re-executed."
    )


@mcp.tool()
@_tool_error_handler("updating paragraph config")
async def update_paragraph_config(
    notebook_id: str,
    paragraph_id: str,
    config: dict[str, Any],
) -> str:
    """Update paragraph visualization/chart config (graph type, columns, display settings).
    Fetches current config and deep-merges provided changes.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to configure
        config: Dict of config fields to set or update. Merged with existing config.
    """
    saved = await _save_paragraph_state(notebook_id, paragraph_id)
    if saved:
        current_config = saved.get("config", {})
        if "graph" in config:
            user_graph = config["graph"]

            col_index_map = {}
            results_msg = saved.get("results", {}).get("msg", [])
            if results_msg:
                first_msg = results_msg[0].get("data", "")
                header_line = first_msg.split("\n", 1)[0]
                if header_line:
                    col_index_map = {name: i for i, name in enumerate(header_line.split("\t"))}

            if col_index_map:
                for field in ("keys", "groups", "values"):
                    for col in user_graph.get(field, []):
                        if "index" not in col or col["index"] is None:
                            name = col.get("name", "")
                            if name in col_index_map:
                                col["index"] = col_index_map[name]
                        if "aggr" not in col:
                            col["aggr"] = "sum"

            merged_graph = {**current_config.get("graph", {}), **user_graph}
            config = {**current_config, **config, "graph": merged_graph}
            results = config.get("results", {})
            for result_data in results.values():
                if isinstance(result_data, dict) and "graph" in result_data:
                    result_data["graph"] = {**result_data["graph"], **user_graph}
        else:
            config = {**current_config, **config}

    await zeppelin.request(
        "PUT",
        f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}/config",
        json=config,
    )
    graph = config.get("graph", {})
    mode = graph.get("mode")
    parts = [f"Updated config for paragraph {paragraph_id}"]
    if mode:
        parts.append(f"chart type: {mode}")
    if graph.get("keys"):
        parts.append(f"keys: {[k['name'] for k in graph['keys']]}")
    if graph.get("groups"):
        parts.append(f"groups: {[g['name'] for g in graph['groups']]}")
    if graph.get("values"):
        parts.append(f"values: {[v['name'] for v in graph['values']]}")
    return ". ".join(parts) + "."


@mcp.tool()
@_tool_error_handler("creating notebook")
async def create_notebook(name: str) -> str:
    """Create a new empty notebook.

    Args:
        name: Name for the new notebook
    """
    data = _check_status(await zeppelin.request("POST", "/api/notebook", json={"name": name}))
    return f"Created notebook with id: {data.get('body', 'unknown')}"


@mcp.tool()
@_tool_error_handler("adding paragraph")
async def add_paragraph(
    notebook_id: str,
    text: str,
    title: Optional[str] = None,
    index: Optional[int] = None,
) -> str:
    """Add a new paragraph to an existing notebook.

    Args:
        notebook_id: The notebook ID to add the paragraph to
        text: The code/content for the paragraph
        title: Optional title for the paragraph
        index: Optional position index to insert the paragraph at
    """
    body: dict[str, Any] = {"text": text}
    if title is not None:
        body["title"] = title
    if index is not None:
        body["index"] = index
    data = _check_status(await zeppelin.request(
        "POST", f"/api/notebook/{notebook_id}/paragraph", json=body
    ))
    return f"Added paragraph with id: {data.get('body', 'unknown')}"


@mcp.tool()
@_tool_error_handler("running paragraph")
async def run_paragraph(
    notebook_id: str,
    paragraph_id: str,
    params: Optional[dict[str, Any]] = None,
) -> str:
    """Run a paragraph synchronously and return the result.
    Chart settings are saved/restored automatically around execution.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to run
        params: Optional dict of dynamic form values, e.g. {"city": "Seoul"}.
    """
    saved = await _save_paragraph_state(notebook_id, paragraph_id)

    data = _check_status(await zeppelin.request(
        "POST", f"/api/notebook/run/{notebook_id}/{paragraph_id}",
        json=_build_params_body(params),
    ))

    if saved is not None:
        await asyncio.sleep(0.5)
        await _restore_paragraph_config(notebook_id, paragraph_id, saved)

    resp_body = data.get("body", {})
    code = resp_body.get("code", "UNKNOWN")
    lines = [f"Status: {code}"]
    lines.extend(_format_messages(resp_body.get("msg", [])))
    return _truncate("\n".join(lines))


@mcp.tool()
@_tool_error_handler("running all paragraphs")
async def run_all_paragraphs(
    notebook_id: str,
    params: Optional[dict[str, Any]] = None,
) -> str:
    """Run all paragraphs in a notebook and wait for completion.
    Chart settings are saved/restored automatically around execution.

    Args:
        notebook_id: The notebook ID to run
        params: Optional dict of dynamic form values, e.g. {"city": "Seoul"}.
    """
    nb_data = await zeppelin.request("GET", f"/api/notebook/{notebook_id}")
    saved_paragraphs: list[dict] = []
    if nb_data.get("status") == "OK":
        saved_paragraphs = nb_data.get("body", {}).get("paragraphs", [])

    _check_status(await zeppelin.request(
        "POST", f"/api/notebook/job/{notebook_id}",
        json=_build_params_body(params),
    ))

    completed = await _wait_for_notebook_completion(notebook_id)

    restored = 0
    for p in saved_paragraphs:
        pid = p.get("id")
        if pid and p.get("config"):
            await _restore_paragraph_config(notebook_id, pid, p)
            restored += 1

    status = "completed" if completed else "timed out"
    return (
        f"Execution of all paragraphs in notebook {notebook_id} {status}. "
        f"Restored chart settings for {restored} paragraphs."
    )


@mcp.tool()
@_tool_error_handler("getting paragraph status")
async def get_paragraph_status(notebook_id: str, paragraph_id: str) -> str:
    """Get execution status of a specific paragraph (useful after async run).

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to check
    """
    data = _check_status(await zeppelin.request(
        "GET", f"/api/notebook/job/{notebook_id}/{paragraph_id}"
    ))
    body = data.get("body", {})
    status = body.get("status", "UNKNOWN")
    started = body.get("started", "N/A")
    finished = body.get("finished", "N/A")
    progress = body.get("progress", "N/A")
    lines = [
        f"Status: {status}",
        f"Started: {started}",
        f"Finished: {finished}",
        f"Progress: {progress}",
    ]
    if status in ("ERROR", "ABORT"):
        try:
            para_data = _check_status(await zeppelin.request(
                "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
            ))
            results = para_data.get("body", {}).get("results", {})
            if results and results.get("msg"):
                lines.extend(
                    f"\n{l}" for l in _format_messages(results["msg"], label="Error Output")
                )
        except Exception:
            pass
    return _truncate("\n".join(lines))


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
