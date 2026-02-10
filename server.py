import asyncio
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

mcp = FastMCP("zeppelin")


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


@mcp.tool()
async def list_notebooks(name_filter: Optional[str] = None) -> str:
    """List notebooks on the Zeppelin server.

    Args:
        name_filter: Optional substring filter (case-insensitive). When provided, only
            notebooks whose full path contains this string are returned. Matches against
            the full path.
    """
    try:
        data = await zeppelin.request("GET", "/api/notebook")
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
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
    except Exception as e:
        return f"Error listing notebooks: {e}"


@mcp.tool()
async def search_notebooks(query: str) -> str:
    """Full-text search across all notebook paragraphs.

    Args:
        query: Search query string
    """
    try:
        data = await zeppelin.request("GET", f"/api/notebook/search?q={query}")
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        results = data.get("body", [])
        if not results:
            return f"No results found for '{query}'."
        lines = []
        for r in results:
            # Extract notebook_id and paragraph_id from compound id
            # Format: "noteId/paragraph/paragraphId" or just a plain id
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
    except Exception as e:
        return f"Error searching notebooks: {e}"


@mcp.tool()
async def get_notebook(notebook_id: str) -> str:
    """Get full notebook details including all paragraphs and their content.

    Note: This returns ALL paragraph code and output, which can be very large.
    Consider using list_paragraphs (metadata only) + get_paragraph (single paragraph)
    for a much lighter alternative when you don't need the entire notebook.

    Args:
        notebook_id: The notebook ID to retrieve
    """
    try:
        data = await zeppelin.request("GET", f"/api/notebook/{notebook_id}")
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
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
                lines.append(f"    Code:\n{_indent(text, 6)}")
            form_lines = _format_forms(p)
            if form_lines:
                for fl in form_lines:
                    lines.append(f"    {fl}")
            results = p.get("results", {})
            if results and results.get("msg"):
                for msg in results["msg"]:
                    msg_data = msg.get("data", "").strip()
                    if msg_data:
                        lines.append(f"    Output ({msg.get('type', 'TEXT')}):\n{_indent(msg_data, 6)}")
            lines.append("")
        return "\n".join(lines)
    except Exception as e:
        return f"Error getting notebook: {e}"


@mcp.tool()
async def list_paragraphs(notebook_id: str) -> str:
    """List paragraph metadata (index, id, title, status) without code or output.

    Use this instead of get_notebook when you only need to find paragraph positions
    or IDs. Much lighter — returns only metadata, no code or execution output.

    Args:
        notebook_id: The notebook ID to list paragraphs for
    """
    try:
        data = await zeppelin.request("GET", f"/api/notebook/{notebook_id}")
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
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
    except Exception as e:
        return f"Error listing paragraphs: {e}"


@mcp.tool()
async def get_paragraph(notebook_id: str, paragraph_id: str) -> str:
    """Get full content of a single paragraph (code, output, and dynamic forms).

    Use this to inspect a specific paragraph without loading the entire notebook.
    If the paragraph contains dynamic forms, their definitions and current values
    are included in the response.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to retrieve
    """
    try:
        data = await zeppelin.request(
            "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
        )
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
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
            for msg in results["msg"]:
                msg_data = msg.get("data", "").strip()
                if msg_data:
                    lines.append(f"Output ({msg.get('type', 'TEXT')}):\n{_indent(msg_data, 2)}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error getting paragraph: {e}"


@mcp.tool()
async def get_paragraph_forms(notebook_id: str, paragraph_id: str) -> str:
    """Get dynamic form definitions and current parameter values for a paragraph.

    Returns the form fields (name, type, default, options) and their current values.
    Use this to discover what parameters a paragraph accepts before calling
    run_paragraph with params.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to inspect
    """
    try:
        data = await zeppelin.request(
            "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
        )
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        p = data.get("body", {})
        form_lines = _format_forms(p)
        if not form_lines:
            return f"Paragraph {paragraph_id} has no dynamic forms."
        return "\n".join(form_lines)
    except Exception as e:
        return f"Error getting paragraph forms: {e}"


@mcp.tool()
async def update_paragraph_forms(
    notebook_id: str, paragraph_id: str, params: dict[str, Any]
) -> str:
    """Update dynamic form values without re-executing the paragraph.

    This is the safest way to change form parameters when chart/visualization
    settings must be preserved. Unlike run_paragraph with params, this only
    updates the stored parameter values — it does not trigger execution.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to update
        params: Dict of form values to set, e.g. {"city": "Seoul", "limit": "10"}.
            Keys must match the form field names defined in the paragraph.
    """
    try:
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
    except Exception as e:
        return f"Error updating paragraph forms: {e}"


@mcp.tool()
async def update_paragraph_config(
    notebook_id: str,
    paragraph_id: str,
    config: dict[str, Any],
) -> str:
    """Update paragraph visualization/chart config (graph type, column mappings, display settings).

    Uses the dedicated config endpoint which supports partial updates — only the
    keys you provide are changed, the rest are preserved.

    Common config fields:
      - graph.mode: "table", "multiBarChart", "stackedAreaChart", "lineChart",
          "pieChart", "scatterChart"
      - graph.keys: list of key column mappings, e.g. [{"name": "date", "index": 0, "aggr": "sum"}]
      - graph.groups: list of group column mappings, e.g. [{"name": "category", "index": 1, "aggr": "sum"}]
      - graph.values: list of value column mappings, e.g. [{"name": "revenue", "index": 2, "aggr": "sum"}]
          Supported aggr values: "sum", "count", "avg", "min", "max"
      - graph.setting.multiBarChart (or lineChart, etc.): chart-specific options
      - colWidth: paragraph width in the grid (1–12)
      - enabled: whether the paragraph is enabled (true/false)

    Example config for a line chart with date as key, offer_group as group, arppu as value:
        {
            "graph": {
                "mode": "lineChart",
                "keys": [{"name": "date", "index": 0, "aggr": "sum"}],
                "groups": [{"name": "offer_group", "index": 1, "aggr": "sum"}],
                "values": [{"name": "arppu", "index": 2, "aggr": "sum"}]
            }
        }

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to configure
        config: Dict of config fields to set or update. Merged with existing config.
    """
    try:
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
    except Exception as e:
        return f"Error updating paragraph config: {e}"


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
                option_strs = [
                    f"{o.get('value', '')} ({o.get('displayName', o.get('value', ''))})"
                    for o in options
                ]
                entry += f" options: [{', '.join(option_strs)}]"
            lines.append(entry)
    elif params:
        lines.append("Form parameters:")
        for name, value in params.items():
            lines.append(f"  - {name}: {value!r}")
    return lines


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.splitlines())


async def _save_paragraph_state(notebook_id: str, paragraph_id: str) -> dict | None:
    """Fetch paragraph data needed for config save/restore.

    Uses notebook-level GET to ensure the full config (including
    config.results with visualization column mappings) is captured.
    """
    try:
        data = await zeppelin.request("GET", f"/api/notebook/{notebook_id}")
        if data.get("status") != "OK":
            return None
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
    """Restore paragraph config (chart/visualization settings) via the
    dedicated config endpoint.

    Uses PUT /api/notebook/{noteId}/paragraph/{paragraphId}/config which
    persists config changes. The generic paragraph PUT endpoint only
    accepts text and title — it silently ignores config.
    """
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
            data = await zeppelin.request(
                "GET", f"/api/notebook/job/{notebook_id}"
            )
            if data.get("status") != "OK":
                return False
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


@mcp.tool()
async def create_notebook(name: str) -> str:
    """Create a new empty notebook.

    Args:
        name: Name for the new notebook
    """
    try:
        data = await zeppelin.request("POST", "/api/notebook", json={"name": name})
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        return f"Created notebook with id: {data.get('body', 'unknown')}"
    except Exception as e:
        return f"Error creating notebook: {e}"


@mcp.tool()
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
    try:
        body: dict[str, Any] = {"text": text}
        if title is not None:
            body["title"] = title
        if index is not None:
            body["index"] = index
        data = await zeppelin.request(
            "POST", f"/api/notebook/{notebook_id}/paragraph", json=body
        )
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        return f"Added paragraph with id: {data.get('body', 'unknown')}"
    except Exception as e:
        return f"Error adding paragraph: {e}"


@mcp.tool()
async def run_paragraph(
    notebook_id: str,
    paragraph_id: str,
    params: Optional[dict[str, Any]] = None,
) -> str:
    """Run a paragraph synchronously and return the result.

    Supports dynamic forms — pass params to set form values before execution.
    Use get_paragraph to discover available form fields and their current values.

    Chart/visualization settings (config) are automatically saved before
    execution and restored afterward, because Zeppelin resets the config
    object on re-execution.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to run
        params: Optional dict of dynamic form values, e.g. {"city": "Seoul", "limit": "10"}.
            Keys must match the form field names defined in the paragraph.
    """
    try:
        saved = await _save_paragraph_state(notebook_id, paragraph_id)

        body: dict[str, Any] | None = None
        if params:
            body = {"params": params}
        data = await zeppelin.request(
            "POST", f"/api/notebook/run/{notebook_id}/{paragraph_id}", json=body
        )

        if saved is not None:
            await asyncio.sleep(0.5)
            await _restore_paragraph_config(notebook_id, paragraph_id, saved)

        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        resp_body = data.get("body", {})
        code = resp_body.get("code", "UNKNOWN")
        lines = [f"Status: {code}"]
        msgs = resp_body.get("msg", [])
        for msg in msgs:
            msg_data = msg.get("data", "").strip()
            if msg_data:
                lines.append(f"Output ({msg.get('type', 'TEXT')}):\n{msg_data}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error running paragraph: {e}"


@mcp.tool()
async def run_all_paragraphs(
    notebook_id: str,
    params: Optional[dict[str, Any]] = None,
) -> str:
    """Run all paragraphs in a notebook and wait for completion.

    Supports dynamic forms — pass params to set form values for the entire notebook
    before execution.

    Chart/visualization settings (config) for every paragraph are automatically
    saved before execution and restored after all paragraphs finish.

    Args:
        notebook_id: The notebook ID to run
        params: Optional dict of dynamic form values, e.g. {"city": "Seoul", "limit": "10"}.
            Keys must match the form field names defined in the notebook.
    """
    try:
        nb_data = await zeppelin.request("GET", f"/api/notebook/{notebook_id}")
        saved_paragraphs: list[dict] = []
        if nb_data.get("status") == "OK":
            saved_paragraphs = nb_data.get("body", {}).get("paragraphs", [])

        body: dict[str, Any] | None = None
        if params:
            body = {"params": params}
        data = await zeppelin.request(
            "POST", f"/api/notebook/job/{notebook_id}", json=body
        )
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"

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
    except Exception as e:
        return f"Error running all paragraphs: {e}"


@mcp.tool()
async def get_paragraph_status(notebook_id: str, paragraph_id: str) -> str:
    """Get execution status of a specific paragraph (useful after async run).

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to check
    """
    try:
        data = await zeppelin.request(
            "GET", f"/api/notebook/job/{notebook_id}/{paragraph_id}"
        )
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        body = data.get("body", {})
        status = body.get("status", "UNKNOWN")
        started = body.get("started", "N/A")
        finished = body.get("finished", "N/A")
        progress = body.get("progress", "N/A")
        return (
            f"Status: {status}\n"
            f"Started: {started}\n"
            f"Finished: {finished}\n"
            f"Progress: {progress}"
        )
    except Exception as e:
        return f"Error getting paragraph status: {e}"


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
