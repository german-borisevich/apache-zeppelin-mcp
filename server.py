import os
import logging
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP, Context

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
async def list_notebooks(ctx: Context) -> str:
    """List all notebooks on the Zeppelin server."""
    try:
        data = await zeppelin.request("GET", "/api/notebook")
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        notebooks = data.get("body", [])
        if not notebooks:
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
            header = r.get("header", "")
            snippet = r.get("snippet", "").replace("\n", " ")[:200]
            lines.append(
                f"- Notebook: {r.get('notebookName', 'N/A')} | "
                f"Paragraph: {r.get('id', 'N/A')} | "
                f"Header: {header} | "
                f"Snippet: {snippet}"
            )
        return f"Found {len(results)} results for '{query}':\n" + "\n".join(lines)
    except Exception as e:
        return f"Error searching notebooks: {e}"


@mcp.tool()
async def get_notebook(notebook_id: str) -> str:
    """Get full notebook details including all paragraphs and their content.

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


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.splitlines())


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
async def run_paragraph(notebook_id: str, paragraph_id: str) -> str:
    """Run a paragraph synchronously and return the result.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to run
    """
    try:
        data = await zeppelin.request(
            "POST", f"/api/notebook/run/{notebook_id}/{paragraph_id}"
        )
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        body = data.get("body", {})
        code = body.get("code", "UNKNOWN")
        lines = [f"Status: {code}"]
        msgs = body.get("msg", [])
        for msg in msgs:
            msg_data = msg.get("data", "").strip()
            if msg_data:
                lines.append(f"Output ({msg.get('type', 'TEXT')}):\n{msg_data}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error running paragraph: {e}"


@mcp.tool()
async def run_all_paragraphs(notebook_id: str) -> str:
    """Run all paragraphs in a notebook asynchronously.

    Args:
        notebook_id: The notebook ID to run
    """
    try:
        data = await zeppelin.request(
            "POST", f"/api/notebook/job/{notebook_id}"
        )
        if data.get("status") != "OK":
            return f"Error: {data.get('message', 'Unknown error')}"
        return f"Triggered execution of all paragraphs in notebook {notebook_id}. Use get_paragraph_status to check progress."
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
