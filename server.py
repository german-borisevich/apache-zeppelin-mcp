import asyncio
import functools
import hashlib
import html
import json
import os
import logging
import re
import sys
import time
from datetime import datetime, timezone
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("zeppelin-mcp")

ZEPPELIN_BASE_URL = os.environ.get("ZEPPELIN_BASE_URL", "")
ZEPPELIN_USERNAME = os.environ.get("ZEPPELIN_USERNAME", "")
ZEPPELIN_PASSWORD = os.environ.get("ZEPPELIN_PASSWORD", "")

MAX_OUTPUT_CHARS = int(os.environ.get("ZEPPELIN_MAX_OUTPUT_CHARS", "50000"))


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
                raise ToolError(str(e)) from e
            except ToolError:
                raise
            except httpx.HTTPStatusError as e:
                logger.error("HTTP error %s: %s", operation, e, exc_info=True)
                raise ToolError(f"Error {operation}: HTTP {e.response.status_code}") from e
            except Exception as e:
                logger.error("Error %s: %s", operation, e, exc_info=True)
                detail = str(e).replace("\n", " ").strip()
                if len(detail) > 300:
                    detail = detail[:300] + "…"
                msg = f"Error {operation}: {type(e).__name__}"
                if detail:
                    msg += f": {detail}"
                raise ToolError(msg) from e
        return wrapper
    return decorator


_SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _validate_id(value: str, name: str) -> str:
    if not value or not _SAFE_ID_PATTERN.match(value):
        raise ToolError(f"Invalid {name}: must contain only alphanumeric, hyphens, or underscores")
    return value


def _truncate(text: str, limit: int = MAX_OUTPUT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n... Output truncated ({len(text)} chars, limit {limit})"


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.splitlines())


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode HTML entities for plain-text output."""
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _limit_table_rows(text: str, max_rows: int = 50) -> str:
    """Limit tab-separated table output to header + max_rows data rows."""
    lines = text.split("\n")
    # Heuristic: if first line contains tabs, it's likely a TSV table
    if not lines or "\t" not in lines[0]:
        return text
    data_lines = [l for l in lines if l.strip()]
    if len(data_lines) <= max_rows + 1:  # +1 for header
        return text
    limited = data_lines[:max_rows + 1]
    total = len(data_lines) - 1  # exclude header
    limited.append(f"\n... ({max_rows} of {total} rows shown)")
    return "\n".join(limited)


def _format_messages(msgs: list[dict], indent: int = 0, prefix: str = "", label: str = "Output",
                     include_html: bool = True, limit_rows: int = 0) -> list[str]:
    lines = []
    for msg in msgs:
        msg_type = msg.get("type", "TEXT")
        msg_data = msg.get("data", "").strip()
        if not msg_data:
            continue
        if msg_type == "HTML":
            if not include_html:
                lines.append(f"{prefix}{label}: [Visualization output omitted]")
                continue
            msg_data = _strip_html(msg_data)
            if not msg_data:
                continue
        if limit_rows > 0 and msg_type in ("TEXT", "TABLE"):
            msg_data = _limit_table_rows(msg_data, max_rows=limit_rows)
        text = _indent(msg_data, indent) if indent else msg_data
        lines.append(f"{prefix}{label} ({msg_type}):\n{text}")
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


def _format_config(paragraph: dict) -> list[str]:
    """Format paragraph visualization and presentation config for display."""
    config = paragraph.get("config", {})

    # Result-level graph is what the UI actually renders — check it first
    result_graph = {}
    results = config.get("results")
    if isinstance(results, list) and results:
        result_graph = results[0].get("graph", {})
    elif isinstance(results, dict):
        first = next(iter(results.values()), {})
        if isinstance(first, dict):
            result_graph = first.get("graph", {})

    top_graph = config.get("graph", {})
    graph = result_graph or top_graph

    lines = []
    if graph:
        lines.append("Visualization:")
        mode = graph.get("mode", "table")
        lines.append(f"  chart type: {mode}")
        if graph.get("keys"):
            lines.append(f"  keys: {[k['name'] for k in graph['keys']]}")
        if graph.get("groups"):
            lines.append(f"  groups: {[g['name'] for g in graph['groups']]}")
        if graph.get("values"):
            lines.append(f"  values: {[v['name'] for v in graph['values']]}")
        col_width = config.get("colWidth")
        if col_width and col_width != 12:
            lines.append(f"  colWidth: {col_width}")

        # Warn if top-level and result-level configs are out of sync
        if result_graph and top_graph:
            def _col_names(g, field):
                return sorted(c.get("name", "") for c in g.get(field, []))
            for field in ("keys", "groups", "values"):
                if _col_names(result_graph, field) != _col_names(top_graph, field):
                    lines.append(f"  ⚠ WARNING: chart settings out of sync between config.graph and config.results — UI uses result-level config")
                    break

    # Presentation flags — always emitted so their absence is visible too
    editor_hide = config.get("editorHide")
    run_on_change = config.get("runOnSelectionChange")
    lines.append("Presentation:")
    lines.append(
        f"  editorHide: {'not set' if editor_hide is None else str(editor_hide).lower()}"
        + ("" if editor_hide else " (code editor visible to readers)")
    )
    lines.append(
        f"  runOnSelectionChange: {'not set' if run_on_change is None else str(run_on_change).lower()}"
        + ("" if run_on_change is False else " (form changes auto-run the paragraph)")
    )
    return lines


def _presentation_markers(paragraph: dict) -> str:
    """Compact deviation markers for list_paragraphs — non-empty only when a
    presentation flag deviates from the finalized-paragraph standard
    (editorHide: true; runOnSelectionChange: false on paragraphs with forms)."""
    config = paragraph.get("config", {})
    markers = []
    if not config.get("editorHide"):
        markers.append("editor visible")
    forms = (paragraph.get("settings") or {}).get("forms") or {}
    if forms and config.get("runOnSelectionChange") is not False:
        markers.append("runs on selection change")
    return f" [{', '.join(markers)}]" if markers else ""


def _build_params_body(params: Optional[dict[str, Any]]) -> dict[str, Any] | None:
    if params:
        return {"params": params}
    return None


async def _save_paragraph_state(
    zeppelin: "ZeppelinClient", notebook_id: str, paragraph_id: str
) -> dict | None:
    """Fetch current paragraph data (for backup, config merge, or form update)."""
    try:
        data = _check_status(await zeppelin.request(
            "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
        ))
        p = data.get("body", {})
        logger.debug(
            "Saved state for paragraph %s, config keys: %s",
            paragraph_id, list(p.get("config", {}).keys()),
        )
        return p
    except Exception:
        logger.warning("Failed to save state for paragraph %s", paragraph_id, exc_info=True)
        return None


async def _wait_for_notebook_completion(
    zeppelin: "ZeppelinClient",
    notebook_id: str,
    ctx: Context | None = None,
    timeout: float = 600.0,
    poll_interval: float = 2.0,
) -> bool:
    """Poll notebook job status until all paragraphs finish or timeout."""
    deadline = time.monotonic() + timeout
    while True:
        elapsed = timeout - (deadline - time.monotonic())
        if time.monotonic() >= deadline:
            break
        try:
            data = _check_status(await zeppelin.request(
                "GET", f"/api/notebook/job/{notebook_id}"
            ))
            paragraphs = data.get("body", [])
            if not paragraphs or not any(
                p.get("status") in ("RUNNING", "PENDING", "READY")
                for p in paragraphs
            ):
                if ctx:
                    await ctx.report_progress(elapsed, timeout)
                return True
            if ctx:
                await ctx.report_progress(elapsed, timeout)
        except Exception:
            logger.warning("Error polling notebook %s status", notebook_id, exc_info=True)
        await asyncio.sleep(poll_interval)
    logger.warning("Timeout waiting for notebook %s after %.0fs", notebook_id, timeout)
    return False


async def _wait_for_paragraph_completion(
    zeppelin: "ZeppelinClient",
    notebook_id: str,
    paragraph_id: str,
    ctx: Context | None = None,
    timeout: float = 600.0,
    poll_interval: float = 2.0,
) -> dict:
    """Poll paragraph job status until it finishes or timeout. Returns status body."""
    deadline = time.monotonic() + timeout
    while True:
        elapsed = timeout - (deadline - time.monotonic())
        if time.monotonic() >= deadline:
            break
        try:
            data = _check_status(await zeppelin.request(
                "GET", f"/api/notebook/job/{notebook_id}/{paragraph_id}"
            ))
            body = data.get("body", {})
            status = body.get("status", "")
            if status not in ("RUNNING", "PENDING", "READY"):
                if ctx:
                    await ctx.report_progress(elapsed, timeout)
                return body
            if ctx:
                await ctx.report_progress(elapsed, timeout)
        except Exception:
            logger.warning("Error polling paragraph %s status", paragraph_id, exc_info=True)
        await asyncio.sleep(poll_interval)
    logger.warning("Timeout waiting for paragraph %s after %.0fs", paragraph_id, timeout)
    return {"status": "TIMEOUT"}


def _get_zeppelin(ctx: Context) -> "ZeppelinClient":
    """Extract ZeppelinClient from the lifespan context."""
    return ctx.request_context.lifespan_context.zeppelin


async def _get_notebook_path(zeppelin: "ZeppelinClient", notebook_id: str) -> str:
    data = _check_status(await zeppelin.request("GET", f"/api/notebook/{notebook_id}"))
    return data.get("body", {}).get("name", "")


# Both caches store (value, cached_at_monotonic). Entries expire after
# _CACHE_TTL_SECONDS so a notebook renamed/moved outside this server (e.g. via
# the Zeppelin UI) can't satisfy the ~Backups guard with a stale path forever,
# and the caches are wiped at _CACHE_MAX_ENTRIES so they can't grow unbounded.
# Correctness of the backup guard matters more than cache hit rate.
_CACHE_TTL_SECONDS = 300.0
_CACHE_MAX_ENTRIES = 512

_notebook_path_cache: dict[str, tuple[str, float]] = {}


def _cache_get(cache: dict[str, tuple[str, float]], key: str) -> Optional[str]:
    entry = cache.get(key)
    if entry is None:
        return None
    value, cached_at = entry
    if time.monotonic() - cached_at >= _CACHE_TTL_SECONDS:
        del cache[key]
        return None
    return value


def _cache_put(cache: dict[str, tuple[str, float]], key: str, value: str) -> None:
    if len(cache) >= _CACHE_MAX_ENTRIES:
        cache.clear()
    cache[key] = (value, time.monotonic())


async def _check_backup_protection(zeppelin: "ZeppelinClient", notebook_id: str) -> str:
    path = _cache_get(_notebook_path_cache, notebook_id)
    if path is None:
        path = await _get_notebook_path(zeppelin, notebook_id)
        _cache_put(_notebook_path_cache, notebook_id, path)
    if "/~Backups/" in path or path.startswith("~Backups/"):
        raise ToolError("Cannot modify notebooks in ~Backups — these are protected backup notebooks")
    return path


_backup_notebook_id_cache: dict[str, tuple[str, float]] = {}


async def _backup_paragraph(
    zeppelin: "ZeppelinClient", notebook_id: str, notebook_path: str,
    paragraph_id: str, paragraph_data: dict, operation: str = "EDIT",
) -> None:
    clean_path = notebook_path.lstrip("/")
    parent = clean_path.rsplit("/", 1)[0] if "/" in clean_path else ""
    notebook_name = clean_path.rsplit("/", 1)[-1]
    backup_name = f"{notebook_name}_{notebook_id}_backup"
    if parent:
        backup_path = f"Users/{ZEPPELIN_USERNAME}/~Backups/{parent}/{backup_name}"
    else:
        backup_path = f"Users/{ZEPPELIN_USERNAME}/~Backups/{backup_name}"

    # Find or create backup notebook (with cache)
    backup_notebook_id = _cache_get(_backup_notebook_id_cache, backup_path)

    if not backup_notebook_id:
        data = _check_status(await zeppelin.request("GET", "/api/notebook"))
        for nb in data.get("body", []):
            nb_path = nb.get("path", nb.get("name", ""))
            # Normalize: path may have leading /
            if nb_path.lstrip("/") == backup_path:
                backup_notebook_id = nb.get("id")
                break

    if not backup_notebook_id:
        create_data = _check_status(
            await zeppelin.request("POST", "/api/notebook", json={"name": backup_path})
        )
        backup_notebook_id = create_data.get("body")
        if not backup_notebook_id:
            raise ToolError("Failed to create backup notebook")

    _cache_put(_backup_notebook_id_cache, backup_path, backup_notebook_id)

    # Build backup paragraph
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    old_title = paragraph_data.get("title", "")
    if old_title:
        backup_title = f"[{timestamp} | {operation}] {old_title} ({paragraph_id})"
    else:
        backup_title = f"[{timestamp} | {operation}] {paragraph_id}"

    old_text = paragraph_data.get("text", "")

    body: dict[str, Any] = {"text": old_text, "title": backup_title}
    result = _check_status(
        await zeppelin.request("POST", f"/api/notebook/{backup_notebook_id}/paragraph", json=body)
    )
    backup_para_id = result.get("body")
    if not backup_para_id:
        raise ToolError("Failed to create backup paragraph")

    # Make title visible (PUT config merges, so we only need to send the title field)
    try:
        await zeppelin.request(
            "PUT",
            f"/api/notebook/{backup_notebook_id}/paragraph/{backup_para_id}/config",
            json={"title": True},
        )
    except Exception:
        logger.warning("Failed to set title visibility for backup paragraph %s", backup_para_id, exc_info=True)


# ---------------------------------------------------------------------------
# Batch operations — input models, helpers, and per-item logic
#
# The singular paragraph tools and their batch siblings share the same core
# logic via the _apply_* / _run_and_collect helpers below, so behavior stays
# identical. Batch tools call these in a sequential loop (see plan: sequential
# avoids racing on the shared backup-notebook cache and preserves order).
# ---------------------------------------------------------------------------

class ParagraphUpdate(BaseModel):
    """One paragraph edit for batch_update_paragraph."""
    paragraph_id: str
    text: str
    title: Optional[str] = None


class NewParagraph(BaseModel):
    """One paragraph to create for batch_add_paragraph."""
    text: str
    title: Optional[str] = None
    index: Optional[int] = None


class ParagraphConfig(BaseModel):
    """One paragraph config change for batch_update_paragraph_config."""
    paragraph_id: str
    config: dict[str, Any]


class ParagraphRun(BaseModel):
    """One paragraph to run for batch_run_paragraph."""
    paragraph_id: str
    params: Optional[dict[str, Any]] = None


# Paragraph run-result codes that count as a failure for stop_on_error.
_RUN_FAILURE_CODES = {"ERROR", "ABORT", "TIMEOUT"}


def _short_error(e: Exception, limit: int = 200) -> str:
    """Compact, single-line reason string for per-item batch failures."""
    msg = (str(e) or type(e).__name__).replace("\n", " ").strip()
    return msg if len(msg) <= limit else msg[:limit] + "…"


def _format_batch_result(verb: str, succeeded: int, total: int, failures: list[str]) -> str:
    """Render '<verb> N/M paragraphs.' plus an optional failure list."""
    line = f"{verb} {succeeded}/{total} paragraphs."
    if failures:
        line += "\nFailed:\n" + "\n".join(f"- {f}" for f in failures)
    return line


async def _apply_paragraph_update(
    zeppelin: "ZeppelinClient", notebook_id: str, notebook_path: str,
    paragraph_id: str, text: str, title: Optional[str] = None,
) -> None:
    """Core of update_paragraph: backup-on-change, then PUT text/title."""
    saved = await _save_paragraph_state(zeppelin, notebook_id, paragraph_id)
    if saved is None:
        raise ToolError(f"Could not fetch paragraph {paragraph_id}")

    old_text = saved.get("text", "")
    if old_text != text:
        await _backup_paragraph(zeppelin, notebook_id, notebook_path, paragraph_id, saved, "EDIT")

    body: dict[str, Any] = {"text": text}
    if title is not None:
        body["title"] = title
    await zeppelin.request(
        "PUT", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}", json=body
    )

    if title is not None:
        try:
            await zeppelin.request(
                "PUT",
                f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}/config",
                json={"title": True},
            )
        except Exception:
            logger.warning("Failed to set title visibility for %s", paragraph_id, exc_info=True)


async def _apply_paragraph_delete(
    zeppelin: "ZeppelinClient", notebook_id: str, notebook_path: str, paragraph_id: str,
) -> None:
    """Core of delete_paragraph: backup then DELETE."""
    saved = await _save_paragraph_state(zeppelin, notebook_id, paragraph_id)
    if saved is None:
        raise ToolError(f"Could not fetch paragraph {paragraph_id}")
    await _backup_paragraph(zeppelin, notebook_id, notebook_path, paragraph_id, saved, "DELETE")
    _check_status(await zeppelin.request(
        "DELETE", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
    ))


async def _apply_paragraph_add(
    zeppelin: "ZeppelinClient", notebook_id: str,
    text: str, title: Optional[str] = None, index: Optional[int] = None,
) -> str:
    """Core of add_paragraph: POST a new paragraph, return its id."""
    body: dict[str, Any] = {"text": text}
    if title is not None:
        body["title"] = title
    if index is not None:
        body["index"] = index
    data = _check_status(await zeppelin.request(
        "POST", f"/api/notebook/{notebook_id}/paragraph", json=body
    ))
    paragraph_id = data.get("body", "unknown")

    if title is not None and paragraph_id != "unknown":
        try:
            await zeppelin.request(
                "PUT",
                f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}/config",
                json={"title": True},
            )
        except Exception:
            logger.warning("Failed to set title visibility for %s", paragraph_id, exc_info=True)
    return paragraph_id


async def _apply_paragraph_config(
    zeppelin: "ZeppelinClient", notebook_id: str, paragraph_id: str, config: dict[str, Any],
) -> dict[str, Any]:
    """Core of update_paragraph_config: deep-merge config (auto-filling column
    index/aggr from output headers) and PUT it. Returns the merged config."""
    saved = await _save_paragraph_state(zeppelin, notebook_id, paragraph_id)
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
            if results:
                for result_data in results.values():
                    if isinstance(result_data, dict) and "graph" in result_data:
                        merged_result_graph = {**result_data["graph"], **user_graph}
                        # Ensure mode is consistent with top-level
                        if "mode" not in user_graph and "mode" in merged_graph:
                            merged_result_graph["mode"] = merged_graph["mode"]
                        result_data["graph"] = merged_result_graph
            else:
                # No results entries — create one with the merged graph config
                config["results"] = {"0": {"graph": {**merged_graph}}}
        else:
            config = {**current_config, **config}

    await zeppelin.request(
        "PUT",
        f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}/config",
        json=config,
    )
    return config


async def _run_and_collect(
    zeppelin: "ZeppelinClient", notebook_id: str, paragraph_id: str,
    params: Optional[dict[str, Any]], max_rows: int, include_html: bool,
    ctx: Context | None = None,
) -> tuple[str, list[str]]:
    """Core of run_paragraph: start the job, wait, fetch results.
    Returns (status_code, formatted_output_lines)."""
    _check_status(await zeppelin.request(
        "POST", f"/api/notebook/job/{notebook_id}/{paragraph_id}",
        json=_build_params_body(params),
    ))

    job_body = await _wait_for_paragraph_completion(
        zeppelin, notebook_id, paragraph_id, ctx=ctx, timeout=600.0,
    )
    job_status = job_body.get("status", "UNKNOWN")

    # Fetch full paragraph to get results
    data = _check_status(await zeppelin.request(
        "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
    ))
    p = data.get("body", {})
    results = p.get("results", {})
    code = results.get("code", job_status)

    out_lines: list[str] = []
    if results.get("msg"):
        out_lines.extend(_format_messages(results["msg"], include_html=include_html, limit_rows=max_rows))
    return code, out_lines


# ---------------------------------------------------------------------------
# Notebook fingerprinting
#
# Deterministic content hash over the *stable* parts of a notebook, computed
# server-side so agents can check freshness of cached notebook docs with one
# cheap call instead of re-reading the whole notebook.
#
# Only id, title, and code text are hashed. Everything else is excluded as
# volatile: execution status, results/output, run timestamps, form values,
# form *definitions* (re-registered on every run, options may derive from
# query results), and chart configs (Zeppelin sync quirks mutate them without
# a real edit). Paragraph ORDER is also excluded — a reorder alone does not
# flag the notebook stale.
# ---------------------------------------------------------------------------


def _canonical_hash(obj: Any) -> str:
    """sha256 over the canonical JSON serialization of `obj`."""
    encoded = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _paragraph_fingerprint_unit(paragraph: dict) -> dict[str, Any]:
    """Whitelist the stable fields of one paragraph: id, title, code text."""
    return {
        "id": paragraph.get("id"),
        "title": paragraph.get("title") or "",
        "text": (paragraph.get("text") or "").strip(),
    }


def _notebook_fingerprint(notebook_id: str, paragraphs: list[dict]) -> dict[str, Any]:
    """Per-paragraph sub-hashes plus an order-insensitive overall fingerprint."""
    units: dict[str, str] = {}
    for i, p in enumerate(paragraphs):
        if not isinstance(p, dict):
            continue
        pid = p.get("id") or f"paragraph-{i}"
        units[pid] = _canonical_hash(_paragraph_fingerprint_unit(p))
    parts = [notebook_id] + sorted(f"{pid}:{h}" for pid, h in units.items())
    overall = "sha256:" + hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return {"fingerprint": overall, "units": units}


def _fingerprint_diff(current: dict[str, str], known: dict[str, str]) -> dict[str, Any]:
    """Exact unit-level diff between the live fingerprint map and a stored one."""
    return {
        "changed": sorted(k for k in current if k in known and current[k] != known[k]),
        "added": sorted(k for k in current if k not in known),
        "removed": sorted(k for k in known if k not in current),
    }


def _parse_spec_frontmatter(text: str) -> dict[str, Any]:
    """Extract stored fingerprint / units / updated_at from a cached-doc spec.

    Tolerant regex parser: the frontmatter block is machine-written in a fixed
    shape (artifact doc cache protocol §3), so a YAML dependency isn't needed.
    Returns {} when nothing usable is found — never raises.
    """
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end < 0:
        return {}
    fm = text[3:end]
    out: dict[str, Any] = {}
    for field in ("fingerprint", "updated_at"):
        # Capture to end of line minus trailing comment — values may contain
        # spaces (e.g. "updated_at: 2026-06-19 12:34:56").
        m = re.search(rf"^{field}:[ \t]*([^#\n]+)", fm, re.MULTILINE)
        if m:
            value = m.group(1).strip()
            if value:
                out[field] = value
    um = re.search(r"^units:[^\n]*\n((?:[ \t]+[^\n]*\n?)*)", fm, re.MULTILINE)
    if um:
        units: dict[str, str] = {}
        for line in um.group(1).splitlines():
            lm = re.match(r"^[ \t]+([^\s:#]+):\s*([^\s#]+)", line)
            if lm:
                units[lm.group(1)] = lm.group(2)
        if units:
            out["units"] = units
    return out


def _load_spec_frontmatter(spec_path: str) -> tuple[dict[str, Any], Optional[str]]:
    """Read stored hashes from a spec file inside $WIKI_DOCS_PATH.

    Returns (frontmatter, note): on any problem the frontmatter is {} and the
    note says why — the caller degrades to "stored hashes unknown" instead of
    failing the freshness check. The only hard error is a path escaping the
    docs root (that is a caller bug, not an environment condition).
    """
    docs_root = os.environ.get("WIKI_DOCS_PATH", "").strip()
    if not docs_root:
        return {}, "WIKI_DOCS_PATH is not set — spec_path ignored"
    root = os.path.realpath(os.path.expanduser(docs_root))
    path = os.path.realpath(os.path.expanduser(spec_path))
    if not path.startswith(root + os.sep):
        raise ToolError("spec_path must point inside $WIKI_DOCS_PATH")
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return {}, "spec file unreadable — treating stored hashes as unknown"
    fm = _parse_spec_frontmatter(text)
    if not (fm.get("fingerprint") or fm.get("units")):
        return {}, "no stored hashes in spec frontmatter — treating as unknown"
    return fm, None


# ---------------------------------------------------------------------------
# Zeppelin client
# ---------------------------------------------------------------------------

# Retry policy for transient failures: Zeppelin returns 503 and drops
# connections when overloaded/restarting. Module-level so tests can patch the
# backoff to zero.
REQUEST_RETRY_ATTEMPTS = 3
REQUEST_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 3.0)
_RETRYABLE_EXCEPTIONS = (httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError)


class ZeppelinClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.client = httpx.AsyncClient(timeout=30.0)
        self._authenticated = False

    async def login(self) -> None:
        self.client.cookies.clear()
        resp = await self.client.post(
            f"{self.base_url}/api/login",
            data={"userName": self.username, "password": self.password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        self._authenticated = True
        logger.info("Authenticated with Zeppelin")

    async def request(
        self, method: str, path: str, json: Any = None, params: dict | None = None,
        timeout: float | None = None,
    ) -> dict:
        if not self._authenticated:
            await self.login()

        url = f"{self.base_url}{path}"
        kw: dict[str, Any] = {"json": json, "params": params}
        if timeout is not None:
            kw["timeout"] = httpx.Timeout(timeout)

        for attempt in range(REQUEST_RETRY_ATTEMPTS):
            last_attempt = attempt == REQUEST_RETRY_ATTEMPTS - 1
            backoff = REQUEST_RETRY_BACKOFF_SECONDS[
                min(attempt, len(REQUEST_RETRY_BACKOFF_SECONDS) - 1)
            ]
            try:
                resp = await self.client.request(method, url, **kw)
            except _RETRYABLE_EXCEPTIONS as e:
                if last_attempt:
                    raise
                logger.warning(
                    "Transient %s on %s %s — retrying in %.0fs",
                    type(e).__name__, method, path, backoff,
                )
                await asyncio.sleep(backoff)
                continue
            if resp.status_code == 503 and not last_attempt:
                logger.warning(
                    "Zeppelin returned 503 for %s %s (overloaded or restarting) — retrying in %.0fs",
                    method, path, backoff,
                )
                await asyncio.sleep(backoff)
                continue
            break

        if resp.status_code in (401, 403) or resp.is_redirect:
            logger.info("Session expired (HTTP %s), re-authenticating", resp.status_code)
            await self.login()
            resp = await self.client.request(method, url, **kw)
            if resp.is_redirect:
                raise ZeppelinAPIError(
                    "Zeppelin keeps redirecting to the login page — authentication failed "
                    "or the server is restarting; wait and retry"
                )

        resp.raise_for_status()
        return resp.json()

    async def close(self) -> None:
        await self.client.aclose()


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@dataclass
class AppContext:
    zeppelin: ZeppelinClient


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    if not ZEPPELIN_BASE_URL:
        raise ValueError("ZEPPELIN_BASE_URL environment variable is required")
    if not ZEPPELIN_USERNAME:
        raise ValueError("ZEPPELIN_USERNAME environment variable is required")
    if not ZEPPELIN_PASSWORD:
        raise ValueError("ZEPPELIN_PASSWORD environment variable is required")
    client = ZeppelinClient(ZEPPELIN_BASE_URL, ZEPPELIN_USERNAME, ZEPPELIN_PASSWORD)
    try:
        await client.login()
        yield AppContext(zeppelin=client)
    finally:
        await client.close()


mcp = FastMCP("zeppelin", lifespan=app_lifespan)
mcp._mcp_server.version = "0.1.0"


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("listing notebooks")
async def list_notebooks(ctx: Context, name_filter: Optional[str] = None, limit: int = 100) -> str:
    """List notebooks on the Zeppelin server.

    Always use name_filter to narrow results — the server may have thousands of notebooks.

    Args:
        name_filter: Optional substring filter (case-insensitive) matched against the full path.
        limit: Maximum number of notebooks to return (default 100, 0 = unlimited).
    """
    zeppelin = _get_zeppelin(ctx)
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
    total = len(notebooks)
    if limit > 0 and total > limit:
        notebooks = notebooks[:limit]
    lines = [f"- {nb.get('id', 'N/A')}: {nb.get('path', nb.get('name', 'N/A'))}" for nb in notebooks]
    header = f"Found {total} notebooks"
    if limit > 0 and total > limit:
        header += f" (showing first {limit}, use name_filter to narrow)"
    return header + ":\n" + "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("searching notebooks")
async def search_notebooks(ctx: Context, query: str, max_results: int = 20) -> str:
    """Full-text search across all notebook paragraphs.

    Args:
        query: Search query string
        max_results: Maximum number of results to return (default 20, 0 = unlimited).
    """
    if not query or not query.strip():
        raise ToolError("Search query must not be empty")
    if len(query) > 1000:
        raise ToolError("Search query too long (max 1000 characters)")
    zeppelin = _get_zeppelin(ctx)
    data = _check_status(await zeppelin.request("GET", "/api/notebook/search", params={"q": query}))
    results = data.get("body", [])
    if not results:
        return f"No results found for '{query}'."
    total = len(results)
    if max_results > 0 and total > max_results:
        results = results[:max_results]
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
    header_line = f"Found {total} results for '{query}'"
    if max_results > 0 and total > max_results:
        header_line += f" (showing first {max_results})"
    return header_line + ":\n" + "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("getting notebook")
async def get_notebook(ctx: Context, notebook_id: str, include_config: bool = False) -> str:
    """Get notebook overview with all paragraph code, titles, and status.
    Does not include paragraph output — use get_paragraph to inspect output of specific paragraphs.

    For large notebooks, prefer list_paragraphs first to identify paragraphs of interest,
    then get_paragraph_code for targeted reads.

    Args:
        notebook_id: The notebook ID to retrieve
        include_config: If True, include visualization/chart config and presentation
            flags (editorHide, runOnSelectionChange) for each paragraph. Default False.
    """
    _validate_id(notebook_id, "notebook_id")
    zeppelin = _get_zeppelin(ctx)
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
        if include_config:
            config_lines = _format_config(p)
            if config_lines:
                for cl in config_lines:
                    lines.append(f"  {cl}")
        lines.append("")
    return "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("listing paragraphs")
async def list_paragraphs(ctx: Context, notebook_id: str) -> str:
    """List paragraph metadata (index, id, title, status) without code or output.

    Paragraphs whose presentation config deviates from the finalized standard are
    marked inline: [editor visible] when editorHide is not true, and
    [runs on selection change] when the paragraph has forms but
    runOnSelectionChange is not false. Unmarked paragraphs are properly finalized.

    Args:
        notebook_id: The notebook ID to list paragraphs for
    """
    _validate_id(notebook_id, "notebook_id")
    zeppelin = _get_zeppelin(ctx)
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
        markers = _presentation_markers(p)
        lines.append(f"[{i}] {pid} - {label} (status: {status}){markers}")
    return "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("getting paragraph")
async def get_paragraph(
    ctx: Context, notebook_id: str, paragraph_id: str,
    max_rows: int = 50, include_html: bool = False,
) -> str:
    """Get full content of a single paragraph (code, output, dynamic forms,
    chart config, and presentation flags like editorHide / runOnSelectionChange).

    By default, table output is limited to 50 rows and HTML output is omitted to save tokens.
    When investigating data discrepancies, set max_rows=0 for unlimited rows.
    If the paragraph uses HTML rendering (without z.show()), set include_html=True to see
    text extracted from HTML. Alternatively, query the underlying data via SQL if available.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to retrieve
        max_rows: Maximum data rows for table output (default 50, 0 = unlimited). Header row always included.
        include_html: If True, include HTML output converted to plain text. If False (default), HTML is omitted.
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
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
    lines.extend(_format_config(p))
    results = p.get("results", {})
    if results and results.get("msg"):
        lines.extend(_format_messages(results["msg"], indent=2, include_html=include_html, limit_rows=max_rows))
    return _truncate("\n".join(lines))


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("getting paragraph code")
async def get_paragraph_code(ctx: Context, notebook_id: str, paragraph_id: str) -> str:
    """Get only the code/text content of a paragraph, without output or forms.
    Use this instead of get_paragraph when you only need to read the code to save on output size.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to retrieve
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    data = _check_status(await zeppelin.request(
        "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
    ))
    p = data.get("body", {})
    text = p.get("text", "")
    if not text:
        return f"Paragraph {paragraph_id} has no code."
    return text


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("getting paragraph forms")
async def get_paragraph_forms(ctx: Context, notebook_id: str, paragraph_id: str) -> str:
    """Get dynamic form definitions and current parameter values for a paragraph.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to inspect
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    data = _check_status(await zeppelin.request(
        "GET", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}"
    ))
    p = data.get("body", {})
    form_lines = _format_forms(p)
    if not form_lines:
        return f"Paragraph {paragraph_id} has no dynamic forms."
    return "\n".join(form_lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("updating paragraph forms")
async def update_paragraph_forms(
    ctx: Context, notebook_id: str, paragraph_id: str, params: dict[str, Any]
) -> str:
    """Update dynamic form values without re-executing the paragraph.
    Safest way to change form parameters when chart settings must be preserved.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to update
        params: Dict of form values to set, e.g. {"city": "Seoul", "limit": "10"}.
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)
    saved = await _save_paragraph_state(zeppelin, notebook_id, paragraph_id)
    if saved is None:
        raise ToolError(f"Could not fetch paragraph {paragraph_id}")

    settings = saved.get("settings", {})
    existing_params = settings.get("params", {})
    existing_params.update(params)
    settings["params"] = existing_params

    body: dict[str, Any] = {
        "text": saved.get("text", ""),
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


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("updating paragraph config")
async def update_paragraph_config(
    ctx: Context,
    notebook_id: str,
    paragraph_id: str,
    config: dict[str, Any],
) -> str:
    """Update paragraph visualization/chart config (graph type, columns, display settings).
    Fetches current config and deep-merges provided changes.
    Column index and aggr fields are auto-filled from the paragraph's output headers — only provide name.
    Auto-fill needs saved output: run the paragraph at least once first, otherwise index/aggr
    cannot be auto-filled and columns are passed through as given.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to configure
        config: Dict of config fields to set or update. Merged with existing config.
            To change chart type, set graph.mode to one of:
            "table", "multiBarChart", "stackedAreaChart", "lineChart",
            "pieChart", "scatterChart".
            Example: {"graph": {"mode": "multiBarChart",
                                "keys": [{"name": "date_col"}],
                                "values": [{"name": "amount_col"}]}}
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)
    config = await _apply_paragraph_config(zeppelin, notebook_id, paragraph_id, config)
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


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("updating paragraphs config")
async def batch_update_paragraph_config(
    ctx: Context,
    notebook_id: str,
    configs: list[ParagraphConfig],
) -> str:
    """Update visualization/chart config for multiple paragraphs in one call (batch version of
    update_paragraph_config). Each config is deep-merged with that paragraph's existing config.
    Column index/aggr auto-fill needs saved output: run each paragraph at least once first,
    otherwise index/aggr cannot be auto-filled and columns are passed through as given.

    Prefer this over many update_paragraph_config calls — it collapses N round-trips into one.
    Paragraphs are processed sequentially; one failing item does not stop the rest (failures are
    reported in the result).

    Args:
        notebook_id: The notebook ID containing the paragraphs
        configs: List of config changes, each {paragraph_id, config}. See update_paragraph_config
            for the config dict format (graph.mode, keys/groups/values, etc.).
    """
    _validate_id(notebook_id, "notebook_id")
    if not configs:
        raise ToolError("configs must not be empty")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)

    failures: list[str] = []
    succeeded = 0
    for c in configs:
        try:
            _validate_id(c.paragraph_id, "paragraph_id")
            await _apply_paragraph_config(zeppelin, notebook_id, c.paragraph_id, c.config)
            succeeded += 1
        except Exception as e:
            failures.append(f"{c.paragraph_id}: {_short_error(e)}")
    return _format_batch_result("Updated config for", succeeded, len(configs), failures)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True))
@_tool_error_handler("updating paragraph")
async def update_paragraph(
    ctx: Context,
    notebook_id: str,
    paragraph_id: str,
    text: str,
    title: Optional[str] = None,
) -> str:
    """Update paragraph code/text. Previous content is automatically backed up before changes.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to update
        text: The new code/content for the paragraph
        title: Optional new title for the paragraph
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    notebook_path = await _check_backup_protection(zeppelin, notebook_id)
    await _apply_paragraph_update(zeppelin, notebook_id, notebook_path, paragraph_id, text, title)
    return f"Updated paragraph {paragraph_id}."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True))
@_tool_error_handler("updating paragraphs")
async def batch_update_paragraph(
    ctx: Context,
    notebook_id: str,
    updates: list[ParagraphUpdate],
) -> str:
    """Update multiple paragraphs in one call (batch version of update_paragraph). Previous
    content of each changed paragraph is automatically backed up.

    Prefer this over many update_paragraph calls when editing several paragraphs — it collapses
    N round-trips into one. Paragraphs are updated sequentially; one failing item does not stop
    the rest (failures are reported in the result).

    Args:
        notebook_id: The notebook ID containing the paragraphs
        updates: List of edits, each {paragraph_id, text, title?}.
    """
    _validate_id(notebook_id, "notebook_id")
    if not updates:
        raise ToolError("updates must not be empty")
    zeppelin = _get_zeppelin(ctx)
    notebook_path = await _check_backup_protection(zeppelin, notebook_id)

    failures: list[str] = []
    succeeded = 0
    for u in updates:
        try:
            _validate_id(u.paragraph_id, "paragraph_id")
            await _apply_paragraph_update(
                zeppelin, notebook_id, notebook_path, u.paragraph_id, u.text, u.title
            )
            succeeded += 1
        except Exception as e:
            failures.append(f"{u.paragraph_id}: {_short_error(e)}")
    return _format_batch_result("Updated", succeeded, len(updates), failures)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=False))
@_tool_error_handler("deleting paragraph")
async def delete_paragraph(
    ctx: Context,
    notebook_id: str,
    paragraph_id: str,
) -> str:
    """Delete a paragraph. Its content is automatically backed up before deletion.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to delete
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    notebook_path = await _check_backup_protection(zeppelin, notebook_id)
    await _apply_paragraph_delete(zeppelin, notebook_id, notebook_path, paragraph_id)
    return f"Deleted paragraph {paragraph_id}. Previous content backed up."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=False))
@_tool_error_handler("deleting paragraphs")
async def batch_delete_paragraph(
    ctx: Context,
    notebook_id: str,
    paragraph_ids: list[str],
) -> str:
    """Delete multiple paragraphs in one call (batch version of delete_paragraph). Each
    paragraph's content is automatically backed up before deletion.

    Prefer this over many delete_paragraph calls — it collapses N round-trips into one.
    Paragraphs are deleted sequentially; one failing item does not stop the rest (failures are
    reported in the result).

    Args:
        notebook_id: The notebook ID containing the paragraphs
        paragraph_ids: List of paragraph IDs to delete.
    """
    _validate_id(notebook_id, "notebook_id")
    if not paragraph_ids:
        raise ToolError("paragraph_ids must not be empty")
    zeppelin = _get_zeppelin(ctx)
    notebook_path = await _check_backup_protection(zeppelin, notebook_id)

    failures: list[str] = []
    succeeded = 0
    for pid in paragraph_ids:
        try:
            _validate_id(pid, "paragraph_id")
            await _apply_paragraph_delete(zeppelin, notebook_id, notebook_path, pid)
            succeeded += 1
        except Exception as e:
            failures.append(f"{pid}: {_short_error(e)}")
    line = f"Deleted {succeeded}/{len(paragraph_ids)} paragraphs. Previous content backed up."
    if failures:
        line += "\nFailed:\n" + "\n".join(f"- {f}" for f in failures)
    return line


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("moving paragraph")
async def move_paragraph(
    ctx: Context,
    notebook_id: str,
    paragraph_id: str,
    new_index: int,
) -> str:
    """Move a paragraph to a new position within the same notebook.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to move
        new_index: The target position index (0-based)
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    if new_index < 0:
        raise ToolError("new_index must be >= 0")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)
    _check_status(await zeppelin.request(
        "POST", f"/api/notebook/{notebook_id}/paragraph/{paragraph_id}/move/{new_index}"
    ))
    return f"Moved paragraph {paragraph_id} to index {new_index}."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("creating notebook")
async def create_notebook(ctx: Context, name: str) -> str:
    """Create a new empty notebook.

    Args:
        name: Full path for the new notebook, e.g. "Users/john/ProjectName/Notebook Title"
    """
    if "/~Backups/" in name or name.startswith("~Backups/"):
        raise ToolError("Cannot create notebooks in ~Backups — these are protected backup notebooks")
    zeppelin = _get_zeppelin(ctx)
    data = _check_status(await zeppelin.request("POST", "/api/notebook", json={"name": name}))
    return f"Created notebook with id: {data.get('body', 'unknown')}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("adding paragraph")
async def add_paragraph(
    ctx: Context,
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
    _validate_id(notebook_id, "notebook_id")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)
    paragraph_id = await _apply_paragraph_add(zeppelin, notebook_id, text, title, index)
    return f"Added paragraph with id: {paragraph_id}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("adding paragraphs")
async def batch_add_paragraph(
    ctx: Context,
    notebook_id: str,
    paragraphs: list[NewParagraph],
) -> str:
    """Add multiple paragraphs to a notebook in one call (batch version of add_paragraph).
    Paragraphs are created sequentially in the given order.

    Prefer this over many add_paragraph calls when scaffolding several cells — it collapses N
    round-trips into one. One failing item does not stop the rest (failures are reported).

    Args:
        notebook_id: The notebook ID to add paragraphs to
        paragraphs: List of paragraphs to create, each {text, title?, index?}.
    """
    _validate_id(notebook_id, "notebook_id")
    if not paragraphs:
        raise ToolError("paragraphs must not be empty")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)

    new_ids: list[str] = []
    failures: list[str] = []
    for i, para in enumerate(paragraphs):
        try:
            pid = await _apply_paragraph_add(zeppelin, notebook_id, para.text, para.title, para.index)
            new_ids.append(pid)
        except Exception as e:
            failures.append(f"#{i}: {_short_error(e)}")
    line = f"Added {len(new_ids)}/{len(paragraphs)} paragraphs"
    if new_ids:
        line += ": " + ", ".join(new_ids)
    line += "."
    if failures:
        line += "\nFailed:\n" + "\n".join(f"- {f}" for f in failures)
    return line


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("cloning paragraph")
async def clone_paragraph(ctx: Context, notebook_id: str, paragraph_id: str) -> str:
    """Clone a paragraph within the same notebook. The copy is created directly below the
    source paragraph and carries over its code, title, and visualization/chart config.
    The clone's title gets a " (copy)" suffix; untitled paragraphs stay untitled.

    Zeppelin has no dedicated clone-paragraph endpoint, so this reads the source paragraph,
    creates a new one with the same content, then copies its config in a single tool call.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to clone
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)
    saved = await _save_paragraph_state(zeppelin, notebook_id, paragraph_id)
    if saved is None:
        raise ToolError(f"Could not fetch paragraph {paragraph_id}")

    # Find the source paragraph's position so the clone lands directly below it.
    notebook = _check_status(await zeppelin.request("GET", f"/api/notebook/{notebook_id}"))
    paragraphs = notebook.get("body", {}).get("paragraphs", [])
    clone_index: Optional[int] = None
    for i, p in enumerate(paragraphs):
        if p.get("id") == paragraph_id:
            clone_index = i + 1
            break

    body: dict[str, Any] = {"text": saved.get("text", "")}
    source_title = saved.get("title")
    if source_title:
        body["title"] = f"{source_title} (copy)"
    if clone_index is not None:
        body["index"] = clone_index
    data = _check_status(await zeppelin.request(
        "POST", f"/api/notebook/{notebook_id}/paragraph", json=body
    ))
    new_pid = data.get("body", "unknown")

    config = saved.get("config")
    if config and new_pid != "unknown":
        try:
            await zeppelin.request(
                "PUT",
                f"/api/notebook/{notebook_id}/paragraph/{new_pid}/config",
                json=config,
            )
        except Exception:
            logger.warning("Failed to copy config to cloned paragraph %s", new_pid, exc_info=True)

    position = f"at index {clone_index}" if clone_index is not None else "at end of notebook"
    return f"Cloned paragraph {paragraph_id} to new paragraph {new_pid} {position}."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("running paragraph")
async def run_paragraph(
    ctx: Context,
    notebook_id: str,
    paragraph_id: str,
    params: Optional[dict[str, Any]] = None,
    max_rows: int = 50,
    include_html: bool = False,
) -> str:
    """Run a paragraph and return the result.
    Uses the async job endpoint which preserves chart/visualization settings.

    By default, table output is limited to 50 rows and HTML output is omitted to save tokens.
    Set max_rows=0 for unlimited rows when you need full results for analysis.
    Set include_html=True to see HTML output converted to plain text.

    On error, the response includes the error output — examine it before retrying.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to run
        params: Optional dict of dynamic form values, e.g. {"city": "Seoul"}.
        max_rows: Maximum data rows for table output (default 50, 0 = unlimited). Header row always included.
        include_html: If True, include HTML output converted to plain text. If False (default), HTML is omitted.
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)

    code, out_lines = await _run_and_collect(
        zeppelin, notebook_id, paragraph_id, params, max_rows, include_html, ctx
    )
    lines = [f"Status: {code}"] + out_lines
    return _truncate("\n".join(lines))


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("running paragraphs")
async def batch_run_paragraph(
    ctx: Context,
    notebook_id: str,
    runs: list[ParagraphRun],
    params: Optional[dict[str, Any]] = None,
    max_rows: int = 50,
    include_html: bool = False,
    stop_on_error: bool = True,
) -> str:
    """Run several paragraphs one after another in the given order, in a single call.
    Use this instead of many run_paragraph calls when you need a specific sequence of cells run —
    e.g. setup cells before the cells that depend on them. Paragraphs run sequentially (not in
    parallel) because cells typically share interpreter state, and this collapses N round-trips
    into one.

    Each run item may carry its own dynamic form params. The top-level params dict is a shared
    default merged under per-item params (the item wins on key conflicts). The same paragraph_id
    may appear multiple times with different params — a parameter sweep in one call.

    By default table output is limited to 50 rows and HTML output is omitted to save tokens.

    Args:
        notebook_id: The notebook ID containing the paragraphs
        runs: Paragraphs to run in execution order, each {paragraph_id, params?}. Example:
            [{"paragraph_id": "p1"}, {"paragraph_id": "p2", "params": {"city": "Seoul"}}]
        params: Optional shared dynamic form values applied to every item, e.g. {"env": "prod"}.
            Per-item params override these per key.
        max_rows: Maximum data rows per table output (default 50, 0 = unlimited). Header row always included.
        include_html: If True, include HTML output converted to plain text. Default False.
        stop_on_error: If True (default), stop at the first paragraph that errors and report the
            remaining ones as skipped. If False, run every paragraph regardless of failures.
    """
    _validate_id(notebook_id, "notebook_id")
    if not runs:
        raise ToolError("runs must not be empty")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)

    blocks: list[str] = []
    ran = 0
    stopped = False
    for i, item in enumerate(runs):
        try:
            _validate_id(item.paragraph_id, "paragraph_id")
            effective = (
                {**(params or {}), **(item.params or {})}
                if (params or item.params) else None
            )
            code, out_lines = await _run_and_collect(
                zeppelin, notebook_id, item.paragraph_id, effective, max_rows, include_html, ctx
            )
        except Exception as e:
            code, out_lines = "ERROR", [f"Error: {_short_error(e)}"]
        ran += 1
        blocks.append("\n".join([f"=== {item.paragraph_id} — Status: {code} ==="] + out_lines))
        if stop_on_error and code in _RUN_FAILURE_CODES:
            stopped = True
            skipped = [r.paragraph_id for r in runs[i + 1:]]
            if skipped:
                blocks.append(
                    f"Stopped after error (stop_on_error=True). Skipped: {', '.join(skipped)}"
                )
            break
    header = f"Ran {ran}/{len(runs)} paragraphs" + (" (stopped on error)." if stopped else ".")
    return _truncate(header + "\n\n" + "\n\n".join(blocks))


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("running paragraph async")
async def run_paragraph_async(
    ctx: Context,
    notebook_id: str,
    paragraph_id: str,
    params: Optional[dict[str, Any]] = None,
) -> str:
    """Start paragraph execution and return immediately without waiting for results.
    Use this to run multiple paragraphs in parallel, then check status with get_paragraph_status
    and read results with get_paragraph.

    Chart/visualization settings are preserved automatically.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to run
        params: Optional dict of dynamic form values, e.g. {"city": "Seoul"}.
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)

    _check_status(await zeppelin.request(
        "POST", f"/api/notebook/job/{notebook_id}/{paragraph_id}",
        json=_build_params_body(params),
    ))
    return f"Started paragraph {paragraph_id}. Use get_paragraph_status to check completion."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("running all paragraphs")
async def run_all_paragraphs(
    ctx: Context,
    notebook_id: str,
    params: Optional[dict[str, Any]] = None,
) -> str:
    """Run all paragraphs in a notebook and wait for completion.
    Uses the async job endpoint which preserves chart/visualization settings.

    Args:
        notebook_id: The notebook ID to run
        params: Optional dict of dynamic form values, e.g. {"city": "Seoul"}.
    """
    _validate_id(notebook_id, "notebook_id")
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)

    _check_status(await zeppelin.request(
        "POST", f"/api/notebook/job/{notebook_id}",
        json=_build_params_body(params),
    ))

    completed = await _wait_for_notebook_completion(zeppelin, notebook_id, ctx=ctx)

    status = "completed" if completed else "timed out"
    return f"Execution of all paragraphs in notebook {notebook_id} {status}."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("getting paragraph status")
async def get_paragraph_status(ctx: Context, notebook_id: str, paragraph_id: str) -> str:
    """Get execution status of a specific paragraph (useful after async run).

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to check
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
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
            logger.warning(
                "Failed to fetch error details for paragraph %s", paragraph_id, exc_info=True
            )
    return _truncate("\n".join(lines))


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("stopping paragraph")
async def stop_paragraph(ctx: Context, notebook_id: str, paragraph_id: str) -> str:
    """Stop a running paragraph. Use this to cancel long-running or stuck queries.

    Args:
        notebook_id: The notebook ID containing the paragraph
        paragraph_id: The paragraph ID to stop
    """
    _validate_id(notebook_id, "notebook_id")
    _validate_id(paragraph_id, "paragraph_id")
    zeppelin = _get_zeppelin(ctx)
    _check_status(await zeppelin.request(
        "DELETE", f"/api/notebook/job/{notebook_id}/{paragraph_id}"
    ))
    return f"Stop signal sent to paragraph {paragraph_id}."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("getting notebook permissions")
async def get_notebook_permissions(ctx: Context, notebook_id: str) -> str:
    """Get permission information for a notebook (owners, writers, readers).

    Args:
        notebook_id: The notebook ID to get permissions for
    """
    _validate_id(notebook_id, "notebook_id")
    zeppelin = _get_zeppelin(ctx)
    data = _check_status(await zeppelin.request("GET", f"/api/notebook/{notebook_id}/permissions"))
    body = data.get("body", {})
    owners = body.get("owners", [])
    writers = body.get("writers", [])
    readers = body.get("readers", [])
    runners = body.get("runners", [])
    lines = [
        f"Permissions for notebook {notebook_id}:",
        f"  Owners:  {', '.join(owners) if owners else '(none)'}",
        f"  Writers: {', '.join(writers) if writers else '(none)'}",
        f"  Readers: {', '.join(readers) if readers else '(none)'}",
        f"  Runners: {', '.join(runners) if runners else '(none)'}",
    ]
    return "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("setting notebook permissions")
async def set_notebook_permissions(
    ctx: Context,
    notebook_id: str,
    owners: list[str],
    writers: list[str],
    readers: list[str],
    runners: list[str] | None = None,
) -> str:
    """Set permission information for a notebook.

    Args:
        notebook_id: The notebook ID to set permissions for
        owners: List of usernames with owner access
        writers: List of usernames with write access
        readers: List of usernames with read access
        runners: List of usernames with runner access
    """
    _validate_id(notebook_id, "notebook_id")
    if runners is None:
        runners = []
    zeppelin = _get_zeppelin(ctx)
    await _check_backup_protection(zeppelin, notebook_id)
    _check_status(await zeppelin.request(
        "PUT",
        f"/api/notebook/{notebook_id}/permissions",
        json={"owners": owners, "writers": writers, "readers": readers, "runners": runners},
    ))
    lines = [
        f"Updated permissions for notebook {notebook_id}:",
        f"  Owners:  {', '.join(owners) if owners else '(none)'}",
        f"  Writers: {', '.join(writers) if writers else '(none)'}",
        f"  Readers: {', '.join(readers) if readers else '(none)'}",
        f"  Runners: {', '.join(runners) if runners else '(none)'}",
    ]
    return "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("fingerprinting notebook")
async def get_notebook_fingerprint(
    ctx: Context,
    notebook_id: str,
    spec_path: Optional[str] = None,
    known_fingerprint: Optional[str] = None,
    known_units: Optional[dict[str, str]] = None,
) -> str:
    """Get a deterministic content fingerprint of a notebook, plus per-paragraph sub-hashes.

    Cheap freshness check for cached notebook documentation. Only stable content is
    hashed (paragraph id, title, code text); execution status, output, form
    values/definitions, chart configs, and paragraph order are all excluded — none of
    those flag the notebook stale.

    The comparison against the cached doc happens server-side — never hand-compare
    hash maps. Preferred: pass `spec_path` (the spec file under $WIKI_DOCS_PATH) and
    the server reads the stored fingerprint/units from its frontmatter itself; the
    response gains `"match": true|false` and `"diff": {changed, added, removed}`
    naming the exact paragraph ids that differ. If the spec can't be used, the
    response carries a `spec_note` explaining why and no match/diff. Fallback when
    there is no docs repo: pass the stored values explicitly as `known_fingerprint` /
    `known_units` (they take precedence over `spec_path` if both are given).

    Args:
        notebook_id: The notebook ID to fingerprint
        spec_path: Optional path to the cached spec file inside $WIKI_DOCS_PATH
        known_fingerprint: Optional stored overall fingerprint to compare against
        known_units: Optional stored per-paragraph sub-hash map to diff against
    """
    _validate_id(notebook_id, "notebook_id")
    zeppelin = _get_zeppelin(ctx)
    data = _check_status(await zeppelin.request("GET", f"/api/notebook/{notebook_id}"))
    nb = data.get("body", {})
    result = _notebook_fingerprint(nb.get("id", notebook_id), nb.get("paragraphs", []))
    if spec_path:
        stored, note = _load_spec_frontmatter(spec_path)
        if note:
            result["spec_note"] = note
        if known_fingerprint is None:
            known_fingerprint = stored.get("fingerprint")
        if not known_units:
            known_units = stored.get("units")
    if known_fingerprint is not None:
        result["match"] = result["fingerprint"] == known_fingerprint
    if known_units:
        result["diff"] = _fingerprint_diff(result["units"], known_units)
    return json.dumps(result)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True))
@_tool_error_handler("exporting notebook")
async def export_notebook(ctx: Context, notebook_id: str) -> str:
    """Export notebook as JSON (for cross-server migration or backup).
    Warning: output can be very large for notebooks with many paragraphs or large results.

    Args:
        notebook_id: The notebook ID to export
    """
    _validate_id(notebook_id, "notebook_id")
    zeppelin = _get_zeppelin(ctx)
    data = _check_status(await zeppelin.request("GET", f"/api/notebook/export/{notebook_id}"))
    return _truncate(json.dumps(data.get("body", {})))


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("importing notebook")
async def import_notebook(ctx: Context, notebook_json: str, new_name: str | None = None) -> str:
    """Import a previously exported notebook JSON. Optionally rename it.

    Args:
        notebook_json: The full notebook JSON string from export_notebook
        new_name: Optional new name/path for the imported notebook
    """
    zeppelin = _get_zeppelin(ctx)
    body = json.loads(notebook_json)
    if new_name:
        body["name"] = new_name
    import_name = body.get("name", "")
    if "/~Backups/" in import_name or import_name.startswith("~Backups/"):
        raise ToolError("Cannot import notebooks into ~Backups — these are protected backup notebooks")
    data = _check_status(await zeppelin.request("POST", "/api/notebook/import", json=body))
    return f"Imported notebook with id: {data.get('body', 'unknown')}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False))
@_tool_error_handler("cloning notebook")
async def clone_notebook(ctx: Context, notebook_id: str, new_name: str | None = None) -> str:
    """Clone an existing notebook. Optionally rename the copy.

    Args:
        notebook_id: The notebook ID to clone
        new_name: Optional full path/name for the cloned notebook (e.g. "Users/john/Project/Copy").
                  If omitted, Zeppelin auto-generates a name in the source's parent folder.
    """
    _validate_id(notebook_id, "notebook_id")
    if new_name and ("/~Backups/" in new_name or new_name.startswith("~Backups/")):
        raise ToolError("Cannot clone notebooks into ~Backups — these are protected backup notebooks")
    zeppelin = _get_zeppelin(ctx)
    if not new_name:
        # Without an explicit new_name, Zeppelin auto-names the clone in the source's
        # parent folder — which would land in ~Backups if the source is there.
        await _check_backup_protection(zeppelin, notebook_id)
    body = {"name": new_name} if new_name else None
    data = _check_status(await zeppelin.request("POST", f"/api/notebook/{notebook_id}", json=body))
    return f"Cloned notebook with id: {data.get('body', 'unknown')}"


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
