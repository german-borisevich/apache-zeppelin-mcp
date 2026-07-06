---
name: notebook
description: Work with Zeppelin notebooks — create notebooks, add paragraphs, configure charts, and manage notebook content via MCP tools. Use this skill whenever the user asks to create a notebook, add paragraphs or cells to a notebook, configure or change chart types, set up visualizations, or any task involving Zeppelin notebook manipulation. Also trigger when the user references a specific notebook by name or ID and wants changes made to it, or asks to fix/refactor existing notebook code.
argument-hint: "[notebook name/ID or task description]"
---

You are working with Zeppelin notebooks via MCP tools. The user's request is:

$ARGUMENTS

Follow the steps below in order.

## Step 1: Identify the notebook

- If the user provides a notebook ID → use `get_notebook` directly
- If the user provides a name or partial name → use `list_notebooks` with a name filter
- If creating a new notebook → confirm the path follows the folder conventions already visible on the server (e.g. `Users/{username}/{Project}/{Notebook name}`)
  - Use `list_notebooks` to discover existing structure before creating

## Step 2: Understand context

Read existing notebook paragraphs with `get_notebook` to understand the structure, data flow, and coding patterns already in use. This is important because paragraphs share interpreter state — variables and imports from earlier paragraphs are available in later ones.

**Decide how much exploration is needed:**

| Situation | What to explore |
|-----------|----------------|
| Adding a paragraph similar to existing ones | Notebook context is sufficient |
| Adding a new kind of query or metric | Check the project's query templates / reference notebooks, if the project keeps any |
| Creating a new notebook from scratch | Read an existing notebook in the same folder/project to learn the interpreter prefix, connection pattern, and coding style |

**When the data schema is unknown:** discover it live from a scratch paragraph (e.g. inspect the table DDL or list distinct values of a key column with a `LIMIT`) instead of guessing table or column names.

## Step 3: Generate and add content

### Markdown paragraphs: separators yes, commentary only on explicit request

Two kinds of `%md` paragraphs — treat them differently:

- **Section separators** (a short header line that structures the notebook, e.g. `%md ## Retention`) — allowed and encouraged for readability.
- **Commentary** — methodology descriptions, analysis narratives, findings tables, conclusion or recommendation sections — do NOT add these unless the user explicitly asks. Conclusions and methodology belong in the chat response, not in the notebook.

When the user does ask for analysis/conclusion paragraphs: keep them clear and concise — short bullets, key numbers with their source (table, date range), no filler prose or restating what the chart already shows.

### Interpreter prefix

Every Zeppelin paragraph starts with an interpreter prefix. Check existing notebooks on the same server to find the correct one — e.g. `%python`, `%sql`, `%jdbc(...)`, `%md`. Do not guess — using a prefix that isn't configured on the server causes HTTP 500 errors.

### Code patterns

Follow the connection pattern already used in the notebook. If no pattern exists, a common shape for Python paragraphs querying a database is:

```python
%python

import os
from sqlalchemy import create_engine
import pandas as pd

sql = "QUERY"
df = pd.read_sql(sql, create_engine(os.getenv('YOUR_CONNECTION_STRING_ENV')).connect())
```

(adapt the connection source to whatever the server/team already uses — an env var, a configured interpreter, or a helper module).

- Use `z.show(df)` to output DataFrames for Zeppelin's built-in charts
- For plotly (only when Zeppelin charts can't handle it — Sankey, heatmaps, violin plots):
  `print("%html " + fig.to_html(include_plotlyjs='cdn', full_html=False))`

### Adding paragraphs

Set a descriptive `title` for each paragraph. For a single paragraph use `add_paragraph`; for **two or more**, use `batch_add_paragraph` — one call, paragraphs created in the given order, returns the new IDs.

**Fixing existing code:** use `update_paragraph` to edit one paragraph in place, or `batch_update_paragraph` for several (best-effort: per-item failures are reported, not fatal). The MCP server automatically backs up the original content before applying changes.

**Same query, different chart:** use `clone_paragraph` — it copies code, title, and chart config directly below the original, so you only change the chart config afterwards instead of re-sending the code.

### Chart configuration

After running a paragraph that outputs table data, use `update_paragraph_config` to set the chart **and** the presentation flags from "Finalize paragraph config" below in a single call:

```json
{
  "editorHide": true,
  "runOnSelectionChange": false,
  "graph": {
    "mode": "lineChart",
    "keys": [{"name": "date_column"}],
    "groups": [{"name": "segment_column"}],
    "values": [{"name": "metric_column"}]
  }
}
```

Supported `graph.mode` values: `table`, `multiBarChart`, `stackedAreaChart`, `lineChart`, `pieChart`, `scatterChart`.

Set `graph.keys`, `graph.groups`, `graph.values` with `name` only — omit `index` and `aggr` so Zeppelin auto-detects them from the result set.

**Important — these cause broken charts if ignored:**
- Use `{"name": "col"}` only in keys/groups/values — never include `index` or `aggr` fields. The MCP server auto-fills these from paragraph output headers.
- Always verify column names against actual executed output (`get_paragraph`) before setting chart config. Column names in code (e.g. SQL aliases) may differ from what Zeppelin outputs. Guessing causes "out of sync" warnings.

### Dynamic forms

- Use `get_paragraph_forms` to discover available form parameters before running
- Pass `params` dict to `run_paragraph` to set form values (keys must match field names exactly)
- Use `update_paragraph_forms` to change values without re-executing

**Important — new paragraphs ignore pre-set forms:**
- `update_paragraph_forms` before a paragraph's first run is silently ignored by Zeppelin — the forms aren't registered yet. Pass `params` directly to `run_paragraph` instead, which registers forms and executes with correct values in one pass.

### Running paragraphs

- Several paragraphs in order → `batch_run_paragraph` (runs sequentially in the given order; stops on first error by default) — use this for the "run each paragraph one-by-one" step of notebook migrations instead of N separate calls
- Long-running query → `run_paragraph_async` to start without waiting, then poll `get_paragraph_status`
- Stuck or runaway execution → `stop_paragraph`
- Exploratory queries → always add a `LIMIT` so a scratch query can't scan or return unbounded data

### Finalize paragraph config

Every paragraph the assistant adds or edits MUST be finalized with both of the following set via `update_paragraph_config` before the task is considered complete — these are not optional polish, they are part of "done":

- **`editorHide: true`** — hides the code editor so readers see the result (chart/table), not the query. Readers who want the code can unhide it with the paragraph menu.
- **`runOnSelectionChange: false`** — disables Zeppelin's default behavior of auto-re-running the paragraph every time a dropdown or form value changes. Heavy queries otherwise fire on every click, which wastes cluster time and can crash the server. Users re-run explicitly with the Run button.

Minimal call (paragraph without a chart — e.g. markdown or a pure table output):

```json
{
  "editorHide": true,
  "runOnSelectionChange": false
}
```

For paragraphs with charts, fold these keys into the same `update_paragraph_config` call as `graph` (see the Chart configuration example above) — one round-trip, not two.

When finalizing **several** paragraphs, use `batch_update_paragraph_config` — one call with a config object per paragraph (same shape as above) instead of N `update_paragraph_config` calls.

**Verify before writing:** `editorHide` and `runOnSelectionChange` are paragraph-config keys in current Zeppelin versions, but names can shift across versions. Fetch the current config via `get_paragraph` and confirm the key is accepted; if `update_paragraph_config` returns without the key taking effect, check the paragraph's JSON and adapt (e.g. some versions nest the editor-hide flag under `editorSetting`).

## Boundaries

**What MCP can do:** read/search notebooks and paragraphs; add/update/delete/move paragraphs (single or `batch_*` variants); clone paragraphs and notebooks; create/export/import notebooks; modify paragraph config (charts, single or batch); run paragraphs (sync, batch, or async with status polling) and stop them; update form values; read/update permissions.

**What MCP cannot do:** modify or delete `~Backups` notebooks (protected by the MCP server).

**Hard rules — these exist because violations have caused real problems:**
- Never make direct HTTP/curl requests to Zeppelin — all interaction goes through MCP tools only
- Never read credential files, `.env` files, or connection strings from the filesystem
- Never modify or delete `~Backups` notebooks — these are auto-created safety backups and the MCP server blocks mutations on them
