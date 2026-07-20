---
name: review_notebook
description: Review a Zeppelin notebook for methodology, code quality, and best practices
argument-hint: "[notebook name, path, or ID]"
disable-model-invocation: true
---

You are reviewing a Zeppelin notebook. The analyst's request is:

$ARGUMENTS

Follow EVERY step below IN ORDER.

## Step 1: Find and load the notebook

Use `list_notebooks` with a name filter based on the argument to find the notebook. Then use `get_notebook` to load the full overview (paragraph code, titles, status).

If the argument is a notebook ID, use `get_notebook` directly.

## Step 2: Determine project context

Identify the project from the notebook path or table prefixes in the queries (e.g. `myproject.events` means project `myproject`).

- If the current workspace keeps project documentation (data model, event definitions, known pitfalls), use it to validate table and event names.
- Otherwise discover the schema live: run a scratch paragraph to inspect the relevant table DDL and list distinct values of key columns (with a `LIMIT`) rather than assuming names.

## Step 3: Read paragraph outputs

Use `get_paragraph` for each code paragraph to see both the code and its output. Note data flow between paragraphs (shared interpreter state — variables from one paragraph are available in the next).

> Note: Multiple paragraphs may contain identical code — this is intentional in Zeppelin (typically produced with `clone_paragraph`). Each paragraph can have a different chart configuration (different keys, values, grouping, or chart type) applied to the same query result. Do not flag identical paragraphs as duplicates or mistakes.

## Step 4: Check methodology

- Metric calculations: correct formulas, appropriate aggregations
- Cohort definitions: authoritative source for install/signup dates, correct lifetime calculation
- Entity name validity (events, tables, columns): verify against project documentation or the live schema
- Selection/survivorship bias: are we only counting users who did X instead of all users?
- Correct denominators: e.g. retention should divide by cohort size, not active users
- Date range sanity: reasonable periods, no future dates, correct timezone handling

## Step 5: Check analysis & conclusions

Review markdown/text paragraphs for the correctness of conclusions drawn from the data:

- Are conclusions supported by the query results shown?
- Flag unsupported claims, incorrect interpretations, or missing caveats
- Check for logical consistency between different sections of the analysis
- **Triangulate key figures**: does the headline number match an independent source or the known scale of the project (e.g. its typical DAU or revenue)? Flag unexplained discrepancies above ~20%
- **Segment-first check**: if a conclusion is drawn from an aggregate trend, verify key segments (platform, user tier, region) move in the same direction — flag possible Simpson's paradox / mix shift

> **Skip condition**: If the notebook contains no analytical text (only code + charts), this is likely a monitoring dashboard — skip this step and note "monitoring notebook, no analysis to review".

> This step reviews text that already exists. The absence of analysis/methodology/conclusion paragraphs is **not** a defect — never recommend adding markdown commentary paragraphs; such text is added only on the analyst's explicit request.

## Step 6: Check code quality

Review against the team's pitfalls/style documentation if the workspace has any; in all cases check:

- Filters: queries on large event/log tables should filter on the partitioning/indexed columns as early as possible
- Date handling: proper date types and functions for comparisons, not string casting
- JOINs: filter-before-join pattern, explicit column aliases for joined columns
- Parameter/JSON extraction: correct syntax for the engine in use; check whether the value already exists as a direct column before parsing it out of a blob
- Performance: engine-appropriate approximate distinct counts where exactness isn't needed, appropriate use of CTEs
- Python patterns: connection handling consistent with the rest of the notebook
- Sorting: `ORDER BY` present for visualization correctness

## Step 7: Check visualizations

- Chart type appropriate for the data (line for time series, bar for categories, etc.)
- Correct sorting for time-based axes
- Readable column names (no `a.date` artifacts from JOINs)
- Reasonable number of series/categories (not 50 lines on one chart)
- **Paragraph presentation flags**: flag any finalized paragraph whose code editor is still visible (config missing `editorHide: true`) or whose forms have "Run on selection change" enabled (`runOnSelectionChange` not set to `false`). These are set via `update_paragraph_config` and should be standard on every non-draft paragraph — readers shouldn't see raw code, and dropdown changes shouldn't auto-trigger heavy queries. Audit the whole notebook with a single `list_paragraphs` call: deviating paragraphs are marked inline with `[editor visible]` and/or `[runs on selection change]`; unmarked paragraphs are fine. Per-paragraph detail (a `Presentation:` section with both flags) appears in `get_paragraph` output. Do NOT use `export_notebook` for this — its output embeds all paragraph results and routinely exceeds the output limit.

## Step 8: Check documentation

- Paragraph titles present and descriptive
- Comments explaining "why" for non-obvious logic
- Self-explanatory notebook flow (can someone understand it without verbal explanation?) — satisfied by descriptive paragraph titles, section-separator headers, and code comments. Do **not** flag missing analysis/conclusion markdown paragraphs or suggest adding them as a fix.

## Step 9: Report findings

When referencing paragraphs in findings, use the paragraph title (e.g., "in *Revenue by Country*"). Only fall back to paragraph index if the paragraph has no title.

Present findings in this structure:

### Critical Issues
Issues that affect correctness of results (wrong metrics, missing filters, bias).

### Code Quality
Performance issues, pitfall violations, convention deviations.

### Visualization & Documentation
Chart improvements, missing titles, readability.

### Summary
1-2 sentence overall assessment with prioritized action items. End with a confidence verdict on the notebook's conclusions — **High / Medium / Low** — naming the main factor (e.g. "Medium: methodology is sound, but the headline figure was not cross-checked against an independent source").

After the report, offer to add corrective paragraphs to fix critical issues.
