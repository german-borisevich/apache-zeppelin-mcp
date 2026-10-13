"""Find-and-replace paragraph edits: _apply_text_edits semantics (pure), plus
edit_paragraph / batch_edit_paragraph round-trips, backups, and per-paragraph
atomicity against FakeZeppelin."""

import pytest
from mcp.server.fastmcp.exceptions import ToolError

import server
from conftest import FakeZeppelin, _ctx, _para, run
from server import ParagraphEdit, _apply_text_edits


SQL = "%sql\nselect a,\n       b\nfrom t\nwhere x = 1\n"


def _edit(old: str, new: str, replace_all: bool = False, pid: str = "p1") -> ParagraphEdit:
    return ParagraphEdit(paragraph_id=pid, old_string=old, new_string=new, replace_all=replace_all)


def _para_calls(fake: FakeZeppelin, method: str, pid: str) -> int:
    return sum(1 for m, p in fake.calls if m == method and p == f"/api/notebook/NB1/paragraph/{pid}")


# ---------------------------------------------------------------------------
# _apply_text_edits (pure)
# ---------------------------------------------------------------------------

def test_single_unique_replace_reports_line():
    text, count, line = _apply_text_edits(SQL, [_edit("x = 1", "x = 2")])
    assert text == SQL.replace("x = 1", "x = 2")
    assert count == 1
    assert line == 5


def test_not_found_raises():
    with pytest.raises(ToolError, match="not found"):
        _apply_text_edits(SQL, [_edit("x = 99", "x = 2")])


def test_not_found_hints_at_indented_copy():
    # Copied from get_notebook/get_paragraph output, which indent code by 2 spaces.
    with pytest.raises(ToolError, match="indent code by 2 spaces"):
        _apply_text_edits(SQL, [_edit("  from t\n  where x = 1", "from t")])


def test_ambiguous_without_replace_all_raises():
    with pytest.raises(ToolError, match="matches 2 times"):
        _apply_text_edits("a + a", [_edit("a", "b")])


def test_replace_all_counts_every_occurrence():
    text, count, line = _apply_text_edits("a\nb a\na", [_edit("a", "c", replace_all=True)])
    assert text == "c\nb c\nc"
    assert count == 3
    assert line == 1


def test_empty_old_string_raises():
    with pytest.raises(ToolError, match="must not be empty"):
        _apply_text_edits(SQL, [_edit("", "x")])


def test_noop_edit_raises():
    with pytest.raises(ToolError, match="identical"):
        _apply_text_edits(SQL, [_edit("from t", "from t")])


def test_sequential_edits_see_prior_output():
    text, count, _ = _apply_text_edits(SQL, [_edit("from t", "from u"), _edit("from u", "from v")])
    assert "from v" in text and "from t" not in text
    assert count == 2


def test_error_names_failing_edit_index():
    with pytest.raises(ToolError, match="edit 2"):
        _apply_text_edits(SQL, [_edit("from t", "from u"), _edit("nope", "x")])


# ---------------------------------------------------------------------------
# edit_paragraph
# ---------------------------------------------------------------------------

def test_edit_paragraph_updates_text_with_one_get_one_put(no_backup):
    fake = FakeZeppelin(paragraphs={"p1": _para("p1", text=SQL)})
    result = run(server.edit_paragraph(_ctx(fake), "NB1", "p1", "x = 1", "x = 2"))

    assert result == "Edited paragraph p1 (1 replacement, first at line 5)."
    assert fake.paragraphs["p1"]["text"] == SQL.replace("x = 1", "x = 2")
    assert no_backup == ["p1"]
    assert _para_calls(fake, "GET", "p1") == 1
    assert _para_calls(fake, "PUT", "p1") == 1


def test_edit_paragraph_failure_leaves_text_untouched(no_backup):
    fake = FakeZeppelin(paragraphs={"p1": _para("p1", text=SQL)})
    with pytest.raises(ToolError, match="not found"):
        run(server.edit_paragraph(_ctx(fake), "NB1", "p1", "nope", "x"))
    assert fake.paragraphs["p1"]["text"] == SQL
    assert no_backup == []
    assert _para_calls(fake, "PUT", "p1") == 0


def test_edit_paragraph_blocked_inside_backups(no_backup):
    fake = FakeZeppelin(
        path="Users/german/~Backups/Project/NB_2ABC_backup",
        paragraphs={"p1": _para("p1", text=SQL)},
    )
    with pytest.raises(ToolError, match="~Backups"):
        run(server.edit_paragraph(_ctx(fake), "NB1", "p1", "x = 1", "x = 2"))
    assert fake.paragraphs["p1"]["text"] == SQL


# ---------------------------------------------------------------------------
# batch_edit_paragraph
# ---------------------------------------------------------------------------

def test_batch_edit_groups_edits_per_paragraph(no_backup):
    fake = FakeZeppelin(paragraphs={"p1": _para("p1", text=SQL), "p2": _para("p2", text=SQL)})
    edits = [
        _edit("from t", "from u", pid="p1"),
        _edit("x = 1", "x = 2", pid="p2"),
        _edit("x = 1", "x = 3", pid="p1"),
    ]
    result = run(server.batch_edit_paragraph(_ctx(fake), "NB1", edits))

    assert result == "Edited 2/2 paragraphs."
    assert fake.paragraphs["p1"]["text"] == SQL.replace("from t", "from u").replace("x = 1", "x = 3")
    assert fake.paragraphs["p2"]["text"] == SQL.replace("x = 1", "x = 2")
    assert no_backup == ["p1", "p2"]  # one backup per paragraph, first-seen order
    assert _para_calls(fake, "PUT", "p1") == 1
    assert _para_calls(fake, "PUT", "p2") == 1


def test_batch_edit_is_atomic_per_paragraph(no_backup):
    fake = FakeZeppelin(paragraphs={"p1": _para("p1", text=SQL), "p2": _para("p2", text=SQL)})
    edits = [
        _edit("from t", "from u", pid="p1"),
        _edit("nope", "x", pid="p1"),  # fails -> whole p1 untouched
        _edit("x = 1", "x = 2", pid="p2"),
        _edit("x = 1", "x = 2", pid="p3"),  # missing paragraph
    ]
    result = run(server.batch_edit_paragraph(_ctx(fake), "NB1", edits))

    assert "Edited 1/3 paragraphs." in result
    assert "p1: edit 2" in result
    assert "p3:" in result
    assert fake.paragraphs["p1"]["text"] == SQL
    assert fake.paragraphs["p2"]["text"] == SQL.replace("x = 1", "x = 2")
    assert no_backup == ["p2"]


def test_batch_edit_rejects_empty_list():
    fake = FakeZeppelin()
    with pytest.raises(ToolError, match="must not be empty"):
        run(server.batch_edit_paragraph(_ctx(fake), "NB1", []))
