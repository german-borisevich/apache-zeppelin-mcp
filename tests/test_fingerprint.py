"""Unit tests for the notebook fingerprint helpers.

Covers determinism, per-unit sensitivity + locality, exclusion of volatile
fields (status, output, chart configs, form definitions/values, order), the
server-side diff against stored hashes, and the spec_path frontmatter loader.
"""

import copy

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from server import (
    _fingerprint_diff,
    _load_spec_frontmatter,
    _notebook_fingerprint,
    _paragraph_fingerprint_unit,
    _parse_spec_frontmatter,
)


def _paragraph(pid: str, text: str = "%python\nprint(1)", title: str = "") -> dict:
    return {
        "id": pid,
        "title": title,
        "text": text,
        "status": "FINISHED",
        "config": {
            "graph": {"mode": "table", "keys": [], "values": []},
            "results": {"0": {"graph": {"mode": "table"}}},
        },
        "settings": {
            "forms": {"days": {"type": "input", "defaultValue": "7"}},
            "params": {"days": "7"},
        },
        "results": {"code": "SUCCESS", "msg": [{"type": "TABLE", "data": "a\t1"}]},
        "dateStarted": "Jan 1, 2026 12:00:00 PM",
        "dateFinished": "Jan 1, 2026 12:00:05 PM",
    }


def _notebook() -> tuple[str, list[dict]]:
    return "2ABCDEFGH", [
        _paragraph("20250101-120000_abc", title="Setup"),
        _paragraph("20250101-120500_def", text="%python\ndf = query()", title="DAU"),
    ]


def test_determinism():
    nb_id, paragraphs = _notebook()
    a = _notebook_fingerprint(nb_id, paragraphs)
    b = _notebook_fingerprint(nb_id, copy.deepcopy(paragraphs))
    assert a == b
    assert a["fingerprint"].startswith("sha256:")
    assert set(a["units"]) == {"20250101-120000_abc", "20250101-120500_def"}


def test_text_edit_changes_only_that_unit():
    nb_id, paragraphs = _notebook()
    base = _notebook_fingerprint(nb_id, paragraphs)

    edited = copy.deepcopy(paragraphs)
    edited[1]["text"] = "%python\ndf = query_v2()"
    changed = _notebook_fingerprint(nb_id, edited)

    assert changed["fingerprint"] != base["fingerprint"]
    assert changed["units"]["20250101-120500_def"] != base["units"]["20250101-120500_def"]
    assert changed["units"]["20250101-120000_abc"] == base["units"]["20250101-120000_abc"]


def test_title_edit_flags_stale():
    nb_id, paragraphs = _notebook()
    base = _notebook_fingerprint(nb_id, paragraphs)
    retitled = copy.deepcopy(paragraphs)
    retitled[0]["title"] = "Setup v2"
    assert _notebook_fingerprint(nb_id, retitled)["fingerprint"] != base["fingerprint"]


def test_volatile_fields_are_excluded():
    """Status, output, form values AND definitions, chart configs: all excluded —
    Zeppelin mutates these on runs/sync without a real content edit."""
    nb_id, paragraphs = _notebook()
    base = _notebook_fingerprint(nb_id, paragraphs)

    volatile = copy.deepcopy(paragraphs)
    volatile[0]["status"] = "RUNNING"
    volatile[0]["results"] = {"code": "ERROR", "msg": []}
    volatile[0]["settings"]["params"] = {"days": "365"}  # current form VALUE
    volatile[0]["settings"]["forms"]["days"]["defaultValue"] = "30"  # form DEFINITION
    volatile[0]["config"]["graph"]["mode"] = "lineChart"  # chart config
    volatile[1]["dateFinished"] = "Feb 2, 2026 09:00:00 AM"

    assert _notebook_fingerprint(nb_id, volatile) == base


def test_paragraph_order_is_excluded():
    nb_id, paragraphs = _notebook()
    base = _notebook_fingerprint(nb_id, paragraphs)
    reordered = _notebook_fingerprint(nb_id, list(reversed(paragraphs)))
    assert reordered["units"] == base["units"]
    assert reordered["fingerprint"] == base["fingerprint"]


def test_unit_whitelist_shape():
    unit = _paragraph_fingerprint_unit(_paragraph("p1"))
    assert set(unit) == {"id", "title", "text"}


def test_fingerprint_diff():
    current = {"a": "sha256:1", "b": "sha256:2new", "d": "sha256:4"}
    known = {"a": "sha256:1", "b": "sha256:2", "c": "sha256:3"}
    assert _fingerprint_diff(current, known) == {
        "changed": ["b"],
        "added": ["d"],
        "removed": ["c"],
    }
    assert _fingerprint_diff(known, known) == {"changed": [], "added": [], "removed": []}


_SPEC = """---
artifact_id: 2ABCDEF12
artifact_type: zeppelin_notebook
fingerprint: sha256:aaa                  # overall hash
units:                                   # paragraph_id : sub-hash
  20250101-120000_abc: sha256:bbb
  20250101-120500_def: sha256:ccc        # trailing comment
updated_at: 2026-06-19T12:34:56Z
validated: >
  probe run 3d
---

# Body — this part is never parsed
fingerprint: sha256:should-not-be-read
"""


def test_parse_spec_frontmatter():
    fm = _parse_spec_frontmatter(_SPEC)
    assert fm["fingerprint"] == "sha256:aaa"
    assert fm["updated_at"] == "2026-06-19T12:34:56Z"
    assert fm["units"] == {
        "20250101-120000_abc": "sha256:bbb",
        "20250101-120500_def": "sha256:ccc",
    }


def test_parse_spec_frontmatter_value_with_space():
    """updated_at values may contain a space (e.g. '2026-06-19 12:34:56') —
    the parser must capture the full value, not truncate at the space."""
    fm = _parse_spec_frontmatter(
        "---\nfingerprint: sha256:aaa   # overall\nupdated_at: 2026-06-19 12:34:56   # local time\n---\nbody"
    )
    assert fm["fingerprint"] == "sha256:aaa"
    assert fm["updated_at"] == "2026-06-19 12:34:56"


def test_parse_spec_frontmatter_tolerates_garbage():
    assert _parse_spec_frontmatter("no frontmatter here") == {}
    assert _parse_spec_frontmatter("---\nunclosed frontmatter") == {}
    assert _parse_spec_frontmatter("---\ntitle: x\n---\nbody") == {}


def test_load_spec_frontmatter(tmp_path, monkeypatch):
    monkeypatch.setenv("WIKI_DOCS_PATH", str(tmp_path))
    spec = tmp_path / "demoproj" / "notebooks" / "2ABCDEF12.md"
    spec.parent.mkdir(parents=True)
    spec.write_text(_SPEC, encoding="utf-8")

    fm, note = _load_spec_frontmatter(str(spec))
    assert note is None
    assert fm["fingerprint"] == "sha256:aaa"

    # Degrades with a note (never raises) on missing file / empty frontmatter.
    fm, note = _load_spec_frontmatter(str(tmp_path / "demoproj" / "notebooks" / "nope.md"))
    assert fm == {} and "unreadable" in note
    bad = tmp_path / "demoproj" / "notebooks" / "bad.md"
    bad.write_text("---\ntitle: x\n---\n", encoding="utf-8")
    fm, note = _load_spec_frontmatter(str(bad))
    assert fm == {} and "no stored hashes" in note

    # Unset env → ignored with a note; escaping path → hard error.
    monkeypatch.delenv("WIKI_DOCS_PATH")
    fm, note = _load_spec_frontmatter(str(spec))
    assert fm == {} and "not set" in note
    monkeypatch.setenv("WIKI_DOCS_PATH", str(tmp_path / "demoproj"))
    with pytest.raises(ToolError):
        _load_spec_frontmatter(str(tmp_path / "demoproj" / ".." / "outside.md"))
