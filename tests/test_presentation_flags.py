"""Presentation-flag visibility: _format_config must surface editorHide /
runOnSelectionChange (so /review_notebook can audit them), and
_presentation_markers must flag deviations compactly for list_paragraphs."""

from server import _format_config, _presentation_markers


GRAPH_CONFIG = {
    "graph": {
        "mode": "lineChart",
        "keys": [{"name": "lifetime", "index": 0, "aggr": "sum"}],
        "values": [{"name": "retention", "index": 4, "aggr": "sum"}],
    }
}


# ---------------------------------------------------------------------------
# _format_config: Presentation section
# ---------------------------------------------------------------------------

def test_format_config_reports_finalized_flags():
    lines = _format_config({"config": {**GRAPH_CONFIG, "editorHide": True, "runOnSelectionChange": False}})
    text = "\n".join(lines)
    assert "Presentation:" in text
    assert "editorHide: true" in text
    assert "runOnSelectionChange: false" in text
    # finalized paragraph carries no warnings
    assert "visible to readers" not in text
    assert "auto-run" not in text


def test_format_config_warns_on_unset_flags():
    lines = _format_config({"config": GRAPH_CONFIG})
    text = "\n".join(lines)
    assert "editorHide: not set (code editor visible to readers)" in text
    assert "runOnSelectionChange: not set (form changes auto-run the paragraph)" in text


def test_format_config_warns_on_explicit_false_editor_hide():
    lines = _format_config({"config": {"editorHide": False, "runOnSelectionChange": True}})
    text = "\n".join(lines)
    assert "editorHide: false (code editor visible to readers)" in text
    assert "runOnSelectionChange: true (form changes auto-run the paragraph)" in text


def test_format_config_emits_presentation_without_graph():
    # Previously returned [] for paragraphs with no chart config — flags were invisible
    lines = _format_config({"config": {"editorHide": True}})
    text = "\n".join(lines)
    assert "Visualization:" not in text
    assert "Presentation:" in text
    assert "editorHide: true" in text


def test_format_config_keeps_visualization_section():
    lines = _format_config({"config": GRAPH_CONFIG})
    text = "\n".join(lines)
    assert "Visualization:" in text
    assert "chart type: lineChart" in text


# ---------------------------------------------------------------------------
# _presentation_markers: deviation markers for list_paragraphs
# ---------------------------------------------------------------------------

def test_markers_empty_for_finalized_paragraph_with_forms():
    p = {
        "config": {"editorHide": True, "runOnSelectionChange": False},
        "settings": {"forms": {"Start date": {"type": "input"}}},
    }
    assert _presentation_markers(p) == ""


def test_markers_flag_visible_editor():
    assert _presentation_markers({"config": {}}) == " [editor visible]"


def test_markers_flag_run_on_selection_change_only_with_forms():
    with_forms = {
        "config": {"editorHide": True},
        "settings": {"forms": {"OS": {"type": "select"}}},
    }
    assert _presentation_markers(with_forms) == " [runs on selection change]"
    # same config, no forms — auto-run cannot fire, so no marker
    without_forms = {"config": {"editorHide": True}, "settings": {"forms": {}}}
    assert _presentation_markers(without_forms) == ""


def test_markers_combined():
    p = {"config": {}, "settings": {"forms": {"OS": {"type": "select"}}}}
    assert _presentation_markers(p) == " [editor visible, runs on selection change]"
