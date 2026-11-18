from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

import simloop
from simloop import cli
from simloop._html import build_model

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "examples"))

import counter  # noqa: E402

COUNTER = str(ROOT / "examples" / "counter.py")


def _model(page: str) -> dict[str, Any]:
    match = re.search(r'<script id="data" type="application/json">(.*?)</script>', page, re.S)
    assert match is not None
    model: dict[str, Any] = json.loads(match.group(1).replace("<\\/", "</"))
    return model


def test_lanes_group_tasks_under_their_host() -> None:
    result = simloop.run(counter.main, seed=3)
    model = build_model(result.trace.events)
    lanes = [(lane["kind"], lane["name"]) for lane in model["lanes"]]
    host = lanes.index(("host", "counter"))
    assert lanes[host + 1] == ("task", "counter-main")
    assert lanes[0] == ("task", "main")
    assert lanes[-1][0] in ("loop", "net")


def test_sends_pair_with_their_arrivals() -> None:
    result = simloop.run(counter.main, seed=3)
    model = build_model(result.trace.events)
    events = model["events"]
    assert model["messages"]
    for send, recv in model["messages"]:
        assert events[send][2] == "send"
        assert events[recv][2] == "recv"
        assert send < recv
        assert events[send][4].rsplit(" ", 1)[0] == events[recv][4].rsplit(" ", 1)[0]


def test_crash_and_restart_become_a_span() -> None:
    result = simloop.run(counter.main, seed=3)
    model = build_model(result.trace.events)
    crashes = [span for span in model["spans"] if span["kind"] == "crash"]
    assert crashes
    for span in crashes:
        assert model["events"][span["from"]][2] == "crash"
        assert model["events"][span["to"]][2] == "restart"
    restarted = [
        span
        for span in model["spans"]
        if span["kind"] == "task" and model["lanes"][span["lane"]]["name"] == "counter-main"
    ]
    assert len(restarted) >= 2


def test_long_traces_keep_the_end() -> None:
    result = simloop.run(counter.main, seed=3)
    model = build_model(result.trace.events, max_events=50)
    assert len(model["events"]) == 50
    assert model["omitted"] == len(result.trace.events) - 50
    assert model["events"][-1][2] == "failed"


def test_render_escapes_script_end_in_event_text() -> None:
    async def main() -> None:
        simloop.log("</script><b>not markup</b>")

    page = simloop.render_html(simloop.run(main, seed=0), title="a <title>")
    assert page.count("</script>") == 2
    assert "<title>a &lt;title&gt;</title>" in page
    assert any("</script>" in event[4] for event in _model(page)["events"])


def test_render_needs_a_kept_trace() -> None:
    async def main() -> None:
        pass

    result = simloop.run(main, config=simloop.SimConfig(keep_trace=False))
    with pytest.raises(ValueError, match="keep_trace"):
        simloop.render_html(result)


def test_cli_writes_timelines(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    page = tmp_path / "run.html"
    assert cli.main(["run", f"{COUNTER}:main", "--seed", "3", "--html", str(page)]) == 1
    assert _model(page.read_text())["meta"]["failure"].startswith("exception: AssertionError")
    out_dir = tmp_path / "groups"
    code = cli.main(
        ["explore", f"{COUNTER}:main", "--seeds", "5", "--confirm", "0", "--html", str(out_dir)]
    )
    assert code == 1
    assert [p.name for p in out_dir.iterdir()] == ["group-1-seed-0.html"]
    assert "timeline:" in capsys.readouterr().out
