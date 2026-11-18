"""Self-contained HTML timeline of a traced run.

One lane per host (network events and faults) followed by the tasks that run
on it, then tasks with no host, the loop's own callbacks and network-wide
events (partitions). The x axis is trace order, not time: many events share
an instant in a simulation, and order is what explains a race. Time is shown
as tick labels wherever it moves.

TCP sends are paired with the arrival they cause (streams are ordered per
direction, so the n-th ``send`` on a direction matches the n-th ``recv``) and
drawn as arrows. Crashes, pauses and partitions are drawn as spans.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from collections.abc import Iterable
from html import escape
from pathlib import Path
from typing import Any

from ._runtime import RunResult
from ._trace import TraceEvent

NETWORK_KINDS = frozenset(
    {
        "send",
        "recv",
        "connect",
        "accept",
        "close",
        "abort",
        "reset",
        "listen",
        "refused",
        "timeout",
        "drop",
        "crash",
        "pause",
        "resume",
        "restart",
    }
)
FAULT_KINDS = frozenset({"crash", "pause", "resume", "restart", "reset", "partition", "heal"})
MAX_EVENTS = 20_000

_SPAWN = re.compile(r"^(?P<name>\S+) .*?(?: @(?P<host>\S+))?$")
_SIZE = re.compile(r" \d+B(?: \(.*\))?$")


def _lane_of(event: TraceEvent) -> tuple[str, str]:
    if event.actor == "network":
        return "net", "network"
    if event.kind in NETWORK_KINDS:
        return "host", event.actor
    if event.actor in ("loop", "sim"):
        return "loop", "loop"
    return "task", event.actor


def build_model(events: Iterable[TraceEvent], *, max_events: int = MAX_EVENTS) -> dict[str, Any]:
    """The data the page draws; separate from rendering so it can be tested."""
    all_events = list(events)
    omitted = max(0, len(all_events) - max_events)
    shown = all_events[omitted:]

    task_host: dict[str, str | None] = {}
    for event in all_events:
        if event.kind == "spawn" and (match := _SPAWN.match(event.detail)):
            task_host.setdefault(match["name"], match["host"])

    lanes: dict[tuple[str, str], int] = {}
    lane_list: list[dict[str, Any]] = []

    def lane(kind: str, name: str) -> int:
        key = (kind, name)
        if key not in lanes:
            lanes[key] = len(lane_list)
            host = name if kind == "host" else task_host.get(name) if kind == "task" else None
            lane_list.append({"kind": kind, "name": name, "host": host})
        return lanes[key]

    rows: list[list[Any]] = []
    for event in shown:
        kind, name = _lane_of(event)
        if kind == "task" and (host := task_host.get(name)):
            lane("host", host)
        rows.append([event.step, event.time, event.kind, lane(kind, name), event.detail])

    # Lanes: unhosted tasks first, then each host followed by its tasks, then
    # the loop and network-wide events.
    def order(index: int) -> tuple[int, int, int]:
        info = lane_list[index]
        hosts = [i for i, item in enumerate(lane_list) if item["kind"] == "host"]
        if info["kind"] == "task" and info["host"] is None:
            return (0, 0, index)
        if info["kind"] in ("host", "task"):
            host_index = lanes[("host", info["host"])]
            return (1, hosts.index(host_index), 0 if info["kind"] == "host" else 1 + index)
        return (2, 0 if info["kind"] == "loop" else 1, index)

    sorted_lanes = sorted(range(len(lane_list)), key=order)
    remap = {old: new for new, old in enumerate(sorted_lanes)}
    for row in rows:
        row[3] = remap[row[3]]
    lane_list = [lane_list[old] for old in sorted_lanes]

    messages: list[list[int]] = []
    in_flight: dict[str, deque[int]] = defaultdict(deque)
    spans: list[dict[str, Any]] = []
    open_spans: dict[tuple[str, int], int] = {}
    alive: dict[int, int] = {}
    for index, (_, _, kind, lane_index, detail) in enumerate(rows):
        if kind in ("send", "recv") and "(" not in detail.split(" -> ")[0]:
            direction = _SIZE.sub("", detail)
            if kind == "send":
                in_flight[direction].append(index)
            elif in_flight[direction]:
                messages.append([in_flight[direction].popleft(), index])
        elif kind in ("crash", "pause"):
            open_spans.setdefault((kind, lane_index), index)
        elif kind in ("restart", "resume"):
            start_kind = "crash" if kind == "restart" else "pause"
            start = open_spans.pop((start_kind, lane_index), None)
            if start is not None:
                spans.append({"kind": start_kind, "lane": lane_index, "from": start, "to": index})
        elif kind == "partition":
            open_spans.setdefault(("partition", lane_index), index)
        elif kind == "heal":
            start = open_spans.pop(("partition", lane_index), None)
            if start is not None:
                spans.append({"kind": "partition", "lane": lane_index, "from": start, "to": index})
        if lane_list[lane_index]["kind"] == "task":
            # A finished task's lane can come alive again: a host restart
            # reuses the entry task's name.
            if alive.get(lane_index, len(rows)) >= len(rows):
                alive[lane_index] = index
            if kind == "done":
                spans.append(
                    {"kind": "task", "lane": lane_index, "from": alive[lane_index], "to": index}
                )
                alive[lane_index] = len(rows)
    end = len(rows) - 1
    for (kind, lane_index), start in open_spans.items():
        spans.append({"kind": kind, "lane": lane_index, "from": start, "to": end})
    for lane_index, start in alive.items():
        if start < len(rows):
            spans.append({"kind": "task", "lane": lane_index, "from": start, "to": end})

    return {
        "lanes": lane_list,
        "events": rows,
        "messages": messages,
        "spans": spans,
        "omitted": omitted,
    }


def render_html(result: RunResult[Any], *, title: str | None = None) -> str:
    """A standalone page showing ``result``'s trace as a timeline."""
    if not result.trace.keep:
        raise ValueError("the run kept no trace events; rerun it with keep_trace=True")
    model = build_model(result.trace.events)
    failure = result.failure
    model["meta"] = {
        "title": title or f"SimLoop run, seed {result.seed}",
        "seed": result.seed,
        "steps": result.steps,
        "sim_time": result.sim_time,
        "digest": result.digest,
        "scheduler": str(result.config.scheduler),
        "failure": None if failure is None else f"{failure.kind}: {failure.message}",
    }
    # Keep "</script>" in event text from ending the data block.
    data = json.dumps(model, separators=(",", ":")).replace("</", "<\\/")
    page = _TEMPLATE.replace("__TITLE__", escape(model["meta"]["title"]))
    return page.replace("__DATA__", data)


def save_html(result: RunResult[Any], path: str | Path, *, title: str | None = None) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_html(result, title=title), encoding="utf-8")
    return target


_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root {
  --bg: #ffffff; --panel: #f6f8fa; --ink: #1f2328; --muted: #656d76; --grid: #d8dee4;
  --run: #0969da; --spawn: #8250df; --done: #1a7f37; --error: #d1242f; --log: #9a6700;
  --net: #57606a; --fault: #bc4c00; --task: #ddf4ff; --down: #ffebe9; --paused: #fff8c5;
  --partition: #fbefff;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #0d1117; --panel: #161b22; --ink: #e6edf3; --muted: #8d96a0; --grid: #30363d;
    --run: #4493f8; --spawn: #ab7df8; --done: #3fb950; --error: #f85149; --log: #d29922;
    --net: #8d96a0; --fault: #f0883e; --task: #0c2d6b; --down: #490202; --paused: #3b2e00;
    --partition: #2c1846;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink);
  font: 13px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
header { padding: 12px 16px; background: var(--panel); border-bottom: 1px solid var(--grid); }
h1 { font-size: 15px; margin: 0 0 4px; }
.meta { color: var(--muted); }
.failure { color: var(--error); margin-top: 4px; white-space: pre-wrap; }
.controls { display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: center; padding: 8px 16px;
  border-bottom: 1px solid var(--grid); }
.controls label { cursor: pointer; white-space: nowrap; }
.swatch { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 3px; }
#wrap { display: flex; border-bottom: 1px solid var(--grid); }
#names { flex: none; width: 190px; overflow: hidden; border-right: 1px solid var(--grid); }
#names div { height: 24px; padding: 0 8px; display: flex; align-items: center; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; }
#names .host { font-weight: 600; }
#names .task { padding-left: 20px; color: var(--muted); }
#scroll { flex: 1; overflow-x: auto; overflow-y: hidden; }
svg text { fill: var(--muted); font-size: 10px; }
#tip { position: fixed; pointer-events: none; background: var(--panel); border: 1px solid var(--grid);
  padding: 6px 8px; border-radius: 6px; max-width: 520px; display: none; white-space: pre-wrap; z-index: 2; }
#list { padding: 8px 16px; max-height: 40vh; overflow: auto; }
#list div { white-space: pre; }
#list .sel { background: var(--task); }
@media (max-width: 600px) { #names { width: 110px; } }
</style>
</head>
<body>
<header>
  <h1 id="title"></h1>
  <div class="meta" id="meta"></div>
  <div class="failure" id="failure"></div>
</header>
<div class="controls" id="controls">
  <span>zoom <input type="range" id="zoom" min="2" max="24" value="8"></span>
</div>
<div id="wrap"><div id="names"></div><div id="scroll"><svg id="chart"></svg></div></div>
<div id="list"></div>
<div id="tip"></div>
<script id="data" type="application/json">__DATA__</script>
<script>
(() => {
  const model = JSON.parse(document.getElementById("data").textContent);
  const meta = model.meta;
  const LANE = 24, TOP = 22;
  const GROUPS = {
    run: { label: "steps", color: "--run", kinds: ["run", "timer", "executor", "blocked"], on: true },
    task: { label: "spawn/done", color: "--spawn", kinds: ["spawn", "done"], on: true },
    log: { label: "logs", color: "--log", kinds: ["log"], on: true },
    net: { label: "network", color: "--net",
      kinds: ["send", "recv", "connect", "accept", "close", "abort", "listen", "refused", "timeout", "drop"], on: true },
    fault: { label: "faults", color: "--fault",
      kinds: ["crash", "pause", "resume", "restart", "reset", "partition", "heal"], on: true },
    error: { label: "errors", color: "--error", kinds: ["error", "failed", "leak"], on: true },
  };
  const groupOf = {};
  for (const [name, g] of Object.entries(GROUPS)) for (const k of g.kinds) groupOf[k] = name;
  const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
  const colorOf = (ev) => {
    if (ev[2] === "done") return ev[4].startsWith("raised") ? css("--error") : css("--done");
    return css(GROUPS[groupOf[ev[2]] || "run"].color);
  };

  document.title = meta.title;
  document.getElementById("title").textContent = meta.title;
  document.getElementById("meta").textContent =
    `seed ${meta.seed} · scheduler ${meta.scheduler} · ${meta.steps} steps · ` +
    `t=${meta.sim_time.toFixed(6)}s · digest ${meta.digest}` +
    (model.omitted ? ` · first ${model.omitted} events omitted` : "");
  document.getElementById("failure").textContent = meta.failure ? "FAILED " + meta.failure : "";

  const controls = document.getElementById("controls");
  for (const [name, g] of Object.entries(GROUPS)) {
    const label = document.createElement("label");
    const box = document.createElement("input");
    box.type = "checkbox"; box.checked = g.on;
    box.onchange = () => { g.on = box.checked; draw(); };
    const swatch = document.createElement("span");
    swatch.className = "swatch"; swatch.style.background = css(g.color);
    label.append(box, swatch, g.label);
    controls.append(label);
  }
  const names = document.getElementById("names");
  names.style.paddingTop = TOP + "px";
  for (const lane of model.lanes) {
    const div = document.createElement("div");
    div.className = lane.kind;
    div.textContent = lane.kind === "host" ? "⬢ " + lane.name : lane.name;
    div.title = lane.name;
    names.append(div);
  }

  const list = document.getElementById("list");
  const rows = model.events.map((ev, i) => {
    const div = document.createElement("div");
    const lane = model.lanes[ev[3]].name;
    div.textContent = `${ev[1].toFixed(6).padStart(12)}  ${String(ev[0]).padStart(6)}  ` +
      `${lane.padEnd(14)}  ${ev[2].padEnd(9)}${ev[4]}`;
    div.dataset.i = i;
    return div;
  });
  list.append(...rows);

  const tip = document.getElementById("tip");
  const svg = document.getElementById("chart");
  const NS = "http://www.w3.org/2000/svg";
  const el = (name, attrs) => {
    const node = document.createElementNS(NS, name);
    for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
    return node;
  };
  let selected = null;

  function draw() {
    const gap = Number(document.getElementById("zoom").value);
    const visible = model.events.map((ev) => GROUPS[groupOf[ev[2]] || "run"].on);
    const xs = [];
    let x = 10;
    for (let i = 0; i < model.events.length; i++) { xs.push(x); if (visible[i]) x += gap; }
    const width = x + 20, height = TOP + model.lanes.length * LANE + 4;
    svg.setAttribute("width", width); svg.setAttribute("height", height);
    svg.replaceChildren();
    const y = (lane) => TOP + lane * LANE + LANE / 2;
    model.lanes.forEach((lane, i) => {
      if (lane.kind === "host") svg.append(el("line", { x1: 0, x2: width, y1: TOP + i * LANE, y2: TOP + i * LANE,
        stroke: css("--grid") }));
    });
    const spanColor = { task: "--task", crash: "--down", pause: "--paused", partition: "--partition" };
    for (const s of model.spans) {
      const x1 = xs[s.from] - 2, x2 = xs[s.to] + 2;
      const h = s.kind === "task" ? 8 : LANE - 2;
      const rect = el("rect", { x: x1, width: Math.max(2, x2 - x1), y: y(s.lane) - h / 2, height: h,
        rx: 3, fill: css(spanColor[s.kind]) });
      if (s.kind === "partition") { rect.setAttribute("y", TOP); rect.setAttribute("height", height - TOP); }
      svg.append(rect);
    }
    let lastLabel = -1e9, lastTime = null;
    model.events.forEach((ev, i) => {
      if (ev[1] !== lastTime && xs[i] - lastLabel > 70) {
        const t = el("text", { x: xs[i], y: 12 }); t.textContent = ev[1].toFixed(3) + "s";
        svg.append(t, el("line", { x1: xs[i], x2: xs[i], y1: 15, y2: height, stroke: css("--grid"),
          "stroke-dasharray": "2 4" }));
        lastLabel = xs[i];
      }
      lastTime = ev[1];
    });
    if (GROUPS.net.on) for (const [from, to] of model.messages) {
      const a = model.events[from], b = model.events[to];
      svg.append(el("line", { x1: xs[from], y1: y(a[3]), x2: xs[to], y2: y(b[3]), stroke: css("--net"),
        "stroke-width": 1, opacity: 0.6 }));
    }
    model.events.forEach((ev, i) => {
      if (!visible[i]) return;
      const color = colorOf(ev), cx = xs[i], cy = y(ev[3]);
      let mark;
      if (ev[2] === "failed") {
        mark = el("line", { x1: cx, x2: cx, y1: TOP, y2: height, stroke: color, "stroke-width": 2 });
      } else if (groupOf[ev[2]] === "fault" || groupOf[ev[2]] === "error") {
        mark = el("path", { d: `M${cx} ${cy - 6}L${cx + 6} ${cy}L${cx} ${cy + 6}L${cx - 6} ${cy}Z`, fill: color });
      } else {
        mark = el("circle", { cx, cy, r: ev[2] === "run" || ev[2] === "timer" ? 3 : 4, fill: color });
      }
      if (i === selected) mark.setAttribute("stroke", css("--ink"));
      mark.dataset.i = i;
      svg.append(mark);
    });
  }

  const describe = (i) => {
    const ev = model.events[i];
    return `step ${ev[0]} · t=${ev[1].toFixed(6)}s\\n${model.lanes[ev[3]].name}: ${ev[2]} ${ev[4]}`;
  };
  svg.addEventListener("mousemove", (e) => {
    const i = e.target.dataset && e.target.dataset.i;
    if (i === undefined) { tip.style.display = "none"; return; }
    tip.textContent = describe(Number(i));
    tip.style.display = "block";
    tip.style.left = Math.min(e.clientX + 12, innerWidth - 540) + "px";
    tip.style.top = e.clientY + 12 + "px";
  });
  svg.addEventListener("mouseleave", () => { tip.style.display = "none"; });
  svg.addEventListener("click", (e) => {
    const i = e.target.dataset && e.target.dataset.i;
    if (i === undefined) return;
    if (selected !== null) rows[selected].classList.remove("sel");
    selected = Number(i);
    rows[selected].classList.add("sel");
    rows[selected].scrollIntoView({ block: "center" });
    draw();
  });
  list.addEventListener("click", (e) => {
    const i = e.target.dataset && e.target.dataset.i;
    if (i === undefined) return;
    if (selected !== null) rows[selected].classList.remove("sel");
    selected = Number(i);
    rows[selected].classList.add("sel");
    draw();
    const mark = svg.querySelector(`[data-i="${selected}"]`);
    if (mark) document.getElementById("scroll").scrollLeft = Number(mark.getAttribute("cx") ||
      mark.getAttribute("x1") || 0) - 200;
  });
  document.getElementById("zoom").oninput = draw;
  draw();
  const scroll = document.getElementById("scroll");
  scroll.scrollLeft = scroll.scrollWidth;
})();
</script>
</body>
</html>
"""
