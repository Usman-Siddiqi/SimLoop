"""Generate the README images from real SimLoop runs of examples/inventory.py.

    PYTHONHASHSEED=0 python scripts/render_visuals.py

Writes SVGs to docs/assets/. Nothing here is hand-drawn: every mark comes from
an explore report or a recorded trace.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "examples"))

import inventory  # noqa: E402

import simloop  # noqa: E402

ASSETS = ROOT / "docs" / "assets"
SEEDS = range(1000)

FONT = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
INK = "#1f2328"
MUTED = "#656d76"
GRID = "#d0d7de"
PASS = "#9fd3a8"
FAIL = "#d1242f"
LANE = "#0969da"
BACKGROUND = "#ffffff"


def _svg(width: int, height: int, body: list[str]) -> str:
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" font-family="{FONT}">',
            f'<rect width="{width}" height="{height}" rx="8" fill="{BACKGROUND}"/>',
            *body,
            "</svg>",
            "",
        ]
    )


def _text(
    x: float, y: float, content: str, *, size: int = 12, fill: str = INK, **attrs: str
) -> str:
    extra = "".join(f' {k.replace("_", "-")}="{v}"' for k, v in attrs.items())
    position = f'x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{fill}"'
    return f"<text {position}{extra}>{escape(content)}</text>"


@dataclass
class Grid:
    title: str
    outcomes: list[bool]


def _explore(main: simloop.MainFunction[int]) -> tuple[simloop.ExploreReport, list[bool]]:
    outcomes: list[bool] = []
    report = simloop.explore(main, seeds=SEEDS, on_result=lambda r: outcomes.append(r.ok))
    return report, outcomes


def render_seed_grids(grids: list[Grid]) -> str:
    cols, cell, gap = 50, 9, 2
    rows = (len(SEEDS) + cols - 1) // cols
    grid_w = cols * (cell + gap)
    grid_h = rows * (cell + gap)
    pad, header = 24, 44
    width = pad * 2 + grid_w
    height = pad + len(grids) * (header + grid_h + pad)
    body: list[str] = []
    y0 = pad
    for grid in grids:
        failed = grid.outcomes.count(False)
        body.append(_text(pad, y0 + 14, grid.title, size=14, font_weight="600"))
        body.append(
            _text(
                pad,
                y0 + 32,
                f"seeds {SEEDS.start}-{SEEDS.stop - 1}: {failed} failed, "
                f"{len(grid.outcomes) - failed} passed",
                fill=MUTED,
            )
        )
        top = y0 + header
        paths: dict[bool, list[str]] = {True: [], False: []}
        for index, ok in enumerate(grid.outcomes):
            r, c = divmod(index, cols)
            x, y = pad + c * (cell + gap), top + r * (cell + gap)
            paths[ok].append(f"M{x} {y}h{cell}v{cell}h-{cell}z")
        for ok, color in ((True, PASS), (False, FAIL)):
            if paths[ok]:
                body.append(f'<path fill="{color}" d="{"".join(paths[ok])}"/>')
        y0 = top + grid_h + pad
    return _svg(width, height, body)


def render_timeline(result: simloop.RunResult[int]) -> str:
    events = list(result.trace)
    failed = next(e for e in events if e.kind == "failed")
    lanes: dict[str, list[tuple[float, str]]] = {}
    ends: dict[str, float] = {}
    for e in events:
        if e.actor.startswith("Task-") and e.kind == "log":
            lanes.setdefault(e.actor, []).append((e.time, e.detail))
        if e.actor.startswith("Task-") and e.kind == "done":
            ends[e.actor] = e.time
    for e in events:
        if e.kind == "spawn" and e.detail.split()[0].startswith("Task-"):
            lanes.setdefault(e.detail.split()[0], [])

    start = min((t for marks in lanes.values() for t, _ in marks), default=0.0)
    start = max(0.0, start - 0.01)
    stop = failed.time + 0.01
    left, right, top, lane_h = 150, 40, 100, 64
    width = 960
    plot_w = width - left - right
    height = top + lane_h * len(lanes) + 70

    def x_of(t: float) -> float:
        return left + (t - start) / (stop - start) * plot_w

    reads: dict[str, list[str]] = {}
    for actor, marks in lanes.items():
        for _, detail in marks:
            if detail.startswith("read stock="):
                reads.setdefault(detail, []).append(actor)
    racing = next(((d, a) for d, a in reads.items() if len(a) > 1), None)
    caption = (
        f"{' and '.join(sorted(racing[1]))} both {racing[0].replace('stock=', 'stock = ')} "
        "before either writes; the second write overwrites the first."
        if racing
        else "Invariant violated."
    )
    body = [
        _text(24, 28, f"examples/inventory.py, seed {result.seed}", size=15, font_weight="600"),
        _text(24, 48, caption, fill=MUTED),
    ]
    tick_step = 0.01
    tick = round(start / tick_step) * tick_step
    axis_y = top + lane_h * len(lanes)
    while tick <= stop:
        if tick >= start:
            x = x_of(tick)
            body.append(
                f'<line x1="{x:.1f}" y1="{top - 8}" x2="{x:.1f}" y2="{axis_y}" stroke="{GRID}"/>'
            )
            body.append(
                _text(
                    x,
                    axis_y + 16,
                    f"{tick * 1000:.0f} ms",
                    size=10,
                    fill=MUTED,
                    text_anchor="middle",
                )
            )
        tick += tick_step
    body.append(
        _text(
            left + plot_w / 2,
            axis_y + 34,
            "simulated time",
            size=11,
            fill=MUTED,
            text_anchor="middle",
        )
    )

    for row, (actor, marks) in enumerate(sorted(lanes.items(), key=lambda kv: int(kv[0][5:]))):
        y = top + row * lane_h + lane_h / 2
        order = next((d for _, d in marks if d.startswith("order")), "not arrived yet")
        body.append(_text(24, y - 2, actor, size=13, font_weight="600"))
        body.append(_text(24, y + 14, order, size=11, fill=MUTED))
        if not marks:
            continue
        x_start = x_of(marks[0][0])
        x_end = x_of(ends.get(actor, failed.time))
        body.append(
            f'<line x1="{x_start:.1f}" y1="{y:.1f}" x2="{x_end:.1f}" y2="{y:.1f}" '
            f'stroke="{LANE}" stroke-width="3" stroke-linecap="round"/>'
        )
        grouped: dict[float, list[str]] = {}
        for t, detail in marks:
            grouped.setdefault(t, []).append(detail)
        labelled = 0
        for t, details in grouped.items():
            x = x_of(t)
            body.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{BACKGROUND}" '
                f'stroke="{LANE}" stroke-width="2"/>'
            )
            label = ", ".join(d for d in details if not d.startswith("order"))
            if not label:
                continue
            dy = 22 if labelled % 2 == 0 else -12
            labelled += 1
            near_end = x > left + plot_w * 0.85
            body.append(
                _text(
                    x - 10 if near_end else x,
                    y + dy,
                    label,
                    size=11,
                    text_anchor="end" if near_end else "middle",
                )
            )

    fx = x_of(failed.time)
    body.append(
        f'<line x1="{fx:.1f}" y1="{top - 12}" x2="{fx:.1f}" y2="{axis_y}" stroke="{FAIL}" '
        f'stroke-width="2" stroke-dasharray="5 4"/>'
    )
    body.append(
        _text(
            fx - 6,
            top - 18,
            f"t={failed.time * 1000:.1f} ms, step {failed.step}: {failed.detail}",
            size=11,
            fill=FAIL,
            text_anchor="end",
        )
    )
    return _svg(width, height, body)


def main() -> None:
    ASSETS.mkdir(parents=True, exist_ok=True)
    racy, racy_outcomes = _explore(inventory.main)
    fixed, fixed_outcomes = _explore(inventory.main_fixed)
    assert fixed.ok, fixed.summary()
    (ASSETS / "inventory-seeds.svg").write_text(
        render_seed_grids(
            [
                Grid("Shop.reserve (read, then write)", racy_outcomes),
                Grid("Shop.reserve_locked (asyncio.Lock around it)", fixed_outcomes),
            ]
        )
    )
    example = racy.groups[0].example
    assert example is not None
    (ASSETS / "inventory-timeline.svg").write_text(render_timeline(example))
    print(racy.summary())
    print(fixed.summary())


if __name__ == "__main__":
    main()
