"""Render an animated GIF of a real simulated run for the README.

    pip install pillow
    PYTHONHASHSEED=0 python scripts/render_gif.py

Runs examples/counter.py (a crash loses acknowledged writes), shrinks the
failure, and plays the shrunk run's trace event by event using the same
lane model as the HTML timeline (``simloop._html.build_model``). Nothing is
drawn by hand: every mark is a trace event.
"""

from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "examples"))

import counter  # noqa: E402

import simloop  # noqa: E402
from simloop._html import build_model  # noqa: E402

OUT = ROOT / "docs" / "assets" / "counter-run.gif"
SEED = 3
WIDTH, LANE, TOP, LEFT, RIGHT = 960, 22, 58, 150, 20
FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/Library/Fonts/Menlo.ttc",
    "C:/Windows/Fonts/consola.ttf",
]

BG = (255, 255, 255)
INK = (31, 35, 40)
MUTED = (101, 109, 118)
GRID = (216, 222, 228)
COLORS = {
    "run": (9, 105, 218),
    "spawn": (130, 80, 223),
    "done": (26, 127, 55),
    "log": (154, 103, 0),
    "net": (87, 96, 106),
    "fault": (188, 76, 0),
    "error": (209, 36, 47),
}
SPAN_COLORS = {"task": (221, 244, 255), "crash": (255, 235, 233), "pause": (255, 248, 197)}
NET = {"send", "recv", "connect", "accept", "close", "abort", "listen", "refused", "timeout"}
FAULT = {"crash", "pause", "resume", "restart", "reset", "partition", "heal"}


def _font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for path in FONT_PATHS:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _color(kind: str, detail: str) -> tuple[int, int, int]:
    if kind == "done":
        return COLORS["error"] if detail.startswith("raised") else COLORS["done"]
    if kind in NET:
        return COLORS["net"]
    if kind in FAULT:
        return COLORS["fault"]
    if kind in ("failed", "error", "leak"):
        return COLORS["error"]
    return COLORS.get(kind, COLORS["run"])


def main() -> int:
    failing = simloop.run(counter.main, seed=SEED)
    assert failing.failure is not None, "the example should fail on this seed"
    shown = simloop.shrink(counter.main, failing).shrunk
    model = build_model(shown.trace.events)

    # The loop's own timer callbacks are noise here; keep its failure marker.
    keep = [
        i
        for i, ev in enumerate(model["events"])
        if model["lanes"][ev[3]]["kind"] != "loop" or ev[2] == "failed"
    ]
    # The failure is drawn as a full-height line, so it needs no lane.
    lanes_used = sorted({model["events"][i][3] for i in keep if model["events"][i][2] != "failed"})
    row = {lane: n for n, lane in enumerate(lanes_used)}
    position = {event: n for n, event in enumerate(keep)}
    height = TOP + LANE * len(lanes_used) + 56
    gap = (WIDTH - LEFT - RIGHT) / max(1, len(keep) - 1)

    def x(event: int) -> float:
        # Events hidden from the animation sit at the next kept event's x.
        later = [p for e, p in position.items() if e >= event]
        return LEFT + gap * (min(later) if later else len(keep) - 1)

    def y(lane: int) -> float:
        return TOP + row[lane] * LANE + LANE / 2

    small, normal, bold = _font(11), _font(12), _font(13)
    failure = shown.failure
    assert failure is not None
    title = f"SimLoop · examples/counter.py · seed {SEED}, shrunk · {shown.steps} steps"

    def frame(upto: int) -> Image.Image:
        image = Image.new("RGB", (WIDTH, height), BG)
        draw = ImageDraw.Draw(image)
        draw.text((12, 10), title, fill=INK, font=bold)
        current = model["events"][keep[upto]]
        draw.text((12, 30), f"t={current[1]:.3f}s", fill=MUTED, font=normal)
        last = keep[upto]
        for lane in lanes_used:
            info = model["lanes"][lane]
            host = info["kind"] == "host"
            if host:
                cy = y(lane)
                draw.regular_polygon((18, cy, 5), 6, fill=INK)
            draw.text(
                (28, y(lane) - 7), info["name"][:16], fill=INK if host else MUTED, font=normal
            )
            if info["kind"] == "host":
                draw.line([(LEFT - 8, y(lane) - LANE / 2), (WIDTH, y(lane) - LANE / 2)], fill=GRID)
        for span in model["spans"]:
            if span["lane"] not in row or span["from"] > last:
                continue
            x1, x2 = x(span["from"]) - 2, x(min(span["to"], last)) + 2
            half = 4 if span["kind"] == "task" else LANE / 2 - 1
            color = SPAN_COLORS.get(span["kind"], SPAN_COLORS["task"])
            draw.rounded_rectangle(
                [(x1, y(span["lane"]) - half), (max(x2, x1 + 2), y(span["lane"]) + half)],
                3,
                fill=color,
            )
        for send, recv in model["messages"]:
            if recv <= last and send in position and recv in position:
                a, b = model["events"][send], model["events"][recv]
                draw.line([(x(send), y(a[3])), (x(recv), y(b[3]))], fill=GRID, width=1)
        for n in range(upto + 1):
            ev = model["events"][keep[n]]
            color = _color(ev[2], ev[4])
            if ev[2] == "failed":
                cx = x(keep[n])
                draw.line([(cx, TOP), (cx, TOP + LANE * len(lanes_used))], fill=color, width=2)
                continue
            cx, cy = x(keep[n]), y(ev[3])
            if ev[2] in FAULT:
                draw.polygon([(cx, cy - 6), (cx + 6, cy), (cx, cy + 6), (cx - 6, cy)], fill=color)
            else:
                r = 3 if ev[2] == "run" else 4
                draw.ellipse([(cx - r, cy - r), (cx + r, cy + r)], fill=color)
        cx = x(last)
        draw.line([(cx, TOP - 6), (cx, TOP + LANE * len(lanes_used))], fill=MUTED, width=1)
        lane_name = "sim" if current[2] == "failed" else model["lanes"][current[3]]["name"]
        caption = f"{lane_name}: {current[2]} {current[4]}"
        color = (
            COLORS["error"] if current[2] in ("failed", "done") and "rror" in current[4] else INK
        )
        if current[2] == "log":
            color = COLORS["log"]
        draw.text((12, height - 40), caption[:120], fill=color, font=normal)
        draw.text(
            (12, height - 20),
            "x axis: trace order; labels show simulated time",
            fill=MUTED,
            font=small,
        )
        return image

    frames = [frame(n) for n in range(len(keep))]
    durations = [90] * len(frames)
    for n, event in enumerate(keep):
        if model["events"][event][2] in ("log", "crash", "restart", "refused"):
            durations[n] = 700
    durations[-1] = 4000
    OUT.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(
        OUT,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    print(f"wrote {OUT.relative_to(ROOT)} ({len(frames)} frames, {OUT.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
