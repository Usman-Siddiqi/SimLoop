"""Determinism self-check: run a seed repeatedly and locate the first divergence."""

from __future__ import annotations

import gc
from dataclasses import dataclass
from typing import Any

from ._runtime import MainFunction, RunResult, SimConfig, run
from ._trace import Trace, TraceEvent


@dataclass
class Divergence:
    """Where two runs of the same seed stopped agreeing."""

    seed: int
    index: int
    """Position of the first differing event."""
    expected: TraceEvent | None
    actual: TraceEvent | None
    context: list[TraceEvent]
    """Events both runs agree on, just before the divergence."""

    def format(self) -> str:
        def show(event: TraceEvent | None) -> str:
            return "(trace ended)" if event is None else _line(event)

        lines = [f"seed {self.seed} diverged at trace event {self.index}:"]
        lines += [f"      {_line(e)}" for e in self.context]
        lines.append(f"  run 1: {show(self.expected)}")
        lines.append(f"  run 2: {show(self.actual)}")
        return "\n".join(lines)


def _line(event: TraceEvent) -> str:
    text = event.kind if not event.detail else f"{event.kind} {event.detail}"
    return f"t={event.time:.6f} step {event.step} {event.actor}: {text}"


def first_difference(
    a: Trace, b: Trace, *, context: int = 5
) -> tuple[int, list[TraceEvent]] | None:
    """Index of the first event where ``a`` and ``b`` differ, plus preceding events."""
    if not (a.keep and b.keep):
        raise ValueError("both traces must keep their events")
    for index, (x, y) in enumerate(zip(a.events, b.events, strict=False)):
        if x != y:
            return index, a.events[max(0, index - context) : index]
    if len(a.events) != len(b.events):
        index = min(len(a.events), len(b.events))
        return index, a.events[max(0, index - context) : index]
    return None


def check_determinism(
    main: MainFunction[Any],
    seed: int = 0,
    *,
    config: SimConfig | None = None,
    runs: int = 3,
) -> Divergence | None:
    """Run ``seed`` several times and return the first divergence, if any.

    A collection between runs moves objects around in memory, which shakes out
    code whose behaviour depends on addresses (identity hashes, ``id()``).
    """
    base = (config or SimConfig()).replace(keep_trace=True)
    reference: RunResult[Any] = run(main, seed=seed, config=base)
    for _ in range(runs - 1):
        gc.collect()
        _ = [object() for _ in range(64)]  # perturb the allocator
        again = run(main, seed=seed, config=base)
        found = first_difference(reference.trace, again.trace)
        if found is not None:
            index, context = found
            events_a, events_b = reference.trace.events, again.trace.events
            return Divergence(
                seed=seed,
                index=index,
                expected=events_a[index] if index < len(events_a) else None,
                actual=events_b[index] if index < len(events_b) else None,
                context=context,
            )
    return None
