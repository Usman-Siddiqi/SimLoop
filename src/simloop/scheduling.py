"""Scheduling policies: which ready callback the loop runs next.

The loop processes ready callbacks in batches, like asyncio's ``_run_once``:
callbacks made ready while a batch runs wait for the next batch. A policy only
chooses among the callbacks of the current batch.

Each ready callback carries an *origin*: the loop step that scheduled it.
Callbacks scheduled by the same step keep their relative order under every
policy here. asyncio documents that callbacks registered together run in
registration order, and plenty of code relies on it, so reordering them would
produce executions that cannot happen in production.
"""

from __future__ import annotations

import asyncio
from collections.abc import Hashable, Sequence
from typing import Any, Protocol

from ._choices import ChoiceSource


class ReadyEntry(Protocol):
    @property
    def origin(self) -> int: ...

    @property
    def handle(self) -> asyncio.Handle: ...


class Scheduler(Protocol):
    """Chooses the index (within the first ``batch`` entries) to run next."""

    name: str

    def pick(self, ready: Sequence[ReadyEntry], batch: int, choices: ChoiceSource) -> int: ...


class FifoScheduler:
    """Exactly asyncio's order. Makes no random choices."""

    name = "fifo"

    def pick(self, ready: Sequence[ReadyEntry], batch: int, choices: ChoiceSource) -> int:
        return 0


class RandomScheduler:
    """Uniformly random among the batch, preserving per-origin FIFO order.

    Choice 0 is always the FIFO pick, so shrinking a schedule moves it toward
    asyncio's natural order.

    This explores orderings that real asyncio produces only under different
    I/O and timer timing. It can occasionally produce an order real asyncio
    never would (two callbacks whose relative order was fixed by an earlier,
    unrelated ordering), so a failure found only under this policy should be
    double-checked for feasibility.
    """

    name = "random"

    def pick(self, ready: Sequence[ReadyEntry], batch: int, choices: ChoiceSource) -> int:
        if batch == 1:
            return 0
        candidates = _first_per_origin(ready, batch)
        return candidates[choices.below(len(candidates))]


def _first_per_origin(ready: Sequence[ReadyEntry], batch: int) -> list[int]:
    seen: set[int] = set()
    candidates: list[int] = []
    for index in range(batch):
        origin = ready[index].origin
        if origin not in seen:
            seen.add(origin)
            candidates.append(index)
    return candidates


def _owner(entry: ReadyEntry) -> Hashable:
    owner: Any = getattr(entry.handle._callback, "__self__", None)  # type: ignore[attr-defined]
    if isinstance(owner, asyncio.Task):
        return owner
    return ("origin", entry.origin)


class PCTScheduler:
    """Probabilistic Concurrency Testing (Burckhardt et al., ASPLOS 2010).

    Every task gets a random priority when it first becomes ready, and the
    highest-priority ready task always runs. At ``depth - 1`` random points
    among the first ``horizon`` scheduling decisions, the task that runs is
    demoted below everyone else. A bug that needs ``d`` specific ordering
    constraints is found with probability at least ``1 / (n * k**(d-1))`` per
    run for ``n`` tasks and ``k`` steps, which beats uniform random choice for
    bugs that need one task to stall for a long stretch.

    Callbacks that are not task steps are grouped by origin. As with
    ``random``, same-origin callbacks keep their order, and failures should be
    confirmed with asyncio's own order. With every choice at 0 all
    priorities tie and the order is FIFO, so shrinking still works.
    """

    name = "pct"

    def __init__(self, depth: int = 3, horizon: int = 2000) -> None:
        if depth < 1:
            raise ValueError("depth must be at least 1")
        self.depth = depth
        self.horizon = horizon
        self._priorities: dict[Hashable, int] = {}
        self._change_points: list[int] | None = None
        self._decisions = 0
        self._demotions = 0

    def fresh(self) -> PCTScheduler:
        return PCTScheduler(self.depth, self.horizon)

    def _priority(self, owner: Hashable, choices: ChoiceSource) -> int:
        priority = self._priorities.get(owner)
        if priority is None:
            priority = self._priorities[owner] = self.depth + choices.below(1 << 20)
        return priority

    def pick(self, ready: Sequence[ReadyEntry], batch: int, choices: ChoiceSource) -> int:
        if self._change_points is None:
            # A drawn 0 means "no change point", so all-zero choices stay FIFO.
            drawn = (choices.below(self.horizon + 1) for _ in range(self.depth - 1))
            self._change_points = sorted(point - 1 for point in drawn if point > 0)
        candidates = _first_per_origin(ready, batch) if batch > 1 else [0]
        owners = [_owner(ready[index]) for index in candidates]
        best = 0
        best_priority = self._priority(owners[0], choices)
        for position in range(1, len(candidates)):
            priority = self._priority(owners[position], choices)
            if priority > best_priority:
                best, best_priority = position, priority
        while self._change_points and self._change_points[0] == self._decisions:
            self._change_points.pop(0)
            self._demotions += 1
            self._priorities[owners[best]] = self.depth - self._demotions
        self._decisions += 1
        return candidates[best]


def resolve_scheduler(spec: str | Scheduler) -> Scheduler:
    """Build a fresh scheduler for one run from a name or a template instance.

    Names: ``"fifo"``, ``"random"``, ``"pct"`` or ``"pct:DEPTH"``.
    """
    if not isinstance(spec, str):
        fresh = getattr(spec, "fresh", None)
        return fresh() if callable(fresh) else spec
    if spec == "fifo":
        return FifoScheduler()
    if spec == "random":
        return RandomScheduler()
    name, _, depth = spec.partition(":")
    if name == "pct":
        try:
            return PCTScheduler(int(depth)) if depth else PCTScheduler()
        except ValueError:
            pass
    raise ValueError(
        f"unknown scheduler {spec!r} (expected 'fifo', 'random', 'pct' or 'pct:DEPTH')"
    )
