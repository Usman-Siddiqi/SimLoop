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

from collections.abc import Sequence
from typing import Protocol

from ._choices import ChoiceSource


class ReadyEntry(Protocol):
    @property
    def origin(self) -> int: ...


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
        seen: set[int] = set()
        candidates: list[int] = []
        for index in range(batch):
            origin = ready[index].origin
            if origin not in seen:
                seen.add(origin)
                candidates.append(index)
        return candidates[choices.below(len(candidates))]


def resolve_scheduler(spec: str | Scheduler) -> Scheduler:
    if not isinstance(spec, str):
        return spec
    if spec == "fifo":
        return FifoScheduler()
    if spec == "random":
        return RandomScheduler()
    raise ValueError(f"unknown scheduler {spec!r} (expected 'fifo' or 'random')")
