"""Shrinking a failing run to a simpler one that fails the same way.

A run is fully determined by its seed plus the sequence of choices it drew.
Replaying an edited choice sequence (missing values read as 0) gives a
different but equally reproducible run. Because 0 is always the simplest
choice (FIFO order, minimum latency, no fault, no jitter), deleting or
lowering choices pushes a failure toward the plainest execution that still
shows the bug. This is the approach Hypothesis uses, applied to schedules.

A candidate is accepted when it fails with the same signature and its
recorded choices are smaller in shortlex order (fewer choices first, then
smaller values). Trailing zeros are dropped before comparing: replay reads
missing choices as 0, so they change nothing, and keeping them would make an
all-FIFO run (which draws a zero for every scheduling decision) look more
complex than a run that happened to have fewer ties.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from ._runtime import MainFunction, RunResult, SimConfig, run

_wall_clock = time.perf_counter


@dataclass
class ShrinkResult:
    original: RunResult[Any]
    shrunk: RunResult[Any]
    """Fully traced rerun of the smallest failing choice sequence found."""
    runs: int
    wall_time: float

    @property
    def choices(self) -> list[int]:
        """The shrunk choice sequence, without trailing zeros."""
        return normalize(self.shrunk.choices)

    def summary(self) -> str:
        before, after = self.original, self.shrunk
        return (
            f"shrunk in {self.runs} runs ({self.wall_time:.2f}s): "
            f"{len(normalize(before.choices))} -> {len(self.choices)} choices, "
            f"{before.steps} -> {after.steps} steps, "
            f"t={before.sim_time:.6f}s -> t={after.sim_time:.6f}s"
        )


def normalize(choices: list[int]) -> list[int]:
    """Drop trailing zeros, which replay identically to a shorter sequence."""
    end = len(choices)
    while end and choices[end - 1] == 0:
        end -= 1
    return choices[:end]


def _key(choices: list[int]) -> tuple[int, list[int]]:
    return (len(choices), choices)


class _Shrinker:
    def __init__(
        self,
        main: MainFunction[Any],
        failing: RunResult[Any],
        config: SimConfig,
        max_runs: int,
        deadline: float | None,
    ) -> None:
        if failing.failure is None:
            raise ValueError("can only shrink a failing run")
        self.main = main
        self.seed = failing.seed
        self.signature = failing.failure.signature
        self.config = config.replace(keep_trace=False)
        self.best = normalize(failing.choices)
        self.max_runs = max_runs
        self.deadline = deadline
        self.runs = 0

    def exhausted(self) -> bool:
        if self.runs >= self.max_runs:
            return True
        return self.deadline is not None and _wall_clock() >= self.deadline

    def attempt(self, candidate: list[int]) -> bool:
        candidate = normalize(candidate)
        if self.exhausted() or _key(candidate) >= _key(self.best):
            return False
        self.runs += 1
        result = run(self.main, seed=self.seed, config=self.config, replay=candidate)
        if result.failure is None or result.failure.signature != self.signature:
            return False
        recorded = normalize(result.choices)
        if _key(recorded) < _key(self.best):
            self.best = recorded
            return True
        return False

    def shrink(self) -> None:
        # Truncating the tail is the single cheapest big win: everything after
        # the failure point is noise that replays as zeros.
        self.attempt([])
        improved = True
        while improved and not self.exhausted():
            improved = False
            for edit in (self._delete_chunks, self._zero_chunks, self._lower_values):
                if edit():
                    improved = True

    def _chunks(self) -> Iterator[tuple[int, int]]:
        size = 1
        while size * 2 <= len(self.best):
            size *= 2
        while size >= 1:
            index = len(self.best) - size
            while index >= 0:
                yield index, size
                index -= size
            size //= 2

    def _delete_chunks(self) -> bool:
        return self._apply(lambda c, i, k: c[:i] + c[i + k :])

    def _zero_chunks(self) -> bool:
        return self._apply(lambda c, i, k: c[:i] + [0] * k + c[i + k :] if any(c[i : i + k]) else c)

    def _apply(self, edit: Callable[[list[int], int, int], list[int]]) -> bool:
        improved = False
        for index, size in self._chunks():
            if index + size > len(self.best):
                continue
            if self.attempt(edit(self.best, index, size)):
                improved = True
            if self.exhausted():
                break
        return improved

    def _lower_values(self) -> bool:
        improved = False
        index = len(self.best) - 1
        while index >= 0 and not self.exhausted():
            if index < len(self.best) and self.best[index] > 0:
                improved |= self._minimize_at(index)
            index -= 1
        return improved

    def _minimize_at(self, index: int) -> bool:
        if self._replace_at(index, 0):
            return True
        low, high = 1, self.best[index]
        improved = False
        while low < high and not self.exhausted() and index < len(self.best):
            mid = (low + high) // 2
            if self._replace_at(index, mid):
                improved = True
                if index >= len(self.best):
                    break
                high = self.best[index]
            else:
                low = mid + 1
        return improved

    def _replace_at(self, index: int, value: int) -> bool:
        if index >= len(self.best):
            return False
        candidate = list(self.best)
        candidate[index] = value
        return self.attempt(candidate)


def shrink(
    main: MainFunction[Any],
    failing: RunResult[Any],
    *,
    config: SimConfig | None = None,
    max_runs: int = 1000,
    time_budget: float | None = 60.0,
) -> ShrinkResult:
    """Find a simpler run that fails with the same signature as ``failing``."""
    started = _wall_clock()
    base = config or failing.config
    deadline = None if time_budget is None else started + time_budget
    shrinker = _Shrinker(main, failing, base, max_runs, deadline)
    shrinker.shrink()
    final = run(main, seed=failing.seed, config=base.replace(keep_trace=True), replay=shrinker.best)
    if final.failure is None or final.failure.signature != shrinker.signature:
        raise RuntimeError(
            "shrunk choice sequence no longer reproduces the failure; "
            "the code under test is leaking non-determinism"
        )
    return ShrinkResult(failing, final, shrinker.runs + 1, _wall_clock() - started)
