"""Running many seeds and summarizing the failures."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from ._runtime import MainFunction, RunResult, SimConfig, run
from ._selfcheck import Divergence, check_determinism

_wall_clock = time.perf_counter


@dataclass
class FailureGroup:
    """All seeds that failed with the same signature."""

    signature: str
    kind: str
    message: str
    seeds: list[int] = field(default_factory=list)
    example: RunResult[Any] | None = None
    """Traced rerun of the first failing seed."""
    reproducible: bool = True
    """Whether the traced rerun matched the original run's trace digest."""
    divergence: Divergence | None = None
    """For an unreproducible group, where repeated runs of its seed first differ."""


@dataclass
class ExploreReport:
    runs: int
    passed: int
    groups: list[FailureGroup]
    wall_time: float
    sim_time: float
    steps: int
    stopped_early: bool = False

    @property
    def failed(self) -> int:
        return self.runs - self.passed

    @property
    def ok(self) -> bool:
        return not self.groups

    def failing_seeds(self) -> list[int]:
        return sorted(seed for group in self.groups for seed in group.seeds)

    def summary(self) -> str:
        rate = self.runs / self.wall_time if self.wall_time > 0 else float("inf")
        lines = [
            f"{self.runs} runs, {self.passed} passed, {self.failed} failed "
            f"in {self.wall_time:.2f}s ({rate:.0f} runs/s, "
            f"{self.sim_time:.1f}s simulated, {self.steps} steps)"
        ]
        if self.stopped_early:
            lines.append("stopped early: failure limit reached")
        for index, group in enumerate(self.groups, 1):
            seeds = ", ".join(str(s) for s in group.seeds[:10])
            if len(group.seeds) > 10:
                seeds += f", ... ({len(group.seeds)} total)"
            lines.append(f"\n[{index}] {group.kind}: {group.message}")
            lines.append(f"    signature: {group.signature}")
            lines.append(f"    seeds: {seeds}")
            if not group.reproducible:
                lines.append(
                    "    WARNING: rerunning the first seed produced a different trace; "
                    "the code under test is leaking non-determinism"
                )
                if group.divergence is not None:
                    detail = group.divergence.format().replace("\n", "\n    ")
                    lines.append(f"    {detail}")
        return "\n".join(lines)


def explore(
    main: MainFunction[Any],
    *,
    seeds: Iterable[int] = range(100),
    config: SimConfig | None = None,
    max_failures: int | None = None,
    time_budget: float | None = None,
    on_result: Callable[[RunResult[Any]], None] | None = None,
) -> ExploreReport:
    """Run ``main`` once per seed and group failures by signature.

    Exploration runs keep no trace events, only digests. The first failing seed
    of each group is rerun with a full trace, which doubles as a check that the
    failure reproduces exactly.
    """
    base = config or SimConfig()
    fast = base.replace(keep_trace=False)
    groups: dict[str, FailureGroup] = {}
    digests: dict[str, str] = {}
    runs = passed = steps = 0
    sim_time = 0.0
    stopped_early = False
    started = _wall_clock()

    for seed in seeds:
        if time_budget is not None and _wall_clock() - started >= time_budget:
            break
        result = run(main, seed=seed, config=fast)
        runs += 1
        steps += result.steps
        sim_time += result.sim_time
        if on_result is not None:
            on_result(result)
        if result.failure is None:
            passed += 1
            continue
        failure = result.failure
        group = groups.get(failure.signature)
        if group is None:
            group = groups[failure.signature] = FailureGroup(
                failure.signature, failure.kind, failure.message
            )
            digests[failure.signature] = result.digest
        group.seeds.append(seed)
        failures = runs - passed
        if max_failures is not None and failures >= max_failures:
            stopped_early = True
            break

    for group in groups.values():
        example = run(main, seed=group.seeds[0], config=base.replace(keep_trace=True))
        group.example = example
        group.reproducible = example.digest == digests[group.signature]
        if not group.reproducible:
            group.divergence = check_determinism(main, group.seeds[0], config=base, runs=4)

    return ExploreReport(
        runs=runs,
        passed=passed,
        groups=list(groups.values()),
        wall_time=_wall_clock() - started,
        sim_time=sim_time,
        steps=steps,
        stopped_early=stopped_early,
    )
