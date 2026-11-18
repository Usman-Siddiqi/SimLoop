"""Running many seeds and summarizing the failures."""

from __future__ import annotations

import itertools
import multiprocessing
import os
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, NamedTuple

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
    feasible: bool | None = None
    """Whether the failure also occurs with asyncio's own callback order
    (``scheduler="fifo"`` plus timer jitter). ``None`` when not checked."""
    feasible_seed: int | None = None
    feasible_config: SimConfig | None = None


@dataclass
class ExploreReport:
    runs: int
    passed: int
    groups: list[FailureGroup]
    wall_time: float
    sim_time: float
    steps: int
    stopped_early: bool = False
    workers: int = 1

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
            f"{self.sim_time:.1f}s simulated, {self.steps} steps"
            + (f", {self.workers} workers)" if self.workers > 1 else ")")
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
            if group.feasible is True and group.feasible_config is not None:
                cfg = group.feasible_config
                lines.append(
                    f"    asyncio order: reproduced with scheduler={cfg.scheduler!r}, "
                    f"timer_jitter={cfg.timer_jitter} (seed {group.feasible_seed})"
                )
            elif group.feasible is False:
                lines.append(
                    "    asyncio order: NOT reproduced with scheduler='fifo' plus timer jitter; "
                    "this interleaving may be impossible on a real asyncio loop"
                )
            if not group.reproducible:
                lines.append(
                    "    WARNING: rerunning the first seed produced a different trace; "
                    "the code under test is leaking non-determinism"
                )
                if group.divergence is not None:
                    detail = group.divergence.format().replace("\n", "\n    ")
                    lines.append(f"    {detail}")
        return "\n".join(lines)


class _Outcome(NamedTuple):
    """What exploration needs from one run; small and always picklable."""

    seed: int
    steps: int
    sim_time: float
    digest: str
    kind: str | None
    message: str
    signature: str | None

    @classmethod
    def of(cls, result: RunResult[Any]) -> _Outcome:
        failure = result.failure
        return cls(
            result.seed,
            result.steps,
            result.sim_time,
            result.digest,
            failure.kind if failure else None,
            failure.message if failure else "",
            failure.signature if failure else None,
        )


_worker_target: tuple[MainFunction[Any], SimConfig] | None = None


def _init_worker(main: MainFunction[Any], config: SimConfig) -> None:
    global _worker_target
    _worker_target = (main, config)


def _run_chunk(seeds: list[int]) -> list[_Outcome]:
    assert _worker_target is not None
    main, config = _worker_target
    return [_Outcome.of(run(main, seed=seed, config=config)) for seed in seeds]


def resolve_workers(workers: int | None) -> int:
    """``None`` or 0 means one worker per CPU."""
    if workers is None or workers == 0:
        return os.cpu_count() or 1
    if workers < 0:
        raise ValueError("workers must be positive, or 0 for one per CPU")
    return workers


def _pool_context() -> Any:
    # Fork lets workers inherit closures, such as the body of a @simloop.test,
    # which spawn could not pickle. Where fork is missing, main must be a
    # module-level function.
    methods = multiprocessing.get_all_start_methods()
    return multiprocessing.get_context("fork" if "fork" in methods else "spawn")


def _chunks(seeds: Iterable[int], size: int) -> Iterator[list[int]]:
    iterator = iter(seeds)
    while chunk := list(itertools.islice(iterator, size)):
        yield chunk


def _parallel_outcomes(
    main: MainFunction[Any],
    seeds: Iterable[int],
    config: SimConfig,
    workers: int,
    chunk_size: int,
    out_of_time: Callable[[], bool],
) -> Iterator[_Outcome]:
    """Outcomes in seed order, whatever order the workers finish in.

    Closing the generator early (a failure limit) cancels the queued chunks.
    """
    pool = ProcessPoolExecutor(
        max_workers=workers,
        mp_context=_pool_context(),
        initializer=_init_worker,
        initargs=(main, config),
    )
    pending: deque[Future[list[_Outcome]]] = deque()
    chunks = _chunks(seeds, chunk_size)
    try:
        while True:
            while len(pending) < workers * 4 and not out_of_time():
                chunk = next(chunks, None)
                if chunk is None:
                    break
                pending.append(pool.submit(_run_chunk, chunk))
            if not pending:
                return
            yield from pending.popleft().result()
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def explore(
    main: MainFunction[Any],
    *,
    seeds: Iterable[int] = range(100),
    config: SimConfig | None = None,
    max_failures: int | None = None,
    time_budget: float | None = None,
    on_result: Callable[[RunResult[Any]], None] | None = None,
    confirm: int = 0,
    workers: int | None = 1,
    chunk_size: int = 8,
) -> ExploreReport:
    """Run ``main`` once per seed and group failures by signature.

    Exploration runs keep no trace events, only digests. The first failing seed
    of each group is rerun with a full trace, which doubles as a check that the
    failure reproduces exactly.

    With ``workers`` > 1 (0 or ``None`` for one per CPU), seeds run in worker
    processes in chunks of ``chunk_size``. Results are consumed in seed order,
    so the report is the same as a sequential one, except that a
    ``time_budget`` may cut at a different seed. ``on_result`` needs the full
    result of every run and is only allowed with one worker.

    With ``confirm`` > 0, each group found under a non-FIFO scheduler is
    searched for in up to that many seeds with asyncio's own callback order
    (see :func:`confirm_feasible`), and the result is stored on the group.
    """
    base = config or SimConfig()
    fast = base.replace(keep_trace=False)
    count = resolve_workers(workers)
    if count > 1 and on_result is not None:
        raise ValueError("on_result needs workers=1")
    groups: dict[str, FailureGroup] = {}
    digests: dict[str, str] = {}
    runs = passed = steps = 0
    sim_time = 0.0
    stopped_early = False
    started = _wall_clock()

    def out_of_time() -> bool:
        return time_budget is not None and _wall_clock() - started >= time_budget

    outcomes: Iterator[_Outcome]
    if count > 1:
        outcomes = _parallel_outcomes(main, seeds, fast, count, chunk_size, out_of_time)
    else:

        def sequential() -> Iterator[_Outcome]:
            for seed in seeds:
                if out_of_time():
                    return
                result = run(main, seed=seed, config=fast)
                if on_result is not None:
                    on_result(result)
                yield _Outcome.of(result)

        outcomes = sequential()

    try:
        for outcome in outcomes:
            runs += 1
            steps += outcome.steps
            sim_time += outcome.sim_time
            if outcome.signature is None:
                passed += 1
                continue
            assert outcome.kind is not None
            group = groups.get(outcome.signature)
            if group is None:
                group = groups[outcome.signature] = FailureGroup(
                    outcome.signature, outcome.kind, outcome.message
                )
                digests[outcome.signature] = outcome.digest
            group.seeds.append(outcome.seed)
            if max_failures is not None and runs - passed >= max_failures:
                stopped_early = True
                break
    finally:
        close = getattr(outcomes, "close", None)
        if close is not None:
            close()

    # Rates in the summary describe exploration, not the follow-up checks.
    explore_time = _wall_clock() - started
    for group in groups.values():
        example = run(main, seed=group.seeds[0], config=base.replace(keep_trace=True))
        group.example = example
        group.reproducible = example.digest == digests[group.signature]
        if not group.reproducible:
            group.divergence = check_determinism(main, group.seeds[0], config=base, runs=4)
        if confirm > 0:
            if base.scheduler == "fifo":
                group.feasible, group.feasible_seed = True, group.seeds[0]
                group.feasible_config = base
            else:
                found = confirm_feasible(main, group.signature, config=base, seeds=range(confirm))
                group.feasible = found is not None
                if found is not None:
                    group.feasible_seed, group.feasible_config = found

    return ExploreReport(
        runs=runs,
        passed=passed,
        groups=list(groups.values()),
        wall_time=explore_time,
        sim_time=sim_time,
        steps=steps,
        stopped_early=stopped_early,
        workers=count,
    )


DEFAULT_CONFIRM_JITTER = 0.001


def confirm_feasible(
    main: MainFunction[Any],
    signature: str,
    *,
    config: SimConfig | None = None,
    seeds: Iterable[int] = range(200),
    jitter: float | None = None,
) -> tuple[int, SimConfig] | None:
    """Look for ``signature`` using only orderings real asyncio can produce.

    The ``random`` scheduler can reorder callbacks that a real loop always runs
    in scheduling order. ``fifo`` keeps asyncio's order exactly, and timer
    jitter perturbs only *when* things happen, which real timing also does. A
    failure found this way is feasible on a real loop; one that is never found
    may still be, but deserves an argument before it is called a bug.

    Returns the first ``(seed, config)`` that fails with ``signature``.
    """
    base = config or SimConfig()
    amount = jitter if jitter is not None else max(base.timer_jitter, DEFAULT_CONFIRM_JITTER)
    variants = [
        base.replace(scheduler="fifo", timer_jitter=amount, keep_trace=False),
        base.replace(scheduler="fifo", timer_jitter=0.0, keep_trace=False),
    ]
    for seed in seeds:
        for variant in variants:
            result = run(main, seed=seed, config=variant)
            if result.failure is not None and result.failure.signature == signature:
                return seed, variant.replace(keep_trace=True)
    return None
