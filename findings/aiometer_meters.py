"""aiometer 1.0.0: meters hold; ``run_any`` waits for every function.

``amap`` and ``run_on_each`` kept ``max_at_once`` and ``max_per_second`` and
left nothing running after a cancellation or failure (clean).

``run_any`` is documented as running the functions "until the first one
completes" and returning "the first available result". It is ``amap``
plus taking the first item, and leaving ``amap``'s context waits for its
task group, so ``run_any`` returns only when the slowest function has
finished, and the others are never cancelled. Filed as a suspected issue:
waiting may be intended, but it is not what the docs suggest.

Run: python findings/aiometer_meters.py
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import sys
import time

import aiometer

import simloop


async def amap_limits() -> None:
    """``amap`` keeps ``max_at_once`` and ``max_per_second`` and yields every result."""
    rng = simloop.rng()
    n = rng.randint(1, 12)
    at_once = rng.choice([None, 1, 2, 3])
    per_second = rng.choice([None, 5.0, 20.0, 100.0])
    running = 0
    starts: list[float] = []

    async def job(i: int) -> int:
        nonlocal running
        running += 1
        starts.append(simloop.now())
        assert at_once is None or running <= at_once, f"{running} running, max_at_once={at_once}"
        await asyncio.sleep(rng.uniform(0, 0.2))
        running -= 1
        return i

    got = []
    async with aiometer.amap(
        job, list(range(n)), max_at_once=at_once, max_per_second=per_second
    ) as results:
        async for result in results:
            got.append(result)
    assert sorted(got) == list(range(n)), got
    if per_second:
        gaps = [b - a for a, b in itertools.pairwise(starts)]
        assert all(gap >= 1 / per_second - 1e-9 for gap in gaps), (per_second, gaps)


async def run_any_first() -> None:
    """``run_any`` returns when the first function completes."""
    rng = simloop.rng()
    durations = [rng.uniform(0, 0.1) for _ in range(rng.randint(2, 6))]

    def make(i: int):  # type: ignore[no-untyped-def]
        async def fn() -> int:
            await asyncio.sleep(durations[i])
            return i

        return fn

    started = simloop.now()
    first = await aiometer.run_any([make(i) for i in range(len(durations))])
    waited = simloop.now() - started
    assert waited <= durations[first] + 1e-9, (
        f"first result after {durations[first]:.3f}s, run_any returned after {waited:.3f}s"
    )


async def plain_run_any() -> float:
    async def fast() -> str:
        await asyncio.sleep(0.05)
        return "fast"

    async def slow() -> str:
        await asyncio.sleep(1.0)
        return "slow"

    started = time.perf_counter()
    assert await aiometer.run_any([fast, slow]) == "fast"
    return time.perf_counter() - started


async def cancelled_run() -> None:
    """Cancelling or failing ``run_on_each`` leaves no job running."""
    rng = simloop.rng()
    at_once = rng.choice([1, 2, 3])
    running = 0

    async def job(i: int) -> None:
        nonlocal running
        running += 1
        assert running <= at_once
        try:
            await asyncio.sleep(rng.uniform(0, 0.1))
            if rng.random() < 0.2:
                raise ValueError(i)
        finally:
            running -= 1

    with contextlib.suppress(TimeoutError, ValueError, ExceptionGroup):
        await asyncio.wait_for(
            aiometer.run_on_each(job, list(range(10)), max_at_once=at_once), rng.uniform(0, 0.3)
        )
    await asyncio.sleep(0.5)
    assert running == 0, f"{running} jobs still running after run_on_each ended"


def main() -> int:
    for workload in (amap_limits, cancelled_run):
        for scheduler in ("random", "pct", "fifo"):
            config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
            if scheduler == "fifo":
                config = config.replace(timer_jitter=0.001)
            report = simloop.explore(workload, seeds=range(1000), config=config)
            print(f"{workload.__name__} {scheduler}: {report.summary().splitlines()[0]}")
            assert report.ok, report.summary()
    report = simloop.explore(run_any_first, seeds=range(1000))
    print(f"run_any_first: {report.summary()}")
    assert not report.ok, "run_any should be seen waiting for the slower functions"
    waited = asyncio.run(plain_run_any())
    print(f"real asyncio loop: run_any returned the 0.05 s result after {waited:.2f} s")
    assert waited > 0.9
    return 0


if __name__ == "__main__":
    sys.exit(main())
