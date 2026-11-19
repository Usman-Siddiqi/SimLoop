"""tenacity 9.2.1: retry deadlines and cancellation (clean).

With ``stop_before_delay(T)`` no attempt started after ``T`` (allowing for
how late the loop wakes a timer), and cancelling the caller stopped the
retries. ``stop_after_delay`` starts attempts after its delay, as its
docstring says; this harness checks the stricter variant.

Run: python findings/tenacity_retry.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

import tenacity

import simloop

JITTER = 0.001


class Flaky(Exception):
    pass


async def deadline() -> None:
    rng = simloop.rng()
    limit = rng.uniform(0.1, 2.0)
    work = rng.uniform(0, 0.3)
    p_fail = rng.random()
    starts: list[float] = []

    @tenacity.retry(
        stop=tenacity.stop_before_delay(limit),
        wait=tenacity.wait_random_exponential(multiplier=0.05, max=1),
        reraise=True,
    )
    async def op() -> str:
        starts.append(simloop.now())
        await asyncio.sleep(rng.uniform(0, work))
        if rng.random() < p_fail:
            raise Flaky
        return "ok"

    began = simloop.now()
    with contextlib.suppress(Flaky):
        await op()
    late = [round(t - began, 4) for t in starts if t - began > limit + JITTER]
    assert not late, f"stop_before_delay({limit:.4f}), attempts started at {late}"


async def cancel_stops_retrying() -> None:
    rng = simloop.rng()
    calls = 0

    @tenacity.retry(wait=tenacity.wait_fixed(0.05), stop=tenacity.stop_after_attempt(20))
    async def op() -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        raise Flaky

    task = asyncio.get_running_loop().create_task(op())
    await asyncio.sleep(rng.uniform(0, 0.3))
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    before = calls
    await asyncio.sleep(2)
    assert calls == before, "op kept retrying after the caller was cancelled"


def main() -> int:
    for workload in (deadline, cancel_stops_retrying):
        for scheduler in ("random", "fifo"):
            config = simloop.SimConfig(
                scheduler=scheduler, timer_jitter=JITTER, strict_cancellation=True
            )
            report = simloop.explore(workload, seeds=range(1000), config=config)
            print(f"{workload.__name__} {scheduler}: {report.summary().splitlines()[0]}")
            assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
