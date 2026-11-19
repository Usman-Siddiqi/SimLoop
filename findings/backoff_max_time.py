"""backoff 2.2.1: ``max_time`` is checked against a stale, wall-clock elapsed time.

``on_exception(..., max_time=T)`` is documented as "the maximum total amount
of time to try for before giving up". Two things break that bound:

1. ``elapsed`` is measured at the top of the retry loop, before the attempt
   runs. A failed attempt that itself ran past ``T`` is judged by the time
   at which it started, so backoff sleeps and starts one more attempt after
   the deadline (upstream issue #186, open pull request #130).
2. ``elapsed`` comes from ``datetime.datetime.now()``, the wall clock. When
   the host's clock steps backwards during the retries (NTP, a VM resume),
   the deadline moves with it: a 30 s step back turns ``max_time=1`` into
   31 s of retrying (upstream issue #149, open pull request #150).

Both are already known upstream; this script rediscovers the first from a
timing property and shows the second with a SimLoop clock-jump fault.

Run: python findings/backoff_max_time.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

import backoff

import simloop
from simloop import faults


class Flaky(Exception):
    pass


async def no_attempt_after_max_time() -> None:
    rng = simloop.rng()
    max_time = rng.uniform(0.1, 2.0)
    work = rng.uniform(0, 0.3)
    p_fail = rng.random()
    starts: list[float] = []

    @backoff.on_exception(
        backoff.expo, Flaky, max_time=max_time, factor=0.05, jitter=backoff.full_jitter
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
    late = [round(t - began, 3) for t in starts if t - began > max_time + 1e-9]
    assert not late, f"max_time={max_time:.3f}s, attempts started at {late}s"


async def clock_steps_back() -> float:
    simloop.network()
    starts: list[float] = []

    @backoff.on_exception(backoff.constant, Flaky, interval=0.1, jitter=None, max_time=1.0)
    async def op() -> None:
        starts.append(simloop.now())
        raise Flaky

    faults.schedule(faults.at(0.35, faults.clock_jump("main", -30.0))).start()
    with contextlib.suppress(Flaky):
        await op()
    return starts[-1]


async def plain() -> tuple[int, float]:
    loop = asyncio.get_running_loop()
    attempts = 0

    @backoff.on_exception(backoff.constant, Flaky, interval=0.01, jitter=None, max_time=0.1)
    async def op() -> None:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.2)  # each attempt alone outlasts max_time
        raise Flaky

    began = loop.time()
    with contextlib.suppress(Flaky):
        await op()
    return attempts, loop.time() - began


def main() -> int:
    report = simloop.explore(no_attempt_after_max_time, seeds=range(1000))
    print(report.summary())
    assert not report.ok
    last = simloop.run(clock_steps_back).value
    assert last is not None
    print(f"with a 30 s backward clock step, max_time=1.0 kept retrying until t={last:.1f}s")
    assert last > 30
    attempts, took = asyncio.run(plain())
    print(f"real asyncio loop: max_time=0.1, {attempts} attempts, {took:.2f}s")
    assert attempts == 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
