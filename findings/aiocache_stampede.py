"""aiocache 0.12.3: ``cached_stampede`` stops protecting after a failed call.

``cached_stampede`` holds a ``RedLock`` while the first caller computes the
value; the others wait for the lock's release event. When the holder ends
without storing a value (its function raised, or it was cancelled), the
release sets the event, every waiter wakes, finds the cache still empty and
calls the function itself, without taking the lock again. A burst of N
callers that hits one failure therefore runs the function N times
concurrently: the stampede the decorator exists to prevent.

``RedLock`` documents that "blocking just happens for the first time" for an
expired lease; the failure case is not documented. Filed as a suspected
issue: it may be intended.

Run: python findings/aiocache_stampede.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

from aiocache import SimpleMemoryCache, cached_stampede

import simloop


async def harness() -> None:
    """The property SimLoop explored: one computation per burst, unless cancelled."""
    rng = simloop.rng()
    calls = finished = 0

    @cached_stampede(lease=1.0, cache=SimpleMemoryCache, key="k")
    async def compute() -> str:
        nonlocal calls, finished
        calls += 1
        await asyncio.sleep(rng.uniform(0.0, 0.5))
        finished += 1
        return "value"

    async def caller() -> None:
        await asyncio.sleep(rng.choice([0.0, 0.0, rng.uniform(0, 0.6)]))
        timeout = rng.uniform(0, 0.4) if rng.random() < 0.3 else None
        with contextlib.suppress(TimeoutError):
            assert await asyncio.wait_for(compute(), timeout) == "value"

    await asyncio.gather(*(caller() for _ in range(rng.randint(2, 6))))
    cancelled = calls - finished
    # Each cancelled computation may hand the lock to one new caller; more
    # computations than that mean waiters ran without the lock.
    assert calls <= cancelled + 1, f"compute ran {calls} times, {cancelled} cancelled"


running = peak = attempts = 0


@cached_stampede(lease=10, cache=SimpleMemoryCache, key="plain")
async def _fetch() -> str:
    global running, peak, attempts
    attempts += 1
    running += 1
    peak = max(peak, running)
    try:
        await asyncio.sleep(0.05)
        if attempts == 1:
            raise ConnectionError("backend hiccup")
        return "value"
    finally:
        running -= 1


async def plain_asyncio() -> int:
    """Minimal reproduction on the standard asyncio event loop."""

    async def caller() -> str | None:
        try:
            return await _fetch()
        except ConnectionError:
            return None

    await asyncio.gather(*(caller() for _ in range(10)))
    print(f"  10 callers, first call fails: {attempts} calls, {peak} at once (lease 10s)")
    return peak


def main() -> int:
    print("plain asyncio:")
    peak = asyncio.run(plain_asyncio())
    fifo = simloop.SimConfig(scheduler="fifo")
    report = simloop.explore(harness, seeds=range(1000), config=fifo)
    print("simloop explore (scheduler='fifo'):")
    print("  " + report.summary().replace("\n", "\n  "))
    reproduced = peak > 1 and not report.ok
    print("REPRODUCED" if reproduced else "NOT REPRODUCED")
    return 0 if reproduced else 1


if __name__ == "__main__":
    sys.exit(main())
