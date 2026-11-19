"""pyrate-limiter 4.5.1: async acquisitions never exceed the configured rates (clean).

Four workers acquire permits of weight 1 or 2 from one ``Limiter`` with two
rates (5 per second and 12 per 3 seconds), with the limiter's own timeout
or an outer ``wait_for`` that cancels the wait. Every window of each rate,
measured from the times acquisitions were granted, holds at most the
rate's limit. The in-memory bucket normally leaks expired items from a
background thread, which a simulation does not run; the harness marks the
bucket async so the limiter leaks it from an event-loop task instead.

Run: python findings/pyrate_limiter_window.py
"""

from __future__ import annotations

import asyncio
import sys

from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

import simloop

RATES = [(5, 1.0), (12, 3.0)]


class LoopLeakedBucket(InMemoryBucket):
    is_async = True  # leak from an event-loop task instead of a thread


async def harness() -> None:
    """No window of any rate holds more than its limit."""
    rng = simloop.rng()
    loop = asyncio.get_running_loop()
    rates = [Rate(limit, int(seconds * Duration.SECOND)) for limit, seconds in RATES]
    limiter = Limiter(LoopLeakedBucket(rates))
    grants: list[tuple[float, int]] = []

    async def worker() -> None:
        for _ in range(15):
            weight = rng.choice([1, 1, 2])
            try:
                if rng.random() < 0.2:
                    acquire = limiter.try_acquire_async("key", weight)
                    granted = await asyncio.wait_for(acquire, rng.uniform(0.01, 1.0))
                else:
                    timeout = rng.choice([-1, -1, rng.uniform(0.05, 1.5)])
                    granted = await limiter.try_acquire_async("key", weight, timeout=timeout)
            except TimeoutError:
                granted = False
            if granted:
                grants.append((loop.time(), weight))
            await asyncio.sleep(rng.uniform(0, 0.2))

    await asyncio.gather(*(worker() for _ in range(4)))
    grants.sort()
    for limit, seconds in RATES:
        for i, (start, _) in enumerate(grants):
            # Timestamps are whole milliseconds, so allow for rounding at the edge.
            used = sum(w for t, w in grants[i:] if t < start + seconds - 0.002)
            assert used <= limit, f"{used} permits in the {seconds} s from t={start:.3f}"


def main() -> int:
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(harness, seeds=range(300), config=config)
        print(f"{scheduler}: {report.summary()}")
        assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
