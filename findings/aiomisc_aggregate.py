"""aiomisc 18.0.40: ``@aggregate`` under cancellation (clean).

Concurrent single-argument calls are batched into one call of the decorated
function. With random batch windows, ``max_count`` and caller timeouts,
every caller that was not cancelled got its own result, and no argument was
processed by a completed batch more than once. (A batch whose leading caller
is cancelled is restarted by the next caller; only completed batches count.)

Run: python findings/aiomisc_aggregate.py
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import sys

import aiomisc

import simloop


async def aggregate_once() -> None:
    rng = simloop.rng()
    processed: collections.Counter[int] = collections.Counter()

    @aiomisc.aggregate(leeway_ms=rng.choice([5, 20, 50]), max_count=rng.choice([None, 2, 3]))
    async def batch(*args: int) -> list[int]:
        await asyncio.sleep(rng.uniform(0, 0.03))
        processed.update(args)
        return [arg * 10 for arg in args]

    async def caller(i: int) -> None:
        await asyncio.sleep(rng.uniform(0, 0.05))
        timeout = rng.choice([None, None, rng.uniform(0, 0.08)])
        with contextlib.suppress(TimeoutError):
            assert await asyncio.wait_for(batch(i), timeout) == i * 10

    await asyncio.gather(*(caller(i) for i in range(rng.randint(1, 6))))
    await asyncio.sleep(1)
    assert all(count == 1 for count in processed.values()), dict(processed)


def main() -> int:
    for scheduler in ("random", "pct", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(aggregate_once, seeds=range(1000), config=config)
        print(f"aggregate_once {scheduler}: {report.summary().splitlines()[0]}")
        assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
