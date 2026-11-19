"""aiostream 0.8.2: merge, map with task_limit, early exit (clean).

``merge`` delivered every item of every source once and in per-source order;
``map(task_limit=N)`` never ran more than N calls and kept order when asked;
leaving a merged stream early (``break`` or a timeout) closed every source.

Run: python findings/aiostream_operators.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

from aiostream import pipe, stream

import simloop


async def merge_all():
    rng = simloop.rng()
    n = rng.randint(1, 4)
    sizes = [rng.randint(0, 5) for _ in range(n)]

    async def src(k):
        for i in range(sizes[k]):
            await asyncio.sleep(rng.choice([0, rng.uniform(0, 0.01)]))
            yield (k, i)

    got = []
    async with stream.merge(*(src(k) for k in range(n))).stream() as s:
        async for x in s:
            got.append(x)
    assert sorted(got) == sorted((k, i) for k in range(n) for i in range(sizes[k])), got
    for k in range(n):
        assert [i for kk, i in got if kk == k] == list(range(sizes[k]))


async def map_limit():
    rng = simloop.rng()
    limit = rng.randint(1, 3)
    ordered = rng.random() < 0.5
    running = 0
    peak = 0

    async def f(x):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        assert running <= limit, (running, limit)
        try:
            await asyncio.sleep(rng.uniform(0, 0.02))
        finally:
            running -= 1
        return x * 2

    n = rng.randint(0, 8)
    xs = stream.iterate(range(n)) | pipe.map(f, task_limit=limit, ordered=ordered)
    got = []
    async with xs.stream() as s:
        async for y in s:
            got.append(y)
    assert (got if ordered else sorted(got)) == [2 * i for i in range(n)], (ordered, got)


async def early_exit():
    rng = simloop.rng()
    alive = 0

    async def src(k):
        nonlocal alive
        alive += 1
        try:
            for i in range(100):
                await asyncio.sleep(rng.uniform(0, 0.01))
                yield (k, i)
        finally:
            alive -= 1

    take = rng.randint(1, 10)
    async with stream.merge(*(src(k) for k in range(3))).stream() as s:
        count = 0
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(rng.choice([None, rng.uniform(0, 0.05)])):
                async for _item in s:
                    count += 1
                    if count >= take:
                        break
    await asyncio.sleep(0.5)
    assert alive == 0, f"{alive} sources still running after the stream was closed"


def main() -> int:
    for workload in (merge_all, map_limit, early_exit):
        for scheduler in ("random", "pct", "fifo"):
            config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
            if scheduler == "fifo":
                config = config.replace(timer_jitter=0.001)
            report = simloop.explore(workload, seeds=range(1000), config=config)
            print(f"{workload.__name__} {scheduler}: {report.summary().splitlines()[0]}")
            assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
