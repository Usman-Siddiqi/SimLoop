"""aiologic 0.17.1: ``Lock``, ``Semaphore`` and ``Queue`` under cancellation (clean).

Workers enter a ``Lock`` and a ``Semaphore(1-3)`` for random times under
random ``wait_for`` timeouts; producers and consumers use a ``Queue`` of
size 1, 2 or unbounded with timeouts on both sides. Mutual exclusion and the
semaphore bound held, every permit came back, and every item that was put
was delivered exactly once.

Run: python findings/aiologic_primitives.py
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import random
import sys
from collections.abc import Awaitable, Callable

import aiologic

import simloop


async def timeouts(
    rng: random.Random, op: Callable[[], Awaitable[None]], times: int, users: int
) -> None:
    async def user() -> None:
        for _ in range(times):
            await asyncio.sleep(rng.uniform(0, 0.02))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(op(), rng.choice([None, rng.uniform(0, 0.04)]))

    await asyncio.wait_for(asyncio.gather(*(user() for _ in range(users))), 60)


async def lock() -> None:
    rng = simloop.rng()
    mutex = aiologic.Lock()
    inside = 0

    async def op() -> None:
        nonlocal inside
        async with mutex:
            inside += 1
            assert inside == 1, "two tasks inside the lock"
            try:
                await asyncio.sleep(rng.uniform(0, 0.03))
            finally:
                inside -= 1

    await timeouts(rng, op, times=4, users=4)
    await asyncio.wait_for(mutex.async_acquire(), 1)


async def semaphore() -> None:
    rng = simloop.rng()
    limit = rng.randint(1, 3)
    sem = aiologic.Semaphore(limit)
    inside = 0

    async def op() -> None:
        nonlocal inside
        async with sem:
            inside += 1
            assert inside <= limit, f"{inside} tasks inside Semaphore({limit})"
            try:
                await asyncio.sleep(rng.uniform(0, 0.03))
            finally:
                inside -= 1

    await timeouts(rng, op, times=4, users=5)
    for _ in range(limit):
        await asyncio.wait_for(sem.async_acquire(), 1)


async def queue() -> None:
    rng = simloop.rng()
    items: aiologic.Queue[tuple[int, int]] = aiologic.Queue(rng.choice([None, 1, 2]))
    sent: set[tuple[int, int]] = set()
    received: collections.Counter[tuple[int, int]] = collections.Counter()

    async def producer(n: int) -> None:
        for i in range(5):
            with contextlib.suppress(TimeoutError):
                timeout = rng.choice([None, rng.uniform(0, 0.03)])
                await asyncio.wait_for(items.async_put((n, i)), timeout)
                sent.add((n, i))
            await asyncio.sleep(rng.uniform(0, 0.01))

    async def consumer() -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                timeout = rng.choice([None, rng.uniform(0, 0.03)])
                received[await asyncio.wait_for(items.async_get(), timeout)] += 1
                await asyncio.sleep(rng.uniform(0, 0.01))

    consumers = [asyncio.create_task(consumer()) for _ in range(2)]
    await asyncio.gather(*(producer(n) for n in range(3)))
    await asyncio.sleep(5)
    for task in consumers:
        task.cancel()
    await asyncio.gather(*consumers, return_exceptions=True)
    with contextlib.suppress(aiologic.QueueEmpty):
        while True:
            received[items.green_get(blocking=False)] += 1
    twice = {item: count for item, count in received.items() if count > 1}
    assert not twice, f"delivered more than once: {twice}"
    lost = sent - set(received)
    assert not lost, f"put but never delivered: {sorted(lost)}"


def main() -> int:
    for workload in (lock, semaphore, queue):
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
