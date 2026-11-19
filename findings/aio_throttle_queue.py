"""aio-throttle 1.7.0: cancelled waiters still count as queued; the random-reject quota is inverted.

Six workers send requests through one ``Throttler(capacity_limit=2,
queue_limit=2)`` with random priorities, each under a random
``asyncio.timeout``, and hold an accepted slot for a while. Whenever a
request is rejected because the queue is full, the harness counts the
waiters that are really still waiting.

1. ``LifoSemaphore.acquire`` cancels the future of a waiter that gives up
   but leaves it in ``_waiters``, and ``waiting`` is ``len(_waiters)``. The
   throttler's queue check uses ``waiting``, so a request is rejected with
   ``REJECTED_DUE_TO_FULL_QUEUE`` while fewer than ``queue_limit`` callers
   wait, and every ``LOW`` priority request is rejected while a stale
   waiter remains. A release only clears stale futures above the first
   live waiter in the stack, so stale ones below it persist under load.
2. ``RandomRejectThrottleQuota.can_be_accepted`` returns
   ``random() < reject_probability``: it accepts with the reject
   probability, so ``reject_probability=0.1`` rejects 90% of requests.

Run: python findings/aio_throttle_queue.py
"""

from __future__ import annotations

import asyncio
import sys

from aio_throttle import RandomRejectThrottleQuota, ThrottlePriority, Throttler, ThrottleResult

import simloop

CAPACITY, QUEUE_LIMIT = 2, 2


def live_waiters(throttler: Throttler) -> int:
    return sum(not future.done() for future in throttler._semaphore._waiters)


async def harness() -> None:
    """A request is rejected for a full queue only if enough callers really wait."""
    rng = simloop.rng()
    throttler = Throttler(capacity_limit=CAPACITY, queue_limit=QUEUE_LIMIT)

    async def request(priority: ThrottlePriority) -> None:
        live = live_waiters(throttler)
        available = throttler._semaphore.available
        async with throttler.throttle(priority=priority) as result:
            if result == ThrottleResult.REJECTED_DUE_TO_FULL_QUEUE:
                assert (priority == ThrottlePriority.LOW and live > 0) or (
                    live >= QUEUE_LIMIT and available == 0
                ), (
                    f"a {priority} request was rejected for a full queue with {live} live"
                    f" waiters (queue_limit {QUEUE_LIMIT}), {available} free slots and"
                    f" {throttler.stats.queue_size} counted as waiting"
                )
            elif result == ThrottleResult.ACCEPTED:
                await asyncio.sleep(rng.uniform(0.01, 0.1))

    async def worker() -> None:
        for _ in range(15):
            priority = rng.choice(list(ThrottlePriority))
            try:
                async with asyncio.timeout(rng.uniform(0.005, 0.2)):
                    await request(priority)
            except TimeoutError:
                pass
            await asyncio.sleep(rng.uniform(0, 0.03))

    await asyncio.gather(*(worker() for _ in range(6)))


async def plain_queue() -> tuple[str, int, int]:
    throttler = Throttler(capacity_limit=1, queue_limit=1)
    holding = asyncio.Event()
    done = asyncio.Event()

    async def holder() -> None:
        async with throttler.throttle():
            holding.set()
            await done.wait()

    task = asyncio.create_task(holder())
    await holding.wait()
    impatient = asyncio.create_task(throttler.throttle().__aenter__())
    await asyncio.sleep(0.01)
    impatient.cancel()  # the only waiter gives up
    await asyncio.gather(impatient, return_exceptions=True)
    async with throttler.throttle() as result:
        outcome = str(result)
    counted = throttler.stats.queue_size
    done.set()
    await task
    return outcome, live_waiters(throttler), counted


def plain_random_reject() -> float:
    quota = RandomRejectThrottleQuota(0.1, seed=1)
    accepted = sum(quota.can_be_accepted() for _ in range(10_000))
    return 1 - accepted / 10_000


def main() -> int:
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(harness, seeds=range(300), config=config)
        print(f"{scheduler}: {report.summary()}")
        assert not report.ok
        assert all("rejected for a full queue" in group.message for group in report.groups)
    outcome, live, counted = asyncio.run(plain_queue())
    print(
        "real asyncio loop, capacity 1 held, queue_limit=1, the only waiter cancelled:"
        f" the next request is {outcome!r} ({live} live waiters, {counted} counted)"
    )
    assert outcome == str(ThrottleResult.REJECTED_DUE_TO_FULL_QUEUE)
    assert live == 0
    rejected = plain_random_reject()
    print(f"RandomRejectThrottleQuota(0.1) rejected {rejected:.1%} of 10,000 requests")
    assert rejected > 0.85
    return 0


if __name__ == "__main__":
    sys.exit(main())
