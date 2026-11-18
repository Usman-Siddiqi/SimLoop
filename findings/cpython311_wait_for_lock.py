"""CPython 3.11: cancelling ``wait_for`` mid-timeout breaks a Condition's lock.

On 3.11, ``asyncio.wait_for`` runs its awaitable in a separate inner task.
When the timeout fires it cancels that task and waits for it to finish, but
that wait is deliberately cancellable. If the outer task is cancelled while
the inner task is still cleaning up, ``wait_for`` returns at once and leaves
the inner task running.

With ``async with cond: await asyncio.wait_for(cond.wait(), t)`` the inner
task's cleanup is re-acquiring the lock. When the lock is busy and the outer
task is cancelled in that window, ``async with`` releases a lock the outer
task does not hold, i.e. somebody else's. A task queued for the lock walks
into the critical section while its holder is still in it, and the orphaned
inner task later takes the lock with nobody left to release it.

Python 3.12 rewrote ``wait_for`` on top of ``asyncio.timeout``
(python/cpython GH-96764, PR #98518), so the awaitable runs in the calling
task and the problem is gone. This script is a rediscovery of behaviour that
is already fixed upstream, kept as a regression check for SimLoop: on 3.11 it
must find the violation, on 3.12+ it must not.

Run: python findings/cpython311_wait_for_lock.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

import simloop

AFFECTED = sys.version_info < (3, 12)


async def harness() -> None:
    """The property SimLoop explored: at most one task inside ``async with cond``."""
    rng = simloop.rng()
    cond = asyncio.Condition()
    inside = 0

    async def critical(hold: float) -> None:
        nonlocal inside
        inside += 1
        assert inside == 1, f"{inside} tasks inside the condition's lock"
        await asyncio.sleep(hold)
        inside -= 1

    async def waiter() -> None:
        async with cond:
            await asyncio.wait_for(cond.wait(), rng.uniform(0.001, 0.01))

    async def worker() -> None:
        await asyncio.sleep(rng.uniform(0, 0.01))
        async with cond:
            await critical(rng.uniform(0, 0.01))

    waiters = [asyncio.create_task(waiter()) for _ in range(2)]
    workers = [asyncio.create_task(worker()) for _ in range(3)]
    await asyncio.sleep(rng.uniform(0, 0.015))
    for task in waiters:
        task.cancel()
    for task in waiters:
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await task
    await asyncio.gather(*workers)
    await asyncio.sleep(0.05)
    assert not cond.locked(), "the lock is still held after every task finished"


async def plain() -> list[str]:
    """The same window with fixed timings, on a real asyncio loop."""
    cond = asyncio.Condition()
    seen: list[str] = []
    holder_inside = False

    async def waiter() -> None:
        async with cond:
            await asyncio.wait_for(cond.wait(), 0.01)

    async def holder() -> None:
        nonlocal holder_inside
        await asyncio.sleep(0.005)
        async with cond:
            holder_inside = True
            await asyncio.sleep(0.02)
            holder_inside = False

    async def intruder() -> None:
        await asyncio.sleep(0.007)  # queues for the lock behind holder
        async with cond:
            if holder_inside:
                seen.append("intruder entered while holder was inside")

    w = asyncio.create_task(waiter())
    tasks = [asyncio.create_task(holder()), asyncio.create_task(intruder())]
    await asyncio.sleep(0.015)  # the timeout fired; the inner task waits for the lock
    w.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await w
    await asyncio.gather(*tasks)
    return seen


def main() -> int:
    config = simloop.SimConfig(scheduler="fifo", timer_jitter=0.001)
    report = simloop.explore(harness, seeds=range(300), config=config)
    print(f"Python {sys.version.split()[0]}: {report.summary()}")
    real = asyncio.run(plain())
    print(f"real asyncio loop: {real or 'no violation'}")
    if AFFECTED:
        assert not report.ok, "SimLoop should find the violation on 3.11"
        assert real, "the plain asyncio repro should show the violation on 3.11"
        seeds = {group.message: group.seeds[0] for group in report.groups}
        print(f"first seeds (scheduler=fifo, timer_jitter=0.001): {seeds}")
    else:
        assert report.ok, "3.12+ should not violate mutual exclusion"
        assert not real
    return 0


if __name__ == "__main__":
    sys.exit(main())
