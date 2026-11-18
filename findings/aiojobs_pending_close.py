"""aiojobs 1.4.0: closing a pending job lets the scheduler exceed its limit.

``Scheduler.active_count`` is ``len(self._jobs) - self._pending.qsize()``.
Closing a job that is still queued starts and cancels its task, and the
task's done callback removes the job from ``_jobs`` while it is still in the
pending queue. ``active_count`` then undercounts by one, and ``_done`` uses
it to decide how many queued jobs to start, so one job too many runs.

Run: python findings/aiojobs_pending_close.py
"""

from __future__ import annotations

import asyncio
import sys

import aiojobs

import simloop

LIMIT = 2


async def harness() -> None:
    """The generic property SimLoop explored: never more than LIMIT jobs running."""
    rng = simloop.rng()
    running = 0

    async def job() -> None:
        nonlocal running
        running += 1
        try:
            assert running <= LIMIT, f"{running} jobs running with limit={LIMIT}"
            await asyncio.sleep(rng.uniform(0.01, 0.1))
        finally:
            running -= 1

    scheduler = aiojobs.Scheduler(limit=LIMIT, close_timeout=1)
    jobs = []
    for _ in range(8):
        jobs.append(await scheduler.spawn(job()))
        await asyncio.sleep(rng.uniform(0, 0.03))
        if rng.random() < 0.3:
            victim = jobs[rng.randrange(len(jobs))]
            simloop.log(f"closing job {jobs.index(victim)} (pending={victim.pending})")
            await victim.close()
    await scheduler.wait_and_close()


async def plain_asyncio() -> int:
    """Minimal reproduction on the standard asyncio event loop."""
    running = peak = 0

    async def job() -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        try:
            await asyncio.sleep(0.1)
        finally:
            running -= 1

    scheduler = aiojobs.Scheduler(limit=1)
    await scheduler.spawn(job())  # starts
    queued = await scheduler.spawn(job())  # limit reached: queued
    await scheduler.spawn(job())  # queued
    await queued.close()  # close the first queued job before it starts
    await asyncio.sleep(0.01)
    print(f"  jobs running: {running} (limit 1)")
    await scheduler.wait_and_close()
    return peak


def main() -> int:
    print("plain asyncio:")
    peak = asyncio.run(plain_asyncio())
    report = simloop.explore(harness, seeds=range(300))
    print("simloop explore:")
    print("  " + report.summary().replace("\n", "\n  "))
    reproduced = peak > 1 and not report.ok
    print("REPRODUCED" if reproduced else "NOT REPRODUCED")
    return 0 if reproduced else 1


if __name__ == "__main__":
    sys.exit(main())
