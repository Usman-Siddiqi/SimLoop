"""aiorwlock: readers and writers exclude each other under acquire timeouts.

Clean on aiorwlock 1.6.0 (see docs/FINDINGS.md, "Tested, nothing found").

Run: python findings/aiorwlock_exclusion.py
"""

from __future__ import annotations

import asyncio
import sys

from aiorwlock import RWLock

import simloop


async def workload() -> None:
    rng = simloop.rng()
    lock = RWLock(fast=rng.random() < 0.5)
    readers = writers = 0

    async def user(n: int) -> None:
        nonlocal readers, writers
        for _ in range(6):
            await asyncio.sleep(rng.uniform(0, 0.02))
            write = rng.random() < 0.4
            side = lock.writer if write else lock.reader
            timeout = rng.uniform(0, 0.03) if rng.random() < 0.5 else None
            try:
                await asyncio.wait_for(side.acquire(), timeout)
            except TimeoutError:
                continue
            try:
                if write:
                    assert readers == 0, f"writer entered with {readers} readers"
                    assert writers == 0, "two writers at once"
                    writers += 1
                else:
                    assert writers == 0, "reader entered during a write"
                    readers += 1
                await asyncio.sleep(rng.uniform(0, 0.02))
            finally:
                if write:
                    writers -= 1
                else:
                    readers -= 1
                side.release()

    async with asyncio.TaskGroup() as tg:
        for n in range(5):
            tg.create_task(user(n))
    assert not lock.reader.locked, "read lock still held at the end"
    assert not lock.writer.locked, "write lock still held at the end"


def main() -> int:
    report = simloop.explore(workload, seeds=range(1000))
    print(report.summary())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
