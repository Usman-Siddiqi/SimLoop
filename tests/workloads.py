"""Workloads shared by the determinism tests (also run as a subprocess script)."""

from __future__ import annotations

import asyncio
import random
import sys
import time
import uuid

import simloop


async def busy_system() -> dict[str, object]:
    """Many interacting tasks touching every source of choice the framework has."""
    rng = simloop.rng()
    queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue(maxsize=8)
    lock = asyncio.Lock()
    totals: dict[int, int] = {}
    log: list[str] = []

    async def producer(pid: int) -> None:
        for i in range(20):
            await asyncio.sleep(rng.expovariate(10))
            await queue.put((pid, i))

    async def consumer(cid: int) -> None:
        while True:
            pid, item = await queue.get()
            async with lock:
                totals[pid] = totals.get(pid, 0) + item
                await asyncio.sleep(0)
            log.append(f"{cid}:{pid}:{item}")
            queue.task_done()

    async def flaky(n: int) -> int:
        await asyncio.sleep(random.random())
        if rng.random() < 0.3:
            raise ValueError(n)
        return n

    consumers = [asyncio.create_task(consumer(c)) for c in range(3)]
    await asyncio.gather(*(producer(p) for p in range(4)))
    await queue.join()
    for task in consumers:
        task.cancel()
    outcomes = await asyncio.gather(*(flaky(n) for n in range(10)), return_exceptions=True)
    try:
        async with asyncio.timeout(rng.uniform(0.1, 2)):
            await asyncio.sleep(1)
    except TimeoutError:
        log.append("timeout")
    await asyncio.to_thread(time.sleep, 0.25)
    return {
        "log": log,
        "totals": sorted(totals.items()),
        "outcomes": [repr(o) for o in outcomes],
        "uuid": str(uuid.uuid4()),
        "wall": time.time(),
    }


if __name__ == "__main__":
    seed = int(sys.argv[1])
    result = simloop.run(busy_system, seed=seed, config=simloop.SimConfig(timer_jitter=0.001))
    print(result.digest, result.steps, repr(result.sim_time), hash(repr(result.value)) != 0)
