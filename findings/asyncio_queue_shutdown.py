"""CPython 3.13+ ``asyncio.Queue.shutdown()`` under concurrency and cancellation (clean).

Producers and consumers share a ``Queue``, ``LifoQueue`` or
``PriorityQueue`` (``maxsize`` 0, 1, 2 or 4), using blocking, ``*_nowait``
and ``wait_for``-bounded puts and gets, with some ``join()`` waiters; at a
random time the queue is shut down, gracefully or with ``immediate=True``.
Checked against the documentation: ``qsize()`` never exceeds ``maxsize``;
``QueueShutDown`` is raised only after ``shutdown()``, and by ``get`` only
once the queue is empty (unless immediate); every blocked producer and
consumer wakes; no item is received twice or without being put; after a
graceful shutdown every item put was received, and after an immediate one
only drained items are missing; ``join()`` returns once no task is
unfinished. All held on 3.13.18 and 3.14.6, 1000 seeds each with the
random, fifo and PCT schedulers and ``strict_cancellation=True``.

Run: python findings/asyncio_queue_shutdown.py
"""

from __future__ import annotations

import asyncio
import sys
from collections import Counter

import simloop


async def harness() -> None:
    """Items are conserved, limits hold, and shutdown wakes everyone it should."""
    rng = simloop.rng()
    maxsize = rng.choice([0, 1, 2, 4])
    kind = rng.choice([asyncio.Queue, asyncio.LifoQueue, asyncio.PriorityQueue])
    q: asyncio.Queue[int] = kind(maxsize)
    put_ok: Counter[int] = Counter()
    got: Counter[int] = Counter()
    done_calls = [0]
    shut = {"at": None, "immediate": None}
    errors: list[str] = []
    seq = iter(range(10_000))

    async def producer(n: int) -> None:
        for _ in range(rng.randint(1, 15)):
            item = next(seq)
            try:
                if rng.random() < 0.3:
                    await asyncio.wait_for(q.put(item), rng.uniform(0, 0.02))
                elif rng.random() < 0.2:
                    q.put_nowait(item)
                else:
                    await q.put(item)
                put_ok[item] += 1
                if maxsize:
                    assert q.qsize() <= maxsize, f"qsize {q.qsize()} > maxsize {maxsize}"
            except (TimeoutError, asyncio.QueueFull):
                pass
            except asyncio.QueueShutDown:
                if shut["at"] is None:
                    errors.append("put raised QueueShutDown before shutdown()")
                return
            await asyncio.sleep(rng.uniform(0, 0.01))

    async def consumer(n: int) -> None:
        while True:
            try:
                if rng.random() < 0.3:
                    item = await asyncio.wait_for(q.get(), rng.uniform(0, 0.02))
                elif rng.random() < 0.2:
                    item = q.get_nowait()
                else:
                    item = await q.get()
            except (TimeoutError, asyncio.QueueEmpty):
                await asyncio.sleep(rng.uniform(0, 0.01))
                continue
            except asyncio.QueueShutDown:
                if shut["at"] is None:
                    errors.append("get raised QueueShutDown before shutdown()")
                if not shut["immediate"] and q.qsize():
                    errors.append(f"get raised QueueShutDown with {q.qsize()} items left")
                return
            got[item] += 1
            await asyncio.sleep(rng.uniform(0, 0.01))
            if rng.random() < 0.9:
                try:
                    q.task_done()
                    done_calls[0] += 1
                except ValueError:
                    errors.append("task_done raised ValueError")

    async def joiner() -> None:
        await q.join()

    loop = asyncio.get_running_loop()
    producers = [asyncio.create_task(producer(n)) for n in range(rng.randint(1, 3))]
    consumers = [asyncio.create_task(consumer(n)) for n in range(rng.randint(1, 3))]
    joiners = [asyncio.create_task(joiner()) for _ in range(rng.randint(0, 2))]
    await asyncio.sleep(rng.uniform(0, 0.15))
    immediate = rng.random() < 0.5
    shut["at"] = loop.time()
    shut["immediate"] = immediate
    before = q.qsize()
    q.shutdown(immediate=immediate)
    if immediate:
        assert q.qsize() == 0, f"{q.qsize()} items left after shutdown(immediate=True)"
    done, pending = await asyncio.wait(producers + consumers, timeout=10)
    assert not pending, (
        f"{len(pending)} producer/consumer task(s) still blocked 10 s after"
        f" shutdown(immediate={immediate})"
    )
    for t in done:
        t.result()
    assert not errors, errors
    twice = [i for i, n in got.items() if n > 1]
    assert not twice, f"items {twice} received twice"
    phantom = [i for i in got if not put_ok[i]]
    assert not phantom, f"items {phantom} received but never put"
    lost = sorted(i for i in put_ok if not got[i])
    if not immediate:
        assert not lost, f"items {lost} were put but never received (graceful shutdown)"
    else:
        assert len(lost) <= before, f"{len(lost)} items lost, but only {before} were drained"
    _, jpending = await asyncio.wait(joiners, timeout=1) if joiners else (set(), set())
    unfinished = q._unfinished_tasks  # type: ignore[attr-defined]
    if unfinished == 0:
        assert not jpending, "join() still blocked with no unfinished tasks"
    for t in jpending:
        t.cancel()


def main() -> int:
    if sys.version_info < (3, 13):
        print("Queue.shutdown() is new in Python 3.13; skipped")
        return 0
    for scheduler in ("random", "fifo", "pct"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(harness, seeds=range(1000), config=config)
        print(f"{scheduler}: {report.summary().splitlines()[0]}")
        assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
