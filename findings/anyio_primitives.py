"""anyio (asyncio backend): streams, limiters, semaphores and locks under cancellation.

Clean on anyio 4.16.0 (see docs/FINDINGS.md, "Tested, nothing found").

Properties, each with random ``move_on_after`` cancellations:

- memory object streams: every item whose ``send`` returned is received
  exactly once, in order per producer, across buffer sizes 0, 1 and 3 and
  two competing consumers;
- ``CapacityLimiter``: holders never exceed the total, raising the total
  wakes waiters, and no token is left borrowed;
- ``Semaphore`` and ``Lock`` (both ``fast_acquire`` modes): the bound and
  mutual exclusion hold and nothing is left held.

Running anyio under SimLoop needed ``stable_hash`` for its ``CancelScope``
(cancelling a task group walks a set of child scopes).

Run: python findings/anyio_primitives.py
"""

from __future__ import annotations

import sys
from typing import Any

import anyio

import simloop


def _timeout(rng: Any, p: float, high: float) -> float | None:
    return rng.uniform(0, high) if rng.random() < p else None


async def streams() -> None:
    rng = simloop.rng()
    send, receive = anyio.create_memory_object_stream[tuple[int, int]](rng.choice([0, 0, 1, 3]))
    delivered: dict[int, list[int]] = {}
    received: list[tuple[int, int]] = []

    async def producer(p: int, stream: Any) -> None:
        delivered[p] = []
        async with stream:
            for i in range(6):
                await anyio.sleep(rng.uniform(0, 0.02))
                with anyio.move_on_after(_timeout(rng, 0.4, 0.03)) as scope:
                    await stream.send((p, i))
                if not scope.cancelled_caught:
                    delivered[p].append(i)

    async def consumer(stream: Any) -> None:
        async with stream:
            while True:
                with anyio.move_on_after(_timeout(rng, 0.3, 0.05)):
                    try:
                        received.append(await stream.receive())
                    except anyio.EndOfStream:
                        return

    async with anyio.create_task_group() as tg:
        for _ in range(2):
            tg.start_soon(consumer, receive.clone())
        receive.close()
        for p in range(3):
            tg.start_soon(producer, p, send.clone())
        send.close()
    for p, sent in delivered.items():
        got = [i for q, i in received if q == p]
        assert got == sent, f"producer {p}: sent {sent}, received {got}"


async def limiter() -> None:
    rng = simloop.rng()
    lim = anyio.CapacityLimiter(rng.randint(1, 3))
    holders = 0

    async def user() -> None:
        nonlocal holders
        for _ in range(4):
            await anyio.sleep(rng.uniform(0, 0.02))
            with anyio.move_on_after(_timeout(rng, 0.4, 0.03)):
                async with lim:
                    holders += 1
                    assert holders <= lim.total_tokens, (
                        f"{holders} holders, {lim.total_tokens} tokens"
                    )
                    try:
                        if rng.random() < 0.2:
                            # Lowering the total does not revoke tokens, so only raise it.
                            lim.total_tokens += 1
                        await anyio.sleep(rng.uniform(0, 0.03))
                    finally:
                        holders -= 1

    async def no_idle_waiters() -> None:
        while True:
            await anyio.sleep(0.005)
            stats = lim.statistics()
            if stats.tasks_waiting and stats.borrowed_tokens < stats.total_tokens:
                await anyio.sleep(0.001)
                stats = lim.statistics()
                assert not (stats.tasks_waiting and stats.borrowed_tokens < stats.total_tokens), (
                    f"{stats.tasks_waiting} waiting with "
                    f"{stats.borrowed_tokens}/{stats.total_tokens} borrowed"
                )

    async with anyio.create_task_group() as outer:
        outer.start_soon(no_idle_waiters)
        async with anyio.create_task_group() as tg:
            for _ in range(5):
                tg.start_soon(user)
        outer.cancel_scope.cancel()
    assert lim.borrowed_tokens == 0, lim.borrowed_tokens


async def semaphore() -> None:
    rng = simloop.rng()
    value = rng.randint(1, 2)
    sem = anyio.Semaphore(value, fast_acquire=rng.random() < 0.5)
    lock = anyio.Lock(fast_acquire=rng.random() < 0.5)
    inside = locked = 0

    async def user() -> None:
        nonlocal inside, locked
        for _ in range(4):
            await anyio.sleep(rng.uniform(0, 0.02))
            with anyio.move_on_after(_timeout(rng, 0.4, 0.03)):
                async with sem:
                    inside += 1
                    assert inside <= value, f"{inside} inside a Semaphore({value})"
                    try:
                        async with lock:
                            locked += 1
                            assert locked == 1, "two holders of one Lock"
                            try:
                                await anyio.sleep(rng.uniform(0, 0.01))
                            finally:
                                locked -= 1
                    finally:
                        inside -= 1

    async with anyio.create_task_group() as tg:
        for _ in range(5):
            tg.start_soon(user)
    assert sem.value == value, f"semaphore left at {sem.value}"
    assert not lock.locked(), "lock left held"


def main() -> int:
    ok = True
    for workload in (streams, limiter, semaphore):
        for scheduler in ("random", "fifo", "pct"):
            report = simloop.explore(
                workload, seeds=range(300), config=simloop.SimConfig(scheduler=scheduler)
            )
            print(f"{workload.__name__} {scheduler}: {report.summary()}")
            ok = ok and report.ok and all(g.reproducible for g in report.groups)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
