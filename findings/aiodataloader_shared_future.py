"""aiodataloader 0.4.3: one caller's cancellation fails every caller of a key, and stays cached.

``DataLoader.load(key)`` returns a plain ``Future`` and, with caching on (the
default), hands that same future to every caller of the key. A task that is
cancelled while awaiting a future cancels the future itself, so one caller's
timeout, client disconnect or ``TaskGroup`` abort cancels the shared future.
Every concurrent caller of the key then gets ``CancelledError`` although
nothing cancelled it, and the batch result for that key is dropped.

The cancelled future also stays in the cache: ``load`` returns any cached
future without checking it, so every later ``load`` of that key raises
``CancelledError`` at once, until someone calls ``clear(key)``. Loaders are
often created per request, which bounds the damage to that request; a
long-lived loader keeps the key broken for good.

SimLoop finds the first part with the generic ``strict_cancellation`` check
(a task ended cancelled by a future it shared with a cancelled task) and the
second with the harness's own assertion.

Run: python findings/aiodataloader_shared_future.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

from aiodataloader import DataLoader

import simloop


class Users(DataLoader[int, str]):
    def __init__(self, delay: float) -> None:
        super().__init__()
        self.delay = delay

    async def batch_load_fn(self, keys: list[int]) -> list[str]:
        await asyncio.sleep(self.delay)
        return [f"user-{key}" for key in keys]


async def harness() -> None:
    """Callers that are not cancelled get the value, now and later."""
    rng = simloop.rng()
    loader = Users(rng.uniform(0.01, 0.1))

    async def caller() -> None:
        await asyncio.sleep(rng.uniform(0, 0.02))
        with contextlib.suppress(TimeoutError):
            got = await asyncio.wait_for(loader.load(1), rng.choice([None, 0.05]))
            assert got == "user-1", f"a caller got {got!r}"

    await asyncio.gather(*(caller() for _ in range(rng.randint(1, 4))))
    assert await loader.load(1) == "user-1"


async def plain() -> tuple[str, str]:
    loader = Users(0.1)

    async def get(timeout: float | None) -> str:
        return await asyncio.wait_for(loader.load(1), timeout)

    async def outcome(awaitable: asyncio.Future[str]) -> str:
        try:
            return repr(await awaitable)
        except asyncio.CancelledError:
            return "CancelledError"

    impatient = asyncio.create_task(get(0.01))
    patient = asyncio.create_task(get(None))
    with contextlib.suppress(TimeoutError):
        await impatient
    concurrent = await outcome(patient)
    assert patient.cancelling() == 0
    later = await outcome(asyncio.ensure_future(get(1)))
    return concurrent, later


def main() -> int:
    config = simloop.SimConfig(strict_cancellation=True)
    report = simloop.explore(harness, seeds=range(1000), config=config)
    print(report.summary())
    assert "stray_cancel" in {group.kind for group in report.groups}
    fifo = simloop.explore(harness, seeds=range(1000), config=config.replace(scheduler="fifo"))
    print(f"fifo: {fifo.summary().splitlines()[0]}")
    assert not fifo.ok
    loose = simloop.explore(harness, seeds=range(1000))
    print(f"without strict_cancellation: {loose.summary().splitlines()[0]}")
    assert not loose.ok
    concurrent, later = asyncio.run(plain())
    print(f"real asyncio loop: concurrent caller got {concurrent}, a later load got {later}")
    assert concurrent == "CancelledError"
    assert later == "CancelledError"
    return 0


if __name__ == "__main__":
    sys.exit(main())
