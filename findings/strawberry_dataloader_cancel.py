"""strawberry-graphql 0.341.2: one caller's cancellation fails every caller of a DataLoader key.

``strawberry.dataloader.DataLoader.load(key)`` returns the cached future for
a key to every caller. A task cancelled while awaiting a future cancels the
future too, so one caller's cancellation makes every concurrent caller of
that key raise ``CancelledError``. Unlike aiodataloader, strawberry skips a
cancelled future in its cache, so later loads work again.

Already known upstream (open pull request strawberry-graphql/strawberry#4803,
which notes that graphql-core 3.3 cancels sibling resolvers of a failing
non-null field). SimLoop rediscovers it with the generic
``strict_cancellation`` check from a harness that only asserts "a caller
that is not cancelled gets the value".

Run: python findings/strawberry_dataloader_cancel.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

from strawberry.dataloader import DataLoader

import simloop


def users(delay: float) -> DataLoader[int, str]:
    async def load_fn(keys: list[int]) -> list[str]:
        await asyncio.sleep(delay)
        return [f"user-{key}" for key in keys]

    return DataLoader(load_fn=load_fn)


async def harness() -> None:
    """Callers that are not cancelled get the value, now and later."""
    rng = simloop.rng()
    loader = users(rng.uniform(0.01, 0.1))

    async def get() -> str:
        return await loader.load(1)

    async def caller() -> None:
        await asyncio.sleep(rng.uniform(0, 0.02))
        with contextlib.suppress(TimeoutError):
            got = await asyncio.wait_for(get(), rng.choice([None, 0.05]))
            assert got == "user-1", f"a caller got {got!r}"

    await asyncio.gather(*(caller() for _ in range(rng.randint(1, 4))))
    assert await get() == "user-1"


async def plain() -> tuple[str, str]:
    loader = users(0.1)

    async def get() -> str:
        return await loader.load(1)

    impatient = asyncio.create_task(asyncio.wait_for(get(), 0.01))
    patient = asyncio.create_task(get())
    with contextlib.suppress(TimeoutError):
        await impatient
    try:
        concurrent = repr(await patient)
    except asyncio.CancelledError:
        assert patient.cancelling() == 0
        concurrent = "CancelledError"
    return concurrent, repr(await get())


def main() -> int:
    config = simloop.SimConfig(strict_cancellation=True)
    report = simloop.explore(harness, seeds=range(1000), config=config)
    print(report.summary())
    assert {group.kind for group in report.groups} == {"stray_cancel"}
    fifo = simloop.explore(harness, seeds=range(1000), config=config.replace(scheduler="fifo"))
    print(f"fifo: {fifo.summary().splitlines()[0]}")
    assert not fifo.ok
    concurrent, later = asyncio.run(plain())
    print(f"real asyncio loop: concurrent caller got {concurrent}, a later load got {later}")
    assert concurrent == "CancelledError"
    assert later == "'user-1'"
    return 0


if __name__ == "__main__":
    sys.exit(main())
