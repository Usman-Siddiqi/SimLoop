"""aio-pika 10.2.3: the generic ``aio_pika.pool.Pool`` under cancellation (clean).

``Pool`` is what aio-pika's docs use for connection and channel pools. With
a constructor that takes 0-20 ms and fails 10% of the time, ``max_size``
1-3, and four users holding items for random times under random
``wait_for`` timeouts, no item was held by two users at once, the pool never
created more than ``max_size`` items, and every user finished.

Run: python findings/aio_pika_pool.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

from aio_pika.pool import Pool

import simloop


class Item:
    def __init__(self, n: int) -> None:
        self.n = n
        self.holder: int | None = None

    async def close(self) -> None:
        pass


async def harness() -> None:
    rng = simloop.rng()
    created: list[Item] = []
    max_size = rng.randint(1, 3)

    async def constructor() -> Item:
        await asyncio.sleep(rng.uniform(0, 0.02))
        if rng.random() < 0.1:
            raise OSError("connect failed")
        created.append(Item(len(created)))
        return created[-1]

    pool: Pool[Item] = Pool(constructor, max_size=max_size)

    async def use(n: int) -> None:
        async with pool.acquire() as item:
            assert item.holder is None, f"item {item.n} held by {item.holder} and {n}"
            item.holder = n
            try:
                await asyncio.sleep(rng.uniform(0, 0.03))
            finally:
                item.holder = None

    async def user(n: int) -> None:
        for _ in range(5):
            await asyncio.sleep(rng.uniform(0, 0.02))
            with contextlib.suppress(TimeoutError, OSError):
                await asyncio.wait_for(use(n), rng.choice([None, None, rng.uniform(0, 0.05)]))

    await asyncio.wait_for(asyncio.gather(*(user(n) for n in range(4))), 60)
    assert len(created) <= max_size, f"{len(created)} items created, max_size={max_size}"
    await pool.close()


def main() -> int:
    for scheduler in ("random", "pct", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(harness, seeds=range(1000), config=config)
        print(f"{scheduler}: {report.summary().splitlines()[0]}")
        assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
