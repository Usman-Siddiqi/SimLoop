"""A stock reservation service with a check-then-act race.

``reserve`` reads the stock, decides, then writes it back. If two
reservations interleave between the read and the write, one update is lost
and the shop sells stock it doesn't have. Customers arrive at random times,
so the overlap only happens in a small fraction of runs.

    simloop explore examples/inventory.py:main --seeds 500
    simloop run examples/inventory.py:main --seed <failing seed> --trace
    simloop explore examples/inventory.py:main_fixed --seeds 500
"""

from __future__ import annotations

import asyncio

import simloop

INITIAL_STOCK = 10
ORDERS = (4, 3, 5, 2)


class Database:
    """In-memory key-value store with simulated network latency."""

    def __init__(self, stock: int) -> None:
        self._stock = stock

    async def read_stock(self) -> int:
        await asyncio.sleep(simloop.rng().uniform(0.002, 0.020))
        return self._stock

    async def write_stock(self, value: int) -> None:
        await asyncio.sleep(simloop.rng().uniform(0.002, 0.020))
        self._stock = value


class Shop:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.sold = 0
        self._lock = asyncio.Lock()

    async def reserve(self, quantity: int) -> bool:
        stock = await self.db.read_stock()
        simloop.log(f"read stock={stock}")
        if stock < quantity:
            return False
        await self.db.write_stock(stock - quantity)
        simloop.log(f"wrote stock={stock - quantity}")
        self.sold += quantity
        return True

    async def reserve_locked(self, quantity: int) -> bool:
        async with self._lock:
            return await self.reserve(quantity)


async def customer(shop: Shop, quantity: int, *, locked: bool) -> bool:
    await asyncio.sleep(simloop.rng().uniform(0.0, 1.0))
    simloop.log(f"order {quantity}")
    ok = await (shop.reserve_locked(quantity) if locked else shop.reserve(quantity))
    simloop.log("reserved" if ok else "out of stock")
    return ok


async def _run_shop(*, locked: bool) -> int:
    db = Database(INITIAL_STOCK)
    shop = Shop(db)
    simloop.always(lambda: db._stock + shop.sold == INITIAL_STOCK, "no stock is lost")
    async with asyncio.TaskGroup() as tg:
        for quantity in ORDERS:
            tg.create_task(customer(shop, quantity, locked=locked))
    return shop.sold


async def main() -> int:
    return await _run_shop(locked=False)


async def main_fixed() -> int:
    return await _run_shop(locked=True)
