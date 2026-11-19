"""psycopg-pool 3.3.3: ``AsyncConnectionPool`` under client timeouts (clean).

The pool runs against a stand-in connection class (``connection_class`` is
a documented parameter), so no PostgreSQL server is needed: connecting takes
0-20 ms and fails 5% of the time. Five clients take connections with random
``wait_for`` timeouts. No connection was handed to two clients at once, the
pool never held more than ``max_size`` open connections, and afterwards it
could still hand out ``max_size`` connections at once.

Before SimLoop swapped module globals captured with ``from time import
monotonic``, an earlier version of this harness failed on 23 of 300 seeds
(``PoolTimeout``, and runs that did not replay): psycopg_pool's timers read
the real clock while everything else ran on simulated time. Those failures
were artifacts, not bugs.

Run: python findings/psycopg_pool_clients.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from typing import Any, ClassVar

from psycopg import pq
from psycopg_pool import AsyncConnectionPool, PoolTimeout

import simloop

logging.getLogger("psycopg.pool").setLevel(logging.CRITICAL)


class PgConn:
    transaction_status = pq.TransactionStatus.IDLE


class Connection:
    """The parts of ``psycopg.AsyncConnection`` the pool uses."""

    opened: ClassVar[list[Connection]] = []

    def __init__(self) -> None:
        self.pgconn = PgConn()
        self.autocommit = False
        self.closed = False
        self.holder: int | None = None
        Connection.opened.append(self)

    @classmethod
    async def connect(cls, conninfo: str = "", **kwargs: Any) -> Connection:
        rng = simloop.rng()
        await asyncio.sleep(rng.uniform(0, 0.02))
        if rng.random() < 0.05:
            raise OSError("connection refused")
        return cls()

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit = value

    async def execute(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.sleep(0.001)

    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await asyncio.sleep(0.001)  # the COMMIT or ROLLBACK round trip


async def harness() -> None:
    rng = simloop.rng()
    Connection.opened = []
    max_size = rng.randint(1, 3)
    pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
        "",
        name="pool",
        connection_class=Connection,  # type: ignore[arg-type]
        min_size=1,
        max_size=max_size,
        open=False,
        timeout=rng.choice([0.05, 1.0, 30.0]),
        num_workers=2,
    )
    await pool.open()

    async def client(n: int) -> None:
        for _ in range(5):
            await asyncio.sleep(rng.uniform(0, 0.02))
            with contextlib.suppress(TimeoutError, PoolTimeout):
                await asyncio.wait_for(use(n), rng.choice([None, None, rng.uniform(0, 0.05)]))

    async def use(n: int) -> None:
        async with pool.connection() as conn:
            assert conn.holder is None, f"connection held by clients {conn.holder} and {n}"
            assert not conn.closed, "the pool handed out a closed connection"
            conn.holder = n
            try:
                await asyncio.sleep(rng.uniform(0, 0.03))
            finally:
                conn.holder = None

    await asyncio.wait_for(asyncio.gather(*(client(n) for n in range(5))), 120)
    still_open = [conn for conn in Connection.opened if not conn.closed]
    assert len(still_open) <= max_size, f"{len(still_open)} open, max_size={max_size}"
    conns = [await pool.getconn(timeout=30) for _ in range(max_size)]
    for conn in conns:
        await pool.putconn(conn)
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
