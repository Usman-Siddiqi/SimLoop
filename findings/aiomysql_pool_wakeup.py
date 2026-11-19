"""aiomysql 0.3.2: releasing a closed connection does not wake pool waiters, so acquire() hangs.

Six workers share an ``aiomysql`` pool (``maxsize=2``) against the
simulated MySQL server in ``_mysql.py``. Each runs ten ``SELECT SLEEP(t),
'<token>'`` queries, some under a short ``asyncio.wait_for``, and a few
leave a transaction open. Checked: every reply carries its own token, the
pool never holds more than ``maxsize`` connections, and every worker
finishes (within 60 simulated seconds; a fault-free run takes about 1.5).

The last property fails. ``Pool.release`` only wakes a waiting
``acquire()`` when it puts the connection back in the free list. A
connection that is already closed (aiomysql closes it when a query is
cancelled, or when it breaks) or that is closed by the release because a
transaction is still open leaves the pool without waking anyone. The
waiter stays in ``Condition.wait()`` although the pool now has room, and
nothing wakes it until some other connection is released normally: with
no other traffic, never.

Run: python findings/aiomysql_pool_wakeup.py
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from typing import Any

import aiomysql
from _mysql import MysqlServer
from pymysql.err import MySQLError

import simloop

MAXSIZE = 2


async def harness() -> None:
    """Every worker gets its connections and finishes."""
    net = simloop.network()
    net.add_host("db", entry=MysqlServer().serve)
    net.set_link("main", "db", latency=(0.0005, 0.003))
    await asyncio.sleep(0.01)
    rng = simloop.rng()
    pool = await aiomysql.create_pool(
        host="db", user="u", password="p", db="d", minsize=0, maxsize=MAXSIZE, autocommit=True
    )

    async def worker(n: int) -> None:
        for i in range(10):
            token = f"w{n}-{i}"
            try:
                async with pool.acquire() as conn:
                    assert pool.size <= MAXSIZE, f"{pool.size} connections, maxsize {MAXSIZE}"
                    async with conn.cursor() as cur:
                        if rng.random() < 0.05:
                            await conn.begin()  # forgotten: still open on release
                        delay = round(rng.uniform(0, 0.05), 3)
                        query = cur.execute("SELECT SLEEP(%s), %s", (delay, token))
                        if rng.random() < 0.3:
                            await asyncio.wait_for(query, rng.uniform(0, 0.05))
                        else:
                            await query
                        rows = await cur.fetchall()
                        assert rows == (("0", token),), f"{token} got {rows}"
            except (TimeoutError, MySQLError) as exc:
                simloop.log(f"{token}: {type(exc).__name__}")
            await asyncio.sleep(rng.uniform(0, 0.02))

    workers = [asyncio.create_task(worker(n)) for n in range(6)]
    done, pending = await asyncio.wait(workers, timeout=60)
    stuck = len(pending)
    for task in pending:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    for task in done:
        task.result()
    assert not stuck, (
        f"{stuck} worker(s) still waiting in acquire() after 60 s;"
        f" the pool holds {pool.size} of {MAXSIZE} connections"
    )
    pool.close()
    await pool.wait_closed()


def fix_release() -> Callable[[], None]:
    """Wake a waiter on every release, the way a normal release does."""
    release = aiomysql.Pool.release

    def patched(self: Any, conn: Any) -> Any:
        silent = conn in self._used and (conn.closed or conn.get_transaction_status())
        fut = release(self, conn)
        return self._loop.create_task(self._wakeup()) if silent else fut

    aiomysql.Pool.release = patched  # type: ignore[method-assign]

    def undo() -> None:
        aiomysql.Pool.release = release  # type: ignore[method-assign]

    return undo


async def plain(kind: str) -> tuple[bool, int]:
    """On a real loop: does a waiting acquire() get the room a release makes?"""
    server = MysqlServer()
    listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    pool = await aiomysql.create_pool(
        host="127.0.0.1", port=port, user="u", password="p", db="d", minsize=0, maxsize=1
    )
    holding = asyncio.Event()

    async def holder() -> None:
        async with pool.acquire() as conn, conn.cursor() as cur:
            holding.set()
            if kind == "cancelled query":
                await asyncio.wait_for(cur.execute("SELECT SLEEP(1), 'slow'"), 0.1)
            else:
                await conn.begin()  # still open when the connection is released

    async def waiter() -> Any:
        async with pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute("SELECT 'next'")
            return await cur.fetchall()

    first = asyncio.create_task(holder())
    await holding.wait()
    second = asyncio.create_task(waiter())
    await asyncio.gather(first, return_exceptions=True)
    done, _ = await asyncio.wait([second], timeout=2)
    size = pool.size
    second.cancel()
    await asyncio.gather(second, return_exceptions=True)
    pool.close()
    await pool.wait_closed()
    listener.close()
    return bool(done), size


def main() -> int:
    configs = [("random", simloop.SimConfig())]
    configs.append(("fifo", simloop.SimConfig(scheduler="fifo", timer_jitter=0.001)))
    for name, config in configs:
        report = simloop.explore(harness, seeds=range(200), config=config)
        print(f"{name}: {report.summary()}")
        assert report.groups
        assert all("still waiting in acquire()" in g.message for g in report.groups)
    undo = fix_release()
    try:
        for name, config in configs:
            fixed = simloop.explore(harness, seeds=range(200), config=config)
            print(f"{name}, waking a waiter on every release: {fixed.summary()}")
            assert fixed.ok, fixed.summary()
    finally:
        undo()
    for kind in ("cancelled query", "open transaction"):
        served, size = asyncio.run(plain(kind))
        print(
            f"real asyncio loop, maxsize=1, holder's {kind}: waiter served within 2 s: {served};"
            f" pool size {size}"
        )
        assert not served
        assert size == 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
