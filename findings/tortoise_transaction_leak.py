"""tortoise-orm 1.1.9: ``in_transaction()`` leaks its connection when ``BEGIN`` fails.

Tortoise (MySQL backend, asyncmy driver, ``maxsize=2``) against the simulated
MySQL server in ``_mysql.py``. Five workers run eight ``SELECT SLEEP(t),
'<token>'`` queries each, every one in its own task (as a web framework
runs requests), inside ``in_transaction()`` or not. Workloads: 30% of
queries under a short ``asyncio.wait_for``, or 0.3% TCP resets per
segment. Checked: every reply carries its own token, every worker
finishes, and once they have no pool connection is still in use.

Without transactions both pass; with them both fail.
``TransactionContextPooled.__aenter__`` takes a connection from the pool,
points the task's ``connections`` context variable at the transaction, and
awaits ``begin()`` unguarded: if ``BEGIN`` raises or is cancelled,
``__aexit__`` never runs, so the connection is never given back and the
task (and every task it starts later) keeps seeing the dead transaction
as its ``default`` connection; ``Tortoise.close_connections()`` then waits
for the lost connection for good. Known upstream: open issue
tortoise/tortoise-orm#2391 (cancellation) with fix PR #2392; rediscovered
here from a generic "no connection stays in use" check.

Run: python findings/tortoise_transaction_leak.py
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from _mysql import MysqlServer, Session
from asyncmy.errors import MySQLError
from tortoise import Tortoise, connections
from tortoise.backends.base.client import TransactionContextPooled
from tortoise.connection import get_connections
from tortoise.exceptions import BaseORMException
from tortoise.transactions import in_transaction

import simloop

ERRORS = (TimeoutError, MySQLError, BaseORMException, ConnectionError, OSError)
WORKLOADS: dict[str, dict[str, float]] = {
    "timeouts, no transactions": {"cancel": 0.3, "tx": 0},
    "resets, no transactions": {"reset": 0.003, "tx": 0},
    "timeouts in transactions": {"cancel": 0.3, "tx": 1},
    "resets in transactions": {"reset": 0.003, "tx": 1},
}
FAILING = ("timeouts in transactions", "resets in transactions")
EXPECTED = ("still in use", "stuck")
APPS = {"models": {"models": ["_tortoise_models"], "default_connection": "default"}}


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Replies match, nobody hangs, and every connection goes back to the pool."""
        net = simloop.network()
        net.add_host("db", entry=MysqlServer().serve)
        net.set_link("main", "db", latency=(0.0005, 0.003), reset=knobs.get("reset", 0.0))
        await asyncio.sleep(0.01)
        rng = simloop.rng()
        url = "mysql://u:p@db:3306/d?minsize=0&maxsize=2"
        await Tortoise.init(config={"connections": {"default": url}, "apps": APPS})

        async def job(token: str) -> None:
            query = f"SELECT SLEEP({round(rng.uniform(0, 0.03), 3)}), '{token}'"
            if rng.random() < knobs["tx"]:
                async with in_transaction() as transaction:
                    _, rows = await transaction.execute_query(query)
            else:
                _, rows = await connections.get("default").execute_query(query)
            got = [tuple(row.values()) for row in rows]
            assert got == [("0", token)], f"{token} got {got}"

        async def request(token: str) -> None:
            try:
                if rng.random() < knobs.get("cancel", 0):
                    await asyncio.wait_for(job(token), rng.uniform(0, 0.03))
                else:
                    await job(token)
            except ERRORS as exc:
                simloop.log(f"{token}: {type(exc).__name__}")

        async def worker(n: int) -> None:
            for i in range(8):
                await asyncio.create_task(request(f"w{n}-{i}"))
                await asyncio.sleep(rng.uniform(0, 0.01))

        workers = [asyncio.create_task(worker(n)) for n in range(5)]
        done, pending = await asyncio.wait(workers, timeout=60)
        for task in pending:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for task in done:
            task.result()
        pool = connections.get("default")._pool  # type: ignore[attr-defined]
        assert not pending, f"{len(pending)} worker(s) stuck for 60 s; pool size {pool.size}"
        net.set_link("main", "db", latency=0.001, reset=0.0)
        await asyncio.sleep(0.5)
        used = pool.size - pool.freesize
        assert used == 0, f"{used} pool connection(s) still in use after every worker finished"
        await Tortoise.close_connections()

    return harness


def fix_begin() -> Callable[[], None]:
    """Give the connection back and restore the context variable if ``begin()`` fails."""
    original = TransactionContextPooled.__aenter__

    async def aenter(self: Any) -> Any:
        await self.ensure_connection()
        self.client._connection = await self.client._parent._pool.acquire()
        self.token = get_connections().set(self.connection_name, self.client)
        try:
            await self.client.begin()
        except BaseException:
            await self.client._parent._pool.release(self.client._connection)
            get_connections().reset(self.token)
            raise
        return self.client

    TransactionContextPooled.__aenter__ = aenter  # type: ignore[method-assign]

    def undo() -> None:
        TransactionContextPooled.__aenter__ = original  # type: ignore[method-assign]

    return undo


class ScriptedServer(MysqlServer):
    """For the real-loop runs: answer ``BEGIN`` slowly, or drop the connection on it."""

    def __init__(self, begin: str) -> None:
        super().__init__()
        self.begin = begin

    async def query(self, session: Session, sql: str) -> None:
        if sql.strip().upper() == "BEGIN":
            if self.begin == "drop":
                session.writer.transport.abort()
                return
            await asyncio.sleep(0.5)
        await super().query(session, sql)


async def plain(kind: str) -> str:
    """On a real loop, with maxsize=1."""
    server = ScriptedServer("drop" if kind == "the connection drops during BEGIN" else "slow")
    listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    url = f"mysql://u:p@127.0.0.1:{port}/d?minsize=0&maxsize=1"
    await Tortoise.init(config={"connections": {"default": url}, "apps": APPS})
    go = asyncio.Event()

    async def bystander() -> None:
        await go.wait()
        await connections.get("default").execute_query("SELECT 'bystander'")

    early = asyncio.create_task(bystander())
    await asyncio.sleep(0)

    async def transaction() -> None:
        async with in_transaction() as conn:
            await conn.execute_query("SELECT 'inside'")

    async def attempt() -> tuple[str, str]:
        error = "nothing"
        try:
            await asyncio.wait_for(transaction(), 0.1)
        except Exception as exc:
            error = type(exc).__name__
        return error, type(connections.get("default")).__name__

    error, stale = await asyncio.create_task(attempt())
    go.set()
    try:
        await asyncio.wait_for(early, 1)
        served = True
    except TimeoutError:
        served = False
    try:
        await asyncio.wait_for(Tortoise.close_connections(), 1)
        closed = True
    except TimeoutError:
        closed = False
    listener.close()
    return (
        f"the caller got {error}; it then sees {stale} as its connection;"
        f" close_connections() returns within 1 s: {closed};"
        f" another task's query is served within 1 s: {served}"
    )


def main() -> int:
    configs = [("random", simloop.SimConfig())]
    configs.append(("fifo", simloop.SimConfig(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(50), config=config)
            print(f"{workload}, {name}: {report.summary()}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups
            for group in report.groups:
                assert any(text in group.message for text in EXPECTED), group.message
    undo = fix_begin()
    try:
        for workload in FAILING:
            for name, config in configs:
                harness = make_harness(WORKLOADS[workload])
                fixed = simloop.explore(harness, seeds=range(50), config=config)
                print(f"{workload}, {name}, begin() guarded: {fixed.summary()}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    for kind in ("a timeout during BEGIN", "the connection drops during BEGIN"):
        outcome = asyncio.run(plain(kind))
        print(f"real asyncio loop, {kind}: {outcome}")
        assert "TransactionWrapper" in outcome, outcome
        assert outcome.endswith("False"), outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
