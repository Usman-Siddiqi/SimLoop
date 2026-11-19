"""databases 0.9.0: a transaction that fails or is cancelled never gives its connection back.

``Database("mysql+asyncmy://...", max_size=2)`` against the simulated MySQL
server in ``_mysql.py``. Five workers run eight ``SELECT SLEEP(t),
'<token>'`` queries each, inside ``async with db.transaction()`` or not.
Workloads: 30% of queries under a short ``asyncio.wait_for``, or 0.3% TCP
resets per segment. Checked: every reply carries its own token, every
worker finishes, and once they have no pool connection is still in use.

Without transactions both workloads pass. With them both fail:
``Transaction.commit()`` and ``rollback()`` pop the transaction and then
await the driver's commit or rollback, and only after that release the
connection (``Connection.__aexit__``). When that await raises (the
connection broke, or a cancelled query left it unusable) the release is
skipped: the pool slot is gone for good, and the task keeps a
``Connection`` whose counter never returns to zero. ``Transaction.start()``
has the same shape: cancelled during ``BEGIN``, it has already taken the
connection and never gives it back. A caller that times out inside a
transaction also gets the driver's ``InterfaceError`` from the rollback
instead of its ``TimeoutError``. The commit/rollback half is known
upstream: open pull request encode/databases#498.

asyncmy is used because aiomysql's own pool has a separate wakeup bug
(``aiomysql_pool_wakeup.py``); the code at fault is backend-independent.

Run: python findings/databases_transaction_leak.py
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from _mysql import MysqlServer, Session
from asyncmy.errors import MySQLError
from databases import Database
from databases.core import Transaction

import simloop

ERRORS = (TimeoutError, MySQLError, ConnectionError, OSError)
WORKLOADS: dict[str, dict[str, float]] = {
    "timeouts, no transactions": {"cancel": 0.3, "tx": 0},
    "resets, no transactions": {"reset": 0.003, "tx": 0},
    "timeouts in transactions": {"cancel": 0.3, "tx": 1},
    "resets in transactions": {"reset": 0.003, "tx": 1},
}
FAILING = ("timeouts in transactions", "resets in transactions")
EXPECTED = ("still in use", "stuck")


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Replies match, nobody hangs, and every connection goes back to the pool."""
        net = simloop.network()
        net.add_host("db", entry=MysqlServer().serve)
        net.set_link("main", "db", latency=(0.0005, 0.003), reset=knobs.get("reset", 0.0))
        await asyncio.sleep(0.01)
        rng = simloop.rng()
        db = Database("mysql+asyncmy://u:p@db:3306/d", min_size=0, max_size=2)
        await db.connect()

        async def job(token: str) -> None:
            query = f"SELECT SLEEP({round(rng.uniform(0, 0.03), 3)}), '{token}'"
            if rng.random() < knobs["tx"]:
                async with db.transaction():
                    rows = await db.fetch_all(query)
            else:
                rows = await db.fetch_all(query)
            got = [tuple(row.values()) for row in rows]  # type: ignore[attr-defined]
            assert got == [("0", token)], f"{token} got {got}"

        async def worker(n: int) -> None:
            for i in range(8):
                token = f"w{n}-{i}"
                try:
                    if rng.random() < knobs.get("cancel", 0):
                        await asyncio.wait_for(job(token), rng.uniform(0, 0.03))
                    else:
                        await job(token)
                except ERRORS as exc:
                    simloop.log(f"{token}: {type(exc).__name__}")
                await asyncio.sleep(rng.uniform(0, 0.01))

        workers = [asyncio.create_task(worker(n)) for n in range(5)]
        done, pending = await asyncio.wait(workers, timeout=60)
        for task in pending:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for task in done:
            task.result()
        pool = db._backend._pool  # type: ignore[attr-defined]
        assert not pending, f"{len(pending)} worker(s) stuck for 60 s; pool size {pool.size}"
        net.set_link("main", "db", latency=0.001, reset=0.0)
        await asyncio.sleep(0.5)
        used = pool.size - pool.freesize
        assert used == 0, f"{used} pool connection(s) still in use after every worker finished"
        await db.disconnect()

    return harness


def fix_transactions() -> Callable[[], None]:
    """Release the connection however the driver's begin, commit or rollback ends."""
    originals = (Transaction.start, Transaction.commit, Transaction.rollback)

    async def start(self: Any) -> Any:
        self._transaction = self._connection._connection.transaction()
        async with self._connection._transaction_lock:
            is_root = not self._connection._transaction_stack
            await self._connection.__aenter__()
            try:
                await self._transaction.start(is_root=is_root, extra_options=self._extra_options)
            except BaseException:
                await self._connection.__aexit__()
                self._transaction = None
                raise
            self._connection._transaction_stack.append(self)
        return self

    def finisher(name: str) -> Callable[[Any], Coroutine[Any, Any, None]]:
        async def finish(self: Any) -> None:
            async with self._connection._transaction_lock:
                assert self._connection._transaction_stack[-1] is self
                self._connection._transaction_stack.pop()
                try:
                    await getattr(self._transaction, name)()
                finally:
                    await self._connection.__aexit__()
                    self._transaction = None

        return finish

    Transaction.start = start  # type: ignore[method-assign]
    Transaction.commit = finisher("commit")  # type: ignore[method-assign]
    Transaction.rollback = finisher("rollback")  # type: ignore[method-assign]

    def undo() -> None:
        Transaction.start, Transaction.commit, Transaction.rollback = originals  # type: ignore[method-assign]

    return undo


class ScriptedServer(MysqlServer):
    """For the real-loop runs: drop the connection on a marker, or answer BEGIN slowly."""

    async def query(self, session: Session, sql: str) -> None:
        if "DROP-ME" in sql:
            session.writer.transport.abort()
            return
        if sql.strip().upper() == "BEGIN":
            await asyncio.sleep(0.5)
        await super().query(session, sql)


async def plain(kind: str) -> str:
    """On a real loop, with max_size=1."""
    server = ScriptedServer()
    listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    db = Database(f"mysql+asyncmy://u:p@127.0.0.1:{port}/d", min_size=0, max_size=1)
    await db.connect()

    async def transaction() -> None:
        async with db.transaction():
            if kind == "the connection drops":
                await db.fetch_all("SELECT 'DROP-ME'")
            else:
                await db.fetch_all("SELECT SLEEP(1), 'slow'")

    error = "nothing"
    try:
        if kind == "the connection drops":
            await transaction()
        else:
            timeout = 0.1 if kind == "a timeout during BEGIN" else 0.7
            await asyncio.wait_for(transaction(), timeout)
    except Exception as exc:
        error = type(exc).__name__

    async def other_task() -> None:
        await db.fetch_all("SELECT 'next'")

    try:
        await asyncio.wait_for(asyncio.create_task(other_task()), 1)
        served = True
    except TimeoutError:
        served = False
    listener.close()
    return f"the caller got {error}; another task's query is served within 1 s: {served}"


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
    undo = fix_transactions()
    try:
        for workload in FAILING:
            for name, config in configs:
                harness = make_harness(WORKLOADS[workload])
                fixed = simloop.explore(harness, seeds=range(50), config=config)
                print(f"{workload}, {name}, transactions fixed: {fixed.summary()}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    for kind in ("the connection drops", "a timeout during a query", "a timeout during BEGIN"):
        outcome = asyncio.run(plain(kind))
        print(f"real asyncio loop, {kind} inside a transaction: {outcome}")
        assert outcome.endswith("False"), outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
