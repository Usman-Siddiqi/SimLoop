"""pymongo 4.20 AsyncMongoClient: pool, replies and cursors under faults (one suspected issue).

``AsyncMongoClient(maxPoolSize=2)`` runs unmodified against the simulated
MongoDB server in ``_mongo.py`` (its sockets are SimLoop's virtual sockets).
Five workers run eight jobs each: insert a document and read it back with a
slow ``find_one``, iterate a ``find`` with ``batch_size=2`` (sometimes
stopping early, always closing the cursor), or ``find_one_and_update`` with
``$inc``. Workloads: no faults; 30% of jobs under a short
``asyncio.wait_for``; 30% under ``pymongo.timeout()``; 0.3% TCP resets per
segment; server crashes every 0.2-0.8 s. Checked: every reply carries its
own token, the pool never has more than ``maxPoolSize`` connections (CMAP
events), every worker finishes, nothing stays checked out, the client
serves a query afterwards, and ``close()`` leaves no server connection
open. All held, random and fifo, with ``strict_cancellation=True``.

Also checked where it applies (no faults, ``wait_for``): every server
cursor the client learned of is exhausted or killed. That held too. With
``pymongo.timeout()`` or TCP resets it does not, which is the suspected
issue in docs/FINDINGS.md: when a ``getMore`` fails with a network error or
a client-side timeout (``NetworkTimeout``), the cursor marks itself killed
and ``close()`` sends no ``killCursors``, although the server still holds
the cursor. ``cursor_after_timeout()`` shows the single-cursor case.

Run: python findings/pymongo_async_pool.py
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import pymongo
from _mongo import MongoServer
from pymongo import AsyncMongoClient, monitoring
from pymongo.errors import PyMongoError

import simloop
from simloop import faults

POOL = 2
ERRORS = (TimeoutError, PyMongoError, ConnectionError, OSError)
WORKLOADS: dict[str, dict[str, float]] = {
    "no faults": {"cursors": 1},
    "wait_for timeouts": {"cancel": 0.3, "cursors": 1},
    "pymongo.timeout()": {"csot": 0.3},
    "TCP resets": {"reset": 0.003},
    "server crashes": {"crash": 1, "cancel": 0.2},
}


class PoolEvents(monitoring.ConnectionPoolListener):
    """Open and checked-out connection counts, from CMAP events."""

    def __init__(self) -> None:
        self.open = self.peak = self.out = 0

    def connection_created(self, event: Any) -> None:
        self.open += 1
        self.peak = max(self.peak, self.open)

    def connection_closed(self, event: Any) -> None:
        self.open -= 1

    def connection_checked_out(self, event: Any) -> None:
        self.out += 1

    def connection_checked_in(self, event: Any) -> None:
        self.out -= 1

    def pool_created(self, event: Any) -> None: ...
    def pool_ready(self, event: Any) -> None: ...
    def pool_cleared(self, event: Any) -> None: ...
    def pool_closed(self, event: Any) -> None: ...
    def connection_ready(self, event: Any) -> None: ...
    def connection_check_out_started(self, event: Any) -> None: ...
    def connection_check_out_failed(self, event: Any) -> None: ...


class CursorEvents(monitoring.CommandListener):
    """Server cursor ids the client received in a reply."""

    def __init__(self) -> None:
        self.known: set[int] = set()

    def succeeded(self, event: Any) -> None:
        if event.command_name in ("find", "getMore"):
            cursor_id = int(event.reply.get("cursor", {}).get("id", 0))
            if cursor_id:
                self.known.add(cursor_id)

    def started(self, event: Any) -> None: ...
    def failed(self, event: Any) -> None: ...


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Replies match, the pool stays bounded and clean, nothing hangs or leaks."""
        net = simloop.network()
        server = MongoServer()
        net.add_host("mongo", entry=server.serve)
        net.set_link("main", "mongo", latency=(0.0005, 0.003))
        await asyncio.sleep(0.01)
        rng = simloop.rng()
        pool, cursors = PoolEvents(), CursorEvents()
        client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
            "mongodb://mongo:27017/?serverSelectionTimeoutMS=2000&heartbeatFrequencyMS=500",
            maxPoolSize=POOL,
            event_listeners=[pool, cursors],
        )
        coll = client.d.items
        for group in range(3):
            await coll.insert_many([{"_id": f"g{group}-{i}", "group": group} for i in range(5)])

        async def job(token: str) -> None:
            kind, delay = rng.random(), round(rng.uniform(0, 0.03), 3)
            if kind < 0.4:
                await coll.insert_one({"_id": token})
                doc = await coll.find_one({"_id": token, "sleep": delay})
                assert doc == {"_id": token}, f"{token} got {doc}"
            elif kind < 0.7:
                group, seen = rng.randrange(3), 0
                cursor = coll.find({"group": group, "sleep": delay}, batch_size=2)
                try:
                    async for doc in cursor:
                        assert doc["group"] == group, f"{token} got {doc}"
                        seen += 1
                        if seen == 3 and rng.random() < 0.5:
                            break
                finally:
                    await cursor.close()
            else:
                query = {"_id": "counter", "sleep": delay}
                await coll.find_one_and_update(query, {"$inc": {"n": 1}}, upsert=True)

        async def worker(n: int) -> None:
            for i in range(8):
                token = f"w{n}-{i}"
                try:
                    roll = rng.random()
                    if roll < knobs.get("cancel", 0):
                        await asyncio.wait_for(job(token), rng.uniform(0, 0.03))
                    elif roll < knobs.get("csot", 0):
                        with pymongo.timeout(rng.uniform(0, 0.03)):
                            await job(token)
                    else:
                        await job(token)
                except ERRORS as exc:
                    simloop.log(f"{token}: {type(exc).__name__}")
                await asyncio.sleep(rng.uniform(0, 0.01))

        net.set_link("main", "mongo", latency=(0.0005, 0.003), reset=knobs.get("reset", 0.0))
        nemesis = None
        if knobs.get("crash"):
            crashes = faults.random_crashes(["mongo"], every=(0.2, 0.8), downtime=(0.02, 0.1))
            nemesis = asyncio.create_task(crashes)
        workers = [asyncio.create_task(worker(n)) for n in range(5)]
        done, pending = await asyncio.wait(workers, timeout=60)
        for task in pending:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for task in done:
            task.result()
        assert not pending, f"{len(pending)} worker(s) stuck after 60 s"
        assert pool.peak <= POOL, f"{pool.peak} pool connections open, maxPoolSize {POOL}"
        if nemesis is not None:
            nemesis.cancel()
            if not net.host("mongo").up:
                net.host("mongo").restart()
        net.set_link("main", "mongo", latency=0.001, reset=0.0)
        await asyncio.sleep(1.5)
        assert pool.out == 0, f"{pool.out} connection(s) still checked out"
        for attempt in range(3):
            try:
                assert await coll.find_one({"_id": "g0-0"}) == {"_id": "g0-0", "group": 0}
                break
            except ERRORS as exc:
                simloop.log(f"recovery attempt {attempt}: {type(exc).__name__}")
        else:
            raise AssertionError("the client never served a query after faults stopped")
        leaked = sorted(cursors.known & set(server.cursors))
        simloop.log(f"server cursors the client knew of, left open: {leaked}")
        if knobs.get("cursors"):
            assert not leaked, f"server cursors left open: {leaked}"
        await client.close()
        await asyncio.sleep(0.5)
        assert not server.open, f"{len(server.open)} server connection(s) open after close()"

    return harness


class SlowGetMore(MongoServer):
    """Answers every getMore after 0.2 s."""

    async def command(self, conn_id: int, cmd: dict[str, Any]) -> dict[str, Any]:
        if next(iter(cmd)) == "getMore":
            await asyncio.sleep(0.2)
        return await super().command(conn_id, cmd)


def cursor_after_timeout(kind: str) -> Callable[[], Coroutine[Any, Any, str]]:
    async def run() -> str:
        """A cursor whose second batch takes longer than the caller waits, then close()."""
        net = simloop.network()
        server = SlowGetMore()
        net.add_host("mongo", entry=server.serve)
        await asyncio.sleep(0.01)
        options = "socketTimeoutMS=100" if kind == "socketTimeoutMS=100" else ""
        client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(f"mongodb://mongo/?{options}")
        coll = client.d.items
        await coll.insert_many([{"_id": i} for i in range(6)])
        cursor = coll.find({}, batch_size=2)

        async def drain() -> None:
            async for _ in cursor:
                pass

        error = "nothing"
        try:
            if kind == "asyncio.wait_for(0.1)":
                await asyncio.wait_for(drain(), 0.1)
            elif kind == "pymongo.timeout(0.1)":
                with pymongo.timeout(0.1):
                    await drain()
            else:
                await drain()
        except ERRORS as exc:
            error = type(exc).__name__
        await cursor.close()
        await asyncio.sleep(0.5)
        kills = server.commands.count("killCursors")
        await client.close()
        left = len(server.cursors)
        return f"{error}; killCursors sent {kills} time(s); server cursors open: {left}"

    return run


def main() -> int:
    for workload, knobs in WORKLOADS.items():
        for scheduler in ("random", "fifo"):
            config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
            if scheduler == "fifo":
                config = config.replace(timer_jitter=0.001)
            # The client prints each failed background pool refill (pool paused
            # while the server is unreachable) to stderr; count them instead.
            with contextlib.redirect_stderr(io.StringIO()) as noise:
                report = simloop.explore(make_harness(knobs), seeds=range(100), config=config)
            printed = noise.getvalue().count("background task encountered an error")
            line = report.summary().splitlines()[0]
            print(f"{workload}, {scheduler}: {line} ({printed} background errors printed)")
            assert report.ok, report.summary()
    outcomes = {}
    for kind in ("asyncio.wait_for(0.1)", "pymongo.timeout(0.1)", "socketTimeoutMS=100"):
        outcomes[kind] = simloop.run(cursor_after_timeout(kind), seed=0).value
        print(f"getMore slower than {kind}, then close(): {outcomes[kind]}")
    assert outcomes["asyncio.wait_for(0.1)"].endswith("open: 0"), outcomes
    assert outcomes["pymongo.timeout(0.1)"].endswith("open: 1"), outcomes
    assert outcomes["socketTimeoutMS=100"].endswith("open: 1"), outcomes
    return 0


if __name__ == "__main__":
    sys.exit(main())
