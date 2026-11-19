"""generic-connection-pool 0.8.1: the asyncio pool leaks dead connections and cancelled slots.

An asyncio ``ConnectionPool`` (``min_idle=1, max_size=2``) hands fake
connections to six workers doing ten acquire/use/release rounds each, on
one or two endpoints. Workloads: no faults, two endpoints, 30% failed
connects, 10 ms acquire timeouts, short idle and lifetime limits (with and
without the background collector), aliveness checks that fail 10% of the
time, and rounds under a short outer ``asyncio.wait_for``. Checked: a
connection handed out is open and not shared, it is not closed while in
use, at most ``total_max_size`` connections are open, every worker finishes,
no slot is still acquired or reserved once they have, and ``close()``
closes every connection.

The first six workloads pass. Two fail:

* a connection whose aliveness check fails is dropped from the pool and
  passed to ``on_connection_dead``, but never to ``dispose``: it stays open
  for good, and open connections pile up past ``total_max_size``;
* an acquire cancelled while the pool checks a connection's aliveness
  leaves that connection marked acquired, and one cancelled while the pool
  creates a connection leaves the slot reserved and counted (both handlers
  catch ``Exception``, which ``CancelledError`` is not). The pool runs out
  of slots and every acquire waits for good. ``close()`` then never
  returns: it loops, without sleeping, until no connection is acquired or
  reserved, and ignores its ``timeout``. It does the same while a caller
  simply holds a connection, although its documented ``timeout`` is the
  time "after which the pool closes all connection despite they are
  released or not".

Run: python findings/gcpool_leaks.py
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import sys
import time
from collections.abc import Callable, Coroutine
from typing import Any

from generic_connection_pool.asyncio import BaseConnectionManager, ConnectionPool
from generic_connection_pool.asyncio.utils import guard
from generic_connection_pool.common import ConnectionInfo, EventType, Timer

import simloop

logging.getLogger("generic_connection_pool").setLevel(logging.CRITICAL)
MAX = 2
TOTAL = 4
WORKLOADS: dict[str, dict[str, float]] = {
    "no faults": {},
    "two endpoints": {"two": 1},
    "failed connects": {"create_fail": 0.3},
    "acquire timeouts": {"acquire_timeout": 0.01},
    "idle and lifetime limits": {"idle": 0.01, "lifetime": 0.05},
    "the background collector": {"idle": 0.01, "lifetime": 0.05, "collector": 1},
    "failing aliveness checks": {"dead": 0.1},
    "cancelled acquires": {"two": 1, "cancel": 0.3},
}
EXPECTED = {
    "failing aliveness checks": ("connections are open", "left open after close()"),
    "cancelled acquires": ("still waiting", "still acquired or reserved"),
}
# ``Timer(0).timedout`` is ``elapsed > 0``: the collector's zero-timeout poll
# only ends once the clock has moved, as a real clock always has.
CONFIGS = [
    ("random", simloop.SimConfig(step_time=1e-6)),
    ("fifo", simloop.SimConfig(scheduler="fifo", timer_jitter=0.001, step_time=1e-6)),
]


class Broken(Exception):
    pass


class Conn:
    def __init__(self, conn_id: int) -> None:
        self.id = conn_id
        self.closed = False
        self.users = 0

    def __hash__(self) -> int:
        return self.id


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Connections are open and unshared, nothing leaks, nobody hangs."""
        rng = simloop.rng()
        ids = itertools.count()
        live: set[Conn] = set()

        class Manager(BaseConnectionManager[str, Conn]):
            async def create(self, endpoint: str) -> Conn:
                await asyncio.sleep(rng.uniform(0, 0.01))
                if rng.random() < knobs.get("create_fail", 0):
                    raise Broken("connect failed")
                conn = Conn(next(ids))
                live.add(conn)
                assert len(live) <= TOTAL, (
                    f"{len(live)} connections are open, total_max_size is {TOTAL}"
                )
                return conn

            async def dispose(self, endpoint: str, conn: Conn) -> None:
                await asyncio.sleep(rng.uniform(0, 0.005))
                assert not conn.users, f"connection {conn.id} was disposed while in use"
                conn.closed = True
                live.discard(conn)

            async def check_aliveness(self, endpoint: str, conn: Conn) -> bool:
                await asyncio.sleep(rng.uniform(0, 0.005))
                return rng.random() >= knobs.get("dead", 0)

        pool = ConnectionPool[str, Conn](
            Manager(),
            min_idle=1,
            max_size=MAX,
            total_max_size=TOTAL,
            idle_timeout=knobs.get("idle", 60.0),
            max_lifetime=knobs.get("lifetime", 3600.0),
            background_collector=bool(knobs.get("collector")),
        )
        endpoints = ["a", "b"] if knobs.get("two") else ["a"]

        async def use(endpoint: str) -> None:
            timeout = knobs.get("acquire_timeout")
            async with pool.connection(endpoint, timeout=timeout) as conn:
                assert not conn.closed, f"acquire() returned closed connection {conn.id}"
                conn.users += 1
                try:
                    assert conn.users == 1, f"connection {conn.id} handed to {conn.users} callers"
                    await asyncio.sleep(rng.uniform(0, 0.02))
                    assert not conn.closed, f"connection {conn.id} was closed while in use"
                finally:
                    conn.users -= 1

        async def worker(n: int) -> None:
            for i in range(10):
                endpoint = rng.choice(endpoints)
                try:
                    if rng.random() < knobs.get("cancel", 0):
                        await asyncio.wait_for(use(endpoint), rng.uniform(0, 0.02))
                    else:
                        await use(endpoint)
                except (Broken, TimeoutError) as exc:
                    simloop.log(f"w{n}-{i}: {type(exc).__name__}")
                await asyncio.sleep(rng.uniform(0, 0.01))

        workers = [asyncio.create_task(worker(n)) for n in range(6)]
        done, pending = await asyncio.wait(workers, timeout=60)
        for task in pending:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for task in done:
            task.result()
        assert not pending, f"{len(pending)} worker(s) still waiting in acquire() after 60 s"
        taken = {}
        for endpoint in endpoints:
            total = await pool.get_endpoint_pool_size(endpoint)
            taken[endpoint] = total - await pool.get_endpoint_pool_size(endpoint, acquired=False)
        assert not any(taken.values()), (
            f"after every worker finished, slots still acquired or reserved: {taken}"
        )
        await pool.close(timeout=5)
        assert not live, f"{len(live)} connection(s) left open after close()"

    return harness


def fix_pool() -> Callable[[], None]:
    """Dispose dead connections, and undo a check's or a connect's bookkeeping on any exit."""
    originals = (ConnectionPool._acquire_connection, ConnectionPool._create_connection)

    async def acquire_connection(self: Any, endpoint: Any, timeout: float | None) -> Any:
        timer = Timer(timeout)
        while True:
            async with self._pools.acquired(endpoint, setdefault=True) as pool:
                conn_info, extra = await pool.try_acquire_or_reserve(timeout=timer.remains)
                if conn_info is not None:
                    self._event_queue.remove((EventType.LIFETIME, endpoint, conn_info.conn))
                    if extra:
                        self._event_queue.remove((EventType.IDLETIME, endpoint, conn_info.conn))
                    try:
                        alive = await asyncio.wait_for(
                            self._connection_manager.check_aliveness(endpoint, conn_info.conn),
                            timeout=timer.remains,
                        )
                    except BaseException:
                        await self._release_connection(endpoint, conn_info.conn)
                        raise
                    if not alive:
                        info = await guard(pool.detach(conn_info.conn, acquired=True))
                        self._decrease_pool_size()
                        await asyncio.shield(self._dispose_connection(info, timeout=None))
                        continue
                    return conn_info.conn
                try:
                    conn_info = await self._create_connection(endpoint, timer.remains)
                except BaseException:
                    await guard(pool.unreserve())
                    raise
                await guard(pool.attach_and_unreserve(conn_info, acquired=True))
                return conn_info.conn

    async def create_connection(self: Any, endpoint: Any, timeout: float | None = None) -> Any:
        timer = Timer(timeout)
        while True:
            if self._increase_pool_size():
                try:
                    conn = await asyncio.wait_for(
                        self._connection_manager.create(endpoint), timeout=timer.remains
                    )
                except BaseException:
                    self._decrease_pool_size()
                    raise
                return ConnectionInfo(endpoint, conn)
            await self._try_free_slot(timeout=timer.remains)

    ConnectionPool._acquire_connection = acquire_connection  # type: ignore[method-assign]
    ConnectionPool._create_connection = create_connection  # type: ignore[method-assign]

    def undo() -> None:
        ConnectionPool._acquire_connection, ConnectionPool._create_connection = originals  # type: ignore[method-assign]

    return undo


class PlainManager(BaseConnectionManager[str, Conn]):
    """For the real-loop runs: scripted connect delays and aliveness results."""

    def __init__(self, connect: list[float], alive: list[Any]) -> None:
        self.connect = connect
        self.alive = alive
        self.ids = itertools.count()
        self.opened: list[Conn] = []

    async def create(self, endpoint: str) -> Conn:
        await asyncio.sleep(self.connect.pop(0) if self.connect else 0)
        conn = Conn(next(self.ids))
        self.opened.append(conn)
        return conn

    async def dispose(self, endpoint: str, conn: Conn) -> None:
        conn.closed = True

    async def check_aliveness(self, endpoint: str, conn: Conn) -> bool:
        step = self.alive.pop(0) if self.alive else True
        if isinstance(step, float):
            await asyncio.sleep(step)
            return True
        return bool(step)


async def served(pool: ConnectionPool[str, Conn]) -> bool:
    try:
        async with pool.connection("a", timeout=1.0):
            return True
    except TimeoutError:
        return False


async def plain(kind: str) -> str:
    """On a real loop, with max_size=1."""
    if kind == "a failing aliveness check":
        manager = PlainManager([], [False])
        pool = ConnectionPool[str, Conn](manager, min_idle=1, max_size=1)
        await served(pool)  # creates the first connection
        await served(pool)  # its check fails, so a second one is created
        first = manager.opened[0]
        await pool.close(timeout=1)
        return f"the connection that failed its check was closed, even by close(): {first.closed}"
    if kind in ("cancelled during the aliveness check", "cancelled during connect"):
        if kind == "cancelled during connect":
            manager = PlainManager([1.0], [])
        else:
            manager = PlainManager([], [1.0])
        pool = ConnectionPool[str, Conn](manager, min_idle=1, max_size=1)
        if kind == "cancelled during the aliveness check":
            await served(pool)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(pool.acquire("a"), 0.05)
        return f"after one cancelled acquire, the next is served within 1 s: {await served(pool)}"
    manager = PlainManager([], [])
    pool = ConnectionPool[str, Conn](manager, min_idle=1, max_size=1)
    conn = await pool.acquire("a")
    timeout = 0.5 if kind == "close(timeout=0.5) while a connection is held" else None
    closing = asyncio.create_task(pool.close(timeout=timeout))
    cpu = time.process_time()
    await asyncio.sleep(1)
    busy = time.process_time() - cpu
    if timeout is None:
        outcome = f"close() returned within 1 s: {closing.done()} ({busy:.2f} s of CPU used)"
        closing.cancel()
    else:
        error = closing.exception()
        outcome = (
            f"close() raised {type(error).__name__} after spending {busy:.2f} s of CPU;"
            f" the held connection was closed: {conn.closed}"
        )
    await asyncio.gather(closing, return_exceptions=True)
    return outcome


def main() -> int:
    for workload, knobs in WORKLOADS.items():
        for name, config in CONFIGS:
            report = simloop.explore(make_harness(knobs), seeds=range(50), config=config)
            print(f"{workload}, {name}: {report.summary()}")
            if workload not in EXPECTED:
                assert report.ok, report.summary()
                continue
            assert report.groups
            for group in report.groups:
                assert any(text in group.message for text in EXPECTED[workload]), group.message
    undo = fix_pool()
    try:
        for workload in EXPECTED:
            for name, config in CONFIGS:
                harness = make_harness(WORKLOADS[workload])
                fixed = simloop.explore(harness, seeds=range(50), config=config)
                print(f"{workload}, {name}, fixed pool: {fixed.summary()}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    kinds = (
        "a failing aliveness check",
        "cancelled during the aliveness check",
        "cancelled during connect",
        "close(timeout=0.5) while a connection is held",
        "close() while a connection is held",
    )
    for kind in kinds:
        outcome = asyncio.run(plain(kind))
        print(f"real asyncio loop, {kind}: {outcome}")
        assert ": False" in outcome, outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
