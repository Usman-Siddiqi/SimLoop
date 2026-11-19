"""aiomisc 18.0.40: ``PoolBase`` loses capacity for good and hands out recycled instances.

A ``PoolBase`` subclass (``maxsize=2``) hands out fake connections to six
workers, each doing ten acquire/use/release rounds. Five workloads each
turn on one kind of trouble: none, failed connects, a health check that
raises, acquires under a short ``asyncio.wait_for``, and ``recycle``.
Checked: a connection handed out is open and not shared, it is not closed
while in use, the pool never holds more than ``maxsize`` connections that
it has not yet sent to be destroyed, and every worker finishes (within 60
simulated seconds; a fault-free run takes about half a second).

Four ways to break it, all in ``PoolBase.__acquire`` and its helpers:

* a failed ``_create_instance`` keeps the semaphore slot it took, so after
  ``maxsize`` failed connects the pool is dead for good: every acquire
  waits for an instance that nothing will create;
* when ``_check_instance`` raises, the instance is sent to be destroyed and
  then returned to the caller anyway; it is closed under the caller, and
  released back into the pool, which has meanwhile replaced it;
* a caller cancelled during ``_check_instance`` takes the instance with it
  (it is neither returned to the queue nor recycled), and one cancelled
  during ``_create_instance`` keeps the slot, so cancelled acquires drain
  the pool until it hangs;
* releasing an instance past its ``recycle`` deadline frees a slot but
  does not wake an acquire that is already waiting for an instance, so
  that acquire waits until some other caller arrives.

Run: python findings/aiomisc_pool.py
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import random
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import aiomisc
from aiomisc.pool import PoolBase

import simloop

MAXSIZE = 2
WORKLOADS: dict[str, dict[str, float]] = {
    "no faults": {},
    "failed connects": {"create_fail": 0.3},
    "a health check that raises": {"check_raise": 0.05},
    "cancelled acquires": {"cancel": 0.3},
    "recycle": {"recycle": 0.05},
}
logging.getLogger("aiomisc.pool").setLevel(logging.CRITICAL)


class Broken(Exception):
    pass


class Conn:
    def __init__(self, conn_id: int) -> None:
        self.id = conn_id
        self.closed = False
        self.users = 0


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Connections are open and unshared, the pool stays bounded, nobody hangs."""
        rng = simloop.rng()
        ids = itertools.count()
        live: set[Conn] = set()
        destroying: set[Conn] = set()

        class Pool(aiomisc.PoolBase[Conn]):
            async def _create_instance(self) -> Conn:
                await asyncio.sleep(rng.uniform(0, 0.01))
                if rng.random() < knobs.get("create_fail", 0):
                    raise Broken("connect failed")
                conn = Conn(next(ids))
                live.add(conn)
                kept = live - destroying - set(self._recycle_bin._queue)  # type: ignore[attr-defined]
                assert len(kept) <= MAXSIZE, f"the pool holds {len(kept)} connections"
                return conn

            async def _destroy_instance(self, conn: Conn) -> None:
                destroying.add(conn)
                await asyncio.sleep(rng.uniform(0, 0.005))
                conn.closed = True
                live.discard(conn)

            async def _check_instance(self, conn: Conn) -> bool:
                await asyncio.sleep(rng.uniform(0, 0.005))
                if rng.random() < knobs.get("check_raise", 0):
                    raise Broken("check failed")
                return True

        pool = Pool(maxsize=MAXSIZE, recycle=knobs.get("recycle") or None)  # type: ignore[arg-type]

        async def use() -> None:
            async with pool.acquire() as conn:
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
                try:
                    if rng.random() < knobs.get("cancel", 0):
                        await asyncio.wait_for(use(), rng.uniform(0, 0.02))
                    else:
                        await use()
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
        assert not pending, (
            f"{len(pending)} worker(s) still waiting in acquire() after 60 s;"
            f" the pool has {len(live - destroying)} open connection(s), maxsize {MAXSIZE}"
        )
        await pool.close()

    return harness


def fix_pool() -> Callable[[], None]:
    """Give a slot back when a connect fails or is cancelled, retry after a raising check,
    put the instance back when the check is cancelled, and replace a recycled
    instance when someone is waiting for one."""
    names = ("_PoolBase__acquire", "_PoolBase__create_new_instance", "_PoolBase__recycle_instance")
    originals = {name: getattr(PoolBase, name) for name in names}
    recycle = originals["_PoolBase__recycle_instance"]

    async def create_new_instance(self: Any) -> None:
        await self._semaphore.acquire()
        try:
            instance = await self._create_instance()
        except BaseException:
            self._semaphore.release()
            raise
        self._len += 1
        if self._recycle:
            self._recycle_times[instance] += self._recycle * (1 + random.random())
        self._instances.put_nowait(instance)

    async def acquire(self: Any) -> Any:
        if not self._semaphore.locked():
            await create_new_instance(self)
        instance = await self._instances.get()
        try:
            healthy = await self._check_instance(instance)
        except Exception:
            healthy = False
        except BaseException:
            self._instances.put_nowait(instance)
            raise
        if not healthy:
            recycle_instance(self, instance)
            return await acquire(self)
        self._used.add(instance)
        return instance

    def recycle_instance(self: Any, instance: Any) -> None:
        recycle(self, instance)
        if self._instances._getters:
            self._PoolBase__create_task(create_new_instance(self))

    PoolBase._PoolBase__acquire = acquire  # type: ignore[attr-defined]
    PoolBase._PoolBase__create_new_instance = create_new_instance  # type: ignore[attr-defined]
    PoolBase._PoolBase__recycle_instance = recycle_instance  # type: ignore[attr-defined]

    def undo() -> None:
        for name, original in originals.items():
            setattr(PoolBase, name, original)

    return undo


class PlainPool(aiomisc.PoolBase[Conn]):
    """For the real-loop runs: each hook does what the next scripted step says."""

    def __init__(self, script: dict[str, list[Any]], **kwargs: Any) -> None:
        self.script = script
        self.ids = itertools.count()
        super().__init__(**kwargs)

    async def _create_instance(self) -> Conn:
        if self.script["create"] and self.script["create"].pop(0) == "fail":
            raise Broken("connect failed")
        return Conn(next(self.ids))

    async def _destroy_instance(self, conn: Conn) -> None:
        conn.closed = True

    async def _check_instance(self, conn: Conn) -> bool:
        step = self.script["check"].pop(0) if self.script["check"] else "ok"
        if step == "raise":
            raise Broken("check failed")
        if step == "slow":
            await asyncio.sleep(1)
        return True


async def served(pool: PlainPool, within: float = 1.0) -> bool:
    try:
        async with asyncio.timeout(within), pool.acquire():
            return True
    except TimeoutError:
        return False


async def plain(kind: str) -> str:
    """On a real loop, with maxsize=1."""
    if kind == "failed connects":
        pool = PlainPool({"create": ["fail"], "check": []}, maxsize=1)
        with contextlib.suppress(Broken):
            await served(pool)
        outcome = f"after one failed connect, the next acquire is served: {await served(pool)}"
    elif kind == "a health check that raises":
        pool = PlainPool({"create": [], "check": ["raise"]}, maxsize=1)
        async with pool.acquire() as conn:
            await asyncio.sleep(0.01)
            closed = conn.closed
        async with pool.acquire() as again:
            same = again is conn
        outcome = (
            f"the caller's connection is closed while held: {closed};"
            f" the next caller gets the same closed connection: {same}"
        )
    elif kind == "cancelled acquires":
        pool = PlainPool({"create": [], "check": ["slow"]}, maxsize=1)
        await served(pool, within=0.05)
        outcome = f"after one cancelled acquire, the next acquire is served: {await served(pool)}"
    else:
        pool = PlainPool({"create": [], "check": []}, maxsize=1, recycle=0.1)
        holding = asyncio.Event()

        async def holder() -> None:
            async with pool.acquire():
                holding.set()
                await asyncio.sleep(0.3)  # past the deadline, which is 0.1 to 0.2 s

        first = asyncio.create_task(holder())
        await holding.wait()
        waiter = await served(pool, within=1.3)
        await first
        outcome = f"a caller waiting when a recycled instance is released is served: {waiter}"
    await pool.close()
    return outcome


def main() -> int:
    configs = [("random", simloop.SimConfig())]
    configs.append(("fifo", simloop.SimConfig(scheduler="fifo", timer_jitter=0.001)))
    expected = {
        "failed connects": ["still waiting in acquire()"],
        "a health check that raises": [
            "closed connection",
            "closed while in use",
            "the pool holds",
        ],
        "cancelled acquires": ["still waiting in acquire()"],
        "recycle": ["still waiting in acquire()"],
    }
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(100), config=config)
            print(f"{workload}, {name}: {report.summary()}")
            if workload == "no faults":
                assert report.ok, report.summary()
                continue
            assert report.groups
            for group in report.groups:
                assert any(text in group.message for text in expected[workload]), group.message
    undo = fix_pool()
    try:
        for workload, knobs in WORKLOADS.items():
            for name, config in configs:
                fixed = simloop.explore(make_harness(knobs), seeds=range(100), config=config)
                print(f"{workload}, {name}, fixed pool: {fixed.summary()}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    for kind in expected:
        outcome = asyncio.run(plain(kind))
        print(f"real asyncio loop, {kind}: {outcome}")
        broken = "True" if kind == "a health check that raises" else "False"
        assert outcome.endswith(broken), outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
