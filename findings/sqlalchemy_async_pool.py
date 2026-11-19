"""SQLAlchemy 2.1.9 asyncio engine over aiomysql: pool and transactions under faults (clean).

``create_async_engine("mysql+aiomysql://...", pool_size=2, max_overflow=1)``
runs unmodified (greenlets included) against the simulated MySQL server in
``_mysql.py``. Five workers run eight jobs each, through ``engine.connect()``
or ``engine.begin()``: a ``SELECT SLEEP(t), '<token>'``, sometimes a raw
``BEGIN`` left open, sometimes a commit. Workloads: no faults; 30% of jobs
under a short ``asyncio.wait_for``; the same with ``pool_timeout=0.02``;
with ``pool_pre_ping``; 0.3% TCP resets per segment; server crashes every
0.2-0.8 s. Checked: every reply carries its own token, at most
``pool_size + max_overflow`` client connections are open, a checked-out
connection is never inside a transaction on the server, every worker
finishes, nothing stays checked out, the engine serves a query afterwards,
and ``dispose()`` closes every server session. All held, random and fifo,
with ``strict_cancellation=True``.

Run: python findings/sqlalchemy_async_pool.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from _mysql import STATUS_IN_TRANS, MysqlServer
from pymysql.err import MySQLError
from sqlalchemy import event, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

import simloop
from simloop import faults

logging.getLogger("sqlalchemy").setLevel(logging.CRITICAL)
SIZE, OVERFLOW = 2, 1
ERRORS = (TimeoutError, SQLAlchemyError, MySQLError, ConnectionError, OSError)
WORKLOADS: dict[str, dict[str, float]] = {
    "no faults": {},
    "cancelled jobs": {"cancel": 0.3},
    "cancelled jobs, pool_timeout=0.02": {"cancel": 0.3, "pool_timeout": 0.02},
    "cancelled jobs, pool_pre_ping": {"cancel": 0.3, "ping": 1},
    "TCP resets": {"reset": 0.003},
    "server crashes": {"crash": 1, "cancel": 0.2},
}


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Replies match, the pool stays bounded and clean, nothing hangs or leaks."""
        net = simloop.network()
        server = MysqlServer()
        net.add_host("db", entry=server.serve)
        net.set_link("main", "db", latency=(0.0005, 0.003), reset=knobs.get("reset", 0.0))
        await asyncio.sleep(0.01)
        rng = simloop.rng()
        engine = create_async_engine(
            "mysql+aiomysql://u:p@db:3306/d",
            pool_size=SIZE,
            max_overflow=OVERFLOW,
            pool_timeout=knobs.get("pool_timeout", 30),
            pool_pre_ping=bool(knobs.get("ping")),
        )
        drivers: list[Any] = []
        peak = 0

        @event.listens_for(engine.sync_engine, "connect")
        def on_connect(dbapi_conn: Any, record: Any) -> None:
            nonlocal peak
            drivers.append(dbapi_conn.driver_connection)
            peak = max(peak, sum(not driver.closed for driver in drivers))

        @event.listens_for(engine.sync_engine, "checkout")
        def on_checkout(dbapi_conn: Any, record: Any, proxy: Any) -> None:
            session_id = dbapi_conn.driver_connection.server_thread_id[0]
            status = server.sessions[session_id].status
            assert not status & STATUS_IN_TRANS, (
                f"checked out connection {session_id} is still in a transaction"
            )

        async def job(token: str) -> None:
            opener = engine.begin if rng.random() < 0.4 else engine.connect
            async with opener() as conn:
                if rng.random() < 0.15:
                    await conn.execute(text("BEGIN"))
                delay = round(rng.uniform(0, 0.03), 3)
                result = await conn.execute(text(f"SELECT SLEEP({delay}), '{token}'"))
                rows = [tuple(row) for row in result.all()]
                assert rows == [("0", token)], f"{token} got {rows}"
                if opener is engine.connect and rng.random() < 0.3:
                    await conn.commit()

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

        nemesis = None
        if knobs.get("crash"):
            crashes = faults.random_crashes(["db"], every=(0.2, 0.8), downtime=(0.02, 0.1))
            nemesis = asyncio.create_task(crashes)
        workers = [asyncio.create_task(worker(n)) for n in range(5)]
        done, pending = await asyncio.wait(workers, timeout=60)
        for task in pending:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for task in done:
            task.result()
        assert not pending, f"{len(pending)} worker(s) stuck after 60 s: {engine.pool.status()}"
        assert peak <= SIZE + OVERFLOW, f"{peak} connections open, limit {SIZE + OVERFLOW}"
        if nemesis is not None:
            nemesis.cancel()
            if not net.host("db").up:
                net.host("db").restart()
        net.set_link("main", "db", latency=0.001, reset=0.0)
        await asyncio.sleep(0.5)
        out = engine.pool.checkedout()  # type: ignore[attr-defined]
        assert out == 0, f"{out} connection(s) still checked out: {engine.pool.status()}"
        for attempt in range(3):
            try:
                async with engine.connect() as conn:
                    assert (await conn.execute(text("SELECT 'last'"))).all() == [("last",)]
                break
            except ERRORS as exc:
                simloop.log(f"recovery attempt {attempt}: {type(exc).__name__}")
        else:
            raise AssertionError("the engine never served a query after faults stopped")
        await engine.dispose()
        await asyncio.sleep(0.5)
        assert not server.open, f"{len(server.open)} server session(s) open after dispose()"

    return harness


def main() -> int:
    for workload, knobs in WORKLOADS.items():
        for scheduler in ("random", "fifo"):
            config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
            if scheduler == "fifo":
                config = config.replace(timer_jitter=0.001)
            report = simloop.explore(make_harness(knobs), seeds=range(100), config=config)
            print(f"{workload}, {scheduler}: {report.summary().splitlines()[0]}")
            assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
