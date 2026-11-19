"""asyncpg 0.32.0: a connection lost during ``release`` raises ``InternalClientError``.

Against a simulated PostgreSQL server (``findings/_pg.py``), four workers
run ``pool.fetchval`` and two-query ``pool.acquire()`` blocks under random
``wait_for`` timeouts (asyncpg answers each with a CancelRequest), with 1%
TCP resets and server crashes. Every reply belonged to its query and the
pool recovered after the faults, but two things went wrong around release:

1. ``Pool.release`` checks that the connection is still attached, then runs
   the holder's ``release`` in a task (``asyncio.shield``). If the
   connection is lost in between, its ``connection_lost`` callback already
   returned the holder to the pool, and the task raises
   ``InternalClientError: PoolConnectionHolder.release() called on a free
   connection holder``, an error class meant for asyncpg's own bugs, which
   ``except asyncpg.PostgresError`` or ``InterfaceError`` does not catch.
2. Cosmetic: when the caller is cancelled during ``release`` and the reset
   query then fails because the connection dropped, nothing retrieves the
   shielded task's error and asyncio logs "Task exception was never
   retrieved".

Run: python findings/asyncpg_pool.py
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import logging
import sys

import asyncpg
from _pg import PgServer

import simloop
from simloop import faults

logging.getLogger("asyncpg").setLevel(logging.CRITICAL)
ERRORS = (TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError, OSError)
QUERY = "SELECT $1::text"


async def harness() -> None:
    """Every reply belongs to its query, and the pool recovers after faults."""
    rng = simloop.rng()
    net = simloop.network()
    net.add_host("db", entry=PgServer(lambda: rng.uniform(0, 0.05)).serve)
    net.set_link("main", "db", latency=(0.0005, 0.005))
    await asyncio.sleep(0.01)
    size = rng.randint(1, 3)
    pool = await asyncpg.create_pool(
        host="db", user="u", database="d", ssl=False, min_size=1, max_size=size
    )
    net.set_link("main", "db", latency=(0.0005, 0.005), reset=0.01)
    nemesis = asyncio.create_task(
        faults.random_crashes(["db"], every=(0.3, 1.5), downtime=(0.05, 0.3))
    )

    async def held(token: str) -> tuple[str, str]:
        async with pool.acquire() as conn:
            return await conn.fetchval(QUERY, token + "a"), await conn.fetchval(QUERY, token + "b")

    async def worker(n: int) -> None:
        for i in range(10):
            token = f"w{n}-{i}"
            timeout = rng.uniform(0, 0.06) if rng.random() < 0.3 else None
            try:
                if rng.random() < 0.5:
                    got = await asyncio.wait_for(pool.fetchval(QUERY, token), timeout)
                    assert got == token, f"asked for {token!r}, got {got!r}"
                else:
                    pair = await asyncio.wait_for(held(token), timeout)
                    assert pair == (token + "a", token + "b"), f"{token!r} got {pair!r}"
            except ERRORS as exc:
                simloop.log(f"{token}: {type(exc).__name__}")

    await asyncio.wait_for(asyncio.gather(*(worker(n) for n in range(4))), 60)
    nemesis.cancel()
    net.heal()
    net.set_link("main", "db", latency=0.001, reset=0.0)
    if not net.host("db").up:
        net.host("db").restart()
    await asyncio.sleep(1)
    tokens = [f"final{k}" for k in range(size)]
    for attempt in range(5):  # idle pooled connections may have died; asyncpg finds out on use
        try:
            got = await asyncio.wait_for(
                asyncio.gather(*(pool.fetchval(QUERY, token) for token in tokens)), 10
            )
            assert got == tokens, got
            break
        except ERRORS as exc:
            simloop.log(f"recovery attempt {attempt}: {type(exc).__name__}")
    else:
        raise AssertionError("the pool never recovered after the faults stopped")
    await asyncio.wait_for(pool.close(), 10)


def release_failures(report: simloop.ExploreReport) -> set[str]:
    kinds = set()
    for group in report.groups:
        if "called on a free connection holder" in group.message:
            kinds.add("free holder")
        elif group.kind == "unhandled" and "ConnectionDoesNotExistError" in group.message:
            kinds.add("never retrieved")
        else:
            kinds.add(group.message.splitlines()[0])
    return kinds


async def plain_free_holder() -> str:
    server = PgServer(lambda: 0.0)
    listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    pool = await asyncpg.create_pool(
        host="127.0.0.1", port=port, user="u", database="d", ssl=False, min_size=1, max_size=1
    )
    outcome = "released"
    try:
        async with pool.acquire() as conn:
            await conn.fetchval(QUERY, "x")
            # What asyncio's transport does on a socket error such as a reset:
            # schedule connection_lost for the next loop iteration.
            conn._con._transport.abort()  # type: ignore[attr-defined]
    except Exception as exc:
        outcome = f"{type(exc).__name__}: {exc}"
    assert await pool.fetchval(QUERY, "after") == "after"
    pool.terminate()
    listener.close()
    return outcome


async def plain_never_retrieved() -> list[str]:
    server = PgServer(lambda: 0.0)
    listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    logged: list[str] = []
    asyncio.get_running_loop().set_exception_handler(
        lambda loop, context: logged.append(context["message"])
    )
    pool = await asyncpg.create_pool(
        host="127.0.0.1", port=port, user="u", database="d", ssl=False, min_size=1, max_size=1
    )
    server.drop_on_simple_query = True  # the release's reset query stalls, then drops

    async def held() -> None:
        async with pool.acquire() as conn:
            await conn.fetchval(QUERY, "x")

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(held(), 0.02)  # times out during the release
    server.drop_on_simple_query = False
    await asyncio.sleep(0.2)
    gc.collect()
    await asyncio.sleep(0)
    assert await pool.fetchval(QUERY, "after") == "after"
    pool.terminate()
    listener.close()
    return logged


def main() -> int:
    seen: set[str] = set()
    for scheduler in ("random", "pct", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(harness, seeds=range(500), config=config)
        print(f"{scheduler}: {report.summary().splitlines()[0]}")
        kinds = release_failures(report)
        assert kinds <= {"free holder", "never retrieved"}, report.summary()
        seen |= kinds
    assert seen == {"free holder", "never retrieved"}, seen
    outcome = asyncio.run(plain_free_holder())
    print(f"real asyncio loop, connection lost as the block exits: {outcome}")
    assert "called on a free connection holder" in outcome
    logged = asyncio.run(plain_never_retrieved())
    print(f"real asyncio loop, caller cancelled during a failing release: logged {logged}")
    assert logged == ["Task exception was never retrieved"]
    return 0


if __name__ == "__main__":
    sys.exit(main())
