"""redis-py asyncio: every reply goes to the request that asked for it.

Random client timeouts, injected TCP resets and server crashes; afterwards the
client must recover and leave no pooled connection marked in use, and the
pool never holds more than ``max_connections``. Run with ``ConnectionPool``
and with ``BlockingConnectionPool`` (``max_connections=2``, six workers,
pool timeout none, 50 ms or 1 s). Clean on
redis 8.1.0 (see docs/FINDINGS.md, "Tested, nothing found").

Run: python findings/redis_reply_integrity.py
"""

from __future__ import annotations

import asyncio
import sys

import redis.asyncio as redis
from _resp import RespServer
from redis.exceptions import RedisError

import simloop
from simloop import faults

CMD_ERRORS = (TimeoutError, RedisError, ConnectionError, OSError)


async def workload(blocking: bool = False) -> None:
    net = simloop.network()
    server = RespServer(slow=0.05)
    net.add_host("db", entry=server.serve)
    net.set_link("main", "db", latency=(0.001, 0.01), reset=0.01)
    await asyncio.sleep(0.01)
    rng = simloop.rng()
    limit = 2 if blocking else 4
    pool: redis.ConnectionPool
    if blocking:
        timeout = rng.choice([None, 0.05, 1.0])
        pool = redis.BlockingConnectionPool(
            host="db", port=6379, max_connections=limit, protocol=2, timeout=timeout
        )
    else:
        pool = redis.ConnectionPool(host="db", port=6379, max_connections=limit, protocol=2)
    client = redis.Redis(connection_pool=pool)
    nemesis = asyncio.create_task(
        faults.random_crashes(["db"], every=(0.3, 1.5), downtime=(0.05, 0.3))
    )

    async def worker(n: int) -> None:
        for i in range(10):
            token = f"w{n}-{i}"
            timeout = rng.uniform(0.0, 0.06) if rng.random() < 0.3 else None
            try:
                if rng.random() < 0.3:
                    async with client.pipeline(transaction=False) as pipe:
                        pipe.echo(token + "a")
                        pipe.echo(token + "b")
                        replies = await asyncio.wait_for(pipe.execute(), timeout)
                    assert replies == [(token + "a").encode(), (token + "b").encode()], (
                        f"pipeline for {token!r} got {replies!r}"
                    )
                else:
                    reply = await asyncio.wait_for(client.echo(token), timeout)
                    assert reply == token.encode(), f"asked for {token!r}, got {reply!r}"
            except CMD_ERRORS as exc:
                simloop.log(f"{token}: {type(exc).__name__}")
            total = len(pool._in_use_connections) + len(pool._available_connections)
            assert total <= limit, f"the pool holds {total} connections, max {limit}"

    async with asyncio.TaskGroup() as tg:
        for n in range(6 if blocking else 4):
            tg.create_task(worker(n))
    nemesis.cancel()
    net.heal()
    net.set_link("main", "db", latency=0.001, reset=0.0)
    db = net.host("db")
    if not db.up:
        db.restart()
    await asyncio.sleep(1)
    for attempt in range(5):
        try:
            assert await client.echo("final") == b"final"
            break
        except CMD_ERRORS as exc:
            simloop.log(f"recovery attempt {attempt}: {type(exc).__name__}")
            await asyncio.sleep(1)
    else:
        raise AssertionError("client never recovered after faults stopped")
    in_use = len(pool._in_use_connections)
    assert in_use == 0, f"{in_use} connections still marked in use"
    await client.aclose()


async def blocking_workload() -> None:
    await workload(blocking=True)


def main() -> int:
    report = simloop.explore(workload, seeds=range(200))
    print(f"ConnectionPool: {report.summary()}")
    ok = report.ok
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        blocking = simloop.explore(blocking_workload, seeds=range(200), config=config)
        print(f"BlockingConnectionPool, {scheduler}: {blocking.summary()}")
        ok = ok and blocking.ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
