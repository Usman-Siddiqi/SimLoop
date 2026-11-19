"""coredis 6.9.0: two ways cancellation makes the connection pool hang for good.

1. Each cancelled pipeline permanently removes a slot from the pool.
   ``ConnectionPool`` hands out ``max_connections`` slots (64 by default)
   from a queue that starts full of ``None`` placeholders. A pipeline holds
   its connection while it waits for the replies, and a cancelled pipeline
   (a timeout, a client disconnect, a ``TaskGroup`` abort) invalidates that
   connection so that no half-read reply is reused. ``release`` then drops
   the unusable connection without putting a placeholder back, and the
   connection's own cleanup only restores a slot for a connection sitting
   in the queue. After ``max_connections`` such cancellations every later
   command waits forever (the pool's ``timeout`` is ``None`` by default).

2. A caller that gives up waiting for a connection swallows the wakeup
   meant for the next one. ``coredis._concurrency.Queue.get`` appends an
   ``Event`` to ``_getters`` and waits on it; when the waiter is cancelled
   the event stays in the list. The next ``put`` sets that stale event, the
   caller actually waiting is never woken, and it keeps the queue's lock,
   so every caller after it waits too, with a free connection in the pool.

The harness tells the two apart from the pool's state after 30 s.

Run: python findings/coredis_pool_hangs.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from typing import Any

import coredis
from _resp import RespServer

import simloop


async def echo(client: coredis.Redis[bytes], token: str) -> bytes:
    return await client.echo(token)


async def pipelined(client: coredis.Redis[bytes], token: str) -> list[bytes]:
    async with client.pipeline(transaction=False) as pipe:
        first = pipe.echo(token + "a")
        second = pipe.echo(token + "b")
    return [await first, await second]


async def harness() -> None:
    """Every request finishes within 30 s, and gets its own reply."""
    rng = simloop.rng()
    net = simloop.network()
    net.add_host("db", entry=RespServer(slow=0.05, resp3=True).serve)
    net.set_link("main", "db", latency=(0.001, 0.01))
    await asyncio.sleep(0.01)
    client: coredis.Redis[bytes] = coredis.Redis(
        host="db", port=6379, max_connections=rng.randint(1, 3)
    )

    async def worker(n: int) -> None:
        for i in range(10):
            token = f"w{n}-{i}"
            timeout = rng.uniform(0, 0.06) if rng.random() < 0.3 else None
            with contextlib.suppress(TimeoutError):
                if rng.random() < 0.3:
                    replies = await asyncio.wait_for(pipelined(client, token), timeout)
                    assert replies == [(token + "a").encode(), (token + "b").encode()], replies
                else:
                    reply = await asyncio.wait_for(echo(client, token), timeout)
                    assert reply == token.encode(), f"asked for {token!r}, got {reply!r}"

    hang = None
    async with client:
        workers = asyncio.gather(*(worker(n) for n in range(4)))
        try:
            await asyncio.wait_for(workers, 30)
        except TimeoutError:
            hang = stuck(client.connection_pool)
    # Separate asserts so that each cause gets its own failure signature.
    assert hang != "free", "a caller waited 30 s for a connection while one was free in the pool"
    size = client.connection_pool.max_connections
    assert hang != "gone", f"a caller waited 30 s: every pool slot ({size}) is gone"
    assert hang is None, f"a caller waited 30 s ({hang})"


def stuck(pool: coredis.ConnectionPool[Any]) -> str:
    slots = list(pool._available_connections._queue)
    if any(slot is not None for slot in slots):
        return "free"
    if not slots and not pool._online_connections:
        return "gone"
    return f"{len(slots)} free slots, {len(pool._online_connections)} open"


async def plain_waiter() -> str:
    listener = await asyncio.start_server(RespServer(delay=0.05, resp3=True).handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    client: coredis.Redis[bytes] = coredis.Redis(host="127.0.0.1", port=port, max_connections=1)
    async with client:
        await echo(client, "warm up")
        holder = asyncio.create_task(pipelined(client, "held"))  # holds the connection 50 ms
        await asyncio.sleep(0.005)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(echo(client, "impatient"), 0.01)  # gives up waiting
        patient = asyncio.create_task(echo(client, "patient"))
        await holder
        try:
            outcome = repr(await asyncio.wait_for(patient, 2))
        except TimeoutError:
            free = len(client.connection_pool._available_connections)
            outcome = f"still waiting after 2 s with {free} free connection in the pool"
        client.connection_pool.disconnect()
    listener.close()
    return outcome


async def plain() -> tuple[int, str]:
    listener = await asyncio.start_server(RespServer(delay=0.05, resp3=True).handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    client: coredis.Redis[bytes] = coredis.Redis(host="127.0.0.1", port=port, max_connections=2)
    async with client:
        await echo(client, "warm up")
        for _ in range(2):
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(pipelined(client, "x"), 0.01)
        slots = len(client.connection_pool._available_connections._queue)
        try:
            outcome = repr(await asyncio.wait_for(echo(client, "after"), 2))
        except TimeoutError:
            outcome = "still waiting for a connection after 2 s"
        client.connection_pool.disconnect()
    listener.close()
    return slots, outcome


def main() -> int:
    report = simloop.explore(harness, seeds=range(500))
    print(report.summary())
    messages = " ".join(group.message for group in report.groups)
    assert "while one was free" in messages
    assert "every pool slot" in messages
    fifo = simloop.explore(
        harness, seeds=range(500), config=simloop.SimConfig(scheduler="fifo", timer_jitter=0.001)
    )
    print(f"fifo + jitter: {fifo.summary().splitlines()[0]}")
    assert not fifo.ok
    slots, outcome = asyncio.run(plain())
    print(f"real asyncio loop, max_connections=2, after 2 cancelled pipelines: {slots} free slots,")
    print(f"  the next command is {outcome}")
    assert slots == 0
    assert outcome.startswith("still waiting")
    waiter = asyncio.run(plain_waiter())
    print("real asyncio loop, max_connections=1, after one caller gave up waiting for the")
    print(f"  connection: the next caller is {waiter}")
    assert waiter.startswith("still waiting")
    return 0


if __name__ == "__main__":
    sys.exit(main())
