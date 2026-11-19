"""aiomcache 0.8.2: a cancelled command leaves its reply for the next one.

The ``acquire`` decorator around every ``Client`` command catches
``Exception`` only and always returns the connection to the pool. When a
command is cancelled (a timeout, a client disconnect) after it was written
but before its reply was read, the connection goes back to the pool with that
reply still on the wire. The next command on the connection reads it as its
own reply, and every later command on that connection reads the reply of the
one before, until a reply fails to parse. ``get(key)`` returns ``None`` for a
key that exists, or an older value of the same key; ``add`` and ``delete``
can report another command's outcome.

Separately, when a command fails because its connection dropped, the pool
closes that connection without waking a caller already waiting in
``acquire``; with every connection gone that way, the waiter blocks forever
although the pool has room to open a new one.

Both are fixed on the upstream default branch (the reply mix-up is issue
aio-libs/aiomcache#489, fixed by pull request #510 on 2026-11-18; the pool
now wakes a waiter with a placeholder) but not released: 0.8.2 (May 2024)
is still the latest release on PyPI. SimLoop rediscovers the first from a
"reads see your own writes" property with random client timeouts, and the
second as a deadlock under injected TCP resets.

Run: python findings/aiomcache_cancel_desync.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from collections.abc import Callable

import aiomcache

import simloop


class MemcachedServer:
    """get/gets/set/add/delete over the memcached text protocol."""

    def __init__(self, delay: Callable[[], float]) -> None:
        self.delay = delay
        self.data: dict[bytes, bytes] = {}

    async def serve(self) -> None:
        server = await asyncio.start_server(self.handle, port=11211)
        async with server:
            await server.serve_forever()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError, asyncio.IncompleteReadError):
            while line := await reader.readline():
                words = line.split()
                if words[0] in (b"set", b"add"):
                    value = (await reader.readexactly(int(words[4]) + 2))[:-2]
                await asyncio.sleep(self.delay())
                if words[0] in (b"get", b"gets"):
                    for key in words[1:]:
                        if key in self.data:
                            value = self.data[key]
                            writer.write(b"VALUE %s 0 %d\r\n%s\r\n" % (key, len(value), value))
                    writer.write(b"END\r\n")
                elif words[0] == b"set" or (words[0] == b"add" and words[1] not in self.data):
                    self.data[words[1]] = value
                    writer.write(b"STORED\r\n")
                elif words[0] == b"add":
                    writer.write(b"NOT_STORED\r\n")
                elif words[0] == b"delete":
                    found = self.data.pop(words[1], None) is not None
                    writer.write(b"DELETED\r\n" if found else b"NOT_FOUND\r\n")
                else:
                    writer.write(b"ERROR\r\n")
                await writer.drain()
        writer.close()


async def harness() -> None:
    """Each worker owns a key; a get returns a value the worker may have stored."""
    rng = simloop.rng()
    server = MemcachedServer(lambda: rng.uniform(0, 0.02))
    net = simloop.network()
    net.add_host("cache", entry=server.serve)
    net.set_link("main", "cache", latency=(0.0005, 0.002))
    await asyncio.sleep(0.01)
    client = aiomcache.Client("cache", pool_size=2)

    for n in range(3):
        await client.set(b"w%d" % n, b"%d-init" % n)

    async def worker(n: int) -> None:
        key = b"w%d" % n
        possible = {b"%d-init" % n}
        for i in range(8):
            timeout = rng.choice([None, None, rng.uniform(0, 0.02)])
            try:
                if rng.random() < 0.5:
                    token = b"%d-%d" % (n, i)
                    possible.add(token)
                    await asyncio.wait_for(client.set(key, token), timeout)
                    possible = {token}
                else:
                    got = await asyncio.wait_for(client.get(key), timeout)
                    assert got in possible, f"get({key!r}) returned {got!r}, stored {possible}"
            except (TimeoutError, aiomcache.exceptions.ClientException) as exc:
                simloop.log(f"{key!r} op {i}: {type(exc).__name__}")

    await asyncio.gather(*(worker(n) for n in range(3)))
    await client.close()


async def dropped_connections() -> None:
    """Every request finishes, with a value or an error, while connections reset."""
    rng = simloop.rng()
    server = MemcachedServer(lambda: rng.uniform(0, 0.02))
    net = simloop.network()
    net.add_host("cache", entry=server.serve)
    net.set_link("main", "cache", latency=(0.0005, 0.002), reset=rng.choice([0.05, 0.2]))
    await asyncio.sleep(0.01)
    client = aiomcache.Client("cache", pool_size=rng.randint(1, 2))

    async def request() -> None:
        await asyncio.sleep(rng.uniform(0, 0.05))
        with contextlib.suppress(aiomcache.exceptions.ClientException, ConnectionError):
            await client.get(b"k")

    # A waiter the pool never wakes shows up as a deadlock.
    await asyncio.gather(*(request() for _ in range(rng.randint(2, 5))))
    await client.close()


async def plain() -> tuple[object, object]:
    server = MemcachedServer(lambda: 0.05)
    listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    client = aiomcache.Client("127.0.0.1", port, pool_size=1)
    await client.set(b"a", b"apple")
    await client.set(b"b", b"banana")
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(client.get(b"a"), 0.01)
    got_b = await client.get(b"b")  # reads the reply to get(b"a")
    got_a = await client.get(b"a")  # reads the reply to get(b"b")
    await client.close()
    listener.close()
    return got_b, got_a


async def plain_waiter() -> str:
    async def drop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readline()
        await asyncio.sleep(0.05)
        writer.close()  # the connection drops mid-command, as in a server restart

    listener = await asyncio.start_server(drop, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    client = aiomcache.Client("127.0.0.1", port, pool_size=1)
    first = asyncio.create_task(client.get(b"k"))
    await asyncio.sleep(0.01)
    second = asyncio.create_task(client.get(b"k"))
    with contextlib.suppress(aiomcache.exceptions.ClientException):
        await first
    try:
        await asyncio.wait_for(second, 2)
        outcome = "finished"
    except TimeoutError:
        outcome = f"still waiting after 2 s, pool size {client._pool.size()}"
    listener.close()
    return outcome


def main() -> int:
    report = simloop.explore(harness, seeds=range(1000))
    print(report.summary())
    assert not report.ok
    fifo = simloop.explore(
        harness, seeds=range(1000), config=simloop.SimConfig(scheduler="fifo", timer_jitter=0.001)
    )
    print(f"fifo + jitter: {fifo.summary().splitlines()[0]}")
    assert not fifo.ok
    hangs = simloop.explore(dropped_connections, seeds=range(1000))
    print(f"dropped connections: {hangs.summary()}")
    assert {group.kind for group in hangs.groups} == {"deadlock"}
    got_b, got_a = asyncio.run(plain())
    print(f"real asyncio loop, after a get timed out: get(b'b') {got_b!r}, get(b'a') {got_a!r}")
    assert got_b is None
    assert got_a is None
    waiter = asyncio.run(plain_waiter())
    print(f"real asyncio loop, pool_size=1, after the connection dropped: second caller {waiter}")
    assert waiter.startswith("still waiting")
    return 0


if __name__ == "__main__":
    sys.exit(main())
