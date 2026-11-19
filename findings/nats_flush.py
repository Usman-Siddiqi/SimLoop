"""nats-py 2.17.0: ``flush()`` reports success during a reconnect, and ``rtt()`` asserts.

Against a simulated NATS server (``findings/_nats.py``), a client sends
requests, publishes, flushes and RTT probes, some under random
``wait_for`` timeouts, with 1% TCP resets and server crashes; a second
client answers requests and collects the publishes. Request/reply integrity,
per-publisher ordering and recovery after the faults all held. Three
things went wrong:

1. ``flush()`` returns at once, without a PING, whenever the client's read
   or flusher task is not running, which is the case for the whole of a
   reconnect: a fallback meant for tasks killed from outside (PR #853)
   treats a reconnect the same way. So ``publish(); flush()`` reports
   success while the message sits in the client's buffer, and if the
   client then gives up reconnecting, the message is never sent.
2. ``rtt()`` has no such check and calls ``_send_ping``, which asserts that
   the transport is connected. During an outage, once a reconnect attempt
   has failed, ``rtt()`` raises a bare ``AssertionError: Client.connect
   must be called first``.
3. Known upstream (#1085, open PR #1063): ``flush()`` writes its PING
   straight to the socket, ahead of commands still queued for the flusher
   task, so the PONG can come back before the server has the PUB.

Run: python findings/nats_flush.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

import nats
import nats.errors
from _nats import NatsServer

import simloop
from simloop import faults

logging.getLogger("nats").setLevel(logging.CRITICAL)
ERRORS = (TimeoutError, nats.errors.Error, OSError)
DURING_RECONNECT = "flush() succeeded during a reconnect, without the server having"
WHILE_CONNECTED = "flush() succeeded but the server had not yet received"
RTT_ASSERT = "Client.connect must be called first"


async def ignore(_error: Exception) -> None:
    pass


def server_got(server: NatsServer, token: bytes) -> bool:
    return any((b"PUB", token) in conn.log for conn in server.history)


async def harness() -> None:
    """Replies match, flushed publishes reached the server, and the client recovers."""
    rng = simloop.rng()
    net = simloop.network()
    server = NatsServer()
    net.add_host("nats", entry=server.serve)
    net.set_link("main", "nats", latency=(0.0005, 0.005))
    await asyncio.sleep(0.01)
    options: dict[str, Any] = {
        "reconnect_time_wait": 0.05,
        "max_reconnect_attempts": -1,
        "connect_timeout": 1,
        "ping_interval": 1,
        "error_cb": ignore,
    }
    responder = await nats.connect("nats://nats:4222", **options)
    received: list[bytes] = []

    async def respond(msg: Any) -> None:
        await asyncio.sleep(rng.uniform(0, 0.03))
        await msg.respond(msg.data)

    async def collect(msg: Any) -> None:
        received.append(msg.data)

    await responder.subscribe("echo", cb=respond)
    await responder.subscribe("seq", cb=collect)
    await responder.flush()
    client = await nats.connect("nats://nats:4222", **options)
    net.set_link("main", "nats", latency=(0.0005, 0.005), reset=0.01)
    nemesis = asyncio.create_task(
        faults.random_crashes(["nats"], every=(0.5, 2.0), downtime=(0.05, 0.3))
    )

    async def publish_and_flush(token: bytes) -> None:
        await client.publish("seq", token)
        reconnecting = client._reading_task.done() or client._flusher_task.done()  # type: ignore[attr-defined]
        await client.flush(timeout=1)
        if not server_got(server, token):
            # Separate asserts so that each cause gets its own failure signature.
            assert not reconnecting, f"{DURING_RECONNECT} {token!r}"
            raise AssertionError(f"{WHILE_CONNECTED} {token!r}")

    async def worker(n: int) -> None:
        for i in range(10):
            token = f"w{n}-{i}".encode()
            timeout = rng.uniform(0.001, 0.05) if rng.random() < 0.3 else 0.5
            roll = rng.random()
            try:
                if roll < 0.15:
                    await asyncio.wait_for(client.flush(timeout=1), timeout)
                elif roll < 0.25:
                    await asyncio.wait_for(client.rtt(timeout=1), timeout)
                elif roll < 0.4:
                    await publish_and_flush(b"s" + token)
                else:
                    if roll < 0.5:
                        msg = await client.request("echo", token, timeout, old_style=True)
                    elif roll < 0.6:
                        call = client.request("echo", token, timeout=1)
                        msg = await asyncio.wait_for(call, timeout)
                    else:
                        msg = await client.request("echo", token, timeout)
                    assert msg.data == token, f"asked for {token!r}, got {msg.data!r}"
            except ERRORS as exc:
                simloop.log(f"{token!r}: {type(exc).__name__}")

    await asyncio.wait_for(asyncio.gather(*(worker(n) for n in range(4))), 60)
    nemesis.cancel()
    net.heal()
    net.set_link("main", "nats", latency=0.001, reset=0.0)
    if not net.host("nats").up:
        net.host("nats").restart()
    await asyncio.sleep(3)
    msg = await client.request("echo", b"final", timeout=1)
    assert msg.data == b"final", msg.data
    await client.flush(timeout=2)
    await asyncio.sleep(0.5)
    assert len(received) == len(set(received)), f"duplicate deliveries: {received}"
    for n in range(4):
        mine = [int(t.split(b"-")[1]) for t in received if t.startswith(f"sw{n}-".encode())]
        assert mine == sorted(mine), f"worker {n}'s publishes arrived out of order: {mine}"
    await asyncio.wait_for(client.close(), 10)
    await asyncio.wait_for(responder.close(), 10)


def kinds(report: simloop.ExploreReport) -> set[str]:
    found = set()
    for group in report.groups:
        for name, marker in (
            ("during reconnect", DURING_RECONNECT),
            ("while connected", WHILE_CONNECTED),
            ("rtt assert", RTT_ASSERT),
        ):
            if marker in group.message:
                found.add(name)
                break
        else:
            found.add(group.message.splitlines()[0])
    return found


async def outage() -> tuple[NatsServer, Any, asyncio.Event]:
    """A connected client whose only server has just gone away for good."""
    server = NatsServer()
    listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    closed = asyncio.Event()

    async def on_closed() -> None:
        closed.set()

    client = await nats.connect(
        f"nats://127.0.0.1:{port}",
        reconnect_time_wait=0.1,
        max_reconnect_attempts=3,
        error_cb=ignore,
        closed_cb=on_closed,
    )
    listener.close()
    for conn in list(server.conns.values()):
        conn.writer.transport.abort()
    return server, client, closed


async def plain_flush() -> tuple[float, bool, bool]:
    server, client, closed = await outage()
    await asyncio.sleep(0.05)
    await client.publish("orders", b"important")
    loop = asyncio.get_running_loop()
    start = loop.time()
    await client.flush(timeout=2)
    took = loop.time() - start
    await asyncio.wait_for(closed.wait(), 5)  # the client gives up reconnecting
    return took, server_got(server, b"important"), client.is_closed


async def plain_rtt() -> str:
    _server, client, _closed = await outage()
    await asyncio.sleep(0.15)  # the first reconnect attempt has failed
    try:
        await client.rtt(timeout=1)
        outcome = "returned"
    except Exception as exc:
        outcome = f"{type(exc).__name__}: {exc}"
    await client.close()
    return outcome


def main() -> int:
    seen: set[str] = set()
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(harness, seeds=range(300), config=config)
        print(f"{scheduler}: {report.summary()}")
        found = kinds(report)
        assert found <= {"during reconnect", "while connected", "rtt assert"}, report.summary()
        seen |= found
    assert seen == {"during reconnect", "while connected", "rtt assert"}, seen
    took, got, closed = asyncio.run(plain_flush())
    print(
        f"real asyncio loop, server gone: publish + flush() returned in {took * 1000:.3f} ms;"
        f" the client then closed={closed} and the server got the message={got}"
    )
    assert took < 0.01
    assert closed
    assert not got
    outcome = asyncio.run(plain_rtt())
    print(f"real asyncio loop, server gone: rtt() raised {outcome}")
    assert outcome == f"AssertionError: {RTT_ASSERT}"
    return 0


if __name__ == "__main__":
    sys.exit(main())
