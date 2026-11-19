"""grpclib 0.4.9: a stream reset before its handler starts leaks it and its flow-control window.

A grpclib server and client (JSON codec, no protobuf) talk over the
simulated network. Four clients make unary calls with payloads of 0, 100
or 70,000 bytes, some under the call's own timeout and some under an outer
``asyncio.wait_for``. Checked: replies match requests and, once every call
has finished, the server no longer holds any of the HTTP/2 streams.

That fails. When ``HEADERS``, ``DATA`` and ``RST_STREAM`` for a stream are
processed in one ``data_received`` call, ``Handler.accept`` creates the
handler task and ``Handler.cancel`` cancels it before its first step.
asyncio then never enters the coroutine, so its ``finally:
release_stream()`` never runs: the stream stays registered and the DATA
it buffered is never acknowledged, so it is lost from the connection's
receive window for good. Known upstream (#217, fix in open PR #218);
rediscovered here from a generic "no stream outlives its call" check.

A second harness applies a fix for that and adds server crashes, connection
resets, server-streaming and bidirectional calls: replies stayed correct,
no call outlived its deadline, and every server handler exited (clean).

Run: python findings/grpclib_reset_leak.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import Callable
from typing import Any

import grpclib.server
from grpclib.client import Channel, StreamStreamMethod, UnaryStreamMethod, UnaryUnaryMethod
from grpclib.const import Cardinality, Handler
from grpclib.encoding.base import CodecBase
from grpclib.exceptions import GRPCError, StreamTerminatedError
from grpclib.protocol import H2Protocol
from grpclib.server import Server

import simloop
from simloop import faults

logging.getLogger("grpclib").setLevel(logging.CRITICAL)
ERRORS = (GRPCError, StreamTerminatedError, TimeoutError, ConnectionError, OSError)


class JsonCodec(CodecBase):
    __content_subtype__ = "json"

    def encode(self, message: Any, message_type: Any) -> bytes:
        return json.dumps(message).encode()

    def decode(self, data: bytes, message_type: Any) -> Any:
        return json.loads(data)


class TrackingServer(Server):
    """Keeps every connection's protocol, to look at its registered streams."""

    def __init__(self, handlers: list[Any]) -> None:
        super().__init__(handlers, codec=JsonCodec())
        self.protocols: list[H2Protocol] = []

    def _protocol_factory(self) -> H2Protocol:
        protocol = super()._protocol_factory()
        self.protocols.append(protocol)
        return protocol

    def held_streams(self) -> list[int]:
        return sorted(s for p in self.protocols for s in p.processor.streams)


class Service:
    def __init__(self) -> None:
        self.active = 0

    async def unary(self, stream: Any) -> None:
        request = await stream.recv_message()
        self.active += 1
        try:
            await asyncio.sleep(request["delay"])
            await stream.send_message({"id": request["id"]})
        finally:
            self.active -= 1

    async def many(self, stream: Any) -> None:
        request = await stream.recv_message()
        self.active += 1
        try:
            for k in range(request["n"]):
                await asyncio.sleep(request["delay"])
                await stream.send_message({"id": request["id"], "k": k})
        finally:
            self.active -= 1

    async def echo(self, stream: Any) -> None:
        self.active += 1
        try:
            async for message in stream:
                await stream.send_message(message)
        finally:
            self.active -= 1

    def __mapping__(self) -> dict[str, Handler]:
        return {
            "/t.S/Unary": Handler(self.unary, Cardinality.UNARY_UNARY, None, None),
            "/t.S/Many": Handler(self.many, Cardinality.UNARY_STREAM, None, None),
            "/t.S/Echo": Handler(self.echo, Cardinality.STREAM_STREAM, None, None),
        }


def start_server(net: simloop.Network, service: Service) -> dict[str, TrackingServer]:
    box: dict[str, TrackingServer] = {}

    async def serve() -> None:
        server = TrackingServer([service])
        await server.start("0.0.0.0", 50051)
        box["server"] = server
        await server.wait_closed()

    net.add_host("srv", entry=serve)
    return box


async def leak_harness() -> None:
    """No HTTP/2 stream outlives its call on the server."""
    net = simloop.network()
    service = Service()
    box = start_server(net, service)
    net.set_link("main", "srv", latency=(0.0005, 0.005))
    await asyncio.sleep(0.01)
    rng = simloop.rng()
    channel = Channel("srv", 50051, codec=JsonCodec())
    unary = UnaryUnaryMethod(channel, "/t.S/Unary", None, None)

    async def client(n: int) -> None:
        for i in range(15):
            rid = f"{n}-{i}"
            pad = "x" * rng.choice([0, 100, 70_000])
            request = {"id": rid, "delay": rng.uniform(0, 0.05), "pad": pad}
            try:
                if rng.random() < 0.3:
                    reply = await asyncio.wait_for(unary(request), rng.uniform(0, 0.05))
                else:
                    reply = await unary(
                        request, timeout=rng.choice([None, rng.uniform(0.001, 0.1)])
                    )
                assert reply == {"id": rid}, f"asked for {rid}, got {reply}"
            except ERRORS as exc:
                simloop.log(f"{rid}: {type(exc).__name__}")

    await asyncio.gather(*(client(n) for n in range(4)))
    await asyncio.sleep(1)
    assert await unary({"id": "last", "delay": 0}, timeout=5) == {"id": "last"}
    held = box["server"].held_streams()
    assert not held, f"after every call finished the server still holds streams {held}"
    channel.close()


def fix_reset_before_start() -> Callable[[], None]:
    """Release the stream of a handler task cancelled before its first step."""
    accept = grpclib.server.Handler.accept

    def patched(self: Any, stream: Any, headers: Any, release_stream: Callable[[], Any]) -> None:
        released: list[bool] = []

        def release_once() -> None:
            if not released:
                released.append(True)
                release_stream()

        accept(self, stream, headers, release_once)
        self._tasks[stream].add_done_callback(lambda t: t.cancelled() and release_once())

    grpclib.server.Handler.accept = patched  # type: ignore[method-assign]

    def undo() -> None:
        grpclib.server.Handler.accept = accept  # type: ignore[method-assign]

    return undo


async def faults_harness() -> None:
    """Replies match, no call outlives its deadline, every handler exits."""
    net = simloop.network()
    service = Service()
    start_server(net, service)
    net.set_link("main", "srv", latency=(0.0005, 0.005), reset=0.002)
    await asyncio.sleep(0.01)
    rng = simloop.rng()
    loop = asyncio.get_running_loop()
    nemesis = asyncio.create_task(
        faults.random_crashes(["srv"], every=(0.3, 1.5), downtime=(0.05, 0.2))
    )
    channel = Channel("srv", 50051, codec=JsonCodec())
    unary = UnaryUnaryMethod(channel, "/t.S/Unary", None, None)
    many = UnaryStreamMethod(channel, "/t.S/Many", None, None)
    echo = StreamStreamMethod(channel, "/t.S/Echo", None, None)

    async def call(rid: str, kind: str, timeout: float) -> None:
        if kind == "unary":
            reply = await unary({"id": rid, "delay": rng.uniform(0, 0.05)}, timeout=timeout)
            assert reply == {"id": rid}, f"{rid}: got {reply}"
        elif kind == "many":
            n = rng.randint(1, 5)
            request = {"id": rid, "delay": rng.uniform(0, 0.02), "n": n}
            replies = await many(request, timeout=timeout)
            assert replies == [{"id": rid, "k": k} for k in range(n)], f"{rid}: got {replies}"
        else:
            async with echo.open(timeout=timeout) as stream:
                for j in range(rng.randint(1, 4)):
                    await stream.send_message({"id": rid, "j": j})
                    reply = await stream.recv_message()
                    assert reply == {"id": rid, "j": j}, f"{rid}: got {reply}"
                await stream.end()
                assert await stream.recv_message() is None

    async def client(n: int) -> None:
        for i in range(12):
            rid, kind = f"{n}-{i}", rng.choice(["unary", "many", "echo"])
            timeout = rng.uniform(0.01, 0.3)
            started = loop.time()
            try:
                await call(rid, kind, timeout)
            except ERRORS as exc:
                simloop.log(f"{rid} {kind}: {type(exc).__name__}")
            elapsed = loop.time() - started
            assert elapsed <= timeout + 0.05, f"{rid} took {elapsed:.3f} s, timeout {timeout:.3f} s"
            await asyncio.sleep(rng.uniform(0, 0.02))

    await asyncio.gather(*(client(n) for n in range(4)))
    nemesis.cancel()
    net.heal()
    net.set_link("main", "srv", latency=0.001, reset=0.0)
    if not net.host("srv").up:
        net.host("srv").restart()
    await asyncio.sleep(1)
    for attempt in range(5):
        try:
            assert await unary({"id": "last", "delay": 0}, timeout=1) == {"id": "last"}
            break
        except ERRORS as exc:
            simloop.log(f"recovery attempt {attempt}: {type(exc).__name__}")
            await asyncio.sleep(0.5)
    else:
        raise AssertionError("the channel never recovered after faults stopped")
    await asyncio.sleep(0.5)
    assert service.active == 0, f"{service.active} handlers still running"
    channel.close()


class GaveUp(Exception):
    pass


async def plain_window() -> tuple[int, int, int]:
    """Send a 60 KB request and give up at once, 64 times, on a real loop."""
    server = TrackingServer([Service()])
    await server.start("127.0.0.1", 0)
    assert server._server is not None
    port = server._server.sockets[0].getsockname()[1]
    channel = Channel("127.0.0.1", port, codec=JsonCodec())
    unary = UnaryUnaryMethod(channel, "/t.S/Unary", None, None)
    await unary({"id": "first", "delay": 0}, timeout=2)
    protocol = server.protocols[0]
    before = protocol.connection._connection.inbound_flow_control_window
    for _ in range(64):
        try:
            async with unary.open(timeout=5) as stream:
                await stream.send_message({"id": "x", "delay": 0, "pad": "x" * 60_000}, end=True)
                raise GaveUp  # the caller stops waiting right after sending
        except GaveUp:
            pass
        await asyncio.sleep(0.01)
    after = protocol.connection._connection.inbound_flow_control_window
    held = len(server.held_streams())
    channel.close()
    server.close()
    await server.wait_closed()
    return before, after, held


def main() -> int:
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(leak_harness, seeds=range(100), config=config)
        print(f"{scheduler}: {report.summary()}")
        assert report.groups
        assert all("still holds streams" in g.message for g in report.groups)
    undo = fix_reset_before_start()
    try:
        for scheduler in ("random", "fifo"):
            config = simloop.SimConfig(scheduler=scheduler)
            if scheduler == "fifo":
                config = config.replace(timer_jitter=0.001)
            fixed = simloop.explore(leak_harness, seeds=range(100), config=config)
            print(f"{scheduler}, with the stream released: {fixed.summary()}")
            assert fixed.ok, fixed.summary()
            clean = simloop.explore(faults_harness, seeds=range(100), config=config)
            print(f"{scheduler}, crashes, resets and streaming calls: {clean.summary()}")
            assert clean.ok, clean.summary()
    finally:
        undo()
    before, after, held = asyncio.run(plain_window())
    print(
        f"real asyncio loop: {held} of 64 abandoned calls left their stream registered;"
        f" the connection's receive window went from {before} to {after} bytes"
    )
    assert held > 0
    assert after < before // 10
    return 0


if __name__ == "__main__":
    sys.exit(main())
