"""python-engineio 4.15.1: a reset while reading a poll response leaves a zombie client.

A python-socketio ``AsyncServer`` (ASGI, under uvicorn on the simulated
network) and three ``socketio.AsyncClient``s with reconnection enabled.
Each client makes ten ``call("echo", ...)`` round trips with 0-30 ms server
delays. Checked: every reply is the caller's own token, and once faults
stop, every client that says it is connected answers a call within 5 s
and the server sees exactly the clients' session ids.

The websocket transport passes with 0.3% TCP resets per segment, and the
polling transport passes without faults. Polling with resets fails: when
the connection breaks after a long-poll ``GET`` has returned its headers,
``AsyncClient._read_loop_polling`` gets ``aiohttp.ClientPayloadError``
from ``await r.read()``. It only catches ``ValueError`` there (the request
itself is wrapped in ``_send_request``, the body read is not), so the read
loop task dies with the exception. Nothing fires the disconnect handler
or resets the client: ``client.connected`` stays ``True`` and
``eio.state`` stays ``"connected"`` while the server has dropped the
session, every ``call`` times out, and reconnection never starts.
Catching ``aiohttp.ClientError`` beside ``ValueError`` fixes it.

Run: python findings/engineio_polling_zombie.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import warnings
from collections.abc import Callable, Coroutine
from typing import Any

import aiohttp
import engineio.async_client
import socketio
import uvicorn
from engineio import payload

import simloop

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore", category=DeprecationWarning)
ERRORS = (socketio.exceptions.SocketIOError,)
WORKLOADS: dict[str, dict[str, Any]] = {
    "websocket, TCP resets": {"transport": "websocket", "reset": 0.003},
    "polling, no faults": {"transport": "polling"},
    "polling, TCP resets": {"transport": "polling", "reset": 0.003},
}
FAILING = ("polling, TCP resets",)


def echo_server() -> tuple[socketio.AsyncServer, set[str]]:
    sio = socketio.AsyncServer(async_mode="asgi", ping_interval=1, ping_timeout=1)
    connected: set[str] = set()

    @sio.event  # type: ignore[misc]
    async def connect(sid: str, environ: Any, auth: Any = None) -> None:
        connected.add(sid)

    @sio.event  # type: ignore[misc]
    async def disconnect(sid: str, reason: Any = None) -> None:
        connected.discard(sid)

    @sio.event  # type: ignore[misc]
    async def echo(sid: str, data: dict[str, Any]) -> str:
        await asyncio.sleep(data["delay"])
        return str(data["token"])

    return sio, connected


def uvicorn_server(sio: socketio.AsyncServer, port: int) -> uvicorn.Server:
    config = uvicorn.Config(
        socketio.ASGIApp(sio),
        host="0.0.0.0",
        port=port,
        log_level="critical",
        lifespan="off",
        loop="none",
        http="h11",
        ws="websockets-sansio",
    )
    return uvicorn.Server(config)


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Replies match; after faults stop, a client that says it is connected works."""
        rng = simloop.rng()
        sio, connected = echo_server()
        web = uvicorn_server(sio, 8000)
        net = simloop.network()
        net.add_host("ws", entry=web.serve)
        net.set_link("main", "ws", latency=(0.0005, 0.003))
        await asyncio.sleep(0.5)
        clients = [
            socketio.AsyncClient(
                reconnection_delay=0.1, reconnection_delay_max=0.5, randomization_factor=0
            )
            for _ in range(3)
        ]
        for client in clients:
            await client.connect("http://ws:8000", transports=[knobs["transport"]], wait_timeout=5)
        errors: list[str] = []

        async def worker(n: int, client: socketio.AsyncClient) -> None:
            for i in range(10):
                token = f"c{n}-{i}"
                data = {"token": token, "delay": round(rng.uniform(0, 0.03), 3)}
                try:
                    reply = await client.call("echo", data, timeout=2)
                    if reply != token:
                        errors.append(f"{token} got {reply!r}")
                except ERRORS as exc:
                    simloop.log(f"{token}: {type(exc).__name__}")
                await asyncio.sleep(rng.uniform(0, 0.05))

        net.set_link("main", "ws", latency=(0.0005, 0.003), reset=knobs.get("reset", 0.0))
        workers = [asyncio.create_task(worker(n, c)) for n, c in enumerate(clients)]
        done, pending = await asyncio.wait(workers, timeout=60)
        assert not pending, f"{len(pending)} worker(s) stuck after 60 s"
        for task in done:
            task.result()
        assert not errors, errors[:3]
        net.set_link("main", "ws", latency=0.001, reset=0.0)
        await asyncio.sleep(5)
        for n, client in enumerate(clients):
            if not client.connected:
                continue
            try:
                reply = await client.call("echo", {"token": f"last{n}", "delay": 0}, timeout=5)
            except socketio.exceptions.TimeoutError:
                reader = client.eio.read_loop_task
                died = reader.exception() if reader is not None and reader.done() else None
                raise AssertionError(
                    f"client {n} says it is connected (engine.io state {client.eio.state!r}) "
                    f"but gets no reply 10 s after faults stopped; its read loop ended with "
                    f"{died!r}; the server has {len(connected)} session(s)"
                ) from None
            assert reply == f"last{n}", reply
        sids = {c.get_sid() for c in clients if c.connected}
        assert connected == sids, f"server sees {sorted(connected)}, clients {sorted(sids)}"
        for client in clients:
            await client.disconnect()
        web.should_exit = True
        await asyncio.sleep(1)

    return harness


def fix_read_loop() -> Callable[[], None]:
    """``_read_loop_polling`` with a failed body read handled like a malformed one."""
    original = engineio.async_client.AsyncClient._read_loop_polling

    async def read_loop(self: Any) -> None:
        while self.state == "connected" and self.write_loop_task:
            url = self.base_url + self._get_url_timestamp()
            timeout = max(self.ping_interval, self.ping_timeout) + 5
            r = await self._send_request("GET", url, timeout=timeout)
            if r is None or isinstance(r, str) or r.status < 200 or r.status >= 300:
                await self.queue.put(None)
                break
            try:
                p = payload.Payload(encoded_payload=(await r.read()).decode("utf-8"))
            except (ValueError, aiohttp.ClientError):
                await self.queue.put(None)
                break
            for pkt in p.packets:
                await self._receive_packet(pkt)
        if self.write_loop_task:
            await self.write_loop_task
        if self.state == "connected":
            await self._trigger_event("disconnect", self.reason.TRANSPORT_ERROR, run_async=False)
            with contextlib.suppress(ValueError):
                engineio.base_client.connected_clients.remove(self)
            await self._reset()

    engineio.async_client.AsyncClient._read_loop_polling = read_loop  # type: ignore[method-assign]

    def undo() -> None:
        engineio.async_client.AsyncClient._read_loop_polling = original  # type: ignore[method-assign]

    return undo


async def relay(target_port: int, cut: dict[str, bool]) -> asyncio.Server:
    """Forwards TCP; once ``cut`` is armed, ends the next poll response right after its headers."""

    async def pump(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter, polls: list[bool], up: bool
    ) -> None:
        with contextlib.suppress(ConnectionError):
            while data := await reader.read(65536):
                if up:
                    polls.append(data.startswith(b"GET "))
                elif cut["armed"] and polls and polls[-1] and b"\r\n\r\n" in data:
                    head = data[: data.index(b"\r\n\r\n") + 4]
                    if len(data) > len(head):
                        cut["armed"] = False
                        writer.write(head + data[len(head) : len(head) + 1])
                        await writer.drain()
                        writer.transport.abort()
                        return
                writer.write(data)
                await writer.drain()
        writer.close()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", target_port)
        polls: list[bool] = []
        await asyncio.gather(
            pump(reader, up_writer, polls, True), pump(up_reader, writer, polls, False)
        )

    return await asyncio.start_server(handle, "127.0.0.1", 0)


async def plain() -> str:
    """On a real loop: one poll response is cut off after its headers."""
    sio, connected = echo_server()
    web = uvicorn_server(sio, 0)
    serving = asyncio.create_task(web.serve())
    while not web.started:
        await asyncio.sleep(0.01)
    port = web.servers[0].sockets[0].getsockname()[1]
    cut = {"armed": False}
    proxy = await relay(port, cut)
    proxy_port = proxy.sockets[0].getsockname()[1]
    client = socketio.AsyncClient(reconnection_delay=0.1, randomization_factor=0)
    await client.connect(f"http://127.0.0.1:{proxy_port}", transports=["polling"])
    assert await client.call("echo", {"token": "before", "delay": 0}, timeout=2) == "before"
    cut["armed"] = True
    await asyncio.sleep(5)
    try:
        reply = await client.call("echo", {"token": "after", "delay": 0}, timeout=2)
        answered = f"answered {reply!r}"
    except socketio.exceptions.TimeoutError:
        answered = "timed out"
    reader = client.eio.read_loop_task
    died = reader.exception() if reader is not None and reader.done() else None
    outcome = (
        f"5 s after the cut: client.connected={client.connected}, a call {answered}, "
        f"the read loop ended with {type(died).__name__ if died else None}, "
        f"server sessions: {len(connected)}"
    )
    with contextlib.suppress(Exception):
        await asyncio.wait_for(client.disconnect(), 2)
    proxy.close()
    web.should_exit = True
    await serving
    return outcome


def main() -> int:
    configs = [("random", simloop.SimConfig(strict_cancellation=True))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(50), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups, report.summary()
            for group in report.groups:
                assert "says it is connected" in group.message, group.message
    undo = fix_read_loop()
    try:
        for workload in FAILING:
            for name, config in configs:
                fixed = simloop.explore(
                    make_harness(WORKLOADS[workload]), seeds=range(50), config=config
                )
                print(f"{workload}, {name}, read loop fixed: {fixed.summary().splitlines()[0]}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    outcome = asyncio.run(plain())
    print(f"real asyncio loop, polling: {outcome}")
    assert "client.connected=True, a call timed out" in outcome, outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
