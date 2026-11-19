"""httpcore 1.0.9 and Hypercorn 0.18.0 over HTTP/2: leaked connections and stuck handlers.

An httpx 0.28.1 client (``http1=False, http2=True``, so httpcore speaks
HTTP/2 with prior knowledge) and a Hypercorn server with a plain ASGI echo
app on the simulated network. Six tasks POST up to 100 KB each, six times,
over the client's one connection pool, and the app streams the body back
in chunks of up to 40 KB. Checked: every response carries its own request's
body; once the client is closed it has no TCP connection left open; and
once the faults stop, no app handler is still running. That held with no
faults, random and fifo, with ``strict_cancellation=True``.

httpcore: with 40% of requests under ``asyncio.wait_for``, the client keeps
TCP connections open after ``aclose()``. Concurrent requests share one new
HTTP/2 connection while it is still connecting; the first one connects and
the rest wait on its ``_request_lock``. ``AsyncHTTPConnection
.handle_async_request`` sets ``_connect_failed = True`` on any exception
in that block, so a request cancelled while it merely waits marks the
connection as failed. ``is_closed()`` then returns True, and the pool
drops the connection without closing it, while the first request
finishes connecting and keeps using it. Nothing references the socket any
more: ``aclose()`` does not close it, and the server keeps the connection,
and any handler still reading from it, forever. Marking only a failed
connect as failed (and not connecting again on a connection marked
failed) passes.

Hypercorn: with TCP resets (0.2% per segment), a handler that is sending a
response when the connection is lost never returns from ``send()``. It
waits for its stream's buffer to drain, and ``H2Protocol.handle(Closed())``
closes the streams but not their buffers. Closing the buffers there and on
``RST_STREAM`` passes. Known upstream: pgjones/hypercorn#390 (open).

Run: python findings/http2_httpx_hypercorn.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import h2.events
import httpx
from httpcore import ConnectionNotAvailable
from httpcore._async.connection import AsyncHTTPConnection
from httpcore._async.http2 import AsyncHTTP2Connection
from httpcore._async.http11 import AsyncHTTP11Connection
from hypercorn.asyncio import serve
from hypercorn.config import Config
from hypercorn.events import Closed
from hypercorn.protocol.h2 import H2Protocol

import simloop

logging.getLogger("hypercorn").setLevel(logging.CRITICAL)
WORKLOADS: dict[str, dict[str, Any]] = {
    "no faults": {},
    "wait_for, client connections": {"cancel": 0.4},
    "TCP resets, server handlers": {"reset": 0.002},
    "TCP resets, wait_for, both fixed": {"reset": 0.002, "cancel": 0.3},
}
FAILING = {
    "wait_for, client connections": "client connections still open",
    "TCP resets, server handlers": "handlers still running",
}
FIXES = {
    "wait_for, client connections": ("httpcore",),
    "TCP resets, server handlers": ("hypercorn",),
    "TCP resets, wait_for, both fixed": ("httpcore", "hypercorn"),
}


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Responses intact, no client connection left, no handler left running."""
        rng = simloop.rng()
        net = simloop.network()
        running: dict[str, int] = {}

        async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
            if scope["type"] != "http":
                return
            path = scope["path"]
            running[path] = running.get(path, 0) + 1
            try:
                body = b""
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    body += message.get("body", b"")
                    if not message.get("more_body"):
                        break
                await asyncio.sleep(rng.uniform(0, 0.02))
                await send({"type": "http.response.start", "status": 200, "headers": []})
                for at in range(0, len(body), step := rng.randrange(1, 40_000)):
                    chunk = body[at : at + step]
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
                await send({"type": "http.response.body", "body": b""})
            finally:
                running[path] -= 1
                if not running[path]:
                    del running[path]

        async def server() -> None:
            config = Config()
            config.bind = ["0.0.0.0:8000"]
            config.accesslog = None
            config.errorlog = None
            await serve(app, config)  # type: ignore[arg-type]

        net.add_host("web", entry=server)
        net.set_link("main", "web", latency=(0.0005, 0.005))
        await asyncio.sleep(0.2)
        wrong: list[str] = []
        async with httpx.AsyncClient(
            base_url="http://web:8000", http1=False, http2=True, timeout=10
        ) as client:
            net.set_link("main", "web", latency=(0.0005, 0.005), reset=knobs.get("reset", 0.0))

            async def post(tag: str) -> None:
                pattern = tag.encode() + b":" + bytes(rng.randrange(256) for _ in range(32))
                payload = (pattern * 4000)[: rng.randrange(1, 100_000)]
                try:
                    if rng.random() < knobs.get("cancel", 0):
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(
                                client.post(f"/{tag}", content=payload), rng.uniform(0, 0.05)
                            )
                        return
                    response = await client.post(f"/{tag}", content=payload)
                except httpx.HTTPError as exc:
                    simloop.log(f"{tag}: {type(exc).__name__}")
                    return
                if response.content != payload:
                    wrong.append(f"{tag}: sent {len(payload)} bytes, got other bytes back")

            async def worker(n: int) -> None:
                for i in range(6):
                    await post(f"c{n}-{i}")
                    await asyncio.sleep(rng.uniform(0, 0.01))

            tasks = [asyncio.create_task(worker(n)) for n in range(6)]
            done, pending = await asyncio.wait(tasks, timeout=120)
            assert not pending, f"{len(pending)} worker(s) stuck"
            for task in done:
                task.result()
            assert not wrong, wrong[:3]
            net.set_link("main", "web", latency=0.001, reset=0.0)
            await asyncio.sleep(2)
        await asyncio.sleep(1)
        left = net.host("main").connections
        assert not left, f"client connections still open after aclose(): {len(left)}"
        assert not running, f"handlers still running: {sorted(running)}"

    return harness


def fix_httpcore() -> Callable[[], None]:
    """Only a failed connect marks the connection failed; never connect twice."""
    original = AsyncHTTPConnection.handle_async_request

    async def handle_async_request(self: Any, request: Any) -> Any:
        async with self._request_lock:
            if self._connection is None:
                if self._connect_failed:
                    raise ConnectionNotAvailable()
                try:
                    stream = await self._connect(request)
                except BaseException:
                    self._connect_failed = True
                    raise
                # No TLS in the harness, so no ALPN: HTTP/2 only with prior knowledge.
                h2_only = self._http2 and not self._http1
                kind = AsyncHTTP2Connection if h2_only else AsyncHTTP11Connection
                self._connection = kind(
                    origin=self._origin, stream=stream, keepalive_expiry=self._keepalive_expiry
                )
        return await self._connection.handle_async_request(request)

    AsyncHTTPConnection.handle_async_request = handle_async_request  # type: ignore[method-assign]

    def undo() -> None:
        AsyncHTTPConnection.handle_async_request = original  # type: ignore[method-assign]

    return undo


def fix_hypercorn() -> Callable[[], None]:
    """Close a stream's send buffer when the stream or the connection goes away."""
    original_handle = H2Protocol.handle
    original_events = H2Protocol._handle_events

    async def handle(self: Any, event: Any) -> None:
        await original_handle(self, event)
        if isinstance(event, Closed):
            for buffer in list(self.stream_buffers.values()):
                await buffer.close()

    async def handle_events(self: Any, events: list[Any]) -> None:
        for event in events:
            if isinstance(event, h2.events.StreamReset) and event.stream_id in self.stream_buffers:
                await self.stream_buffers[event.stream_id].close()
        await original_events(self, events)

    H2Protocol.handle = handle  # type: ignore[method-assign]
    H2Protocol._handle_events = handle_events  # type: ignore[method-assign]

    def undo() -> None:
        H2Protocol.handle = original_handle  # type: ignore[method-assign]
        H2Protocol._handle_events = original_events  # type: ignore[method-assign]

    return undo


async def plain_httpcore(*, cancel: bool) -> str:
    """On a real loop: two requests share a connecting HTTP/2 connection; one is cancelled."""

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

    config = Config()
    config.bind = ["127.0.0.1:18443"]
    config.accesslog = None
    config.errorlog = None
    stop = asyncio.Event()
    server = asyncio.create_task(serve(app, config, shutdown_trigger=stop.wait))  # type: ignore[arg-type]
    await asyncio.sleep(0.3)
    client = httpx.AsyncClient(http1=False, http2=True)
    first = asyncio.create_task(client.get("http://127.0.0.1:18443/a"))
    second = asyncio.create_task(client.get("http://127.0.0.1:18443/b"))
    for _ in range(3):
        await asyncio.sleep(0)
    if cancel:
        second.cancel()
    response = await first
    await asyncio.gather(second, return_exceptions=True)
    pooled = len(client._transport._pool.connections)  # type: ignore[attr-defined]
    sock = response.extensions["network_stream"].get_extra_info("socket")
    await client.aclose()
    outcome = (
        f"first request {response.status_code} {response.http_version}, "
        f"connections in the pool: {pooled}, "
        f"its socket after aclose(): {'open' if sock.fileno() != -1 else 'closed'}"
    )
    stop.set()
    await asyncio.wait_for(server, 5)
    return outcome


def main() -> int:
    configs = [("random", simloop.SimConfig(step_time=1e-6, strict_cancellation=True))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    fixes = {"httpcore": fix_httpcore, "hypercorn": fix_hypercorn}
    for workload, knobs in WORKLOADS.items():
        if workload not in FAILING and workload in FIXES:
            continue
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(30), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups, report.summary()
            for group in report.groups:
                assert FAILING[workload] in group.message, group.message
    for workload, names in FIXES.items():
        undos = [fixes[fix]() for fix in names]
        try:
            for name, config in configs:
                fixed = simloop.explore(
                    make_harness(WORKLOADS[workload]), seeds=range(30), config=config
                )
                label = " and ".join(names)
                print(f"{workload}, {name}, {label} fixed: {fixed.summary().splitlines()[0]}")
                assert fixed.ok, fixed.summary()
        finally:
            for undo in undos:
                undo()
    cancelled = asyncio.run(plain_httpcore(cancel=True))
    print(f"real asyncio loop, second request cancelled: {cancelled}")
    assert cancelled.endswith("pool: 0, its socket after aclose(): open"), cancelled
    control = asyncio.run(plain_httpcore(cancel=False))
    print(f"real asyncio loop, nothing cancelled: {control}")
    assert control.endswith("pool: 1, its socket after aclose(): closed"), control
    return 0


if __name__ == "__main__":
    sys.exit(main())
