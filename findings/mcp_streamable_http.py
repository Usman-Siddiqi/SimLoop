"""MCP Python SDK 2.3.0: streamable HTTP loses cancellations in JSON mode and dies on one reset.

An ``MCPServer`` with one tool (``echo``: sleep, then return the token) and
``mcp.Client``, first in-process (memory streams), then over streamable
HTTP: the server's Starlette app under uvicorn on the simulated network,
the client's own HTTP stack (httpx2) connecting to it. Four callers make
six calls each; 30% of them, given a 5 s handler, give up early through
``asyncio.wait_for`` or ``read_timeout_seconds``. Checked: every call that
completes returns its own token, no caller hangs, the server cancels the
handler of every call its caller gave up on, and the client serves a call
afterwards. In-process and over HTTP with SSE responses all of this held,
random and fifo, with ``strict_cancellation=True``.

Two workloads fail, both known upstream:

- ``json_response=True``: abandoned handlers keep running. On the
  2026-07-28 protocol the client cancels a request by closing its POST
  (it swallows ``notifications/cancelled``), and the server's SSE path
  watches for ``http.disconnect``, but its JSON path awaits the handler
  with no watcher. Open issue modelcontextprotocol/python-sdk#3762.
- 0.5% TCP resets: a transport error while a POST is in flight
  (``httpx2.ReadError``) is not handled per request. It escapes into the
  transport's task group, which cancels every other request: calls in
  flight fail ("Connection closed", or ``CancelledError`` for the task
  that owns the client), so does every later call on that client, and
  leaving ``async with Client(...)`` raises an ``ExceptionGroup``. Open
  issues #3522 and #3560.

Run: python findings/mcp_streamable_http.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import warnings
from collections.abc import Callable, Coroutine
from typing import Any

import anyio
import uvicorn
from mcp import Client
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

import simloop

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore", module="mcp")
WORKLOADS: dict[str, dict[str, Any]] = {
    "in-process, wait_for": {"cancel": 0.3},
    "in-process, read timeouts": {"read_timeout": 0.3},
    "HTTP, wait_for": {"http": True, "cancel": 0.3},
    "HTTP, read timeouts": {"http": True, "read_timeout": 0.3},
    "HTTP, JSON responses, wait_for": {"http": True, "json": True, "cancel": 0.3},
    "HTTP, 0.5% TCP resets": {"http": True, "reset": 0.005},
}
FAILING = {
    "HTTP, JSON responses, wait_for": "handlers still running",
    "HTTP, 0.5% TCP resets": "ExceptionGroup",
}


def echo_server() -> tuple[MCPServer, dict[str, int]]:
    server = MCPServer("echo")
    running: dict[str, int] = {}

    @server.tool()
    async def echo(token: str, delay: float) -> str:
        running[token] = running.get(token, 0) + 1
        try:
            await anyio.sleep(delay)
            return token
        finally:
            running[token] -= 1
            if not running[token]:
                del running[token]

    return server, running


def serve(server: MCPServer, port: int, **app_options: Any) -> uvicorn.Server:
    security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
    app = server.streamable_http_app(transport_security=security, **app_options)
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=port,
        log_level="critical",
        lifespan="on",
        loop="none",
        http="h11",
    )
    return uvicorn.Server(config)


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Replies match, nobody hangs, abandoned handlers are cancelled, the client recovers."""
        rng = simloop.rng()
        server, running = echo_server()
        net = simloop.network()
        target: Any = server
        uvicorns: list[uvicorn.Server] = []
        if knobs.get("http"):
            web = serve(server, 8000, json_response=bool(knobs.get("json")))
            uvicorns.append(web)
            net.add_host("mcp", entry=web.serve)
            net.set_link("main", "mcp", latency=(0.0005, 0.003))
            await asyncio.sleep(0.5)
            target = "http://mcp:8000/mcp"
        errors: list[str] = []
        async with Client(target) as client:

            async def caller(n: int) -> None:
                for i in range(6):
                    token, delay = f"c{n}-{i}", round(rng.uniform(0, 0.05), 3)
                    arguments = {"token": token, "delay": delay}
                    roll, limit = rng.random(), rng.uniform(0, 0.05)
                    try:
                        if roll < knobs.get("cancel", 0):
                            arguments["delay"] = 5.0
                            call = client.call_tool("echo", arguments)
                            result = await asyncio.wait_for(call, limit)
                        elif roll < knobs.get("read_timeout", 0):
                            arguments["delay"] = 5.0
                            result = await client.call_tool("echo", arguments, limit)
                        else:
                            result = await client.call_tool("echo", arguments)
                        text = getattr(result.content[0], "text", None) if result.content else None
                        if result.is_error or text != token:
                            errors.append(f"{token} got {text!r} (is_error={result.is_error})")
                    except TimeoutError:
                        pass
                    except Exception as exc:
                        if "timed out" not in str(exc).lower():
                            errors.append(f"{token}: {type(exc).__name__}: {exc}")
                    await asyncio.sleep(rng.uniform(0, 0.01))

            if knobs.get("http"):
                net.set_link("main", "mcp", latency=(0.0005, 0.003), reset=knobs.get("reset", 0.0))
            callers = [asyncio.create_task(caller(n)) for n in range(4)]
            done, pending = await asyncio.wait(callers, timeout=30)
            for task in done:
                task.result()
            if knobs.get("http"):
                net.set_link("main", "mcp", latency=0.001, reset=0.0)
            await asyncio.sleep(0.3)
            abandoned = dict(running)
            try:
                last = await asyncio.wait_for(
                    client.call_tool("echo", {"token": "last", "delay": 0}), 5
                )
                recovered = getattr(last.content[0], "text", None) == "last"
            except Exception as exc:
                recovered = False
                simloop.log(f"call after the faults stopped: {exc!r}")
        # Checked outside the client, whose task groups would wrap the failure.
        assert not pending, f"{len(pending)} caller(s) stuck after 30 s"
        if not knobs.get("reset"):
            assert not errors, f"{len(errors)} call(s) failed: {errors[:3]}"
        assert not abandoned, f"handlers still running after their callers gave up: {abandoned}"
        assert recovered, "no call succeeds after the faults stopped"
        for web in uvicorns:
            web.should_exit = True
        await asyncio.sleep(1)

    return harness


async def proxy(listen_port: int, target_port: int, cut: dict[str, bool]) -> asyncio.Server:
    """Forwards TCP to the server; once ``cut`` is armed, resets the next tool call's connection."""

    async def pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, client: Any) -> None:
        with contextlib.suppress(ConnectionError):
            while data := await reader.read(65536):
                if client is not None and cut["armed"] and b"tools/call" in data:
                    cut["armed"] = False
                    client.transport.abort()
                    writer.transport.abort()
                    return
                writer.write(data)
                await writer.drain()
        writer.close()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", target_port)
        await asyncio.gather(pump(reader, up_writer, writer), pump(up_reader, writer, None))

    return await asyncio.start_server(handle, "127.0.0.1", listen_port)


async def plain() -> list[str]:
    """On a real loop: one tool call's connection is reset, then the same client is used again."""
    server, _ = echo_server()
    web = serve(server, 0)
    serving = asyncio.create_task(web.serve())
    while not web.started:
        await asyncio.sleep(0.01)
    port = web.servers[0].sockets[0].getsockname()[1]
    cut = {"armed": False}
    relay = await proxy(0, port, cut)
    relay_port = relay.sockets[0].getsockname()[1]
    seen: list[str] = []
    try:
        async with Client(f"http://127.0.0.1:{relay_port}/mcp") as client:
            first = await client.call_tool("echo", {"token": "before", "delay": 0})
            seen.append(f"first call: {getattr(first.content[0], 'text', None)!r}")
            cut["armed"] = True
            for label in ("the reset call", "the next call"):
                try:
                    call = client.call_tool("echo", {"token": "after", "delay": 0})
                    result = await asyncio.wait_for(call, 2)
                    seen.append(f"{label}: {getattr(result.content[0], 'text', None)!r}")
                except asyncio.CancelledError:  # #3560: what a failed POST can look like
                    seen.append(f"{label}: CancelledError")
                except Exception as exc:
                    seen.append(f"{label}: {type(exc).__name__}: {exc}")
        seen.append("leaving the client: returned")
    except BaseException as exc:
        seen.append(f"leaving the client: {type(exc).__name__}")
    relay.close()
    web.should_exit = True
    await serving
    return seen


async def plain_json() -> str:
    """On a real loop, ``json_response=True``: a caller gives up on a 1 s call after 0.1 s."""
    server, running = echo_server()
    web = serve(server, 0, json_response=True)
    serving = asyncio.create_task(web.serve())
    while not web.started:
        await asyncio.sleep(0.01)
    port = web.servers[0].sockets[0].getsockname()[1]
    async with Client(f"http://127.0.0.1:{port}/mcp") as client:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(client.call_tool("echo", {"token": "t", "delay": 1.0}), 0.1)
        await asyncio.sleep(0.4)
        still = bool(running)
    web.should_exit = True
    await serving
    return f"0.4 s after the caller gave up, the handler is still running: {still}"


def main() -> int:
    configs = [("random", simloop.SimConfig(strict_cancellation=True))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(50), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
            else:
                assert report.groups, report.summary()
                for group in report.groups:
                    assert FAILING[workload] in group.message, group.message
    seen = asyncio.run(plain())
    print("real asyncio loop, one call's connection reset: " + "; ".join(seen))
    assert seen[0] == "first call: 'before'", seen
    assert not any(text.endswith("'after'") for text in seen[1:3]), seen
    assert seen[3] == "leaving the client: ExceptionGroup", seen
    outcome = asyncio.run(plain_json())
    print(f"real asyncio loop, JSON responses: {outcome}")
    assert outcome.endswith("True"), outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
