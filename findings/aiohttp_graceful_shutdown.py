"""aiohttp 3.14.5: shutdown cancels handlers one ``shutdown_timeout`` late and does not wait.

An aiohttp server (``AppRunner(shutdown_timeout=0.5)``) serves requests
whose handlers take 0-3 s and, when cancelled, spend 50 ms cleaning up (a
rollback, a connection released to a pool). Clients send requests until
``runner.cleanup()`` starts at a random time. Checked, from the graceful
shutdown documentation ("4. Wait a short time for running handlers to
complete ... ``shutdown_timeout``. 5. Close any remaining connections and
cancel their handlers. It will wait on the canceling handlers for a short
time, again adjustable with ``shutdown_timeout``. 6. Call the
``Application.on_cleanup`` signal"):

* a handler still running ``shutdown_timeout`` after shutdown starts is
  cancelled then;
* ``on_cleanup`` runs only after cancelled handlers have finished.

Both fail. ``RequestHandler.shutdown`` waits ``shutdown_timeout`` for the
handler, then "cancels" it by setting an exception on the request's body
stream (``request._cancel``), which a handler that is not reading the body
never sees, and waits a second ``shutdown_timeout`` for it, shielded. Only
then does it call ``task.cancel()``, and it returns without waiting, so
``on_cleanup`` runs, and ``cleanup()`` returns, while the cancelled
handlers are still running their ``except``/``finally`` code.

Run: python findings/aiohttp_graceful_shutdown.py
"""

from __future__ import annotations

import asyncio
import logging
import sys

import aiohttp
from aiohttp import web

import simloop

TIMEOUT = 0.5  # shutdown_timeout
CLEANUP = 0.05  # how long a cancelled handler takes to clean up
logging.getLogger("aiohttp").setLevel(logging.CRITICAL)


async def harness() -> None:
    """Handlers are cancelled after shutdown_timeout, and finish before on_cleanup."""
    net = simloop.network()
    rng = simloop.rng()
    loop = asyncio.get_running_loop()
    running: set[str] = set()
    cancelled_at: dict[str, float] = {}
    at_cleanup: list[str] = []
    ready, stop, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()
    shutdown: dict[str, float] = {}

    async def handle(request: web.Request) -> web.Response:
        rid = request.match_info["rid"]
        running.add(rid)
        try:
            await asyncio.sleep(float(request.query["seconds"]))
            return web.Response(text=rid)
        except asyncio.CancelledError:
            cancelled_at[rid] = loop.time()
            await asyncio.sleep(CLEANUP)
            raise
        finally:
            running.discard(rid)

    async def on_cleanup(app: web.Application) -> None:
        at_cleanup.extend(sorted(running))

    async def serve() -> None:
        app = web.Application()
        app.router.add_get("/r/{rid}", handle)
        app.on_cleanup.append(on_cleanup)
        runner = web.AppRunner(app, shutdown_timeout=TIMEOUT)
        await runner.setup()
        await web.TCPSite(runner, port=8080).start()
        ready.set()
        await stop.wait()
        shutdown["start"] = loop.time()
        await runner.cleanup()
        stopped.set()
        await asyncio.Event().wait()

    net.add_host("web", entry=serve)
    net.set_link("main", "web", latency=(0.0005, 0.005))
    await ready.wait()
    async with aiohttp.ClientSession() as session:

        async def client(n: int) -> None:
            for i in range(10):
                rid, seconds = f"{n}-{i}", rng.choice([rng.uniform(0, 0.2), rng.uniform(0, 3)])
                url = f"http://web:8080/r/{rid}"
                try:
                    async with session.get(url, params={"seconds": f"{seconds:.3f}"}) as response:
                        assert await response.text() == rid
                except aiohttp.ClientError as exc:
                    simloop.log(f"{rid}: {type(exc).__name__}")
                await asyncio.sleep(rng.uniform(0, 0.05))

        async def stopper() -> None:
            await asyncio.sleep(rng.uniform(0.3, 1.5))
            stop.set()

        await asyncio.gather(stopper(), *(client(n) for n in range(4)))
    await stopped.wait()
    problems = []
    if cancelled_at:
        late = max(cancelled_at.values()) - shutdown["start"]
        if late > TIMEOUT + 0.05:
            problems.append(
                f"handlers were cancelled {late:.2f} s after shutdown began"
                f" (shutdown_timeout={TIMEOUT})"
            )
    if at_cleanup:
        problems.append(f"on_cleanup ran while {len(at_cleanup)} cancelled handler(s) still ran")
    assert not problems, "; ".join(problems)


async def plain() -> list[tuple[float, str]]:
    """On a real loop: one handler that outlives the grace period."""
    events: list[tuple[float, str]] = []
    loop = asyncio.get_running_loop()
    started = 0.0

    def mark(what: str) -> None:
        events.append((round(loop.time() - started, 1), what))

    async def handle(request: web.Request) -> web.Response:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            mark("handler cancelled")
            await asyncio.sleep(0.1)
            mark("handler finished cleaning up")
            raise
        return web.Response()

    async def on_cleanup(app: web.Application) -> None:
        mark("on_cleanup")

    app = web.Application()
    app.router.add_get("/", handle)
    app.on_cleanup.append(on_cleanup)
    runner = web.AppRunner(app, shutdown_timeout=TIMEOUT)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[attr-defined]
    async with aiohttp.ClientSession() as session:
        request = asyncio.create_task(session.get(f"http://127.0.0.1:{port}/"))
        await asyncio.sleep(0.2)
        started = loop.time()
        mark("cleanup() called")
        await runner.cleanup()
        mark("cleanup() returned")
        await asyncio.gather(request, return_exceptions=True)
        await asyncio.sleep(0.3)
    return events


def main() -> int:
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(harness, seeds=range(100), config=config)
        print(f"{scheduler}: {report.summary()}")
        assert report.groups
        for group in report.groups:
            assert "after shutdown began" in group.message
            assert "on_cleanup ran while" in group.message
    events = asyncio.run(plain())
    print(f"real asyncio loop, shutdown_timeout={TIMEOUT}:")
    for when, what in events:
        print(f"  {when:4.1f} s  {what}")
    order = [what for _, what in events]
    assert {w: t for t, w in events}["handler cancelled"] >= 2 * TIMEOUT
    assert order.index("on_cleanup") < order.index("handler finished cleaning up")
    assert order.index("cleanup() returned") < order.index("handler finished cleaning up")
    return 0


if __name__ == "__main__":
    sys.exit(main())
