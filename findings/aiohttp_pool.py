"""aiohttp: replies match requests, and the client pool recovers after faults.

Client timeouts and cancellations, injected TCP resets and server crashes
against an aiohttp server. Clean on aiohttp 3.14.5 with asyncio's own callback
order. With the random scheduler it produces a server-side TypeError that
needs an impossible ordering; see docs/FINDINGS.md, "False positives".

Run: python findings/aiohttp_pool.py
"""

from __future__ import annotations

import asyncio
import sys

import aiohttp
from aiohttp import web

import simloop
from simloop import faults

ERRORS = (aiohttp.ClientError, TimeoutError, OSError)


async def server() -> None:
    rng = simloop.rng()

    async def echo(request: web.Request) -> web.Response:
        await asyncio.sleep(rng.uniform(0, 0.05))
        return web.Response(text=request.query["n"])

    app = web.Application()
    app.router.add_get("/", echo)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, port=8080).start()
    await asyncio.Event().wait()


async def workload() -> None:
    net = simloop.network()
    net.add_host("api", entry=server)
    net.set_link("main", "api", latency=(0.001, 0.01), reset=0.005)
    await asyncio.sleep(0.05)
    rng = simloop.rng()
    nemesis = asyncio.create_task(
        faults.random_crashes(["api"], every=(0.3, 1.5), downtime=(0.05, 0.3))
    )
    connector = aiohttp.TCPConnector(limit=3)
    async with aiohttp.ClientSession(connector=connector) as session:

        async def get(token: str, timeout: float | None) -> str:
            async with session.get(
                "http://api:8080/",
                params={"n": token},
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                return await resp.text()

        async def worker(w: int) -> None:
            for i in range(8):
                token = f"w{w}-{i}"
                timeout = rng.uniform(0.0, 0.08) if rng.random() < 0.4 else None
                try:
                    if rng.random() < 0.3:
                        body = await asyncio.wait_for(get(token, None), rng.uniform(0, 0.08))
                    else:
                        body = await get(token, timeout)
                except ERRORS as exc:
                    simloop.log(f"{token}: {type(exc).__name__}")
                    continue
                assert body == token, f"asked for {token!r}, got {body!r}"

        async with asyncio.TaskGroup() as tg:
            for w in range(5):
                tg.create_task(worker(w))
        nemesis.cancel()
        api = net.host("api")
        if not api.up:
            api.restart()
        net.set_link("main", "api", latency=0.001, reset=0.0)
        await asyncio.sleep(1)
        # The pool must still serve `limit` concurrent requests.
        results = await asyncio.wait_for(
            asyncio.gather(*(get(f"final{i}", None) for i in range(3))), 30
        )
        assert results == ["final0", "final1", "final2"]
        assert not connector._acquired, f"{len(connector._acquired)} connections still acquired"


def main() -> int:
    config = simloop.SimConfig(scheduler="fifo", timer_jitter=0.002)
    report = simloop.explore(workload, seeds=range(150), config=config)
    print(report.summary())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
