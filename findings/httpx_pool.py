"""httpx/httpcore: replies match requests, and the pool recovers after faults.

Client cancellations, injected TCP resets and server crashes against an
aiohttp server, with a 3-connection pool. Clean on httpx 0.28.1 / httpcore
1.0.9 with asyncio's own callback order (see docs/FINDINGS.md).

Run: python findings/httpx_pool.py
"""

from __future__ import annotations

import asyncio
import sys

import httpx
from aiohttp import web

import simloop
from simloop import faults

ERRORS = (httpx.HTTPError, TimeoutError, OSError)


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
    crashes = faults.random_crashes(["api"], every=(0.3, 1.5), downtime=(0.05, 0.3))
    nemesis = asyncio.create_task(crashes)
    limits = httpx.Limits(max_connections=3, max_keepalive_connections=3)
    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(5.0, pool=5.0)) as client:

        async def get(token: str) -> str:
            resp = await client.get("http://api:8080/", params={"n": token})
            return resp.text

        async def worker(w: int) -> None:
            for i in range(8):
                token = f"w{w}-{i}"
                try:
                    if rng.random() < 0.4:
                        body = await asyncio.wait_for(get(token), rng.uniform(0, 0.08))
                    else:
                        body = await get(token)
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
        finals = asyncio.gather(*(get(f"final{i}") for i in range(3)))
        assert await asyncio.wait_for(finals, 30) == ["final0", "final1", "final2"]
        pool = client._transport._pool
        busy = [c for c in pool.connections if not c.is_idle() and not c.is_closed()]
        assert not busy, f"{len(busy)} pooled connections still busy: {busy}"


def main() -> int:
    config = simloop.SimConfig(scheduler="fifo", timer_jitter=0.002)
    report = simloop.explore(workload, seeds=range(100), config=config)
    print(report.summary())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
