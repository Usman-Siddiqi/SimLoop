"""uvicorn + Starlette served over the simulated network, httpx as the client.

Clean on uvicorn 0.54.0 / Starlette 1.10.0 / httpx 0.28.1 (h11 protocol);
see docs/FINDINGS.md, "Tested, nothing found". All three run unmodified.

Properties:

- integrity: every 200 response carries the id and body of its own request,
  across keep-alive reuse, client timeouts, server crashes and restarts,
  injected TCP resets and partitions (a fault schedule), and a fresh request
  succeeds once the faults stop;
- ``limit_concurrency``: running handlers never exceed the limit;
- graceful shutdown: every request whose handler started before
  ``should_exit`` gets its response.

Run: python findings/uvicorn_starlette.py
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

import simloop
from simloop import faults

state: dict[str, Any] = {}


async def echo(request: Request) -> PlainTextResponse:
    rid = request.query_params["id"]
    state["running"] += 1
    state["started"].add(rid)
    try:
        limit = state["limit"]
        assert limit is None or state["running"] <= limit, (
            f"{state['running']} handlers running with limit_concurrency={limit}"
        )
        body = await request.body()
        await asyncio.sleep(state["rng"].uniform(0, state["work"]))
        return PlainTextResponse(f"{rid}:{body.decode()}")
    finally:
        state["running"] -= 1


app = Starlette(routes=[Route("/echo", echo, methods=["POST"])])


def _start_server(rng: Any, servers: list[uvicorn.Server], **options: Any) -> None:
    async def serve() -> None:
        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="critical",
            lifespan="off",
            loop="none",
            http="h11",
            **options,
        )
        server = uvicorn.Server(config)
        servers.append(server)
        await server.serve()

    simloop.network().add_host("web", entry=serve)


async def integrity() -> None:
    rng = simloop.rng()
    limit = rng.choice([None, 2, 4])
    state.clear()
    state.update(running=0, limit=limit, rng=rng, started=set(), work=0.05)
    servers: list[uvicorn.Server] = []
    _start_server(rng, servers, limit_concurrency=limit, timeout_keep_alive=rng.choice([0.05, 5]))
    await asyncio.sleep(0.5)
    if rng.random() < 0.7:
        faults.schedule(
            faults.every((0.2, 1.0), faults.crash("web"), duration=(0.05, 0.3), until=2.5),
            faults.every(
                (0.1, 0.5),
                faults.degrade_link("main", "web", reset=0.02),
                duration=(0.05, 0.2),
                until=2.5,
            ),
            faults.every((0.3, 1.0), faults.isolate("web"), duration=(0.05, 0.3), until=2.5),
        ).start()

    async def client_task(c: int, client: httpx.AsyncClient) -> None:
        for i in range(6):
            await asyncio.sleep(rng.choice([0, rng.uniform(0, 0.3)]))
            rid = f"{c}-{i}"
            body = "x" * rng.choice([1, 10, 3000])
            timeout = rng.uniform(0.01, 0.1) if rng.random() < 0.3 else 2.0
            try:
                post = client.post(f"/echo?id={rid}", content=body.encode())
                response = await asyncio.wait_for(post, timeout)
            except (TimeoutError, httpx.TransportError):
                continue
            if response.status_code == 503 and limit is not None:
                continue
            assert response.status_code == 200, (response.status_code, response.text)
            assert response.text == f"{rid}:{body}", f"{rid} got {response.text[:20]!r}"

    limits = httpx.Limits(max_connections=rng.choice([1, 3, 10]), max_keepalive_connections=5)
    async with (
        httpx.AsyncClient(base_url="http://web:8000", limits=limits) as client,
        asyncio.TaskGroup() as tg,
    ):
        for c in range(4):
            tg.create_task(client_task(c, client))
    await asyncio.sleep(3.0)
    async with httpx.AsyncClient(base_url="http://web:8000") as fresh:
        response = await asyncio.wait_for(fresh.post("/echo?id=final", content=b"z"), 5)
        assert response.text == "final:z", response.text
    servers[-1].should_exit = True
    await asyncio.sleep(1.0)
    assert state["running"] == 0, f"{state['running']} handlers still running after shutdown"


async def shutdown() -> None:
    rng = simloop.rng()
    state.clear()
    state.update(running=0, limit=None, rng=rng, started=set(), work=0.2)
    servers: list[uvicorn.Server] = []
    _start_server(rng, servers, timeout_keep_alive=rng.choice([0.05, 5]))
    await asyncio.sleep(0.5)
    outcome: dict[str, object] = {}

    async def client_task(c: int, client: httpx.AsyncClient) -> None:
        for i in range(5):
            await asyncio.sleep(rng.choice([0, rng.uniform(0, 0.1)]))
            rid = f"{c}-{i}"
            try:
                response = await client.post(f"/echo?id={rid}", content=b"b")
                outcome[rid] = response.status_code
            except httpx.TransportError as exc:
                outcome[rid] = type(exc).__name__

    async with (
        httpx.AsyncClient(base_url="http://web:8000", timeout=10) as client,
        asyncio.TaskGroup() as tg,
    ):
        for c in range(4):
            tg.create_task(client_task(c, client))
        await asyncio.sleep(rng.uniform(0, 0.4))
        simloop.log("shutdown requested")
        servers[-1].should_exit = True
    lost = sorted((rid, outcome.get(rid)) for rid in state["started"] if outcome.get(rid) != 200)
    assert not lost, f"handlers started but responses lost in graceful shutdown: {lost}"


def main() -> int:
    ok = True
    for workload in (integrity, shutdown):
        for config in (
            simloop.SimConfig(scheduler="random"),
            simloop.SimConfig(scheduler="fifo", timer_jitter=0.002),
        ):
            report = simloop.explore(workload, seeds=range(60), config=config)
            print(f"{workload.__name__} {config.scheduler}: {report.summary()}")
            ok = ok and report.ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
