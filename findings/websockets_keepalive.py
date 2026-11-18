"""websockets: ordered delivery, and keepalive notices a dead peer in time.

An echo server behind random partitions of 0.2-4 s. Every echo must come
back in order, and no recv() may wait longer than ping interval + ping
timeout + close timeout on a dead link. Clean on websockets 17.4.

Run: python findings/websockets_keepalive.py
"""

from __future__ import annotations

import asyncio
import sys

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

import simloop
from simloop import faults

PING_INTERVAL = 1.0
PING_TIMEOUT = 1.0
CLOSE_TIMEOUT = 1.0


async def server() -> None:
    async def echo(ws) -> None:
        try:
            async for message in ws:
                await ws.send(message)
        except ConnectionClosed:
            pass

    async with serve(
        echo,
        port=8765,
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_TIMEOUT,
        close_timeout=CLOSE_TIMEOUT,
    ):
        await asyncio.Event().wait()


async def workload() -> None:
    net = simloop.network()
    net.add_host("ws", entry=server)
    await asyncio.sleep(0.05)
    rng = simloop.rng()
    chaos = asyncio.create_task(
        faults.random_partitions(["ws"], include_driver=True, every=(0.5, 3), duration=(0.2, 4))
    )
    for attempt in range(4):
        try:
            async with connect(
                "ws://ws:8765",
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=CLOSE_TIMEOUT,
                open_timeout=3,
            ) as ws:
                expected = 0
                for sent in range(1, 21):  # `sent` messages are out after this send
                    await ws.send(str(sent - 1))
                    if rng.random() < 0.5:
                        while expected < sent:
                            started = simloop.now()
                            try:
                                reply = await ws.recv()
                            finally:
                                waited = simloop.now() - started
                                # Keepalive should notice a silent peer: one ping
                                # interval, the pong timeout, then the close timeout.
                                bound = PING_INTERVAL + PING_TIMEOUT + CLOSE_TIMEOUT + 0.5
                                assert waited <= bound, f"recv waited {waited:.2f}s on a dead link"
                            assert reply == str(expected), f"expected {expected}, got {reply!r}"
                            expected += 1
                    await asyncio.sleep(rng.uniform(0, 0.3))
        except (ConnectionClosed, OSError, TimeoutError) as exc:
            simloop.log(f"attempt {attempt}: {type(exc).__name__}")
    chaos.cancel()
    net.heal()
    async with connect("ws://ws:8765", open_timeout=5) as ws:
        await ws.send("final")
        assert await asyncio.wait_for(ws.recv(), 5) == "final"


def main() -> int:
    config = simloop.SimConfig(scheduler="fifo", timer_jitter=0.002, max_time=600)
    report = simloop.explore(workload, seeds=range(300), config=config)
    print(report.summary())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
