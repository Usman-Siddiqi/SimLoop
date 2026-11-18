"""A counter service that forgets acknowledged writes when it crashes.

The server keeps its count in memory. A client increments it over TCP while a
nemesis crashes and restarts the server host; after a restart the count
starts again from zero, so the client sees a value lower than the number of
increments the server already acknowledged.

    simloop explore examples/counter.py:main --seeds 200 --shrink
    simloop explore examples/counter.py:main_durable --seeds 200
"""

from __future__ import annotations

import asyncio

import simloop
from simloop import faults


async def counter_server(*, durable: bool) -> None:
    host = simloop.current_host()
    value = host.disk.get("value", 0) if durable else 0

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal value
        while await reader.readline():
            value += 1
            if durable:
                host.disk["value"] = value  # written before the reply is sent
            writer.write(f"{value}\n".encode())
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, port=7000)
    async with server:
        await server.serve_forever()


async def increment() -> int | None:
    try:
        reader, writer = await asyncio.open_connection("counter", 7000)
    except OSError:
        return None
    try:
        writer.write(b"incr\n")
        await writer.drain()
        line = await reader.readline()
        return int(line) if line else None
    except OSError:
        return None
    finally:
        writer.close()


async def _workload(*, durable: bool) -> None:
    net = simloop.network()
    net.add_host("counter", entry=lambda: counter_server(durable=durable))
    nemesis = asyncio.create_task(
        faults.random_crashes(["counter"], every=(1, 3), downtime=(0.1, 0.5))
    )
    acknowledged = 0
    for _ in range(40):
        await asyncio.sleep(0.2)
        reply = await increment()
        if reply is None:
            simloop.log("increment failed")
            continue
        acknowledged += 1
        simloop.log(f"server says {reply}")
        assert reply >= acknowledged, f"{acknowledged} increments acknowledged, server says {reply}"
    nemesis.cancel()


async def main() -> None:
    await _workload(durable=False)


async def main_durable() -> None:
    await _workload(durable=True)
