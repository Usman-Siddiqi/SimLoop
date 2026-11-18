from __future__ import annotations

import asyncio
import time

import pytest

import simloop
from simloop import faults

from .helpers import run_ok
from .test_network import ask, echo_server


def test_buggify_is_off_outside_simulations() -> None:
    assert faults.buggify(1.0) is False


def test_buggify_is_seeded_per_site() -> None:
    async def main() -> tuple[int, int]:
        a = sum(faults.buggify(0.5) for _ in range(100))
        b = sum(faults.buggify(0.5) for _ in range(100))
        return a, b

    outcomes = [simloop.run(main, seed=s).value for s in range(40)]
    assert outcomes == [simloop.run(main, seed=s).value for s in range(40)]
    sites = [site for pair in outcomes if pair for site in pair]
    assert 0 in sites
    assert all(n == 0 or 25 < n < 75 for n in sites)
    assert any(n > 0 for n in sites)
    disabled = simloop.run(main, seed=1, config=simloop.SimConfig(buggify=False))
    assert disabled.value == (0, 0)


def test_buggify_fires_are_logged() -> None:
    async def main() -> None:
        for _ in range(50):
            faults.buggify(1.0)

    traces = [simloop.run(main, seed=s).trace for s in range(20)]
    assert any(any("buggify fired" in e.detail for e in t) for t in traces)


def test_paused_host_freezes_timers_and_traffic() -> None:
    async def main() -> list[str]:
        net = simloop.network()
        log: list[str] = []

        async def ticker() -> None:
            for _ in range(4):
                await asyncio.sleep(1)
                log.append(f"tick {simloop.now():.1f}")

        worker = net.add_host("worker")
        worker.spawn(ticker())
        server = net.add_host("server", entry=echo_server())
        net.set_link("main", "server", latency=0.01)
        await asyncio.sleep(1.5)
        worker.pause(3)
        server.pause(2)
        start = simloop.now()
        reader, writer = await asyncio.open_connection("server", 7000)
        await ask(reader, writer, "x")
        log.append(f"echo after {simloop.now() - start:.1f}s")
        writer.close()
        await asyncio.sleep(10)
        return log

    assert run_ok(main, scheduler="fifo").value == [
        "tick 1.0",
        "echo after 2.0s",
        "tick 4.5",
        "tick 5.5",
        "tick 6.5",
    ]


def test_clock_skew_is_per_host() -> None:
    async def main() -> list[float]:
        net = simloop.network()
        fast = net.add_host("fast")
        fast.clock_offset = 30.0
        fast.clock_drift = 0.01

        async def read_clock() -> float:
            return time.time()

        await asyncio.sleep(100)
        return [time.time(), await fast.run(read_clock())]

    here, there = run_ok(main).value or [0, 0]
    assert there - here == pytest.approx(31.0)


@pytest.mark.parametrize(
    "nemesis",
    [
        lambda: faults.random_partitions(every=(0.5, 2), duration=(0.5, 2)),
        lambda: faults.random_crashes(every=(0.5, 2), downtime=(0.1, 1), power_loss=0.5),
        lambda: faults.random_pauses(every=(0.5, 2)),
        lambda: faults.clock_jumps(every=(0.5, 2)),
        lambda: faults.flaky_links(every=(0.5, 2), duration=(0.5, 2)),
    ],
)
def test_nemeses_run_deterministically(nemesis) -> None:
    async def main() -> int:
        net = simloop.network()
        for name in ("a", "b", "c"):
            net.add_host(name, entry=echo_server())
        chaos = asyncio.create_task(nemesis())
        await asyncio.sleep(30)
        chaos.cancel()
        return len(simloop.current().trace)

    first = run_ok(main, seed=4)
    again = run_ok(main, seed=4)
    assert first.digest == again.digest
    kinds = {e.kind for e in first.trace}
    assert kinds & {"partition", "crash", "pause", "log"}


def test_crash_recovery_bug_is_found_by_nemesis() -> None:
    """A counter service that only keeps state in memory loses acknowledged writes."""

    async def counter_server() -> None:
        host = simloop.current_host()
        value = host.disk.get("value", 0) if host.disk.get("durable") else 0

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal value
            while await reader.readline():
                value += 1
                writer.write(f"{value}\n".encode())
                await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, port=1)
        async with server:
            await server.serve_forever()

    async def workload() -> None:
        net = simloop.network()
        net.add_host("counter", entry=counter_server)
        chaos = faults.random_crashes(["counter"], every=(1, 3), downtime=(0.1, 0.5))
        nemesis = asyncio.create_task(chaos)
        acknowledged = 0
        for _ in range(40):
            await asyncio.sleep(0.2)
            try:
                reader, writer = await asyncio.open_connection("counter", 1)
                reply = await ask(reader, writer, "incr")
                writer.close()
            except (OSError, asyncio.IncompleteReadError):
                continue
            if reply:
                acknowledged += 1
                assert int(reply) >= acknowledged, (
                    f"acknowledged {acknowledged}, server says {int(reply)}"
                )

    report = simloop.explore(workload, seeds=range(20))
    assert not report.ok
    assert report.groups[0].kind == "exception"
    assert "acknowledged" in report.groups[0].message
