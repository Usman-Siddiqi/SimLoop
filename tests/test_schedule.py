from __future__ import annotations

import asyncio
from typing import Any

import pytest

import simloop
from simloop import faults

from .helpers import run_ok
from .test_network import ask, echo_server


def _schedule_log(result: simloop.RunResult[Any]) -> list[tuple[float, str]]:
    return [
        (round(e.time, 6), e.detail.removeprefix("schedule: "))
        for e in result.trace
        if e.kind == "log" and e.detail.startswith("schedule: ")
    ]


def test_at_and_between_run_at_their_times() -> None:
    async def main() -> list[tuple[float, bool, bool]]:
        net = simloop.network()
        net.add_host("db", entry=echo_server())
        net.add_host("cache")
        faults.schedule(
            faults.at(1.0, faults.crash("db")),
            faults.at(2.5, faults.restart("db")),
            faults.between(3.0, 4.0, faults.pause("cache")),
        ).start()
        seen = []
        for t in (0.5, 1.5, 2.6, 3.5, 4.5):
            await asyncio.sleep(t - simloop.now())
            seen.append((t, net.host("db").up, net.host("cache").paused))
        return seen

    result = run_ok(main)
    assert result.value == [
        (0.5, True, False),
        (1.5, False, False),
        (2.6, True, False),
        (3.5, True, True),
        (4.5, True, False),
    ]
    assert _schedule_log(result) == [
        (1.0, "crash db"),
        (2.5, "restart db"),
        (3.0, "pause cache"),
    ]


def test_during_crash_restarts_the_host() -> None:
    async def main() -> tuple[int, bool]:
        net = simloop.network()
        db = net.add_host("db", entry=echo_server())
        await faults.schedule(faults.during(1.0, 0.5, faults.crash("db"))).run()
        return db.crashes, db.up

    result = run_ok(main)
    assert result.value == (1, True)
    assert [line for _, line in _schedule_log(result)] == ["crash db", "restart db"]


def test_partition_holds_traffic_until_its_window_ends() -> None:
    async def main() -> float:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        net.set_link("main", "server", latency=0.001)
        faults.schedule(faults.between(1.0, 3.0, faults.isolate("server"))).start()
        await asyncio.sleep(0.1)
        reader, writer = await asyncio.open_connection("server", 7000)
        await asyncio.sleep(1.0)
        start = simloop.now()
        assert await ask(reader, writer, "hello") == b"echo hello\n"
        writer.close()
        return simloop.now() - start

    waited = run_ok(main, scheduler="fifo").value
    assert waited is not None
    assert 1.8 < waited < 2.1


def test_overlapping_partitions_compose() -> None:
    async def main() -> list[bool]:
        net = simloop.network()
        a, b = net.add_host("a"), net.add_host("b")
        faults.schedule(
            faults.between(1.0, 3.0, faults.partition("a", "b")),
            faults.between(2.0, 4.0, faults.partition(["a"], ["b"])),
        ).start()
        seen = []
        for t in (1.5, 3.5, 4.5):
            await asyncio.sleep(t - simloop.now())
            seen.append(net.reachable(a, b))
        return seen

    assert run_ok(main).value == [False, False, True]


def test_one_way_partition() -> None:
    async def main() -> tuple[bool, bool]:
        net = simloop.network()
        a, b = net.add_host("a"), net.add_host("b")
        await faults.schedule(faults.at(0.0, faults.partition_one_way("a", "b"))).run()
        return net.reachable(a, b), net.reachable(b, a)

    assert run_ok(main).value == (False, True)


def test_every_repeats_a_bounded_number_of_times() -> None:
    async def main() -> int:
        net = simloop.network()
        db = net.add_host("db", entry=echo_server())
        await faults.schedule(
            faults.every((1.0, 2.0), faults.crash("db"), duration=(0.1, 0.2), times=3)
        ).run()
        assert db.up
        return db.crashes

    assert run_ok(main).value == 3


def test_every_stops_at_until() -> None:
    async def main() -> int:
        net = simloop.network()
        net.add_host("db")
        await faults.schedule(
            faults.every(1.0, faults.clock_jump("db", 0.5), duration=0.1, until=3.5)
        ).run()
        return round(net.host("db").clock_offset * 10)

    result = run_ok(main)
    assert result.value == 0
    assert [t for t, line in _schedule_log(result) if "back" not in line] == [1.0, 2.1, 3.2]


def test_ranges_shrink_to_their_low_end() -> None:
    async def main() -> None:
        net = simloop.network()
        net.add_host("db")
        await faults.schedule(
            faults.during((5.0, 9.0), (1.0, 3.0), faults.crash(faults.any_of("db", "main")))
        ).run()

    zero = simloop.run(main, replay=[])
    assert _schedule_log(zero)[0] == (5.0, "crash db")
    assert zero.sim_time == 6.0


def test_random_targets_and_times_are_seeded() -> None:
    async def main() -> None:
        net = simloop.network()
        for name in ("a", "b", "c"):
            net.add_host(name)
        await faults.schedule(
            faults.every(
                (0.5, 1.5), faults.pause(faults.any_of("a", "b", "c")), duration=0.2, times=6
            )
        ).run()

    logs = [_schedule_log(simloop.run(main, seed=s)) for s in range(10)]
    assert logs == [_schedule_log(simloop.run(main, seed=s)) for s in range(10)]
    assert len({tuple(log) for log in logs}) > 5
    targets = {line.split()[-1] for log in logs for _, line in log}
    assert targets == {"a", "b", "c"}


def test_degraded_link_is_restored() -> None:
    async def main() -> tuple[float, float]:
        net = simloop.network()
        net.add_host("db")
        net.set_link("main", "db", drop=0.0)
        faults.schedule(
            faults.between(1.0, 2.0, faults.degrade_link("main", "db", drop=0.5))
        ).start()
        await asyncio.sleep(1.5)
        during = net.link("main", "db").drop
        await asyncio.sleep(1.0)
        return during, net.link("db", "main").drop

    assert run_ok(main).value == (0.5, 0.0)


def test_custom_fault_with_undo() -> None:
    events: list[str] = []

    def break_cache() -> faults.Undo:
        events.append(f"break {simloop.now()}")
        return lambda: events.append(f"fix {simloop.now()}")

    async def main() -> None:
        await faults.schedule(faults.during(1.0, 2.0, faults.call(break_cache))).run()

    run_ok(main)
    assert events == ["break 1.0", "fix 3.0"]


def test_schedule_validates_its_input() -> None:
    with pytest.raises(TypeError, match="not a schedule step"):
        faults.schedule(faults.crash("db"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="before start"):
        faults.between(2.0, 1.0, faults.crash("db"))
    with pytest.raises(TypeError, match="unknown link settings"):
        faults.degrade_link("a", "b", lag=1)
    with pytest.raises(ValueError, match="at least one"):
        faults.any_of()


def test_schedule_is_deterministic() -> None:
    async def main() -> None:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        faults.schedule(
            faults.every(
                (0.2, 1.0), faults.isolate(faults.any_of("server", "main")), duration=(0.1, 0.5)
            ),
            faults.every((0.5, 2.0), faults.crash("server"), duration=(0.1, 0.3)),
        ).start()
        for i in range(10):
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection("server", 7000), 1)
                await asyncio.wait_for(ask(reader, writer, f"m{i}"), 1)
                writer.close()
            except (OSError, TimeoutError):
                pass
            await asyncio.sleep(0.3)

    digests = {simloop.run(main, seed=11).digest for _ in range(3)}
    assert len(digests) == 1
