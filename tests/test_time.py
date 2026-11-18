from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

import simloop

from .helpers import run_ok
from .workloads import busy_system


def test_sleep_advances_simulated_time_only() -> None:
    async def main() -> float:
        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.sleep(3600)
        return loop.time() - start

    wall_start = time.perf_counter()
    result = run_ok(main)
    assert result.value == 3600
    assert result.sim_time == 3600
    assert time.perf_counter() - wall_start < 1.0


def test_a_simulated_day_of_periodic_work_is_fast() -> None:
    async def main() -> int:
        ticks = 0
        while simloop.now() < 86_400:
            await asyncio.sleep(1)
            ticks += 1
        return ticks

    wall_start = time.perf_counter()
    result = run_ok(main)
    assert result.value == 86_400
    assert time.perf_counter() - wall_start < 10.0


def test_timers_fire_in_deadline_order_and_ties_in_scheduling_order() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        fired: list[str] = []
        done = loop.create_future()
        for name, delay in [("c", 3), ("a", 1), ("b1", 2), ("b2", 2), ("b3", 2)]:
            loop.call_later(delay, fired.append, name)
        loop.call_later(4, done.set_result, None)
        await done
        return fired

    assert run_ok(main, scheduler="fifo").value == ["a", "b1", "b2", "b3", "c"]


def test_call_at_and_cancelled_timers() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        fired: list[str] = []
        keep = loop.call_at(loop.time() + 5, fired.append, "kept")
        drop = loop.call_at(loop.time() + 1, fired.append, "dropped")
        drop.cancel()
        await asyncio.sleep(10)
        assert keep.when() == 5
        return fired

    assert run_ok(main).value == ["kept"]


def test_clock_jumps_straight_to_next_timer() -> None:
    async def main() -> list[float]:
        seen = []
        for delay in (0.5, 10, 0.001):
            await asyncio.sleep(delay)
            seen.append(simloop.now())
        return seen

    assert run_ok(main).value == [0.5, 10.5, 10.501]


def test_start_time_offsets_the_clock() -> None:
    async def main() -> float:
        await asyncio.sleep(1)
        return asyncio.get_running_loop().time()

    result = run_ok(main, start_time=100.0)
    assert result.value == 101.0
    assert result.sim_time == 1.0


def test_timeouts_take_no_wall_time() -> None:
    async def main() -> str:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.Event().wait(), timeout=30)
        async with asyncio.timeout(60):
            await asyncio.sleep(59)
        try:
            async with asyncio.timeout(1):
                await asyncio.sleep(2)
        except TimeoutError:
            return f"timed out at {simloop.now()}"
        return "no timeout"

    assert run_ok(main).value == "timed out at 90.0"


@pytest.mark.parametrize("seed", range(5))
def test_jitter_only_delays_timers(seed: int) -> None:
    async def main() -> list[float]:
        lateness = []
        for _ in range(50):
            target = simloop.now() + 1
            await asyncio.sleep(1)
            lateness.append(simloop.now() - target)
        return lateness

    lateness = run_ok(main, seed=seed, timer_jitter=0.05).value
    assert lateness is not None
    assert all(-1e-9 <= late <= 0.05 + 1e-9 for late in lateness)
    assert len(set(lateness)) > 1


def test_jitter_can_reorder_close_timers() -> None:
    async def main() -> str:
        loop = asyncio.get_running_loop()
        order: list[str] = []
        loop.call_later(1.000, order.append, "first")
        loop.call_later(1.001, order.append, "second")
        await asyncio.sleep(2)
        return ",".join(order)

    outcomes = {
        simloop.run(main, seed=s, config=simloop.SimConfig(timer_jitter=0.01)).value
        for s in range(40)
    }
    assert outcomes == {"first,second", "second,first"}


async def _spin_until_the_clock_moves() -> int:
    # The shape of a float-rounding wait: re-arm a timer for "now" until the
    # clock has moved at all.
    loop = asyncio.get_running_loop()
    start = loop.time()
    done = loop.create_future()
    spins = 0

    def check() -> None:
        nonlocal spins
        spins += 1
        if loop.time() > start:
            done.set_result(None)
        else:
            loop.call_at(start, check)

    loop.call_at(start, check)
    await done
    return spins


def test_a_zero_time_spin_is_explained() -> None:
    result = simloop.run(_spin_until_the_clock_moves, config=simloop.SimConfig(max_steps=5000))
    assert result.failure is not None
    assert result.failure.kind == "step_limit"
    assert "steps all ran at t=0.0" in result.failure.message
    assert "step_time=1e-6" in result.failure.message


def test_step_time_lets_the_clock_move_between_callbacks() -> None:
    result = run_ok(_spin_until_the_clock_moves, step_time=1e-6)
    assert result.value == 1

    async def sleeper() -> None:
        await asyncio.sleep(10)

    assert run_ok(sleeper, step_time=0.001).sim_time > 10


def test_step_time_is_deterministic() -> None:
    config = simloop.SimConfig(step_time=1e-6)
    digests = {simloop.run(busy_system, seed=4, config=config).digest for _ in range(3)}
    assert len(digests) == 1
