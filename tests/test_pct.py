from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

import simloop
from simloop.scheduling import PCTScheduler, resolve_scheduler

from .workloads import busy_system

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

import inventory


async def _always_second() -> None:
    # Fails only if `b` beats `a` in every one of 12 rounds. Each round is a
    # separate coin flip for the random scheduler (1 in 4096), but PCT keeps a
    # task's priority across rounds, so half of its runs keep `b` ahead.
    order: list[str] = []

    async def worker(name: str) -> None:
        for _ in range(12):
            await asyncio.sleep(0)
            order.append(name)

    await asyncio.gather(worker("a"), worker("b"))
    assert any(order[i] == "a" for i in range(0, len(order), 2)), order


def test_pct_is_deterministic() -> None:
    config = simloop.SimConfig(scheduler="pct")
    digests = {simloop.run(busy_system, seed=3, config=config).digest for _ in range(3)}
    assert len(digests) == 1


def test_pct_with_all_zero_choices_is_fifo() -> None:
    fifo = simloop.run(busy_system, seed=5, config=simloop.SimConfig(scheduler="fifo"), replay=[])
    pct = simloop.run(busy_system, seed=5, config=simloop.SimConfig(scheduler="pct"), replay=[])
    assert pct.digest == fifo.digest


def test_pct_finds_a_consistent_order_that_random_misses() -> None:
    pct = simloop.explore(
        _always_second, seeds=range(50), config=simloop.SimConfig(scheduler="pct")
    )
    uniform = simloop.explore(_always_second, seeds=range(50))
    assert pct.failed >= 10
    assert uniform.failed == 0


def test_pct_finds_the_inventory_race() -> None:
    report = simloop.explore(
        inventory.main, seeds=range(200), config=simloop.SimConfig(scheduler="pct:2")
    )
    assert not report.ok
    assert report.groups[0].signature == "invariant:no stock is lost"


def test_scheduler_names() -> None:
    assert isinstance(resolve_scheduler("pct"), PCTScheduler)
    assert resolve_scheduler("pct:5").depth == 5  # type: ignore[attr-defined]
    template = PCTScheduler(depth=4)
    assert resolve_scheduler(template) is not template
    with pytest.raises(ValueError, match="unknown scheduler"):
        resolve_scheduler("pct:x")
    with pytest.raises(ValueError, match="depth"):
        PCTScheduler(depth=0)
