"""The core promise: the same seed produces exactly the same execution."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import simloop

from .workloads import busy_system

ROOT = Path(__file__).resolve().parent.parent
CONFIG = simloop.SimConfig(timer_jitter=0.001)

# Captured at import, before any sandbox exists, so it stays the real clock.
_real_perf_counter = time.perf_counter


@pytest.mark.parametrize("seed", [0, 1, 42, 2**40])
def test_same_seed_same_trace_and_result(seed: int) -> None:
    first = simloop.run(busy_system, seed=seed, config=CONFIG)
    second = simloop.run(busy_system, seed=seed, config=CONFIG)
    assert first.ok, first.failure
    assert first.digest == second.digest
    assert [e.line() for e in first.trace] == [e.line() for e in second.trace]
    assert first.value == second.value
    assert first.choices == second.choices
    assert first.steps == second.steps
    assert first.sim_time == second.sim_time


def test_digest_does_not_depend_on_keeping_events() -> None:
    kept = simloop.run(busy_system, seed=3, config=CONFIG)
    lean = simloop.run(busy_system, seed=3, config=CONFIG.replace(keep_trace=False))
    assert kept.digest == lean.digest
    assert len(lean.trace.events) == 0
    assert len(lean.trace) == len(kept.trace)


def test_different_seeds_explore_different_executions() -> None:
    digests = {simloop.run(busy_system, seed=s, config=CONFIG).digest for s in range(20)}
    assert len(digests) == 20


def test_interleaved_runs_do_not_affect_each_other() -> None:
    baseline = [simloop.run(busy_system, seed=s, config=CONFIG).digest for s in (5, 6)]
    simloop.run(busy_system, seed=99, config=CONFIG)
    again = [simloop.run(busy_system, seed=s, config=CONFIG).digest for s in (6, 5)]
    assert again == baseline[::-1]


@pytest.mark.parametrize("scheduler", ["fifo", "random"])
def test_replaying_recorded_choices_reproduces_the_run(scheduler: str) -> None:
    async def main() -> list[float]:
        rng = simloop.rng()
        out: list[float] = []

        async def worker() -> None:
            for _ in range(5):
                await asyncio.sleep(rng.random())
                out.append(simloop.now())

        await asyncio.gather(*(worker() for _ in range(4)))
        return out

    config = simloop.SimConfig(scheduler=scheduler, timer_jitter=0.01)
    original = simloop.run(main, seed=17, config=config)
    replayed = simloop.run(main, seed=12345, config=config, replay=original.choices)
    assert replayed.digest == original.digest
    assert replayed.value == original.value


def test_identical_across_processes_and_hash_seeds() -> None:
    script = ROOT / "tests" / "workloads.py"
    outputs = set()
    for hash_seed in ("0", "1", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": hash_seed, "PYTHONPATH": str(ROOT / "src")}
        completed = subprocess.run(
            [sys.executable, str(script), "7"],
            capture_output=True,
            text=True,
            env=env,
            check=True,
            cwd=ROOT,
        )
        outputs.add(completed.stdout)
    in_process = simloop.run(busy_system, seed=7, config=CONFIG)
    assert len(outputs) == 1
    assert outputs.pop().split()[0] == in_process.digest


def test_explore_flags_code_that_leaks_real_time() -> None:
    async def leaky() -> None:
        # A reference taken before the sandbox: wall-clock noise steers the run.
        await asyncio.sleep((_real_perf_counter() * 1e6) % 1)
        raise AssertionError("always fails")

    report = simloop.explore(leaky, seeds=range(3))
    assert len(report.groups) == 1
    assert report.groups[0].reproducible is False
    assert "leaking non-determinism" in report.summary()


def test_explore_confirms_reproducible_failures() -> None:
    async def flaky() -> None:
        if simloop.rng().random() < 0.5:
            raise AssertionError("bad luck")

    report = simloop.explore(flaky, seeds=range(20))
    assert report.groups
    assert all(group.reproducible for group in report.groups)
