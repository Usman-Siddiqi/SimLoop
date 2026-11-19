"""The core promise: the same seed produces exactly the same execution."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

import simloop

from .workloads import busy_system

ROOT = Path(__file__).resolve().parent.parent
CONFIG = simloop.SimConfig(timer_jitter=0.001)

# Captured at import inside a container, where the sandbox does not look, so
# it stays the real clock. (A plain module global would be swapped.)
# Kept in a dict, which the sandbox does not reach: real entropy during a run.
_REAL = {"urandom": os.urandom}


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
        # A reference taken before the sandbox: real entropy steers the run.
        await asyncio.sleep(int.from_bytes(_REAL["urandom"](4)) / 2**32)
        raise AssertionError("always fails")

    report = simloop.explore(leaky, seeds=range(3))
    assert len(report.groups) == 1
    assert report.groups[0].reproducible is False
    assert report.groups[0].divergence is not None
    assert "leaking non-determinism" in report.summary()
    assert "diverged at trace event" in report.summary()


def test_explore_confirms_reproducible_failures() -> None:
    async def flaky() -> None:
        if simloop.rng().random() < 0.5:
            raise AssertionError("bad luck")

    report = simloop.explore(flaky, seeds=range(20))
    assert report.groups
    assert all(group.reproducible for group in report.groups)


async def _taskgroup_abort() -> None:
    async def child(n: int) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            simloop.log(f"child {n} cancelled")
            raise

    async def failing() -> None:
        await asyncio.sleep(1)
        raise ValueError("boom")

    async def group() -> None:
        async with asyncio.TaskGroup() as tg:
            for n in range(20):
                tg.create_task(child(n))
            tg.create_task(failing())

    with pytest.raises(ExceptionGroup):
        await group()
    done, pending = await asyncio.wait(
        [asyncio.ensure_future(asyncio.sleep(n % 3)) for n in range(10)], timeout=1.5
    )
    simloop.log(f"wait: {len(done)} done, {len(pending)} pending")
    for future in pending:
        future.cancel()
        await asyncio.sleep(0)
        simloop.log("cancelled one")


def test_task_sets_iterate_in_a_reproducible_order() -> None:
    # TaskGroup cancels its children by iterating a set and asyncio.wait
    # returns sets; with identity hashes the order would follow memory layout.
    assert simloop.check_determinism(_taskgroup_abort, seed=3, runs=5) is None
    first = simloop.run(_taskgroup_abort, seed=3)
    cancelled = [e.detail for e in first.trace if e.detail.startswith("child")]
    assert len(cancelled) == 20


_leaked_state = {"runs": 0}


async def _leaky() -> None:
    await asyncio.sleep(1)
    _leaked_state["runs"] += 1
    simloop.log(f"run number {_leaked_state['runs']}")
    await asyncio.sleep(1)


def test_check_determinism_reports_the_first_divergent_event() -> None:
    divergence = simloop.check_determinism(_leaky, seed=0)
    assert divergence is not None
    assert divergence.expected is not None
    assert divergence.actual is not None
    assert divergence.expected.detail == "run number 1"
    assert divergence.actual.detail == "run number 2"
    text = divergence.format()
    assert "diverged at trace event" in text
    assert "run 1:" in text
    assert "run 2:" in text


def test_cli_check_command(capsys: pytest.CaptureFixture[str]) -> None:
    from simloop import cli

    target = f"{Path(__file__).resolve().parent / 'workloads.py'}:busy_system"
    assert cli.main(["check", target, "--seeds", "2"]) == 0
    assert "deterministic: 2 seed(s)" in capsys.readouterr().out
