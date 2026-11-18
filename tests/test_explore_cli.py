from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

import simloop
from simloop import cli

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "examples"))

import inventory  # noqa: E402

INVENTORY = str(ROOT / "examples" / "inventory.py")


def test_explore_finds_and_groups_the_inventory_race() -> None:
    report = simloop.explore(inventory.main, seeds=range(300))
    assert report.runs == 300
    assert 0 < report.failed < 300
    assert [g.signature for g in report.groups] == ["invariant:no stock is lost"]
    group = report.groups[0]
    assert group.reproducible
    assert group.example is not None
    assert group.example.seed == group.seeds[0]
    assert report.failing_seeds() == sorted(group.seeds)
    assert "invariant:no stock is lost" in report.summary()


def test_failing_seed_reproduces_and_fix_passes() -> None:
    report = simloop.explore(inventory.main, seeds=range(100))
    seed = report.groups[0].seeds[0]
    for _ in range(3):
        result = simloop.run(inventory.main, seed=seed)
        assert result.failure is not None
        assert result.failure.signature == "invariant:no stock is lost"
    assert simloop.explore(inventory.main_fixed, seeds=range(300)).ok


def test_explore_stops_at_max_failures_and_time_budget() -> None:
    async def always_fails() -> None:
        raise AssertionError

    report = simloop.explore(always_fails, seeds=range(100), max_failures=3)
    assert report.runs == 3
    assert report.stopped_early
    budget = simloop.explore(always_fails, seeds=range(10**9), time_budget=0.2)
    assert 0 < budget.runs < 10**9


def test_explore_groups_distinct_failures() -> None:
    async def main() -> None:
        roll = simloop.rng().randrange(3)
        if roll == 1:
            raise KeyError("one")
        if roll == 2:
            await asyncio.Event().wait()

    report = simloop.explore(main, seeds=range(60))
    assert {g.kind for g in report.groups} == {"exception", "deadlock"}
    assert report.passed + sum(len(g.seeds) for g in report.groups) == 60


def test_on_result_callback_sees_every_run() -> None:
    seen: list[int] = []

    async def main() -> None:
        pass

    simloop.explore(main, seeds=[3, 1, 2], on_result=lambda r: seen.append(r.seed))
    assert seen == [3, 1, 2]


def test_cli_explore_reports_failure(capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(["explore", f"{INVENTORY}:main", "--seeds", "200", "--trace-last", "5"])
    out = capsys.readouterr().out
    assert code == 1
    assert "invariant: always 'no stock is lost' was violated" in out
    assert f"simloop run {INVENTORY}:main --seed" in out
    assert "failed   always 'no stock is lost' was violated" in out


def test_cli_run_single_seed(capsys: pytest.CaptureFixture[str]) -> None:
    seed = simloop.explore(inventory.main, seeds=range(100)).groups[0].seeds[0]
    assert cli.main(["run", f"{INVENTORY}:main", "--seed", str(seed)]) == 1
    assert f"FAILED (seed {seed})" in capsys.readouterr().out
    assert cli.main(["run", f"{INVENTORY}:main_fixed", "--seed", str(seed), "--trace"]) == 0
    out = capsys.readouterr().out
    assert "ok (seed" in out
    assert "spawn" in out


def test_cli_module_target_and_fifo(capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(["explore", "inventory:main_fixed", "--seeds", "20", "--scheduler", "fifo"])
    assert code == 0
    assert "20 runs, 20 passed" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("no_colon", "must look like"),
        ("missing_file.py:main", "no such file"),
        (f"{INVENTORY}:nope", "has no attribute"),
        (f"{INVENTORY}:INITIAL_STOCK", "not callable"),
    ],
)
def test_cli_bad_targets(target: str, message: str) -> None:
    with pytest.raises(SystemExit, match=message):
        cli.main(["run", target])


async def _impossible_on_asyncio() -> None:
    # 'x' is scheduled before 'y', so a real loop always runs it first. The
    # random scheduler may not, because they come from different steps.
    loop = asyncio.get_running_loop()
    order: list[str] = []
    loop.call_soon(lambda: loop.call_soon(order.append, "x"))
    loop.call_soon(lambda: loop.call_soon(order.append, "y"))
    await asyncio.sleep(0.1)
    assert order == ["x", "y"], order


def test_confirm_marks_failures_that_need_an_impossible_order() -> None:
    report = simloop.explore(_impossible_on_asyncio, seeds=range(20), confirm=30)
    assert len(report.groups) == 1
    assert report.groups[0].feasible is False
    assert "may be impossible on a real asyncio loop" in report.summary()
    assert simloop.confirm_feasible(_impossible_on_asyncio, report.groups[0].signature) is None


def test_confirm_reproduces_timing_races_with_asyncio_order() -> None:
    report = simloop.explore(inventory.main, seeds=range(100), confirm=100)
    group = report.groups[0]
    assert group.feasible is True
    assert group.feasible_config is not None
    assert group.feasible_config.scheduler == "fifo"
    assert group.feasible_seed is not None
    rerun = simloop.run(inventory.main, seed=group.feasible_seed, config=group.feasible_config)
    assert rerun.failure is not None
    assert rerun.failure.signature == group.signature
    assert "asyncio order: reproduced" in report.summary()
