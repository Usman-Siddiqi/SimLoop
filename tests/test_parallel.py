from __future__ import annotations

import asyncio
import multiprocessing
import sys
from pathlib import Path

import pytest

import simloop
from simloop import _testing, cli

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "examples"))

import inventory  # noqa: E402

INVENTORY = str(ROOT / "examples" / "inventory.py")

needs_fork = pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(), reason="closures need fork"
)


def _shape(report: simloop.ExploreReport) -> tuple[object, ...]:
    groups = [(g.signature, g.seeds, g.reproducible) for g in report.groups]
    return report.runs, report.passed, report.steps, report.sim_time, groups


def test_parallel_report_matches_sequential() -> None:
    sequential = simloop.explore(inventory.main, seeds=range(120))
    parallel = simloop.explore(inventory.main, seeds=range(120), workers=3, chunk_size=7)
    assert _shape(parallel) == _shape(sequential)
    assert parallel.workers == 3
    assert "3 workers" in parallel.summary()


def test_parallel_failure_limit_stops_at_the_same_seed() -> None:
    sequential = simloop.explore(inventory.main, seeds=range(500), max_failures=3)
    parallel = simloop.explore(inventory.main, seeds=range(500), max_failures=3, workers=2)
    assert parallel.stopped_early
    assert _shape(parallel) == _shape(sequential)


@needs_fork
def test_parallel_workers_run_closures() -> None:
    limit = 2

    async def main() -> None:
        hits = 0

        async def bump() -> None:
            nonlocal hits
            seen = hits
            await asyncio.sleep(0)
            hits = seen + 1

        await asyncio.gather(*(bump() for _ in range(3)))
        assert hits > limit, hits

    report = simloop.explore(main, seeds=range(40), workers=2)
    assert report.failed > 0
    assert _shape(report) == _shape(simloop.explore(main, seeds=range(40)))


def test_on_result_needs_one_worker() -> None:
    with pytest.raises(ValueError, match="on_result"):
        simloop.explore(inventory.main, seeds=range(4), workers=2, on_result=print)


def test_negative_workers_are_rejected() -> None:
    with pytest.raises(ValueError, match="workers"):
        simloop.explore(inventory.main, seeds=range(4), workers=-1)


def test_cli_explore_with_workers(capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(["explore", f"{INVENTORY}:main", "--seeds", "60", "-j", "2", "--confirm", "0"])
    out = capsys.readouterr().out
    assert code == 1
    assert "2 workers" in out
    assert "no stock is lost" in out


@needs_fork
def test_simloop_test_with_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_testing, "settings", _testing.TestSettings(database=None, confirm=0))

    @simloop.test(seeds=50, workers=2, shrink_failures=False)
    async def check() -> None:
        await inventory.main()

    with pytest.raises(simloop.SimulationFailed, match="no stock is lost"):
        check()


def test_workers_setting_overrides_the_decorator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_testing, "settings", _testing.TestSettings(workers=4))
    assert _testing._pick_workers(1) == 4
    monkeypatch.setattr(_testing, "settings", _testing.TestSettings())
    monkeypatch.setenv("SIMLOOP_WORKERS", "3")
    assert _testing._pick_workers(1) == 3
