from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest

import simloop
from simloop import _testing

pytest_plugins = ["pytester"]


@pytest.fixture(autouse=True)
def isolated_settings(tmp_path: Path) -> Iterator[_testing.TestSettings]:
    saved = _testing.settings
    _testing.settings = _testing.TestSettings(database=tmp_path / "examples")
    yield _testing.settings
    _testing.settings = saved


class Shop:
    broken = True

    def __init__(self) -> None:
        self.stock = 10
        self.lock = asyncio.Lock()

    async def take(self, n: int) -> None:
        if self.broken:
            seen = self.stock
            await asyncio.sleep(simloop.rng().uniform(0, 0.01))
            self.stock = seen - n
        else:
            async with self.lock:
                seen = self.stock
                await asyncio.sleep(simloop.rng().uniform(0, 0.01))
                self.stock = seen - n


async def shop_workload() -> None:
    shop = Shop()
    async with asyncio.TaskGroup() as tg:
        for n in (1, 2, 3):
            tg.create_task(shop.take(n))
    assert shop.stock == 4, f"stock is {shop.stock}"


def test_passing_simulation_test() -> None:
    calls = 0

    @simloop.test(seeds=20)
    async def check() -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(1)

    check()
    assert calls == 20


def test_bare_decorator_uses_default_seeds() -> None:
    seen: list[int] = []

    @simloop.test
    async def check() -> None:
        seen.append(simloop.current().seed)

    check()
    assert seen == list(range(_testing.DEFAULT_SEEDS))


def test_failure_is_shrunk_and_explained() -> None:
    check = simloop.test(seeds=50)(shop_workload)
    with pytest.raises(simloop.SimulationFailed) as info:
        check()
    message = str(info.value)
    assert "failed with exception: AssertionError: stock is" in message
    assert "shrunk in" in message
    assert "reproduce: pytest" in message
    assert "Last 30 trace events" in message
    assert isinstance(info.value.__cause__, AssertionError)
    assert info.value.result.failure is not None


def test_example_database_replays_remembered_failures(
    isolated_settings: _testing.TestSettings,
) -> None:
    check = simloop.test(seeds=50)(shop_workload)
    with pytest.raises(simloop.SimulationFailed):
        check()
    assert isolated_settings.database is not None
    saved = list(isolated_settings.database.glob("*.json"))
    assert len(saved) == 1

    # A seed range that never fails on its own still fails: the remembered
    # reproduction runs first.
    passing = [s for s in range(200) if simloop.run(shop_workload, seed=s).ok][:5]
    again = simloop.test(seeds=passing)(shop_workload)
    with pytest.raises(simloop.SimulationFailed, match="a remembered failure"):
        again()

    Shop.broken = False
    try:
        again()
    finally:
        Shop.broken = True
    assert not saved[0].exists()


def test_single_seed_setting_runs_only_that_seed(
    isolated_settings: _testing.TestSettings,
) -> None:
    seen: list[int] = []

    @simloop.test(seeds=100)
    async def check() -> None:
        seen.append(simloop.current().seed)

    isolated_settings.seed = 7
    check()
    assert seen == [7]


def test_rejects_sync_functions() -> None:
    def not_async() -> None:
        pass

    with pytest.raises(TypeError, match="async def"):
        simloop.test()(not_async)  # type: ignore[type-var]


PLUGIN_TESTS = """
import asyncio
import pytest
import simloop

@simloop.test(seeds=30)
@pytest.mark.parametrize("n", [1, 2])
async def test_with_fixtures(tmp_path, n):
    assert tmp_path.exists()
    await asyncio.sleep(n)

@simloop.test(seeds=30)
async def test_racy():
    state = {"v": 0}
    async def bump():
        v = state["v"]
        await asyncio.sleep(simloop.rng().uniform(0, 0.01))
        state["v"] = v + 1
    await asyncio.gather(bump(), bump())
    assert state["v"] == 2

@simloop.test(seeds=10)
async def test_count():
    print("SEED", simloop.current().seed)
"""


def test_pytest_plugin_end_to_end(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_sim=PLUGIN_TESTS)
    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=3, failed=1)
    result.stdout.fnmatch_lines(
        [
            "*SimLoop: seed * (found after * run*) failed with exception*",
            "*reproduce: pytest 'test_sim.py::test_racy' --simloop-seed*",
        ]
    )
    assert list((pytester.path / ".simloop" / "examples").glob("*test_racy*.json"))

    single = pytester.runpytest_subprocess("-k", "test_count", "-s", "--simloop-seed", "5")
    single.assert_outcomes(passed=1)
    assert single.stdout.str().count("SEED") == 1

    many = pytester.runpytest_subprocess("-k", "test_count", "-s", "--simloop-seeds", "3")
    assert many.stdout.str().count("SEED") == 3

    no_db = pytester.runpytest_subprocess("-k", "racy", "--simloop-no-db", "--simloop-no-shrink")
    no_db.assert_outcomes(failed=1)
    assert "SimLoop: a remembered failure" not in no_db.stdout.str()
    assert "shrunk in" not in no_db.stdout.str()

    timelines = pytester.path / "timelines"
    html = pytester.runpytest_subprocess(
        "-k", "racy", "--simloop-no-db", "--simloop-html", str(timelines)
    )
    html.assert_outcomes(failed=1)
    html.stdout.fnmatch_lines(["*timeline: *test_racy*.html*"])
    assert len(list(timelines.glob("*test_racy*.html"))) == 1
