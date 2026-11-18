from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import pytest

import simloop
from simloop import cli
from simloop._shrink import normalize

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "examples"))

import inventory  # noqa: E402

INVENTORY = str(ROOT / "examples" / "inventory.py")


def first_failure(
    main: Callable[[], Coroutine[Any, Any, Any]], seeds: range = range(200)
) -> simloop.RunResult[Any]:
    for seed in seeds:
        result = simloop.run(main, seed=seed)
        if not result.ok:
            return result
    raise AssertionError("no failing seed")


async def needle() -> None:
    """Fails only when one specific draw is large; everything else is noise."""
    rng = simloop.rng()
    for _ in range(30):
        rng.random()
    if rng.randrange(1000) >= 700:
        raise ValueError("needle")
    for _ in range(30):
        rng.random()


async def two_bugs() -> None:
    roll = simloop.rng().randrange(100)
    await asyncio.sleep(simloop.rng().uniform(0, 1))
    if roll < 10:
        raise KeyError("small")
    if roll > 90:
        raise LookupError("large")


def test_normalize_drops_trailing_zeros_only() -> None:
    assert normalize([0, 3, 0, 0]) == [0, 3]
    assert normalize([0, 0]) == []
    assert normalize([]) == []


def test_inventory_race_shrinks_to_fifo_schedule() -> None:
    failing = first_failure(inventory.main)
    result = simloop.shrink(inventory.main, failing)
    assert result.choices == []
    assert result.shrunk.failure is not None
    assert result.shrunk.failure.signature == failing.failure.signature  # type: ignore[union-attr]
    assert result.shrunk.sim_time <= failing.sim_time
    assert "shrunk in" in result.summary()


def test_shrinking_finds_the_boundary_value() -> None:
    failing = first_failure(needle)
    result = simloop.shrink(needle, failing)
    assert result.choices[:-1] == [0] * 30
    assert result.choices[-1] == 700
    assert result.runs < 300


def test_shrinking_keeps_the_failure_signature() -> None:
    failing = next(
        r
        for r in (simloop.run(two_bugs, seed=s) for s in range(500))
        if r.failure is not None and "LookupError" in r.failure.signature
    )
    result = simloop.shrink(two_bugs, failing)
    assert result.shrunk.failure is not None
    assert result.shrunk.failure.signature == failing.failure.signature  # type: ignore[union-attr]
    assert result.choices == [91]


def test_shrinking_is_deterministic() -> None:
    failing = first_failure(needle)
    first = simloop.shrink(needle, failing)
    again = simloop.shrink(needle, failing)
    assert first.choices == again.choices
    assert first.shrunk.digest == again.shrunk.digest
    replayed = simloop.run(needle, seed=failing.seed, replay=first.choices)
    assert replayed.digest == first.shrunk.digest


def test_shrinking_respects_the_run_budget() -> None:
    failing = first_failure(needle)
    result = simloop.shrink(needle, failing, max_runs=5)
    assert result.runs <= 6
    assert result.shrunk.failure is not None


def test_shrinking_a_passing_run_is_an_error() -> None:
    async def fine() -> None:
        pass

    with pytest.raises(ValueError, match="failing"):
        simloop.shrink(fine, simloop.run(fine))


def test_reproduction_round_trips_through_json(tmp_path: Path) -> None:
    config = simloop.SimConfig(
        scheduler="fifo", timer_jitter=0.01, link=simloop.LinkConfig(latency=(0.1, 0.2))
    )
    failing = simloop.run(
        needle,
        seed=next(s for s in range(200) if not simloop.run(needle, seed=s).ok),
        config=config,
    )
    shrunk = simloop.shrink(needle, failing).shrunk
    repro = simloop.Reproduction.from_result(shrunk, target="mod:needle")
    path = repro.save(tmp_path / repro.filename())
    loaded = simloop.Reproduction.load(path)
    assert loaded == repro
    assert loaded.config.link.latency == (0.1, 0.2)
    assert loaded.run(needle).digest == shrunk.digest
    assert json.loads(path.read_text())["choices"] == shrunk.choices


def test_unshrunk_reproduction_replays_from_seed() -> None:
    failing = first_failure(needle)
    repro = simloop.Reproduction.from_result(failing)
    assert repro.choices is None
    assert repro.run(needle).digest == simloop.run(needle, seed=failing.seed).digest


def test_config_rejects_unknown_fields() -> None:
    with pytest.raises(ValueError, match="bogus"):
        simloop.SimConfig.from_dict({"bogus": 1})
    assert simloop.SimConfig.from_dict(simloop.SimConfig().to_dict()) == simloop.SimConfig()


def test_cli_shrink_save_and_replay(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    code = cli.main(["explore", f"{INVENTORY}:main", "--seeds", "50", "--shrink", "--save"])
    out = capsys.readouterr().out
    assert code == 1
    assert "12 -> 0 choices" in out
    saved = list((tmp_path / ".simloop" / "failures").glob("*.json"))
    assert len(saved) == 1
    assert f"simloop replay {saved[0].relative_to(tmp_path)}" in out

    assert cli.main(["replay", str(saved[0]), "--trace-last", "3"]) == 1
    out = capsys.readouterr().out
    assert "always 'no stock is lost' was violated" in out

    assert cli.main(["replay", str(saved[0]), "--target", f"{INVENTORY}:main_fixed"]) == 0
    assert "no longer reproduces" in capsys.readouterr().out
