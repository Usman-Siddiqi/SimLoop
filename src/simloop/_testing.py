"""``@simloop.test``: turn an async test into a seed exploration.

The decorated function becomes a plain synchronous test that any runner can
call. Under pytest the plugin in :mod:`simloop.pytest_plugin` lets the command
line override the seeds, and the plugin's rootdir anchors the failure
database.

Failing seeds are remembered in ``.simloop/examples/`` (Hypothesis calls this
the example database) and replayed first on the next run, so a bug found once
keeps failing fast until it is fixed, even if the seed range changes.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import os
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar, overload

from ._explore import explore
from ._repro import Reproduction
from ._runtime import RunResult, SimConfig, run
from ._shrink import shrink

F = TypeVar("F", bound=Callable[..., Coroutine[Any, Any, Any]])

DEFAULT_SEEDS = 100
_SEEDS_ENV = "SIMLOOP_SEEDS"
_SEED_ENV = "SIMLOOP_SEED"


@dataclass
class TestSettings:
    """Process-wide overrides, set by the pytest plugin or the environment."""

    __test__ = False

    seeds: int | None = None
    start: int = 0
    seed: int | None = None
    """Run exactly this seed (with a full trace) instead of exploring."""
    shrink: bool | None = None
    database: Path | None = field(default_factory=lambda: Path(".simloop") / "examples")
    trace_last: int = 30


settings = TestSettings()


class SimulationFailed(AssertionError):
    """A ``@simloop.test`` found a failing run. The message tells how to replay it."""

    __module__ = "simloop"

    def __init__(self, message: str, result: RunResult[Any]) -> None:
        super().__init__(message)
        self.result = result


def _env_int(name: str) -> int | None:
    value = os.environ.get(name)
    return int(value) if value else None


def _test_key(fn: Callable[..., Any]) -> str:
    # Identify a test by module and qualname, not by file path, so the database
    # survives moving the checkout.
    name = f"{fn.__module__}.{fn.__qualname__}"
    digest = hashlib.blake2b(name.encode(), digest_size=6).hexdigest()
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return f"{safe[-80:]}-{digest}"


class _Database:
    def __init__(self, root: Path | None, key: str) -> None:
        self.path = None if root is None else root / f"{key}.json"

    def load(self) -> list[Reproduction]:
        if self.path is None or not self.path.is_file():
            return []
        try:
            entries = json.loads(self.path.read_text())
            return [Reproduction.from_dict(entry) for entry in entries]
        except (ValueError, KeyError, TypeError):
            # A stale or hand-edited file should never break the test run.
            return []

    def store(self, repros: list[Reproduction]) -> None:
        if self.path is None:
            return
        if not repros:
            self.path.unlink(missing_ok=True)
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = [r.to_dict() for r in repros]
        self.path.write_text(json.dumps(payload, indent=2) + "\n")


def _cause(result: RunResult[Any]) -> BaseException | None:
    return result.failure.exception if result.failure else None


def _signature(result: RunResult[Any]) -> str | None:
    return result.failure.signature if result.failure else None


def _reproduce_hint(seed: int) -> str:
    current = os.environ.get("PYTEST_CURRENT_TEST")
    if current:
        nodeid = current.rsplit(" ", 1)[0]
        return f"pytest '{nodeid}' --simloop-seed {seed}"
    return f"{_SEED_ENV}={seed} <your test command>"


def _describe(
    result: RunResult[Any],
    *,
    origin: str,
    shrunk_summary: str | None,
    trace_last: int,
) -> str:
    failure = result.failure
    assert failure is not None
    lines = [
        f"SimLoop: {origin} failed with {failure.kind}: {failure.message}",
        f"  seed {result.seed}, {result.steps} steps, t={result.sim_time:.6f}s",
    ]
    if shrunk_summary is not None:
        lines.append(f"  {shrunk_summary}")
    lines.append(f"  reproduce: {_reproduce_hint(result.seed)}")
    if result.replayed:
        lines.append("  (the shown run is the shrunk one; it is saved in the example database)")
    if result.trace.keep and trace_last:
        lines += ["", f"Last {trace_last} trace events:", result.trace.format(last=trace_last)]
    return "\n".join(lines)


@overload
def test(seeds: F, /) -> Callable[..., None]: ...


@overload
def test(
    seeds: int | Iterable[int] = ...,
    *,
    config: SimConfig | None = ...,
    shrink_failures: bool = ...,
    max_failures: int | None = ...,
    time_budget: float | None = ...,
) -> Callable[[F], Callable[..., None]]: ...


def test(
    seeds: Any = DEFAULT_SEEDS,
    *,
    config: SimConfig | None = None,
    shrink_failures: bool = True,
    max_failures: int | None = 1,
    time_budget: float | None = None,
) -> Any:
    """Run an async test under SimLoop for many seeds.

    ::

        @simloop.test(seeds=500)
        async def test_no_lost_updates():
            ...

    Bare ``@simloop.test`` uses :data:`DEFAULT_SEEDS` seeds. The test fails on
    the first failing seed (after replaying any seeds the
    example database remembers), shrinks it, and raises
    :class:`SimulationFailed` with the seed and a trace tail. Arguments of the
    decorated function are passed through, so pytest fixtures work.
    """

    if inspect.iscoroutinefunction(seeds):
        return test()(seeds)
    seed_spec: int | list[int] | range = (
        seeds if isinstance(seeds, int | range) else [int(s) for s in seeds]
    )

    def decorate(fn: F) -> Callable[..., None]:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("@simloop.test needs an async def function")
        key = _test_key(fn)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            __tracebackhide__ = True

            def main() -> Coroutine[Any, Any, Any]:
                return fn(*args, **kwargs)

            _run_test(
                main,
                key=key,
                seeds=seed_spec,
                config=config or SimConfig(),
                shrink_failures=shrink_failures,
                max_failures=max_failures,
                time_budget=time_budget,
            )

        # pytest must call the sync wrapper, not treat it as a coroutine test.
        wrapper.__dict__.pop("__wrapped__", None)
        wrapper.__signature__ = inspect.signature(fn)  # type: ignore[attr-defined]
        wrapper.simloop_key = key  # type: ignore[attr-defined]
        return wrapper

    return decorate


test.__test__ = False  # type: ignore[attr-defined]


def _run_test(
    main: Callable[[], Coroutine[Any, Any, Any]],
    *,
    key: str,
    seeds: int | Iterable[int],
    config: SimConfig,
    shrink_failures: bool,
    max_failures: int | None,
    time_budget: float | None,
) -> None:
    __tracebackhide__ = True
    single = settings.seed if settings.seed is not None else _env_int(_SEED_ENV)
    if single is not None:
        result = run(main, seed=single, config=config)
        if not result.ok:
            message = _describe(
                result,
                origin=f"seed {single}",
                shrunk_summary=None,
                trace_last=settings.trace_last,
            )
            raise SimulationFailed(message, result) from _cause(result)
        return

    database = _Database(settings.database, key)
    remembered = database.load()
    still_failing: list[Reproduction] = []
    first: RunResult[Any] | None = None
    for repro in remembered:
        if repro.config != config:
            continue
        result = repro.run(main)
        if not result.ok and repro.signature in (None, _signature(result)):
            still_failing.append(repro)
            first = first or result
    if first is not None:
        database.store(still_failing)
        message = _describe(
            first,
            origin="a remembered failure",
            shrunk_summary=None,
            trace_last=settings.trace_last,
        )
        raise SimulationFailed(message, first) from _cause(first)

    count = settings.seeds if settings.seeds is not None else _env_int(_SEEDS_ENV)
    if count is not None:
        seed_range: Iterable[int] = range(settings.start, settings.start + count)
    elif isinstance(seeds, int):
        seed_range = range(settings.start, settings.start + seeds)
    else:
        seed_range = seeds
    report = explore(
        main, seeds=seed_range, config=config, max_failures=max_failures, time_budget=time_budget
    )
    if report.ok:
        database.store([])
        return
    group = report.groups[0]
    example = group.example
    assert example is not None
    summary = None
    shown = example
    do_shrink = settings.shrink if settings.shrink is not None else shrink_failures
    if do_shrink:
        shrunk = shrink(main, example)
        summary = shrunk.summary()
        shown = shrunk.shrunk
    database.store([Reproduction.from_result(shown)])
    runs = "1 run" if report.runs == 1 else f"{report.runs} runs"
    origin = f"seed {group.seeds[0]} (found after {runs})"
    message = _describe(
        shown,
        origin=origin,
        shrunk_summary=summary,
        trace_last=settings.trace_last,
    )
    if not group.reproducible:
        message += (
            "\n\nWARNING: rerunning this seed produced a different trace; "
            "the code under test is leaking non-determinism"
        )
    raise SimulationFailed(message, shown) from _cause(shown)
