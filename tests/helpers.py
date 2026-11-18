from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import pytest

import simloop

T = TypeVar("T")


def run_ok(
    main: Callable[[], Coroutine[Any, Any, T]], *, seed: int = 0, **config: Any
) -> simloop.RunResult[T]:
    """Run ``main`` and fail the test with the simulation's own report if it failed."""
    result = simloop.run(main, seed=seed, config=simloop.SimConfig(**config))
    if result.failure is not None:
        pytest.fail(f"simulation failed:\n{result.failure.format()}", pytrace=False)
    return result
