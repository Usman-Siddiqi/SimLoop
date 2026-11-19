"""Imports time and entropy functions by name, like many libraries do."""

from __future__ import annotations

import dataclasses
import time
from os import urandom
from time import monotonic, sleep


def readings() -> tuple[float, bytes]:
    sleep(0.5)
    return monotonic(), urandom(4)


@dataclasses.dataclass
class Stamp:
    """Captures the clock in a field's ``default_factory``."""

    at: float = dataclasses.field(default_factory=time.monotonic)


class Clock:
    """Captures the clock as a class attribute."""

    now = time.monotonic


def elapsed(started: float, clock: object = time.monotonic) -> float:
    """Captures the clock as a default argument."""
    assert callable(clock)
    return float(clock()) - started


def make_timer() -> object:
    clock = time.monotonic

    def read() -> float:
        return clock()

    return read


timer = make_timer()
"""Captures the clock in a closure."""
