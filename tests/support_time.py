"""Imports time and entropy functions by name, like many libraries do."""

from __future__ import annotations

from os import urandom
from time import monotonic, sleep


def readings() -> tuple[float, bytes]:
    sleep(0.5)
    return monotonic(), urandom(4)
