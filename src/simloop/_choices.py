"""The single source of randomness for everything the framework decides.

Every decision (which ready callback runs next, timer jitter, network latency,
whether a fault fires) is drawn here as a bounded integer and recorded. A run
is reproducible from its seed alone, and the recorded sequence can be replayed
or edited for shrinking. Draws are arranged so that 0 is always the "simplest"
choice (FIFO order, no jitter, no fault), which is what makes shrinking toward
smaller values meaningful.
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Sequence
from typing import Any, NoReturn

_FLOAT_BITS = 53
_FLOAT_SCALE = 1 << _FLOAT_BITS


def derive_seed(seed: int, purpose: str) -> int:
    """Derive an independent 64-bit seed for ``purpose`` from a run seed."""
    digest = hashlib.blake2b(f"{seed}/{purpose}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")


class ChoiceSource:
    """Seeded, recorded stream of bounded integer choices.

    With ``replay`` set, choices are taken from that sequence instead of the
    PRNG (clamped to each draw's bound); once it is exhausted every further
    choice is 0.
    """

    __slots__ = ("_pos", "_replay", "_rng", "record", "seed")

    def __init__(self, seed: int, replay: Sequence[int] | None = None) -> None:
        self.seed = seed
        self._rng = random.Random(derive_seed(seed, "choices"))
        self._replay = list(replay) if replay is not None else None
        self._pos = 0
        self.record: list[int] = []

    def below(self, n: int) -> int:
        """Return an integer in ``[0, n)``. A bound of 1 records nothing."""
        if n <= 0:
            raise ValueError(f"bound must be positive, got {n}")
        if n == 1:
            return 0
        if self._replay is None:
            value = self._rng.randrange(n)
        elif self._pos < len(self._replay):
            value = min(self._replay[self._pos], n - 1)
            self._pos += 1
        else:
            value = 0
        self.record.append(value)
        return value

    def unit(self) -> float:
        """Return a float in ``[0, 1)``."""
        return self.below(_FLOAT_SCALE) / _FLOAT_SCALE

    def uniform(self, low: float, high: float) -> float:
        """Return a float in ``[low, high)``; a draw of 0 yields ``low``."""
        if high <= low:
            return low
        return low + (high - low) * self.unit()

    def chance(self, probability: float) -> bool:
        """Return True with the given probability; a draw of 0 yields False."""
        if probability <= 0.0:
            return False
        if probability >= 1.0:
            return True
        return self.unit() >= 1.0 - probability


class ChoiceRandom(random.Random):
    """A :class:`random.Random` whose entropy comes from a :class:`ChoiceSource`.

    Everything in the ``random.Random`` API is built on ``random()`` and
    ``getrandbits()``, so overriding those two routes all draws through the
    recorded choice stream.
    """

    def __init__(self, source: ChoiceSource) -> None:
        self._source = source
        super().__init__()

    def seed(self, a: Any = None, version: int = 2) -> None:
        # The simulation owns the seed; reseeding from user code is a no-op so
        # that it cannot pull in OS entropy.
        pass

    def random(self) -> float:
        return self._source.unit()

    def getrandbits(self, k: int, /) -> int:
        if k < 0:
            raise ValueError("number of bits must be non-negative")
        return self._source.below(1 << k) if k else 0

    def getstate(self) -> NoReturn:
        raise TypeError("the simulation random generator has no restorable state")

    def setstate(self, state: Any) -> NoReturn:
        raise TypeError("the simulation random generator has no restorable state")
