"""Exceptions raised by SimLoop."""

from __future__ import annotations


class SimulationError(Exception):
    """Base class for errors raised by the simulation itself."""


class DeadlockError(SimulationError):
    """Nothing can make progress: no ready callbacks, no timers, work pending."""

    def __init__(self, message: str, blocked: list[str]) -> None:
        super().__init__(message)
        self.blocked = blocked


class StepLimitExceeded(SimulationError):
    """The run executed more callbacks than ``max_steps`` allows."""


class TimeLimitExceeded(SimulationError):
    """Simulated time would pass ``max_time`` before the run finished."""


class InvariantViolation(SimulationError):
    """An ``always`` or ``eventually`` property did not hold."""

    def __init__(self, message: str, property_name: str) -> None:
        super().__init__(message)
        self.property_name = property_name


class CancellationLeak(SimulationError):
    """Cancellation crossed a task boundary it should not have.

    ``kind`` is ``"cancel_leak"`` for a task that finished with cancellation
    requests nobody consumed, or ``"stray_cancel"`` for a task that ended
    cancelled although nothing called ``cancel()`` on it. Raised only with
    ``SimConfig(strict_cancellation=True)``.
    """

    def __init__(
        self, message: str, task_name: str, coroutine: str, kind: str = "cancel_leak"
    ) -> None:
        super().__init__(message)
        self.task_name = task_name
        self.coroutine = coroutine
        self.kind = kind


class NondeterminismError(RuntimeError):
    """Code inside a simulation touched a real, non-deterministic resource.

    Deliberately not an ``OSError``: code that swallows network errors must not
    be able to quietly route around a leak. The leak is also recorded on the
    run, so it fails even if this exception is caught.
    """


class UnsupportedOperation(NotImplementedError):
    """The event loop method has no simulated implementation."""
