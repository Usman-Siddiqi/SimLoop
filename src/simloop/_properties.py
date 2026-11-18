"""``always`` and ``eventually`` properties checked by the simulation loop."""

from __future__ import annotations

from collections.abc import Callable

from .errors import InvariantViolation

Predicate = Callable[[], bool]


class Property:
    """A registered property. Call :meth:`cancel` to stop checking it."""

    __slots__ = ("active", "deadline", "kind", "name", "predicate")

    def __init__(self, kind: str, name: str, predicate: Predicate, deadline: float | None) -> None:
        self.kind = kind
        self.name = name
        self.predicate = predicate
        self.deadline = deadline
        self.active = True

    def cancel(self) -> None:
        self.active = False

    def __repr__(self) -> str:
        return f"<{self.kind} {self.name!r}{'' if self.active else ' cancelled'}>"


class PropertyChecker:
    """Evaluates registered properties after each loop step and at the end."""

    def __init__(self, clock: Callable[[], float]) -> None:
        self._clock = clock
        self._always: list[Property] = []
        self._eventually: list[Property] = []

    def add_always(self, name: str, predicate: Predicate) -> Property:
        prop = Property("always", name, predicate, None)
        self._always.append(prop)
        self._check_always(prop)
        return prop

    def add_eventually(self, name: str, predicate: Predicate, within: float | None) -> Property:
        deadline = None if within is None else self._clock() + within
        prop = Property("eventually", name, predicate, deadline)
        self._eventually.append(prop)
        return prop

    def after_step(self) -> None:
        if self._always:
            for prop in self._always:
                if prop.active:
                    self._check_always(prop)
            self._always = [p for p in self._always if p.active]
        if self._eventually:
            for prop in self._eventually:
                if prop.active and _holds(prop):
                    prop.active = False
            self._eventually = [p for p in self._eventually if p.active]

    def before_advance(self, new_time: float) -> None:
        for prop in self._eventually:
            if prop.active and prop.deadline is not None and new_time > prop.deadline:
                raise InvariantViolation(
                    f"eventually {prop.name!r} did not hold by t={prop.deadline!r}",
                    prop.name,
                )

    def at_end(self) -> None:
        for prop in self._eventually:
            if prop.active and not _holds(prop):
                raise InvariantViolation(
                    f"eventually {prop.name!r} never held before the run ended", prop.name
                )

    def _check_always(self, prop: Property) -> None:
        if not _holds(prop):
            raise InvariantViolation(f"always {prop.name!r} was violated", prop.name)


def _holds(prop: Property) -> bool:
    try:
        return bool(prop.predicate())
    except Exception as exc:
        raise InvariantViolation(
            f"{prop.kind} {prop.name!r} raised {type(exc).__name__}: {exc}", prop.name
        ) from exc
