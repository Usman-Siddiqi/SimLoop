"""Simulated ``datetime.now()``, ``datetime.utcnow()`` and ``date.today()``.

CPython's ``datetime`` reads the OS clock in C, so patching ``time.time`` does
not reach it, and its types are immutable, so ``datetime.now`` cannot be
replaced in place. Instead, during a run, ``datetime.datetime`` and
``datetime.date`` are replaced by stand-in classes whose clock methods read the
simulated wall clock. Every value they produce is an ordinary ``datetime`` or
``date``, so pickling, ``type()`` checks and arithmetic see nothing unusual,
and ``isinstance`` against a stand-in accepts real instances.

``from datetime import datetime`` binds the real class into the importing
module, so loaded modules are scanned for such references (as freezegun
does). Each module is scanned once per process, which keeps the per-run cost
to patching the references already found.
"""

from __future__ import annotations

import datetime as _datetime
import sys
from collections.abc import Callable
from types import ModuleType
from typing import Any

real_datetime = _datetime.datetime
real_date = _datetime.date

_wall: Callable[[], float] | None = None


def _now() -> float:
    if _wall is None:
        raise RuntimeError("simulated datetime used outside a simulation")
    return _wall()


class _StandInMeta(type):
    _real: type

    def __instancecheck__(cls, instance: object) -> bool:
        return isinstance(instance, cls._real)

    def __subclasscheck__(cls, subclass: type) -> bool:
        return issubclass(subclass, cls._real)


class SimDate(real_date, metaclass=_StandInMeta):
    _real = real_date

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        return real_date(*args, **kwargs)

    @classmethod
    def today(cls) -> Any:
        return real_date.fromtimestamp(_now())


class SimDatetime(real_datetime, metaclass=_StandInMeta):
    _real = real_datetime

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        return real_datetime(*args, **kwargs)

    @classmethod
    def now(cls, tz: _datetime.tzinfo | None = None) -> Any:
        return real_datetime.fromtimestamp(_now(), tz)

    @classmethod
    def today(cls) -> Any:
        return real_datetime.fromtimestamp(_now())

    @classmethod
    def utcnow(cls) -> Any:
        return real_datetime.fromtimestamp(_now(), _datetime.UTC).replace(tzinfo=None)


_STAND_INS: dict[int, type] = {id(real_datetime): SimDatetime, id(real_date): SimDate}
_scanned: set[str] = set()
_references: list[tuple[ModuleType, str, type]] = []


def _scan() -> None:
    for name, module in list(sys.modules.items()):
        if name in _scanned:
            continue
        _scanned.add(name)
        if module is None or module is _datetime or name == __name__:
            continue
        try:
            items = list(vars(module).items())
        except TypeError:
            continue
        for attr, value in items:
            if isinstance(value, type) and id(value) in _STAND_INS:
                _references.append((module, attr, value))


def install(wall: Callable[[], float]) -> list[tuple[object, str, object]]:
    """Patch in the stand-ins; returns ``(owner, name, original)`` to undo."""
    global _wall
    _wall = wall
    _scan()
    undo: list[tuple[object, str, object]] = []
    targets: list[tuple[object, str, type]] = [
        (_datetime, "datetime", real_datetime),
        (_datetime, "date", real_date),
        *_references,
    ]
    for owner, attr, original in targets:
        # Only replace a reference that still points at the real class.
        if getattr(owner, attr, None) is original:
            setattr(owner, attr, _STAND_INS[id(original)])
            undo.append((owner, attr, original))
    return undo


def uninstall(undo: list[tuple[object, str, object]]) -> None:
    global _wall
    for owner, attr, original in reversed(undo):
        if getattr(owner, attr, None) is _STAND_INS[id(original)]:
            setattr(owner, attr, original)
    _wall = None
