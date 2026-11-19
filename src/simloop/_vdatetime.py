"""Simulated ``datetime.now()``, ``datetime.utcnow()`` and ``date.today()``.

Local time (``now()``, ``today()``, ``fromtimestamp()`` without a zone) is
the run's simulated zone, a fixed UTC offset, not the host's.

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
_zone: _datetime.timezone = _datetime.UTC


def fixed_zone(hours: float) -> _datetime.timezone:
    """The simulated local zone: a fixed offset, named like ``UTC+05:30``."""
    if not -24 < hours < 24:
        raise ValueError(f"utc_offset must be between -24 and 24 hours, not {hours}")
    if hours == 0:
        return _datetime.UTC
    minutes = round(hours * 60)
    sign = "+" if minutes > 0 else "-"
    name = f"UTC{sign}{abs(minutes) // 60:02d}:{abs(minutes) % 60:02d}"
    return _datetime.timezone(_datetime.timedelta(minutes=minutes), name)


def tz_variable(zone: _datetime.timezone) -> str:
    """A ``TZ`` value for ``zone``: an IANA name when one exists, else POSIX."""
    offset = zone.utcoffset(None)
    minutes = round(offset.total_seconds() / 60)
    if minutes == 0:
        return "UTC"
    if minutes % 60 == 0:
        # IANA's Etc zones have the sign inverted (POSIX style): UTC-05:00 is Etc/GMT+5.
        return f"Etc/GMT{-minutes // 60:+d}"
    west = -minutes
    sign = "-" if west < 0 else ""
    return f"<{zone.tzname(None)}>{sign}{abs(west) // 60}:{abs(west) % 60:02d}"


def _local(timestamp: float) -> Any:
    return real_datetime.fromtimestamp(timestamp, _zone).replace(tzinfo=None)


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
        return _local(_now()).date()

    @classmethod
    def fromtimestamp(cls, timestamp: float) -> Any:
        return _local(timestamp).date()


class SimDatetime(real_datetime, metaclass=_StandInMeta):
    _real = real_datetime

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        return real_datetime(*args, **kwargs)

    @classmethod
    def now(cls, tz: _datetime.tzinfo | None = None) -> Any:
        return _local(_now()) if tz is None else real_datetime.fromtimestamp(_now(), tz)

    @classmethod
    def today(cls) -> Any:
        return _local(_now())

    @classmethod
    def fromtimestamp(cls, timestamp: float, tz: _datetime.tzinfo | None = None) -> Any:
        return _local(timestamp) if tz is None else real_datetime.fromtimestamp(timestamp, tz)

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


def install(
    wall: Callable[[], float], zone: _datetime.timezone = _datetime.UTC
) -> list[tuple[object, str, object]]:
    """Patch in the stand-ins; returns ``(owner, name, original)`` to undo."""
    global _wall, _zone
    _wall = wall
    _zone = zone
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
    global _wall, _zone
    for owner, attr, original in reversed(undo):
        if getattr(owner, attr, None) is _STAND_INS[id(original)]:
            setattr(owner, attr, original)
    _wall = None
    _zone = _datetime.UTC
