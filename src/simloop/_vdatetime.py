"""Simulated ``datetime.now()``, ``datetime.utcnow()`` and ``date.today()``.

Local time (``now()``, ``today()``, ``fromtimestamp()`` without a zone) is
the run's simulated zone (:class:`Zone`), not the host's.

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
import zoneinfo
from collections.abc import Callable
from types import ModuleType
from typing import Any

real_datetime = _datetime.datetime
real_date = _datetime.date

_wall: Callable[[], float] | None = None
_zone: _datetime.tzinfo = _datetime.UTC


class Zone:
    """The simulated local time zone: a fixed UTC offset or an IANA zone.

    ``spec`` is an offset in hours (``-5``, ``5.5``) or a zone name
    (``"America/New_York"``, which brings daylight-saving transitions).
    """

    def __init__(self, spec: str | float) -> None:
        self.spec = spec
        self.tzinfo: _datetime.tzinfo
        if isinstance(spec, str) and spec.upper() != "UTC":
            try:
                self.tzinfo = zoneinfo.ZoneInfo(spec)
            except (zoneinfo.ZoneInfoNotFoundError, ValueError) as exc:
                raise ValueError(f"unknown timezone {spec!r}") from exc
            self.tz_variable = spec
            return
        hours = 0.0 if isinstance(spec, str) else float(spec)
        if not -24 < hours < 24:
            raise ValueError(f"timezone offset must be between -24 and 24 hours, not {hours}")
        minutes = round(hours * 60)
        if minutes == 0:
            self.tzinfo = _datetime.UTC
        else:
            sign = "+" if minutes > 0 else "-"
            name = f"UTC{sign}{abs(minutes) // 60:02d}:{abs(minutes) % 60:02d}"
            self.tzinfo = _datetime.timezone(_datetime.timedelta(minutes=minutes), name)
        self.tz_variable = _fixed_tz_variable(minutes, self.tzinfo)

    def local(self, timestamp: float) -> Any:
        """The aware local time at ``timestamp``."""
        return real_datetime.fromtimestamp(timestamp, self.tzinfo)

    def timestamp(self, fields: tuple[int, ...], isdst: int) -> float:
        """Inverse of :meth:`local` for broken-down time (``mktime``)."""
        year, month, day, hour, minute, second = fields[:6]
        naive = real_datetime(year, month, day, hour, minute, second)
        # In a repeated hour, tm_isdst=0 picks the second (standard) instance.
        fold = 1 if isdst == 0 and self._ambiguous(naive) else 0
        return naive.replace(tzinfo=self.tzinfo, fold=fold).timestamp()

    def _ambiguous(self, naive: Any) -> bool:
        first = naive.replace(tzinfo=self.tzinfo, fold=0).utcoffset()
        second = naive.replace(tzinfo=self.tzinfo, fold=1).utcoffset()
        return bool(first != second)

    def constants(self, year: int) -> tuple[int, int, int, tuple[str, str]]:
        """``time.timezone``, ``altzone``, ``daylight`` and ``tzname`` for ``year``."""
        samples = [real_datetime(year, month, 1, tzinfo=self.tzinfo) for month in (1, 7)]
        standard = [d for d in samples if not d.dst()] or samples[:1]
        summer = [d for d in samples if d.dst()] or standard
        std, dst = standard[0], summer[0]

        def west(moment: Any) -> int:
            return -round(float(moment.utcoffset().total_seconds()))

        return west(std), west(dst), int(dst is not std), (str(std.tzname()), str(dst.tzname()))


# Etc has whole hours only. These zones keep a fractional offset all year
# (no daylight saving today), so tzlocal, which accepts only IANA names,
# still works; their historical offsets can differ.
_FRACTIONAL_ZONES = {
    -570: "Pacific/Marquesas",
    210: "Asia/Tehran",
    270: "Asia/Kabul",
    330: "Asia/Kolkata",
    345: "Asia/Kathmandu",
    390: "Asia/Yangon",
    525: "Australia/Eucla",
    570: "Australia/Darwin",
}


def _fixed_tz_variable(minutes: int, tzinfo: _datetime.tzinfo) -> str:
    """A ``TZ`` value for a fixed offset: an IANA name when one exists, else POSIX."""
    if minutes == 0:
        return "UTC"
    if minutes % 60 == 0:
        # IANA's Etc zones have the sign inverted (POSIX style): UTC-05:00 is Etc/GMT+5.
        return f"Etc/GMT{-minutes // 60:+d}"
    if minutes in _FRACTIONAL_ZONES:
        return _FRACTIONAL_ZONES[minutes]
    west = -minutes
    sign = "-" if west < 0 else ""
    return f"<{tzinfo.tzname(None)}>{sign}{abs(west) // 60}:{abs(west) % 60:02d}"


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
    wall: Callable[[], float], zone: _datetime.tzinfo = _datetime.UTC
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
