"""Simulated ``datetime.now()``, ``datetime.utcnow()`` and ``date.today()``.

Local time (``now()``, ``today()``, ``fromtimestamp()`` without a zone) is
the run's simulated zone (:class:`Zone`), not the host's.

CPython's ``datetime`` reads the OS clock in C, so patching ``time.time`` does
not reach it. During a run the clock methods of the real ``datetime`` and
``date`` classes are replaced in place, so the classes themselves never
change: ``from datetime import datetime`` references, subclasses, pickling,
``type()`` checks and C extensions all see the real types, and subclasses'
``now()`` returns instances of the subclass, as it does outside.
"""

from __future__ import annotations

import ctypes
import datetime as _datetime
import gc
import zoneinfo
from collections.abc import Callable
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


def _type_dict(cls: type) -> dict[str, Any]:
    """The dict behind ``cls.__dict__``, writable even for C types."""
    return gc.get_referents(cls.__dict__)[0]  # type: ignore[no-any-return]


_type_modified = ctypes.pythonapi.PyType_Modified
_type_modified.argtypes = [ctypes.py_object]
_type_modified.restype = None

_real_fromtimestamp = {cls: cls.__dict__["fromtimestamp"] for cls in (real_datetime, real_date)}


def _from(cls: type, timestamp: float, tz: _datetime.tzinfo | None) -> Any:
    """The original ``fromtimestamp`` of ``cls``'s base, building a ``cls``."""
    base = real_datetime if issubclass(cls, real_datetime) else real_date
    return _real_fromtimestamp[base].__get__(None, cls)(timestamp, tz)


def _datetime_now(cls: type, tz: _datetime.tzinfo | None = None) -> Any:
    return _datetime_fromtimestamp(cls, _now(), tz)


def _datetime_fromtimestamp(cls: type, timestamp: float, tz: _datetime.tzinfo | None = None) -> Any:
    if tz is not None:
        return _from(cls, timestamp, tz)
    return _from(cls, timestamp, _zone).replace(tzinfo=None)


def _datetime_utcnow(cls: type) -> Any:
    return _from(cls, _now(), _datetime.UTC).replace(tzinfo=None)


def _date_fromtimestamp(cls: type, timestamp: float) -> Any:
    local = _datetime_fromtimestamp(real_datetime, timestamp)
    return cls(local.year, local.month, local.day)


def _today(cls: type) -> Any:
    # As in C: today() is fromtimestamp(now), so subclasses that override
    # fromtimestamp see it called.
    return cls.fromtimestamp(_now())  # type: ignore[attr-defined]


# CPython's ``datetime`` reads the OS clock in C. Rather than replacing the
# classes (which breaks extensions that check types at the C level and cache
# them, such as PyO3's abi3 datetime support used by ``cryptography``), the
# clock methods are swapped in the real classes' own dicts for the run, the
# way time-machine does it. ``date.today`` also serves ``datetime.today``.
_PATCHES: list[tuple[type, str, Any]] = [
    (real_datetime, "now", classmethod(_datetime_now)),
    (real_datetime, "utcnow", classmethod(_datetime_utcnow)),
    (real_datetime, "fromtimestamp", classmethod(_datetime_fromtimestamp)),
    (real_date, "today", classmethod(_today)),
    (real_date, "fromtimestamp", classmethod(_date_fromtimestamp)),
]


def install(
    wall: Callable[[], float], zone: _datetime.tzinfo = _datetime.UTC
) -> list[tuple[type, str, Any]]:
    """Patch the clock methods in; returns ``(class, name, original)`` to undo."""
    global _wall, _zone
    _wall = wall
    _zone = zone
    undo: list[tuple[type, str, Any]] = []
    for cls, name, replacement in _PATCHES:
        namespace = _type_dict(cls)
        undo.append((cls, name, namespace[name]))
        namespace[name] = replacement
    for cls in (real_datetime, real_date):
        _type_modified(cls)
    return undo


def uninstall(undo: list[tuple[type, str, Any]]) -> None:
    global _wall, _zone
    for cls, name, original in reversed(undo):
        _type_dict(cls)[name] = original
    for cls in (real_datetime, real_date):
        _type_modified(cls)
    _wall = None
    _zone = _datetime.UTC
