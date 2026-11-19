from __future__ import annotations

import asyncio
import datetime
import os
import sys
import time
import types
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

import simloop

from .helpers import run_ok


@contextmanager
def host_zone(name: str) -> Iterator[None]:
    saved = os.environ.get("TZ")
    os.environ["TZ"] = name
    time.tzset()
    try:
        yield
    finally:
        if saved is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = saved
        time.tzset()


async def local_views() -> list[str]:
    await asyncio.sleep(3600)
    views = [
        datetime.datetime.now().isoformat(),
        datetime.datetime.today().isoformat(),
        datetime.datetime.fromtimestamp(0).isoformat(),
        datetime.date.today().isoformat(),
        datetime.date.fromtimestamp(0).isoformat(),
        time.strftime("%Y-%m-%d %H:%M %z %Z"),
        time.ctime(0),
        time.asctime(),
        str(time.localtime(0).tm_hour),
        str(time.mktime(time.localtime(1_000_000))),
        f"{time.timezone} {time.altzone} {time.daylight} {time.tzname}",
    ]
    simloop.log(" | ".join(views))
    return views


needs_tzset = pytest.mark.skipif(not hasattr(time, "tzset"), reason="no time.tzset")


@needs_tzset
def test_local_time_does_not_depend_on_the_host_zone() -> None:
    with host_zone("UTC"):
        utc = run_ok(local_views)
    with host_zone("America/New_York"):
        new_york = run_ok(local_views)
    with host_zone("Asia/Kolkata"):
        kolkata = run_ok(local_views)
    assert utc.value == new_york.value == kolkata.value
    assert utc.digest == new_york.digest == kolkata.digest
    assert utc.value is not None
    assert utc.value[0] == "2024-01-01T01:00:00"
    assert utc.value[5] == "2024-01-01 01:00 +0000 UTC"


def test_an_offset_sets_the_simulated_zone() -> None:
    views = run_ok(local_views, timezone=-5).value
    assert views == [
        "2023-12-31T20:00:00",
        "2023-12-31T20:00:00",
        "1969-12-31T19:00:00",
        "2023-12-31",
        "1969-12-31",
        "2023-12-31 20:00 -0500 UTC-05:00",
        "Wed Dec 31 19:00:00 1969",
        "Sun Dec 31 20:00:00 2023",
        "19",
        "1000000.0",
        "18000 18000 0 ('UTC-05:00', 'UTC-05:00')",
    ]


def test_utc_time_ignores_the_offset() -> None:
    async def main() -> tuple[str, str, str]:
        return (
            datetime.datetime.utcnow().isoformat(),
            datetime.datetime.now(datetime.UTC).isoformat(),
            time.strftime("%H:%M", time.gmtime()),
        )

    assert run_ok(main, timezone=5.5).value == (
        "2024-01-01T00:00:00",
        "2024-01-01T00:00:00+00:00",
        "00:00",
    )


def test_fractional_offsets_are_named() -> None:
    async def main() -> str:
        return time.strftime("%H:%M %z %Z")

    assert run_ok(main, timezone=5.5).value == "05:30 +0530 UTC+05:30"


def test_local_time_functions_are_restored() -> None:
    before = (time.localtime, time.mktime, time.timezone, time.tzname)
    run_ok(local_views, timezone=3)
    assert (time.localtime, time.mktime, time.timezone, time.tzname) == before


def test_offset_is_validated() -> None:
    with pytest.raises(ValueError, match="timezone offset"):
        simloop.run(local_views, config=simloop.SimConfig(timezone=30))


def test_same_seed_same_trace_with_an_offset() -> None:
    first = run_ok(local_views, seed=4, timezone=-3)
    second = run_ok(local_views, seed=4, timezone=-3)
    assert first.digest == second.digest
    assert first.digest != run_ok(local_views, seed=4).digest


def test_tz_variable_names_the_simulated_zone() -> None:
    async def main() -> tuple[str, str, str]:
        aware = datetime.datetime(2024, 6, 1, 12).astimezone()
        return os.environ["TZ"], str(aware.utcoffset()), str(aware.tzname())

    tz, offset, _name = run_ok(main, timezone=-5).value or ("", "", "")
    assert tz == "Etc/GMT+5"
    if os.path.exists("/usr/share/zoneinfo/Etc/GMT+5"):
        assert offset == "-1 day, 19:00:00"
    assert run_ok(main).value is not None
    assert (run_ok(main).value or ("",))[0] == "UTC"
    assert (run_ok(main, timezone=5.5).value or ("",))[0] == "Asia/Kolkata"
    assert (run_ok(main, timezone=-3.5).value or ("",))[0] == "<UTC-03:30>3:30"


def test_tz_variable_is_restored() -> None:
    before = os.environ.get("TZ")
    run_ok(local_views, timezone=2)
    assert os.environ.get("TZ") == before


def test_tzlocal_caches_are_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = types.ModuleType("tzlocal.unix")
    fake._cache_tz = "Host/Zone"  # type: ignore[attr-defined]
    fake._cache_tz_name = "Host/Zone"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "tzlocal.unix", fake)

    async def main() -> object:
        seen = fake._cache_tz
        fake._cache_tz = "Simulated"  # type: ignore[attr-defined]
        return seen

    assert run_ok(main, timezone=1).value is None
    assert fake._cache_tz is None


needs_zoneinfo = pytest.mark.skipif(
    not os.path.exists("/usr/share/zoneinfo/America/New_York"), reason="no zone database"
)


@needs_zoneinfo
def test_a_named_zone_follows_daylight_saving() -> None:
    # 2024-03-10 06:59 UTC is 01:59 EST; a minute later clocks jump to 03:00 EDT.
    spring = 1_710_053_940.0
    # 2024-11-03 05:30 UTC is 01:30 EDT; an hour later it is 01:30 EST again.
    autumn = 1_730_611_800.0

    async def main() -> list[str]:
        out = []
        for moment in (spring, spring + 60, autumn, autumn + 3600):
            local = time.localtime(moment)
            out.append(time.strftime("%H:%M %Z", local) + f" dst={local.tm_isdst}")
            out.append(datetime.datetime.fromtimestamp(moment).strftime("%H:%M"))
        out.append(str(time.mktime(time.localtime(autumn))))
        out.append(str(time.mktime(time.localtime(autumn + 3600))))
        out.append(f"{time.timezone} {time.altzone} {time.daylight} {time.tzname}")
        out.append(os.environ["TZ"])
        return out

    assert run_ok(main, timezone="America/New_York").value == [
        "01:59 EST dst=0",
        "01:59",
        "03:00 EDT dst=1",
        "03:00",
        "01:30 EDT dst=1",
        "01:30",
        "01:30 EST dst=0",
        "01:30",
        str(autumn),
        str(autumn + 3600),
        "18000 14400 1 ('EST', 'EDT')",
        "America/New_York",
    ]


def test_unknown_zone_names_are_rejected() -> None:
    with pytest.raises(ValueError, match="unknown timezone"):
        simloop.run(local_views, config=simloop.SimConfig(timezone="Mars/Olympus_Mons"))
