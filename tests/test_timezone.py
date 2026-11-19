from __future__ import annotations

import asyncio
import datetime
import os
import time
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


def test_utc_offset_sets_the_simulated_zone() -> None:
    views = run_ok(local_views, utc_offset=-5).value
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

    assert run_ok(main, utc_offset=5.5).value == (
        "2024-01-01T00:00:00",
        "2024-01-01T00:00:00+00:00",
        "00:00",
    )


def test_fractional_offsets_are_named() -> None:
    async def main() -> str:
        return time.strftime("%H:%M %z %Z")

    assert run_ok(main, utc_offset=5.5).value == "05:30 +0530 UTC+05:30"


def test_local_time_functions_are_restored() -> None:
    before = (time.localtime, time.mktime, time.timezone, time.tzname)
    run_ok(local_views, utc_offset=3)
    assert (time.localtime, time.mktime, time.timezone, time.tzname) == before


def test_offset_is_validated() -> None:
    with pytest.raises(ValueError, match="utc_offset"):
        simloop.run(local_views, config=simloop.SimConfig(utc_offset=30))


def test_same_seed_same_trace_with_an_offset() -> None:
    first = run_ok(local_views, seed=4, utc_offset=-3)
    second = run_ok(local_views, seed=4, utc_offset=-3)
    assert first.digest == second.digest
    assert first.digest != run_ok(local_views, seed=4).digest
