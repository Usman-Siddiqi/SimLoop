"""Cron schedules across daylight-saving changes: aiocron 2.1 and APScheduler 3.11.3.

Each run simulates five hours in ``America/New_York`` from midnight on a
day the clocks change (spring forward 2024-03-10, fall back 2024-11-03),
with timer jitter. Checked:

* ``*/15 * * * *`` fires every 15 minutes of real time through the gap and
  through the repeated hour, always at second 0;
* a daily ``30 1 * * *`` job fires once (aiocron; APScheduler documents that
  such a job runs twice on the night the clocks go back, and it does);
* a daily job in the skipped hour (``30 2 * * *``) still fires that day.

Both ran clean.

Run: python findings/cron_dst.py
"""

from __future__ import annotations

import asyncio
import itertools
import sys
import time
from collections.abc import Awaitable, Callable

import aiocron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import simloop

SPRING = 1_710_046_800.0  # 2024-03-10 00:00 EST
AUTUMN = 1_730_606_400.0  # 2024-11-03 00:00 EDT
HOURS = 5


def start(library: str, spec: str, job: Callable[[], Awaitable[None]]) -> Callable[[], None]:
    if library == "aiocron":
        cron = aiocron.crontab(spec, func=job, start=True)
        return cron.stop
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job, CronTrigger.from_crontab(spec))
    scheduler.start()
    return lambda: scheduler.shutdown(wait=False)


def make_harness(library: str) -> Callable[[], Awaitable[None]]:
    async def harness() -> None:
        """Cron jobs fire on schedule through a daylight-saving change."""
        fired: dict[str, list[float]] = {"*/15 * * * *": [], "30 1 * * *": [], "30 2 * * *": []}
        stops = []
        for spec, times in fired.items():

            async def job(times: list[float] = times) -> None:
                times.append(time.time())

            stops.append(start(library, spec, job))
        await asyncio.sleep(HOURS * 3600)
        for stop in stops:
            stop()
        quarter = fired["*/15 * * * *"]
        assert all(round(t) % 60 == 0 for t in quarter), "a */15 job fired off the minute"
        gaps = {round(b - a) for a, b in itertools.pairwise(quarter)}
        assert gaps == {900}, f"*/15 gaps were {sorted(gaps)} s"
        assert len(quarter) == HOURS * 4, f"*/15 fired {len(quarter)} times in {HOURS} h"
        autumn = time.time() > AUTUMN
        twice = library == "apscheduler" and autumn  # documented by APScheduler
        assert len(fired["30 1 * * *"]) == (2 if twice else 1), fired["30 1 * * *"]
        assert len(fired["30 2 * * *"]) == 1, fired["30 2 * * *"]

    return harness


def main() -> int:
    for library in ("aiocron", "apscheduler"):
        for epoch, label in ((SPRING, "spring forward"), (AUTUMN, "fall back")):
            config = simloop.SimConfig(
                timezone="America/New_York",
                epoch=epoch,
                start_time=7.0,
                scheduler="fifo",
                timer_jitter=0.01,
            )
            report = simloop.explore(make_harness(library), seeds=range(20), config=config)
            print(f"{library}, {label}: {report.summary()}")
            assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
