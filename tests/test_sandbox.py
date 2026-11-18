"""Virtualized globals and leak detection."""

from __future__ import annotations

import _socket
import asyncio
import datetime
import os
import random
import secrets
import socket
import subprocess
import threading
import time
import uuid

import simloop

from .helpers import run_ok


def test_time_functions_read_the_simulated_clock() -> None:
    async def main() -> tuple[float, float, float, int]:
        start_wall, start_mono = time.time(), time.monotonic()
        await asyncio.sleep(90)
        return (
            time.time() - start_wall,
            time.monotonic() - start_mono,
            time.perf_counter(),
            time.time_ns(),
        )

    wall, mono, perf, wall_ns = run_ok(main).value or (0, 0, 0, 0)
    assert wall == 90
    assert mono == 90
    assert perf == 90
    assert wall_ns == (simloop.SimConfig().epoch + 90) * 1_000_000_000


def test_wall_clock_starts_at_the_configured_epoch() -> None:
    async def main() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    assert run_ok(main).value == "2024-01-01 00:00:00"
    assert run_ok(main, epoch=0.0).value == "1970-01-01 00:00:00"


def test_blocking_sleep_stalls_the_whole_loop() -> None:
    async def main() -> list[tuple[str, float]]:
        events: list[tuple[str, float]] = []

        async def ticker() -> None:
            for _ in range(3):
                await asyncio.sleep(1)
                events.append(("tick", simloop.now()))

        task = asyncio.create_task(ticker())
        await asyncio.sleep(0.5)
        time.sleep(5)
        events.append(("unblocked", simloop.now()))
        await task
        return events

    result = run_ok(main, scheduler="fifo")
    assert result.value == [("unblocked", 5.5), ("tick", 5.5), ("tick", 6.5), ("tick", 7.5)]
    assert any(e.kind == "blocked" for e in result.trace)


def test_random_module_is_seeded_and_restored() -> None:
    random.seed(1234)
    expected_after = random.Random(1234).random()

    async def main() -> list[float]:
        values = [random.random() for _ in range(3)]
        random.seed()
        values.append(random.random())
        return values

    first = run_ok(main, seed=5).value
    assert first == run_ok(main, seed=5).value
    assert first != run_ok(main, seed=6).value
    random.seed(1234)
    run_ok(main)
    assert random.random() == expected_after


def test_os_entropy_is_seeded() -> None:
    async def main() -> tuple[str, str, str, int]:
        return (
            os.urandom(8).hex(),
            str(uuid.uuid4()),
            secrets.token_hex(8),
            random.SystemRandom().randint(0, 10**9),
        )

    first = run_ok(main, seed=1).value
    assert first == run_ok(main, seed=1).value
    assert first != run_ok(main, seed=2).value


def test_globals_are_restored_after_the_run() -> None:
    originals = (time.time, time.monotonic, time.sleep, os.urandom, random.seed)

    async def main() -> None:
        await asyncio.sleep(10)

    run_ok(main)
    assert (time.time, time.monotonic, time.sleep, os.urandom, random.seed) == originals
    assert abs(time.time() - datetime.datetime.now().timestamp()) < 5


def test_sandbox_can_be_disabled() -> None:
    async def main() -> float:
        return time.monotonic()

    assert run_ok(main, sandbox=False).value != 0.0


def _assert_leak(main, fragment: str) -> None:
    result = simloop.run(main)
    assert result.failure is not None
    assert result.failure.kind == "nondeterminism", result.failure
    assert fragment in result.failure.message
    assert any(e.kind == "leak" for e in result.trace)


def test_real_socket_is_a_leak_even_if_the_error_is_swallowed() -> None:
    async def main() -> str:
        try:
            _socket.socket()
        except Exception:
            return "swallowed"
        return "created"

    _assert_leak(main, "socket.__new__")


def test_real_dns_is_a_leak() -> None:
    async def main() -> None:
        socket.getaddrinfo("example.com", 443)

    _assert_leak(main, "socket.getaddrinfo")


def test_threads_are_a_leak() -> None:
    async def main() -> None:
        threading.Thread(target=lambda: None).start()

    _assert_leak(main, "Thread.start")


def test_subprocesses_are_a_leak() -> None:
    async def main() -> None:
        subprocess.run(["true"], check=False)

    _assert_leak(main, "subprocess.Popen")


def test_leak_hook_is_inactive_outside_simulations() -> None:
    sock = socket.socket()
    sock.close()


def test_datetime_reads_the_simulated_wall_clock() -> None:
    async def main() -> tuple[str, str, float, bool, bool]:
        start = datetime.datetime.now()
        await asyncio.sleep(3600)
        elapsed = (datetime.datetime.now() - start).total_seconds()
        now = datetime.datetime.now(datetime.UTC)
        return (
            now.isoformat(),
            datetime.datetime.utcnow().isoformat(),
            elapsed,
            type(now) is datetime.datetime,
            isinstance(now, datetime.datetime) and isinstance(now, datetime.date),
        )

    aware, naive, elapsed, exact_type, instance = run_ok(main).value or ("", "", 0.0, False, False)
    assert aware == "2024-01-01T01:00:00+00:00"
    assert naive == "2024-01-01T01:00:00"
    assert elapsed == 3600
    assert exact_type is False  # inside the run the name is the stand-in
    assert instance


def test_datetime_values_are_real_datetimes() -> None:
    real = datetime.datetime

    async def main() -> object:
        built = datetime.datetime(2024, 5, 1, tzinfo=datetime.UTC)
        return (
            built,
            datetime.datetime.now(),
            datetime.date.today(),
            datetime.datetime.fromtimestamp(0),
        )

    values = run_ok(main).value
    assert isinstance(values, tuple)
    assert [type(v) for v in values] == [real, real, datetime.date, real]
    assert datetime.datetime is real


def test_datetime_imported_by_name_is_simulated() -> None:
    from . import support_datetime

    async def main() -> tuple[str, str, str]:
        await asyncio.sleep(86400)
        return support_datetime.stamp()

    aware, today, kind = run_ok(main).value or ("", "", "")
    assert aware == "2024-01-02T00:00:00+00:00"
    assert today == datetime.date.fromtimestamp(simloop.SimConfig().epoch + 86400).isoformat()
    assert kind == "<class 'datetime.datetime'>"
    assert vars(support_datetime)["datetime"] is datetime.datetime


def test_datetime_follows_a_host_clock_offset() -> None:
    async def main() -> str:
        net = simloop.network()

        async def skewed() -> str:
            return datetime.datetime.now(datetime.UTC).isoformat()

        host = net.add_host("skewed")
        host.clock_offset = 30.0
        return str(await host.run(skewed()))

    assert run_ok(main).value == "2024-01-01T00:00:30+00:00"
