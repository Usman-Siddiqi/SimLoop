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
