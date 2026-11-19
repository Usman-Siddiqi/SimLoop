"""Circuit breakers: cancelled calls close the circuit, mixed local/UTC time, growing tracebacks.

One model-based harness runs four async circuit-breaker libraries. Four
workers call a backend through the breaker, each call under a random
``wait_for`` timeout; the backend switches between failing fast, hanging
and working. A model counts consecutive failed calls (a cancelled call
counts as neither failure nor success) and checks that:

* once ``threshold`` calls in a row have failed, calls in the next half of
  the recovery timeout do not reach the backend;
* a rejection reports a time to reopen between 0 and the recovery timeout;
* the exception raised for a rejection has a short traceback;
* once the backend works again, calls succeed within a few recovery timeouts.

Each library runs in three simulated time zones (``SimConfig.timezone``).

Findings:

1. circuitbreaker 2.1.3 and aiocircuitbreaker 2.0.0 record a cancelled call
   as a success: ``__exit__`` resets the breaker for any exception that is
   not a failure, and ``CancelledError`` is not an ``Exception``. A caller's
   timeout around a guarded call therefore wipes the failure count, and a
   half-open trial call that times out closes the circuit.
2. aiobreaker 1.2.0 stores the opening time with ``datetime.utcnow()`` but
   computes the reopen time against ``datetime.now()``: west of UTC the
   reported time to reopen is hours too long (and ``sleep_until_open``
   sleeps that long); east of UTC the reopen time is ``None`` and
   ``CircuitBreakerError.time_remaining`` raises ``TypeError``.
3. purgatory 3.0.2 raises the same ``OpenedState`` instance for every
   rejected call while the circuit is open, so its traceback grows by five
   frames per rejection, holding every frame alive.

Run: python findings/circuit_breakers.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import aiobreaker
import aiocircuitbreaker
import circuitbreaker
from purgatory import AsyncCircuitBreakerFactory
from purgatory.domain.model import OpenedState

import simloop

THRESHOLD = 3
RECOVERY = 1  # seconds; the libraries that take an int need a whole number
ZONES = (0, -5, 1)
MUST_REJECT = "reached the backend although"
BAD_REOPEN = "reported a time to reopen of"
LONG_TRACEBACK = "a rejection's traceback has"
NEVER_RECOVERED = "no call succeeded"


class Rejected(Exception):
    def __init__(self, remaining: float | None, original: BaseException) -> None:
        self.remaining = remaining
        self.original = original


Guard = Callable[[Callable[[], Awaitable[str]]], Awaitable[str]]


def remaining_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value.total_seconds()
    return float(value)


def fabfuel_like(module: Any) -> Guard:
    """circuitbreaker and its fork aiocircuitbreaker share an API."""
    breaker = module.CircuitBreaker(
        failure_threshold=THRESHOLD, recovery_timeout=RECOVERY, expected_exception=ConnectionError
    )

    async def guard(func: Callable[[], Awaitable[str]]) -> str:
        try:
            return await breaker.decorate(func)()
        except module.CircuitBreakerError as exc:
            raise Rejected(breaker.open_remaining, exc) from None

    return guard


def make_aiobreaker() -> Guard:
    breaker = aiobreaker.CircuitBreaker(
        fail_max=THRESHOLD, timeout_duration=timedelta(seconds=RECOVERY)
    )

    async def guard(func: Callable[[], Awaitable[str]]) -> str:
        try:
            return await breaker.call_async(func)
        except aiobreaker.CircuitBreakerError as exc:
            if exc.__cause__ is not None:
                raise exc.__cause__ from None  # the call failed and opened the circuit
            raise Rejected(remaining_seconds(exc.time_remaining), exc) from None

    return guard


def make_purgatory() -> Guard:
    factory = AsyncCircuitBreakerFactory(default_threshold=THRESHOLD, default_ttl=RECOVERY)

    async def guard(func: Callable[[], Awaitable[str]]) -> str:
        try:
            async with await factory.get_breaker("backend"):
                return await func()
        except OpenedState as exc:
            raise Rejected(exc.opened_at + RECOVERY - time.time(), exc) from None

    return guard


LIBRARIES: dict[str, Callable[[], Guard]] = {
    "circuitbreaker": lambda: fabfuel_like(circuitbreaker),
    "aiocircuitbreaker": lambda: fabfuel_like(aiocircuitbreaker),
    "aiobreaker": make_aiobreaker,
    "purgatory": make_purgatory,
}


def traceback_depth(exc: BaseException) -> int:
    depth, tb = 0, exc.__traceback__
    while tb is not None:
        depth += 1
        tb = tb.tb_next
    return depth


def make_harness(library: str) -> Callable[[], Awaitable[None]]:
    async def harness() -> None:
        """The breaker opens after THRESHOLD failures in a row, reports sane times, recovers."""
        rng = simloop.rng()
        loop = asyncio.get_running_loop()
        guard = LIBRARIES[library]()
        mode = "down"
        consecutive = 0
        must_reject_until = -1.0

        async def phases() -> None:
            nonlocal mode
            while True:
                await asyncio.sleep(rng.uniform(0.3, 2.0))
                mode = rng.choice(["down", "down", "hang", "up"])

        async def call(token: str) -> str:
            nonlocal consecutive, must_reject_until
            now = loop.time()
            assert now >= must_reject_until, (
                f"{token} {MUST_REJECT} {THRESHOLD} calls in a row had just failed"
            )
            await asyncio.sleep(rng.uniform(0.001, 0.01))
            if mode == "hang":
                await asyncio.sleep(10)
            if mode != "up":
                consecutive += 1
                if consecutive >= THRESHOLD:
                    must_reject_until = max(must_reject_until, loop.time() + RECOVERY / 2)
                raise ConnectionError("backend down")
            consecutive = 0
            must_reject_until = -1.0
            return token

        async def worker(n: int) -> None:
            for i in range(30):
                token = f"w{n}-{i}"

                async def attempt(token: str = token) -> str:
                    return await call(token)

                try:
                    got = await asyncio.wait_for(guard(attempt), rng.uniform(0.005, 0.1))
                    assert got == token, (got, token)
                except Rejected as exc:
                    depth = traceback_depth(exc.original)
                    assert depth < 50, f"{LONG_TRACEBACK} {depth} frames"
                    remaining = exc.remaining
                    reopen = f"{library} {BAD_REOPEN} {remaining} s (recovery timeout {RECOVERY} s)"
                    assert remaining is not None, reopen
                    assert -1 <= remaining <= RECOVERY + 1, reopen
                except (ConnectionError, TimeoutError):
                    pass
                await asyncio.sleep(rng.uniform(0, 0.05))

        switcher = asyncio.create_task(phases())
        await asyncio.gather(*(worker(n) for n in range(4)))
        switcher.cancel()
        mode = "up"

        async def final() -> str:
            return await call("final")

        for _ in range(40):
            try:
                await asyncio.wait_for(guard(final), 1)
                return
            except (Rejected, ConnectionError, TimeoutError):
                await asyncio.sleep(0.1)
        raise AssertionError(f"{NEVER_RECOVERED} within 4 s after the backend recovered")

    harness.__name__ = f"harness_{library}"
    return harness


def classify(report: simloop.ExploreReport) -> set[str]:
    found = set()
    for group in report.groups:
        message = group.message
        if MUST_REJECT in message:
            found.add("cancelled call counted as success")
        elif BAD_REOPEN in message or "NoneType" in message:
            found.add("reopen time")
        elif LONG_TRACEBACK in message:
            found.add("growing traceback")
        else:
            found.add(message.splitlines()[0])
    return found


EXPECTED = {
    "circuitbreaker": {0: {"cancelled call counted as success"}},
    "aiocircuitbreaker": {0: {"cancelled call counted as success"}},
    "aiobreaker": {0: set(), -5: {"reopen time"}, 1: {"reopen time"}},
    "purgatory": {0: {"growing traceback"}},
}


async def plain_cancelled_trial(module: Any) -> list[str]:
    breaker = module.CircuitBreaker(failure_threshold=2, recovery_timeout=1)

    async def failing() -> None:
        raise ConnectionError("backend down")

    async def hanging() -> None:
        await asyncio.sleep(10)

    states = []
    for _ in range(2):
        with contextlib.suppress(ConnectionError):
            await breaker.decorate(failing)()
    states.append(str(breaker.state))
    await asyncio.sleep(1.1)  # the recovery timeout passes: half-open
    states.append(str(breaker.state))
    with contextlib.suppress(TimeoutError):  # the half-open trial call times out
        await asyncio.wait_for(breaker.decorate(hanging)(), 0.05)
    states.append(str(breaker.state))
    return states


def main() -> int:
    for library, expected_by_zone in EXPECTED.items():
        for zone in ZONES:
            config = simloop.SimConfig(timezone=zone)
            report = simloop.explore(make_harness(library), seeds=range(100), config=config)
            print(f"{library}, timezone={zone:+d}: {report.summary()}")
            found = classify(report)
            expected = expected_by_zone.get(zone, expected_by_zone[0])
            assert found == expected, (library, zone, found)
    for module in (circuitbreaker, aiocircuitbreaker):
        states = asyncio.run(plain_cancelled_trial(module))
        print(f"real asyncio loop, {module.__name__}: after 2 failures, after the recovery")
        print(f"  timeout, after the half-open trial call timed out: {states}")
        assert states[0].lower().endswith("open")
        assert states[-1].lower().endswith("closed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
