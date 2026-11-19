"""cashews 7.6.0: one caller's timeout cancels every concurrent caller.

``@cache(...)`` (and ``@cache.early``/``@cache.soft``) default to
``protected=True``, which wraps the function in ``thunder_protection``: the
first caller for a key starts the call as a task and every concurrent caller
for the same key awaits that same task. Awaiting a task directly ties it to
the awaiting caller: when that caller is cancelled (a timeout, a client
disconnect, a ``TaskGroup`` abort), asyncio cancels the awaited task too.
Every other caller waiting on it then gets ``CancelledError``, although
nothing cancelled them, and the value is not cached.

SimLoop finds it with the generic ``SimConfig(strict_cancellation=True)``
check: a task ends with ``CancelledError`` that came from another task's
cancellation, while nothing called ``cancel()`` on it. Shielding the shared
task (``asyncio.shield``) for the waiters would keep one caller's
cancellation to itself.

Run: python findings/cashews_shared_cancel.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

from cashews import Cache

import simloop


async def harness() -> None:
    """Callers that are not cancelled get the value."""
    rng = simloop.rng()
    cache = Cache()
    cache.setup("mem://")
    calls = 0

    @cache(ttl="1m")
    async def fetch(key: str) -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(rng.uniform(0.01, 0.1))
        return f"value-{key}"

    async def caller(timeout: float | None) -> None:
        await asyncio.sleep(rng.uniform(0, 0.02))
        with contextlib.suppress(TimeoutError):
            value = await asyncio.wait_for(fetch("k"), timeout)
            assert value == "value-k"

    callers = [
        asyncio.create_task(caller(rng.choice([None, rng.uniform(0, 0.1)])))
        for _ in range(rng.randint(2, 4))
    ]
    await asyncio.gather(*callers)


async def plain() -> str:
    """Fixed timings on a real asyncio loop."""
    cache = Cache()
    cache.setup("mem://")

    @cache(ttl="1m")
    async def fetch(key: str) -> str:
        await asyncio.sleep(0.1)
        return f"value-{key}"

    impatient = asyncio.create_task(asyncio.wait_for(fetch("k"), 0.01))
    patient = asyncio.create_task(fetch("k"))
    with contextlib.suppress(TimeoutError):
        await impatient
    try:
        return f"patient caller got {await patient!r}"
    except asyncio.CancelledError:
        cancelled = patient.cancelling() > 0
        return f"patient caller got CancelledError (cancel() called on it: {cancelled})"


def main() -> int:
    config = simloop.SimConfig(strict_cancellation=True)
    report = simloop.explore(harness, seeds=range(1000), config=config)
    print(report.summary())
    assert {group.kind for group in report.groups} == {"stray_cancel"}
    fifo = simloop.explore(harness, seeds=range(1000), config=config.replace(scheduler="fifo"))
    print(f"fifo: {fifo.summary().splitlines()[0]}")
    assert not fifo.ok
    real = asyncio.run(plain())
    print(f"real asyncio loop: {real}")
    assert "CancelledError (cancel() called on it: False)" in real
    return 0


if __name__ == "__main__":
    sys.exit(main())
