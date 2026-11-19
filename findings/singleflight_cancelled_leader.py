"""singleflight 0.1.3: a cancelled leader poisons its key with ``None``.

``SingleFlightAsync.call(fn, key)`` runs ``fn`` in the first caller (the
leader) and makes concurrent callers for the same key wait on an event. The
leader catches ``Exception`` only. When it is cancelled while ``fn`` runs
(a timeout, a ``TaskGroup`` abort), ``CancelledError`` skips the handler,
the ``finally`` sets the event with ``res`` and ``err`` still ``None``, and
the exception then skips the code that deletes the key. Every concurrent
caller returns ``None`` as if it were ``fn``'s result, and the key stays in
the map with its event set, so every later call for that key returns
``None`` at once without calling ``fn``.

Run: python findings/singleflight_cancelled_leader.py
"""

from __future__ import annotations

import asyncio
import contextlib
import sys

from singleflight.asynchronous import SingleFlightAsync

import simloop


async def harness() -> None:
    """Callers that are not cancelled get ``fn``'s result, now and later."""
    rng = simloop.rng()
    flight = SingleFlightAsync()

    async def load() -> str:
        await asyncio.sleep(rng.uniform(0.01, 0.1))
        return "value"

    async def caller() -> None:
        await asyncio.sleep(rng.uniform(0, 0.02))
        with contextlib.suppress(TimeoutError):
            got = await asyncio.wait_for(flight.call(load, "k"), rng.choice([None, 0.05]))
            assert got == "value", f"a caller got {got!r}"

    await asyncio.gather(*(caller() for _ in range(rng.randint(1, 4))))
    later = await asyncio.wait_for(flight.call(load, "k"), 1)
    assert later == "value", f"a later call got {later!r}"


async def plain() -> tuple[object, object]:
    flight = SingleFlightAsync()

    async def load() -> str:
        await asyncio.sleep(0.1)
        return "value"

    leader = asyncio.create_task(asyncio.wait_for(flight.call(load, "k"), 0.01))
    await asyncio.sleep(0)
    follower = asyncio.create_task(flight.call(load, "k"))
    with contextlib.suppress(TimeoutError):
        await leader
    concurrent = await follower
    later = await asyncio.wait_for(flight.call(load, "k"), 1)
    return concurrent, later


def main() -> int:
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        report = simloop.explore(harness, seeds=range(1000), config=config)
        print(f"{scheduler}: {report.summary()}")
        assert not report.ok
    concurrent, later = asyncio.run(plain())
    print(f"real asyncio loop: concurrent caller got {concurrent!r}, a later call got {later!r}")
    assert concurrent is None
    assert later is None
    return 0


if __name__ == "__main__":
    sys.exit(main())
