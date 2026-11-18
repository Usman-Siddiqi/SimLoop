"""asyncstdlib 3.14.0: cancelling one ``tee`` peer silently ends the others.

``tee`` peers share one source iterator. The most advanced peer pulls the
next item by awaiting the source's ``__anext__`` in its own task. If that
peer is cancelled there (a timeout, a ``TaskGroup`` abort), the
``CancelledError`` is thrown into the source async generator, which finishes
and is closed. Every other peer then gets ``StopAsyncIteration`` and its
``async for`` ends normally, as if the stream were complete, although items
were never delivered. No error reaches the peers that were not cancelled.

``tee`` documents that each child provides "the same items in the same
order"; it does not discuss cancellation. This is also how a bare async
generator behaves when ``anext()`` on it is cancelled, so it is filed as a
suspected issue. A ``tee`` could avoid it by fetching from the source in a
task the peers do not own.

With ``lock=asyncio.Lock()`` and no cancellation every peer saw every item
in order (clean).

Run: python findings/asyncstdlib_tee_cancel.py
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator

import asyncstdlib

import simloop


async def concurrent_peers() -> None:
    """Peers consumed concurrently through a lock all see every item."""
    rng = simloop.rng()
    count = rng.randint(0, 8)

    async def source() -> AsyncIterator[int]:
        for i in range(count):
            await asyncio.sleep(rng.choice([0, rng.uniform(0, 0.01)]))
            yield i

    async def consume(peer: AsyncIterator[int]) -> list[int]:
        got = []
        async for item in peer:
            got.append(item)
            await asyncio.sleep(rng.choice([0, rng.uniform(0, 0.01)]))
        return got

    peers = asyncstdlib.tee(source(), n=rng.randint(1, 4), lock=asyncio.Lock())
    results = await asyncio.gather(*(consume(peer) for peer in peers))
    assert all(result == list(range(count)) for result in results), results


async def one_peer_times_out() -> None:
    """A peer that is not cancelled still sees every item."""
    rng = simloop.rng()

    async def source() -> AsyncIterator[int]:
        for i in range(6):
            await asyncio.sleep(0.01)
            yield i

    async def consume(peer: AsyncIterator[int], timeout: float | None) -> list[int]:
        got = []
        try:
            async with asyncio.timeout(timeout):
                async for item in peer:
                    got.append(item)
        except TimeoutError:
            pass
        return got

    first, second = asyncstdlib.tee(source(), n=2, lock=asyncio.Lock())
    early, full = await asyncio.gather(consume(first, rng.uniform(0, 0.08)), consume(second, None))
    assert full == list(range(6)), f"timed-out peer got {early}, the other got only {full}"


async def plain() -> list[int]:
    async def source() -> AsyncIterator[int]:
        for i in range(6):
            await asyncio.sleep(0.01)
            yield i

    first, second = asyncstdlib.tee(source(), n=2, lock=asyncio.Lock())

    async def impatient() -> None:
        try:
            async with asyncio.timeout(0.025):
                async for _ in first:
                    pass
        except TimeoutError:
            pass

    async def patient() -> list[int]:
        await asyncio.sleep(0.001)  # let the impatient peer lead
        return [item async for item in second]

    _, got = await asyncio.gather(impatient(), patient())
    return got


def main() -> int:
    for scheduler in ("random", "pct", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(concurrent_peers, seeds=range(1000), config=config)
        print(f"concurrent_peers {scheduler}: {report.summary().splitlines()[0]}")
        assert report.ok, report.summary()
    fifo = simloop.SimConfig(scheduler="fifo")
    report = simloop.explore(one_peer_times_out, seeds=range(1000), config=fifo)
    print(f"one_peer_times_out fifo: {report.summary()}")
    assert not report.ok
    got = asyncio.run(plain())
    print(f"real asyncio loop: the peer that was not cancelled got {got} of [0..5]")
    assert got != list(range(6))
    return 0


if __name__ == "__main__":
    sys.exit(main())
