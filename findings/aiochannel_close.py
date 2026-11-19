"""aiochannel 1.4.1: ``put()`` on a closed, full channel raises ``ChannelFull``.

Three producers put items into a bounded ``Channel`` (each put under a
random ``asyncio.timeout``) and stop on ``ChannelClosed``; three consumers
get items until ``ChannelClosed``; the channel is closed at a random time
while the producers are still running. Checked: every item whose ``put``
returned is consumed exactly once, consumers and ``join()`` finish, and a
producer that puts after the close gets ``ChannelClosed``.

The last property fails: ``put()`` documents "If the channel is closed or
closing, raise ChannelClosed", but its wait loop stops as soon as the
channel is closed and falls through to ``put_nowait``, which checks
``full()`` before the closed flag. A ``put`` on a closed channel that still
holds ``maxsize`` undrained items therefore raises ``ChannelFull``, which a
blocking ``put`` otherwise never raises, and a producer loop written as
``except ChannelClosed: break`` crashes. Delivery was correct on every seed
once the harness also accepts ``ChannelFull`` after a close.

Run: python findings/aiochannel_close.py
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable

from aiochannel import Channel, ChannelClosed, ChannelFull

import simloop


def make_harness(accept_full: bool) -> Callable[[], Awaitable[None]]:
    closed_errors: tuple[type[Exception], ...] = (
        (ChannelClosed, ChannelFull) if accept_full else (ChannelClosed,)
    )

    async def harness() -> None:
        """Puts after the close raise ChannelClosed; every item put is consumed once."""
        rng = simloop.rng()
        channel: Channel[tuple[int, int]] = Channel(rng.randint(1, 3))
        sent: list[tuple[int, int]] = []
        got: list[tuple[int, int]] = []

        async def producer(n: int) -> None:
            for i in range(15):
                try:
                    async with asyncio.timeout(rng.uniform(0.001, 0.05)):
                        await channel.put((n, i))
                    sent.append((n, i))
                except TimeoutError:
                    pass
                except closed_errors:
                    assert channel.closed()
                    return
                await asyncio.sleep(rng.uniform(0, 0.01))

        async def consumer() -> None:
            while True:
                try:
                    async with asyncio.timeout(rng.uniform(0.001, 0.05)):
                        item = await channel.get()
                except TimeoutError:
                    continue
                except ChannelClosed:
                    return
                got.append(item)
                await asyncio.sleep(rng.uniform(0, 0.02))

        async def closer() -> None:
            await asyncio.sleep(rng.uniform(0.01, 0.3))
            channel.close()

        consumers = [asyncio.create_task(consumer()) for _ in range(3)]
        await asyncio.gather(closer(), *(producer(n) for n in range(3)))
        await asyncio.wait_for(asyncio.gather(*consumers), 5)
        await asyncio.wait_for(channel.join(), 5)
        assert sorted(got) == sorted(sent), "consumed items differ from the items put"

    return harness


async def plain() -> str:
    channel: Channel[str] = Channel(1)
    await channel.put("undrained")
    channel.close()
    try:
        await channel.put("late")
    except Exception as exc:
        return type(exc).__name__
    return "no error"


def main() -> int:
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler, strict_cancellation=True)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        report = simloop.explore(make_harness(False), seeds=range(500), config=config)
        print(f"{scheduler}: {report.summary()}")
        assert [group.message for group in report.groups] == ["ChannelFull"]
        lenient = simloop.explore(make_harness(True), seeds=range(500), config=config)
        print(f"{scheduler}, accepting ChannelFull after the close: {lenient.summary()}")
        assert lenient.ok
    outcome = asyncio.run(plain())
    print(f"real asyncio loop: put() on a closed channel holding maxsize items raised {outcome}")
    assert outcome == "ChannelFull"
    return 0


if __name__ == "__main__":
    sys.exit(main())
