"""broadcaster 0.3.1: two tasks subscribing to a new channel at once lose one of them.

``Broadcast("redis-stream://...")`` against the simulated Redis server in
``_resp.py``. A publisher writes a numbered message to channels ``a`` and
``b`` every 5 ms. Subscribers repeatedly enter ``broadcast.subscribe(ch)``,
wait for a message published after they joined, and leave. A third
subscriber stays on channel ``keep`` throughout, so the stream backend never
runs out of streams (the separate, known bug in encode/broadcaster#149).
Checked: every subscriber gets a fresh message within 1 s, messages arrive
in order, nobody gets an exception, and the listener task is still alive.

With one subscriber per channel it passes. With several sharing a channel it
fails: ``Broadcast.subscribe`` checks ``if not self._subscribers.get(channel)``,
awaits ``backend.subscribe(channel)`` and only then assigns
``self._subscribers[channel] = {queue}``. A second task that arrives during
that await sees no subscribers too, subscribes again, and its assignment
replaces the first task's set. The first subscriber then never receives
anything, and whichever leaves second gets ``KeyError`` from the
``finally`` block (``remove`` on a set that no longer holds its queue, or on
a channel the other one already deleted). Any backend whose ``subscribe``
waits on I/O is affected: redis-stream (``XINFO STREAM``), Postgres
(``LISTEN``, where the unlocked shared connection also raises "another
operation is in progress", encode/broadcaster#22) and Kafka (waits for
partition assignment). The plain Redis backend's ``subscribe`` happens not
to suspend. Serialising the bookkeeping with a lock fixes it.

Run: python findings/broadcaster_subscribe_race.py
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

from _resp import RespServer
from broadcaster import Broadcast, Event
from broadcaster._base import Subscriber

import simloop

URL = "redis-stream://redis:6379?protocol=2"
WORKLOADS: dict[str, tuple[str, ...]] = {
    "one subscriber per channel": ("a", "b"),
    "subscribers sharing channels": ("a", "a", "a", "b", "b", "b"),
}
FAILING = ("subscribers sharing channels",)


def make_harness(channels: tuple[str, ...]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Every subscriber hears what is published while it is subscribed, and leaves cleanly."""
        net = simloop.network()
        net.add_host("redis", entry=RespServer().serve)
        net.set_link("main", "redis", latency=(0.0005, 0.003))
        await asyncio.sleep(0.01)
        rng = simloop.rng()
        broadcast = Broadcast(URL)
        await broadcast.connect()
        sent = {"a": 0, "b": 0}
        failures: list[str] = []
        stop = asyncio.Event()

        async def publisher() -> None:
            for n in itertools.count(1):
                for channel in sent:
                    sent[channel] = n
                    await broadcast.publish(channel, str(n))
                await asyncio.sleep(0.005)

        async def keeper() -> None:
            async with broadcast.subscribe("keep"):
                await stop.wait()

        async def subscriber(name: str, channel: str) -> None:
            for round_ in range(4):
                await asyncio.sleep(rng.uniform(0, 0.02))
                try:
                    async with broadcast.subscribe(channel) as sub:
                        joined, last = sent[channel], 0
                        while last <= joined:
                            event = await asyncio.wait_for(sub.get(), 1)
                            assert event.channel == channel, f"{name} got {event}"
                            n = int(event.message)
                            assert n > last, f"{name} got {n} after {last}"
                            last = n
                except Exception as exc:
                    failures.append(f"{name} round {round_} on {channel!r}: {exc!r}")

        keeping = asyncio.create_task(keeper())
        await asyncio.sleep(0.01)
        publishing = asyncio.create_task(publisher())
        names = [f"s{i}" for i in range(len(channels))]
        await asyncio.gather(*(subscriber(n, c) for n, c in zip(names, channels, strict=True)))
        stop.set()
        await keeping
        publishing.cancel()
        alive = not broadcast._listener_task.done()
        assert not failures, f"{len(failures)} subscription(s) failed: {failures[:3]}"
        assert alive, f"the listener task died: {broadcast._listener_task!r}"
        await broadcast.disconnect()

    return harness


def fix_subscribe() -> Callable[[], None]:
    """Hold one lock while the subscriber sets and the backend's subscriptions change."""
    original = Broadcast.subscribe

    @contextlib.asynccontextmanager
    async def subscribe(self: Broadcast, channel: str) -> AsyncIterator[Subscriber]:
        lock = self.__dict__.setdefault("_bookkeeping", asyncio.Lock())
        queue: asyncio.Queue[Event | None] = asyncio.Queue()
        async with lock:
            if not self._subscribers.get(channel):
                await self._backend.subscribe(channel)
                self._subscribers[channel] = {queue}
            else:
                self._subscribers[channel].add(queue)
        try:
            yield Subscriber(queue)
        finally:
            async with lock:
                self._subscribers[channel].discard(queue)
                if not self._subscribers[channel]:
                    del self._subscribers[channel]
                    await self._backend.unsubscribe(channel)
            await queue.put(None)

    Broadcast.subscribe = subscribe  # type: ignore[method-assign,assignment]

    def undo() -> None:
        Broadcast.subscribe = original  # type: ignore[method-assign]

    return undo


async def plain() -> dict[str, str]:
    """On a real loop: two tasks subscribe to one channel at the same moment."""
    listener = await asyncio.start_server(RespServer(delay=0.01).handle, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    broadcast = Broadcast(f"redis-stream://127.0.0.1:{port}?protocol=2")
    await broadcast.connect()
    outcome: dict[str, str] = {}
    joined = asyncio.Barrier(3)

    async def subscriber(name: str) -> None:
        try:
            async with broadcast.subscribe("news") as sub:
                await joined.wait()
                event = await asyncio.wait_for(sub.get(), 0.5)
                outcome[name] = f"got {event.message!r}"
        except Exception as exc:
            outcome[name] = f"{outcome.get(name, 'got nothing')}, then {exc!r}"

    async def keeper() -> None:
        async with broadcast.subscribe("keep"):
            await asyncio.sleep(2)

    keeping = asyncio.create_task(keeper())
    await asyncio.sleep(0.1)
    tasks = [asyncio.create_task(subscriber(name)) for name in ("first", "second")]
    await joined.wait()
    await broadcast.publish("news", "hello")
    await asyncio.gather(*tasks)
    keeping.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await keeping
    listener.close()
    return dict(sorted(outcome.items()))


def main() -> int:
    configs = [("random", simloop.SimConfig(strict_cancellation=True))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, channels in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(channels), seeds=range(50), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups
            for group in report.groups:
                assert "subscription(s) failed" in group.message, group.message
    undo = fix_subscribe()
    try:
        for workload in FAILING:
            for name, config in configs:
                harness = make_harness(WORKLOADS[workload])
                fixed = simloop.explore(harness, seeds=range(50), config=config)
                print(f"{workload}, {name}, subscribe fixed: {fixed.summary().splitlines()[0]}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    outcome = asyncio.run(plain())
    print(f"real asyncio loop, two concurrent first subscribers: {outcome}")
    assert sum(text.startswith("got 'hello'") for text in outcome.values()) < 2, outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
