"""taskiq 0.13.0: the worker's ``Receiver`` under shutdown and task timeouts (clean).

A broker backed by an ``asyncio.Queue`` delivers 30 queued tasks (0-200 ms
each, some with a 10-100 ms ``timeout`` label) as ackable messages to a
``Receiver(max_async_tasks=3, max_prefetch=0, 1 or 3)``, and the finish
event is set at a random time. Checked: at most ``max_async_tasks`` tasks
run at once, at most ``max_async_tasks + max_prefetch`` deliveries are
unacknowledged at any time, no task runs twice and no message is acked
twice, every task that finished had its message acked, and without
``wait_tasks_timeout`` ``listen()`` returns only after every running task
has finished. All held, random and fifo.

Run: python findings/taskiq_receiver.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections import Counter
from collections.abc import AsyncGenerator, Callable, Coroutine
from typing import Any

from taskiq import AckableMessage, AsyncBroker, BrokerMessage
from taskiq.receiver import Receiver

import simloop

logging.getLogger("taskiq").setLevel(logging.CRITICAL)
MAX_ASYNC = 3


class QueueBroker(AsyncBroker):
    def __init__(self) -> None:
        super().__init__()
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()
        self.acks: Counter[bytes] = Counter()
        self.delivered = 0
        self.capacity = 0

    async def kick(self, message: BrokerMessage) -> None:
        await self.queue.put(message.message)

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        while True:
            data = await self.queue.get()
            self.delivered += 1
            # A delivery's slot is freed after its ack, so this undercounts.
            held = self.delivered - sum(self.acks.values())
            assert held <= self.capacity, f"{held} deliveries unacked, capacity {self.capacity}"

            async def ack(data: bytes = data) -> None:
                await asyncio.sleep(0.001)
                self.acks[data] += 1

            yield AckableMessage(data=data, ack=ack)


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Limits hold, nothing runs or acks twice, and a graceful stop waits."""
        rng = simloop.rng()
        broker = QueueBroker()
        broker.is_worker_process = True
        running: set[int] = set()
        peak = 0
        started: Counter[int] = Counter()
        finished: Counter[int] = Counter()

        @broker.task(task_name="work")
        async def work(i: int, delay: float) -> int:
            nonlocal peak
            started[i] += 1
            running.add(i)
            peak = max(peak, len(running))
            try:
                await asyncio.sleep(delay)
            finally:
                running.discard(i)
            finished[i] += 1
            return i

        for i in range(30):
            kicker = work.kicker()
            if rng.random() < knobs.get("timeout", 0):
                kicker = kicker.with_labels(timeout=rng.uniform(0.01, 0.1))
            await kicker.kiq(i, rng.uniform(0, 0.2))

        prefetch = rng.choice([0, 1, 3])
        broker.capacity = MAX_ASYNC + prefetch
        receiver = Receiver(
            broker,
            max_async_tasks=MAX_ASYNC,
            max_prefetch=prefetch,
            wait_tasks_timeout=knobs.get("wait"),
        )
        finish = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.call_later(rng.uniform(0.05, 2.5), finish.set)
        await asyncio.wait_for(receiver.listen(finish), 30)
        if knobs.get("wait") is None:
            assert not running, f"listen() returned while tasks {sorted(running)} still ran"
        await asyncio.sleep(1)
        assert peak <= MAX_ASYNC, f"{peak} tasks ran at once, max_async_tasks={MAX_ASYNC}"
        assert max(started.values(), default=0) <= 1, f"a task ran twice: {started}"
        assert max(broker.acks.values(), default=0) <= 1, "a message was acked twice"
        acked, done = sum(broker.acks.values()), sum(finished.values())
        assert acked >= done, f"{done} tasks finished but only {acked} messages were acked"

    return harness


WORKLOADS = {
    "graceful stop": {},
    "task timeouts": {"timeout": 0.3},
    "wait_tasks_timeout=0.05": {"wait": 0.05},
}


def main() -> int:
    for workload, knobs in WORKLOADS.items():
        for scheduler in ("random", "fifo"):
            config = simloop.SimConfig(scheduler=scheduler)
            if scheduler == "fifo":
                config = config.replace(timer_jitter=0.001)
            report = simloop.explore(make_harness(knobs), seeds=range(200), config=config)
            print(f"{workload}, {scheduler}: {report.summary().splitlines()[0]}")
            assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
