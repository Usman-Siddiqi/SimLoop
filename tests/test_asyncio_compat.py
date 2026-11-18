"""Standard asyncio code must behave the same on SimEventLoop as on a real loop."""

from __future__ import annotations

import asyncio
import contextvars
import functools
from collections.abc import AsyncIterator

import pytest

import simloop

from .helpers import run_ok

request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


async def _ordering_program() -> list[str]:
    """Exercises tasks, futures, queues and locks without timers."""
    events: list[str] = []
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=2)
    lock = asyncio.Lock()
    ready = asyncio.Event()

    async def producer() -> None:
        for i in range(5):
            await queue.put(i)
            events.append(f"put {i}")
        await queue.put(-1)

    async def consumer(name: str) -> None:
        await ready.wait()
        while True:
            item = await queue.get()
            if item < 0:
                await queue.put(-1)
                return
            async with lock:
                events.append(f"{name} got {item}")
                await asyncio.sleep(0)

    async def chatter(name: str) -> None:
        for i in range(3):
            events.append(f"{name} {i}")
            await asyncio.sleep(0)

    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    fut.add_done_callback(lambda f: events.append("cb1"))
    fut.add_done_callback(lambda f: events.append("cb2"))
    loop.call_soon(fut.set_result, None)
    tasks = [
        asyncio.create_task(producer()),
        asyncio.create_task(consumer("A")),
        asyncio.create_task(consumer("B")),
        asyncio.create_task(chatter("x")),
        asyncio.create_task(chatter("y")),
    ]
    await asyncio.sleep(0)
    ready.set()
    await asyncio.gather(*tasks)
    return events


def _append_ignoring_future(events: list[str], item: str, _fut: object) -> None:
    events.append(item)


def test_fifo_scheduler_matches_real_asyncio_order() -> None:
    expected = asyncio.run(_ordering_program())
    assert run_ok(_ordering_program, scheduler="fifo").value == expected


def test_random_scheduler_explores_other_orders() -> None:
    outcomes = {tuple(simloop.run(_ordering_program, seed=s).value or ()) for s in range(30)}
    assert len(outcomes) > 5


def test_random_scheduler_keeps_callbacks_from_one_step_in_order() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        events: list[str] = []
        fut = loop.create_future()
        for i in range(5):
            fut.add_done_callback(functools.partial(_append_ignoring_future, events, f"cb{i}"))
        for i in range(3):
            loop.call_soon(events.append, f"soon{i}")
        fut.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return events

    for seed in range(30):
        events = run_ok(main, seed=seed).value
        assert events is not None
        assert [e for e in events if e.startswith("cb")] == [f"cb{i}" for i in range(5)]
        assert [e for e in events if e.startswith("soon")] == [f"soon{i}" for i in range(3)]


def test_gather_wait_and_as_completed() -> None:
    async def work(delay: float, value: int) -> int:
        await asyncio.sleep(delay)
        return value

    async def main() -> tuple[list[int], int, list[int]]:
        gathered = list(await asyncio.gather(work(3, 1), work(1, 2), work(2, 3)))
        done, pending = await asyncio.wait(
            [asyncio.create_task(work(d, d)) for d in (5, 1, 9)],
            return_when=asyncio.FIRST_COMPLETED,
        )
        first = done.pop().result()
        for task in pending:
            task.cancel()
        order = [await f for f in asyncio.as_completed([work(2, 20), work(1, 10)])]
        return gathered, first, order

    assert run_ok(main).value == ([1, 2, 3], 1, [10, 20])


def test_gather_propagates_and_return_exceptions() -> None:
    async def boom() -> None:
        await asyncio.sleep(1)
        raise ValueError("boom")

    async def main() -> str:
        results = await asyncio.gather(boom(), asyncio.sleep(2, "ok"), return_exceptions=True)
        assert isinstance(results[0], ValueError)
        try:
            await asyncio.gather(boom(), asyncio.sleep(5))
        except ValueError:
            return "raised"
        return "not raised"

    assert run_ok(main).value == "raised"


def test_taskgroup_cancels_siblings_on_error() -> None:
    cancelled: list[str] = []

    async def sibling(name: str) -> None:
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    async def failing() -> None:
        await asyncio.sleep(1)
        raise KeyError("bad")

    async def main() -> float:
        failed_at = -1.0
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(sibling("a"))
                tg.create_task(sibling("b"))
                tg.create_task(failing())
        except* KeyError:
            failed_at = simloop.now()
        return failed_at

    assert run_ok(main, scheduler="fifo").value == 1
    assert sorted(cancelled) == ["a", "b"]


def test_cancellation_and_shield() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def inner() -> str:
            await asyncio.sleep(5)
            log.append("inner finished")
            return "inner"

        protected = asyncio.ensure_future(inner())

        async def waits() -> str:
            return await asyncio.shield(protected)

        outer = asyncio.create_task(waits())
        await asyncio.sleep(1)
        outer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await outer
        log.append(await protected)

        async def stubborn() -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                log.append("cleanup")
                raise

        task = asyncio.create_task(stubborn())
        await asyncio.sleep(0)
        task.cancel("stop")
        with pytest.raises(asyncio.CancelledError):
            await task
        log.append(f"cancelled={task.cancelled()}")
        return log

    assert run_ok(main).value == ["inner finished", "inner", "cleanup", "cancelled=True"]


def test_synchronization_primitives() -> None:
    async def main() -> tuple[int, list[int], int]:
        sem = asyncio.Semaphore(2)
        active = peak = 0

        async def limited() -> None:
            nonlocal active, peak
            async with sem:
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(1)
                active -= 1

        await asyncio.gather(*(limited() for _ in range(6)))

        cond = asyncio.Condition()
        items: list[int] = []

        async def waiter() -> int:
            async with cond:
                await cond.wait_for(lambda: len(items) >= 3)
                return sum(items)

        task = asyncio.create_task(waiter())
        for i in range(3):
            await asyncio.sleep(1)
            async with cond:
                items.append(i)
                cond.notify_all()

        barrier = asyncio.Barrier(3)
        arrivals = await asyncio.gather(*(barrier.wait() for _ in range(3)))
        return peak, sorted(arrivals), await task

    assert run_ok(main).value == (2, [0, 1, 2], 3)


def test_async_generators_are_finalized() -> None:
    closed: list[str] = []

    async def numbers() -> AsyncIterator[int]:
        try:
            for i in range(100):
                await asyncio.sleep(1)
                yield i
        finally:
            closed.append("closed")

    async def main() -> list[int]:
        taken = []
        gen = numbers()
        async for value in gen:
            taken.append(value)
            if value == 2:
                break
        return taken

    assert run_ok(main).value == [0, 1, 2]
    assert closed == ["closed"]


def test_contextvars_propagate_to_tasks_and_callbacks() -> None:
    async def main() -> list[str]:
        seen: list[str] = []
        request_id.set("req-1")

        async def child() -> None:
            seen.append(request_id.get())

        await asyncio.create_task(child())
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        def callback() -> None:
            seen.append(request_id.get())
            done.set_result(None)

        loop.call_soon(callback)
        await done
        ctx = contextvars.Context()
        ctx.run(request_id.set, "req-2")
        await asyncio.create_task(child(), context=ctx)
        return seen

    assert run_ok(main).value == ["req-1", "req-1", "req-2"]


def test_to_thread_runs_inline_and_reports_errors() -> None:
    import time

    def blocking(x: int) -> int:
        time.sleep(2)
        return x * 2

    def broken() -> None:
        raise OSError("disk on fire")

    async def main() -> tuple[int, float, str]:
        start = simloop.now()
        value = await asyncio.to_thread(blocking, 21)
        elapsed = simloop.now() - start
        try:
            await asyncio.get_running_loop().run_in_executor(None, broken)
        except OSError as exc:
            return value, elapsed, str(exc)
        return value, elapsed, "no error"

    assert run_ok(main).value == (42, 2.0, "disk on fire")


def test_task_names_are_per_run_and_deterministic() -> None:
    async def main() -> list[str]:
        tasks = [asyncio.create_task(asyncio.sleep(0)) for _ in range(3)]
        named = asyncio.create_task(asyncio.sleep(0), name="custom")
        await asyncio.gather(*tasks, named)
        current = asyncio.current_task()
        assert current is not None
        return [current.get_name(), *(t.get_name() for t in tasks), named.get_name()]

    first = run_ok(main).value
    assert first == ["main", "Task-1", "Task-2", "Task-3", "custom"]
    assert run_ok(main).value == first


def test_custom_exception_handler_receives_callback_errors() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        seen: list[str] = []
        loop.set_exception_handler(lambda _loop, ctx: seen.append(str(ctx["exception"])))

        def explode() -> None:
            raise RuntimeError("callback failed")

        loop.call_soon(explode)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return seen

    assert run_ok(main).value == ["callback failed"]


def test_unsimulated_io_fails_closed() -> None:
    async def main() -> None:
        await asyncio.open_unix_connection("/tmp/socket")

    result = simloop.run(main)
    assert result.failure is not None
    assert isinstance(result.failure.exception, simloop.UnsupportedOperation)


def test_loop_rejects_reentrant_run() -> None:
    async def main() -> str:
        loop = asyncio.get_running_loop()
        coro = asyncio.sleep(0)
        try:
            loop.run_until_complete(coro)
        except RuntimeError as exc:
            return str(exc)
        finally:
            coro.close()
        return "no error"

    assert run_ok(main).value == "This event loop is already running"
