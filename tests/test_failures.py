"""Failure detection and classification."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

import simloop


def _run(main, **config) -> simloop.RunResult:  # type: ignore[type-arg]
    return simloop.run(main, config=simloop.SimConfig(**config))


def test_exception_in_main_is_a_failure_with_location() -> None:
    async def main() -> None:
        await asyncio.sleep(1)
        raise ValueError("broken")

    result = _run(main)
    assert result.failure is not None
    assert result.failure.kind == "exception"
    assert result.failure.message == "ValueError: broken"
    assert result.failure.signature.startswith("exception:ValueError@test_failures.py:")
    assert "ValueError: broken" in result.failure.format()
    with pytest.raises(ValueError, match="broken"):
        result.raise_for_failure()


def test_successful_run() -> None:
    async def main() -> int:
        return 7

    result = _run(main)
    assert result.ok
    assert result.value == 7
    result.raise_for_failure()


def test_deadlock_lists_blocked_tasks() -> None:
    async def main() -> None:
        lock_a, lock_b = asyncio.Lock(), asyncio.Lock()

        async def one() -> None:
            async with lock_a:
                await asyncio.sleep(1)
                async with lock_b:
                    pass

        async def two() -> None:
            async with lock_b:
                await asyncio.sleep(1)
                async with lock_a:
                    pass

        await asyncio.gather(one(), two())

    result = _run(main)
    assert result.failure is not None
    assert result.failure.kind == "deadlock"
    exc = result.failure.exception
    assert isinstance(exc, simloop.DeadlockError)
    assert len(exc.blocked) == 3
    assert any(entry.endswith("main.<locals>.one") for entry in exc.blocked)
    assert any(entry.endswith("main.<locals>.two") for entry in exc.blocked)
    assert result.sim_time == 1


def test_deadlock_signature_ignores_task_names() -> None:
    async def main() -> None:
        await asyncio.Event().wait()

    first, second = _run(main), _run(main)
    assert first.failure is not None
    assert second.failure is not None
    assert first.failure.signature == second.failure.signature
    assert "Task-" not in first.failure.signature


def test_step_limit_catches_livelock() -> None:
    async def main() -> None:
        while True:
            await asyncio.sleep(0)

    result = _run(main, max_steps=500)
    assert result.failure is not None
    assert result.failure.kind == "step_limit"


def test_time_limit() -> None:
    async def main() -> None:
        while True:
            await asyncio.sleep(60)

    result = _run(main, max_time=3600)
    assert result.failure is not None
    assert result.failure.kind == "time_limit"
    assert result.sim_time <= 3600


def test_unretrieved_background_exception_fails_the_run() -> None:
    async def background() -> None:
        await asyncio.sleep(1)
        raise RuntimeError("lost")

    async def main() -> None:
        asyncio.create_task(background())  # noqa: RUF006
        await asyncio.sleep(5)

    result = _run(main)
    assert result.failure is not None
    assert result.failure.kind == "unhandled"
    assert "lost" in result.failure.message
    assert _run(main, fail_on_unhandled=False).ok


def test_awaited_background_exception_is_not_unhandled() -> None:
    async def background() -> None:
        raise RuntimeError("seen")

    async def main() -> str:
        task = asyncio.create_task(background())
        await asyncio.sleep(1)
        try:
            await task
        except RuntimeError:
            return "handled"
        return "?"

    result = _run(main)
    assert result.ok, result.failure
    assert result.value == "handled"


def test_exception_in_plain_callback_fails_the_run() -> None:
    async def main() -> None:
        asyncio.get_running_loop().call_soon(lambda: 1 / 0)
        await asyncio.sleep(1)

    result = _run(main)
    assert result.failure is not None
    assert result.failure.kind == "unhandled"
    assert "ZeroDivisionError" in result.failure.message


def test_always_is_checked_after_every_step() -> None:
    async def main() -> None:
        state = {"balance": 10}
        simloop.always(lambda: state["balance"] >= 0, "balance never negative")
        await asyncio.sleep(1)
        state["balance"] -= 20
        await asyncio.sleep(1)
        state["balance"] += 20

    result = _run(main)
    assert result.failure is not None
    assert result.failure.kind == "invariant"
    assert result.failure.signature == "invariant:balance never negative"
    assert result.sim_time == 1
    assert result.trace.events[-1].kind == "failed"


def test_always_must_hold_when_registered() -> None:
    async def main() -> None:
        simloop.always(lambda: False, "never")

    result = _run(main)
    assert result.failure is not None
    assert result.failure.signature == "invariant:never"


def test_cancelled_property_is_no_longer_checked() -> None:
    async def main() -> None:
        flag = {"ok": True}
        prop = simloop.always(lambda: flag["ok"], "flag")
        await asyncio.sleep(1)
        prop.cancel()
        flag["ok"] = False
        await asyncio.sleep(1)

    assert _run(main).ok


def test_predicate_errors_count_as_violations() -> None:
    async def main() -> None:
        data: dict[str, int] = {}
        simloop.always(lambda: data["missing"] > 0, "lookup")

    result = _run(main)
    assert result.failure is not None
    assert "KeyError" in result.failure.message


def test_eventually_within_deadline() -> None:
    async def main(delay: float) -> None:
        state = {"ready": False}
        simloop.eventually(lambda: state["ready"], "becomes ready", within=5)

        async def warm_up() -> None:
            await asyncio.sleep(delay)
            state["ready"] = True

        await asyncio.gather(warm_up(), asyncio.sleep(10))

    assert _run(lambda: main(4)).ok
    late = _run(lambda: main(6))
    assert late.failure is not None
    assert late.failure.kind == "invariant"
    assert "did not hold by t=5" in late.failure.message


def test_eventually_without_deadline_is_checked_at_the_end() -> None:
    async def main(finish: bool) -> None:
        state = {"done": False}
        simloop.eventually(lambda: state["done"], "finishes")
        await asyncio.sleep(1)
        state["done"] = finish

    assert _run(lambda: main(True)).ok
    result = _run(lambda: main(False))
    assert result.failure is not None
    assert "never held" in result.failure.message


def test_api_outside_simulation_raises() -> None:
    assert not simloop.in_simulation()
    with pytest.raises(RuntimeError, match="not inside"):
        simloop.now()


def test_passing_a_coroutine_instead_of_function_is_rejected() -> None:
    async def main() -> None:
        pass

    coro = main()
    with pytest.raises(TypeError, match="coroutine \\*function\\*"):
        simloop.run(coro)  # type: ignore[arg-type]
    coro.close()


def test_leftover_tasks_are_cancelled_after_main_returns() -> None:
    cleaned: list[str] = []

    async def server() -> None:
        try:
            await asyncio.sleep(1e9)
        finally:
            cleaned.append("server stopped")

    async def main() -> None:
        asyncio.get_running_loop().create_task(server())
        await asyncio.sleep(1)

    assert _run(main).ok
    assert cleaned == ["server stopped"]


def test_tasks_cleanup_cannot_finish_are_closed_inside_the_run() -> None:
    seen: list[float] = []

    async def stubborn() -> None:
        try:
            while True:
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.Event().wait()
        finally:
            # Outside the simulation this would raise instead.
            seen.append(simloop.now())

    async def main() -> None:
        asyncio.get_running_loop().create_task(stubborn())
        await asyncio.sleep(1.0)
        raise ValueError("boom")

    result = simloop.run(main)
    assert result.failure is not None
    assert result.failure.kind == "exception"
    assert seen == [1.0]


def _leaky_scope_then_more_work() -> Callable[[], Coroutine[Any, Any, None]]:
    async def main() -> None:
        task = asyncio.current_task()
        assert task is not None
        asyncio.get_running_loop().call_later(0.1, task.cancel)
        with contextlib.suppress(asyncio.CancelledError):  # swallowed without uncancel()
            await asyncio.sleep(1.0)
        await asyncio.sleep(1.0)

    return main


def test_strict_cancellation_flags_a_cancel_request_left_pending() -> None:
    main = _leaky_scope_then_more_work()
    assert simloop.run(main).ok
    result = simloop.run(main, config=simloop.SimConfig(strict_cancellation=True))
    assert result.failure is not None
    assert result.failure.kind == "cancel_leak"
    assert result.failure.signature == "cancel_leak:_leaky_scope_then_more_work.<locals>.main"
    assert "1 cancellation request still pending" in result.failure.message
    assert "t=0.1" in result.failure.message


def test_strict_cancellation_accepts_well_behaved_code() -> None:
    async def handler() -> None:
        # A catch-all that ends the task right away leaks nothing.
        with contextlib.suppress(BaseException):
            await asyncio.sleep(10)

    async def failing_child() -> None:
        raise ValueError("child")

    async def main() -> None:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(0.1):
                await asyncio.sleep(1)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.sleep(1), 0.1)
        with contextlib.suppress(ExceptionGroup):
            async with asyncio.TaskGroup() as tg:
                tg.create_task(failing_child())
                await asyncio.sleep(1)
        task = asyncio.get_running_loop().create_task(handler())
        await asyncio.sleep(0.1)
        task.cancel()
        await task
        cleanup = asyncio.get_running_loop().create_task(asyncio.sleep(10))
        await asyncio.sleep(0)
        cleanup.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup

    config = simloop.SimConfig(strict_cancellation=True)
    for seed in range(20):
        result = simloop.run(main, seed=seed, config=config)
        assert result.ok, result.failure


def test_strict_cancellation_flags_a_cancellation_from_a_shared_task() -> None:
    async def main() -> None:
        shared = asyncio.get_running_loop().create_task(asyncio.sleep(1.0))

        async def impatient() -> None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(shared, 0.1)  # cancels `shared` on timeout

        async def patient() -> None:
            await shared

        await asyncio.gather(impatient(), patient())

    config = simloop.SimConfig(strict_cancellation=True)
    result = simloop.run(main, config=config)
    assert result.failure is not None
    assert result.failure.kind == "stray_cancel"
    assert "nothing called cancel() on it" in result.failure.message
    assert "cancelled at t=0.1" in result.failure.message


def test_strict_cancellation_ignores_cancelling_your_own_future() -> None:
    async def main() -> None:
        loop = asyncio.get_running_loop()
        waiter = loop.create_future()

        async def worker() -> None:
            await waiter  # woken by cancelling the future, as aiohttp does

        task = loop.create_task(worker())
        await asyncio.sleep(0.1)
        waiter.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        other = loop.create_task(asyncio.sleep(1))
        await asyncio.sleep(0)
        other.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await other

    result = simloop.run(main, config=simloop.SimConfig(strict_cancellation=True))
    assert result.ok, result.failure


def test_strict_cancellation_flags_a_cancellation_from_a_shared_future() -> None:
    async def main() -> None:
        loop = asyncio.get_running_loop()
        shared: asyncio.Future[str] = loop.create_future()

        def resolve() -> None:
            if not shared.done():
                shared.set_result("value")

        loop.call_later(1, resolve)

        async def impatient() -> None:
            # Cancelling the task cancels the future it awaits. (wait_for on
            # 3.11 cancels the future itself instead, which is not attributed.)
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(0.1):
                    await shared

        async def patient() -> str:
            return await shared

        waiter = loop.create_task(patient(), name="patient")
        await asyncio.gather(impatient(), waiter, return_exceptions=True)

    result = simloop.run(main, config=simloop.SimConfig(strict_cancellation=True))
    assert result.failure is not None
    assert result.failure.kind == "stray_cancel"
    assert "'patient'" in result.failure.message
    assert "a future it shared with" in result.failure.message
