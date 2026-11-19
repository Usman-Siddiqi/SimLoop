"""grpclib 0.4.9: a call that runs past its deadline leaves a cancel request on the caller's task.

A grpclib server and client (JSON codec, the service from
``grpclib_reset_leak.py``) over the simulated network. Three client tasks
make six unary calls each, with server-side delays of 0-60 ms, under no
deadline, the call's own ``timeout=`` or an outer ``asyncio.wait_for``.
Checked: after every call, however it ended, the calling task has no
cancellation request pending (``task.cancelling() == 0``). SimLoop's
``strict_cancellation`` check flagged this first, in the reset-leak
harness.

Without deadlines, and with ``wait_for``, it passes. With ``timeout=`` it
fails: ``DeadlineWrapper`` fires ``Wrapper.cancel()``, which calls
``task.cancel()`` on every task inside the wrapper, and ``Wrapper.__exit__``
then raises its own ``TimeoutError`` in place of the ``CancelledError``
without calling ``task.uncancel()``. Each expired call leaves one more
request behind. On Python 3.13 and later this makes the task's next
``asyncio.TaskGroup`` that sees a child fail leave a cancellation armed, so
the following ``await`` raises ``CancelledError``: the task is cancelled
by nobody. A ``Wrapper`` that uncancels the tasks it cancelled passes.

Run: python findings/grpclib_deadline_cancel.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable, Coroutine
from types import TracebackType
from typing import Any

from grpclib.client import Channel, UnaryUnaryMethod
from grpclib.exceptions import GRPCError, StreamTerminatedError
from grpclib.utils import Wrapper
from grpclib_reset_leak import JsonCodec, Service, TrackingServer, start_server

import simloop

logging.getLogger("grpclib").setLevel(logging.CRITICAL)
ERRORS = (GRPCError, StreamTerminatedError, TimeoutError, ConnectionError, OSError)
WORKLOADS = {"no deadlines": "none", "outer wait_for": "wait_for", "call deadlines": "timeout"}


def make_harness(mode: str) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """No call leaves a cancellation request on the task that made it."""
        net = simloop.network()
        start_server(net, Service())
        net.set_link("main", "srv", latency=(0.0005, 0.005))
        await asyncio.sleep(0.01)
        rng = simloop.rng()
        channel = Channel("srv", 50051, codec=JsonCodec())
        unary = UnaryUnaryMethod(channel, "/t.S/Unary", None, None)  # type: ignore[var-annotated]
        leaks: list[str] = []

        async def client(n: int) -> None:
            task = asyncio.current_task()
            assert task is not None
            for i in range(6):
                rid = f"{n}-{i}"
                request = {"id": rid, "delay": round(rng.uniform(0, 0.06), 3)}
                limit = rng.uniform(0.01, 0.05)
                try:
                    if mode == "timeout" and rng.random() < 0.5:
                        reply = await unary(request, timeout=limit)
                    elif mode == "wait_for" and rng.random() < 0.5:
                        reply = await asyncio.wait_for(unary(request), limit)
                    else:
                        reply = await unary(request)
                    assert reply == {"id": rid}, f"{rid} got {reply}"
                    outcome = "returned"
                except ERRORS as exc:
                    outcome = f"raised {type(exc).__name__}"
                if task.cancelling():
                    leaks.append(f"{rid} {outcome}, task.cancelling() == {task.cancelling()}")
                    return

        await asyncio.gather(*(client(n) for n in range(3)))
        channel.close()
        assert not leaks, f"calls left cancel requests on their task: {leaks}"

    return harness


def fix_wrapper() -> Callable[[], None]:
    """A Wrapper that takes back the cancel requests it made."""
    original_cancel, original_exit = Wrapper.cancel, Wrapper.__exit__

    def cancel(self: Wrapper, error: Exception) -> None:
        self.__dict__.setdefault("cancelled_tasks", set()).update(self._tasks)
        original_cancel(self, error)

    def exit_(
        self: Wrapper,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        task = asyncio.current_task()
        if task in self.__dict__.get("cancelled_tasks", ()):
            self.__dict__["cancelled_tasks"].discard(task)
            task.uncancel()  # type: ignore[union-attr]
        original_exit(self, exc_type, exc_val, exc_tb)

    Wrapper.cancel = cancel  # type: ignore[method-assign]
    Wrapper.__exit__ = exit_  # type: ignore[method-assign]

    def undo() -> None:
        Wrapper.cancel, Wrapper.__exit__ = original_cancel, original_exit  # type: ignore[method-assign]

    return undo


async def plain() -> list[str]:
    """On a real loop: one call past its deadline, then an ordinary failing TaskGroup."""
    server = TrackingServer([Service()])
    await server.start("127.0.0.1", 0)
    port = server._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    channel = Channel("127.0.0.1", port, codec=JsonCodec())
    unary = UnaryUnaryMethod(channel, "/t.S/Unary", None, None)  # type: ignore[var-annotated]
    task = asyncio.current_task()
    assert task is not None
    seen = []
    try:
        await unary({"id": "slow", "delay": 1.0}, timeout=0.1)
    except TimeoutError:
        seen.append(f"the call raised TimeoutError; task.cancelling() == {task.cancelling()}")

    async def fail() -> None:
        raise ValueError("boom")

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(fail())
    except ExceptionGroup:
        pass
    try:
        await asyncio.sleep(0.01)
        seen.append("after a TaskGroup whose child failed, the next await returned")
    except asyncio.CancelledError:
        seen.append("after a TaskGroup whose child failed, the next await raised CancelledError")
    channel.close()
    server.close()
    return seen


def main() -> int:
    configs = [("random", simloop.SimConfig())]
    configs.append(("fifo", simloop.SimConfig(scheduler="fifo", timer_jitter=0.001)))
    for workload, mode in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(mode), seeds=range(100), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if mode != "timeout":
                assert report.ok, report.summary()
            else:
                assert report.groups
                assert all("cancel requests" in g.message for g in report.groups), report.summary()
    undo = fix_wrapper()
    try:
        for name, config in configs:
            fixed = simloop.explore(make_harness("timeout"), seeds=range(100), config=config)
            print(f"call deadlines, {name}, Wrapper fixed: {fixed.summary().splitlines()[0]}")
            assert fixed.ok, fixed.summary()
    finally:
        undo()
    seen = asyncio.run(plain())
    print(f"real asyncio loop, Python {sys.version.split()[0]}: " + "; ".join(seen))
    assert seen[0].endswith("== 1"), seen
    if sys.version_info >= (3, 13):
        assert seen[1].endswith("CancelledError"), seen
    return 0


if __name__ == "__main__":
    sys.exit(main())
