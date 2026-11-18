"""A from-scratch asyncio event loop driven by simulated time and seeded choices.

It derives from ``AbstractEventLoop`` rather than ``BaseEventLoop`` on purpose:
anything not implemented here fails closed instead of silently reaching for
real sockets, threads or the system clock.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import heapq
import itertools
import os
import sys
import threading
import types
import weakref
from collections import deque
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, TypeVarTuple, Unpack, cast

from . import _transport
from ._choices import ChoiceSource
from ._trace import Trace
from .errors import DeadlockError, StepLimitExceeded, TimeLimitExceeded, UnsupportedOperation
from .scheduling import Scheduler

if TYPE_CHECKING:
    from asyncio.events import _ExceptionHandler, _TaskFactory

    from .net import Network

_T = TypeVar("_T")
_Ts = TypeVarTuple("_Ts")

_ASYNCIO_DIR = os.path.dirname(asyncio.__file__)
_SIMLOOP_DIR = os.path.dirname(__file__)
_INTERNAL_DIRS = (_ASYNCIO_DIR, _SIMLOOP_DIR)


@dataclass(slots=True)
class _Ready:
    handle: asyncio.Handle
    origin: int


StepHook = Callable[[], None]
AdvanceHook = Callable[[float], None]


def _unsupported(name: str) -> Any:
    def method(self: SimEventLoop, *args: Any, **kwargs: Any) -> Any:
        raise UnsupportedOperation(f"SimEventLoop.{name} is not simulated")

    method.__name__ = name
    method.__qualname__ = f"SimEventLoop.{name}"
    return method


class _SimFuture(asyncio.Future[_T]):
    """A future whose hash is its creation order instead of its address.

    asyncio keeps tasks and futures in sets (``TaskGroup`` cancels its
    children by iterating one, ``asyncio.wait`` returns two), so with the
    default identity hash the iteration order, and everything downstream of
    it, would depend on memory layout.
    """

    def __init__(self, *, loop: asyncio.AbstractEventLoop, order: int) -> None:
        self._simloop_order = order
        super().__init__(loop=loop)

    def __hash__(self) -> int:
        return self._simloop_order


class _SimTask(asyncio.Task[_T]):
    """A task with a deterministic hash; see :class:`_SimFuture`."""

    def __init__(
        self,
        coro: Coroutine[Any, Any, _T] | Generator[Any, None, _T],
        *,
        loop: asyncio.AbstractEventLoop,
        name: str,
        context: contextvars.Context | None,
        order: int,
    ) -> None:
        # Set before Task.__init__, which already hashes the task to register it.
        self._simloop_order = order
        super().__init__(coro, loop=loop, name=name, context=context)

    def __hash__(self) -> int:
        return self._simloop_order


class SimEventLoop(asyncio.AbstractEventLoop):
    """Event loop with a virtual clock and a pluggable scheduling policy."""

    def __init__(
        self,
        *,
        choices: ChoiceSource,
        scheduler: Scheduler,
        trace: Trace,
        start_time: float = 0.0,
        timer_jitter: float = 0.0,
        max_steps: int | None = None,
        max_time: float | None = None,
        step_time: float = 0.0,
    ) -> None:
        self.choices = choices
        self.scheduler = scheduler
        self.trace = trace
        self.timer_jitter = timer_jitter
        self.max_steps = max_steps
        self.max_time = None if max_time is None else start_time + max_time
        self.step_time = step_time

        self._now = start_time
        self._step = 0
        # Steps run in a row without the clock moving, to explain a step limit.
        self._still_steps = 0
        self._still_since = start_time
        self._ready: deque[_Ready] = deque()
        self._batch_left = 0
        # (fire time, tie-breaker, handle, origin or None for a fresh one)
        self._timers: list[tuple[float, int, asyncio.TimerHandle, int | None]] = []
        self._timer_seq = itertools.count()
        self._origins = itertools.count(-1, -1)
        self._current_origin: int | None = None

        self._closed = False
        self._stopping = False
        self._running = False
        self._thread_id: int | None = None
        self._debug = False
        self._exception_handler: _ExceptionHandler | None = None
        self._task_factory: _TaskFactory | None = None

        # Value: whether the task's completion has been recorded.
        self._tasks: dict[asyncio.Task[Any], bool] = {}
        self._task_counter = itertools.count(1)
        self._hash_counter = itertools.count(1)
        self._asyncgens: weakref.WeakValueDictionary[int, AsyncGenerator[Any, Any]] = (
            weakref.WeakValueDictionary()
        )
        self._asyncgen_ids: weakref.WeakKeyDictionary[AsyncGenerator[Any, Any], int] = (
            weakref.WeakKeyDictionary()
        )
        self._asyncgen_seq = itertools.count()
        self._asyncgens_shutdown_called = False

        self.network: Network | None = None
        # Returns True to take a ready handle out of circulation (paused hosts).
        self.hold_filter: Callable[[asyncio.Handle], bool] | None = None
        self.errors: list[dict[str, Any]] = []
        self.step_hooks: list[StepHook] = []
        self.advance_hooks: list[AdvanceHook] = []
        self._executor_elapsed: list[float] = []

    # -- clock -------------------------------------------------------------

    def time(self) -> float:
        return self._now

    @property
    def steps(self) -> int:
        return self._step

    def block_for(self, seconds: float) -> None:
        """Model a blocking call: the whole loop is stuck for ``seconds``."""
        if seconds <= 0:
            return
        if self._executor_elapsed:
            self._executor_elapsed[-1] += seconds
            return
        self._record("blocked", self._current_actor(), f"blocking sleep {seconds!r}s")
        self._advance_to(self._now + seconds)

    def _advance_to(self, when: float) -> None:
        if when <= self._now:
            return
        for hook in self.advance_hooks:
            hook(when)
        self._now = when

    # -- scheduling ----------------------------------------------------------

    def call_soon(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: *_Ts,
        context: contextvars.Context | None = None,
    ) -> asyncio.Handle:
        self._check_closed()
        handle = asyncio.Handle(callback, args, self, context)
        origin = self._current_origin if self._current_origin is not None else next(self._origins)
        self._ready.append(_Ready(handle, origin))
        return handle

    call_soon_threadsafe = call_soon

    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: *_Ts,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        if delay is None:
            raise TypeError("delay must not be None")
        return self.call_at(self._now + delay, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: *_Ts,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        if when is None:
            raise TypeError("when cannot be None")
        self._check_closed()
        handle = asyncio.TimerHandle(when, callback, args, self, context)
        fire_at = when
        if self.timer_jitter > 0:
            # Real timers only ever fire late, so jitter is always non-negative.
            fire_at += self.choices.uniform(0.0, self.timer_jitter)
        heapq.heappush(self._timers, (fire_at, next(self._timer_seq), handle, None))
        handle._scheduled = True  # type: ignore[attr-defined]
        return handle

    def requeue(self, handle: asyncio.Handle, origin: int) -> None:
        """Put a held handle back into the ready queue."""
        self._ready.append(_Ready(handle, origin))

    def new_origin(self) -> int:
        """An origin id for callbacks that must keep their relative order."""
        return next(self._origins)

    def _call_at_exact(
        self,
        when: float,
        callback: Callable[..., object],
        *args: Any,
        context: contextvars.Context | None = None,
        origin: int | None = None,
    ) -> asyncio.TimerHandle:
        """Schedule a framework event (e.g. a network delivery) without timer jitter.

        Events sharing an ``origin`` that fire at the same time keep their
        scheduling order under every scheduler, which is how a TCP stream stays
        in order.
        """
        self._check_closed()
        handle = asyncio.TimerHandle(when, callback, args, self, context)
        heapq.heappush(self._timers, (when, next(self._timer_seq), handle, origin))
        handle._scheduled = True  # type: ignore[attr-defined]
        return handle

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        # Cancelled timers are dropped lazily when they reach the top of the heap.
        pass

    def _release_due_timers(self) -> None:
        timers = self._timers
        while timers:
            fire_at, _, handle, origin = timers[0]
            if handle.cancelled():
                heapq.heappop(timers)
                handle._scheduled = False  # type: ignore[attr-defined]
                continue
            if fire_at > self._now:
                break
            heapq.heappop(timers)
            handle._scheduled = False  # type: ignore[attr-defined]
            self._ready.append(_Ready(handle, next(self._origins) if origin is None else origin))

    def _next_timer(self) -> float | None:
        timers = self._timers
        while timers and timers[0][2].cancelled():
            handle = heapq.heappop(timers)[2]
            handle._scheduled = False  # type: ignore[attr-defined]
        return timers[0][0] if timers else None

    def _start_batch(self) -> bool:
        self._release_due_timers()
        if not self._ready:
            fire_at = self._next_timer()
            if fire_at is None or fire_at == float("inf"):
                return False
            if self.max_time is not None and fire_at > self.max_time:
                raise TimeLimitExceeded(
                    f"simulated time would pass the limit of {self.max_time!r}s "
                    f"(next timer at {fire_at!r}s)"
                )
            self._advance_to(fire_at)
            self._release_due_timers()
        self._batch_left = len(self._ready)
        return self._batch_left > 0

    def _run_once(self) -> None:
        if self._batch_left == 0 and not self._start_batch():
            raise self._deadlock()
        index = self.scheduler.pick(self._ready, self._batch_left, self.choices)
        if index == 0:
            entry = self._ready.popleft()
        else:
            entry = self._ready[index]
            del self._ready[index]
        self._batch_left -= 1
        handle = entry.handle
        if handle.cancelled():
            return
        if self.hold_filter is not None and self.hold_filter(handle):
            return

        self._step += 1
        if self._now == self._still_since:
            self._still_steps += 1
        else:
            self._still_since, self._still_steps = self._now, 1
        if self.max_steps is not None and self._step > self.max_steps:
            raise StepLimitExceeded(self._step_limit_message())
        task = _owning_task(handle)
        if task is not None:
            self._register(task)
            self._record("run", task.get_name(), f"resume {_suspension_point(task)}")
        else:
            kind = "timer" if isinstance(handle, asyncio.TimerHandle) else "run"
            self._record(kind, "loop", _callable_name(handle._callback))  # type: ignore[attr-defined]

        self._current_origin = self._step
        try:
            handle._run()
        finally:
            self._current_origin = None

        if task is not None and task.done():
            self._task_finished(task)
        for hook in self.step_hooks:
            hook()
        if self.step_time:
            self._advance_to(self._now + self.step_time)

    def _step_limit_message(self) -> str:
        message = f"exceeded the limit of {self.max_steps} steps"
        if self._still_steps * 2 >= self._step and self._still_steps >= 1000:
            message += (
                f"; the last {self._still_steps} steps all ran at t={self._now!r} "
                "(something keeps rescheduling itself while no time passes; if it "
                "waits for the clock to move, a real loop would get there because "
                "its clock advances between iterations: SimConfig(step_time=1e-6) "
                "models that)"
            )
        return message

    def _deadlock(self) -> DeadlockError:
        blocked = [
            f"{task.get_name()} waiting at {_suspension_point(task)}"
            for task in self._tasks
            if not task.done()
        ]
        message = "deadlock: no runnable callbacks and no pending timers"
        if blocked:
            message += "; blocked tasks:\n  " + "\n  ".join(blocked)
        return DeadlockError(message, blocked)

    # -- tasks and futures -----------------------------------------------------

    def create_future(self) -> asyncio.Future[Any]:
        return _SimFuture(loop=self, order=next(self._hash_counter))

    def create_task(
        self,
        coro: Coroutine[Any, Any, _T] | Generator[Any, None, _T],
        *,
        name: str | None = None,
        context: contextvars.Context | None = None,
    ) -> asyncio.Task[_T]:
        self._check_closed()
        if name is None:
            name = f"Task-{next(self._task_counter)}"
        if self._task_factory is None:
            task: asyncio.Task[_T] = _SimTask(
                coro, loop=self, name=name, context=context, order=next(self._hash_counter)
            )
        else:
            factory = cast(Callable[..., asyncio.Task[_T]], self._task_factory)
            task = factory(self, coro) if context is None else factory(self, coro, context=context)
            task.set_name(name)
        self._tasks[task] = False
        host = self.network._adopt(task, context) if self.network is not None else None
        parent = asyncio.current_task(self)
        self._record(
            "spawn",
            parent.get_name() if parent is not None else "loop",
            f"{name} {_coro_name(coro)}" + (f" @{host}" if host else ""),
        )
        return task

    def _register(self, task: asyncio.Task[Any]) -> None:
        if task in self._tasks or task.done():
            return
        # Created by instantiating asyncio.Task directly; its default name comes
        # from a process-wide counter, so replace it with a per-run one.
        if task.get_name().startswith("Task-"):
            task.set_name(f"Task-{next(self._task_counter)}")
        self._tasks[task] = False
        self._record("spawn", "loop", f"{task.get_name()} {_coro_name(task.get_coro())}")

    def _task_finished(self, task: asyncio.Task[Any]) -> None:
        if self._tasks.get(task, True):
            return
        if task.cancelled():
            outcome = "cancelled"
        # Read the private attribute: task.exception() would mark the error as
        # retrieved and hide it from unhandled-exception detection.
        elif (exc := task._exception) is not None:
            outcome = f"raised {_describe_exception(exc)}"
        else:
            outcome = "returned"
        self._record("done", task.get_name(), outcome)
        if outcome == "returned" or outcome == "cancelled":
            # Nothing left to inspect; drop it so long runs don't hoard tasks.
            del self._tasks[task]
        else:
            self._tasks[task] = True

    @property
    def tasks(self) -> list[asyncio.Task[Any]]:
        """Pending tasks and tasks that failed, in creation order."""
        return list(self._tasks)

    def get_task_factory(self) -> _TaskFactory | None:
        return self._task_factory

    def set_task_factory(self, factory: _TaskFactory | None) -> None:
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

    # -- running -------------------------------------------------------------

    def run_forever(self) -> None:
        self._check_closed()
        self._check_not_running()
        old_hooks = sys.get_asyncgen_hooks()
        self._thread_id = threading.get_ident()
        self._running = True
        sys.set_asyncgen_hooks(
            firstiter=self._asyncgen_firstiter_hook,
            finalizer=self._asyncgen_finalizer_hook,
        )
        asyncio.events._set_running_loop(self)
        try:
            while True:
                self._run_once()
                if self._stopping and self._batch_left == 0:
                    break
        finally:
            self._stopping = False
            self._running = False
            self._thread_id = None
            asyncio.events._set_running_loop(None)
            sys.set_asyncgen_hooks(*old_hooks)

    def run_until_complete(self, future: asyncio.Future[_T] | Coroutine[Any, Any, _T]) -> _T:  # type: ignore[override]
        __tracebackhide__ = True
        self._check_closed()
        self._check_not_running()
        new_task = not asyncio.isfuture(future)
        fut = asyncio.ensure_future(future, loop=self)
        fut.add_done_callback(_stop_loop)
        try:
            self.run_forever()
        except BaseException:
            if new_task and fut.done() and not fut.cancelled():
                fut.exception()
            raise
        finally:
            fut.remove_done_callback(_stop_loop)
        if not fut.done():
            raise RuntimeError("Event loop stopped before Future completed.")
        return fut.result()

    def stop(self) -> None:
        self._stopping = True

    def is_running(self) -> bool:
        return self._running

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._running:
            raise RuntimeError("Cannot close a running event loop")
        if self._closed:
            return
        self._closed = True
        self._ready.clear()
        self._batch_left = 0
        for _, _, handle, _ in self._timers:
            handle._scheduled = False  # type: ignore[attr-defined]
        self._timers.clear()

    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("Event loop is closed")

    def _check_not_running(self) -> None:
        if self._running:
            raise RuntimeError("This event loop is already running")
        if asyncio.events._get_running_loop() is not None:
            raise RuntimeError("Cannot run the event loop while another loop is running")

    # -- async generators ------------------------------------------------------

    def _asyncgen_firstiter_hook(self, agen: AsyncGenerator[Any, Any]) -> None:
        seq = next(self._asyncgen_seq)
        self._asyncgens[seq] = agen
        self._asyncgen_ids[agen] = seq

    def _asyncgen_finalizer_hook(self, agen: AsyncGenerator[Any, Any]) -> None:
        seq = self._asyncgen_ids.pop(agen, None)
        if seq is not None:
            self._asyncgens.pop(seq, None)
        if not self._closed:
            self.call_soon(self.create_task, agen.aclose())

    @property
    def has_asyncgens(self) -> bool:
        return len(self._asyncgens) > 0

    async def shutdown_asyncgens(self) -> None:
        self._asyncgens_shutdown_called = True
        # WeakValueDictionary keeps insertion order, so this is creation order.
        agens = list(self._asyncgens.values())
        self._asyncgens.clear()
        if not agens:
            return
        results = await asyncio.gather(*(ag.aclose() for ag in agens), return_exceptions=True)
        for result, agen in zip(results, agens, strict=True):
            if isinstance(result, Exception):
                self.call_exception_handler(
                    {
                        "message": f"an error occurred during closing of asynchronous generator "
                        f"{getattr(agen, '__qualname__', agen)}",
                        "exception": result,
                        "asyncgen": agen,
                    }
                )

    # -- executors -------------------------------------------------------------

    def run_in_executor(
        self, executor: Executor | None, func: Callable[[Unpack[_Ts]], _T], *args: *_Ts
    ) -> asyncio.Future[_T]:
        """Run ``func`` inline, delivering its result after its simulated duration.

        Real threads would make the run non-deterministic. Blocking sleeps inside
        ``func`` count toward the work's duration instead of blocking the loop.
        """
        self._check_closed()
        future: asyncio.Future[_T] = self.create_future()
        self._executor_elapsed.append(0.0)
        try:
            result = func(*args)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            outcome: tuple[bool, Any] = (False, exc)
        else:
            outcome = (True, result)
        finally:
            elapsed = self._executor_elapsed.pop()
        self._record("executor", self._current_actor(), f"{_callable_name(func)} took {elapsed!r}s")
        self.call_at(self._now + elapsed, _deliver, future, outcome)
        return future

    def set_default_executor(self, executor: Any) -> None:
        pass

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        pass

    # -- error handling ----------------------------------------------------------

    def get_exception_handler(self) -> _ExceptionHandler | None:
        return self._exception_handler

    def set_exception_handler(self, handler: _ExceptionHandler | None) -> None:
        if handler is not None and not callable(handler):
            raise TypeError(f"A callable object or None is expected, got {handler!r}")
        self._exception_handler = handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        if self._closed:
            return
        self.errors.append(context)
        exc = context.get("exception")
        detail = context.get("message", "unhandled error")
        if isinstance(exc, BaseException):
            detail = f"{detail}: {_describe_exception(exc)}"
        self._record("error", self._current_actor(), detail)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        if self._exception_handler is None:
            self.default_exception_handler(context)
            return
        try:
            self._exception_handler(self, context)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self.default_exception_handler(
                {
                    "message": "Unhandled error in exception handler",
                    "exception": exc,
                    "context": context,
                }
            )

    def get_debug(self) -> bool:
        return self._debug

    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled

    # -- tracing helpers -------------------------------------------------------

    def log(self, message: str) -> None:
        self._record("log", self._current_actor(), message)

    def _record(self, kind: str, actor: str, detail: str = "") -> None:
        self.trace.record(self._step, self._now, kind, actor, detail)

    def _current_actor(self) -> str:
        task = asyncio.current_task(self) if self._running else None
        return task.get_name() if task is not None else "loop"

    # -- simulated network ------------------------------------------------------------

    def _require_network(self) -> Network:
        if self.network is None:
            raise UnsupportedOperation("this SimEventLoop has no simulated network attached")
        return self.network

    async def getaddrinfo(
        self,
        host: bytes | str | None,
        port: bytes | str | int | None,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[tuple[Any, ...]]:
        return await self._require_network().getaddrinfo(
            host, port, family=family, type=type, proto=proto, flags=flags
        )

    async def getnameinfo(self, sockaddr: tuple[Any, ...], flags: int = 0) -> tuple[str, str]:
        return await self._require_network().getnameinfo(sockaddr, flags)

    async def create_connection(  # type: ignore[override]
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: str | None = None,
        port: int | None = None,
        **kwargs: Any,
    ) -> tuple[asyncio.Transport, asyncio.BaseProtocol]:
        return await _transport.create_connection(
            self._require_network(), protocol_factory, host, port, **kwargs
        )

    async def create_server(  # type: ignore[override]
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: str | list[str] | None = None,
        port: int | None = None,
        **kwargs: Any,
    ) -> asyncio.AbstractServer:
        return await _transport.create_server(
            self._require_network(), protocol_factory, host, port, **kwargs
        )

    async def create_datagram_endpoint(  # type: ignore[override]
        self,
        protocol_factory: Callable[[], asyncio.DatagramProtocol],
        local_addr: tuple[str, int] | None = None,
        remote_addr: tuple[str, int] | None = None,
        **kwargs: Any,
    ) -> tuple[asyncio.DatagramTransport, asyncio.DatagramProtocol]:
        return await _transport.create_datagram_endpoint(
            self._require_network(), protocol_factory, local_addr, remote_addr, **kwargs
        )

    async def sock_connect(self, sock: Any, address: Any) -> None:
        await _transport.sock_connect(self._require_network(), sock, address)

    # -- not simulated: fail closed ----------------------------------------------

    create_unix_connection = _unsupported("create_unix_connection")
    create_unix_server = _unsupported("create_unix_server")
    connect_accepted_socket = _unsupported("connect_accepted_socket")
    sendfile = _unsupported("sendfile")
    start_tls = _unsupported("start_tls")
    sock_recv = _unsupported("sock_recv")
    sock_recv_into = _unsupported("sock_recv_into")
    sock_recvfrom = _unsupported("sock_recvfrom")
    sock_recvfrom_into = _unsupported("sock_recvfrom_into")
    sock_sendall = _unsupported("sock_sendall")
    sock_sendto = _unsupported("sock_sendto")
    sock_accept = _unsupported("sock_accept")
    sock_sendfile = _unsupported("sock_sendfile")
    add_reader = _unsupported("add_reader")
    remove_reader = _unsupported("remove_reader")
    add_writer = _unsupported("add_writer")
    remove_writer = _unsupported("remove_writer")
    add_signal_handler = _unsupported("add_signal_handler")
    remove_signal_handler = _unsupported("remove_signal_handler")
    connect_read_pipe = _unsupported("connect_read_pipe")
    connect_write_pipe = _unsupported("connect_write_pipe")
    subprocess_exec = _unsupported("subprocess_exec")
    subprocess_shell = _unsupported("subprocess_shell")


def _stop_loop(fut: asyncio.Future[Any]) -> None:
    fut.get_loop().stop()


def _deliver(future: asyncio.Future[Any], outcome: tuple[bool, Any]) -> None:
    if future.cancelled():
        return
    ok, value = outcome
    if ok:
        future.set_result(value)
    else:
        future.set_exception(value)


def _owning_task(handle: asyncio.Handle) -> asyncio.Task[Any] | None:
    owner = getattr(handle._callback, "__self__", None)  # type: ignore[attr-defined]
    return owner if isinstance(owner, asyncio.Task) else None


def _callable_name(func: Any) -> str:
    while isinstance(func, functools.partial):
        # asyncio.to_thread wraps the real function as partial(context.run, func).
        func = func.args[0] if _is_context_run(func.func) and func.args else func.func
    name = getattr(func, "__qualname__", None)
    if isinstance(name, str):
        return name
    return type(func).__qualname__


def _is_context_run(func: Any) -> bool:
    return isinstance(getattr(func, "__self__", None), contextvars.Context)


def _coro_name(coro: Any) -> str:
    name = getattr(coro, "__qualname__", None)
    return name if isinstance(name, str) else type(coro).__qualname__


def _describe_exception(exc: BaseException) -> str:
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _suspension_point(task: asyncio.Task[Any]) -> str:
    """Deepest user-code frame of a suspended task, e.g. ``app.py:12 in fetch``."""
    frames: list[types.FrameType] = []
    obj: Any = task.get_coro()
    while obj is not None:
        frame = getattr(obj, "cr_frame", None) or getattr(obj, "gi_frame", None)
        frame = frame or getattr(obj, "ag_frame", None)
        if frame is None:
            break
        frames.append(frame)
        obj = (
            getattr(obj, "cr_await", None)
            or getattr(obj, "gi_yieldfrom", None)
            or getattr(obj, "ag_await", None)
        )
    if not frames:
        return "<not started>" if not task.done() else "<finished>"
    user = [f for f in frames if not f.f_code.co_filename.startswith(_INTERNAL_DIRS)]
    frame = user[-1] if user else frames[-1]
    code = frame.f_code
    return f"{os.path.basename(code.co_filename)}:{frame.f_lineno} in {code.co_qualname}"
