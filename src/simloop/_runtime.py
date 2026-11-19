"""Running a coroutine function inside a simulation and classifying the outcome."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import gc
import os
import random
import threading
import time
import traceback
from collections.abc import Callable, Coroutine, Iterator, Sequence
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Generic, TypeVar

from . import _stablehash
from ._choices import ChoiceRandom, ChoiceSource
from ._loop import _INTERNAL_DIRS, SimEventLoop
from ._properties import Predicate, Property, PropertyChecker
from ._sandbox import Sandbox
from ._trace import Trace
from .errors import (
    CancellationLeak,
    DeadlockError,
    InvariantViolation,
    NondeterminismError,
    SimulationError,
    StepLimitExceeded,
    TimeLimitExceeded,
)
from .net import Host, LinkConfig, Network
from .scheduling import Scheduler, resolve_scheduler

T = TypeVar("T")

MainFunction = Callable[[], Coroutine[Any, Any, T]]

_wall_clock = time.perf_counter
_local = threading.local()

# 2024-01-01T00:00:00Z: what time.time() reports at simulated t=0.
DEFAULT_EPOCH = 1_704_067_200.0


@dataclass(frozen=True)
class SimConfig:
    """Settings for a simulation run. Every field is part of a reproduction."""

    scheduler: str | Scheduler = "random"
    """``"random"`` explores orderings of ready callbacks, ``"fifo"`` is asyncio's
    order, ``"pct"`` / ``"pct:DEPTH"`` is priority-based (PCT) exploration."""
    timer_jitter: float = 0.0
    """Upper bound (seconds) of a random, non-negative delay added to every timer."""
    max_steps: int | None = 1_000_000
    max_time: float | None = None
    """Fail if simulated time would pass this many seconds."""
    step_time: float = 0.0
    """Simulated seconds that each callback takes. Zero freezes the clock while
    callbacks run; a small value (``1e-6``) models a real loop's clock moving
    between iterations, which code that spins until time passes relies on."""
    start_time: float = 0.0
    epoch: float = DEFAULT_EPOCH
    timezone: str | float = "UTC"
    """The simulated local time zone: an offset from UTC in hours (``-5``,
    ``5.5``) or an IANA name (``"America/New_York"``, with its
    daylight-saving transitions). ``time.localtime``, ``mktime``,
    ``strftime``, ``datetime.now()``, ``fromtimestamp()``, ``date.today()``,
    ``TZ`` and tzlocal use it instead of the host's zone, so runs do not
    depend on the machine's TZ; another zone exposes code that mixes local
    and UTC times."""
    sandbox: bool = True
    """Virtualize time/randomness and reject real I/O while the run is active."""
    fail_on_unhandled: bool = True
    """Fail when a background task or callback raises and nobody handles it."""
    gc_interval: int | None = 10_000
    """Disable automatic GC during the run and collect every N steps instead,
    so finalizers run at reproducible points. ``None`` leaves GC alone."""
    keep_trace: bool = True
    link: LinkConfig = field(default_factory=LinkConfig)
    """Default behaviour of every link in the simulated network."""
    connect_timeout: float = 30.0
    """How long a TCP connect keeps retrying an unreachable host."""
    buggify: bool = True
    """Let :func:`simloop.faults.buggify` sites fire."""
    strict_cancellation: bool = False
    """Check that cancellation stays with the task it was meant for. Fails a
    run when a task finishes normally after running on with ``cancelling()``
    above zero (a ``cancel()`` nothing matched with ``uncancel()``, which makes
    a later ``asyncio.timeout`` or ``TaskGroup`` in that task mistake it for a
    cancellation from outside), and when a task ends cancelled although
    nothing called ``cancel()`` on it (a ``CancelledError`` from a shared task
    or future that was cancelled on someone else's behalf)."""

    def replace(self, **changes: Any) -> SimConfig:
        return dataclasses.replace(self, **changes)

    def to_dict(self) -> dict[str, Any]:
        """JSON-compatible form, for saving reproductions."""
        if not isinstance(self.scheduler, str):
            raise ValueError("only configs with a named scheduler can be serialized")
        data = dataclasses.asdict(self)
        latency = self.link.latency
        data["link"]["latency"] = list(latency) if isinstance(latency, tuple) else latency
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SimConfig:
        known = {f.name for f in dataclasses.fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown SimConfig fields: {', '.join(sorted(unknown))}")
        values = dict(data)
        if "link" in values:
            link = dict(values["link"])
            if isinstance(link.get("latency"), list):
                link["latency"] = tuple(link["latency"])
            values["link"] = LinkConfig(**link)
        return cls(**values)


@dataclass(frozen=True)
class Failure:
    """Why a run failed."""

    kind: str
    """One of: exception, invariant, deadlock, step_limit, time_limit,
    unhandled, nondeterminism."""
    message: str
    signature: str
    """Stable identity used to group failures across seeds."""
    exception: BaseException | None = field(default=None, compare=False, repr=False)

    def format(self) -> str:
        if self.exception is None or self.kind in {"deadlock", "invariant"}:
            return f"{self.kind}: {self.message}"
        return "".join(traceback.format_exception(self.exception)).rstrip()


@dataclass
class RunResult(Generic[T]):
    """Outcome of one simulated run."""

    seed: int
    config: SimConfig
    value: T | None
    failure: Failure | None
    steps: int
    sim_time: float
    wall_time: float
    trace: Trace
    choices: list[int]
    replayed: bool = False
    """Whether choices came from a replayed sequence rather than the seed."""

    @property
    def ok(self) -> bool:
        return self.failure is None

    @property
    def digest(self) -> str:
        return self.trace.digest()

    def raise_for_failure(self) -> None:
        if self.failure is None:
            return
        if self.failure.exception is not None:
            raise self.failure.exception
        raise SimulationError(self.failure.message)


class Simulation:
    """State of one in-progress run; reachable from inside it via :func:`current`."""

    def __init__(
        self,
        seed: int,
        config: SimConfig,
        replay: Sequence[int] | None = None,
        *,
        digest: bool = True,
    ) -> None:
        self.seed = seed
        self.config = config
        self.choices = ChoiceSource(seed, replay)
        self.trace = Trace(keep=config.keep_trace, digest=digest)
        self.loop = SimEventLoop(
            choices=self.choices,
            scheduler=resolve_scheduler(config.scheduler),
            trace=self.trace,
            start_time=config.start_time,
            timer_jitter=config.timer_jitter,
            max_steps=config.max_steps,
            max_time=config.max_time,
            step_time=config.step_time,
            strict_cancellation=config.strict_cancellation,
        )
        self.network = Network(self.loop, link=config.link, connect_timeout=config.connect_timeout)
        self.loop.network = self.network
        self.properties = PropertyChecker(self.loop.time)
        self.loop.step_hooks.append(self.properties.after_step)
        self.loop.advance_hooks.append(self.properties.before_advance)
        if config.gc_interval:
            self.loop.step_hooks.append(self._make_gc_hook(config.gc_interval))
        self.random: random.Random = ChoiceRandom(self.choices)
        self.sandbox: Sandbox | None = None
        self.buggify_sites: dict[tuple[str, int], bool] = {}
        self._failed_at: tuple[int, float] | None = None

    def _make_gc_hook(self, interval: int) -> Callable[[], None]:
        loop = self.loop

        def collect() -> None:
            if loop.steps % interval == 0:
                gc.collect()

        return collect

    def execute(self, main: MainFunction[T]) -> RunResult[T]:
        __tracebackhide__ = True  # keep pytest reports focused on user code
        if not callable(main):
            raise TypeError(
                "simloop needs a coroutine *function* so every run gets a fresh coroutine; "
                "pass `main`, not `main()`"
            )
        started = _wall_clock()
        value: T | None = None
        failure: Failure | None = None
        loop = self.loop
        with self._activated() as sandbox:
            main_task = loop.create_task(main(), name="main")
            try:
                value = loop.run_until_complete(main_task)
            except (KeyboardInterrupt, SystemExit):
                self._cleanup(abandon=True)
                raise
            except BaseException as exc:
                failure = self._fail(_classify(exc))
            if failure is None:
                failure = self._cleanup(abandon=False)
                if failure is not None:
                    self._fail(failure)
            else:
                self._cleanup(abandon=True)
            if failure is None:
                try:
                    self.properties.at_end()
                except InvariantViolation as exc:
                    failure = self._fail(_classify(exc))
            unhandled = self._collect_unhandled(main_task)
            if failure is None and unhandled and self.config.fail_on_unhandled:
                failure = self._fail(unhandled[0])
            if sandbox is not None and sandbox.leaks:
                failure = Failure(
                    "nondeterminism",
                    sandbox.leaks[0],
                    f"nondeterminism:{sandbox.leaks[0].split(' ', 1)[0]}",
                    sandbox.leak_errors[0],
                )
        # A failed run reports where it failed, not where cleanup ended.
        steps, end_time = self._failed_at or (loop.steps, loop.time())
        return RunResult(
            seed=self.seed,
            config=self.config,
            value=value,
            failure=failure,
            steps=steps,
            sim_time=end_time - self.config.start_time,
            wall_time=_wall_clock() - started,
            trace=self.trace,
            choices=list(self.choices.record),
            replayed=self.choices.replaying,
        )

    def _fail(self, failure: Failure) -> Failure:
        self.loop._record("failed", "sim", failure.message.splitlines()[0])
        self.trace.freeze()
        self._failed_at = (self.loop.steps, self.loop.time())
        return failure

    @contextlib.contextmanager
    def _activated(self) -> Iterator[Sandbox | None]:
        if getattr(_local, "simulation", None) is not None:
            raise RuntimeError("simulations cannot be nested")
        gc_was_enabled = gc.isenabled()
        if self.config.gc_interval:
            # Clear garbage left by earlier runs, so its finalizers cannot fire
            # at a collection inside this run. A full collection: objects that
            # survived an earlier run's collections sit in the oldest generation.
            gc.collect()
            gc.disable()
        sandbox = (
            Sandbox(self.loop, self.seed, self.config.epoch, self.config.timezone)
            if self.config.sandbox
            else None
        )
        _local.simulation = self
        _stablehash.begin_run()
        try:
            with sandbox if sandbox is not None else contextlib.nullcontext():
                self.sandbox = sandbox
                yield sandbox
        finally:
            _stablehash.end_run()
            _local.simulation = None
            self.loop.close()
            if gc_was_enabled:
                gc.enable()

    def _cleanup(self, *, abandon: bool) -> Failure | None:
        """Cancel leftover tasks and finalize async generators, like ``asyncio.run``.

        After a failure the run is already decided, so cleanup problems are ignored.
        """
        loop = self.loop
        loop.step_hooks.remove(self.properties.after_step)
        loop.advance_hooks.remove(self.properties.before_advance)
        if not abandon:
            loop.max_still_steps = None
        if abandon and loop.max_steps is not None:
            # Cleanup gets no more steps than the run itself had, so a run cut
            # short at a small limit (a shrink candidate) stays cheap.
            loop.max_steps = loop.steps + min(10_000, loop.max_steps)
        try:
            pending = [task for task in loop.tasks if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            if loop.has_asyncgens:
                loop.run_until_complete(loop.shutdown_asyncgens())
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            if not abandon:
                return _classify(exc)
        finally:
            self.network._shutdown()
            for task in loop.tasks:
                if not task.done():
                    task._log_destroy_pending = False  # type: ignore[attr-defined]
                    _close_abandoned(task)
        return None

    def _collect_unhandled(self, main_task: asyncio.Task[Any]) -> list[Failure]:
        failures: list[Failure] = []
        for task in self.loop.tasks:
            if task is main_task or not task.done() or task.cancelled():
                continue
            exc = task._exception
            if exc is not None and task._log_traceback:
                task._log_traceback = False
                failures.append(
                    Failure(
                        "unhandled",
                        f"exception in task {task.get_name()!r} was never retrieved: "
                        f"{type(exc).__name__}: {exc}",
                        f"unhandled:{type(exc).__name__}@{_location(exc)}",
                        exc,
                    )
                )
        for context in self.loop.errors:
            exc = context.get("exception")
            message = str(context.get("message", "unhandled error in callback"))
            failures.append(
                Failure(
                    "unhandled",
                    message if exc is None else f"{message}: {type(exc).__name__}: {exc}",
                    "unhandled:"
                    + (message if exc is None else f"{type(exc).__name__}@{_location(exc)}"),
                    exc if isinstance(exc, BaseException) else None,
                )
            )
        return failures


def run(
    main: MainFunction[T],
    *,
    seed: int = 0,
    config: SimConfig | None = None,
    replay: Sequence[int] | None = None,
) -> RunResult[T]:
    """Run ``main()`` in a fresh simulation and return the outcome.

    ``replay`` replays a recorded choice sequence (``RunResult.choices``)
    instead of drawing choices from the seed.
    """
    return Simulation(seed, config or SimConfig(), replay).execute(main)


def run_measured(
    main: MainFunction[T],
    *,
    seed: int,
    config: SimConfig,
    replay: Sequence[int] | None = None,
    max_still_steps: int | None = None,
) -> tuple[RunResult[T], int]:
    """``run``, also returning the most steps run in a row at one instant.

    ``max_still_steps`` fails the run with a step limit once a streak gets
    longer than that. With ``keep_trace`` off the run computes no trace
    digest either, so the result's ``digest`` is meaningless.
    """
    sim = Simulation(seed, config, replay, digest=config.keep_trace)
    sim.loop.max_still_steps = max_still_steps
    return sim.execute(main), sim.loop.longest_still


# -- API for code running inside a simulation ------------------------------------


def current() -> Simulation:
    """The simulation running on this thread. Raises if there is none."""
    sim: Simulation | None = getattr(_local, "simulation", None)
    if sim is None:
        raise RuntimeError("not inside a simloop simulation")
    return sim


def in_simulation() -> bool:
    return getattr(_local, "simulation", None) is not None


def now() -> float:
    """Current simulated time in seconds."""
    return current().loop.time()


def rng() -> random.Random:
    """The simulation's random generator: seeded, recorded and shrinkable."""
    return current().random


def network() -> Network:
    """The simulated network of the running simulation."""
    return current().network


def current_host() -> Host:
    """The simulated host the calling task runs on."""
    return current().network.current_host()


def log(message: str) -> None:
    """Add a message to the run's trace, attributed to the current task."""
    current().loop.log(message)


def always(predicate: Predicate, name: str | None = None) -> Property:
    """Require ``predicate()`` to be true now and after every subsequent step."""
    return current().properties.add_always(name or _predicate_name(predicate), predicate)


def eventually(
    predicate: Predicate, name: str | None = None, *, within: float | None = None
) -> Property:
    """Require ``predicate()`` to become true, within ``within`` simulated seconds
    if given, otherwise before the run ends."""
    return current().properties.add_eventually(
        name or _predicate_name(predicate), predicate, within
    )


# -- failure classification --------------------------------------------------------


def _classify(exc: BaseException) -> Failure:
    if isinstance(exc, InvariantViolation):
        return Failure("invariant", str(exc), f"invariant:{exc.property_name}", exc)
    if isinstance(exc, DeadlockError):
        points = sorted({entry.split(" waiting at ", 1)[-1] for entry in exc.blocked})
        return Failure("deadlock", str(exc), "deadlock:" + ";".join(points), exc)
    if isinstance(exc, StepLimitExceeded):
        return Failure("step_limit", str(exc), "step_limit", exc)
    if isinstance(exc, TimeLimitExceeded):
        return Failure("time_limit", str(exc), "time_limit", exc)
    if isinstance(exc, CancellationLeak):
        return Failure(exc.kind, str(exc), f"{exc.kind}:{exc.coroutine}", exc)
    if isinstance(exc, NondeterminismError):
        return Failure("nondeterminism", str(exc), "nondeterminism", exc)
    message = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    return Failure("exception", message, f"exception:{type(exc).__name__}@{_location(exc)}", exc)


def _close_abandoned(task: asyncio.Task[Any]) -> None:
    """Close a task that cleanup could not finish, while the simulation is active.

    Otherwise the garbage collector closes its coroutine later, outside the
    simulation, and its ``finally`` blocks and ``async with`` exits run
    against a closed loop and print "Exception ignored" noise into whatever
    runs next.
    """
    coro = task.get_coro()
    close = getattr(coro, "close", None)
    if close is None:
        return
    try:
        close()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass


def _location(exc: BaseException) -> str:
    """``file:line`` of the deepest non-framework frame where ``exc`` was raised."""
    tb: TracebackType | None = exc.__traceback__
    location = "?"
    while tb is not None:
        filename = tb.tb_frame.f_code.co_filename
        if not filename.startswith(_INTERNAL_DIRS):
            location = f"{os.path.basename(filename)}:{tb.tb_lineno}"
        tb = tb.tb_next
    return location


def _predicate_name(predicate: Predicate) -> str:
    name = getattr(predicate, "__qualname__", None)
    return name if isinstance(name, str) else repr(predicate)
