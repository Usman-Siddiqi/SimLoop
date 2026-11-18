"""Declarative fault schedules.

A schedule says *what* goes wrong and *when*, instead of writing a nemesis
loop by hand::

    plan = faults.schedule(
        faults.at(1.0, faults.crash("db1")),
        faults.between(2.0, 4.0, faults.partition(["db1"], ["db2", "db3"])),
        faults.during((5, 10), (0.5, 2), faults.pause(faults.any_of("db1", "db2"))),
        faults.every((1, 3), faults.degrade_link("app", "db1", drop=0.3), duration=0.5),
    )
    plan.start()          # background task; or `await plan.run()`

Times are seconds after the schedule starts. Any time, duration, period or
target can be a fixed value or a random choice (a ``(low, high)`` range,
:func:`any_of`), drawn from the run's choice source when it is needed. That
keeps runs reproducible from the seed and lets shrinking move a fault to the
low end of its range, so a minimized failure shows the earliest, shortest
fault that still breaks the system.

``during(start, duration, ...)`` and ``between(start, end, ...)`` apply a
fault and undo it later: a crash restarts, a pause
resumes, a partition heals (only the links it cut, so overlapping
partitions compose), a degraded link gets its old settings back.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from ._runtime import current
from .net import Host, LinkConfig, Network

Range = float | tuple[float, float]


def _draw(value: Range) -> float:
    if isinstance(value, tuple):
        low, high = value
        if high < low:
            raise ValueError(f"range ({low}, {high}) is empty")
        return current().choices.uniform(low, high)
    return float(value)


@dataclass(frozen=True)
class AnyOf:
    """A target chosen from ``names`` when the fault is applied."""

    names: tuple[str, ...]

    def resolve(self) -> str:
        return self.names[current().choices.below(len(self.names))]


Target = str | Host | AnyOf


def any_of(*names: str | Host) -> AnyOf:
    """Pick one of these hosts, from the seed, each time the fault fires."""
    if not names:
        raise ValueError("any_of needs at least one host")
    return AnyOf(tuple(n.name if isinstance(n, Host) else n for n in names))


def _resolve(target: Target) -> str:
    if isinstance(target, AnyOf):
        return target.resolve()
    return target.name if isinstance(target, Host) else target


def _network() -> Network:
    return current().network


def _log(message: str) -> None:
    current().loop.log(f"schedule: {message}")


Undo = Callable[[], None]


class Fault:
    """Something that can go wrong. ``apply`` returns how to put it right."""

    def apply(self) -> Undo | None:
        raise NotImplementedError

    def describe(self) -> str:
        return type(self).__name__


@dataclass(frozen=True)
class _Crash(Fault):
    host: Target
    power_loss: bool | float = False

    def apply(self) -> Undo | None:
        host = _network().host(_resolve(self.host))
        if not host.up:
            _log(f"crash {host.name} skipped: already down")
            return None
        power_loss = self.power_loss
        if not isinstance(power_loss, bool):
            power_loss = current().choices.chance(power_loss)
        _log(f"crash {host.name}" + (" (power loss)" if power_loss else ""))
        host.crash(power_loss=power_loss)

        def undo() -> None:
            if not host.up:
                _log(f"restart {host.name}")
                host.restart()

        return undo

    def describe(self) -> str:
        return f"crash {self.host}"


@dataclass(frozen=True)
class _Restart(Fault):
    host: Target

    def apply(self) -> Undo | None:
        host = _network().host(_resolve(self.host))
        if host.up:
            _log(f"restart {host.name} skipped: already up")
        else:
            _log(f"restart {host.name}")
            host.restart()
        return None

    def describe(self) -> str:
        return f"restart {self.host}"


@dataclass(frozen=True)
class _Pause(Fault):
    host: Target

    def apply(self) -> Undo | None:
        host = _network().host(_resolve(self.host))
        if not host.up or host.paused:
            _log(f"pause {host.name} skipped")
            return None
        _log(f"pause {host.name}")
        host.pause()

        def undo() -> None:
            if host.paused:
                host.resume()

        return undo

    def describe(self) -> str:
        return f"pause {self.host}"


def _side(hosts: Target | Iterable[Target]) -> list[str]:
    if isinstance(hosts, str | Host | AnyOf):
        return [_resolve(hosts)]
    return [_resolve(h) for h in hosts]


@dataclass(frozen=True)
class _Partition(Fault):
    a: Any
    b: Any = None
    one_way: bool = False

    def apply(self) -> Undo | None:
        network = _network()
        side_a = _side(self.a)
        if self.b is None:
            side_b = [h.name for h in network.hosts if h.name not in side_a]
        else:
            side_b = _side(self.b)
        pairs = [(x, y) for x in side_a for y in side_b if x != y]
        if self.one_way:
            for x, y in pairs:
                network.block(x, y)
        else:
            network.partition(side_a, side_b)
        arrow = "->" if self.one_way else "|"
        _log(f"partition {sorted(side_a)} {arrow} {sorted(side_b)}")

        def undo() -> None:
            _log(f"heal {sorted(side_a)} {arrow} {sorted(side_b)}")
            links = pairs if self.one_way else pairs + [(y, x) for x, y in pairs]
            network.release(links)

        return undo

    def describe(self) -> str:
        return f"partition {self.a} | {self.b if self.b is not None else 'rest'}"


@dataclass(frozen=True)
class _DegradeLink(Fault):
    a: Target
    b: Target
    changes: dict[str, Any] = field(default_factory=dict)

    def apply(self) -> Undo | None:
        network = _network()
        a, b = _resolve(self.a), _resolve(self.b)
        before = (network.link(a, b), network.link(b, a))
        network.set_link(a, b, **self.changes)
        _log(f"degrade link {a} <-> {b}: {self.changes}")

        def undo() -> None:
            _log(f"restore link {a} <-> {b}")
            network.set_link(a, b, before[0], both_ways=False)
            network.set_link(b, a, before[1], both_ways=False)

        return undo

    def describe(self) -> str:
        return f"degrade link {self.a} <-> {self.b}"


@dataclass(frozen=True)
class _ClockJump(Fault):
    host: Target
    delta: Range

    def apply(self) -> Undo | None:
        host = _network().host(_resolve(self.host))
        delta = _draw(self.delta)
        host.clock_offset += delta
        _log(f"clock of {host.name} jumped {delta:+.3f}s")

        def undo() -> None:
            host.clock_offset -= delta
            _log(f"clock of {host.name} jumped back {-delta:+.3f}s")

        return undo

    def describe(self) -> str:
        return f"clock jump {self.host}"


@dataclass(frozen=True)
class _Call(Fault):
    action: Callable[[], Callable[[], None] | None]
    name: str

    def apply(self) -> Undo | None:
        _log(self.name)
        return self.action()

    def describe(self) -> str:
        return self.name


def crash(host: Target, *, power_loss: bool | float = False) -> Fault:
    """Crash ``host``; undone by restarting it. ``power_loss`` may be a probability."""
    return _Crash(host, power_loss)


def restart(host: Target) -> Fault:
    """Restart ``host`` if it is down."""
    return _Restart(host)


def pause(host: Target) -> Fault:
    """Freeze ``host`` (GC pause, stalled VM); undone by resuming it."""
    return _Pause(host)


def partition(a: Target | Iterable[Target], b: Target | Iterable[Target] | None = None) -> Fault:
    """Cut ``a`` off from ``b`` (default: everyone else); undone by healing those links."""
    return _Partition(a if isinstance(a, str | Host | AnyOf) else tuple(a), _freeze(b))


def partition_one_way(src: Target | Iterable[Target], dst: Target | Iterable[Target]) -> Fault:
    """Drop traffic from ``src`` to ``dst`` only."""
    return _Partition(
        src if isinstance(src, str | Host | AnyOf) else tuple(src), _freeze(dst), one_way=True
    )


def _freeze(value: Any) -> Any:
    if value is None or isinstance(value, str | Host | AnyOf):
        return value
    return tuple(value)


def isolate(host: Target) -> Fault:
    """Cut ``host`` off from every other host."""
    return _Partition(host, None)


def degrade_link(a: Target, b: Target, **changes: Any) -> Fault:
    """Change the link between ``a`` and ``b`` (``latency=``, ``drop=``, ``reset=``...)."""
    unknown = set(changes) - set(LinkConfig.__dataclass_fields__)
    if unknown:
        raise TypeError(f"unknown link settings: {', '.join(sorted(unknown))}")
    return _DegradeLink(a, b, dict(changes))


def clock_jump(host: Target, delta: Range) -> Fault:
    """Step ``host``'s wall clock by ``delta`` seconds; undone by stepping it back."""
    return _ClockJump(host, delta)


def call(action: Callable[[], Callable[[], None] | None], name: str | None = None) -> Fault:
    """A custom fault: ``action()`` does the damage and may return an undo function."""
    return _Call(action, name or str(getattr(action, "__name__", "custom fault")))


# -- timing ---------------------------------------------------------------------------


class Step:
    async def run(self, origin: float) -> None:
        raise NotImplementedError


async def _sleep_until(origin: float, offset: float) -> None:
    loop = asyncio.get_running_loop()
    delay = origin + offset - loop.time()
    if delay > 0:
        await asyncio.sleep(delay)


@dataclass(frozen=True)
class _At(Step):
    when: Range
    fault: Fault

    async def run(self, origin: float) -> None:
        await _sleep_until(origin, _draw(self.when))
        self.fault.apply()


@dataclass(frozen=True)
class _During(Step):
    start: Range
    duration: Range
    fault: Fault

    async def run(self, origin: float) -> None:
        start = _draw(self.start)
        duration = _draw(self.duration)
        await _sleep_until(origin, start)
        undo = self.fault.apply()
        await asyncio.sleep(duration)
        if undo is not None:
            undo()


@dataclass(frozen=True)
class _Every(Step):
    period: Range
    fault: Fault
    duration: Range | None
    start: Range
    until: float | None
    times: int | None

    async def run(self, origin: float) -> None:
        loop = asyncio.get_running_loop()
        await _sleep_until(origin, _draw(self.start))
        fired = 0
        while self.times is None or fired < self.times:
            await asyncio.sleep(_draw(self.period))
            if self.until is not None and loop.time() - origin >= self.until:
                return
            undo = self.fault.apply()
            fired += 1
            if self.duration is not None:
                await asyncio.sleep(_draw(self.duration))
                if undo is not None:
                    undo()


def at(when: Range, fault: Fault) -> Step:
    """Apply ``fault`` at ``when`` seconds (and leave it in place)."""
    return _At(when, fault)


def during(start: Range, duration: Range, fault: Fault) -> Step:
    """Apply ``fault`` at ``start`` and undo it ``duration`` seconds later."""
    return _During(start, duration, fault)


def between(start: float, end: float, fault: Fault) -> Step:
    """Apply ``fault`` from ``start`` until ``end`` (fixed times, in seconds)."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    return _During(start, end - start, fault)


def every(
    period: Range,
    fault: Fault,
    *,
    duration: Range | None = None,
    start: Range = 0.0,
    until: float | None = None,
    times: int | None = None,
) -> Step:
    """Apply ``fault`` repeatedly, waiting ``period`` before each time.

    With ``duration`` each occurrence is undone after that long. ``until``
    (seconds after the schedule starts) and ``times`` bound the repetition.
    """
    return _Every(period, fault, duration, start, until, times)


@dataclass(frozen=True)
class Schedule:
    """A set of timed faults that runs next to a workload."""

    steps: tuple[Step, ...]

    async def run(self) -> None:
        """Run every step to completion (forever, if a step repeats without bound)."""
        origin = asyncio.get_running_loop().time()
        async with asyncio.TaskGroup() as group:
            for index, step in enumerate(self.steps):
                group.create_task(step.run(origin), name=f"schedule-{index}")

    def start(self) -> asyncio.Task[None]:
        """Run the schedule as a background task named ``schedule``."""
        return asyncio.get_running_loop().create_task(self.run(), name="schedule")


def schedule(*steps: Step | Sequence[Step]) -> Schedule:
    """Combine steps (or lists of steps) into a :class:`Schedule`."""
    flat: list[Step] = []
    for item in steps:
        if isinstance(item, Step):
            flat.append(item)
        elif isinstance(item, Fault):
            raise TypeError(f"not a schedule step: {item.describe()} (wrap it in at/during/every)")
        else:
            flat.extend(item)
    for item in flat:
        if not isinstance(item, Step):
            raise TypeError(f"not a schedule step: {item!r} (wrap it in at/during/every)")
    return Schedule(tuple(flat))
