"""Fault injection: BUGGIFY points and nemesis tasks.

``buggify()`` is FoundationDB's idea: sprinkle it through your own code at
places where something unusual *could* happen (a short read, an early
timeout, a slow path) and the simulation decides, per run and from the seed,
which of those places misbehave.

The nemesis coroutines attack the system from outside, the way Jepsen does:
partitions, crashes, pauses and clock jumps on a seeded schedule. Run them as
background tasks next to the workload::

    asyncio.create_task(faults.random_partitions(every=(1, 5)))
    asyncio.create_task(faults.random_crashes(["db1", "db2"], every=(5, 20)))

For a fixed or partly random plan, use a declarative schedule instead
(:func:`schedule` with :func:`at`, :func:`during`, :func:`between` and
:func:`every`; see :mod:`simloop._schedule`).

Every decision is drawn from the run's choice source, so shrinking a failure
tends to remove faults that did not matter.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Sequence

from ._runtime import current, in_simulation
from ._schedule import (
    AnyOf,
    Fault,
    Schedule,
    Step,
    Undo,
    any_of,
    at,
    between,
    call,
    clock_jump,
    crash,
    degrade_link,
    during,
    every,
    isolate,
    partition,
    partition_one_way,
    pause,
    restart,
    schedule,
)
from .net import Host

__all__ = [
    "AnyOf",
    "Fault",
    "Schedule",
    "Step",
    "Undo",
    "any_of",
    "at",
    "between",
    "buggify",
    "call",
    "clock_jump",
    "clock_jumps",
    "crash",
    "degrade_link",
    "during",
    "every",
    "flaky_links",
    "isolate",
    "partition",
    "partition_one_way",
    "pause",
    "random_crashes",
    "random_partitions",
    "random_pauses",
    "restart",
    "schedule",
]

Range = float | tuple[float, float]

BUGGIFY_SITE_RATE = 0.25
"""Fraction of buggify sites that are switched on in a given run."""


def buggify(probability: float = 0.25) -> bool:
    """Return True when this call site should misbehave in this run.

    Each call site is enabled for the whole run with probability
    :data:`BUGGIFY_SITE_RATE`; an enabled site then fires with
    ``probability`` each time it is reached. Outside a simulation, or with
    ``SimConfig(buggify=False)``, it is always False, so it is safe to leave in
    production code.
    """
    if not in_simulation():
        return False
    sim = current()
    if not sim.config.buggify:
        return False
    caller = sys._getframe(1)
    site = (caller.f_code.co_filename, caller.f_lineno)
    enabled = sim.buggify_sites.get(site)
    if enabled is None:
        enabled = sim.choices.chance(BUGGIFY_SITE_RATE)
        sim.buggify_sites[site] = enabled
    if not enabled or not sim.choices.chance(probability):
        return False
    sim.loop.log(f"buggify fired at {caller.f_code.co_name}:{caller.f_lineno}")
    return True


def _draw(value: Range) -> float:
    if isinstance(value, tuple):
        return current().choices.uniform(*value)
    return value


def _hosts(names: Sequence[str | Host] | None) -> list[Host]:
    network = current().network
    if names is None:
        return [h for h in network.hosts if h is not network.default_host]
    return [n if isinstance(n, Host) else network.host(n) for n in names]


def _pick(hosts: list[Host]) -> Host:
    return hosts[current().choices.below(len(hosts))]


async def random_partitions(
    hosts: Sequence[str | Host] | None = None,
    *,
    every: Range = (1.0, 10.0),
    duration: Range = (0.5, 5.0),
    include_driver: bool = False,
) -> None:
    """Repeatedly split the hosts into two random sides, then heal."""
    network = current().network
    pool = _hosts(hosts)
    if include_driver:
        pool.append(network.default_host)
    if len(pool) < 2:
        raise ValueError("partitions need at least two hosts")
    while True:
        await asyncio.sleep(_draw(every))
        side = [h for h in pool if current().choices.chance(0.5)]
        if not side or len(side) == len(pool):
            side = [_pick(pool)]
        rest = [h for h in pool if h not in side]
        network.partition(side, rest)
        await asyncio.sleep(_draw(duration))
        network.heal()


async def random_crashes(
    hosts: Sequence[str | Host] | None = None,
    *,
    every: Range = (5.0, 30.0),
    downtime: Range = (0.5, 5.0),
    power_loss: float = 0.0,
) -> None:
    """Repeatedly crash a random host and restart it after some downtime."""
    pool = _hosts(hosts)
    if not pool:
        raise ValueError("no hosts to crash")
    while True:
        await asyncio.sleep(_draw(every))
        victim = _pick(pool)
        if not victim.up:
            continue
        victim.crash(power_loss=current().choices.chance(power_loss))
        await asyncio.sleep(_draw(downtime))
        victim.restart()


async def random_pauses(
    hosts: Sequence[str | Host] | None = None,
    *,
    every: Range = (2.0, 20.0),
    duration: Range = (0.1, 3.0),
) -> None:
    """Repeatedly freeze a random host for a while (GC pauses, stalled VMs)."""
    pool = _hosts(hosts)
    if not pool:
        raise ValueError("no hosts to pause")
    while True:
        await asyncio.sleep(_draw(every))
        victim = _pick(pool)
        if victim.up and not victim.paused:
            victim.pause(_draw(duration))


async def clock_jumps(
    hosts: Sequence[str | Host] | None = None,
    *,
    every: Range = (5.0, 30.0),
    jump: Range = (-5.0, 5.0),
) -> None:
    """Repeatedly step a random host's wall clock forwards or backwards (NTP fixes)."""
    pool = _hosts(hosts)
    if not pool:
        raise ValueError("no hosts to skew")
    while True:
        await asyncio.sleep(_draw(every))
        victim = _pick(pool)
        delta = _draw(jump)
        victim.clock_offset += delta
        current().loop.log(f"clock of {victim.name} jumped {delta:+.3f}s")


async def flaky_links(
    hosts: Sequence[str | Host] | None = None,
    *,
    every: Range = (2.0, 15.0),
    duration: Range = (1.0, 10.0),
    latency: tuple[float, float] = (0.05, 1.0),
    drop: float = 0.2,
) -> None:
    """Repeatedly degrade the link between two random hosts, then restore it."""
    network = current().network
    pool = _hosts(hosts)
    pool.append(network.default_host)
    while True:
        await asyncio.sleep(_draw(every))
        a = _pick(pool)
        b = _pick([h for h in pool if h is not a])
        before = network.link(a, b)
        network.set_link(a, b, latency=latency, drop=drop)
        current().loop.log(f"link {a.name}<->{b.name} degraded")
        await asyncio.sleep(_draw(duration))
        network.set_link(a, b, before)
