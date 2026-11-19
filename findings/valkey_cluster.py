"""valkey-py 6.2.0 ValkeyCluster: redis-py's cluster bugs, plus connections left on dropped nodes.

valkey-py forked redis-py, and its asyncio cluster client still has the code
behind findings 41 to 43: ``aclose()`` returns without closing anything while
the refresh flag is set (here: right after the ``reinitialize_steps``-th
``MOVED``), ``reinitialize_steps=1`` fails every moved command with "TTL
exhausted", and ``ASKING`` goes out on a different pooled connection than
the command it is meant for. The harness is the one in ``redis_cluster.py``
with valkey's classes, and the plain-asyncio reproductions are the same.

One more, valkey only: ``NodesManager.initialize`` builds a new
``ClusterNode`` for every address on each topology refresh and disconnects
the old ones. A command that already holds an old node (between ``ASKING``
and the command, or retrying after the ``MOVED`` that reset the client)
reconnects it after that disconnect, because a connection connects itself
on the next send. Nothing references the old node any more, so neither
``aclose()`` nor a later refresh closes that connection: it stays open until
the node is garbage collected ("Unclosed ClusterNode object"). redis-py
keeps the existing node for an address, and disconnects connections that
were busy during a refresh once they are released. Disconnecting a dropped
node again whenever a command on it finishes passes.

Run: python findings/valkey_cluster.py
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import re
import sys
from collections.abc import Callable
from typing import Any

import redis_cluster as base
from valkey.asyncio.cluster import ClusterNode, NodesManager, ValkeyCluster
from valkey.exceptions import ClusterError, ValkeyClusterException, ValkeyError

import simloop

logging.getLogger("valkey").setLevel(logging.CRITICAL)
ERRORS = (ValkeyError, ValkeyClusterException, ConnectionError, OSError, TimeoutError)
VALKEY = base.Library(ValkeyCluster, ClusterNode, ClusterError, ERRORS)
SLOW = {"migrate": 0.9, "pause": (0.05, 0.4)}
CHAOS = {"migrate": 0.6, "failover": 0.25, "reset": 0.002, "cancel": 0.2}
WORKLOADS: dict[str, dict[str, Any]] = {
    "no faults": {},
    "slow migrations, aclose fixed": {**SLOW, "fix": ("aclose",)},
    "migrations, failovers, resets, wait_for, aclose and dropped nodes fixed": {
        **CHAOS,
        "fix": ("aclose", "dropped"),
    },
    "slow migrations, pipelines, wait_for, all fixed": {
        **SLOW,
        "pipe": 0.3,
        "cancel": 0.3,
        "fix": ("aclose", "dropped", "asking"),
    },
    "migrations, reinitialize_steps=1": {"migrate": 0.8, "steps": 1},
    "migrations, reinitialize_steps=1, all fixed": {
        "migrate": 0.8,
        "steps": 1,
        "fix": ("aclose", "dropped", "moved"),
    },
}
# A client dropped with its connections open is also reported when collected.
LEAK = ("connections left open", "Unclosed ClusterNode")
FAILING = {
    "slow migrations, aclose fixed": (*LEAK, "TTL exhausted"),
    "migrations, reinitialize_steps=1": ("TTL exhausted", *LEAK),
}


def fix_dropped() -> Callable[[], None]:
    """Disconnect a node dropped by a refresh again whenever a command on it finishes."""
    original_close = NodesManager._close_pending_node_disconnects
    original_execute = ClusterNode.execute_command
    # ClusterNode has __slots__, so the marks live here, for one run at a time.
    dropped: dict[int, ClusterNode] = {}

    async def close(self: Any) -> None:
        dropped.update((id(node), node) for node in self._pending_node_disconnects.values())
        await original_close(self)

    async def execute_command(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return await original_execute(self, *args, **kwargs)
        finally:
            if dropped.get(id(self)) is self:
                with contextlib.suppress(*ERRORS):
                    await self.disconnect()

    NodesManager._close_pending_node_disconnects = close  # type: ignore[method-assign]
    ClusterNode.execute_command = execute_command  # type: ignore[method-assign]

    def undo() -> None:
        NodesManager._close_pending_node_disconnects = original_close  # type: ignore[method-assign]
        ClusterNode.execute_command = original_execute  # type: ignore[method-assign]
        dropped.clear()

    return undo


def fix_moved() -> Callable[[], None]:
    """As redis-py's #4322: initialize again right after the reset at the MOVED threshold."""
    original = ValkeyCluster._reset_for_reinitialize

    async def reset(self: Any) -> None:
        await original(self)
        with contextlib.suppress(*ERRORS):
            await self.initialize()

    ValkeyCluster._reset_for_reinitialize = reset  # type: ignore[method-assign]

    def undo() -> None:
        ValkeyCluster._reset_for_reinitialize = original  # type: ignore[method-assign]

    return undo


def fix_asking() -> Callable[[], None]:
    """Send ASKING on the connection that carries the redirected command."""
    original = ClusterNode.execute_command
    pending: contextvars.ContextVar[ClusterNode | None] = contextvars.ContextVar(
        "pending_asking", default=None
    )

    async def execute_command(self: Any, *args: Any, **kwargs: Any) -> Any:
        if args == ("ASKING",):
            pending.set(self)
            return b"OK"
        if pending.get() is not self:
            return await original(self, *args, **kwargs)
        pending.set(None)
        connection = self.acquire_connection()
        try:
            commands = [("ASKING",), args]
            await connection.send_packed_command(connection.pack_commands(commands), False)
            await connection.read_response()
            return await self.parse_response(connection, args[0], **kwargs)
        finally:
            self._free.append(connection)

    ClusterNode.execute_command = execute_command  # type: ignore[method-assign]

    def undo() -> None:
        ClusterNode.execute_command = original  # type: ignore[method-assign]

    return undo


def main() -> int:
    configs = [("random", simloop.SimConfig(step_time=1e-6))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    fixes: dict[str, Callable[[], Callable[[], None]]] = {
        "aclose": lambda: base.fix_aclose(ValkeyCluster),
        "dropped": fix_dropped,
        "moved": fix_moved,
        "asking": fix_asking,
    }
    for workload, knobs in WORKLOADS.items():
        # Applied in this order, so fix_dropped wraps the ASKING fix.
        wanted = knobs.get("fix", ())
        undos = [fixes[fix]() for fix in ("asking", "aclose", "moved", "dropped") if fix in wanted]
        try:
            for name, config in configs:
                harness = base.make_harness(knobs, VALKEY)
                report = simloop.explore(harness, seeds=range(30), config=config)
                print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
                if workload not in FAILING:
                    assert report.ok, report.summary()
                    continue
                assert report.groups, report.summary()
                for group in report.groups:
                    assert any(part in group.message for part in FAILING[workload]), group.message
        finally:
            for undo in reversed(undos):
                undo()
    moved = asyncio.run(base.plain(17410, "steps=5", VALKEY))
    print(f"real asyncio loop, reinitialize_steps=5: {moved}")
    assert re.search(r"before aclose\(\) ([1-9]\d*), after \1$", moved), moved
    exhausted = asyncio.run(base.plain(17420, "steps=1", VALKEY))
    print(f"real asyncio loop, reinitialize_steps=1: {exhausted}")
    assert "TTL exhausted" in exhausted, exhausted
    asked = asyncio.run(base.plain(17430, "ask", VALKEY))
    print(f"real asyncio loop, a key mid-migration: {asked}")
    assert "TTL exhausted" in asked, asked
    return 0


if __name__ == "__main__":
    sys.exit(main())
