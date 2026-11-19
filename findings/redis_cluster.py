"""redis-py 8.1.0 RedisCluster: aclose() leaks, MOVED and ASK redirects end in "TTL exhausted".

A simulated Redis Cluster (``_resp_cluster``: three primaries with
synchronously replicated replicas, ``MOVED``/``ASK``/``TRYAGAIN``, slot
migrations key by key, failovers) and the asyncio cluster client. Four
tasks write unique tokens to their own keys and read them back while slots
migrate between primaries, primaries crash and their replicas take over,
TCP connections reset and ``wait_for`` cancels commands. Checked: a read
returns the last acknowledged write, or a write whose outcome was unknown
(cancelled or failed, which may still land later); after the faults stop
every key holds such a value; no command fails with "TTL exhausted"; and
once a client is closed, no TCP connection is left open. With the
``aclose()`` fix below, all of that held, random and fifo.

``aclose()``: the client uses one flag, ``_initialize``, both for "closed"
and for "refresh the topology before the next command". A connection error
or timeout sets it, and so does every ``reinitialize_steps``-th MOVED
(which closes the client and then retries the command on the node the
redirect named, reconnecting). ``aclose()`` returns at once when the flag is
set. So a client closed after a failed command, or right after the
command that crossed the MOVED threshold, keeps its connections open; with
``async with RedisCluster(...)``, every block whose last command failed
leaks them. Fixed on master, where ``aclose()`` checks for a topology
instead (apparently with #4448, after the release); closing whenever asked
passes.

``reinitialize_steps=1`` ("to reinitialize the cluster on every MOVED
error", as documented): the MOVED handler closes the client and retries
with the stale slot map, gets MOVED again, closes again, and so on until
"TTL exhausted" is raised to the caller: every command that meets a moved
slot fails. Known upstream: fixed by #4322 (merged after the release),
which initializes after closing; doing that passes.

``ASK`` (a slot mid-migration, its key already on the importing node): the
client sends ``ASKING`` and then the command as two separate
``ClusterNode.execute_command`` calls, and each takes a connection from the
node's pool. ``ASKING`` only applies to the next command on its own
connection, and the pool hands connections out first in, first out, so once
the importing node has two idle connections the command goes out on the
other one, without ``ASKING``. The node answers ``MOVED`` back to the old
owner, which answers ``ASK`` again, until "TTL exhausted". Slow migrations
(as ``redis-cli --cluster reshard`` runs them) fail commands this way;
sending ``ASKING`` and the command together on one connection passes. Still
the code on master.

Run: python findings/redis_cluster.py
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import logging
import re
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from _resp_cluster import Cluster, key_slot
from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.exceptions import ClusterError, RedisClusterException, RedisError

import simloop

logging.getLogger("redis").setLevel(logging.CRITICAL)
ERRORS = (RedisError, RedisClusterException, ConnectionError, OSError, TimeoutError)


@dataclasses.dataclass(frozen=True)
class Library:
    """The client under test, so that forks (valkey-py) can reuse the harness."""

    cluster: Any
    node: Any
    cluster_error: type[Exception]
    errors: tuple[type[BaseException], ...]


REDIS = Library(RedisCluster, ClusterNode, ClusterError, ERRORS)
WORKLOADS: dict[str, dict[str, Any]] = {
    "no faults": {},
    "migrations, wait_for, aclose fixed": {"migrate": 0.8, "cancel": 0.3, "fix": ("aclose",)},
    "migrations, failovers, resets, wait_for, aclose fixed": {
        "migrate": 0.6,
        "failover": 0.15,
        "reset": 0.005,
        "cancel": 0.2,
        "fix": ("aclose",),
    },
    "failovers, a client per batch": {"failover": 0.3, "batch": 4},
    "failovers, a client per batch, aclose fixed": {
        "failover": 0.3,
        "batch": 4,
        "fix": ("aclose",),
    },
    "slow migrations": {"migrate": 0.9, "pause": (0.05, 0.4), "fix": ("aclose",)},
    "slow migrations, pipelines, wait_for, ASKING fixed": {
        "migrate": 0.9,
        "pause": (0.05, 0.4),
        "pipe": 0.5,
        "cancel": 0.2,
        "fix": ("aclose", "asking"),
    },
    "migrations, reinitialize_steps=1": {"migrate": 0.8, "steps": 1},
    "migrations, reinitialize_steps=1, both fixed": {
        "migrate": 0.8,
        "steps": 1,
        "fix": ("aclose", "moved"),
    },
}
FAILING = {
    # A client dropped with its connections open is also reported when collected.
    "failovers, a client per batch": ("connections left open", "Unclosed ClusterNode"),
    "slow migrations": ("TTL exhausted",),
    "migrations, reinitialize_steps=1": ("TTL exhausted",),
}


def make_harness(
    knobs: dict[str, Any], lib: Library = REDIS
) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Reads see possible writes, no TTL exhausted, closed clients leave nothing open."""
        rng = simloop.rng()
        net = simloop.network()
        cluster = Cluster(["r0", "r1", "r2"], ["r3", "r4", "r5"])
        for name, node in cluster.nodes.items():
            net.add_host(name, entry=node.serve)
            net.set_link("main", name, latency=(0.0005, 0.005))
        await asyncio.sleep(0.05)

        def connect() -> Any:
            return lib.cluster(
                startup_nodes=[lib.node(name, 6379) for name in ("r0", "r1", "r2")],
                protocol=2,
                socket_timeout=1.0,
                reinitialize_steps=knobs.get("steps", 5),
            )

        shared = connect()
        await shared.initialize()
        for name in cluster.nodes:
            net.set_link("main", name, latency=(0.0005, 0.005), reset=knobs.get("reset", 0.0))
        keys = [[f"w{n}:k{j}".encode() for j in range(3)] for n in range(4)]
        # A SET that was cancelled or failed may still be applied at any later time.
        unknown: dict[bytes, set[bytes]] = {k: set() for row in keys for k in row}
        final: dict[bytes, set[bytes]] = {}
        wrong: list[str] = []
        stop = asyncio.Event()

        async def nemesis() -> None:
            hot = sorted({key_slot(k) for row in keys for k in row})
            while not stop.is_set():
                await asyncio.sleep(rng.uniform(0.05, 0.3))
                roll = rng.random()
                if roll < knobs.get("migrate", 0.0):
                    slot = rng.choice(hot)
                    others = [p for p in sorted(set(cluster.owner)) if p != cluster.owner[slot]]
                    pause = knobs.get("pause", (0.0, 0.02))
                    await cluster.migrate(slot, rng.choice(others), pause=pause)
                elif roll < knobs.get("migrate", 0.0) + knobs.get("failover", 0.0):
                    victim = rng.choice(sorted(set(cluster.owner)))
                    await cluster.failover(victim, rng.uniform(0.2, 1.5))
                    await asyncio.sleep(rng.uniform(0.1, 0.5))
                    net.host(victim).restart()

        async def step(client: Any, n: int, i: int, last: dict[bytes, set[bytes]]) -> None:
            key = rng.choice(keys[n])
            token = f"{n}-{i}".encode()
            try:
                unknown[key].add(token)
                if rng.random() < knobs.get("pipe", 0):
                    async with client.pipeline(transaction=knobs.get("tx", False)) as pipe:
                        pipe.set(key, token)
                        pipe.get(key)
                        replies = await pipe.execute()
                    unknown[key].discard(token)
                    if replies[1] != token:
                        wrong.append(f"{key!r}: pipelined SET {token!r}, GET gave {replies}")
                    last[key] = {token}
                    return
                if rng.random() < knobs.get("cancel", 0):
                    try:
                        await asyncio.wait_for(client.set(key, token), rng.uniform(0, 0.02))
                    except TimeoutError:
                        return
                else:
                    await client.set(key, token)
                unknown[key].discard(token)
                last[key] = {token}
                got = await client.get(key) or b""
                if got not in last[key] | unknown[key]:
                    wrong.append(f"{key!r}: read {got!r}, expected {sorted(last[key])}")
                last[key] = {got}
            except lib.cluster_error as exc:
                if "TTL exhausted" in str(exc):
                    wrong.append(f"{key!r}: {exc}")
            except lib.errors:
                pass

        async def worker(n: int) -> None:
            last: dict[bytes, set[bytes]] = {k: {b""} for k in keys[n]}
            batch = knobs.get("batch")
            for i in range(0, 80, batch or 80):
                if batch:
                    with contextlib.suppress(*lib.errors):
                        async with connect() as client:
                            for j in range(i, i + batch):
                                await step(client, n, j, last)
                                await asyncio.sleep(rng.uniform(0, 0.03))
                else:
                    for j in range(80):
                        await step(shared, n, j, last)
                        await asyncio.sleep(rng.uniform(0, 0.03))
            final.update({key: last[key] for key in keys[n]})

        chaos = asyncio.create_task(nemesis())
        tasks = [asyncio.create_task(worker(n)) for n in range(4)]
        done, pending = await asyncio.wait(tasks, timeout=300)
        stop.set()
        await chaos
        assert not pending, f"{len(pending)} worker(s) stuck"
        for task in done:
            task.result()
        assert not wrong, wrong[:3]
        for name in cluster.nodes:
            net.set_link("main", name, latency=0.001, reset=0.0)
            if not net.host(name).up:
                net.host(name).restart()
        await asyncio.sleep(2)
        for key, possible in final.items():
            got = await asyncio.wait_for(shared.get(key), 10) or b""
            assert got in possible | unknown[key], f"{key!r} is {got!r} after the faults"
        await shared.aclose()
        await asyncio.sleep(1)
        left = net.host("main").connections
        assert not left, f"{len(left)} connections left open after every client was closed"

    return harness


def fix_aclose(cluster: Any = RedisCluster) -> Callable[[], None]:
    """Close the nodes whenever asked, whatever the refresh flag says."""
    original = cluster.aclose

    async def aclose(self: Any) -> None:
        self._initialize = True
        await self.nodes_manager.aclose()
        await self.nodes_manager.aclose("startup_nodes")

    cluster.aclose = aclose

    def undo() -> None:
        cluster.aclose = original

    return undo


def fix_moved() -> Callable[[], None]:
    """As in #4322: when the MOVED handler closes the client, initialize it again."""
    original_execute = RedisCluster._execute_command
    original_aclose = RedisCluster.aclose
    inside = contextvars.ContextVar("inside", default=False)

    async def execute(self: Any, *args: Any, **kwargs: Any) -> Any:
        token = inside.set(True)
        try:
            return await original_execute(self, *args, **kwargs)
        finally:
            inside.reset(token)

    async def aclose(self: Any) -> None:
        await RedisCluster._closing(self)
        if inside.get():
            await self.initialize()

    RedisCluster._closing = RedisCluster.aclose  # type: ignore[attr-defined]
    RedisCluster._execute_command = execute  # type: ignore[method-assign]
    RedisCluster.aclose = aclose  # type: ignore[method-assign]

    def undo() -> None:
        RedisCluster._execute_command = original_execute  # type: ignore[method-assign]
        RedisCluster.aclose = original_aclose  # type: ignore[method-assign]
        del RedisCluster._closing  # type: ignore[attr-defined]

    return undo


def fix_asking() -> Callable[[], None]:
    """Send ASKING on the connection that carries the redirected command."""
    original = ClusterNode.execute_command
    pending: contextvars.ContextVar[ClusterNode | None] = contextvars.ContextVar(
        "pending_asking", default=None
    )

    async def execute_command(self: Any, *args: Any, asking: bool = False, **kwargs: Any) -> Any:
        if args == ("ASKING",):
            pending.set(self)
            return b"OK"
        if pending.get() is not self:
            return await original(self, *args, asking=asking, **kwargs)
        pending.set(None)
        connection = self.acquire_connection()
        try:
            await self.disconnect_if_needed(connection)
            await connection.send_packed_command(connection.pack_commands([("ASKING",), args]))
            await connection.read_response()
            return await self.parse_response(connection, args[0], **kwargs)
        finally:
            try:
                await self.disconnect_if_needed(connection)
            finally:
                self.release(connection)

    ClusterNode.execute_command = execute_command  # type: ignore[method-assign]

    def undo() -> None:
        ClusterNode.execute_command = original  # type: ignore[method-assign]

    return undo


def open_sessions() -> int:
    return sum(1 for task in asyncio.all_tasks() if "RespServer.handle" in repr(task.get_coro()))


async def plain(port: int, scenario: str, lib: Library = REDIS) -> str:
    """On a real loop: a cluster on localhost, then aclose() after an error or a MOVED."""
    names = ["a", "b", "c"]
    cluster = Cluster(names, addresses={n: ("127.0.0.1", port + i) for i, n in enumerate(names)})
    servers = {
        name: await asyncio.start_server(node.handle, "127.0.0.1", cluster.endpoint(name)[1])
        for name, node in cluster.nodes.items()
    }
    steps = 1 if scenario == "steps=1" else 5
    client = lib.cluster(host="127.0.0.1", port=port, protocol=2, reinitialize_steps=steps)
    await client.initialize()
    keys = [f"k{i}".encode() for i in range(30)]
    for key in keys:
        await client.set(key, b"v")
    if scenario == "lost node":
        servers["c"].close()
        for task in list(asyncio.all_tasks()):
            frame = task.get_coro().cr_frame  # type: ignore[union-attr]
            if frame is not None and frame.f_locals.get("self") is cluster.nodes["c"]:
                task.cancel()
        await asyncio.sleep(0.1)
        try:
            await client.get(next(k for k in keys if cluster.owner[key_slot(k)] == "c"))
            outcome = "GET answered"
        except lib.errors as exc:
            outcome = f"GET raised {type(exc).__name__}"
    elif scenario == "ask":
        key = keys[0]
        slot = key_slot(key)
        source = cluster.owner[slot]
        target = next(n for n in names if n != source)
        # Two concurrent commands leave the importing node with two pooled connections.
        on_target = [k for k in keys if cluster.owner[key_slot(k)] == target][:2]
        await asyncio.gather(*(client.get(k) for k in on_target))
        pooled = len(client.get_node(*cluster.endpoint(target))._connections)
        # Mid-migration, as redis-cli --cluster reshard leaves it: the key already moved.
        cluster.migrating[slot] = (source, target)
        cluster.nodes[target].adopt(cluster.nodes[source], key)
        cluster.nodes[source].remove(key)
        try:
            outcome = f"GET answered {await client.get(key)!r}"
        except lib.errors as exc:
            outcome = f"GET raised {type(exc).__name__}: {exc}"
        outcome += f" with {pooled} pooled connections; redirects {dict(cluster.redirects)}"
        cluster.migrating.pop(slot)
    else:
        for key in keys[:5]:
            slot = key_slot(key)
            target = next(n for n in names if n != cluster.owner[slot])
            await cluster.migrate(slot, target, pause=(0, 0))
        results = []
        for key in keys[:5]:
            try:
                results.append((await client.get(key)).decode())
            except lib.errors as exc:
                results.append(str(exc))
        outcome = f"GETs of 5 moved keys: {results}"
    before = open_sessions()
    await client.aclose()
    await asyncio.sleep(0.2)
    outcome += f"; server connections before aclose() {before}, after {open_sessions()}"
    for server in servers.values():
        server.close()
    return outcome


def main() -> int:
    configs = [("random", simloop.SimConfig(step_time=1e-6))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    fixes = {"aclose": fix_aclose, "moved": fix_moved, "asking": fix_asking}
    for workload, knobs in WORKLOADS.items():
        undos = [fixes[fix]() for fix in knobs.get("fix", ())]
        try:
            for name, config in configs:
                report = simloop.explore(make_harness(knobs), seeds=range(30), config=config)
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
    lost = asyncio.run(plain(17100, "lost node"))
    print(f"real asyncio loop, a node lost: {lost}")
    assert re.search(r"before aclose\(\) ([1-9]\d*), after \1$", lost), lost
    moved = asyncio.run(plain(17110, "steps=5"))
    print(f"real asyncio loop, reinitialize_steps=5: {moved}")
    assert re.search(r"before aclose\(\) ([1-9]\d*), after \1$", moved), moved
    exhausted = asyncio.run(plain(17120, "steps=1"))
    print(f"real asyncio loop, reinitialize_steps=1: {exhausted}")
    assert "TTL exhausted" in exhausted, exhausted
    asked = asyncio.run(plain(17130, "ask"))
    print(f"real asyncio loop, a key mid-migration: {asked}")
    assert "TTL exhausted" in asked, asked
    return 0


if __name__ == "__main__":
    sys.exit(main())
