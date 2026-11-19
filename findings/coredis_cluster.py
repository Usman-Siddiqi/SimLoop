"""coredis 6.9.0 RedisCluster under migrations, failovers and resets: nothing found.

The cluster harness of ``redis_cluster.py`` (the simulated cluster in
``_resp_cluster``, speaking RESP3 as coredis requires) with coredis's
cluster client. Four tasks write unique tokens to their own keys and read
them back, some through non-transactional pipelines, while slots migrate
(quickly, or slowly as ``redis-cli --cluster reshard`` moves them),
primaries fail over to their replicas, TCP connections reset and
``wait_for`` cancels writes. Checked: a read returns the last acknowledged
write or one whose outcome was unknown; a pipelined ``GET`` sees the
``SET`` before it; no command gives up with "Maximum retries exhausted"
while no node is down; after the faults stop every key holds such a value;
and once the client is closed no connection is left open.

All of that held, random and fifo. coredis sends ``ASKING`` on the
connection that then carries the redirected command, which redis-py does
not (finding 43).

Run: python findings/coredis_cluster.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import coredis
from _resp_cluster import Cluster, key_slot

import simloop

logging.getLogger("coredis").setLevel(logging.CRITICAL)
ERRORS = (coredis.exceptions.RedisError, ConnectionError, OSError, TimeoutError)
WORKLOADS: dict[str, dict[str, Any]] = {
    "no faults": {},
    "migrations, pipelines, wait_for": {"migrate": 0.8, "pipe": 0.3, "cancel": 0.3},
    "slow migrations, pipelines": {"migrate": 0.9, "pause": (0.05, 0.4), "pipe": 0.3},
    "migrations, failovers, resets, wait_for": {
        "migrate": 0.5,
        "failover": 0.3,
        "reset": 0.002,
        "cancel": 0.2,
        "pipe": 0.2,
    },
}


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        rng = simloop.rng()
        net = simloop.network()
        cluster = Cluster(["r0", "r1", "r2"], ["r3", "r4", "r5"], resp3=True)
        for name, node in cluster.nodes.items():
            net.add_host(name, entry=node.serve)
            net.set_link("main", name, latency=(0.0005, 0.005))
        await asyncio.sleep(0.05)
        client: Any = coredis.RedisCluster(
            startup_nodes=[coredis.connection.TCPLocation(n, 6379) for n in ("r0", "r1", "r2")],
            stream_timeout=1.0,
        )
        keys = [[f"w{n}:k{j}".encode() for j in range(3)] for n in range(4)]
        unknown: dict[bytes, set[bytes]] = {k: set() for row in keys for k in row}
        final: dict[bytes, set[bytes]] = {}
        wrong: list[str] = []
        stop = asyncio.Event()
        faulty = bool(knobs.get("failover") or knobs.get("reset"))

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

        async def step(n: int, i: int, last: dict[bytes, set[bytes]]) -> None:
            key = rng.choice(keys[n])
            token = f"{n}-{i}".encode()
            try:
                unknown[key].add(token)
                if rng.random() < knobs.get("pipe", 0):
                    async with client.pipeline(transaction=False) as pipe:
                        first = pipe.set(key, token)
                        second = pipe.get(key)
                    replies = [await first, await second]
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
            except coredis.exceptions.ClusterError as exc:
                if not faulty:
                    wrong.append(f"{key!r}: {exc}")
            except ERRORS:
                pass

        async def worker(n: int) -> None:
            last: dict[bytes, set[bytes]] = {k: {b""} for k in keys[n]}
            for j in range(60):
                await step(n, j, last)
                await asyncio.sleep(rng.uniform(0, 0.03))
            final.update({key: last[key] for key in keys[n]})

        async with client:
            for name in cluster.nodes:
                net.set_link("main", name, latency=(0.0005, 0.005), reset=knobs.get("reset", 0.0))
            chaos = asyncio.create_task(nemesis())
            tasks = [asyncio.create_task(worker(n)) for n in range(4)]
            done, pending = await asyncio.wait(tasks, timeout=300)
            stop.set()
            await chaos
            assert not pending, f"{len(pending)} worker(s) stuck"
            for task in done:
                task.result()
            simloop.log(f"redirects answered by the servers: {dict(cluster.redirects)}")
            assert not wrong, wrong[:3]
            for name in cluster.nodes:
                net.set_link("main", name, latency=0.001, reset=0.0)
                if not net.host(name).up:
                    net.host(name).restart()
            await asyncio.sleep(2)
            for key, possible in final.items():
                got = await asyncio.wait_for(client.get(key), 10) or b""
                assert got in possible | unknown[key], f"{key!r} is {got!r} after the faults"
        await asyncio.sleep(1)
        left = net.host("main").connections
        assert not left, f"{len(left)} connections left open after the client was closed"

    return harness


def main() -> int:
    configs = [("random", simloop.SimConfig(step_time=1e-6))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(30), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            assert report.ok, report.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
