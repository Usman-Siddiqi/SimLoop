"""asyncua 2.1.0: reconnects flood the server with Republish requests and can lose monitored items.

An asyncua ``Server`` with one writable variable per worker and an asyncua
``Client`` with ``auto_reconnect=True`` on the simulated network. The
client subscribes to every variable; three tasks write increasing values to
their own variable and read them back. Checked: every read returns a value
that could be the latest; after the faults stop, a final write to each
variable reaches the subscription; reconnecting sends a bounded number of
Republish requests; and ``disconnect()`` leaves no TCP connection open.
That held with no faults and with ``wait_for`` cancellations, random and
fifo.

Republish: when the connection drops, the client reconnects, re-activates
its session and takes over its subscription. It then asks for the
notifications it missed with Republish requests, one sequence number after
another, until the server answers ``BadMessageNotAvailable``. The asyncua
server never does: for a sequence number it does not hold it returns an
empty ``NotificationMessage``, which the client accepts as a message. The
loop in ``Subscription._republish_gaps`` then never ends. On a real loop,
one dropped connection makes the client send thousands of Republish requests
a second for as long as it runs, and its supervisor never gets back to
watching the connection. Answering ``BadMessageNotAvailable`` for a missing
message (Part 4, 5.13.6) passes; the client could also stop when the
message's sequence number is not the one it asked for.

Monitored items: ``Subscription.recreate`` is not atomic. It clears its
monitored items, creates a new subscription, and only then adds the items
back and creates them on the server. If the connection drops while
``CreateSubscription`` is in flight, the items are gone, and the next
reconnect recreates a subscription that monitors nothing. If it drops
while ``CreateMonitoredItems`` is in flight, the client keeps its items
but the server's new subscription has none, and the next reconnect takes
that empty subscription over. Either way the client gets no data changes
again, with no error. The client recreates when it cannot take its
subscription over, as after a server restart (and, unfixed, when a reset
ends the Republish loop). With the Republish fix, a server restart and 6%
resets per segment lose the data changes in most runs. Keeping the items
when a recreate fails, and recreating again on the next reconnect, passes.

Run: python findings/asyncua_reconnect.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from asyncua import Client, Server, ua
from asyncua.common.subscription import Subscription
from asyncua.common.utils import ServiceError
from asyncua.server.internal_subscription import InternalSubscription

import simloop

logging.getLogger("asyncua").setLevel(logging.CRITICAL)
WORKERS = 3
WORKLOADS: dict[str, dict[str, Any]] = {
    "no faults": {},
    "wait_for": {"cancel": 0.3},
    "TCP resets": {"reset": 0.01},
    "TCP resets, both fixed": {"reset": 0.01},
    "server restart, TCP resets, Republish fixed": {"reset": 0.06, "restart": True},
    "server restart, TCP resets, wait_for, both fixed": {
        "reset": 0.06,
        "restart": True,
        "cancel": 0.3,
    },
}
FAILING = {
    "TCP resets": ("Republish requests", "subscription stopped"),
    "server restart, TCP resets, Republish fixed": ("subscription stopped",),
}
FIXES = {
    "TCP resets, both fixed": ("republish", "recreate"),
    "server restart, TCP resets, Republish fixed": ("republish",),
    "server restart, TCP resets, wait_for, both fixed": ("republish", "recreate"),
}
CHECKED = (ConnectionError, TimeoutError, ua.UaError)


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Reads possible, subscription alive, Republish bounded, nothing left open."""
        rng = simloop.rng()
        net = simloop.network()
        ready = asyncio.Event()
        node_ids: list[str] = []

        async def server() -> None:
            node_ids.clear()
            opcua = Server()
            await opcua.init()
            opcua.set_endpoint("opc.tcp://0.0.0.0:4840/")
            idx = await opcua.register_namespace("urn:sim")
            box = await opcua.nodes.objects.add_object(idx, "Box")
            for n in range(WORKERS):
                var = await box.add_variable(idx, f"v{n}", 0, varianttype=ua.VariantType.Int64)
                await var.set_writable()
                node_ids.append(var.nodeid.to_string())
            async with opcua:
                ready.set()
                await asyncio.Future()

        net.add_host("plc", entry=server)
        net.set_link("main", "plc", latency=(0.0005, 0.005))
        await ready.wait()
        client = Client(
            "opc.tcp://plc:4840/",
            timeout=2,
            watchdog_intervall=0.5,
            auto_reconnect=True,
            reconnect_max_delay=1.0,
        )
        await client.connect()
        session = client.uaclient.session
        republish = session.republish
        republished = [0]

        async def counting(*args: Any, **kwargs: Any) -> Any:
            republished[0] += 1
            return await republish(*args, **kwargs)

        session.republish = counting  # type: ignore[method-assign]
        latest: dict[str, int] = {}

        class Handler:
            def datachange_notification(self, node: Any, val: int, data: Any) -> None:
                latest[node.nodeid.to_string()] = val

        nodes = [client.get_node(node_id) for node_id in node_ids]
        subscription = await client.create_subscription(50, Handler())
        await subscription.subscribe_data_change(nodes)
        net.set_link("main", "plc", latency=(0.0005, 0.005), reset=knobs.get("reset", 0.0))
        wrong: list[str] = []

        async def restart() -> None:
            await asyncio.sleep(rng.uniform(0.1, 0.5))
            ready.clear()
            net.host("plc").crash()
            await asyncio.sleep(rng.uniform(0.1, 1))
            net.host("plc").restart()

        async def worker(n: int) -> None:
            node = nodes[n]
            possible = {0}
            for i in range(1, 21):
                with contextlib.suppress(*CHECKED):
                    possible.add(i)
                    if rng.random() < knobs.get("cancel", 0):
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(
                                node.write_value(i, ua.VariantType.Int64), rng.uniform(0, 0.02)
                            )
                            possible = {i}
                    else:
                        await node.write_value(i, ua.VariantType.Int64)
                        possible = {i}
                    got = await node.read_value()
                    if got not in possible and not (got == 0 and knobs.get("restart")):
                        wrong.append(f"v{n}: read {got}, expected one of {sorted(possible)}")
                    possible = {got}
                await asyncio.sleep(rng.uniform(0, 0.05))

        tasks = [asyncio.create_task(worker(n)) for n in range(WORKERS)]
        if knobs.get("restart"):
            tasks.append(asyncio.create_task(restart()))
        done, pending = await asyncio.wait(tasks, timeout=300)
        assert not pending, f"{len(pending)} worker(s) stuck"
        for task in done:
            task.result()
        assert not wrong, wrong[:3]
        net.set_link("main", "plc", latency=0.001, reset=0.0)
        await asyncio.sleep(8)
        for n, node in enumerate(nodes):
            await asyncio.wait_for(node.write_value(1000 + n, ua.VariantType.Int64), 10)
        await asyncio.sleep(1)
        assert republished[0] <= 100, (
            f"{republished[0]} Republish requests, still sending after the faults stopped"
        )
        missed = [node_id for n, node_id in enumerate(node_ids) if latest.get(node_id) != 1000 + n]
        assert not missed, (
            f"subscription stopped: no data change for {len(missed)} of {WORKERS} variables, "
            f"{len(subscription._monitored_items)} monitored item(s) left"
        )
        await client.disconnect()
        await asyncio.sleep(1)
        left = net.host("main").connections
        assert not left, f"connections left after disconnect(): {left}"

    return harness


def fix_republish() -> Callable[[], None]:
    """Answer a Republish for a message the server does not hold with BadMessageNotAvailable."""
    original = InternalSubscription.republish

    def republish(self: Any, nb: int) -> ua.NotificationMessage:
        result = self._not_acknowledged_results.pop(nb, None)
        if result is None:
            raise ServiceError(ua.StatusCodes.BadMessageNotAvailable)
        return result.NotificationMessage  # type: ignore[no-any-return]

    InternalSubscription.republish = republish  # type: ignore[method-assign]

    def undo() -> None:
        InternalSubscription.republish = original  # type: ignore[method-assign]

    return undo


def fix_recreate() -> Callable[[], None]:
    """Keep the monitored items when a recreate fails, and finish it on the next reconnect."""
    original_recreate = Subscription.recreate
    original_restore = Subscription.restore

    async def recreate(self: Any) -> None:
        saved = dict(self._monitored_items)
        try:
            await original_recreate(self)
        except BaseException:
            for handle, item in saved.items():
                self._monitored_items.setdefault(handle, item)
            self._recreate_pending = True
            raise
        self._recreate_pending = False

    async def restore(self: Any) -> None:
        if getattr(self, "_recreate_pending", False):
            await self.recreate()
        else:
            await original_restore(self)

    Subscription.recreate = recreate  # type: ignore[method-assign]
    Subscription.restore = restore  # type: ignore[method-assign]

    def undo() -> None:
        Subscription.recreate = original_recreate  # type: ignore[method-assign]
        Subscription.restore = original_restore  # type: ignore[method-assign]

    return undo


async def plain(port: int, *, drop_during_recreate: bool) -> str:
    """On a real loop: drop the client's connection once, or fail one recreate."""
    opcua = Server()
    await opcua.init()
    opcua.set_endpoint(f"opc.tcp://127.0.0.1:{port}/")
    idx = await opcua.register_namespace("urn:plain")
    var = await (await opcua.nodes.objects.add_object(idx, "Box")).add_variable(idx, "v", 0)
    await var.set_writable()
    seen: list[int] = []

    class Handler:
        def datachange_notification(self, node: Any, val: int, data: Any) -> None:
            seen.append(val)

    async with opcua:
        client = Client(f"opc.tcp://127.0.0.1:{port}/", auto_reconnect=True, watchdog_intervall=0.5)
        await client.connect()
        session = client.uaclient.session
        republish = session.republish
        republished = [0]

        async def counting(*args: Any, **kwargs: Any) -> Any:
            republished[0] += 1
            return await republish(*args, **kwargs)

        session.republish = counting  # type: ignore[method-assign]
        subscription = await client.create_subscription(50, Handler())
        await subscription.subscribe_data_change(client.get_node(var.nodeid))
        await asyncio.sleep(0.3)
        if drop_during_recreate:
            create = session.create_subscription

            async def lost(*args: Any, **kwargs: Any) -> Any:
                raise ConnectionError("connection lost during CreateSubscription")

            session.create_subscription = lost  # type: ignore[method-assign]
            with contextlib.suppress(ConnectionError):
                await subscription.recreate()
            session.create_subscription = create  # type: ignore[method-assign]
            await subscription.recreate()
            await var.write_value(5)
            await asyncio.sleep(0.5)
            outcome = (
                f"monitored items after the second recreate: {len(subscription._monitored_items)}, "
                f"data changes for a write of 5: {seen.count(5)}"
            )
        else:
            assert client.uaclient.protocol is not None
            assert client.uaclient.protocol.transport is not None
            client.uaclient.protocol.transport.abort()
            await asyncio.sleep(2)
            outcome = (
                f"Republish requests in the 2 s after one dropped connection: {republished[0]}"
            )
        await client.disconnect()
    return outcome


def main() -> int:
    configs = [("random", simloop.SimConfig(step_time=1e-6))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    fixes = {"republish": fix_republish, "recreate": fix_recreate}
    for workload, knobs in WORKLOADS.items():
        undos = [fixes[fix]() for fix in FIXES.get(workload, ())]
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
            for undo in undos:
                undo()
    flood = asyncio.run(plain(48401, drop_during_recreate=False))
    print(f"real asyncio loop: {flood}")
    assert int(flood.rsplit(" ", 1)[1]) > 1000, flood
    lost = asyncio.run(plain(48402, drop_during_recreate=True))
    print(f"real asyncio loop, one failed recreate: {lost}")
    assert lost.endswith("second recreate: 0, data changes for a write of 5: 0"), lost
    return 0


if __name__ == "__main__":
    sys.exit(main())
