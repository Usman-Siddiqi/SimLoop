"""amqtt 0.12.2: a QoS 2 message is lost when its PUBREL is re-sent right after CONNACK.

An amqtt broker and clients on the simulated network (keep-alive 5 s,
persistent sessions with a username, so the broker keeps them). One
subscriber takes ``t/#`` at QoS 2 and reconnects automatically; three
publishers send eight messages each at QoS 1 or 2. Checked: every message
whose ``publish()`` returned reaches the subscriber, a QoS 2 one exactly
once, and nobody hangs. That held with no faults, random and fifo.

With TCP resets (2% per segment) it fails. A subscriber that has answered
a QoS 2 ``PUBLISH`` with ``PUBREC`` keeps it in ``session.inflight_in`` until
the broker's ``PUBREL``. After a reconnect the broker re-sends that
``PUBREL`` straight after ``CONNACK``, as MQTT 3.1.1 requires
[MQTT-4.4.0-1], and the client's reader dispatches it before
``_retry_deliveries`` has started the flow that waits for it. With no
waiter, ``handle_pubrel`` only logs "Received PUBREL for unknown pending
message" and drops it: the message is never delivered and no ``PUBCOMP``
is sent, although a receiver must answer every ``PUBREL`` with one
(MQTT 3.1.1, section 4.3.3). The broker times out waiting
for ``PUBCOMP`` and forgets the message; the retry flow waits ten seconds
for a ``PUBREL`` that already came, which delays the reconnect past the
broker's keep-alive. A ``handle_pubrel`` that completes a message it
still holds in ``inflight_in`` (and always answers ``PUBCOMP``) passes.
The retry logic is new in 0.12 (PR #402), and the main branch is
unchanged here.

Also checked, with the same resets: a publisher nobody cancelled sees
only ordinary errors. It fails: when the connection drops while
``publish()`` waits for its ``PUBACK``, ``PUBREC`` or ``PUBCOMP``,
``ProtocolHandler.stop()`` cancels the waiter and ``publish()`` raises
``CancelledError`` into the caller. ``except Exception`` misses it, and
``TaskGroup`` or ``gather`` take it for a cancellation. Raising an
ordinary error there passes.

Noticed while writing the harness, and checked on a real loop: the broker
delivers every message at the subscription's QoS, so a QoS 0 publish
reaches a QoS 2 subscriber as QoS 2 ([MQTT-3.8.4-6] asks for the minimum
of the two). That is why QoS 1 publishes can be lost the same way here.

Run: python findings/amqtt_qos2_resume.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from amqtt.broker import Broker
from amqtt.client import MQTTClient
from amqtt.errors import AMQTTError
from amqtt.mqtt.protocol.handler import ProtocolHandler
from amqtt.mqtt.pubcomp import PubcompPacket
from amqtt.mqtt.pubrel import PubrelPacket
from amqtt.session import OUTGOING

import simloop

logging.getLogger("amqtt").setLevel(logging.CRITICAL)
logging.getLogger("transitions").setLevel(logging.CRITICAL)
WORKLOADS: dict[str, dict[str, Any]] = {
    "no faults": {},
    "no faults, publisher errors": {"own_errors": True},
    "TCP resets": {"reset": 0.02},
    "TCP resets, publisher errors": {"reset": 0.02, "own_errors": True},
}
FAILING = {
    "TCP resets": "publish returned",
    "TCP resets, publisher errors": "CancelledError in a task nobody cancelled",
}
CLIENT = {"auto_reconnect": True, "reconnect_retries": -1, "reconnect_max_interval": 1}
URI = "mqtt://u:pw@mq:1883/"


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Every message whose publish returned arrives; QoS 2 exactly once."""
        rng = simloop.rng()
        net = simloop.network()

        async def broker() -> None:
            await Broker(
                {
                    "listeners": {"default": {"type": "tcp", "bind": "0.0.0.0:1883"}},
                    "plugins": {
                        "amqtt.plugins.authentication.AnonymousAuthPlugin": {
                            "allow_anonymous": False
                        }
                    },
                }
            ).start()
            await asyncio.Future()

        net.add_host("mq", entry=broker)
        net.set_link("main", "mq", latency=(0.0005, 0.005))
        await asyncio.sleep(0.1)
        sub = MQTTClient(client_id="sub", config={**CLIENT, "keep_alive": 5})
        await sub.connect(URI, cleansession=False)
        await sub.subscribe([("t/#", 2)])
        got: list[str] = []

        async def receive() -> None:
            while True:
                try:
                    msg = await sub.deliver_message()
                except (AMQTTError, TimeoutError):
                    msg = None
                if msg is None:
                    await asyncio.sleep(0.05)
                    continue
                got.append(msg.data.decode())

        receiver = asyncio.create_task(receive())
        sent: dict[str, int] = {}
        stray: list[str] = []

        async def publisher(n: int) -> None:
            pub = MQTTClient(client_id=f"pub{n}", config={**CLIENT, "keep_alive": 5})
            await pub.connect(URI, cleansession=False)
            for i in range(8):
                tag = f"p{n}-{i}"
                qos = rng.choice([1, 2])
                try:
                    await pub.publish(f"t/{n}", tag.encode(), qos=qos)
                    sent[tag] = qos
                except asyncio.CancelledError:
                    task = asyncio.current_task()
                    if task is not None and task.cancelling():
                        raise
                    stray.append(tag)
                except (AMQTTError, TimeoutError) as exc:
                    simloop.log(f"{tag}: {type(exc).__name__}")
                await asyncio.sleep(rng.uniform(0, 0.02))
            with contextlib.suppress(AMQTTError, asyncio.CancelledError):
                await pub.disconnect()

        net.set_link("main", "mq", latency=(0.0005, 0.005), reset=knobs.get("reset", 0.0))
        tasks = [asyncio.create_task(publisher(n)) for n in range(3)]
        done, pending = await asyncio.wait(tasks, timeout=120)
        assert not pending, f"{len(pending)} publisher(s) stuck"
        for task in done:
            task.result()
        if knobs.get("own_errors"):
            assert not stray, f"publish() raised CancelledError in a task nobody cancelled: {stray}"
            receiver.cancel()
            return
        net.set_link("main", "mq", latency=0.001, reset=0.0)
        # Long enough for every retry (10 s each) and a reconnect after it.
        await asyncio.sleep(40)
        receiver.cancel()
        for tag, qos in sent.items():
            count = got.count(tag)
            if count == 0 or (qos == 2 and count > 1):
                raise AssertionError(
                    f"{tag} (QoS {qos}): publish returned, the subscriber got it {count} time(s)"
                )

    return harness


def fix_pubrel() -> Callable[[], None]:
    """``handle_pubrel`` that completes a message it holds and always answers PUBCOMP."""
    original_pubrel = ProtocolHandler.handle_pubrel
    original_flow = ProtocolHandler._handle_qos2_message_flow

    async def handle_pubrel(self: Any, pubrel: PubrelPacket) -> None:
        packet_id = pubrel.packet_id
        waiter = self._pubrel_waiters.get(packet_id)
        if waiter is not None and not waiter.done():
            waiter.set_result(pubrel)
            return
        message = self.session.inflight_in.pop(packet_id, None)
        if message is not None:
            await self.session.delivered_message_queue.put(message)
        await self._send_packet(PubcompPacket.build(packet_id))

    async def flow(self: Any, message: Any) -> None:
        released = message.pubrec_packet is not None and message.packet_id not in (
            self.session.inflight_in
        )
        if message.direction == OUTGOING or not released:
            await original_flow(self, message)

    ProtocolHandler.handle_pubrel = handle_pubrel  # type: ignore[method-assign]
    ProtocolHandler._handle_qos2_message_flow = flow  # type: ignore[method-assign]

    def undo() -> None:
        ProtocolHandler.handle_pubrel = original_pubrel  # type: ignore[method-assign]
        ProtocolHandler._handle_qos2_message_flow = original_flow  # type: ignore[method-assign]

    return undo


def fix_publish_errors() -> Callable[[], None]:
    """``mqtt_publish`` that reports a lost connection as an error, not a cancellation."""
    original = ProtocolHandler.mqtt_publish

    async def mqtt_publish(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return await original(self, *args, **kwargs)
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            raise AMQTTError("connection lost before the publish was acknowledged") from None

    ProtocolHandler.mqtt_publish = mqtt_publish  # type: ignore[method-assign]

    def undo() -> None:
        ProtocolHandler.mqtt_publish = original  # type: ignore[method-assign]

    return undo


def packet(kind: int, body: bytes) -> bytes:
    return bytes([kind, len(body)]) + body


async def read_packet(reader: asyncio.StreamReader) -> int:
    first = (await reader.readexactly(1))[0]
    length, shift = 0, 0
    while True:
        byte = (await reader.readexactly(1))[0]
        length |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            break
    await reader.readexactly(length)
    return first >> 4


async def plain_pubrel(*, wait_for_pubrec: bool) -> str:
    """On a real loop: a broker that resumes a QoS 2 delivery after a reconnect."""
    names = {5: "PUBREC", 7: "PUBCOMP", 12: "PINGREQ"}
    seen: list[str] = []

    async def broker(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_packet(reader)  # CONNECT
        if not seen:
            writer.write(packet(0x20, b"\x00\x00") + packet(0x34, b"\x00\x01t\x00\x07hello"))
            while await read_packet(reader) != 5:
                pass
            seen.append("PUBREC")
            writer.transport.abort()  # the connection drops before PUBREL
            return
        seen.append("reconnect")
        # Resume: CONNACK with the session present, then the unacknowledged PUBREL.
        if wait_for_pubrec:
            writer.write(packet(0x20, b"\x01\x00"))
            seen.append(names.get(await read_packet(reader), "?"))
            writer.write(packet(0x62, b"\x00\x07"))
        else:
            writer.write(packet(0x20, b"\x01\x00") + packet(0x62, b"\x00\x07"))
        with contextlib.suppress(TimeoutError, asyncio.IncompleteReadError, ConnectionError):
            while True:
                kind = await asyncio.wait_for(read_packet(reader), 3)
                seen.append(names.get(kind, str(kind)))
        writer.close()

    server = await asyncio.start_server(broker, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = MQTTClient(client_id="c", config={"auto_reconnect": True, "reconnect_retries": 0})
    await client.connect(f"mqtt://127.0.0.1:{port}/", cleansession=False)
    try:
        msg = await client.deliver_message(timeout_duration=4)
        delivered = repr(msg.data) if msg is not None else "None"
    except TimeoutError:
        delivered = "nothing"
    server.close()
    return f"delivered {delivered}, broker saw {seen[:4]}"


async def plain_publish() -> str:
    """On a real loop: the broker drops the connection before it acknowledges a publish."""

    async def broker(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await read_packet(reader)
        writer.write(packet(0x20, b"\x00\x00"))
        await read_packet(reader)  # PUBLISH
        writer.transport.abort()

    server = await asyncio.start_server(broker, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = MQTTClient(client_id="c", config={"auto_reconnect": False})
    await client.connect(f"mqtt://127.0.0.1:{port}/")

    async def publish() -> str:
        try:
            await client.publish("t", b"x", qos=1)
        except Exception as exc:
            return f"raised {type(exc).__name__}"
        return "returned"

    task = asyncio.create_task(publish())
    try:
        outcome = await task
    except asyncio.CancelledError:
        outcome = "raised CancelledError (the task was never cancelled)"
    server.close()
    return outcome


async def plain_qos() -> list[int]:
    """On a real loop: the QoS a QoS 2 subscriber receives for QoS 0, 1 and 2 publishes."""
    broker = Broker(
        {
            "listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:18831"}},
            "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {}},
        }
    )
    await broker.start()
    sub, pub = MQTTClient(client_id="s"), MQTTClient(client_id="p")
    await sub.connect("mqtt://127.0.0.1:18831/")
    await sub.subscribe([("t", 2)])
    await pub.connect("mqtt://127.0.0.1:18831/")
    received = []
    for qos in (0, 1, 2):
        await pub.publish("t", b"x", qos=qos)
        msg = await sub.deliver_message(timeout_duration=2)
        received.append(-1 if msg is None or msg.qos is None else msg.qos)
    await pub.disconnect()
    await sub.disconnect()
    await broker.shutdown()
    return received


def main() -> int:
    configs = [("random", simloop.SimConfig(step_time=1e-6, fail_on_unhandled=False))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(40), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups, report.summary()
            for group in report.groups:
                assert FAILING[workload] in group.message, group.message
    for workload, fix in (
        ("TCP resets", fix_pubrel),
        ("TCP resets, publisher errors", fix_publish_errors),
    ):
        undo = fix()
        try:
            for name, config in configs:
                fixed = simloop.explore(
                    make_harness(WORKLOADS[workload]), seeds=range(40), config=config
                )
                print(f"{workload}, {name}, {fix.__name__}: {fixed.summary().splitlines()[0]}")
                assert fixed.ok, fixed.summary()
        finally:
            undo()
    resumed = asyncio.run(plain_pubrel(wait_for_pubrec=False))
    print(f"real asyncio loop, PUBREL right after CONNACK: {resumed}")
    assert resumed.startswith("delivered nothing"), resumed
    assert "PUBCOMP" not in resumed, resumed
    control = asyncio.run(plain_pubrel(wait_for_pubrec=True))
    print(f"real asyncio loop, PUBREL after the client's PUBREC: {control}")
    assert control.startswith("delivered b'hello'"), control
    assert "PUBCOMP" in control, control
    outcome = asyncio.run(plain_publish())
    print(f"real asyncio loop, connection lost before PUBACK: publish() {outcome}")
    assert "CancelledError" in outcome, outcome
    received = asyncio.run(plain_qos())
    print(f"real asyncio loop, QoS 2 subscriber gets QoS 0, 1, 2 publishes at QoS {received}")
    assert received == [2, 2, 2], received
    return 0


if __name__ == "__main__":
    sys.exit(main())
