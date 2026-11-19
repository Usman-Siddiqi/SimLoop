"""aioquic 1.8.0: two tasks that create a stream before either writes get the same stream.

An aioquic server (echo on every stream) and client on the simulated
network, over UDP with 1-20 ms latency. Four tasks open four streams each
and push up to 150 KB through them; some give up early through
``asyncio.wait_for``. Checked: every stream returns exactly its own bytes,
nobody hangs, and the connection serves a stream afterwards. With each
stream's first byte written as soon as it is created, all of this held with
no faults, with 5% datagram loss and 5% duplication, and with 10% loss,
duplication and cancellations, random and fifo, with
``strict_cancellation=True``. Loss recovery and flow control held up.

Without that first write it fails. ``QuicConnectionProtocol.create_stream``
returns ``get_next_available_stream_id()``, which only moves on once data
is sent on that stream, so a task that creates a stream and yields (to
``gather`` its reader and writer, say) lets the next task's
``create_stream`` return the same stream id. Both then write into one QUIC
stream: replies mix, one side's ``write`` raises ``StreamStateError:
Cannot send data after FIN``, or a reader waits forever. A
``create_stream`` that reserves the id passes. Known and fixed upstream:
aiortc/aioquic#835 (merged 2026-11-15, after 1.8.0), which also fixes
streams that hang or end cleanly when the connection closes.

aioquic re-queues the streams it served from a ``set`` of stream objects,
which iterates in memory-address order and made runs differ under loss;
the harness hashes streams by id so that a seed replays (the order is
arbitrary either way). The certificate uses Ed25519, whose signatures have
a fixed length, for the same reason.

Run: python findings/aioquic_streams.py
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from aioquic.asyncio import QuicConnectionProtocol, connect, serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.stream import QuicStream
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.x509.oid import NameOID

import simloop

logging.getLogger("quic").setLevel(logging.CRITICAL)
QuicStream.__hash__ = lambda self: self.stream_id  # type: ignore[assignment,method-assign]
ERRORS = (ConnectionError, OSError, TimeoutError, asyncio.IncompleteReadError)
WORKLOADS: dict[str, dict[str, Any]] = {
    "no faults": {"reserve": True},
    "5% loss, 5% duplicates": {"reserve": True, "drop": 0.05, "dup": 0.05},
    "10% loss, duplicates, wait_for": {"reserve": True, "drop": 0.1, "dup": 0.05, "cancel": 0.3},
    "streams created before their first write": {},
}
FAILING = ("streams created before their first write",)
SYMPTOMS = ("Cannot send data after FIN", "stuck after", "different bytes back")


def certificate() -> tuple[x509.Certificate, ed25519.Ed25519PrivateKey]:
    key = ed25519.Ed25519PrivateKey.generate()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "quic")])
    start = datetime.datetime(2023, 1, 1)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(start)
        .not_valid_after(start + datetime.timedelta(days=3650))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("quic")]), critical=False)
        .sign(key, None)
    )
    return cert, key


def server_config(cert: Any, key: Any) -> QuicConfiguration:
    config = QuicConfiguration(is_client=False, alpn_protocols=["echo"])
    config.certificate, config.private_key = cert, key
    return config


def client_config() -> QuicConfiguration:
    return QuicConfiguration(
        is_client=True, alpn_protocols=["echo"], verify_mode=0, server_name="quic"
    )


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Every stream returns its own bytes, nobody hangs, the connection recovers."""
        rng = simloop.rng()
        cert, key = certificate()
        net = simloop.network()

        async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while data := await reader.read(rng.randrange(1, 20_000)):
                    await asyncio.sleep(rng.uniform(0, 0.002))
                    writer.write(data)
                writer.write_eof()
            except ERRORS:
                pass

        async def server() -> None:
            handlers: set[asyncio.Task[None]] = set()

            def on_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                task = asyncio.ensure_future(echo(reader, writer))
                handlers.add(task)
                task.add_done_callback(handlers.discard)

            await serve(
                "0.0.0.0", 4433, configuration=server_config(cert, key), stream_handler=on_stream
            )
            await asyncio.Future()

        net.add_host("quic", entry=server)
        net.set_link("main", "quic", latency=(0.001, 0.005))
        await asyncio.sleep(0.1)
        errors: list[str] = []
        async with connect("quic", 4433, configuration=client_config()) as client:
            net.set_link(
                "main",
                "quic",
                latency=(0.001, 0.02),
                drop=knobs.get("drop", 0.0),
                duplicate=knobs.get("dup", 0.0),
            )

            async def stream(tag: str) -> None:
                pattern = tag.encode() + b":" + bytes(rng.randrange(256) for _ in range(64))
                payload = (pattern * 3000)[: rng.randrange(1, 150_000)]
                reader, writer = await client.create_stream()
                start = 0
                if knobs.get("reserve"):
                    writer.write(payload[:1])
                    start = 1

                async def pump() -> None:
                    for at in range(start, len(payload), step := rng.randrange(1, 30_000)):
                        writer.write(payload[at : at + step])
                        await asyncio.sleep(0)
                    writer.write_eof()

                async def both() -> bytes:
                    _, got = await asyncio.gather(pump(), reader.read())
                    return got

                if rng.random() < knobs.get("cancel", 0):
                    try:
                        await asyncio.wait_for(both(), rng.uniform(0, 0.05))
                    except TimeoutError:
                        return
                elif await both() != payload:
                    errors.append(f"{tag}: {len(payload)} bytes sent, different bytes back")

            async def worker(n: int) -> None:
                for i in range(4):
                    try:
                        await stream(f"c{n}-{i}")
                    except ERRORS as exc:
                        simloop.log(f"c{n}-{i}: {type(exc).__name__}")
                    await asyncio.sleep(rng.uniform(0, 0.01))

            tasks = [asyncio.create_task(worker(n)) for n in range(4)]
            done, pending = await asyncio.wait(tasks, timeout=60)
            assert not pending, f"{len(pending)} worker(s) stuck after 60 s"
            for task in done:
                task.result()
            assert not errors, errors[:3]
            net.set_link("main", "quic", latency=0.002, drop=0.0, duplicate=0.0)
            await asyncio.sleep(1)
            reader, writer = await client.create_stream()
            writer.write(b"last")
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(), 5) == b"last"

    return harness


def fix_create_stream() -> Callable[[], None]:
    """``create_stream`` that reserves its stream id before returning."""
    original = QuicConnectionProtocol.create_stream

    async def create_stream(self: Any, is_unidirectional: bool = False) -> Any:
        stream_id = self._quic.get_next_available_stream_id(is_unidirectional=is_unidirectional)
        self._quic._get_or_create_stream_for_send(stream_id)
        return self._create_stream(stream_id)

    QuicConnectionProtocol.create_stream = create_stream  # type: ignore[method-assign]

    def undo() -> None:
        QuicConnectionProtocol.create_stream = original  # type: ignore[method-assign]

    return undo


async def plain() -> str:
    """On a real loop: two tasks each create a stream, then yield before writing."""
    cert, key = certificate()
    server = await serve(
        "127.0.0.1", 0, configuration=server_config(cert, key), stream_handler=lambda r, w: None
    )
    port = server._transport.get_extra_info("sockname")[1]
    # What connect() does, on IPv4: it insists on a dual-stack IPv6 socket.
    loop = asyncio.get_running_loop()
    connection = QuicConnection(configuration=client_config())
    transport, client = await loop.create_datagram_endpoint(
        lambda: QuicConnectionProtocol(connection), local_addr=("127.0.0.1", 0)
    )
    client.connect(("127.0.0.1", port))
    await client.wait_connected()

    async def open_one() -> int:
        _, writer = await client.create_stream()
        await asyncio.sleep(0)
        return int(writer.get_extra_info("stream_id"))

    ids = await asyncio.gather(open_one(), open_one())
    client.close()
    await client.wait_closed()
    transport.close()
    server.close()
    return f"stream ids: {ids}"


def main() -> int:
    configs = [("random", simloop.SimConfig(strict_cancellation=True, step_time=1e-6))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(20), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups, report.summary()
            for group in report.groups:
                assert any(s in group.message for s in SYMPTOMS), group.message
    undo = fix_create_stream()
    try:
        for workload in FAILING:
            for name, config in configs:
                fixed = simloop.explore(
                    make_harness(WORKLOADS[workload]), seeds=range(20), config=config
                )
                print(f"{workload}, {name}, ids reserved: {fixed.summary().splitlines()[0]}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    outcome = asyncio.run(plain())
    print(f"real asyncio loop: {outcome}")
    assert outcome == "stream ids: [0, 0]", outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
