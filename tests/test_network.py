"""The simulated network, driven through the standard asyncio APIs."""

from __future__ import annotations

import asyncio
import socket
import sys
from collections.abc import Callable, Coroutine
from socket import gethostname as gethostname_by_name
from typing import Any

import pytest

import simloop

from .helpers import run_ok

try:
    from fcntl import fcntl as fcntl_by_name
except ImportError:  # Windows
    fcntl_by_name = None  # type: ignore[assignment]


async def echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while line := await reader.readline():
            writer.write(b"echo " + line)
            await writer.drain()
    finally:
        writer.close()


def echo_server(port: int = 7000) -> Callable[[], Coroutine[Any, Any, None]]:
    async def serve() -> None:
        server = await asyncio.start_server(echo_handler, port=port)
        async with server:
            await server.serve_forever()

    return serve


async def ask(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, text: str) -> bytes:
    writer.write(text.encode() + b"\n")
    await writer.drain()
    return await reader.readline()


def test_streams_between_hosts() -> None:
    async def main() -> tuple[list[bytes], object, object, str]:
        simloop.network().add_host("server", entry=echo_server())
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        replies = [await ask(reader, writer, f"msg {i}") for i in range(3)]
        peer, local = writer.get_extra_info("peername"), writer.get_extra_info("sockname")
        writer.close()
        await writer.wait_closed()
        return replies, peer, local, simloop.current_host().name

    replies, peer, local, host = run_ok(main).value  # type: ignore[misc]
    assert replies == [b"echo msg 0\n", b"echo msg 1\n", b"echo msg 2\n"]
    assert peer == ("10.0.0.2", 7000)
    assert local == ("10.0.0.1", 49152)
    assert host == "main"


def test_loopback_without_extra_hosts() -> None:
    async def main() -> bytes:
        server = await asyncio.start_server(echo_handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("localhost", port)
        reply = await ask(reader, writer, "ping")
        writer.close()
        server.close()
        return reply

    assert run_ok(main).value == b"echo ping\n"


def test_fixed_latency_gives_exact_timing() -> None:
    async def main() -> list[float]:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        net.set_link("main", "server", latency=0.05)
        await asyncio.sleep(1)
        start = simloop.now()
        reader, writer = await asyncio.open_connection("server", 7000)
        connected = simloop.now() - start
        start = simloop.now()
        await ask(reader, writer, "x")
        writer.close()
        return [connected, simloop.now() - start]

    connected, round_trip = run_ok(main).value or [0, 0]
    assert connected == pytest.approx(0.1)
    assert round_trip == pytest.approx(0.1)


@pytest.mark.parametrize("seed", range(5))
def test_stream_preserves_order_under_random_latency_and_loss(seed: int) -> None:
    async def main() -> bytes:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        net.set_link("main", "server", latency=(0.001, 0.2), drop=0.3)
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        for i in range(50):
            writer.write(f"{i}\n".encode())
        expected = b"".join(f"echo {i}\n".encode() for i in range(50))
        reply = await reader.readexactly(len(expected))
        writer.close()
        return reply

    expected = b"".join(f"echo {i}\n".encode() for i in range(50))
    assert run_ok(main, seed=seed).value == expected


def test_lost_tcp_segments_cost_retransmission_time() -> None:
    async def main(drop: float) -> float:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        net.set_link("main", "server", latency=0.01, drop=drop)
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        start = simloop.now()
        for i in range(20):
            await ask(reader, writer, str(i))
        writer.close()
        return simloop.now() - start

    clean = run_ok(lambda: main(0.0)).value or 0
    lossy = run_ok(lambda: main(0.3)).value or 0
    assert clean == pytest.approx(0.4)
    assert lossy > clean + 0.2


def test_partition_holds_data_until_heal() -> None:
    async def main() -> tuple[str, bytes, float]:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        net.partition("main", "server")
        writer.write(b"stuck\n")
        try:
            async with asyncio.timeout(10):
                await reader.readline()
            outcome = "delivered"
        except TimeoutError:
            outcome = "timed out"
        net.heal()
        healed_at = simloop.now()
        reply = await reader.readline()
        writer.close()
        return outcome, reply, simloop.now() - healed_at

    outcome, reply, delay = run_ok(main).value  # type: ignore[misc]
    assert outcome == "timed out"
    assert reply == b"echo stuck\n"
    assert 0 < delay < 0.1


def test_connect_during_partition_times_out_or_succeeds_after_heal() -> None:
    async def main(heal_after: float | None) -> str:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        await asyncio.sleep(0.01)
        net.isolate("server")
        if heal_after is not None:
            asyncio.get_running_loop().call_later(heal_after, net.heal)
        start = simloop.now()
        try:
            _, writer = await asyncio.open_connection("server", 7000)
        except TimeoutError:
            return f"timeout after {simloop.now() - start:.0f}s"
        writer.close()
        return f"connected after {simloop.now() - start:.0f}s"

    assert run_ok(lambda: main(None), connect_timeout=20).value == "timeout after 20s"
    assert run_ok(lambda: main(2.5), connect_timeout=20).value == "connected after 3s"


def test_asymmetric_block() -> None:
    async def main() -> list[str]:
        net = simloop.network()
        received: list[str] = []

        class Sink(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
                received.append(f"{simloop.current_host().name} got {data.decode()}")

        a, b = net.add_host("a"), net.add_host("b")

        async def bind() -> asyncio.DatagramTransport:
            transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
                Sink, local_addr=("0.0.0.0", 9000)
            )
            return transport

        ta = await a.run(bind())
        tb = await b.run(bind())
        net.block("a", "b")
        ta.sendto(b"a->b", ("b", 9000))
        tb.sendto(b"b->a", ("a", 9000))
        await asyncio.sleep(1)
        return received

    assert run_ok(main).value == ["a got b->a"]


def test_refused_and_unknown_hosts() -> None:
    async def main() -> list[str]:
        simloop.network().add_host("server")
        out = []
        try:
            await asyncio.open_connection("server", 1234)
        except ConnectionRefusedError:
            out.append("refused")
        try:
            await asyncio.open_connection("nowhere.example", 80)
        except socket.gaierror:
            out.append("gaierror")
        return out

    assert run_ok(main).value == ["refused", "gaierror"]


def test_process_crash_resets_peers_and_restart_runs_entry_again() -> None:
    async def main() -> list[str]:
        net = simloop.network()
        server = net.add_host("server", entry=echo_server())
        server.disk["boot"] = 0
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        await ask(reader, writer, "hello")
        server.crash()
        out = [f"tasks after crash: {len(server.tasks)}"]
        writer.write(b"anyone?\n")
        try:
            await reader.readline()
        except ConnectionResetError:
            out.append("reset")
        try:
            await asyncio.open_connection("server", 7000)
        except ConnectionRefusedError:
            out.append("refused while down")
        server.restart()
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        out.append((await ask(reader, writer, "back")).decode().strip())
        writer.close()
        out.append(f"disk kept: {'boot' in server.disk}, crashes: {server.crashes}")
        return out

    assert run_ok(main).value == [
        "tasks after crash: 0",
        "reset",
        "refused while down",
        "echo back",
        "disk kept: True, crashes: 1",
    ]


def test_power_loss_makes_the_host_silent() -> None:
    async def main() -> list[str]:
        net = simloop.network()
        server = net.add_host("server", entry=echo_server())
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        server.crash(power_loss=True)
        out = []
        writer.write(b"hello?\n")
        try:
            async with asyncio.timeout(30):
                await reader.readline()
        except TimeoutError:
            out.append("no answer")
        writer.close()
        try:
            await asyncio.open_connection("server", 7000)
        except TimeoutError:
            out.append("connect timed out")
        return out

    assert run_ok(main, connect_timeout=10).value == ["no answer", "connect timed out"]


def test_crash_cancels_only_that_hosts_tasks() -> None:
    async def main() -> list[str]:
        net = simloop.network()
        log: list[str] = []

        async def worker(name: str) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                log.append(f"{name} cancelled")
                raise

        a, b = net.add_host("a"), net.add_host("b")
        a.spawn(worker("a1"))
        a.spawn(worker("a2"))
        survivor = b.spawn(worker("b1"))
        await asyncio.sleep(1)
        a.crash()
        await asyncio.sleep(1)
        log.append(f"b1 done: {survivor.done()}")
        survivor.cancel()
        return log

    assert run_ok(main).value == ["a1 cancelled", "a2 cancelled", "b1 done: False", "b1 cancelled"]


def test_injected_resets() -> None:
    async def main() -> str:
        net = simloop.network()
        net.add_host("server", entry=echo_server())
        net.set_link("main", "server", reset=0.2)
        await asyncio.sleep(0.01)
        reader, writer = await asyncio.open_connection("server", 7000)
        try:
            for i in range(100):
                await ask(reader, writer, str(i))
        except (ConnectionResetError, asyncio.IncompleteReadError):
            return "reset"
        finally:
            writer.close()
        return "survived"

    outcomes = {simloop.run(main, seed=s).value for s in range(10)}
    assert outcomes == {"reset"}


def test_getaddrinfo() -> None:
    async def main() -> list[object]:
        net = simloop.network()
        net.add_host("db")
        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo("db", 5432)
        local = await loop.getaddrinfo("localhost", "http")
        try:
            await loop.getaddrinfo("missing", 1)
        except socket.gaierror:
            missing = "gaierror"
        return [infos[0][4], local[0][4], missing, await loop.getnameinfo(("10.0.0.2", 5432))]

    assert run_ok(main).value == [("10.0.0.2", 5432), ("127.0.0.1", 80), "gaierror", ("db", "5432")]


def test_blocking_name_lookups_resolve_simulated_hosts() -> None:
    async def main() -> list[object]:
        net = simloop.network()
        net.add_host("db")

        async def on_db() -> list[object]:
            return [socket.gethostname(), gethostname_by_name(), socket.getfqdn()]

        names = await net.host("db").run(on_db())
        try:
            socket.gethostbyaddr("10.9.9.9")
        except socket.herror:
            unknown = "herror"
        return [
            socket.gethostname(),
            socket.getfqdn(),
            names,
            socket.gethostbyname("db"),
            socket.gethostbyname_ex("db"),
            socket.gethostbyaddr("10.0.0.2"),
            socket.getfqdn("10.0.0.2"),
            socket.getfqdn("nowhere"),
            unknown,
            socket.getaddrinfo("db", "http")[0][4],
            socket.getnameinfo(("10.0.0.2", 80), 0),
        ]

    assert run_ok(main).value == [
        "main",
        "main",
        ["db", "db", "db"],
        "10.0.0.2",
        ("db", [], ["10.0.0.2"]),
        ("db", [], ["10.0.0.2"]),
        "db",
        "nowhere",
        "herror",
        ("10.0.0.2", 80),
        ("db", "80"),
    ]
    assert gethostname_by_name is socket.gethostname


def test_udp_drops_duplicates_and_reorders() -> None:
    async def main() -> list[int]:
        net = simloop.network()
        received: list[int] = []

        class Sink(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
                received.append(int(data))

        sink = net.add_host("sink")
        await sink.run(
            asyncio.get_running_loop().create_datagram_endpoint(Sink, local_addr=("0.0.0.0", 53))
        )
        net.set_link("main", "sink", latency=(0.001, 0.05), drop=0.2, duplicate=0.2)
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            asyncio.DatagramProtocol, remote_addr=("sink", 53)
        )
        for i in range(200):
            transport.sendto(str(i).encode())
        await asyncio.sleep(1)
        transport.close()
        return received

    received = run_ok(main, seed=3).value or []
    unique = set(received)
    assert 100 < len(unique) < 200
    assert len(received) > len(unique)
    assert received != sorted(received)


def test_port_reuse_and_server_close() -> None:
    async def main() -> list[str]:
        out = []
        server = await asyncio.start_server(echo_handler, port=8000)
        try:
            await asyncio.start_server(echo_handler, port=8000)
        except OSError:
            out.append("in use")
        reader, writer = await asyncio.open_connection("localhost", 8000)
        server.close()
        try:
            await asyncio.open_connection("localhost", 8000)
        except ConnectionRefusedError:
            out.append("refused after close")
        out.append((await ask(reader, writer, "still open")).decode().strip())
        writer.close()
        await server.wait_closed()
        again = await asyncio.start_server(echo_handler, port=8000)
        again.close()
        out.append("rebound")
        return out

    assert run_ok(main).value == ["in use", "refused after close", "echo still open", "rebound"]


def test_half_close() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read()
        writer.write(data[::-1])
        writer.close()

    async def main() -> bytes:
        await asyncio.start_server(handler, port=9)
        reader, writer = await asyncio.open_connection("localhost", 9)
        writer.write(b"abc")
        writer.write_eof()
        reply = await reader.read()
        writer.close()
        return reply

    assert run_ok(main).value == b"cba"


def test_handlers_run_on_the_server_host() -> None:
    async def main() -> list[str]:
        seen: list[str] = []

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            seen.append(simloop.current_host().name)
            writer.close()

        async def serve() -> None:
            await asyncio.start_server(handler, port=1)
            await asyncio.sleep(1e6)

        simloop.network().add_host("server", entry=serve)
        await asyncio.sleep(0.01)
        _, writer = await asyncio.open_connection("server", 1)
        await asyncio.sleep(1)
        seen.append(simloop.current_host().name)
        writer.close()
        return seen

    assert run_ok(main).value == ["server", "main"]


def test_network_runs_are_deterministic() -> None:
    async def main() -> list[bytes]:
        net = simloop.network()
        net.default_link = simloop.LinkConfig(latency=(0.001, 0.1), drop=0.1)
        for i in range(3):
            net.add_host(f"s{i}", entry=echo_server())
        await asyncio.sleep(0.01)

        async def client(i: int) -> bytes:
            reader, writer = await asyncio.open_connection(f"s{i % 3}", 7000)
            reply = await ask(reader, writer, f"c{i}")
            writer.close()
            return reply

        return list(await asyncio.gather(*(client(i) for i in range(9))))

    runs = [simloop.run(main, seed=11) for _ in range(3)]
    assert all(r.ok for r in runs)
    assert len({r.digest for r in runs}) == 1
    assert simloop.run(main, seed=12).digest != runs[0].digest


def test_virtual_sockets_for_libraries_that_bring_their_own() -> None:
    async def main() -> list[object]:
        loop = asyncio.get_running_loop()
        listener = socket.socket()
        listener.bind(("0.0.0.0", 0))
        listener.listen()
        port = listener.getsockname()[1]
        server = await asyncio.start_server(echo_handler, sock=listener)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setblocking(False)
        await loop.sock_connect(client, ("127.0.0.1", port))
        reader, writer = await asyncio.open_connection(sock=client)
        reply = await ask(reader, writer, "via sock")
        writer.close()
        server.close()
        try:
            socket.socket().recv(1)
        except simloop.UnsupportedOperation:
            blocking = "blocking I/O rejected"
        return [type(listener).__name__, reply, blocking]

    assert run_ok(main).value == ["socket", b"echo via sock\n", "blocking I/O rejected"]


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is POSIX-only")
def test_fcntl_flags_on_a_virtual_socket_stay_in_the_simulation() -> None:
    import fcntl
    import os

    async def main() -> list[object]:
        sock = socket.socket()
        seen: list[object] = [fcntl.fcntl(sock.fileno(), fcntl.F_GETFD) & fcntl.FD_CLOEXEC]
        assert fcntl_by_name is not None
        fcntl_by_name(sock, fcntl.F_SETFD, 0)  # imported by name, as pymongo does
        seen.append(fcntl.fcntl(sock, fcntl.F_GETFD))
        flags = fcntl.fcntl(sock, fcntl.F_GETFL)
        fcntl.fcntl(sock, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        seen.append(sock.gettimeout())
        with pytest.raises(simloop.UnsupportedOperation):
            fcntl.fcntl(sock, fcntl.F_GETLK)
        read_end, write_end = os.pipe()
        try:
            seen.append(fcntl.fcntl(read_end, fcntl.F_GETFD) == fcntl.FD_CLOEXEC)
        finally:
            os.close(read_end)
            os.close(write_end)
        return seen

    assert run_ok(main).value == [fcntl.FD_CLOEXEC, 0, 0.0, True]


def test_reset_is_reported_after_callbacks_scheduled_by_earlier_data() -> None:
    # asyncio's _force_close reports connection_lost from a later callback, so
    # work scheduled while handling the last data runs first.
    async def main() -> list[str]:
        net = simloop.network()
        events: list[str] = []
        loop = asyncio.get_running_loop()

        async def rude_server() -> None:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                writer.write(b"bye")
                writer.transport.abort()

            server = await asyncio.start_server(handle, port=9)
            async with server:
                await server.serve_forever()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                assert isinstance(transport, asyncio.Transport)
                transport.write(b"hi\n")

            def data_received(self, data: bytes) -> None:
                events.append("data")
                loop.call_soon(events.append, "scheduled by data")

            def connection_lost(self, exc: Exception | None) -> None:
                events.append(f"lost {type(exc).__name__}")

        net.add_host("rude", entry=rude_server)
        net.set_link("rude", "main", latency=0.01)
        await asyncio.sleep(0.01)
        await loop.create_connection(Client, "rude", 9)
        await asyncio.sleep(1)
        return events

    result = run_ok(main, scheduler="fifo")
    assert result.value == ["data", "scheduled by data", "lost ConnectionResetError"]


def test_select_and_poll_see_simulated_connection_state() -> None:
    import select

    async def main() -> list[object]:
        net = simloop.network()

        async def closer() -> None:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                writer.close()

            server = await asyncio.start_server(handle, port=5)
            async with server:
                await server.serve_forever()

        net.add_host("closer", entry=closer)
        await asyncio.sleep(0.01)
        _reader, writer = await asyncio.open_connection("closer", 5)
        sock = writer.get_extra_info("socket")
        seen: list[object] = [sock.fileno() >= 0, select.select([sock], [], [], 0)[0]]
        poller = select.poll()
        poller.register(sock, select.POLLIN)
        seen.append(poller.poll(0))
        writer.write(b"bye\n")
        await asyncio.sleep(1)  # the server closes its end
        seen.append(select.select([sock], [], [], 0)[0] == [sock])
        seen.append(poller.poll(0) == [(sock.fileno(), select.POLLIN)])
        with pytest.raises(simloop.UnsupportedOperation, match="stall"):
            select.select([], [], [], 1.0)
        with pytest.raises(simloop.UnsupportedOperation, match="not a simulated socket"):
            select.select([0], [], [], 0)
        writer.close()
        return seen

    assert run_ok(main).value == [True, [], [], True, True]


async def _held_then_fresh() -> list[bytes]:
    net = simloop.network()
    healed = asyncio.Event()

    async def talker() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.readline()
            writer.write(b"held\n")  # sent while partitioned
            await healed.wait()
            writer.write(b"fresh\n")  # sent the moment the partition heals
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, port=3)
        async with server:
            await server.serve_forever()

    net.add_host("talker", entry=talker)
    net.set_link("main", "talker", latency=(0.001, 0.05))
    await asyncio.sleep(0.01)
    reader, writer = await asyncio.open_connection("talker", 3)
    writer.write(b"go\n")
    await asyncio.sleep(0.06)
    net.partition(["main"], ["talker"])
    await asyncio.sleep(1)
    net.heal()
    healed.set()
    lines = [await reader.readline(), await reader.readline()]
    writer.close()
    return lines


@pytest.mark.parametrize("seed", range(20))
def test_stream_order_survives_a_partition_heal(seed: int) -> None:
    assert run_ok(_held_then_fresh, seed=seed, scheduler="fifo").value == [b"held\n", b"fresh\n"]


def test_segments_in_flight_at_heal_stay_behind_held_ones() -> None:
    async def main() -> list[bytes]:
        net = simloop.network()
        received: list[bytes] = []
        done = asyncio.Event()

        async def sink() -> None:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                received.extend([await reader.readline(), await reader.readline()])
                done.set()
                writer.close()

            server = await asyncio.start_server(handle, port=4)
            async with server:
                await server.serve_forever()

        net.add_host("sink", entry=sink)
        net.set_link("main", "sink", latency=0.05)
        await asyncio.sleep(0.01)
        _reader, writer = await asyncio.open_connection("sink", 4)
        net.partition(["main"], ["sink"])
        writer.write(b"first\n")  # arrives during the partition: held
        await asyncio.sleep(0.04)
        writer.write(b"second\n")  # still in flight when the partition heals
        await asyncio.sleep(0.02)
        net.heal()
        await done.wait()
        writer.close()
        return received

    assert run_ok(main, scheduler="fifo").value == [b"first\n", b"second\n"]


class SmallBuffers(asyncio.BufferedProtocol):
    """Receives into a 3-byte buffer; optionally pauses after the first chunk."""

    def __init__(self, pause_once: bool = False) -> None:
        self.buffer = bytearray(3)
        self.received = bytearray()
        self.chunks: list[int] = []
        self.pause_once = pause_once
        self.done: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        self.transport: asyncio.Transport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        self.transport = transport
        transport.write(b"hello\n")

    def get_buffer(self, sizehint: int) -> bytearray:
        return self.buffer

    def buffer_updated(self, nbytes: int) -> None:
        self.chunks.append(nbytes)
        self.received += self.buffer[:nbytes]
        if self.pause_once and self.transport is not None:
            self.pause_once = False
            self.transport.pause_reading()
            asyncio.get_running_loop().call_later(0.5, self.transport.resume_reading)
        if self.received.endswith(b"\n"):
            self.done.set_result(bytes(self.received))

    def connection_lost(self, exc: Exception | None) -> None:
        pass


@pytest.mark.parametrize("pause_once", [False, True])
def test_buffered_protocol_receives_through_its_buffer(pause_once: bool) -> None:
    async def main() -> tuple[bytes, list[int], float]:
        net = simloop.network()
        net.add_host("echo", entry=echo_server())
        await asyncio.sleep(0.01)
        loop = asyncio.get_running_loop()
        protocol = SmallBuffers(pause_once)
        transport, _ = await loop.create_connection(lambda: protocol, "echo", 7000)
        started = loop.time()
        reply = await protocol.done
        transport.close()
        return reply, protocol.chunks, loop.time() - started

    result = run_ok(main)
    assert result.value is not None
    reply, chunks, elapsed = result.value
    assert reply == b"echo hello\n"
    assert chunks == [3, 3, 3, 2]
    assert (elapsed >= 0.5) is pause_once
    again = run_ok(main)
    assert again.digest == result.digest


class DuckProtocol(asyncio.BaseProtocol):
    """A stream protocol that, like asyncssh's port forwarders, is not an
    ``asyncio.Protocol`` subclass: asyncio only duck-types these methods."""

    def __init__(self) -> None:
        self.received = b""
        self.done: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    def data_received(self, data: bytes) -> None:
        self.received += data

    def eof_received(self) -> bool:
        self.done.set_result(self.received)
        return False


def test_stream_protocol_need_not_subclass_protocol() -> None:
    async def main() -> bytes:
        net = simloop.network()
        net.add_host("echo", entry=echo_server())
        await asyncio.sleep(0.01)
        protocol = DuckProtocol()
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_connection(lambda: protocol, "echo", 7000)
        transport.write(b"hello\n")
        transport.write_eof()
        return await protocol.done

    assert run_ok(main).value == b"echo hello\n"


def test_dual_stack_ipv6_udp_sees_ipv4_mapped_addresses() -> None:
    async def main() -> list[object]:
        net = simloop.network()
        seen: list[object] = []

        class Echo(asyncio.DatagramProtocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def datagram_received(self, data: bytes, addr: tuple[Any, ...]) -> None:
                seen.append(addr)
                self.transport.sendto(data, addr)  # type: ignore[attr-defined]

        server = net.add_host("srv")
        loop = asyncio.get_running_loop()
        await server.run(loop.create_datagram_endpoint(Echo, local_addr=("::", 4433)))
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        sock.bind(("::", 0, 0, 0))
        replies: asyncio.Queue[tuple[bytes, object]] = asyncio.Queue()

        class Client(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, addr: tuple[Any, ...]) -> None:
                replies.put_nowait((data, addr))

        transport, _ = await loop.create_datagram_endpoint(Client, sock=sock)
        transport.sendto(b"ping", ("::ffff:10.0.0.2", 4433, 0, 0))
        reply = await replies.get()
        return [seen, reply, transport.get_extra_info("sockname")[0]]

    assert run_ok(main).value == [
        [("::ffff:10.0.0.1", 49152, 0, 0)],
        (b"ping", ("::ffff:10.0.0.2", 4433, 0, 0)),
        "::ffff:10.0.0.1",
    ]
