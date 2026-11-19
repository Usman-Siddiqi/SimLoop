"""asyncssh 2.24.1: a connection that closes while the server opens a forward leaks the forward.

An asyncssh server and client on the simulated network. Sessions: four
tasks open 16 exec channels on one connection and stream up to 120 KB each
through a server-side echo, with receive windows down to 2 KB and packets
down to 1 KB; some give up early through ``asyncio.wait_for``. Checked:
every byte comes back in order, nobody hangs, every server handler ends and
the client has no channel left. That held with no faults, with
cancellations, with 0.2% TCP resets per segment, and with partitions under
keepalives, random and fifo, with ``strict_cancellation=True``.

Port forwarding: the client forwards a local port to an echo host through
the server (``forward_local_port``), and four tasks push data through it.
Checked as well: once the faults stop, no forwarded connection to the
target is still open while the server has no SSH connection. With TCP
resets this fails. ``SSHChannel._finish_open_request`` awaits the session
(``forward_connection``, which connects to the target) and only then checks
whether the SSH connection is still there; if it closed meanwhile it raises
``ChannelOpenError`` and cleans the channel up, but the session it just got
(an ``SSHForwarder`` holding the open TCP connection to the target) is
never closed. Closing it there passes.

Run: python findings/asyncssh_forward_leak.py
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import asyncssh
from asyncssh.channel import SSHChannel

import simloop
from simloop import faults

logging.getLogger("asyncssh").setLevel(logging.CRITICAL)
ERRORS = (asyncssh.Error, OSError, asyncio.IncompleteReadError, TimeoutError)
WORKLOADS: dict[str, dict[str, Any]] = {
    "sessions, no faults": {},
    "sessions, wait_for": {"cancel": 0.4},
    "sessions, TCP resets": {"reset": 0.002},
    "sessions, partitions, keepalives": {"partition": True, "cancel": 0.3},
    "forwarding, no faults": {"forward": True},
    "forwarding, wait_for": {"forward": True, "cancel": 0.4},
    "forwarding, TCP resets": {"forward": True, "reset": 0.002},
}
FAILING = ("forwarding, TCP resets",)


class Server(asyncssh.SSHServer):
    """No authentication; forwarding allowed; tracks open connections."""

    open: set[int]

    def begin_auth(self, username: str) -> bool:
        return False

    def connection_requested(self, *args: Any) -> bool:
        return True

    def connection_made(self, conn: asyncssh.SSHServerConnection) -> None:
        self.open.add(id(self))

    def connection_lost(self, exc: Exception | None) -> None:
        self.open.discard(id(self))


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Data intact, nobody hangs, and nothing stays open after faults stop."""
        rng = simloop.rng()
        key = asyncssh.generate_private_key("ssh-ed25519")
        net = simloop.network()
        server_conns: set[int] = set()
        handlers: dict[str, int] = {}
        targets: set[int] = set()
        keepalive = 0.5 if knobs.get("partition") else 0

        class Tracked(Server):
            open = server_conns

        async def echo_process(process: asyncssh.SSHServerProcess[bytes]) -> None:
            name = str(process.command)
            handlers[name] = handlers.get(name, 0) + 1
            try:
                while data := await process.stdin.read(rng.randrange(1, 9000)):
                    await asyncio.sleep(rng.uniform(0, 0.002))
                    process.stdout.write(data)
                process.exit(0)
            except ERRORS:
                pass
            finally:
                handlers[name] -= 1
                if not handlers[name]:
                    del handlers[name]

        async def ssh_server() -> None:
            server = await asyncssh.create_server(
                Tracked,
                "",
                8022,
                server_host_keys=[key],
                process_factory=echo_process,
                encoding=None,
                keepalive_interval=keepalive,
            )
            await server.wait_closed()

        async def echo_target() -> None:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                me = id(writer)
                targets.add(me)
                try:
                    while data := await reader.read(5000):
                        await asyncio.sleep(rng.uniform(0, 0.002))
                        writer.write(data)
                        await writer.drain()
                except ConnectionError:
                    pass
                finally:
                    targets.discard(me)
                    writer.close()

            server = await asyncio.start_server(handle, port=7)
            await server.serve_forever()

        net.add_host("ssh", entry=ssh_server)
        net.add_host("echo", entry=echo_target)
        net.set_link("main", "ssh", latency=(0.0005, 0.003))
        await asyncio.sleep(0.1)
        conn = await asyncssh.connect(
            "ssh",
            8022,
            known_hosts=None,
            username="u",
            keepalive_interval=keepalive,
            keepalive_count_max=2,
        )
        listener = None
        if knobs.get("forward"):
            listener = await conn.forward_local_port("", 9000, "echo", 7)
        errors: list[str] = []

        async def transfer(tag: str) -> None:
            pattern = tag.encode() + b":" + bytes(rng.randrange(256) for _ in range(64))
            payload = (pattern * (120_000 // len(pattern) + 1))[: rng.randrange(1, 120_000)]
            if listener is not None:
                reader, writer = await asyncio.open_connection("127.0.0.1", 9000)
                write, drain, close = writer.write, writer.drain, writer.close
                read_back = reader.readexactly
            else:
                process = await conn.create_process(
                    tag,
                    encoding=None,
                    window=rng.choice([2048, 16384, 2 * 1024 * 1024]),
                    max_pktsize=rng.choice([1024, 32768]),
                )
                write, drain = process.stdin.write, process.stdin.drain
                read_back, close = process.stdout.readexactly, process.close

            async def pump() -> None:
                for at in range(0, len(payload), step := rng.randrange(1, 20_000)):
                    write(payload[at : at + step])
                    await drain()

            async def both() -> bytes:
                _, got = await asyncio.gather(pump(), read_back(len(payload)))
                return got

            try:
                if rng.random() < knobs.get("cancel", 0):
                    try:
                        await asyncio.wait_for(both(), rng.uniform(0, 0.05))
                    except TimeoutError:
                        return
                elif await both() != payload:
                    errors.append(f"{tag}: {len(payload)} bytes sent, different bytes back")
            finally:
                close()

        async def client(n: int) -> None:
            for i in range(4):
                try:
                    await transfer(f"c{n}-{i}")
                except ERRORS as exc:
                    simloop.log(f"c{n}-{i}: {type(exc).__name__}")
                await asyncio.sleep(rng.uniform(0, 0.01))

        net.set_link("main", "ssh", latency=(0.0005, 0.003), reset=knobs.get("reset", 0.0))
        nemesis = None
        if knobs.get("partition"):
            partitions = faults.random_partitions(
                ["ssh"], include_driver=True, every=(0.05, 0.5), duration=(0.2, 3.0)
            )
            nemesis = asyncio.create_task(partitions)
        tasks = [asyncio.create_task(client(n)) for n in range(4)]
        done, pending = await asyncio.wait(tasks, timeout=60)
        assert not pending, f"{len(pending)} client(s) stuck after 60 s"
        for task in done:
            task.result()
        assert not errors, errors[:3]
        if nemesis is not None:
            nemesis.cancel()
            net.heal()
        net.set_link("main", "ssh", latency=0.001, reset=0.0)
        await asyncio.sleep(2)
        assert not handlers, f"server handlers still running: {handlers}"
        if targets and not server_conns:
            raise AssertionError(
                f"{len(targets)} forwarded connection(s) to the target still open "
                "although the server has no SSH connection"
            )
        if not knobs.get("reset") and not knobs.get("partition"):
            assert not conn._channels, f"{len(conn._channels)} client channel(s) left open"
        if listener is not None:
            listener.close()
        conn.close()
        await conn.wait_closed()

    return harness


def fix_open_request() -> Callable[[], None]:
    """``_finish_open_request`` that closes a session it can no longer use."""
    original = SSHChannel._finish_open_request

    async def finish(self: Any, result: Any) -> None:
        if inspect.isawaitable(result):
            try:
                result = await result
            except asyncssh.ChannelOpenError as exc:
                error = exc

                async def failed() -> Any:
                    raise error

                await original(self, failed())
                return
            if not self._conn:
                result.connection_lost(None)
        await original(self, result)

    SSHChannel._finish_open_request = finish  # type: ignore[method-assign]

    def undo() -> None:
        SSHChannel._finish_open_request = original  # type: ignore[method-assign]

    return undo


async def plain() -> str:
    """On a real loop: 20 clients each ask for a forward and close the connection at once."""
    key = asyncssh.generate_private_key("ssh-ed25519")
    targets: set[asyncio.StreamWriter] = set()

    async def target(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        targets.add(writer)
        try:
            await reader.read()
        finally:
            targets.discard(writer)

    target_server = await asyncio.start_server(target, "127.0.0.1", 0)
    target_port = target_server.sockets[0].getsockname()[1]
    server_conns: set[int] = set()

    class Tracked(Server):
        open = server_conns

    ssh = await asyncssh.create_server(
        Tracked, "127.0.0.1", 0, server_host_keys=[key], gss_host=None
    )
    port = ssh.sockets[0].getsockname()[1]
    for _ in range(20):
        conn = await asyncssh.connect(
            "127.0.0.1", port, known_hosts=None, username="u", gss_host=None
        )
        opening = asyncio.ensure_future(conn.open_connection("127.0.0.1", target_port))
        await asyncio.sleep(0)
        conn.close()
        await conn.wait_closed()
        await asyncio.gather(opening, return_exceptions=True)
        await asyncio.sleep(0.05)
    await asyncio.sleep(1)
    outcome = (
        f"server SSH connections open: {len(server_conns)}, "
        f"forwarded connections to the target open: {len(targets)}"
    )
    for writer in list(targets):
        writer.close()
    ssh.close()
    target_server.close()
    return outcome


def main() -> int:
    configs = [("random", simloop.SimConfig(strict_cancellation=True))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(30), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups, report.summary()
            for group in report.groups:
                assert "still open although" in group.message, group.message
    undo = fix_open_request()
    try:
        for workload in FAILING:
            for name, config in configs:
                fixed = simloop.explore(
                    make_harness(WORKLOADS[workload]), seeds=range(30), config=config
                )
                print(f"{workload}, {name}, open fixed: {fixed.summary().splitlines()[0]}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    outcome = asyncio.run(plain())
    print(f"real asyncio loop, 20 forwards cut off by the client closing: {outcome}")
    assert outcome.startswith("server SSH connections open: 0, "), outcome
    assert not outcome.endswith("open: 0"), outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
