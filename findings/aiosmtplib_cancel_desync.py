"""aiosmtplib 5.1.3: a cancelled command's late reply answers the next command.

An aiosmtpd 1.4.6 server (``handle_RCPT`` and ``handle_DATA`` take 0-20 ms)
and one ``aiosmtplib.SMTP`` client on the simulated network. Three tasks
send six messages each, each to its own recipient, over the shared client.
Checked: the server receives every message the client reports as sent
exactly once, with that message's sender, recipients and content, and a
send nobody cancelled fails only by a timeout or a lost connection. That
held with no faults and with a 20 ms read timeout (``timeout=``), random
and fifo, with ``strict_cancellation=True``.

With 40% of sends under an outer ``asyncio.wait_for`` it fails. A command
cancelled while waiting for its reply leaves the connection open (only a
read *timeout* closes it), with its reply still in flight. The next
command takes that reply as its own answer, and every reply after it is
one command late: the next message's ``MAIL FROM`` gets the cancelled
``RCPT TO``'s ``250``, its ``RCPT TO`` gets the server's ``503 nested
MAIL``, and a send that nobody cancelled is refused with
``SMTPRecipientsRefused``; later commands wait for replies that already
came and time out. Closing the connection on cancellation passes. Known
and fixed on aiosmtplib's main branch (5.1.4, unreleased): "cancelling a
command while waiting for its reply ... now closes the connection".

An earlier version of this harness also saw the next message delivered
inside the cancelled message's envelope. That needed two separate reads
of one socket before the woken task ran, which SimLoop then allowed and a
real socket does not; SimLoop now coalesces data that arrives together.

Run: python findings/aiosmtplib_cancel_desync.py
"""

from __future__ import annotations

import asyncio
import contextlib
import email.message
import logging
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import aiosmtplib
from aiosmtpd.smtp import SMTP as SMTPServerProtocol
from aiosmtplib.protocol import SMTPProtocol

import simloop

logging.getLogger("mail.log").setLevel(logging.CRITICAL)
ERRORS = (aiosmtplib.SMTPException, OSError, TimeoutError)
WORKLOADS: dict[str, dict[str, float]] = {
    "no faults": {},
    "20 ms read timeouts": {"timeout": 0.02},
    "wait_for cancellations": {"cancel": 0.4},
}
FAILING = ("wait_for cancellations",)


def message(tag: str, recipient: str | None = None) -> email.message.EmailMessage:
    msg = email.message.EmailMessage()
    msg["From"] = f"{tag}@a.test"
    msg["To"] = recipient or f"to-{tag}@b.test"
    msg["Subject"] = tag
    msg.set_content(f"body {tag}\n")
    return msg


class Recorder:
    """An aiosmtpd handler that takes ``delay()`` for each recipient and message."""

    def __init__(self, delay: Callable[[], float]) -> None:
        self.delay = delay
        self.received: list[tuple[str, tuple[str, ...], str]] = []  # sender, rcpts, subject

    async def handle_RCPT(
        self, server: Any, session: Any, envelope: Any, address: str, options: Any
    ) -> str:
        await asyncio.sleep(self.delay())
        envelope.rcpt_tos.append(address)
        return "250 OK"

    async def handle_DATA(self, server: Any, session: Any, envelope: Any) -> str:
        await asyncio.sleep(self.delay())
        lines = envelope.content.decode(errors="replace").splitlines()
        subject = next(line[9:] for line in lines if line.startswith("Subject: "))
        self.received.append((envelope.mail_from, tuple(envelope.rcpt_tos), subject))
        return "250 OK"


def make_harness(knobs: dict[str, float]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Every message the client sent arrives once, intact, to its own recipient."""
        rng = simloop.rng()
        net = simloop.network()
        handler = Recorder(lambda: rng.uniform(0, 0.02))

        async def serve() -> None:
            loop = asyncio.get_running_loop()
            server = await loop.create_server(
                lambda: SMTPServerProtocol(handler, hostname="mx"), port=25
            )
            await server.serve_forever()

        net.add_host("mx", entry=serve)
        net.set_link("main", "mx", latency=(0.0005, 0.005))
        await asyncio.sleep(0.1)
        client = aiosmtplib.SMTP(hostname="mx", port=25, timeout=knobs.get("timeout", 5))
        sent: list[str] = []
        refused: list[str] = []
        connecting = asyncio.Lock()

        async def send(tag: str) -> None:
            async with connecting:
                if not client.is_connected:
                    await client.connect()
            await client.send_message(message(tag))
            sent.append(tag)

        async def worker(n: int) -> None:
            for i in range(6):
                tag = f"m{n}-{i}"
                cancellable = rng.random() < knobs.get("cancel", 0)
                try:
                    if cancellable:
                        await asyncio.wait_for(send(tag), rng.uniform(0, 0.03))
                    else:
                        await send(tag)
                except ERRORS as exc:
                    simloop.log(f"{tag}: {type(exc).__name__}")
                    # A send may fail by its own deadline, a read timeout, or the
                    # shared connection being closed under it by another task's.
                    expected = (aiosmtplib.SMTPTimeoutError, aiosmtplib.SMTPServerDisconnected)
                    if not cancellable and not isinstance(exc, expected):
                        refused.append(f"{tag}: {type(exc).__name__}: {exc}")
                await asyncio.sleep(rng.uniform(0, 0.01))

        tasks = [asyncio.create_task(worker(n)) for n in range(3)]
        done, pending = await asyncio.wait(tasks, timeout=120)
        assert not pending, f"{len(pending)} worker(s) stuck"
        for task in done:
            task.result()
        assert not refused, f"a send nobody cancelled failed: {refused[0]}"
        await asyncio.sleep(1)
        delivered: dict[str, int] = {}
        for sender, recipients, subject in handler.received:
            tag = sender.split("@")[0]
            if recipients != (f"to-{tag}@b.test",) or subject != tag:
                raise AssertionError(
                    f"the server delivered {subject}'s message from {sender} to {list(recipients)}"
                )
            delivered[tag] = delivered.get(tag, 0) + 1
        for tag in sent:
            count = delivered.get(tag, 0)
            assert count == 1, (
                f"{tag}: the client says sent, the server received it {count} time(s)"
            )
        twice = {tag: count for tag, count in delivered.items() if count > 1}
        assert not twice, f"delivered more than once: {twice}"
        client.close()

    return harness


def fix_cancelled_reads() -> Callable[[], None]:
    """``read_response`` that closes the connection when it is cancelled."""
    original = SMTPProtocol.read_response

    async def read_response(self: SMTPProtocol, timeout: float | None = None) -> Any:
        try:
            return await original(self, timeout)
        except asyncio.CancelledError:
            if self.transport is not None:
                self.transport.close()
            raise

    SMTPProtocol.read_response = read_response  # type: ignore[method-assign]

    def undo() -> None:
        SMTPProtocol.read_response = original  # type: ignore[method-assign]

    return undo


async def plain() -> list[str]:
    """On a real loop: a send is cancelled while its RCPT is answered, then two more sends."""
    handler = Recorder(lambda: 0.2)
    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        lambda: SMTPServerProtocol(handler, hostname="mx"), "127.0.0.1", 0
    )
    port = server.sockets[0].getsockname()[1]
    client = aiosmtplib.SMTP(hostname="127.0.0.1", port=port, timeout=5)
    await client.connect()
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(client.send_message(message("first", "alice@b.test")), 0.1)
    seen = []
    for tag, recipient in (("second", "bob@b.test"), ("third", "carol@b.test")):
        try:
            await client.send_message(message(tag, recipient))
            seen.append(f"{tag}: sent")
        except aiosmtplib.SMTPException as exc:
            seen.append(f"{tag}: {type(exc).__name__}: {exc}")
    await asyncio.sleep(0.5)
    client.close()
    server.close()
    delivered = [f"{subject} to {list(rcpts)}" for _, rcpts, subject in handler.received]
    return [*seen, f"delivered: {delivered}"]


def main() -> int:
    configs = [("random", simloop.SimConfig(strict_cancellation=True))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(100), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups, report.summary()
            for group in report.groups:
                assert "a send nobody cancelled failed" in group.message, group.message
    undo = fix_cancelled_reads()
    try:
        for workload in FAILING:
            for name, config in configs:
                fixed = simloop.explore(
                    make_harness(WORKLOADS[workload]), seeds=range(100), config=config
                )
                print(f"{workload}, {name}, reads fixed: {fixed.summary().splitlines()[0]}")
                assert fixed.ok, fixed.summary()
    finally:
        undo()
    outcome = asyncio.run(plain())
    print("real asyncio loop: " + "; ".join(outcome))
    assert "nested MAIL" in outcome[0], outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
