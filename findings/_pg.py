"""A tiny PostgreSQL wire-protocol server for driving asyncpg inside a simulation.

It answers the startup handshake, prepared ``SELECT $1::text`` (which echoes
its parameter after ``delay()`` seconds), simple queries (the pool's reset
query) and CancelRequest. Everything else gets an error.
"""

from __future__ import annotations

import asyncio
import itertools
import struct
from collections.abc import Callable

SSL_REQUEST = 80877103
CANCEL_REQUEST = 80877102
TEXT_OID = 25
PARAMETERS = {
    "server_version": "16.0",
    "client_encoding": "UTF8",
    "server_encoding": "UTF8",
    "DateStyle": "ISO, MDY",
    "integer_datetimes": "on",
    "standard_conforming_strings": "on",
    "TimeZone": "UTC",
}


def message(kind: bytes, payload: bytes = b"") -> bytes:
    return kind + struct.pack("!I", len(payload) + 4) + payload


def cstr(text: str) -> bytes:
    return text.encode() + b"\0"


def error(code: str, text: str) -> bytes:
    fields = b"S" + cstr("ERROR") + b"V" + cstr("ERROR") + b"C" + cstr(code) + b"M" + cstr(text)
    return message(b"E", fields + b"\0")


class PgServer:
    def __init__(self, delay: Callable[[], float]) -> None:
        self.delay = delay
        self.cancels = 0
        # Set to make the next simple query stall, then drop the connection.
        self.drop_on_simple_query = False
        self._backends: dict[int, tuple[int, Backend]] = {}
        self._pids = itertools.count(1000)

    async def serve(self, port: int = 5432) -> None:
        server = await asyncio.start_server(self.handle, port=port)
        async with server:
            await server.serve_forever()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                (length,) = struct.unpack("!I", await reader.readexactly(4))
                body = await reader.readexactly(length - 4)
                (code,) = struct.unpack("!I", body[:4])
                if code == SSL_REQUEST:
                    writer.write(b"N")
                    continue
                if code == CANCEL_REQUEST:
                    pid, secret = struct.unpack("!II", body[4:12])
                    entry = self._backends.get(pid)
                    if entry is not None and entry[0] == secret:
                        self.cancels += 1
                        entry[1].cancel()
                    return
                break  # a StartupMessage
            pid = next(self._pids)
            secret = pid * 7 + 3
            backend = Backend(self, reader, writer)
            self._backends[pid] = (secret, backend)
            hello = message(b"R", struct.pack("!I", 0))
            for name, value in PARAMETERS.items():
                hello += message(b"S", cstr(name) + cstr(value))
            hello += message(b"K", struct.pack("!II", pid, secret)) + message(b"Z", b"I")
            writer.write(hello)
            try:
                await backend.run()
            finally:
                del self._backends[pid]
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()


class Backend:
    def __init__(
        self, server: PgServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.server = server
        self.reader = reader
        self.writer = writer
        self.value = b""
        self.failed = False  # after an error, skip messages until Sync
        self.working: asyncio.Future[None] | None = None

    def cancel(self) -> None:
        if self.working is not None and not self.working.done():
            self.working.cancel()

    async def run(self) -> None:
        while True:
            kind = await self.reader.readexactly(1)
            (length,) = struct.unpack("!I", await self.reader.readexactly(4))
            body = await self.reader.readexactly(length - 4)
            if kind == b"X":
                return
            if kind == b"S":
                self.failed = False
                self.writer.write(message(b"Z", b"I"))
                await self.writer.drain()
            elif kind == b"H" or self.failed:
                pass
            elif kind == b"Q":
                if self.server.drop_on_simple_query:
                    await asyncio.sleep(0.05)
                    self.writer.transport.abort()
                    return
                self.writer.write(message(b"C", cstr("SELECT 0")) + message(b"Z", b"I"))
            elif kind == b"P":
                self.writer.write(message(b"1"))
            elif kind == b"D":
                field = cstr("text") + struct.pack("!IHIhih", 0, 0, TEXT_OID, -1, -1, 0)
                self.writer.write(message(b"t", struct.pack("!HI", 1, TEXT_OID)))
                self.writer.write(message(b"T", struct.pack("!H", 1) + field))
            elif kind == b"B":
                self.value = bind_parameter(body)
                self.writer.write(message(b"2"))
            elif kind == b"E":
                await self.execute()
            elif kind == b"C":
                self.writer.write(message(b"3"))
            else:
                self.writer.write(error("0A000", f"unsupported message {kind!r}"))
                self.failed = True

    async def execute(self) -> None:
        self.working = asyncio.ensure_future(asyncio.sleep(self.server.delay()))
        try:
            await self.working
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            self.writer.write(error("57014", "canceling statement due to user request"))
            self.failed = True
            return
        finally:
            self.working = None
        row = struct.pack("!Hi", 1, len(self.value)) + self.value
        self.writer.write(message(b"D", row) + message(b"C", cstr("SELECT 1")))


def bind_parameter(body: bytes) -> bytes:
    """The first parameter value of a Bind message."""
    _portal, _statement, rest = body.split(b"\0", 2)
    (formats,) = struct.unpack("!H", rest[:2])
    rest = rest[2 + 2 * formats :]
    (count,) = struct.unpack("!H", rest[:2])
    if not count:
        return b""
    (size,) = struct.unpack("!i", rest[2:6])
    return rest[6 : 6 + size]
