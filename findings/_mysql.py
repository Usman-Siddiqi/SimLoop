"""A small MySQL wire-protocol server for driving aiomysql inside a simulation.

It accepts any login (``mysql_native_password`` is advertised, the scramble
is not checked) and answers ``COM_QUERY`` and ``COM_PING``:

* ``SELECT '<text>'`` returns one row with one column holding ``text``;
* ``SELECT SLEEP(<seconds>), '<text>'`` waits first, then returns ``0`` and
  ``text``;
* ``BEGIN``/``START TRANSACTION`` and ``COMMIT``/``ROLLBACK`` set and clear
  the in-transaction status flag; any other statement gets an OK packet.

``log`` records every statement each connection ran, by connection id.
"""

from __future__ import annotations

import asyncio
import itertools
import re
import struct

CLIENT_LONG_PASSWORD = 0x1
CLIENT_CONNECT_WITH_DB = 0x8
CLIENT_PROTOCOL_41 = 0x200
CLIENT_TRANSACTIONS = 0x2000
CLIENT_SECURE_CONNECTION = 0x8000
CLIENT_PLUGIN_AUTH = 0x80000
CAPABILITIES = (
    CLIENT_LONG_PASSWORD
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
)
STATUS_IN_TRANS = 0x1
STATUS_AUTOCOMMIT = 0x2
COM_QUIT, COM_QUERY, COM_PING = 0x01, 0x03, 0x0E
TYPE_VAR_STRING = 0xFD
UTF8MB4 = 45
SELECT_TEXT = re.compile(r"SELECT\s+(?:SLEEP\(([\d.eE+-]+)\)\s*,\s*)?'((?:[^'\\]|\\.)*)'", re.I)


def lenenc(data: bytes) -> bytes:
    if len(data) < 251:
        return bytes([len(data)]) + data
    return b"\xfc" + struct.pack("<H", len(data)) + data


def column(name: str) -> bytes:
    fields = b"".join(lenenc(f.encode()) for f in ("def", "", "", "", name, ""))
    return fields + b"\x0c" + struct.pack("<HIBHB", UTF8MB4, 1024, TYPE_VAR_STRING, 0, 0) + b"\0\0"


class Session:
    def __init__(self, conn_id: int, writer: asyncio.StreamWriter) -> None:
        self.id = conn_id
        self.writer = writer
        self.status = STATUS_AUTOCOMMIT
        self.seq = 0

    def send(self, payload: bytes) -> None:
        self.writer.write(struct.pack("<I", len(payload))[:3] + bytes([self.seq]) + payload)
        self.seq = (self.seq + 1) % 256

    def ok(self) -> None:
        self.send(b"\0\0\0" + struct.pack("<HH", self.status, 0))

    def eof(self) -> None:
        self.send(b"\xfe" + struct.pack("<HH", 0, self.status))

    def error(self, code: int, text: str) -> None:
        self.send(b"\xff" + struct.pack("<H", code) + b"#HY000" + text.encode())


class MysqlServer:
    def __init__(self) -> None:
        self.ids = itertools.count(1)
        self.log: dict[int, list[str]] = {}

    async def serve(self, port: int = 3306) -> None:
        server = await asyncio.start_server(self.handle, port=port)
        async with server:
            await server.serve_forever()

    async def read(self, reader: asyncio.StreamReader, session: Session) -> bytes:
        header = await reader.readexactly(4)
        size = header[0] | header[1] << 8 | header[2] << 16
        session.seq = (header[3] + 1) % 256
        return await reader.readexactly(size)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = Session(next(self.ids), writer)
        self.log[session.id] = []
        try:
            salt = bytes(range(1, 21))
            session.send(
                b"\x0a8.0.36\0"
                + struct.pack("<I", session.id)
                + salt[:8]
                + b"\0"
                + struct.pack("<HB", CAPABILITIES & 0xFFFF, UTF8MB4)
                + struct.pack("<HHB", session.status, CAPABILITIES >> 16, 21)
                + b"\0" * 10
                + salt[8:]
                + b"\0mysql_native_password\0"
            )
            await self.read(reader, session)  # HandshakeResponse41: any login is accepted
            session.ok()
            while True:
                payload = await self.read(reader, session)
                if payload[0] == COM_QUIT:
                    break
                if payload[0] == COM_PING:
                    session.ok()
                elif payload[0] == COM_QUERY:
                    await self.query(session, payload[1:].decode())
                else:
                    session.error(1047, "Unknown command")
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def query(self, session: Session, sql: str) -> None:
        self.log[session.id].append(sql)
        statement = sql.strip().upper()
        if match := SELECT_TEXT.match(sql.strip()):
            values = [match.group(2).replace("\\'", "'").encode()]
            if match.group(1):
                await asyncio.sleep(float(match.group(1)))
                values.insert(0, b"0")
            session.send(bytes([len(values)]))
            for n in range(len(values)):
                session.send(column(f"c{n}"))
            session.eof()
            session.send(b"".join(lenenc(v) for v in values))
            session.eof()
        elif statement in ("BEGIN", "START TRANSACTION"):
            session.status |= STATUS_IN_TRANS
            session.ok()
        elif statement in ("COMMIT", "ROLLBACK"):
            session.status &= ~STATUS_IN_TRANS
            session.ok()
        else:
            session.ok()
