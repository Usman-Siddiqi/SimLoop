"""A tiny NATS server for driving nats-py inside a simulation.

It speaks the core protocol: INFO/CONNECT, PING/PONG, SUB/UNSUB (with an
auto-unsubscribe limit), PUB/HPUB and MSG/HMSG, routed between every
connection it serves. ``log`` keeps each connection's commands in the order
they were processed, which is what the harnesses check flush ordering
against.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from dataclasses import dataclass, field

INFO = {
    "server_id": "SIM",
    "server_name": "sim",
    "version": "2.10.0",
    "go": "go1.22",
    "host": "0.0.0.0",
    "port": 4222,
    "headers": True,
    "max_payload": 1048576,
    "proto": 1,
}


@dataclass
class Sub:
    conn: Conn
    sid: bytes
    subject: bytes
    queue: bytes | None
    remaining: int = 0  # 0: no auto-unsubscribe limit


@dataclass
class Conn:
    cid: int
    writer: asyncio.StreamWriter
    subs: dict[bytes, Sub] = field(default_factory=dict)
    log: list[tuple[bytes, bytes]] = field(default_factory=list)


def matches(pattern: bytes, subject: bytes) -> bool:
    want = pattern.split(b".")
    have = subject.split(b".")
    for i, token in enumerate(want):
        if token == b">":
            return len(have) > i
        if i >= len(have) or (token != b"*" and token != have[i]):
            return False
    return len(want) == len(have)


class NatsServer:
    def __init__(self) -> None:
        self.conns: dict[int, Conn] = {}
        self.history: list[Conn] = []
        self._ids = itertools.count(1)

    async def serve(self, port: int = 4222) -> None:
        server = await asyncio.start_server(self.handle, port=port)
        async with server:
            await server.serve_forever()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = Conn(next(self._ids), writer)
        self.conns[conn.cid] = conn
        self.history.append(conn)
        writer.write(b"INFO " + json.dumps(INFO | {"client_id": conn.cid}).encode() + b"\r\n")
        try:
            while line := await reader.readline():
                await self.command(conn, reader, line.rstrip(b"\r\n"))
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            del self.conns[conn.cid]
            writer.close()

    async def command(self, conn: Conn, reader: asyncio.StreamReader, line: bytes) -> None:
        op, _, rest = line.partition(b" ")
        op = op.upper()
        args = rest.split()
        if op == b"PING":
            conn.log.append((b"PING", b""))
            conn.writer.write(b"PONG\r\n")
        elif op in (b"PONG", b"CONNECT"):
            pass
        elif op == b"SUB":
            sid = args[-1]
            queue = args[1] if len(args) == 3 else None
            conn.subs[sid] = Sub(conn, sid, args[0], queue)
            conn.log.append((b"SUB", args[0]))
        elif op == b"UNSUB":
            sub = conn.subs.get(args[0])
            if sub is not None:
                limit = int(args[1]) if len(args) > 1 else 0
                if limit:
                    sub.remaining = limit
                else:
                    del conn.subs[args[0]]
        elif op in (b"PUB", b"HPUB"):
            headers = op == b"HPUB"
            subject = args[0]
            total = int(args[-1])
            hdr = int(args[-2]) if headers else 0
            reply = args[1] if len(args) == (4 if headers else 3) else None
            body = (await reader.readexactly(total + 2))[:-2]
            conn.log.append((op, body[hdr:]))
            self.route(subject, reply, body, hdr if headers else None)
        else:
            conn.writer.write(b"-ERR 'Unknown Protocol Operation'\r\n")

    def route(self, subject: bytes, reply: bytes | None, body: bytes, hdr: int | None) -> None:
        groups: dict[bytes, list[Sub]] = {}
        targets: list[Sub] = []
        for conn in self.conns.values():
            for sub in list(conn.subs.values()):
                if not matches(sub.subject, subject):
                    continue
                if sub.queue is None:
                    targets.append(sub)
                else:
                    groups.setdefault(sub.queue, []).append(sub)
        # A queue group delivers to one member; the first is as good as any here.
        targets.extend(members[0] for members in groups.values())
        for sub in targets:
            parts = [subject, sub.sid] + ([reply] if reply else [])
            if hdr is None:
                head = b"MSG " + b" ".join([*parts, str(len(body)).encode()])
            else:
                sizes = [str(hdr).encode(), str(len(body)).encode()]
                head = b"HMSG " + b" ".join([*parts, *sizes])
            sub.conn.writer.write(head + b"\r\n" + body + b"\r\n")
            if sub.remaining:
                sub.remaining -= 1
                if not sub.remaining:
                    del sub.conn.subs[sub.sid]
