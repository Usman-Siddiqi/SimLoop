"""A small MongoDB wire-protocol server for driving pymongo's async client in a simulation.

It answers ``OP_MSG`` (and a legacy ``OP_QUERY`` handshake) as a standalone
server: ``hello``/``isMaster``, ``ping``, ``buildInfo``, ``endSessions``,
``insert``, ``find`` (equality filters, ``limit``, ``batchSize``),
``getMore``, ``killCursors``, ``delete``, ``count`` and ``findAndModify``
with ``$inc``. A filter or document field ``sleep`` (seconds) delays the
reply, which is how a harness makes an operation slow. No monitoring
streaming (no ``topologyVersion``), so clients poll.
"""

from __future__ import annotations

import asyncio
import datetime
import itertools
import struct
from typing import Any

import bson

OP_REPLY = 1
OP_QUERY = 2004
OP_MSG = 2013
MORE_TO_COME = 1 << 1
HEADER = struct.Struct("<iiii")


def frame(request_id: int, response_to: int, op: int, payload: bytes) -> bytes:
    return HEADER.pack(HEADER.size + len(payload), request_id, response_to, op) + payload


def matches(doc: dict[str, Any], query: dict[str, Any]) -> bool:
    return all(doc.get(k) == v for k, v in query.items() if k != "sleep")


class MongoServer:
    def __init__(self) -> None:
        self.collections: dict[str, list[dict[str, Any]]] = {}
        self.cursors: dict[int, tuple[str, list[dict[str, Any]], int]] = {}
        self.open: set[int] = set()  # connection ids currently connected
        self.commands: list[str] = []
        self._ids = itertools.count(1)
        self._cursor_ids = itertools.count(1000)
        self._request_ids = itertools.count(1)

    async def serve(self, port: int = 27017) -> None:
        server = await asyncio.start_server(self.handle, port=port)
        async with server:
            await server.serve_forever()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn_id = next(self._ids)
        self.open.add(conn_id)
        try:
            while True:
                header = await reader.readexactly(HEADER.size)
                length, request_id, _, op = HEADER.unpack(header)
                body = await reader.readexactly(length - HEADER.size)
                if op == OP_QUERY:
                    query = self.parse_query(body)
                    reply = await self.command(conn_id, query)
                    payload = struct.pack("<iqii", 8, 0, 0, 1) + bson.encode(reply)
                    writer.write(frame(next(self._request_ids), request_id, OP_REPLY, payload))
                elif op == OP_MSG:
                    flags, command = self.parse_msg(body)
                    reply = await self.command(conn_id, command)
                    if flags & MORE_TO_COME:
                        continue
                    payload = struct.pack("<IB", 0, 0) + bson.encode(reply)
                    writer.write(frame(next(self._request_ids), request_id, OP_MSG, payload))
                else:
                    return
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self.open.discard(conn_id)
            writer.close()

    @staticmethod
    def parse_query(body: bytes) -> dict[str, Any]:
        end = body.index(b"\0", 4)
        start = end + 1 + 8
        (size,) = struct.unpack("<i", body[start : start + 4])
        return bson.decode(body[start : start + size])

    @staticmethod
    def parse_msg(body: bytes) -> tuple[int, dict[str, Any]]:
        (flags,) = struct.unpack("<I", body[:4])
        at, command, documents = 4, None, {}
        while at < len(body) - (4 if flags & 1 else 0):
            kind = body[at]
            at += 1
            (size,) = struct.unpack("<i", body[at : at + 4])
            if kind == 0:
                command = bson.decode(body[at : at + size])
            else:
                end = body.index(b"\0", at + 4)
                name = body[at + 4 : end].decode()
                documents[name] = list(bson.decode_all(body[end + 1 : at + size]))
            at += size
        assert command is not None
        command.update(documents)
        return flags, command

    async def command(self, conn_id: int, cmd: dict[str, Any]) -> dict[str, Any]:
        name = next(iter(cmd))
        self.commands.append(name)
        lowered = name.lower()
        if lowered in ("hello", "ismaster"):
            return {
                "helloOk": True,
                "ismaster": True,
                "isWritablePrimary": True,
                "maxBsonObjectSize": 16 * 1024 * 1024,
                "maxMessageSizeBytes": 48_000_000,
                "maxWriteBatchSize": 100_000,
                "localTime": datetime.datetime.now(datetime.UTC),
                "logicalSessionTimeoutMinutes": 30,
                "connectionId": conn_id,
                "minWireVersion": 0,
                "maxWireVersion": 21,
                "readOnly": False,
                "ok": 1.0,
            }
        if lowered in ("ping", "endsessions"):
            return {"ok": 1.0}
        if lowered == "buildinfo":
            return {"version": "7.0.0", "versionArray": [7, 0, 0, 0], "ok": 1.0}
        if name == "insert":
            docs = cmd.get("documents", [])
            for doc in docs:
                await self.pause(doc)
            self.collections.setdefault(cmd["insert"], []).extend(docs)
            return {"n": len(docs), "ok": 1.0}
        if name == "find":
            query = cmd.get("filter", {})
            await self.pause(query)
            coll = cmd["find"]
            found = [d for d in self.collections.get(coll, []) if matches(d, query)]
            if cmd.get("limit"):
                found = found[: abs(int(cmd["limit"]))]
            single = cmd.get("singleBatch")
            return self.batch(coll, found, cmd.get("batchSize"), "firstBatch", single)
        if name == "getMore":
            cursor_id = int(cmd["getMore"])
            if cursor_id not in self.cursors:
                return {"ok": 0.0, "errmsg": f"cursor id {cursor_id} not found", "code": 43}
            coll, rest, size = self.cursors.pop(cursor_id)
            size = cmd.get("batchSize") or size
            return self.batch(coll, rest, size, "nextBatch", False, cursor_id)
        if name == "killCursors":
            killed = [c for c in cmd["cursors"] if self.cursors.pop(c, None) is not None]
            return {"cursorsKilled": killed, "ok": 1.0}
        if name == "delete":
            coll = self.collections.get(cmd["delete"], [])
            n = 0
            for spec in cmd.get("deletes", []):
                keep = [d for d in coll if not matches(d, spec["q"])]
                n += len(coll) - len(keep)
                coll[:] = keep
            return {"n": n, "ok": 1.0}
        if name == "count":
            query = cmd.get("query", {})
            await self.pause(query)
            docs = self.collections.get(cmd["count"], [])
            return {"n": sum(matches(d, query) for d in docs), "ok": 1.0}
        if name == "findAndModify":
            query = cmd.get("query", {})
            await self.pause(query)
            docs = self.collections.setdefault(cmd["findAndModify"], [])
            doc = next((d for d in docs if matches(d, query)), None)
            if doc is None and cmd.get("upsert"):
                doc = {k: v for k, v in query.items() if k != "sleep"}
                docs.append(doc)
            before = dict(doc) if doc is not None else None
            for field, step in cmd.get("update", {}).get("$inc", {}).items():
                if doc is not None:
                    doc[field] = doc.get(field, 0) + step
            value = dict(doc) if doc is not None and cmd.get("new") else before
            return {"value": value, "ok": 1.0}
        return {"ok": 0.0, "errmsg": f"no such command: '{name}'", "code": 59}

    @staticmethod
    async def pause(doc: dict[str, Any]) -> None:
        if doc.get("sleep"):
            await asyncio.sleep(float(doc["sleep"]))

    def batch(
        self,
        coll: str,
        docs: list[dict[str, Any]],
        size: Any,
        key: str,
        single: Any,
        cursor_id: int | None = None,
    ) -> dict[str, Any]:
        size = int(size) if size else len(docs)
        first, rest = docs[:size], docs[size:]
        if rest and not single:
            cursor_id = cursor_id or next(self._cursor_ids)
            self.cursors[cursor_id] = (coll, rest, size)
        else:
            cursor_id = 0
        return {"cursor": {"id": bson.Int64(cursor_id), "ns": f"d.{coll}", key: first}, "ok": 1.0}
