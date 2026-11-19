"""A small RESP2 server for driving Redis clients inside a simulation.

It keeps strings, sorted sets and streams with expiry on the (simulated)
wall clock, has pub/sub and a blocking ``XREAD``, and implements
``WATCH``/``MULTI``/``EXEC`` with Redis's semantics: a transaction is
discarded if a watched key was written, deleted or expired after the
``WATCH``.
"""

from __future__ import annotations

import asyncio
import fnmatch
import time

import simloop

QUEUED = object()
NIL_ARRAY = object()


def encode(value: object) -> bytes:
    if value is None:
        return b"$-1\r\n"
    if value is NIL_ARRAY:
        return b"*-1\r\n"
    if value is QUEUED:
        return b"+QUEUED\r\n"
    if isinstance(value, Exception):
        text = str(value)
        prefixed = text.split(" ", 1)[0].isupper()
        return f"-{text if prefixed else 'ERR ' + text}\r\n".encode()
    if isinstance(value, bool):
        return f":{int(value)}\r\n".encode()
    if isinstance(value, int):
        return f":{value}\r\n".encode()
    if isinstance(value, float):
        return encode(format_score(value).encode())
    if isinstance(value, str) and value in ("OK", "PONG"):
        return f"+{value}\r\n".encode()
    if isinstance(value, list):
        return f"*{len(value)}\r\n".encode() + b"".join(encode(v) for v in value)
    data = value if isinstance(value, bytes) else str(value).encode()
    return b"$%d\r\n%s\r\n" % (len(data), data)


def stream_id(raw: bytes) -> tuple[int, int]:
    ms, _, seq = raw.decode().partition("-")
    return int(ms), int(seq or 0)


def format_score(score: float) -> str:
    if score in (float("inf"), float("-inf")):
        return "inf" if score > 0 else "-inf"
    return str(int(score)) if score.is_integer() else repr(score)


def parse_bound(raw: bytes) -> tuple[float, bool]:
    """A ZRANGEBYSCORE bound: (value, exclusive)."""
    text = raw.decode()
    exclusive = text.startswith("(")
    return float(text.lstrip("(")), exclusive


async def read_command(reader: asyncio.StreamReader) -> list[bytes] | None:
    line = await reader.readline()
    if not line:
        return None
    if not line.startswith(b"*"):
        return line.strip().split()
    args = []
    for _ in range(int(line[1:])):
        size = int((await reader.readline())[1:])
        args.append((await reader.readexactly(size + 2))[:-2])
    return args


# Reply to HELLO 3 for RESP3-only clients; every other reply is the same in RESP2.
HELLO3 = (
    b"%7\r\n+server\r\n+redis\r\n+version\r\n+7.2.0\r\n+proto\r\n:3\r\n+id\r\n:1\r\n"
    b"+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n"
)


class Session:
    """Per-connection transaction state."""

    def __init__(self, writer: asyncio.StreamWriter | None = None) -> None:
        self.watched: dict[bytes, int] = {}
        self.queued: list[list[bytes]] | None = None
        self.failed = False
        self.writer = writer
        self.channels: set[bytes] = set()


class RespServer:
    """Strings, sorted sets, streams, expiry, transactions, pub/sub and the handshake.

    ``slow`` adds a random processing delay per command, which is what turns a
    client timeout into a late reply on a reused connection; ``delay`` adds a
    fixed one (usable outside a simulation). ``resp3`` answers ``HELLO 3``.
    ``log`` records every executed command (transactions as one entry each).
    """

    def __init__(self, slow: float = 0.0, *, delay: float = 0.0, resp3: bool = False) -> None:
        self.slow = slow
        self.delay = delay
        self.resp3 = resp3
        self.data: dict[bytes, bytes] = {}
        self.zsets: dict[bytes, dict[bytes, float]] = {}
        self.expires: dict[bytes, float] = {}
        self.versions: dict[bytes, int] = {}
        self.log: list[list[bytes]] = []
        self.subscribers: dict[bytes, set[Session]] = {}
        self.streams: dict[bytes, list[tuple[bytes, list[bytes]]]] = {}
        self.last_ids: dict[bytes, tuple[int, int]] = {}
        self.stream_waiters: list[asyncio.Future[None]] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = Session(writer)
        try:
            while (command := await read_command(reader)) is not None:
                if self.resp3 and command[0].upper() == b"HELLO":
                    writer.write(HELLO3)
                    continue
                if self.slow:
                    await asyncio.sleep(simloop.rng().uniform(0, self.slow))
                elif self.delay:
                    await asyncio.sleep(self.delay)
                frames = self.pubsub(session, command)
                if frames is None and command[0].upper() == b"XREAD":
                    frames = [await self.xread(command)]
                if frames is None:
                    frames = [self.dispatch(session, command)]
                writer.write(b"".join(encode(frame) for frame in frames))
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            for channel in session.channels:
                self.subscribers.get(channel, set()).discard(session)
            writer.close()

    def pubsub(self, session: Session, command: list[bytes]) -> list[object] | None:
        """SUBSCRIBE, UNSUBSCRIBE, PUBLISH, and PING while subscribed (RESP2 push frames)."""
        name = command[0].upper()
        if name == b"SUBSCRIBE":
            frames: list[object] = []
            for channel in command[1:]:
                session.channels.add(channel)
                self.subscribers.setdefault(channel, set()).add(session)
                frames.append([b"subscribe", channel, len(session.channels)])
            self.log.append(command)
            return frames
        if name == b"UNSUBSCRIBE":
            channels = command[1:] or sorted(session.channels)
            frames = []
            for channel in channels:
                session.channels.discard(channel)
                self.subscribers.get(channel, set()).discard(session)
                frames.append([b"unsubscribe", channel, len(session.channels)])
            self.log.append(command)
            return frames or [[b"unsubscribe", None, 0]]
        if name == b"PUBLISH":
            channel, data = command[1], command[2]
            receivers = list(self.subscribers.get(channel, ()))
            for receiver in receivers:
                if receiver.writer is not None and not receiver.writer.is_closing():
                    receiver.writer.write(encode([b"message", channel, data]))
            self.log.append(command)
            return [len(receivers)]
        if name == b"PING" and session.channels:
            return [[b"pong", b""]]
        return None

    async def xread(self, command: list[bytes]) -> object:
        """``XREAD [COUNT n] [BLOCK ms] STREAMS key... id...``, waiting for an XADD if asked."""
        self.log.append(command)
        options = [a.upper() for a in command]
        count = int(command[options.index(b"COUNT") + 1]) if b"COUNT" in options else None
        block = int(command[options.index(b"BLOCK") + 1]) if b"BLOCK" in options else None
        rest = command[options.index(b"STREAMS") + 1 :]
        keys, ids = rest[: len(rest) // 2], rest[len(rest) // 2 :]
        after = [
            self.last_ids.get(k, (0, 0)) if i == b"$" else stream_id(i)
            for k, i in zip(keys, ids, strict=True)
        ]
        deadline = (
            None if block is None else asyncio.get_running_loop().time() + (block or 1e9) / 1000
        )
        while True:
            found = []
            for key, start in zip(keys, after, strict=True):
                entries = [e for e in self.streams.get(key, []) if stream_id(e[0]) > start]
                if entries:
                    found.append([key, [[i, f] for i, f in entries[:count]]])
            if found or deadline is None:
                return found or NIL_ARRAY
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return NIL_ARRAY
            waiter = asyncio.get_running_loop().create_future()
            self.stream_waiters.append(waiter)
            try:
                await asyncio.wait_for(waiter, remaining)
            except TimeoutError:
                return NIL_ARRAY
            finally:
                if waiter in self.stream_waiters:
                    self.stream_waiters.remove(waiter)

    def xadd(self, key: bytes, args: list[bytes]) -> object:
        if args[0] != b"*":
            return Exception("only auto-generated stream IDs are supported")
        ms = int(time.time() * 1000)
        last = self.last_ids.get(key, (0, 0))
        new = (ms, 0) if ms > last[0] else (last[0], last[1] + 1)
        entry_id = f"{new[0]}-{new[1]}".encode()
        self.last_ids[key] = new
        self.streams.setdefault(key, []).append((entry_id, list(args[1:])))
        waiters, self.stream_waiters = self.stream_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)
        return entry_id

    def xinfo_stream(self, key: bytes) -> object:
        entries = self.streams.get(key)
        if entries is None:
            return Exception("no such key")
        first = [entries[0][0], entries[0][1]] if entries else None
        last = [entries[-1][0], entries[-1][1]] if entries else None
        ms, seq = self.last_ids[key]
        return [
            b"length", len(entries), b"radix-tree-keys", 1, b"radix-tree-nodes", 2,
            b"last-generated-id", f"{ms}-{seq}".encode(), b"groups", 0,
            b"first-entry", first, b"last-entry", last,
        ]  # fmt: skip

    def dispatch(self, session: Session, command: list[bytes]) -> object:
        name = command[0].upper()
        if name == b"MULTI":
            if session.queued is not None:
                return Exception("MULTI calls can not be nested")
            session.queued, session.failed = [], False
            return "OK"
        if name == b"DISCARD":
            if session.queued is None:
                return Exception("DISCARD without MULTI")
            session.queued = None
            session.watched.clear()
            return "OK"
        if name == b"EXEC":
            if session.queued is None:
                return Exception("EXEC without MULTI")
            queued, session.queued = session.queued, None
            watched, session.watched = session.watched, {}
            if session.failed:
                return Exception("EXECABORT Transaction discarded because of previous errors.")
            for key, version in watched.items():
                self.alive(key)
                if self.versions.get(key, 0) != version:
                    return NIL_ARRAY
            replies = [self.execute(c) for c in queued]
            self.log.append([b"EXEC", *(b" ".join(c) for c in queued)])
            return replies
        if name == b"WATCH":
            if session.queued is not None:
                return Exception("WATCH inside MULTI is not allowed")
            for key in command[1:]:
                self.alive(key)
                session.watched.setdefault(key, self.versions.get(key, 0))
            return "OK"
        if name in (b"UNWATCH", b"RESET"):
            session.watched.clear()
            session.queued = None
            return "OK" if name == b"UNWATCH" else "RESET"
        if session.queued is not None:
            if not self.known(name):
                session.failed = True
                return Exception(f"unknown command '{name.decode()}'")
            session.queued.append(command)
            return QUEUED
        self.log.append(command)
        return self.execute(command)

    # -- keyspace -----------------------------------------------------------

    def touch(self, key: bytes) -> None:
        self.versions[key] = self.versions.get(key, 0) + 1

    def alive(self, key: bytes) -> bool:
        deadline = self.expires.get(key)
        if deadline is not None and deadline <= time.time():
            self.remove(key)
        return key in self.data or key in self.zsets

    def remove(self, key: bytes) -> bool:
        existed = self.data.pop(key, None) is not None
        existed = self.zsets.pop(key, None) is not None or existed
        self.expires.pop(key, None)
        if existed:
            self.touch(key)
        return existed

    def set_string(self, key: bytes, value: bytes, ttl: float | None) -> None:
        self.remove(key)
        self.data[key] = value
        if ttl is not None:
            self.expires[key] = time.time() + ttl
        self.touch(key)

    def zset(self, key: bytes) -> dict[bytes, float]:
        self.alive(key)
        return self.zsets.get(key, {})

    def ranked(self, key: bytes) -> list[tuple[bytes, float]]:
        return sorted(self.zset(key).items(), key=lambda item: (item[1], item[0]))

    def store_zset(self, key: bytes, members: dict[bytes, float]) -> None:
        if members:
            self.zsets[key] = members
        else:
            self.zsets.pop(key, None)
            self.expires.pop(key, None)
        self.touch(key)

    # -- commands -------------------------------------------------------------

    COMMANDS = frozenset(
        b"PING ECHO GET SET SETEX PSETEX INCR INCRBY DEL EXISTS EXPIRE PEXPIRE PTTL KEYS DBSIZE"
        b" INFO ZADD ZREM ZSCORE ZINCRBY ZCARD ZRANGE ZRANGEBYSCORE ZREMRANGEBYSCORE"
        b" XADD XINFO CLIENT SELECT AUTH".split()
    )

    def known(self, name: bytes) -> bool:
        return name in self.COMMANDS

    def execute(self, command: list[bytes]) -> object:
        name = command[0].upper()
        args = command[1:]
        if name == b"PING":
            return "PONG"
        if name == b"ECHO":
            return args[0]
        if name == b"GET":
            return self.data.get(args[0]) if self.alive(args[0]) else None
        if name == b"SET":
            ttl = None
            options = [a.upper() for a in args[2:]]
            for flag, scale in ((b"PX", 0.001), (b"EX", 1.0)):
                if flag in options:
                    ttl = int(args[2 + options.index(flag) + 1]) * scale
            exists = self.alive(args[0])
            if (b"NX" in options and exists) or (b"XX" in options and not exists):
                return None
            self.set_string(args[0], args[1], ttl)
            return "OK"
        if name in (b"SETEX", b"PSETEX"):
            scale = 1.0 if name == b"SETEX" else 0.001
            self.set_string(args[0], args[2], int(args[1]) * scale)
            return "OK"
        if name in (b"INCR", b"INCRBY"):
            step = int(args[1]) if name == b"INCRBY" else 1
            value = int(self.data.get(args[0], b"0") if self.alive(args[0]) else b"0") + step
            self.data[args[0]] = str(value).encode()
            self.touch(args[0])
            return value
        if name == b"DEL":
            return sum(self.remove(k) for k in args if self.alive(k))
        if name == b"EXISTS":
            return sum(self.alive(k) for k in args)
        if name in (b"EXPIRE", b"PEXPIRE"):
            if not self.alive(args[0]):
                return 0
            scale = 1.0 if name == b"EXPIRE" else 0.001
            self.expires[args[0]] = time.time() + int(args[1]) * scale
            self.touch(args[0])
            return 1
        if name == b"PTTL":
            if not self.alive(args[0]):
                return -2
            deadline = self.expires.get(args[0])
            return -1 if deadline is None else round((deadline - time.time()) * 1000)
        if name == b"KEYS":
            pattern = args[0].decode()
            keys = [k for k in [*self.data, *self.zsets] if self.alive(k)]
            return sorted(k for k in keys if fnmatch.fnmatchcase(k.decode(), pattern))
        if name == b"DBSIZE":
            return sum(self.alive(k) for k in [*self.data, *self.zsets])
        if name == b"INFO":
            return b"redis_version:7.2.0\r\nconnected_clients:1\r\nused_memory:1000\r\n"
        if name == b"ZADD":
            return self.zadd(args[0], args[1:])
        if name == b"ZREM":
            members = dict(self.zset(args[0]))
            removed = sum(members.pop(m, None) is not None for m in args[1:])
            if removed:
                self.store_zset(args[0], members)
            return removed
        if name == b"ZSCORE":
            return self.zset(args[0]).get(args[1])
        if name == b"ZINCRBY":
            members = dict(self.zset(args[0]))
            members[args[2]] = members.get(args[2], 0.0) + float(args[1])
            self.store_zset(args[0], members)
            return members[args[2]]
        if name == b"ZCARD":
            return len(self.zset(args[0]))
        if name == b"ZRANGE":
            ranked = self.ranked(args[0])
            start, stop = int(args[1]), int(args[2])
            stop = len(ranked) + stop if stop < 0 else stop
            start = max(len(ranked) + start if start < 0 else start, 0)
            return self.listing(ranked[start : stop + 1], args[3:])
        if name in (b"ZRANGEBYSCORE", b"ZREMRANGEBYSCORE"):
            (low, low_open), (high, high_open) = parse_bound(args[1]), parse_bound(args[2])
            ranked = [
                (m, s)
                for m, s in self.ranked(args[0])
                if (s > low if low_open else s >= low) and (s < high if high_open else s <= high)
            ]
            if name == b"ZREMRANGEBYSCORE":
                if ranked:
                    members = dict(self.zset(args[0]))
                    for member, _ in ranked:
                        del members[member]
                    self.store_zset(args[0], members)
                return len(ranked)
            options = [a.upper() for a in args[3:]]
            if b"LIMIT" in options:
                at = options.index(b"LIMIT")
                offset, count = int(args[3 + at + 1]), int(args[3 + at + 2])
                ranked = ranked[offset:] if count < 0 else ranked[offset : offset + count]
            return self.listing(ranked, args[3:])
        if name == b"XADD":
            return self.xadd(args[0], args[1:])
        if name == b"XINFO" and args[0].upper() == b"STREAM":
            return self.xinfo_stream(args[1])
        if name in (b"CLIENT", b"SELECT", b"AUTH"):
            return "OK"
        if name == b"HELLO":
            return Exception("unknown command 'HELLO'")
        return Exception(f"unknown command '{name.decode()}'")

    def zadd(self, key: bytes, args: list[bytes]) -> object:
        flags = set()
        while args and args[0].upper() in (b"NX", b"XX", b"GT", b"LT", b"CH", b"INCR"):
            flags.add(args[0].upper())
            args = args[1:]
        members = dict(self.zset(key))
        changed = added = 0
        for score_raw, member in zip(args[::2], args[1::2], strict=True):
            score = float(score_raw)
            if (b"NX" in flags and member in members) or (b"XX" in flags and member not in members):
                continue
            if b"INCR" in flags:
                score += members.get(member, 0.0)
            added += member not in members
            changed += members.get(member) != score
            members[member] = score
        if changed:
            self.store_zset(key, members)
        if b"INCR" in flags:
            return members.get(args[1])
        return changed if b"CH" in flags else added

    @staticmethod
    def listing(ranked: list[tuple[bytes, float]], options: list[bytes]) -> list[object]:
        if b"WITHSCORES" in (o.upper() for o in options):
            return [x for member, score in ranked for x in (member, score)]
        return [member for member, _ in ranked]

    async def serve(self, port: int = 6379) -> None:
        server = await asyncio.start_server(self.handle, port=port)
        async with server:
            await server.serve_forever()
