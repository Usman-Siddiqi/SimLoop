"""A simulated Redis Cluster built from ``_resp.RespServer`` nodes.

Every node answers ``CLUSTER SLOTS``, ``CLUSTER SHARDS``-free clients'
``COMMAND`` and ``ASKING``, and redirects a keyed command for a slot it does
not serve with ``MOVED``. A slot migration follows Redis: while a slot
migrates, the source serves the keys it still has and answers ``ASK`` for
the others (``TRYAGAIN`` for a multi-key command split across both nodes),
and the target serves the slot only right after ``ASKING``. Keys move one at
a time, and the slot changes owner when the last one has moved.

Each primary may have a replica that shares its keyspace (synchronous
replication, so a failover loses no acknowledged write). ``failover``
crashes the primary's host and, after a delay, gives its slots to the
replica. Replicas answer ``MOVED`` to their primary until promoted.
"""

from __future__ import annotations

import asyncio
import binascii
import collections

from _resp import RespServer, Session

import simloop

SLOTS = 16384


def key_slot(key: bytes) -> int:
    start = key.find(b"{")
    if start != -1:
        end = key.find(b"}", start + 1)
        if end > start + 1:
            key = key[start + 1 : end]
    return binascii.crc_hqx(key, 0) % SLOTS


# name: (arity, flags, first key, last key, step); keyless commands have 0, 0, 0.
_TABLE: dict[bytes, tuple[int, tuple[bytes, ...], int, int, int]] = {
    b"get": (2, (b"readonly", b"fast"), 1, 1, 1),
    b"set": (-3, (b"write",), 1, 1, 1),
    b"setex": (4, (b"write",), 1, 1, 1),
    b"psetex": (4, (b"write",), 1, 1, 1),
    b"incr": (2, (b"write", b"fast"), 1, 1, 1),
    b"incrby": (3, (b"write", b"fast"), 1, 1, 1),
    b"del": (-2, (b"write",), 1, -1, 1),
    b"exists": (-2, (b"readonly", b"fast"), 1, -1, 1),
    b"expire": (-3, (b"write", b"fast"), 1, 1, 1),
    b"pexpire": (-3, (b"write", b"fast"), 1, 1, 1),
    b"pttl": (2, (b"readonly", b"fast"), 1, 1, 1),
    b"zadd": (-4, (b"write",), 1, 1, 1),
    b"zrem": (-3, (b"write",), 1, 1, 1),
    b"zscore": (3, (b"readonly",), 1, 1, 1),
    b"zincrby": (4, (b"write",), 1, 1, 1),
    b"zcard": (2, (b"readonly",), 1, 1, 1),
    b"zrange": (-4, (b"readonly",), 1, 1, 1),
    b"zrangebyscore": (-4, (b"readonly",), 1, 1, 1),
    b"zremrangebyscore": (4, (b"write",), 1, 1, 1),
    b"xadd": (-5, (b"write",), 1, 1, 1),
    b"watch": (-2, (b"noscript", b"fast"), 1, -1, 1),
    b"ping": (-1, (b"fast",), 0, 0, 0),
    b"echo": (2, (b"fast",), 0, 0, 0),
    b"keys": (2, (b"readonly",), 0, 0, 0),
    b"dbsize": (1, (b"readonly", b"fast"), 0, 0, 0),
    b"info": (-1, (b"loading",), 0, 0, 0),
    b"client": (-2, (b"loading",), 0, 0, 0),
    b"select": (2, (b"loading",), 0, 0, 0),
    b"auth": (-2, (b"loading",), 0, 0, 0),
    b"multi": (1, (b"fast",), 0, 0, 0),
    b"exec": (1, (b"skip_slowlog",), 0, 0, 0),
    b"discard": (1, (b"fast",), 0, 0, 0),
    b"unwatch": (1, (b"fast",), 0, 0, 0),
    b"cluster": (-2, (b"loading",), 0, 0, 0),
    b"command": (-1, (b"loading",), 0, 0, 0),
    b"asking": (1, (b"fast",), 0, 0, 0),
    b"readonly": (1, (b"fast",), 0, 0, 0),
    b"readwrite": (1, (b"fast",), 0, 0, 0),
    b"config": (-2, (b"admin",), 0, 0, 0),
}
COMMAND_REPLY = [
    [name, arity, list(flags), first, last, step]
    for name, (arity, flags, first, last, step) in _TABLE.items()
]


def command_keys(command: list[bytes]) -> list[bytes]:
    entry = _TABLE.get(command[0].lower())
    if entry is None or entry[2] == 0:
        return []
    _, _, first, last, step = entry
    last = len(command) + last if last < 0 else last
    return command[first : last + 1 : step]


class Cluster:
    """Slot ownership, migrations and failovers shared by all nodes."""

    def __init__(
        self,
        primaries: list[str],
        replicas: list[str] | None = None,
        port: int = 6379,
        *,
        addresses: dict[str, tuple[str, int]] | None = None,
        resp3: bool = False,
    ) -> None:
        self.port = port
        # Outside a simulation all nodes share one host, so each needs its own port.
        self.addresses = addresses or {}
        self.nodes: dict[str, ClusterServer] = {}
        self.replica_of: dict[str, str] = {}
        self.owner: list[str] = []
        self.migrating: dict[int, tuple[str, str]] = {}
        self.redirects: collections.Counter[str] = collections.Counter()
        share = SLOTS // len(primaries)
        for index, name in enumerate(primaries):
            self.nodes[name] = ClusterServer(self, name, resp3=resp3)
            end = SLOTS if index == len(primaries) - 1 else (index + 1) * share
            self.owner.extend([name] * (end - len(self.owner)))
        for primary, replica in zip(primaries, replicas or [], strict=False):
            node = ClusterServer(self, replica, resp3=resp3)
            node.share_keyspace(self.nodes[primary])
            self.nodes[replica] = node
            self.replica_of[replica] = primary

    def endpoint(self, name: str) -> tuple[str, int]:
        return self.addresses.get(name, (name, self.port))

    def address(self, name: str) -> str:
        host, port = self.endpoint(name)
        return f"{host}:{port}"

    def node_id(self, name: str) -> bytes:
        return name.encode().ljust(40, b"0")

    def node_entry(self, name: str) -> list[object]:
        host, port = self.endpoint(name)
        return [host.encode(), port, self.node_id(name)]

    def slots_reply(self) -> list[object]:
        ranges: list[object] = []
        start = 0
        for slot in range(1, SLOTS + 1):
            if slot == SLOTS or self.owner[slot] != self.owner[start]:
                name = self.owner[start]
                entry: list[object] = [start, slot - 1, self.node_entry(name)]
                for replica, primary in self.replica_of.items():
                    if primary == name:
                        entry.append(self.node_entry(replica))
                ranges.append(entry)
                start = slot
        return ranges

    def route(self, node: ClusterServer, keys: list[bytes], asking: bool) -> str | None:
        """The redirection for a keyed command sent to ``node``, or None to serve it."""
        slots = {key_slot(key) for key in keys}
        if len(slots) > 1:
            return "CROSSSLOT Keys in request don't hash to the same slot"
        slot = slots.pop()
        owner = self.owner[slot]
        migration = self.migrating.get(slot)
        if owner == node.name:
            if migration is None:
                return None
            present = [node.alive(key) for key in keys]
            if all(present):
                return None
            if any(present):
                return "TRYAGAIN Multiple keys request during rehashing of slot"
            return f"ASK {slot} {self.address(migration[1])}"
        if migration is not None and migration[1] == node.name and asking:
            return None
        return f"MOVED {slot} {self.address(owner)}"

    async def migrate(self, slot: int, target: str, pause: tuple[float, float] = (0, 0.01)) -> None:
        """Move one slot to ``target`` the way ``redis-cli --cluster reshard`` does."""
        source = self.owner[slot]
        if source == target or slot in self.migrating:
            return
        self.migrating[slot] = (source, target)
        old, new = self.nodes[source], self.nodes[target]
        try:
            while True:
                keys = sorted(k for k in [*old.data, *old.zsets] if key_slot(k) == slot)
                if not keys:
                    break
                key = keys[0]
                if old.alive(key):
                    new.adopt(old, key)
                old.remove(key)
                old.touch(key)
                await asyncio.sleep(simloop.rng().uniform(*pause) if pause[1] else 0)
            # A failover during the migration may have replaced the target.
            self.owner[slot] = self.replica_of.get(target, target)
        finally:
            del self.migrating[slot]

    async def failover(self, primary: str, delay: float) -> str:
        """Crash ``primary``'s host and promote its replica after ``delay`` seconds."""
        replica = next(r for r, p in self.replica_of.items() if p == primary)
        net = simloop.network()
        net.host(primary).crash()
        await asyncio.sleep(delay)
        while any(source == primary for source, _ in self.migrating.values()):
            await asyncio.sleep(0.01)
        self.owner = [replica if name == primary else name for name in self.owner]
        del self.replica_of[replica]
        self.replica_of[primary] = replica
        return replica


class ClusterServer(RespServer):
    """One cluster node: routing checks in front of a ``RespServer``."""

    def __init__(self, cluster: Cluster, name: str, *, resp3: bool = False) -> None:
        super().__init__(resp3=resp3)
        self.cluster = cluster
        self.name = name

    def share_keyspace(self, other: RespServer) -> None:
        self.data, self.zsets, self.expires = other.data, other.zsets, other.expires
        self.versions, self.streams, self.last_ids = other.versions, other.streams, other.last_ids

    def adopt(self, other: RespServer, key: bytes) -> None:
        if key in other.data:
            self.data[key] = other.data[key]
        if key in other.zsets:
            self.zsets[key] = dict(other.zsets[key])
        if key in other.expires:
            self.expires[key] = other.expires[key]
        self.touch(key)

    def dispatch(self, session: Session, command: list[bytes]) -> object:
        name = command[0].upper()
        asking = getattr(session, "asking", False)
        session.asking = False  # type: ignore[attr-defined]
        if name == b"ASKING":
            session.asking = True  # type: ignore[attr-defined]
            return "OK"
        if name in (b"READONLY", b"READWRITE"):
            return "OK"
        if name == b"CLUSTER":
            sub = command[1].upper()
            if sub == b"SLOTS":
                return self.cluster.slots_reply()
            if sub == b"KEYSLOT":
                return key_slot(command[2])
            if sub == b"MYID":
                return self.cluster.node_id(self.name)
            return Exception(f"unknown subcommand '{sub.decode()}'")
        if name == b"CONFIG" and command[1].upper() == b"GET":
            known = {b"cluster-require-full-coverage": b"yes"}
            return [x for k, v in known.items() if k == command[2].lower() for x in (k, v)]
        if name == b"COMMAND":
            return COMMAND_REPLY if len(command) == 1 else Exception("unsupported")
        keys = command_keys(command)
        if keys:
            redirect = self.cluster.route(self, keys, asking)
            if redirect is not None:
                if session.queued is not None:
                    session.failed = True
                self.cluster.redirects[redirect.split(" ", 1)[0]] += 1
                return Exception(redirect)
        return super().dispatch(session, command)

    def known(self, name: bytes) -> bool:
        return super().known(name) or name.lower() in _TABLE
