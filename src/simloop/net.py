"""Simulated network: hosts, links, partitions and DNS.

Every simulation has a :class:`Network`. Code reaches it through the normal
asyncio APIs (``asyncio.open_connection``, ``asyncio.start_server``,
``loop.getaddrinfo``, ``loop.create_datagram_endpoint``); nothing touches a
real socket. Tasks belong to the host they were spawned on, which decides the
source address of their connections and what dies when the host crashes.

Delivery times, losses and duplicates are drawn from the run's
:class:`~simloop.ChoiceSource`, so a seed fixes every network decision and a
draw of 0 always means "minimum latency, nothing lost".
"""

from __future__ import annotations

import asyncio
import contextvars
import errno
import ipaddress
import itertools
import socket
import weakref
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._loop import SimEventLoop
    from ._transport import DatagramEndpoint, SimServer, StreamTransport

VIRTUAL_FD_BASE = 1 << 20
"""Simulated sockets get descriptors from here up, far above any real one."""

_current_host: contextvars.ContextVar[Host | None] = contextvars.ContextVar(
    "simloop_current_host", default=None
)

_LOOPBACK_NAMES = frozenset({"localhost", "127.0.0.1", "::1", "ip6-localhost"})
_ANY_ADDRESSES = frozenset({None, "", "0.0.0.0", "::"})
_SERVICES = {"http": 80, "https": 443, "redis": 6379, "postgresql": 5432, "amqp": 5672}
_FIRST_EPHEMERAL_PORT = 49152
_MAX_RETRANSMITS = 6

HostEntry = Callable[[], Coroutine[Any, Any, Any]]


@dataclass(frozen=True)
class LinkConfig:
    """Behaviour of traffic from one host to another.

    ``latency`` is a fixed delay or a ``(min, max)`` range drawn uniformly per
    segment or datagram. ``drop`` loses a datagram; for TCP a lost segment is
    retransmitted after ``rto`` (doubling each time), so it shows up as delay,
    in order. ``duplicate`` delivers a datagram twice. ``reset`` is the chance
    that a TCP segment kills its connection with a reset on both ends.
    """

    latency: float | tuple[float, float] = (0.001, 0.010)
    drop: float = 0.0
    duplicate: float = 0.0
    reset: float = 0.0
    rto: float = 0.2

    def __post_init__(self) -> None:
        low, high = self.latency if isinstance(self.latency, tuple) else (self.latency,) * 2
        if low < 0 or high < low:
            raise ValueError(f"invalid latency {self.latency!r}")
        for name in ("drop", "duplicate", "reset"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be a probability, got {value!r}")

    def replace(self, **changes: Any) -> LinkConfig:
        return replace(self, **changes)


class Host:
    """A simulated machine with its own address, tasks, ports and disk.

    ``disk`` is a plain dict that survives :meth:`crash` and :meth:`restart`;
    everything else the host's tasks held in memory is lost.
    """

    def __init__(self, network: Network, name: str, ip: str, entry: HostEntry | None) -> None:
        self.network = network
        self.name = name
        self.ip = ip
        self.entry = entry
        self.up = True
        self.silent = False
        self.disk: dict[Any, Any] = {}
        self.crashes = 0
        self.paused = False
        self.clock_offset = 0.0
        """Seconds added to this host's wall clock (``time.time()``)."""
        self.clock_drift = 0.0
        """Relative rate error of this host's wall clock, e.g. ``1e-4`` is 100 ppm fast."""
        self._held_handles: list[asyncio.Handle] = []
        self._context = contextvars.Context()
        self._context.run(_current_host.set, self)
        self._tasks: dict[asyncio.Task[Any], None] = {}
        self._prune_at = 64
        self._streams: dict[StreamTransport, None] = {}
        self._next_port = _FIRST_EPHEMERAL_PORT

    def __repr__(self) -> str:
        state = "up" if self.up else "down"
        return f"<Host {self.name} {self.ip} {state}>"

    # -- tasks -------------------------------------------------------------------

    def spawn(
        self, coro: Coroutine[Any, Any, Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        """Start ``coro`` as a task running on this host."""
        if not self.up:
            coro.close()
            raise RuntimeError(f"host {self.name!r} is down")
        return self.network.loop.create_task(coro, name=name, context=self.new_context())

    async def run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Run ``coro`` on this host and wait for its result."""
        return await self.spawn(coro)

    def new_context(self) -> contextvars.Context:
        return self._context.copy()

    def _hold_if_paused(self, handle: asyncio.Handle) -> bool:
        context: contextvars.Context | None = getattr(handle, "_context", None)
        host = context.get(_current_host) if context is not None else None
        if host is None or not host.paused:
            return False
        host._held_handles.append(handle)
        return True

    def _adopt(self, task: asyncio.Task[Any]) -> None:
        self._tasks[task] = None
        if len(self._tasks) >= self._prune_at:
            self._tasks = {t: None for t in self._tasks if not t.done()}
            self._prune_at = max(64, 2 * len(self._tasks))

    @property
    def tasks(self) -> list[asyncio.Task[Any]]:
        """This host's unfinished tasks, in creation order."""
        return [task for task in self._tasks if not task.done()]

    # -- faults ------------------------------------------------------------------

    def crash(self, *, power_loss: bool = False) -> None:
        """Kill the host: cancel its tasks and drop its sockets and listeners.

        By default this models a process crash: the peer's operating system
        still answers, so peers see their connections reset. With
        ``power_loss=True`` the machine goes silent instead and peers only
        notice through timeouts.
        """
        if self is self.network.default_host:
            raise RuntimeError("the default host runs the test driver and cannot crash")
        if not self.up:
            return
        if self.paused:
            self.resume()
        self.up = False
        self.silent = power_loss
        self.crashes += 1
        self.network._record(self.name, "crash", "power loss" if power_loss else "process crash")
        for stream in list(self._streams):
            stream._host_crashed(notify_peer=not power_loss)
        self._streams.clear()
        self.network._unbind_host(self)
        for task in self.tasks:
            task.cancel(f"host {self.name} crashed")
        self._tasks.clear()
        # Tasks spawned from now on (e.g. by cleanup code) must not inherit the
        # dead incarnation's context.
        self._context = contextvars.Context()
        self._context.run(_current_host.set, self)

    def pause(self, duration: float | None = None) -> None:
        """Freeze the host (a GC pause, a stalled VM, SIGSTOP) without killing it.

        Nothing on the host runs while paused: its timers fire late and data sent
        to it waits. Peers carry on. With ``duration`` it resumes on its own.
        """
        if self.paused or not self.up:
            return
        self.paused = True
        self.network._paused_hosts += 1
        loop = self.network.loop
        loop.hold_filter = self.network._hold_if_paused
        self.network._record(self.name, "pause", "" if duration is None else f"for {duration!r}s")
        if duration is not None:
            loop._call_at_exact(loop.time() + duration, self.resume)

    def resume(self) -> None:
        if not self.paused:
            return
        self.paused = False
        network = self.network
        network._paused_hosts -= 1
        if network._paused_hosts == 0:
            network.loop.hold_filter = None
        network._record(self.name, "resume", f"{len(self._held_handles)} held callbacks")
        origin = network.loop.new_origin()
        held, self._held_handles = self._held_handles, []
        for handle in held:
            network.loop.requeue(handle, origin)

    def wall_time(self, simulated: float, epoch: float) -> float:
        return epoch + simulated * (1.0 + self.clock_drift) + self.clock_offset

    def restart(self) -> asyncio.Task[Any] | None:
        """Bring a crashed host back and run its entry point again, if it has one."""
        if self.up:
            raise RuntimeError(f"host {self.name!r} is already up")
        self.up = True
        self.silent = False
        self.network._record(self.name, "restart", f"after {self.crashes} crash(es)")
        if self.entry is None:
            return None
        return self.spawn(self.entry(), name=f"{self.name}-main")

    # -- ports -------------------------------------------------------------------

    def _ephemeral_port(self) -> int:
        while True:
            port = self._next_port
            self._next_port = port + 1 if port < 65535 else _FIRST_EPHEMERAL_PORT
            if not self.network._port_in_use(self, port):
                return port


class Network:
    """All hosts of a simulation and the links between them."""

    def __init__(
        self, loop: SimEventLoop, *, link: LinkConfig | None = None, connect_timeout: float = 30.0
    ) -> None:
        self.loop = loop
        self.default_link = link or LinkConfig()
        self.connect_timeout = connect_timeout
        self._links: dict[tuple[str, str], LinkConfig] = {}
        self._hosts: dict[str, Host] = {}
        self._by_ip: dict[str, Host] = {}
        self._blocked: set[tuple[str, str]] = set()
        self._held: dict[Callable[[], None], None] = {}
        self._listeners: dict[tuple[str, int], SimServer] = {}
        self._datagrams: dict[tuple[str, int], DatagramEndpoint] = {}
        self._used_ports: dict[tuple[str, int], int] = {}
        self._paused_hosts = 0
        self._fd_counter = itertools.count(VIRTUAL_FD_BASE)
        self._sockets_by_fd: weakref.WeakValueDictionary[int, Any] = weakref.WeakValueDictionary()
        self.default_host = self.add_host("main")

    # -- hosts ---------------------------------------------------------------------

    def add_host(self, name: str, *, entry: HostEntry | None = None, ip: str | None = None) -> Host:
        """Add a host. If ``entry`` is given it is started now and on every restart."""
        if name in self._hosts or name in _LOOPBACK_NAMES:
            raise ValueError(f"host name {name!r} is already taken")
        address = ip or str(ipaddress.IPv4Address(0x0A000001 + len(self._hosts)))
        if address in self._by_ip:
            raise ValueError(f"address {address} is already taken")
        host = Host(self, name, address, entry)
        self._hosts[name] = host
        self._by_ip[address] = host
        if entry is not None:
            host.spawn(entry(), name=f"{name}-main")
        return host

    def host(self, name: str) -> Host:
        try:
            return self._hosts[name]
        except KeyError:
            raise KeyError(f"no host named {name!r}") from None

    @property
    def hosts(self) -> list[Host]:
        return list(self._hosts.values())

    def current_host(self) -> Host:
        host = _current_host.get()
        return host if host is not None and host.network is self else self.default_host

    def _hold_if_paused(self, handle: asyncio.Handle) -> bool:
        context: contextvars.Context | None = getattr(handle, "_context", None)
        host = context.get(_current_host) if context is not None else None
        if host is None or not host.paused:
            return False
        host._held_handles.append(handle)
        return True

    def _adopt(self, task: asyncio.Task[Any], context: contextvars.Context | None) -> str | None:
        """Attach ``task`` to the host it was created on; returns the host name."""
        host = context.get(_current_host) if context is not None else _current_host.get()
        if host is not None and host.network is self:
            host._adopt(task)
            return host.name
        return None

    # -- links and partitions ---------------------------------------------------------

    def set_link(
        self,
        a: str | Host,
        b: str | Host,
        config: LinkConfig | None = None,
        *,
        both_ways: bool = True,
        **changes: Any,
    ) -> None:
        """Configure traffic from ``a`` to ``b`` (and back, unless ``both_ways=False``)."""
        src, dst = _name(a), _name(b)
        base = config or self.link(src, dst)
        updated = base.replace(**changes) if changes else base
        self._links[(src, dst)] = updated
        if both_ways:
            self._links[(dst, src)] = updated

    def link(self, a: str | Host, b: str | Host) -> LinkConfig:
        return self._links.get((_name(a), _name(b)), self.default_link)

    def partition(
        self, a: str | Host | Iterable[str | Host], b: str | Host | Iterable[str | Host]
    ) -> None:
        """Cut every host in ``a`` off from every host in ``b``, in both directions."""
        for x in _names(a):
            for y in _names(b):
                if x != y:
                    self._blocked.add((x, y))
                    self._blocked.add((y, x))
        self._record("network", "partition", f"{sorted(_names(a))} | {sorted(_names(b))}")

    def block(self, src: str | Host, dst: str | Host) -> None:
        """Drop traffic from ``src`` to ``dst`` only (an asymmetric partition)."""
        self._blocked.add((_name(src), _name(dst)))
        self._record("network", "partition", f"{_name(src)} -> {_name(dst)}")

    def isolate(self, host: str | Host) -> None:
        """Cut ``host`` off from every other host."""
        name = _name(host)
        self.partition(name, [h for h in self._hosts if h != name])

    def heal(self, a: str | Host | None = None, b: str | Host | None = None) -> None:
        """Remove all partitions, those involving ``a``, or those between ``a`` and ``b``."""
        if a is None:
            self._blocked.clear()
        else:
            x = _name(a)
            ys = None if b is None else _name(b)
            self._blocked = {
                (s, d)
                for s, d in self._blocked
                if not ((s == x or d == x) and (ys is None or s == ys or d == ys))
            }
        self._record("network", "heal", "all" if a is None else f"{_name(a)}")
        held, self._held = self._held, {}
        for flush in held:
            flush()

    def reachable(self, src: Host, dst: Host) -> bool:
        return src.up and dst.up and (src.name, dst.name) not in self._blocked

    def _blocked_between(self, src: Host, dst: Host) -> bool:
        return (src.name, dst.name) in self._blocked

    def _hold_until_heal(self, flush: Callable[[], None]) -> None:
        self._held[flush] = None

    # -- delays --------------------------------------------------------------------------

    def _latency(self, config: LinkConfig) -> float:
        if isinstance(config.latency, tuple):
            return self.loop.choices.uniform(*config.latency)
        return config.latency

    def _segment_delay(self, src: Host, dst: Host) -> float:
        config = self.link(src, dst)
        delay = self._latency(config)
        retransmits = 0
        while (
            config.drop and retransmits < _MAX_RETRANSMITS and self.loop.choices.chance(config.drop)
        ):
            delay += config.rto * (2**retransmits)
            retransmits += 1
        return delay

    def _datagram_delays(self, src: Host, dst: Host) -> list[float]:
        config = self.link(src, dst)
        if config.drop and self.loop.choices.chance(config.drop):
            return []
        delays = [self._latency(config)]
        if config.duplicate and self.loop.choices.chance(config.duplicate):
            delays.append(self._latency(config))
        return delays

    def _reset_now(self, src: Host, dst: Host) -> bool:
        config = self.link(src, dst)
        return bool(config.reset) and self.loop.choices.chance(config.reset)

    # -- addressing ----------------------------------------------------------------------

    def resolve(self, name: str, *, origin: Host | None = None) -> Host:
        """Map a host name or address to a host, as DNS would."""
        if name in _LOOPBACK_NAMES:
            return origin or self.current_host()
        host = self._hosts.get(name) or self._by_ip.get(name)
        if host is None:
            raise socket.gaierror(socket.EAI_NONAME, f"Name or service not known: {name!r}")
        return host

    def _bind_address(self, host: Host, address: str | None) -> None:
        if address in _ANY_ADDRESSES or address in _LOOPBACK_NAMES:
            return
        if address not in (host.ip, host.name):
            raise OSError(
                errno.EADDRNOTAVAIL,
                f"cannot assign requested address {address!r} on host {host.name!r}",
            )

    def _port_in_use(self, host: Host, port: int) -> bool:
        return (host.name, port) in self._used_ports

    def _claim_port(self, host: Host, port: int) -> int:
        if port == 0:
            port = host._ephemeral_port()
        key = (host.name, port)
        if key in self._used_ports:
            raise OSError(errno.EADDRINUSE, f"address already in use: {host.ip}:{port}")
        self._used_ports[key] = 1
        return port

    def _release_port(self, host: Host, port: int) -> None:
        self._used_ports.pop((host.name, port), None)

    def _unbind_host(self, host: Host) -> None:
        for key in [k for k in self._listeners if k[0] == host.name]:
            self._listeners.pop(key)._crashed()
        for key in [k for k in self._datagrams if k[0] == host.name]:
            self._datagrams.pop(key)._crashed()
        for key in [k for k in self._used_ports if k[0] == host.name]:
            del self._used_ports[key]

    async def getaddrinfo(
        self,
        host: bytes | str | None,
        port: bytes | str | int | None,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[tuple[Any, ...]]:
        name = host.decode() if isinstance(host, bytes) else host
        target = self.current_host() if name is None else self.resolve(name)
        if family not in (0, socket.AF_INET, socket.AF_UNSPEC):
            raise socket.gaierror(socket.EAI_FAMILY, "only IPv4 is simulated")
        port_number = _port_number(port)
        kinds = [type] if type else [socket.SOCK_STREAM]
        address = "127.0.0.1" if name in _LOOPBACK_NAMES else target.ip
        return [
            (
                socket.AF_INET,
                kind,
                proto or (socket.IPPROTO_UDP if kind == socket.SOCK_DGRAM else socket.IPPROTO_TCP),
                "",
                (address, port_number),
            )
            for kind in kinds
        ]

    async def getnameinfo(self, sockaddr: tuple[Any, ...], flags: int = 0) -> tuple[str, str]:
        host = self.resolve(str(sockaddr[0]))
        return host.name, str(sockaddr[1])

    def _register_fd(self, sock: Any) -> int:
        fd = next(self._fd_counter)
        self._sockets_by_fd[fd] = sock
        return fd

    def socket_for_fd(self, fd: int) -> Any:
        """The simulated socket behind a virtual descriptor, or ``None``."""
        return self._sockets_by_fd.get(fd)

    def _shutdown(self) -> None:
        """Power everything off at the end of a run without running callbacks."""
        for host in self._hosts.values():
            for stream in list(host._streams):
                stream._release()
            host._streams.clear()
        for server in list(self._listeners.values()):
            server._crashed()
        for endpoint in list(self._datagrams.values()):
            endpoint._crashed()
        self._listeners.clear()
        self._datagrams.clear()
        self._held.clear()

    # -- tracing ------------------------------------------------------------------------

    def _record(self, actor: str, kind: str, detail: str) -> None:
        self.loop._record(kind, actor, detail)


def _name(host: str | Host) -> str:
    return host.name if isinstance(host, Host) else host


def _names(hosts: str | Host | Iterable[str | Host]) -> list[str]:
    if isinstance(hosts, (str, Host)):
        return [_name(hosts)]
    return [_name(h) for h in hosts]


def _port_number(port: bytes | str | int | None) -> int:
    if port is None:
        return 0
    if isinstance(port, int):
        return port
    text = port.decode() if isinstance(port, bytes) else port
    if text.isdigit():
        return int(text)
    if text in _SERVICES:
        return _SERVICES[text]
    raise socket.gaierror(socket.EAI_SERVICE, f"Servname not supported: {text!r}")
