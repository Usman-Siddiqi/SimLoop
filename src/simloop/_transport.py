"""asyncio transports and servers over the simulated network.

TCP is modelled as an ordered, reliable byte stream per direction. Each
segment gets a delivery time from the link; a direction never delivers out of
order. Segments that arrive while the link is partitioned are held and
delivered after the partition heals, the way TCP retransmission would. A
closed or crashed endpoint answers data with a reset, so the sender sees
``ConnectionResetError`` like it would against a real dead process.
"""

from __future__ import annotations

import asyncio
import contextvars
import errno
import socket
import sys
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, cast

from .errors import UnsupportedOperation
from .net import Host, Network

if TYPE_CHECKING:
    from ._loop import SimEventLoop

Address = tuple[str, int]

_MAPPED = "::ffff:"
# asyncio's selector transports read at most this much per recv().
_READ_SIZE = 256 * 1024


def _as_ipv6(address: Address) -> tuple[str, int, int, int]:
    """How a dual-stack IPv6 socket sees an IPv4 peer: an IPv4-mapped address."""
    return (_MAPPED + address[0], address[1], 0, 0)


def _unmapped(name: str) -> str:
    return name[len(_MAPPED) :] if name.lower().startswith(_MAPPED) else name


SegmentKind = Literal["synack", "data", "fin", "rst"]
Segment = tuple[SegmentKind, bytes]

# Python 3.12 changed Server.wait_closed to also wait for open connections.
_WAIT_FOR_CONNECTIONS: bool = sys.version_info >= (3, 12)
_SYN_RETRY_DELAYS = (1.0, 2.0, 4.0, 8.0, 16.0, 32.0)


class SimSocket:
    """Stand-in for ``transport.get_extra_info("socket")``.

    It owns no OS descriptor. ``fileno()`` returns a virtual one that the
    sandboxed ``select.select`` / ``select.poll`` understand, so libraries that
    poll an idle socket to see whether the peer closed it (httpcore,
    urllib3-style pools) see the simulated connection's state.
    """

    def __init__(
        self,
        kind: int,
        sockname: Address,
        peername: Address | None = None,
        *,
        network: Network | None = None,
        readable: Callable[[], bool] | None = None,
        family: int = socket.AF_INET,
    ) -> None:
        self.family = family
        self.type = kind
        self.proto = socket.IPPROTO_TCP if kind == socket.SOCK_STREAM else socket.IPPROTO_UDP
        self._sockname = sockname
        self._peername = peername
        self._readable = readable
        self._fd = -1
        if network is not None:
            self._fd = network._register_fd(self)

    def readable(self) -> bool:
        return self._readable() if self._readable is not None else False

    def getsockname(self) -> Address:
        return self._sockname

    def getpeername(self) -> Address:
        if self._peername is None:
            raise OSError(errno.ENOTCONN, "Transport endpoint is not connected")
        return self._peername

    def setsockopt(self, *args: Any) -> None:
        pass

    def getsockopt(self, *args: Any) -> int:
        return 0

    def fileno(self) -> int:
        return self._fd

    def setblocking(self, flag: bool) -> None:
        pass

    def close(self) -> None:
        pass

    def __repr__(self) -> str:
        return f"<SimSocket {self._sockname} -> {self._peername}>"


class StreamTransport(asyncio.Transport):
    """One end of a simulated TCP connection."""

    def __init__(
        self,
        network: Network,
        host: Host,
        local: Address,
        remote: Address,
        context: contextvars.Context,
        server: SimServer | None = None,
    ) -> None:
        super().__init__()
        # Client ends own an ephemeral port; server ends share the listening port.
        self._owns_port = server is None
        self._network = network
        self._loop: SimEventLoop = network.loop
        self._host = host
        self._local = local
        self._remote = remote
        self._context = context
        self._server = server
        self._protocol: asyncio.BaseProtocol | None = None
        self.peer: StreamTransport | None = None
        self._next_arrival = 0.0
        # Deliveries towards this endpoint share an origin so the scheduler
        # never reorders a TCP stream.
        self._origin = network.loop.new_origin()
        self._closing = False
        self._dead = False
        self._eof_sent = False
        self._paused = False
        self._pending: deque[Segment] = deque()
        self._read_scheduled = False
        self._held: deque[Segment] = deque()
        self._replay_scheduled = False
        self._eof_received = False
        self._socket = SimSocket(
            socket.SOCK_STREAM, local, remote, network=network, readable=self._socket_readable
        )
        host._streams[self] = None

    def __repr__(self) -> str:
        state = "dead" if self._dead else "closing" if self._closing else "open"
        return f"<StreamTransport {self._host.name} {self._local} -> {self._remote} {state}>"

    @property
    def _label(self) -> str:
        return f"{self._local[0]}:{self._local[1]} -> {self._remote[0]}:{self._remote[1]}"

    @property
    def _incoming_label(self) -> str:
        # Same direction string as the sender's "send" event, so a timeline
        # can pair them.
        return f"{self._remote[0]}:{self._remote[1]} -> {self._local[0]}:{self._local[1]}"

    def _socket_readable(self) -> bool:
        # The kernel buffer holds what arrived and has not been read yet (for
        # a loop iteration, or while reading is paused), or the end-of-stream
        # / reset that a recv() would report.
        return bool(self._pending) or self._eof_received or self._dead

    # -- asyncio.Transport API ---------------------------------------------------------

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        info = {
            "peername": self._remote,
            "sockname": self._local,
            "socket": self._socket,
            "sslcontext": None,
            "ssl_object": None,
        }
        return info.get(name, default)

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol

    def get_protocol(self) -> asyncio.BaseProtocol:
        assert self._protocol is not None
        return self._protocol

    def is_closing(self) -> bool:
        return self._closing

    def is_reading(self) -> bool:
        return not self._paused and not self._closing

    def pause_reading(self) -> None:
        self._paused = True

    def resume_reading(self) -> None:
        if not self._paused:
            return
        self._paused = False
        if self._pending:
            self._schedule_read()

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"data argument must be a bytes-like object, not {type(data).__name__!r}"
            )
        if self._eof_sent:
            raise RuntimeError("Cannot call write() after write_eof()")
        if not data or self._closing:
            return
        payload = bytes(data)
        self._network._record(self._host.name, "send", f"{self._label} {len(payload)}B")
        self._send("data", payload)

    def writelines(self, list_of_data: Any) -> None:
        self.write(b"".join(list_of_data))

    def write_eof(self) -> None:
        if self._closing or self._eof_sent:
            return
        self._eof_sent = True
        self._send("fin")

    def can_write_eof(self) -> bool:
        return True

    def get_write_buffer_size(self) -> int:
        return 0

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return (0, 0)

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        pass

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._network._record(self._host.name, "close", self._label)
        if not self._eof_sent:
            self._eof_sent = True
            self._send("fin")
        self._loop.call_soon(self._lose, None, context=self._context)

    def abort(self) -> None:
        if self._dead:
            return
        self._closing = True
        self._network._record(self._host.name, "abort", self._label)
        self._send("rst")
        self._loop.call_soon(self._lose, None, context=self._context)

    # -- sending -------------------------------------------------------------------------

    def _send(self, kind: SegmentKind, payload: bytes = b"") -> None:
        peer = self.peer
        if peer is None or not self._host.up:
            return
        network = self._network
        if kind == "data" and network._reset_now(self._host, peer._host):
            network._record(self._host.name, "reset", f"{self._label} (injected)")
            self._send("rst")
            self._loop.call_soon(
                self._lose,
                ConnectionResetError(errno.ECONNRESET, "Connection reset by peer"),
                context=self._context,
            )
            self._closing = True
            return
        arrival = max(
            self._next_arrival, self._loop.time() + network._segment_delay(self._host, peer._host)
        )
        self._next_arrival = arrival
        self._loop._call_at_exact(
            arrival,
            peer._arrive,
            (kind, payload),
            self._host,
            context=peer._context,
            origin=peer._origin,
        )

    # -- receiving -----------------------------------------------------------------------

    def _arrive(self, segment: Segment, sender: Host) -> None:
        if not self._host.up:
            return
        # Once anything is held, later segments queue behind it, including
        # ones still in flight when the partition heals: TCP delivers in order.
        if self._held or self._network._blocked_between(sender, self._host):
            if not self._held and not self._replay_scheduled:
                self._network._hold_until_heal(self._flush_held)
            self._held.append(segment)
            return
        self._accept_segment(segment)

    def _flush_held(self) -> None:
        peer = self.peer
        if peer is None or not self._held:
            self._held.clear()
            return
        # Retransmission after the heal: everything held arrives together, one
        # link delay from now, followed by whatever was still in flight.
        arrival = self._loop.time() + self._network._segment_delay(peer._host, self._host)
        peer._next_arrival = max(peer._next_arrival, arrival)
        self._replay_scheduled = True
        self._loop._call_at_exact(
            arrival, self._replay_held, peer._host, context=self._context, origin=self._origin
        )

    def _replay_held(self, sender: Host) -> None:
        self._replay_scheduled = False
        if not self._host.up:
            self._held.clear()
            return
        if self._network._blocked_between(sender, self._host):
            self._network._hold_until_heal(self._flush_held)
            return
        while self._held:
            self._accept_segment(self._held.popleft())

    def _accept_segment(self, segment: Segment) -> None:
        kind, _payload = segment
        if kind == "rst":
            if self._pending and not self._dead and not self._closing:
                # The kernel keeps data that arrived before a reset readable;
                # recv() reports the reset once it has been read.
                self._pending.append(segment)
                return
            self._reset_by_peer()
            return
        if self._dead or (self._closing and kind == "data"):
            if kind == "data" and self.peer is not None and not self.peer._dead:
                # Data for a socket nobody reads any more: the kernel answers with RST.
                self._send_reset_back()
            return
        # Like the kernel's receive buffer: segments wait there until the loop
        # gets round to reading, and everything that arrived by then comes
        # back from one recv() (one data_received call), so a protocol never
        # sees two reads of one socket with nothing run in between.
        self._pending.append(segment)
        if not self._paused and self._protocol is not None:
            self._schedule_read()

    def _schedule_read(self) -> None:
        if not self._read_scheduled:
            self._read_scheduled = True
            self._loop.call_soon(self._read_ready, context=self._context)

    def _read_ready(self) -> None:
        self._read_scheduled = False
        self._drain_pending()

    def _reset_by_peer(self) -> None:
        self._eof_received = True
        if self._dead:
            return
        self._network._record(self._host.name, "reset", f"{self._label} by peer")
        # Like asyncio's _force_close: stop reading now, but report the loss
        # from a later callback, after anything already scheduled.
        self._closing = True
        self._pending.clear()
        self._loop.call_soon(
            self._lose,
            ConnectionResetError(errno.ECONNRESET, "Connection reset by peer"),
            context=self._context,
        )

    def _send_reset_back(self) -> None:
        peer = self.peer
        if peer is None:
            return
        arrival = self._loop.time() + self._network._segment_delay(self._host, peer._host)
        self._loop._call_at_exact(
            arrival,
            peer._arrive,
            ("rst", b""),
            self._host,
            context=peer._context,
            origin=peer._origin,
        )

    def _drain_pending(self) -> None:
        while self._pending and not self._paused and not self._closing:
            kind, payload = self._pending.popleft()
            if kind == "data":
                # One read takes all the data waiting, up to asyncio's read size.
                chunks = [payload]
                size = len(payload)
                while self._pending and self._pending[0][0] == "data" and size < _READ_SIZE:
                    more = self._pending.popleft()[1]
                    chunks.append(more)
                    size += len(more)
                payload = b"".join(chunks)
                if size > _READ_SIZE:
                    self._pending.appendleft(("data", payload[_READ_SIZE:]))
                    payload = payload[:_READ_SIZE]
                self._deliver(("data", payload))
                if self._pending and not self._paused:
                    # Whatever is left comes from the next read, a loop
                    # iteration later.
                    self._schedule_read()
                    return
            elif kind == "rst":
                self._reset_by_peer()
            else:
                self._deliver((kind, payload))

    def _deliver(self, segment: Segment) -> None:
        kind, payload = segment
        protocol = self._protocol
        if protocol is None:
            return
        if kind == "data":
            self._network._record(
                self._host.name, "recv", f"{self._incoming_label} {len(payload)}B"
            )
            # asyncio only checks for BufferedProtocol and duck-types the
            # rest: protocols need not subclass asyncio.Protocol (asyncssh's
            # port forwarders do not).
            if isinstance(protocol, asyncio.BufferedProtocol):
                self._fill_buffers(protocol, payload)
            else:
                cast(Any, protocol).data_received(payload)
        elif kind == "fin":
            self._eof_received = True
            if self._closing:
                return
            keep_open = cast(Any, protocol).eof_received()
            if not keep_open:
                self.close()

    def _fill_buffers(self, protocol: asyncio.BufferedProtocol, payload: bytes) -> None:
        # Like asyncio's recv_into path: each get_buffer(-1) call takes as much
        # as the buffer holds; the rest waits for the next call, or for
        # resume_reading if the protocol paused the transport meanwhile.
        view = memoryview(payload)
        while view:
            buffer = memoryview(protocol.get_buffer(-1)).cast("B")
            if not len(buffer):
                raise RuntimeError("get_buffer() returned an empty buffer")
            size = min(len(buffer), len(view))
            buffer[:size] = view[:size]
            view = view[size:]
            protocol.buffer_updated(size)
            if view and (self._paused or self._closing):
                self._pending.appendleft(("data", bytes(view)))
                return

    def _lose(self, exc: Exception | None) -> None:
        if self._dead:
            return
        self._release()
        if self._protocol is not None:
            self._protocol.connection_lost(exc)

    def _release(self) -> None:
        self._dead = True
        self._closing = True
        self._host._streams.pop(self, None)
        if self._owns_port:
            self._network._release_port(self._host, self._local[1])
        if self._server is not None:
            self._server._detach(self)

    def _host_crashed(self, *, notify_peer: bool) -> None:
        if self._dead:
            return
        if notify_peer and self.peer is not None and not self.peer._dead:
            # The peer's kernel is still alive: it resets the connection.
            arrival = self._loop.time() + self._network._segment_delay(self._host, self.peer._host)
            self._loop._call_at_exact(
                arrival,
                self.peer._arrive,
                ("rst", b""),
                self._host,
                context=self.peer._context,
                origin=self.peer._origin,
            )
        self._release()


class SimServer(asyncio.AbstractServer):
    """A listening socket on a simulated host."""

    def __init__(
        self,
        network: Network,
        host: Host,
        port: int,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        context: contextvars.Context,
    ) -> None:
        self._network = network
        self._loop = network.loop
        self._host = host
        self._port = port
        self._protocol_factory = protocol_factory
        self._context = context
        self._serving = False
        self._closed = False
        self._connections: dict[StreamTransport, None] = {}
        self._waiters: list[asyncio.Future[None]] = []
        self._serving_forever: asyncio.Future[None] | None = None
        self.sockets = (SimSocket(socket.SOCK_STREAM, (host.ip, port), network=network),)

    def __repr__(self) -> str:
        return f"<SimServer {self._host.name} {self._host.ip}:{self._port}>"

    @property
    def address(self) -> Address:
        return (self._host.ip, self._port)

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def is_serving(self) -> bool:
        return self._serving

    async def start_serving(self) -> None:
        self._serving = not self._closed

    async def serve_forever(self) -> None:
        if self._serving_forever is not None:
            raise RuntimeError(f"server {self!r} is already being awaited on serve_forever()")
        if self._closed:
            raise RuntimeError(f"server {self!r} is closed")
        self._serving = True
        self._serving_forever = self._loop.create_future()
        try:
            await self._serving_forever
        except asyncio.CancelledError:
            try:
                self.close()
                await self.wait_closed()
            finally:
                raise
        finally:
            self._serving_forever = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._serving = False
        key = (self._host.name, self._port)
        if self._network._listeners.get(key) is self:
            del self._network._listeners[key]
            self._network._release_port(self._host, self._port)
        if self._serving_forever is not None and not self._serving_forever.done():
            self._serving_forever.cancel()
        self._wake_waiters()

    def close_clients(self) -> None:
        for transport in list(self._connections):
            transport.close()

    def abort_clients(self) -> None:
        for transport in list(self._connections):
            transport.abort()

    async def wait_closed(self) -> None:
        if self._is_fully_closed():
            return
        if not self._closed and not _WAIT_FOR_CONNECTIONS:
            return
        waiter: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter

    async def __aenter__(self) -> SimServer:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.close()
        await self.wait_closed()

    def _is_fully_closed(self) -> bool:
        if not _WAIT_FOR_CONNECTIONS:
            return self._closed
        return self._closed and not self._connections

    def _wake_waiters(self) -> None:
        if not self._is_fully_closed():
            return
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _attach(self, transport: StreamTransport) -> None:
        self._connections[transport] = None

    def _detach(self, transport: StreamTransport) -> None:
        self._connections.pop(transport, None)
        self._wake_waiters()

    def _accept(self, client: StreamTransport) -> None:
        network = self._network
        context = self._context.copy()
        server_side = StreamTransport(
            network, self._host, client._remote, client._local, context, server=self
        )
        server_side.peer = client
        client.peer = server_side
        self._attach(server_side)
        network._record(
            self._host.name,
            "accept",
            f"{client._remote[0]}:{client._remote[1]} <- {client._local[0]}:{client._local[1]}",
        )

        def connection_made() -> None:
            try:
                protocol = self._protocol_factory()
            except Exception as exc:
                self._loop.call_exception_handler(
                    {"message": "Error in protocol factory of simulated server", "exception": exc}
                )
                server_side.abort()
                return
            server_side._protocol = protocol
            protocol.connection_made(server_side)
            server_side._drain_pending()

        # Runs before any data from the client can arrive (that needs a network delay).
        self._loop.call_soon(connection_made, context=context)

    def _crashed(self) -> None:
        self._connections.clear()
        self.close()


class DatagramEndpoint(asyncio.DatagramTransport):
    """A bound UDP socket on a simulated host."""

    def __init__(
        self,
        network: Network,
        host: Host,
        port: int,
        protocol: asyncio.DatagramProtocol,
        remote: Address | None,
        context: contextvars.Context,
        family: int = socket.AF_INET,
    ) -> None:
        super().__init__()
        self._network = network
        self._loop = network.loop
        self._host = host
        self._port = port
        self._protocol = protocol
        self._remote = remote
        self._context = context
        self._closing = False
        # An AF_INET6 endpoint is dual-stack: the simulated network is IPv4,
        # and the protocol sees its peers as IPv4-mapped addresses.
        self._ipv6 = family == socket.AF_INET6
        self._socket = SimSocket(
            socket.SOCK_DGRAM,
            self._external((host.ip, port)),
            None if remote is None else self._external(remote),
            network=network,
            family=family,
        )

    def _external(self, address: Address) -> Any:
        return _as_ipv6(address) if self._ipv6 else address

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        info = {
            "sockname": self._external((self._host.ip, self._port)),
            "peername": None if self._remote is None else self._external(self._remote),
            "socket": self._socket,
        }
        return info.get(name, default)

    def get_protocol(self) -> asyncio.BaseProtocol:
        return self._protocol

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol  # type: ignore[assignment]

    def is_closing(self) -> bool:
        return self._closing

    def get_write_buffer_size(self) -> int:
        return 0

    def sendto(self, data: bytes | bytearray | memoryview, addr: Any = None) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"data argument must be a bytes-like object, not {type(data).__name__!r}"
            )
        if self._remote is not None:
            if addr is not None and addr not in (self._remote, self._external(self._remote)):
                raise ValueError(f"Invalid address: must be None or {self._remote}")
            addr = self._remote
        if addr is None:
            raise ValueError("no destination address and the endpoint is not connected")
        if self._closing or not self._host.up:
            return
        network = self._network
        try:
            target = network.resolve(_unmapped(str(addr[0])), origin=self._host)
        except socket.gaierror:
            return
        port = int(addr[1])
        payload = bytes(data)
        source = (self._host.ip, self._port)
        if network._blocked_between(self._host, target) or not target.up:
            network._record(
                self._host.name, "drop", f"{source} -> {addr} {len(payload)}B (unreachable)"
            )
            return
        delays = network._datagram_delays(self._host, target)
        if not delays:
            network._record(self._host.name, "drop", f"{source} -> {addr} {len(payload)}B")
            return
        network._record(
            self._host.name,
            "send",
            f"{source} -> {addr} {len(payload)}B" + (" (duplicated)" if len(delays) > 1 else ""),
        )
        for delay in delays:
            self._loop._call_at_exact(
                self._loop.time() + delay, _deliver_datagram, network, target, port, payload, source
            )

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        key = (self._host.name, self._port)
        if self._network._datagrams.get(key) is self:
            del self._network._datagrams[key]
            self._network._release_port(self._host, self._port)
        self._loop.call_soon(self._protocol.connection_lost, None, context=self._context)

    def abort(self) -> None:
        self.close()

    def _receive(self, payload: bytes, source: Address) -> None:
        if self._closing:
            return
        if self._remote is not None and source != self._remote:
            return
        self._network._record(
            self._host.name, "recv", f"{source} -> {(self._host.ip, self._port)} {len(payload)}B"
        )
        self._loop.call_soon(
            self._protocol.datagram_received,
            payload,
            self._external(source),
            context=self._context,
        )

    def _crashed(self) -> None:
        self._closing = True


def _deliver_datagram(
    network: Network, target: Host, port: int, payload: bytes, source: Address
) -> None:
    if not target.up:
        return
    endpoint = network._datagrams.get((target.name, port))
    if endpoint is not None:
        endpoint._receive(payload, source)


# -- loop entry points --------------------------------------------------------------------


async def create_connection(
    network: Network,
    protocol_factory: Callable[[], asyncio.BaseProtocol],
    host: str | None,
    port: int | None,
    *,
    ssl: Any = None,
    sock: Any = None,
    local_addr: tuple[str, int] | None = None,
    **_: Any,
) -> tuple[StreamTransport, asyncio.BaseProtocol]:
    _reject_unsupported(ssl=ssl)
    if sock is not None:
        if host is not None or port is not None:
            raise ValueError("host/port and sock can not be specified at the same time")
        client = _virtual(sock)._take_transport()
    else:
        if host is None or port is None:
            raise ValueError("host and port was not specified and no sock specified")
        client = await connect_stream(network, host, int(port), local_addr=local_addr)
    protocol = protocol_factory()
    client._protocol = protocol
    protocol.connection_made(client)
    client._drain_pending()
    return client, protocol


async def connect_stream(
    network: Network,
    host: str,
    port: int,
    *,
    local_addr: tuple[str, int] | None = None,
    local_port: int | None = None,
) -> StreamTransport:
    """Open a TCP connection and return its client end, not yet attached to a protocol.

    ``local_port`` is an already-claimed port (from a bound virtual socket).
    """
    source = network.current_host()
    if not source.up:
        raise OSError(errno.ENETDOWN, f"host {source.name!r} is down")
    target = network.resolve(host, origin=source)
    if local_port is None:
        if local_addr is not None:
            network._bind_address(source, local_addr[0])
            local_port = network._claim_port(source, int(local_addr[1]))
        else:
            local_port = network._claim_port(source, 0)
    local = (source.ip, local_port)
    remote = (target.ip, port)
    client = StreamTransport(network, source, local, remote, contextvars.copy_context())
    network._record(source.name, "connect", f"{local[0]}:{local[1]} -> {remote[0]}:{remote[1]}")
    try:
        await _handshake(network, client, target, port)
    except BaseException:
        if client.peer is not None and not client._dead:
            client._send("rst")
        client._release()
        raise
    return client


async def _handshake(network: Network, client: StreamTransport, target: Host, port: int) -> None:
    loop = network.loop
    source = client._host
    deadline = loop.time() + network.connect_timeout
    retries = iter(_SYN_RETRY_DELAYS)
    while True:
        answered = (
            not network._blocked_between(source, target)
            and not network._blocked_between(target, source)
            and (target.up or not target.silent)
        )
        if answered:
            await asyncio.sleep(network._segment_delay(source, target))
            listener = network._listeners.get((target.name, port))
            if listener is not None and listener.is_serving() and target.up and source.up:
                listener._accept(client)
                await asyncio.sleep(network._segment_delay(target, source))
                if client._dead:
                    raise ConnectionResetError(errno.ECONNRESET, "Connection reset by peer")
                return
            await asyncio.sleep(network._segment_delay(target, source))
            network._record(source.name, "refused", f"{target.ip}:{port}")
            raise ConnectionRefusedError(
                errno.ECONNREFUSED, f"Connect call failed ({target.ip!r}, {port})"
            )
        delay = next(retries, None)
        if delay is None or loop.time() + delay > deadline:
            await asyncio.sleep(max(0.0, deadline - loop.time()))
            network._record(source.name, "timeout", f"connect to {target.ip}:{port}")
            raise TimeoutError(
                errno.ETIMEDOUT, f"Connect call failed ({target.ip!r}, {port}): timed out"
            )
        await asyncio.sleep(delay)


async def create_server(
    network: Network,
    protocol_factory: Callable[[], asyncio.BaseProtocol],
    host: str | list[str] | None = None,
    port: int | None = None,
    *,
    ssl: Any = None,
    sock: Any = None,
    start_serving: bool = True,
    **_: Any,
) -> SimServer:
    _reject_unsupported(ssl=ssl)
    owner = network.current_host()
    if not owner.up:
        raise OSError(errno.ENETDOWN, f"host {owner.name!r} is down")
    if sock is not None:
        bound = _virtual(sock)._take_listening_port()
    else:
        for address in host if isinstance(host, list) else [host]:
            network._bind_address(owner, address)
        bound = network._claim_port(owner, int(port or 0))
    server = SimServer(network, owner, bound, protocol_factory, contextvars.copy_context())
    network._listeners[(owner.name, bound)] = server
    network._record(owner.name, "listen", f"{owner.ip}:{bound}")
    if start_serving:
        await server.start_serving()
    return server


async def create_datagram_endpoint(
    network: Network,
    protocol_factory: Callable[[], asyncio.DatagramProtocol],
    local_addr: tuple[str, int] | None = None,
    remote_addr: tuple[str, int] | None = None,
    *,
    sock: Any = None,
    family: int = 0,
    **_: Any,
) -> tuple[DatagramEndpoint, asyncio.DatagramProtocol]:
    owner = network.current_host()
    if not owner.up:
        raise OSError(errno.ENETDOWN, f"host {owner.name!r} is down")
    if sock is not None:
        virtual = _virtual(sock)
        family = virtual.family
        bound = virtual._take_bound_port()
        remote_addr = remote_addr or virtual._peer
    else:
        port = 0
        if local_addr is not None:
            if not family and ":" in str(local_addr[0]):
                family = socket.AF_INET6
            network._bind_address(owner, local_addr[0])
            port = int(local_addr[1])
        bound = network._claim_port(owner, port)
    remote: Address | None = None
    if remote_addr is not None:
        name = _unmapped(str(remote_addr[0]))
        target = network.resolve(name, origin=owner)
        remote = (
            target.ip if name not in ("localhost", "127.0.0.1", "::1") else owner.ip,
            int(remote_addr[1]),
        )
    protocol = protocol_factory()
    context = contextvars.copy_context()
    endpoint = DatagramEndpoint(
        network, owner, bound, protocol, remote, context, family or socket.AF_INET
    )
    network._datagrams[(owner.name, bound)] = endpoint
    loop = network.loop
    # As in asyncio: connection_made runs before the endpoint is returned.
    waiter = loop.create_future()
    loop.call_soon(protocol.connection_made, endpoint, context=context)
    loop.call_soon(_set_result_unless_cancelled, waiter, context=context)
    try:
        await waiter
    except BaseException:
        endpoint.close()
        raise
    return endpoint, protocol


def _set_result_unless_cancelled(waiter: asyncio.Future[None]) -> None:
    if not waiter.cancelled():
        waiter.set_result(None)


def _virtual(sock: Any) -> VirtualSocket:
    if not isinstance(sock, VirtualSocket):
        raise UnsupportedOperation(
            "only sockets created inside the simulation can be passed as sock="
        )
    return sock


async def sock_connect(network: Network, sock: Any, address: Any) -> None:
    virtual = _virtual(sock)
    if virtual.type == socket.SOCK_DGRAM:
        virtual.connect(address)
        return
    if virtual._transport is not None:
        raise OSError(errno.EISCONN, "Transport endpoint is already connected")
    host, port = str(address[0]), int(address[1])
    transport = await connect_stream(network, host, port, local_port=virtual._detach_port())
    virtual._attach(transport)


class VirtualSocket:
    """What ``socket.socket()`` returns inside a simulation.

    Supports the non-blocking subset that asyncio code uses before handing a
    socket to the loop: options, ``bind``, ``listen``, ``loop.sock_connect`` and
    passing it as ``sock=`` to ``create_connection``/``create_server``/
    ``create_datagram_endpoint``. Blocking I/O is not simulated.
    """

    network: Network  # set on the per-simulation subclass

    def __init__(
        self,
        family: int = -1,
        type: int = -1,
        proto: int = -1,
        fileno: int | None = None,
    ) -> None:
        if fileno is not None:
            raise UnsupportedOperation("wrapping a real file descriptor is not simulated")
        self.family = socket.AF_INET if family == -1 else family
        self.type = socket.SOCK_STREAM if type == -1 else type
        self.proto = 0 if proto == -1 else proto
        self._host = self.network.current_host()
        self._port: int | None = None
        self._peer: Address | None = None
        self._transport: StreamTransport | None = None
        self._listening = False
        self._closed = False
        self._timeout: float | None = None
        self._fd = self.network._register_fd(self)

    def readable(self) -> bool:
        transport = self._transport
        return transport is not None and transport._socket_readable()

    def __repr__(self) -> str:
        return f"<VirtualSocket {self._host.name} port={self._port} peer={self._peer}>"

    def __enter__(self) -> VirtualSocket:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def fileno(self) -> int:
        return self._fd

    def detach(self) -> int:
        return -1

    def setblocking(self, flag: bool) -> None:
        self._timeout = None if flag else 0.0

    def settimeout(self, value: float | None) -> None:
        self._timeout = value

    def gettimeout(self) -> float | None:
        return self._timeout

    def setsockopt(self, *args: Any) -> None:
        pass

    def getsockopt(self, *args: Any) -> int:
        return 0

    def bind(self, address: Any) -> None:
        if self._port is not None:
            raise OSError(errno.EINVAL, "Invalid argument")
        self.network._bind_address(self._host, _unmapped(address[0]) or None)
        self._port = self.network._claim_port(self._host, int(address[1]))

    def listen(self, backlog: int = 0) -> None:
        if self._port is None:
            self.bind(("", 0))
        self._listening = True

    def connect(self, address: Any) -> None:
        if self.type != socket.SOCK_DGRAM:
            raise UnsupportedOperation(
                "blocking connect() is not simulated; use await loop.sock_connect()"
            )
        target = self.network.resolve(_unmapped(str(address[0])), origin=self._host)
        self._peer = (target.ip, int(address[1]))

    def getsockname(self) -> Any:
        if self._transport is not None:
            return self._transport._local
        address = (self._host.ip, self._port or 0)
        return _as_ipv6(address) if self.family == socket.AF_INET6 else address

    def getpeername(self) -> Any:
        if self._peer is None:
            raise OSError(errno.ENOTCONN, "Transport endpoint is not connected")
        return _as_ipv6(self._peer) if self.family == socket.AF_INET6 else self._peer

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._transport is not None:
            self._transport.abort()
            self._transport = None
        elif self._port is not None:
            self.network._release_port(self._host, self._port)
        self._port = None

    def _blocking(self, *args: Any, **kwargs: Any) -> Any:
        raise UnsupportedOperation("blocking socket I/O is not simulated; use asyncio APIs")

    send = sendall = recv = recv_into = sendto = recvfrom = accept = _blocking

    def _detach_port(self) -> int | None:
        port, self._port = self._port, None
        return port

    def _attach(self, transport: StreamTransport) -> None:
        self._transport = transport
        self._peer = transport._remote

    def _take_transport(self) -> StreamTransport:
        if self._transport is None:
            raise OSError(errno.ENOTCONN, "socket is not connected")
        transport, self._transport = self._transport, None
        self._closed = True
        return transport

    def _take_listening_port(self) -> int:
        if self._port is None:
            self.bind(("", 0))
        port = self._detach_port()
        assert port is not None
        self._closed = True
        return port

    def _take_bound_port(self) -> int:
        return self._take_listening_port()


def virtual_socket_class(network: Network) -> type[VirtualSocket]:
    return type("socket", (VirtualSocket,), {"network": network})


def _reject_unsupported(**options: Any) -> None:
    for name, value in options.items():
        if value:
            raise UnsupportedOperation(f"{name}= is not supported by the simulated network")
