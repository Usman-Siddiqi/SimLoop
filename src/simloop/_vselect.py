"""``select.select`` and ``select.poll`` for simulated sockets.

Some libraries check an idle pooled connection with a zero-timeout poll to
see whether the server closed it. Inside a simulation the descriptors are
virtual, so the sandbox swaps these functions for versions that read the
simulated connection state. Waiting (a non-zero timeout with nothing ready)
would block the whole simulated loop, and real descriptors would bring in the
real OS, so both raise.
"""

from __future__ import annotations

import select
from collections.abc import Iterable
from typing import Any

from .errors import UnsupportedOperation
from .net import Network


def _fd(obj: Any) -> int:
    return obj if isinstance(obj, int) else int(obj.fileno())


class VirtualSelect:
    def __init__(self, network: Network) -> None:
        self.network = network

    def _socket(self, fd: int) -> Any:
        sock = self.network.socket_for_fd(fd)
        if sock is None:
            raise UnsupportedOperation(
                f"select/poll on descriptor {fd}, which is not a simulated socket"
            )
        return sock

    def _check_wait(self, timeout: float | None, ready: bool) -> None:
        if not ready and (timeout is None or timeout > 0):
            raise UnsupportedOperation(
                "a blocking select/poll would stall the simulated event loop; "
                "only zero-timeout readiness checks are simulated"
            )

    def select(
        self,
        rlist: Iterable[Any],
        wlist: Iterable[Any],
        xlist: Iterable[Any],
        timeout: float | None = None,
    ) -> tuple[list[Any], list[Any], list[Any]]:
        readable = [obj for obj in rlist if self._socket(_fd(obj)).readable()]
        writable = [obj for obj in wlist if self._socket(_fd(obj)) is not None]
        for obj in xlist:
            self._socket(_fd(obj))
        self._check_wait(timeout, bool(readable or writable))
        return readable, writable, []

    def poll(self) -> VirtualPoll:
        return VirtualPoll(self)


class VirtualPoll:
    def __init__(self, owner: VirtualSelect) -> None:
        self._owner = owner
        self._fds: dict[int, int] = {}

    def register(
        self, fd: Any, eventmask: int = select.POLLIN | select.POLLPRI | select.POLLOUT
    ) -> None:
        number = _fd(fd)
        self._owner._socket(number)
        self._fds[number] = eventmask

    def modify(self, fd: Any, eventmask: int) -> None:
        number = _fd(fd)
        if number not in self._fds:
            raise FileNotFoundError(2, "No such file or directory")
        self._fds[number] = eventmask

    def unregister(self, fd: Any) -> None:
        del self._fds[_fd(fd)]

    def poll(self, timeout: float | None = None) -> list[tuple[int, int]]:
        events = []
        for fd, mask in self._fds.items():
            sock = self._owner._socket(fd)
            ready = 0
            if mask & select.POLLIN and sock.readable():
                ready |= select.POLLIN
            if mask & select.POLLOUT:
                ready |= select.POLLOUT
            if ready:
                events.append((fd, ready))
        # poll takes milliseconds; None or a negative value waits forever.
        wait = None if timeout is None or timeout < 0 else timeout
        self._owner._check_wait(wait, bool(events))
        return events
