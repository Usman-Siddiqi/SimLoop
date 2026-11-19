"""Virtualize or block process-wide sources of non-determinism during a run.

Two mechanisms:

* Patching: ``time`` functions read the simulated clock, ``time.sleep`` blocks
  simulated time, the ``random`` module's global generator is reseeded from
  the run seed, ``datetime.now()`` and ``date.today()`` read the simulated
  wall clock (see :mod:`simloop._vdatetime`), and ``os.urandom`` (and with
  it ``uuid4``, ``secrets`` and ``SystemRandom``) returns seeded bytes.
  Everything is restored on exit. Third-party modules that imported these
  functions by name (``from time import monotonic``) have those module
  globals swapped too; references kept anywhere else (default arguments,
  closures, class attributes) and in the standard library are not.
* An audit hook (PEP 578) rejects real sockets, DNS, threads and subprocesses
  while a sandbox is active on the current thread, and records the leak so
  the run fails even if the code catches the exception.
"""

from __future__ import annotations

import os
import random
import select
import socket
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterable
from types import ModuleType, TracebackType
from typing import Any

from . import _vdatetime
from ._choices import derive_seed
from ._loop import SimEventLoop
from ._transport import virtual_socket_class
from ._vselect import VirtualSelect
from .errors import NondeterminismError

_BLOCKED_EVENTS = frozenset(
    {
        "socket.__new__",
        "socket.connect",
        "socket.bind",
        "socket.getaddrinfo",
        "socket.gethostbyname",
        "socket.gethostbyaddr",
        "socket.getnameinfo",
        "socket.sendto",
        "subprocess.Popen",
        "os.system",
        "os.exec",
        "os.fork",
        "os.forkpty",
        "os.posix_spawn",
        "os.spawn",
        "_thread.start_new_thread",
        "_thread.start_joinable_thread",
    }
)

# Functions that libraries commonly import by name. Their originals are taken
# here, at import time, before any sandbox patches them.
_CAPTURABLE: dict[int, object] = {
    id(function): function
    for function in (
        time.time,
        time.time_ns,
        time.monotonic,
        time.monotonic_ns,
        time.perf_counter,
        time.perf_counter_ns,
        time.sleep,
        os.urandom,
    )
}
# Standard-library modules keep the real functions: threading, queue and the
# like time real threads, which a simulated clock would hang. Test tooling
# measures real durations.
_KEEP_REAL = frozenset(sys.stdlib_module_names) | {
    "simloop",
    "_pytest",
    "pytest",
    "pluggy",
    "coverage",
}
_captured_scanned: set[str] = set()
_captured: list[tuple[ModuleType, str, object]] = []


def _scan_captured(names: Iterable[str]) -> list[tuple[ModuleType, str, object]]:
    found: list[tuple[ModuleType, str, object]] = []
    for name in names:
        module = sys.modules.get(name)
        if not isinstance(module, ModuleType) or name.partition(".")[0] in _KEEP_REAL:
            continue
        try:
            items = list(vars(module).items())
        except TypeError:
            continue
        for attr, value in items:
            if id(value) in _CAPTURABLE and _CAPTURABLE[id(value)] is value:
                found.append((module, attr, value))
    return found


_local = threading.local()
_hook_lock = threading.Lock()
_hook_installed = False


def _audit_hook(event: str, args: tuple[Any, ...]) -> None:
    if event not in _BLOCKED_EVENTS:
        return
    sandbox: Sandbox | None = getattr(_local, "sandbox", None)
    if sandbox is None or sandbox.suspended:
        return
    sandbox.report_leak(event, args)


def _install_audit_hook() -> None:
    global _hook_installed
    with _hook_lock:
        if not _hook_installed:
            sys.addaudithook(_audit_hook)
            _hook_installed = True


class Sandbox:
    """Context manager that confines the current thread to the simulation."""

    def __init__(self, loop: SimEventLoop, seed: int, epoch: float) -> None:
        self.loop = loop
        self.seed = seed
        self.epoch = epoch
        self.leaks: list[str] = []
        self.leak_errors: list[NondeterminismError] = []
        self.suspended = False
        self._saved: list[tuple[object, str, object]] = []
        self._datetime_undo: list[tuple[object, str, object]] = []
        self._captured_undo: list[tuple[ModuleType, str, object]] = []
        self._replacements: dict[int, object] = {}
        self._modules_before: frozenset[str] = frozenset()
        self._saved_env: dict[str, str] = {}
        self._random_state: tuple[Any, ...] | None = None
        self._bytes = random.Random(derive_seed(seed, "os.urandom"))

    def report_leak(self, event: str, args: tuple[Any, ...]) -> None:
        message = f"{event} called inside the simulation"
        if event.startswith("socket.") and len(args) > 1:
            message += f" (address {args[1]!r})"
        self.leaks.append(message)
        self.loop._record("leak", self.loop._current_actor(), message)
        error = NondeterminismError(message)
        stack = [f for f in traceback.extract_stack()[:-1] if f.filename != __file__]
        error.add_note(
            "Leaked from (most recent call last):\n"
            + "".join(traceback.format_list(stack[-12:])).rstrip()
        )
        self.leak_errors.append(error)
        raise error

    # -- patched functions ---------------------------------------------------

    def _wall(self) -> float:
        network = self.loop.network
        if network is None:
            return self.epoch + self.loop.time()
        return network.current_host().wall_time(self.loop.time(), self.epoch)

    def _urandom(self, size: int) -> bytes:
        if size < 0:
            raise ValueError("negative argument not allowed")
        return self._bytes.randbytes(size)

    def _sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("sleep length must be non-negative")
        self.loop.block_for(seconds)

    def _socket_patches(self) -> list[tuple[object, str, object]]:
        network = self.loop.network
        if network is None:
            return []
        selector = VirtualSelect(network)
        patches: list[tuple[object, str, object]] = [
            (socket, "socket", virtual_socket_class(network)),
            (select, "select", selector.select),
        ]
        if hasattr(select, "poll"):
            patches.append((select, "poll", selector.poll))
        return patches

    def _thread_start(self, *args: Any, **kwargs: Any) -> None:
        self.report_leak("threading.Thread.start", ())

    def _random_seed(self, a: Any = None, version: int = 2) -> None:
        if a is None:
            a = derive_seed(self.seed, "random.seed")
        random._inst.seed(a, version)

    def _patches(self) -> list[tuple[object, str, object]]:
        loop = self.loop
        wall = self._wall
        original_localtime = time.localtime
        original_gmtime = time.gmtime
        original_ctime = time.ctime
        original_strftime = time.strftime

        def localtime(secs: float | None = None) -> time.struct_time:
            return original_localtime(wall() if secs is None else secs)

        def gmtime(secs: float | None = None) -> time.struct_time:
            return original_gmtime(wall() if secs is None else secs)

        def ctime(secs: float | None = None) -> str:
            return original_ctime(wall() if secs is None else secs)

        def strftime(fmt: str, t: Any = None) -> str:
            return original_strftime(fmt, original_localtime(wall()) if t is None else t)

        def ns(func: Callable[[], float]) -> Callable[[], int]:
            return lambda: round(func() * 1_000_000_000)

        return [
            (time, "time", wall),
            (time, "time_ns", ns(wall)),
            (time, "monotonic", loop.time),
            (time, "monotonic_ns", ns(loop.time)),
            (time, "perf_counter", loop.time),
            (time, "perf_counter_ns", ns(loop.time)),
            (time, "process_time", loop.time),
            (time, "process_time_ns", ns(loop.time)),
            (time, "thread_time", loop.time),
            (time, "thread_time_ns", ns(loop.time)),
            (time, "sleep", self._sleep),
            (time, "localtime", localtime),
            (time, "gmtime", gmtime),
            (time, "ctime", ctime),
            (time, "strftime", strftime),
            (os, "urandom", self._urandom),
            (random, "_urandom", self._urandom),
            (random, "seed", self._random_seed),
            *self._socket_patches(),
            # Python 3.11 has no audit event for starting a thread.
            (threading.Thread, "start", self._thread_start),
        ]

    def _install_captured(self) -> None:
        new = [name for name in list(sys.modules) if name not in _captured_scanned]
        _captured_scanned.update(new)
        _captured.extend(_scan_captured(new))
        for module, attr, original in _captured:
            if getattr(module, attr, None) is original:
                setattr(module, attr, self._replacements[id(original)])
                self._captured_undo.append((module, attr, original))
        self._modules_before = frozenset(sys.modules)

    def _uninstall_captured(self) -> None:
        for module, attr, original in reversed(self._captured_undo):
            if getattr(module, attr, None) is self._replacements[id(original)]:
                setattr(module, attr, original)
        self._captured_undo.clear()
        # A module first imported inside the run captured the simulated
        # function; point it back at the real one so it outlives the run.
        originals = {id(new): _CAPTURABLE[key] for key, new in self._replacements.items()}
        for name in [name for name in list(sys.modules) if name not in self._modules_before]:
            loaded = sys.modules.get(name)
            if not isinstance(loaded, ModuleType):
                continue
            try:
                items = list(vars(loaded).items())
            except TypeError:
                continue
            for attr, value in items:
                if (
                    id(value) in originals
                    and self._replacements.get(id(originals[id(value)])) is value
                ):
                    setattr(loaded, attr, originals[id(value)])

    # -- context manager -------------------------------------------------------

    def __enter__(self) -> Sandbox:
        if getattr(_local, "sandbox", None) is not None:
            raise RuntimeError("simulations cannot be nested")
        _install_audit_hook()
        inst: random.Random = random._inst
        self._random_state = inst.getstate()
        inst.seed(derive_seed(self.seed, "random"))
        for owner, name, replacement in self._patches():
            original = getattr(owner, name)
            self._saved.append((owner, name, original))
            if id(original) in _CAPTURABLE:
                self._replacements[id(original)] = replacement
            setattr(owner, name, replacement)
        self._install_captured()
        self._datetime_undo = _vdatetime.install(self._wall)
        # A simulated network has no proxy; proxy settings inherited from the
        # real environment would send simulated traffic to an address that does
        # not exist (and make runs machine-dependent).
        self._saved_env = {
            key: os.environ[key] for key in list(os.environ) if key.lower().endswith("_proxy")
        }
        for key in self._saved_env:
            del os.environ[key]
        _local.sandbox = self
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _local.sandbox = None
        os.environ.update(self._saved_env)
        _vdatetime.uninstall(self._datetime_undo)
        self._uninstall_captured()
        self._datetime_undo = []
        for owner, name, original in reversed(self._saved):
            setattr(owner, name, original)
        self._saved.clear()
        if self._random_state is not None:
            random._inst.setstate(self._random_state)


def active_sandbox() -> Sandbox | None:
    return getattr(_local, "sandbox", None)
