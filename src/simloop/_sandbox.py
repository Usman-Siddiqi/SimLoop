"""Virtualize or block process-wide sources of non-determinism during a run.

Two mechanisms:

* Patching: ``time`` functions read the simulated clock, ``time.sleep`` blocks
  simulated time, the ``random`` module's global generator is reseeded from
  the run seed, ``datetime.now()`` and ``date.today()`` read the simulated
  wall clock (see :mod:`simloop._vdatetime`), local time is a fixed simulated
  zone (``SimConfig.timezone``, UTC by default) instead of the host's, and
  ``os.urandom`` (and with it ``uuid4``, ``secrets`` and ``SystemRandom``)
  returns seeded bytes.
  Everything is restored on exit. Third-party modules that imported these
  functions by name (``from time import monotonic``) have those module
  globals swapped too, and so are references held in their classes'
  attributes, dataclass ``default_factory`` fields, default arguments and
  closures; references in the standard library are not.
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
from dataclasses import Field
from types import CellType, FunctionType, ModuleType, TracebackType
from typing import Any

from . import _vdatetime
from ._choices import derive_seed
from ._loop import SimEventLoop
from ._transport import virtual_socket_class
from ._vselect import VirtualSelect, virtual_fcntl
from .errors import NondeterminismError

try:
    import fcntl as _fcntl
except ImportError:  # Windows
    _fcntl = None  # type: ignore[assignment]

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
        *([_fcntl.fcntl] if _fcntl is not None else []),
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


class _Ref:
    """One place that holds a capturable function: a module or class attribute,
    a dataclass field's ``default_factory``, a closure cell or a default argument."""

    __slots__ = ("holder", "key")

    def __init__(self, holder: Any, key: str | int | None) -> None:
        self.holder = holder
        self.key = key

    def get(self) -> object:
        holder, key = self.holder, self.key
        if isinstance(holder, CellType):
            try:
                return holder.cell_contents
            except ValueError:
                return None
        if isinstance(key, int):
            defaults = holder.__defaults__ or ()
            return defaults[key] if key < len(defaults) else None
        if isinstance(holder, FunctionType):
            return (holder.__kwdefaults__ or {}).get(str(key))
        return getattr(holder, str(key), None)

    def set(self, value: object) -> None:
        holder, key = self.holder, self.key
        if isinstance(holder, CellType):
            holder.cell_contents = value
        elif isinstance(key, int):
            defaults = list(holder.__defaults__)
            defaults[key] = value
            holder.__defaults__ = tuple(defaults)
        elif isinstance(holder, FunctionType):
            holder.__kwdefaults__ = {**(holder.__kwdefaults__ or {}), str(key): value}
        else:
            setattr(holder, str(key), value)


_captured: list[tuple[_Ref, object]] = []


def _scan_captured(names: Iterable[str], table: dict[int, object]) -> list[tuple[_Ref, object]]:
    """Find every reference to a function in ``table`` held by the named modules."""
    found: list[tuple[_Ref, object]] = []
    seen: set[int] = set()

    def hit(value: object) -> bool:
        return id(value) in table and table[id(value)] is value

    def function(fn: FunctionType, depth: int) -> None:
        if id(fn) in seen or depth > 3:
            return
        seen.add(id(fn))
        for index, value in enumerate(fn.__defaults__ or ()):
            if hit(value):
                found.append((_Ref(fn, index), value))
        for name, value in (fn.__kwdefaults__ or {}).items():
            if hit(value):
                found.append((_Ref(fn, name), value))
        for cell in fn.__closure__ or ():
            try:
                value = cell.cell_contents
            except ValueError:
                continue
            if hit(value):
                found.append((_Ref(cell, None), value))
            elif isinstance(value, FunctionType):
                function(value, depth + 1)

    def member(value: object, depth: int) -> None:
        if isinstance(value, (staticmethod, classmethod)):
            value = value.__func__
        if isinstance(value, FunctionType):
            function(value, depth)

    for name in names:
        module = sys.modules.get(name)
        if not isinstance(module, ModuleType) or name.partition(".")[0] in _KEEP_REAL:
            continue
        try:
            items = list(vars(module).items())
        except TypeError:
            continue
        for attr, value in items:
            if hit(value):
                found.append((_Ref(module, attr), value))
            elif isinstance(value, type) and getattr(value, "__module__", None) == name:
                if id(value) in seen:
                    continue
                seen.add(id(value))
                for key, inner in list(vars(value).items()):
                    if hit(inner):
                        found.append((_Ref(value, key), inner))
                    else:
                        member(inner, 0)
                fields = vars(value).get("__dataclass_fields__")
                for field in fields.values() if isinstance(fields, dict) else ():
                    if isinstance(field, Field) and hit(field.default_factory):
                        found.append((_Ref(field, "default_factory"), field.default_factory))
            elif getattr(value, "__module__", None) == name:
                member(value, 0)
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

    def __init__(
        self, loop: SimEventLoop, seed: int, epoch: float, timezone: str | float = "UTC"
    ) -> None:
        self.loop = loop
        self.seed = seed
        self.epoch = epoch
        self.zone = _vdatetime.Zone(timezone)
        self.leaks: list[str] = []
        self.leak_errors: list[NondeterminismError] = []
        self.suspended = False
        self._saved: list[tuple[object, str, object]] = []
        self._datetime_undo: list[tuple[object, str, object]] = []
        self._captured_undo: list[tuple[_Ref, object]] = []
        self._replacements: dict[int, object] = {}
        self._modules_before: frozenset[str] = frozenset()
        self._saved_env: dict[str, str] = {}
        self._saved_tz: str | None = None
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
        patches: list[tuple[object, str, object]] = []
        if _fcntl is not None:
            patches.append((_fcntl, "fcntl", virtual_fcntl(network, _fcntl.fcntl)))
        if network is None:
            return patches
        selector = VirtualSelect(network)
        patches += [
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
        original_gmtime = time.gmtime
        original_asctime = time.asctime
        original_strftime = time.strftime
        # Local time is the simulated zone, never the host's: a run must not
        # depend on the TZ of the machine it happens to execute on.
        zone = self.zone

        def localtime(secs: float | None = None) -> time.struct_time:
            moment = zone.local(wall() if secs is None else secs)
            offset = round(moment.utcoffset().total_seconds())
            fields = (*moment.timetuple()[:8], 1 if moment.dst() else 0)
            return time.struct_time(fields, {"tm_zone": moment.tzname(), "tm_gmtoff": offset})

        def gmtime(secs: float | None = None) -> time.struct_time:
            return original_gmtime(wall() if secs is None else secs)

        def mktime(t: Any) -> float:
            fields = tuple(t)
            return float(round(zone.timestamp(fields, fields[8])))

        def asctime(t: Any = None) -> str:
            return original_asctime(localtime() if t is None else t)

        def ctime(secs: float | None = None) -> str:
            return original_asctime(localtime(secs))

        def strftime(fmt: str, t: Any = None) -> str:
            return original_strftime(fmt, localtime() if t is None else t)

        west, west_dst, daylight, tzname = zone.constants(original_gmtime(wall()).tm_year)

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
            (time, "mktime", mktime),
            (time, "asctime", asctime),
            (time, "ctime", ctime),
            (time, "strftime", strftime),
            (time, "timezone", west),
            (time, "altzone", west_dst),
            (time, "daylight", daylight),
            (time, "tzname", tzname),
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
        _captured.extend(_scan_captured(new, _CAPTURABLE))
        for ref, original in _captured:
            if ref.get() is original:
                ref.set(self._replacements[id(original)])
                self._captured_undo.append((ref, original))
        self._modules_before = frozenset(sys.modules)

    def _uninstall_captured(self) -> None:
        for ref, original in reversed(self._captured_undo):
            if ref.get() is self._replacements[id(original)]:
                ref.set(original)
        self._captured_undo.clear()
        # A module first imported inside the run captured the simulated
        # function; point it back at the real one so it outlives the run.
        originals = {id(new): _CAPTURABLE[key] for key, new in self._replacements.items()}
        replacements = {id(new): new for new in self._replacements.values()}
        late = [name for name in list(sys.modules) if name not in self._modules_before]
        for ref, replacement in _scan_captured(late, replacements):
            ref.set(originals[id(replacement)])

    # -- context manager -------------------------------------------------------

    def __enter__(self) -> Sandbox:
        if getattr(_local, "sandbox", None) is not None:
            raise RuntimeError("simulations cannot be nested")
        _install_audit_hook()
        # Libraries that look up the local zone themselves (tzlocal, and the C
        # library behind naive ``astimezone()``) read TZ: point it at the
        # simulated zone too, before the time module's zone constants are
        # patched (tzset rewrites them).
        self._saved_tz = os.environ.get("TZ")
        os.environ["TZ"] = self.zone.tz_variable
        _tzset()
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
        self._datetime_undo = _vdatetime.install(self._wall, self.zone.tzinfo)
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
        if self._saved_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self._saved_tz
        _tzset()


def _tzset() -> None:
    if hasattr(time, "tzset"):
        time.tzset()
    # tzlocal caches the zone it found on first use; drop that so it reads
    # TZ again (inside a run: the simulated zone; after it: the host's).
    for name in ("tzlocal.unix", "tzlocal.win32"):
        module = sys.modules.get(name)
        for attr in ("_cache_tz", "_cache_tz_name"):
            if module is not None and hasattr(module, attr):
                setattr(module, attr, None)


def active_sandbox() -> Sandbox | None:
    return getattr(_local, "sandbox", None)
