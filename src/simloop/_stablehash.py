"""Deterministic hashes for classes that hash by identity.

A set of objects that use ``object.__hash__`` iterates in memory-address
order, which changes from run to run, so any code that iterates such a set
(cancelling every child scope, notifying every waiter) leaks
non-determinism into the trace. SimLoop's own tasks and futures already hash
by creation order; :func:`stable_hash` gives other classes the same
treatment: inside a simulation an instance's hash is the order in which it
was first hashed, which is deterministic when the run is. Outside a
simulation it is the ordinary identity hash, so patching a class changes
nothing for code that never runs under SimLoop.

The first hash is remembered for the rest of the run in a side table (which
also works for classes with ``__slots__``). An object hashed before a run
and hashed again inside it gets a different value, so an object should not
sit in a set across the start of a run; in practice objects created by the
code under test are created inside it.

Some library classes are patched automatically when a run starts (see
:data:`KNOWN_CLASSES`).
"""

from __future__ import annotations

import itertools
import sys
import threading
import weakref
from collections.abc import Callable
from typing import Any, TypeVar

C = TypeVar("C", bound=type)

KNOWN_CLASSES: dict[str, tuple[str, ...]] = {
    # CancelScope._child_scopes is a set; cancelling a task group walks it.
    "anyio._backends._asyncio": ("CancelScope",),
}
"""Identity-hashed library classes that are kept in sets, by module."""


class _Table:
    def __init__(self) -> None:
        self.counter = itertools.count(1)
        self.by_id: dict[int, tuple[Callable[[], object] | object, int]] = {}


_state = threading.local()
_patched: set[type] = set()
_IDENTITY_HASH: Any = object.__hash__


def _hashes_by_identity(cls: type) -> bool:
    method: Any = cls.__hash__
    return method is _IDENTITY_HASH


def _stable_hash(obj: object) -> int:
    table: _Table | None = getattr(_state, "table", None)
    if table is None:
        return object.__hash__(obj)
    key = id(obj)
    entry = table.by_id.get(key)
    if entry is not None:
        holder, value = entry
        target = holder() if isinstance(holder, weakref.ref) else holder
        if target is obj:
            return value
    value = next(table.counter)
    try:
        holder = weakref.ref(obj)
    except TypeError:
        # No weakref support: keep the object alive for the run so its id
        # cannot be reused by another object.
        holder = obj
    table.by_id[key] = (holder, value)
    return value


def stable_hash(cls: C) -> C:
    """Make ``cls`` hash deterministically inside simulations. Usable as a decorator."""
    if cls in _patched:
        return cls
    if not _hashes_by_identity(cls):
        raise TypeError(
            f"{cls.__qualname__} defines its own __hash__; only identity-hashed classes "
            "need stable_hash"
        )
    setattr(cls, "__hash__", _stable_hash)  # noqa: B010
    _patched.add(cls)
    return cls


def _patch_known() -> None:
    for module_name, names in KNOWN_CLASSES.items():
        module = sys.modules.get(module_name)
        if module is None:
            continue
        for name in names:
            cls: Any = getattr(module, name, None)
            if isinstance(cls, type) and cls not in _patched and _hashes_by_identity(cls):
                stable_hash(cls)


def begin_run() -> None:
    _patch_known()
    _state.table = _Table()


def end_run() -> None:
    _state.table = None
