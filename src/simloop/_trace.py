"""Execution trace: what ran, when, and in which order.

Every event feeds a running digest whether or not events are kept, so cheap
exploration runs and fully traced reruns of the same seed can be compared.
Event text must be free of object ids, wall-clock time and hash ordering.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

_ADDRESS = re.compile(r"\b0x[0-9a-fA-F]{8,}\b")


@dataclass(frozen=True, slots=True)
class TraceEvent:
    step: int
    time: float
    kind: str
    actor: str
    detail: str

    def line(self) -> str:
        return f"{self.step}|{self.time!r}|{self.kind}|{self.actor}|{self.detail}"


class Trace:
    """Ordered record of simulation events plus a digest over all of them."""

    def __init__(self, *, keep: bool = True, digest: bool = True) -> None:
        self.keep = keep
        self._events: list[TraceEvent] = []
        self._hash = hashlib.blake2b(digest_size=16)
        self._count = 0
        self._frozen = False
        self.recording = keep or digest
        """Whether events are used at all; callers can skip building them when not."""

    def freeze(self) -> None:
        """Ignore further events: after a failure, only cleanup remains."""
        self._frozen = True
        self.recording = False

    def record(self, step: int, time: float, kind: str, actor: str, detail: str = "") -> None:
        if not self.recording:
            return
        # Libraries put object reprs into task names and messages; their
        # addresses change from run to run and would break replay.
        if "0x" in actor:
            actor = _ADDRESS.sub("0x?", actor)
        if "0x" in detail:
            detail = _ADDRESS.sub("0x?", detail)
        event = TraceEvent(step, time, kind, actor, detail)
        self._hash.update(event.line().encode())
        self._hash.update(b"\n")
        self._count += 1
        if self.keep:
            self._events.append(event)

    @property
    def events(self) -> list[TraceEvent]:
        return self._events

    def __len__(self) -> int:
        return self._count

    def __iter__(self) -> Iterator[TraceEvent]:
        return iter(self._events)

    def digest(self) -> str:
        return self._hash.hexdigest()

    def format(self, *, kinds: Iterable[str] | None = None, last: int | None = None) -> str:
        """Render kept events as a human-readable timeline."""
        selected = set(kinds) if kinds is not None else None
        events = [e for e in self._events if selected is None or e.kind in selected]
        omitted = 0
        if last is not None and len(events) > last:
            omitted = len(events) - last
            events = events[-last:]
        width = max((len(e.actor) for e in events), default=5)
        lines = [f"{'time':>12}  {'step':>6}  {'actor':<{width}}  event"]
        if omitted:
            lines.append(f"{'':>12}  {'':>6}  {'...':<{width}}  ({omitted} earlier events)")
        for e in events:
            text = e.kind if not e.detail else f"{e.kind:<8} {e.detail}"
            lines.append(f"{e.time:>12.6f}  {e.step:>6}  {e.actor:<{width}}  {text}")
        return "\n".join(lines)
