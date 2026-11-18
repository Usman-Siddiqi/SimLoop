"""aiotools 2.2.4: a ``TaskScope`` timeout leaves a cancel request on the task.

When a ``TaskScope(timeout=...)`` (and so ``move_on_after`` and
``ShieldScope(timeout=...)``) expires, ``TaskScope._on_timeout`` calls the
host task's real ``cancel()`` to interrupt the scope body, and the scope
turns the resulting ``CancelledError`` into ``TimeoutError`` (or swallows it,
for ``move_on_after``). Nothing calls ``uncancel()`` for that request, so the
task's ``cancelling()`` count stays at 1 after the scope is gone.
``asyncio.timeout()`` does call ``uncancel()`` for its own request.

The leaked count changes how asyncio's own structured concurrency behaves
later in the same task. On Python 3.13+, ``TaskGroup.__aexit__`` reads a non-zero
``cancelling()`` as "this task is being cancelled from outside": when a child
then fails, it raises the ``ExceptionGroup`` but re-cancels the task, so the
next ``await`` raises ``CancelledError`` although nothing cancelled it.
``asyncio.run`` then ends with ``CancelledError`` instead of a result.

SimLoop finds it with a generic check, ``SimConfig(strict_cancellation=True)``,
which fails a run when a task keeps running with a cancel request nobody
consumed and then finishes normally.

Run: python findings/aiotools_scope_timeout.py
"""

from __future__ import annotations

import asyncio
import sys

import aiotools

import simloop


async def harness() -> None:
    """Mix aiotools scopes with timeouts and asyncio code in one task."""
    rng = simloop.rng()

    async def work(i: int) -> int:
        await asyncio.sleep(rng.uniform(0, 0.1))
        return i

    for _ in range(3):
        kind = rng.choice(["move_on_after", "scope", "race", "supervisor"])
        if kind == "move_on_after":
            async with aiotools.move_on_after(rng.uniform(0, 0.1)):
                await work(0)
        elif kind == "scope":
            try:
                async with aiotools.TaskScope(timeout=rng.uniform(0, 0.1)) as ts:
                    for i in range(3):
                        ts.create_task(work(i))
            except TimeoutError:
                pass
        elif kind == "race":
            await aiotools.race([work(i) for i in range(3)])
        else:
            async with aiotools.Supervisor() as sv:
                sv.create_task(work(0))
        async with asyncio.timeout(1.0):
            await work(1)


async def plain() -> str:
    """Fixed timings on a real asyncio loop."""
    async with aiotools.move_on_after(0.01):
        await asyncio.sleep(1)
    leaked = asyncio.current_task().cancelling()  # type: ignore[union-attr]

    async def fail() -> None:
        raise ValueError("child failed")

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(fail())
            await asyncio.sleep(1)
    except ExceptionGroup:
        pass
    try:
        await asyncio.sleep(0)
    except asyncio.CancelledError:
        return f"cancelling()={leaked} after move_on_after; a plain sleep was then cancelled"
    return f"cancelling()={leaked} after move_on_after; no spurious cancellation"


def main() -> int:
    config = simloop.SimConfig(strict_cancellation=True)
    report = simloop.explore(harness, seeds=range(200), config=config)
    print(report.summary())
    assert not report.ok, "the leak should be found"
    assert {group.kind for group in report.groups} == {"cancel_leak"}
    real = asyncio.run(plain())
    print(f"real asyncio loop: {real}")
    assert real.startswith("cancelling()=1")
    # TaskGroup re-cancels a task that looks cancelled from outside since 3.13.
    if sys.version_info >= (3, 13):
        assert "was then cancelled" in real
    return 0


if __name__ == "__main__":
    sys.exit(main())
