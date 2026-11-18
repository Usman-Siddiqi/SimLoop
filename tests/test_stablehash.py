from __future__ import annotations

import asyncio

import pytest

import simloop

from .helpers import run_ok


@simloop.stable_hash
class Scope:
    def __init__(self, n: int) -> None:
        self.n = n


@simloop.stable_hash
class SlotScope:
    __slots__ = ("n",)

    def __init__(self, n: int) -> None:
        self.n = n


def _set_order(cls: type) -> list[int]:
    async def main() -> list[int]:
        # Interleave garbage so addresses differ between runs.
        junk = [object() for _ in range(simloop.rng().randint(0, 50))]
        items = {cls(n) for n in range(40)}
        await asyncio.sleep(0)
        del junk
        return [item.n for item in items]

    value = run_ok(main).value
    assert value is not None
    return value


@pytest.mark.parametrize("cls", [Scope, SlotScope])
def test_sets_iterate_in_creation_order(cls: type) -> None:
    first = _set_order(cls)
    for _ in range(5):
        junk = [cls(0) for _ in range(17)]  # shift the allocator between runs
        assert _set_order(cls) == first
        del junk


def test_identity_hash_outside_simulations() -> None:
    scope = Scope(1)
    assert hash(scope) == object.__hash__(scope)
    assert scope != Scope(1)


def test_hash_is_stable_within_a_run() -> None:
    async def main() -> bool:
        scope = Scope(1)
        first = hash(scope)
        await asyncio.sleep(1)
        return hash(scope) == first and scope in {scope}

    assert run_ok(main).value is True


def test_classes_with_their_own_hash_are_rejected() -> None:
    class Point:
        def __hash__(self) -> int:
            return 1

    with pytest.raises(TypeError, match="defines its own __hash__"):
        simloop.stable_hash(Point)


def test_anyio_cancel_scopes_are_patched() -> None:
    anyio = pytest.importorskip("anyio")

    async def main() -> None:
        async def child(delay: float) -> None:
            with anyio.move_on_after(delay):
                await anyio.sleep(10)
            raise RuntimeError("boom")

        async with anyio.create_task_group() as tg:
            for n in range(6):
                tg.start_soon(child, 0.1 * (n % 2))

    first = simloop.run(main, seed=0)
    assert first.failure is not None
    assert simloop.check_determinism(main, seed=0, runs=4) is None
