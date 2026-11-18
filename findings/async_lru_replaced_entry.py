"""async-lru 2.4.0: a stale call removes the cache entry that replaced it.

After ``cache_invalidate(key)`` the original call keeps running. If another
call fills the key before it finishes, the original call's cancellation
handler (last waiter cancelled) or done callback (call failed) pops
``self.__cache[key]`` without checking that the entry is still its own, so it
evicts the replacement. The next call for the key starts a duplicate
execution instead of joining the one in flight. A successful stale call also
starts the replacement's TTL early.

Already reported upstream: open pull requests aio-libs/async-lru#797 and #807
(not in the 2.4.0 release). SimLoop rediscovered it from a generic property.

Run: python findings/async_lru_replaced_entry.py
"""

from __future__ import annotations

import asyncio
import sys

from async_lru import alru_cache

import simloop

KEYS = 2


async def harness() -> None:
    """Property: after the last invalidation, at most one execution per key is live."""
    rng = simloop.rng()
    generation = [0] * KEYS
    live: list[dict[asyncio.Task[object], int]] = [{} for _ in range(KEYS)]

    @alru_cache(maxsize=None)
    async def fetch(key: int) -> int:
        me = asyncio.current_task()
        assert me is not None
        others = [
            t
            for t, g in live[key].items()
            if g == generation[key] and t is not me and not t.done() and not t.cancelling()
        ]
        assert not others, f"key {key}: second execution while one is in flight"
        live[key][me] = generation[key]
        try:
            await asyncio.sleep(rng.uniform(0.01, 0.1))
            return key
        finally:
            del live[key][me]

    async def client() -> None:
        for _ in range(6):
            await asyncio.sleep(rng.uniform(0, 0.05))
            key = rng.randrange(KEYS)
            op = rng.random()
            if op < 0.15:
                generation[key] += 1
                simloop.log(f"invalidate {key}")
                fetch.cache_invalidate(key)
                continue
            timeout = rng.uniform(0.0, 0.12) if op < 0.5 else None
            try:
                assert await asyncio.wait_for(fetch(key), timeout) == key
            except TimeoutError:
                simloop.log(f"get {key} timed out")

    async with asyncio.TaskGroup() as tg:
        for _ in range(4):
            tg.create_task(client())


async def plain_asyncio() -> int:
    """Minimal reproduction on the standard asyncio event loop."""
    executions = 0

    @alru_cache()
    async def fetch(key: int) -> int:
        nonlocal executions
        executions += 1
        await asyncio.sleep(0.1)
        return key

    stale = asyncio.create_task(fetch(1))
    await asyncio.sleep(0)
    fetch.cache_invalidate(1)
    fresh = asyncio.create_task(fetch(1))  # replaces the entry
    await asyncio.sleep(0)
    stale.cancel()  # last waiter of the stale entry
    await asyncio.sleep(0)
    print(f"  replacement still cached: {fetch.cache_contains(1)}")
    joiner = asyncio.create_task(fetch(1))  # should join `fresh`
    await asyncio.gather(fresh, joiner)
    print(f"  executions: {executions} (expected 2)")
    return executions


def main() -> int:
    print("plain asyncio:")
    executions = asyncio.run(plain_asyncio())
    report = simloop.explore(harness, seeds=range(1000))
    print("simloop explore:")
    print("  " + report.summary().replace("\n", "\n  "))
    reproduced = executions > 2 and not report.ok
    print("REPRODUCED" if reproduced else "NOT REPRODUCED")
    return 0 if reproduced else 1


if __name__ == "__main__":
    sys.exit(main())
