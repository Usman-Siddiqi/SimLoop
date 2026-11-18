"""aiolimiter: the leaky-bucket bound holds and no acquire is lost.

Clean on aiolimiter 1.3.0 with ``step_time=1e-6`` (see docs/FINDINGS.md:
"Tested, nothing found", and the false positive about a zero-time spin).

The property: for any two grants at times t1 <= t2, the capacity granted in
[t1, t2] is at most ``max_rate + rate * (t2 - t1)``. Acquires use mixed
amounts (including a full bucket) and random ``wait_for`` timeouts, so
cancelled waiters are exercised; a waiter that is never woken shows up as a
deadlock.

Run: python findings/aiolimiter_rate.py
"""

from __future__ import annotations

import asyncio
import sys

from aiolimiter import AsyncLimiter

import simloop

# A real loop's clock moves between iterations; aiolimiter's float rounding
# relies on that (see docs/FINDINGS.md).
CONFIG = simloop.SimConfig(step_time=1e-6, max_steps=200_000)


async def workload() -> None:
    rng = simloop.rng()
    max_rate = rng.choice([1, 2, 3, 5])
    period = rng.choice([0.1, 0.3, 1.0])
    limiter = AsyncLimiter(max_rate, period)
    rate = max_rate / period
    loop = asyncio.get_running_loop()
    grants: list[tuple[float, float]] = []

    async def user() -> None:
        for _ in range(5):
            await asyncio.sleep(rng.uniform(0, period))
            amount = rng.choice([1, 1, 1, 0.5, max_rate])
            timeout = rng.uniform(0, period) if rng.random() < 0.4 else None
            try:
                await asyncio.wait_for(limiter.acquire(amount), timeout)
            except TimeoutError:
                continue
            grants.append((loop.time(), amount))

    async with asyncio.TaskGroup() as tg:
        for _ in range(4):
            tg.create_task(user())
    grants.sort()
    for i, (start, _) in enumerate(grants):
        total = 0.0
        for when, amount in grants[i:]:
            total += amount
            bound = max_rate + rate * (when - start) + 1e-9
            assert total <= bound, f"{total} granted in {when - start:.4f}s, bound {bound:.4f}"


def main() -> int:
    ok = True
    for scheduler in ("random", "fifo", "pct"):
        report = simloop.explore(
            workload, seeds=range(1000), config=CONFIG.replace(scheduler=scheduler)
        )
        print(f"{scheduler}: {report.summary()}")
        ok = ok and report.ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
