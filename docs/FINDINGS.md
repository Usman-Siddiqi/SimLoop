# Findings log

Every suspected bug found by running real libraries under SimLoop is recorded
here. Rules (see `CLAUDE.md`):

- Only list something under **Confirmed bugs** when it reproduces from the
  recorded seed and settings, and the execution it relies on is possible under
  real asyncio.
- Each entry needs: library, version, minimal reproduction script (in
  `findings/`), seed, SimLoop settings, and an explanation.
- Reporting upstream is not done from here. Nothing in this file has been
  reported to any project unless an entry says so explicitly.

## Confirmed bugs

### 1. aiojobs 1.4.0: closing a pending job lets the scheduler exceed `limit`

- **Library:** [aiojobs](https://pypi.org/project/aiojobs/) 1.4.0 (latest
  release on 2026-11-18; the code is unchanged on the upstream default branch).
- **Reproduction:** [`findings/aiojobs_pending_close.py`](../findings/aiojobs_pending_close.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** fails on 202 of seeds 0-999 with the default settings
  (`scheduler="random"`; first failing seed 1) and on 174 of 1000 with
  `scheduler="fifo", timer_jitter=0.001` (first failing seed 0).
- **Property violated:** a `Scheduler(limit=N)` never runs more than N jobs
  at once.
- **Explanation:** `Scheduler.active_count` is computed as
  `len(self._jobs) - self._pending.qsize()`. `Job.close()` on a job that is
  still queued starts and immediately cancels its task; the task's done
  callback calls `Scheduler._done(job)`, which removes the job from `_jobs`
  while the job is still in the pending queue. `active_count` is now one too
  low, and `_done` uses it to decide how many queued jobs to start, so one
  extra job starts while the limit is already reached. In the minimal
  reproduction (limit 1: one running job, two queued, close the first queued
  job) two jobs run at the same time.
- **Feasibility:** reproduces on the standard asyncio event loop with no
  special timing (`asyncio.run`, see the script), and under SimLoop with FIFO
  scheduling.
- **Upstream status:** no matching issue or pull request found in a search of
  aio-libs/aiojobs on 2026-11-18. Not reported: Usman decided on 2026-11-18
  not to report anything upstream yet.

### 2. async-lru 2.4.0: a stale call evicts the entry that replaced it

- **Library:** [async-lru](https://pypi.org/project/async-lru/) 2.4.0 (latest
  release on 2026-11-18).
- **Reproduction:** [`findings/async_lru_replaced_entry.py`](../findings/async_lru_replaced_entry.py).
- **Seeds:** fails on 15 of seeds 0-999 with the default settings (first
  failing seed 135) and on 16 of 1000 with `scheduler="fifo",
  timer_jitter=0.001` (first failing seed 73). The shrunk run of seed 135 is
  40 steps long: a call times out after its key was invalidated and refilled,
  and the next call for that key starts a second execution.
- **Property violated:** concurrent calls for the same key share one
  execution (after the most recent invalidation).
- **Explanation:** after `cache_invalidate(key)` the original call keeps
  running. If another call fills the key first, the original call's cleanup
  pops `self.__cache[key]` without checking that the entry is still its own:
  in `_shield_and_handle_cancelled_error` when its last waiter is cancelled,
  and in `_task_done_callback` when it fails. The replacement entry is
  evicted, and the next call for the key starts a duplicate execution
  instead of joining the one in flight. A successful stale call also starts
  the replacement's TTL timer early.
- **Feasibility:** reproduces on the standard asyncio event loop (see the
  script: 3 executions where 2 are expected).
- **Upstream status:** already known. Open pull requests
  aio-libs/async-lru#797 and #807 describe and fix the same problem; neither
  is in a release yet. Listed here as a rediscovery from a generic property,
  not as a new report.

### 3. CPython 3.11 asyncio: cancelling `wait_for` mid-timeout releases another task's lock

- **Library:** CPython `asyncio`, 3.11.1 through 3.11.18 (tested 3.11.1,
  3.11.4, 3.11.7, 3.11.9, 3.11.18). Not affected: 3.12.0 and 3.13.
- **Reproduction:** [`findings/cpython311_wait_for_lock.py`](../findings/cpython311_wait_for_lock.py)
  (plain-asyncio reproduction plus the SimLoop harness; CI runs it on 3.11,
  where it must reproduce, and on 3.13, where it must not).
- **Seeds:** on 3.11.18, fails on 223 of seeds 0-999 with the default
  settings (first failing seed 2) and on 247 of 1000 with
  `scheduler="fifo", timer_jitter=0.001` (first failing seed 2). Two
  signatures: `RuntimeError: Lock is not acquired.` and a second task inside
  the lock (first seed 5 under FIFO).
- **Property violated:** at most one task is inside `async with cond`, and
  the lock is free once every task has finished.
- **How it was found:** a generic workload over stdlib primitives
  (producers and consumers doing
  `async with cond: await asyncio.wait_for(cond.wait_for(pred), t)`)
  deadlocked on 3.11 only. Shrinking it is what exposed the shrinker
  problem fixed in `8a48f64`.
- **Explanation:** 3.11's `wait_for` runs its awaitable in a separate inner
  task. On timeout it cancels that task and waits for it in
  `_cancel_and_wait`, which is deliberately cancellable. If the calling task
  is cancelled during that wait, `wait_for` raises at once and leaves the
  inner task running. Here the inner task is `Condition.wait`, whose cleanup
  re-acquires the lock; when the lock is busy, the caller's `async with`
  exits and releases a lock it does not hold. The next queued task enters
  the critical section while the real holder is still in it, and the
  orphaned inner task later acquires the lock with nobody left to release
  it (the deadlock in the original workload).
- **Feasibility:** reproduces on the standard asyncio event loop with fixed
  sleeps (see `plain()` in the script: the intruder enters while the holder
  is inside). Nothing beyond a timeout that fires while the lock is busy and
  a cancellation (for example shutdown) before the lock frees up.
- **Upstream status:** fixed in 3.12 as a side effect of
  python/cpython GH-96764 (PR #98518), which rewrote `wait_for` on top of
  `asyncio.timeout` so the awaitable runs in the calling task. `locks.py` is
  identical in 3.11.18 and 3.12.0, so the change in behaviour comes from
  `wait_for`. No report of this exact symptom was looked for or filed; 3.11
  only receives security fixes. Listed as a rediscovery of fixed behaviour.

### 4. aiotools 2.2.4: a `TaskScope` timeout leaves a cancel request on the task

- **Library:** [aiotools](https://pypi.org/project/aiotools/) 2.2.4 (latest
  release on 2026-11-18; `taskscope.py` is unchanged on the upstream default
  branch). Affects `TaskScope(timeout=...)`, `move_on_after()` and
  `ShieldScope(timeout=...)`.
- **Reproduction:** [`findings/aiotools_scope_timeout.py`](../findings/aiotools_scope_timeout.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** with `SimConfig(strict_cancellation=True)`, fails on 684 of
  seeds 0-999 with the default scheduler (first failing seed 0) and on 684
  of 1000 with `scheduler="fifo", timer_jitter=0.001` (first failing seed
  0). The shrunk run is 8 steps: one `move_on_after` whose timeout fires,
  then more work in the same task.
- **Property violated:** a task that is not being cancelled has
  `cancelling() == 0`. asyncio's `TaskGroup` and `timeout()` depend on this
  count: whoever suppresses a cancellation it requested must call
  `uncancel()` (as `asyncio.timeout()` does).
- **Explanation:** on expiry `TaskScope._on_timeout` calls the host task's
  real `cancel()` to interrupt the scope body, and the scope converts the
  resulting `CancelledError` into `TimeoutError` (`move_on_after` then
  swallows that). Nothing calls `uncancel()` for that request, so the host
  task's `cancelling()` stays at 1 after the scope is gone, and grows by one
  per expired scope.
- **Consequence:** on Python 3.13 and later, `TaskGroup.__aexit__` treats a
  non-zero `cancelling()` as a cancellation from outside. When a child then
  fails, it raises the `ExceptionGroup` and also re-cancels the task, so the
  next `await` raises `CancelledError` although nothing cancelled the task
  (and `asyncio.run` ends with `CancelledError`). On 3.11 and 3.12 the count
  still leaks, but `TaskGroup` there does not re-cancel, so the visible
  effect is limited to code that reads `cancelling()`.
- **Feasibility:** deterministic; no particular schedule is needed. The
  script's `plain()` reproduces it with `asyncio.run` and fixed sleeps.
- **How it was found:** spotted while reading `TaskScope` to write a
  harness. SimLoop's new generic `strict_cancellation` check (added for this)
  finds it from the harness without any aiotools-specific assertion.
- **Upstream status:** no matching issue or pull request found in searches of
  achimnol/aiotools issues and pull requests for "uncancel", "cancelling",
  "move_on_after" and "timeout TaskScope" on 2026-11-18. Not reported.

### 5. cashews 7.6.0: one caller's timeout cancels every concurrent caller

- **Library:** [cashews](https://pypi.org/project/cashews/) 7.6.0 (latest
  release on 2026-11-18; `thunder_protection` is unchanged on the upstream
  default branch). Affects `@cache(...)`, `@cache.early(...)` and
  `@cache.soft(...)`, which default to `protected=True`, with any backend.
- **Reproduction:** [`findings/cashews_shared_cancel.py`](../findings/cashews_shared_cancel.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** 2 to 4 concurrent callers for one key, some under a random
  `wait_for` timeout: with `SimConfig(strict_cancellation=True)` fails on 510
  of seeds 0-999 with the default scheduler (first failing seed 1) and on
  510 of 1000 with `scheduler="fifo"`.
- **Property violated:** a caller that is not cancelled gets the value.
- **Explanation:** `thunder_protection` starts the first call for a key as a
  task and has every concurrent caller `await` that same task. Awaiting a
  task directly propagates the awaiting caller's cancellation into it, so
  when any one caller is cancelled (its `wait_for` timeout, a client
  disconnect, a `TaskGroup` abort), the shared task is cancelled, every other
  caller for that key raises `CancelledError`, and nothing is cached. On
  plain asyncio, a caller with a 10 ms timeout and a caller with none, for a
  100 ms function: the second caller gets `CancelledError` and
  `cancelling()` on its task is 0 (nothing cancelled it).
- **Feasibility:** deterministic once two callers overlap and one is
  cancelled; reproduces with `asyncio.run` and fixed sleeps.
- **How it was found:** spotted while reading `thunder_protection` for a
  harness; SimLoop's `strict_cancellation` check (extended for this to flag
  a `CancelledError` that came from another task's cancellation) finds it
  from a harness with no cashews-specific assertion beyond "got the value".
- **Upstream status:** no matching issue or pull request found in searches
  of Krukov/cashews for "thunder_protection", "CancelledError", "cancel" and
  "shield" on 2026-11-18 (#150 is about Redis timeouts under many cancelled
  requests, with no root cause identified). Not reported.

### 6. backoff 2.2.1: `max_time` uses a stale, wall-clock elapsed time

- **Library:** [backoff](https://pypi.org/project/backoff/) 2.2.1 (latest
  release on 2026-11-18). Both the async and the sync decorators.
- **Reproduction:** [`findings/backoff_max_time.py`](../findings/backoff_max_time.py)
  (plain-asyncio reproduction, SimLoop harness and a clock-jump run).
- **Seeds:** "no attempt starts after `max_time`" fails on 148 of seeds
  0-999 with the default settings and with `scheduler="fifo"` (first
  failing seed 1).
- **Property violated:** `max_time` is documented as "the maximum total
  amount of time to try for before giving up".
- **Explanation:** the retry loop computes `elapsed` at its top, before the
  attempt runs, and checks `elapsed >= max_time` after the attempt fails. An
  attempt that itself runs past the deadline is judged by when it started,
  so backoff sleeps and starts one more attempt after `max_time`. On plain
  asyncio, `max_time=0.1` with 0.2 s attempts makes 2 attempts over 0.41 s.
  `elapsed` also comes from `datetime.datetime.now()`: with SimLoop's
  `faults.clock_jump("main", -30.0)` at t=0.35, `max_time=1.0` kept retrying
  until t=31.0.
- **Feasibility:** deterministic for slow attempts; the clock case needs a
  wall-clock step backwards (NTP, VM resume), which SimLoop injects.
- **Upstream status:** already known. Issue litl/backoff#186 and open pull
  request #130 describe the stale `elapsed`; issue #149 and open pull
  request #150 the wall clock. Listed as a rediscovery, not a new report.

### 7. singleflight 0.1.3: a cancelled leader poisons its key with `None`

- **Library:** [singleflight](https://pypi.org/project/singleflight/) 0.1.3
  (latest release on 2026-11-18; `asynchronous.py` is unchanged on the
  upstream default branch), `SingleFlightAsync`.
- **Reproduction:** [`findings/singleflight_cancelled_leader.py`](../findings/singleflight_cancelled_leader.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** 1 to 4 callers for one key, some with a 50 ms timeout: fails on
  263 of seeds 0-999 with the default scheduler and with `scheduler="fifo"`
  (first failing seed 1: a concurrent caller gets `None`; first seed 18 of
  the second signature: a later call gets `None`).
- **Property violated:** every caller that is not cancelled gets `fn`'s
  result, and the key can be used again once the call is over.
- **Explanation:** the leader runs `fn` inside `try/except Exception/
  finally`. A `CancelledError` is not an `Exception`, so it skips the
  handler; the `finally` sets the event with `res` and `err` still `None`,
  and the exception then skips the `del self.m[key]` after the block.
  Concurrent callers wake and return `None`, and the key's entry stays in
  the map with its event set, so every later call for that key returns
  `None` immediately without calling `fn`. On plain asyncio: a leader with a
  10 ms timeout over a 100 ms `fn`; the concurrent caller and a later call
  both get `None`.
- **Feasibility:** deterministic once the leader is cancelled during `fn`.
- **Upstream status:** the repository (aarondwi/singleflight) has no issues
  or pull requests at all (search on 2026-11-18). Not reported.

### 8. aiodataloader 0.4.3: one caller's cancellation fails every caller of a key, and stays cached

- **Library:** [aiodataloader](https://pypi.org/project/aiodataloader/) 0.4.3
  (latest release on 2026-11-19; `load()` and the cache lookup are unchanged
  on the upstream default branch), `DataLoader` with caching on (the
  default).
- **Reproduction:** [`findings/aiodataloader_shared_future.py`](../findings/aiodataloader_shared_future.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** 1 to 4 callers of one key, some with a 50 ms timeout: with
  `SimConfig(strict_cancellation=True)` fails on 390 of seeds 0-999 with the
  default scheduler and with `scheduler="fifo"` (first failing seed 0: a
  concurrent caller ends with a stray `CancelledError`; first seed 5 of the
  second signature: a later `load` raises `CancelledError`). Without
  `strict_cancellation` the same 390 seeds fail on the harness's own
  assertions.
- **Property violated:** a caller that is not cancelled gets the value, and
  the key loads again once the batch is over.
- **Explanation:** `load(key)` returns a plain `Future` and caches it, so
  every caller of the key awaits the same object. A task that is cancelled
  while awaiting a future cancels the future itself, so one caller's
  `wait_for` timeout, client disconnect or `TaskGroup` abort cancels the
  shared future: every concurrent caller of the key raises `CancelledError`
  although nothing cancelled it, and the batch result for the key is
  dropped (the `done()` guards added for the `InvalidStateError` reports
  skip it). The cancelled future also stays in the cache, and `load`
  returns a cached future without checking its state, so every later `load`
  of that key raises `CancelledError` at once until `clear(key)` is called.
  Loaders are often created per request, which limits the damage to that
  request; a long-lived loader keeps the key broken. On plain asyncio, a
  caller with a 10 ms timeout and a caller with none, over a 100 ms batch:
  the second caller gets `CancelledError` with `cancelling()` 0, and a later
  `load` also gets `CancelledError`.
- **Feasibility:** deterministic once two callers of a key overlap and one
  is cancelled; reproduces with `asyncio.run` and fixed sleeps.
- **How it was found:** SimLoop's `strict_cancellation` check, extended for
  this to follow cancellation through a shared plain future, flags the
  concurrent caller; the later-load assertion catches the cache poisoning.
- **Upstream status:** searches of syrusakbary/aiodataloader on 2026-11-19
  found no issue about either effect. Closed pull requests #44 and #55 fix
  the `InvalidStateError` from setting a result on a cancelled future, and
  open pull request #60 is about an older failed batch clearing a
  replacement entry; none covers cancellation spreading between callers or
  the cancelled future staying cached. Not reported.

### 9. strawberry-graphql 0.341.2: one caller's cancellation fails every caller of a DataLoader key

- **Library:** [strawberry-graphql](https://pypi.org/project/strawberry-graphql/)
  0.341.2 (latest release on 2026-11-19), `strawberry.dataloader.DataLoader`.
- **Reproduction:** [`findings/strawberry_dataloader_cancel.py`](../findings/strawberry_dataloader_cancel.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** the same workload as the aiodataloader harness fails on 316 of
  seeds 0-999 with `strict_cancellation=True`, with the default scheduler and
  with `scheduler="fifo"` (first failing seed 0).
- **Property violated:** a caller that is not cancelled gets the value.
- **Explanation:** the same shared-future mechanism as aiodataloader: every
  caller of a cached key awaits one future, and one caller's cancellation
  cancels it for all of them. Strawberry skips a cancelled future in its
  cache, so later loads work. On plain asyncio the concurrent caller gets
  `CancelledError` with `cancelling()` 0, and a later load returns the value.
- **Feasibility:** deterministic once two callers overlap and one is
  cancelled. The upstream pull request notes that graphql-core 3.3 cancels
  the sibling resolvers of a failing non-null field, which triggers it
  inside a single query.
- **Upstream status:** already known: open pull request
  strawberry-graphql/strawberry#4803 (2026-11-10). Listed as a rediscovery
  by the generic check, not a new report.

## Suspected issues

### aiocache 0.12.3: `cached_stampede` stops protecting after a failed call

- **Library:** [aiocache](https://pypi.org/project/aiocache/) 0.12.3, memory
  backend.
- **Reproduction:** [`findings/aiocache_stampede.py`](../findings/aiocache_stampede.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** fails on 102 of seeds 0-999 with `scheduler="fifo"` (first
  failing seed 21) and on 98 of 1000 with the random scheduler; every random
  failure was also found with asyncio's own order. Shrinking reduces it to
  four callers at t=0, one of them with a zero timeout.
- **What happens:** `cached_stampede` holds a `RedLock` while the first
  caller computes; the others wait on the lock's release event. If the
  holder ends without storing a value (its function raised, or it was
  cancelled), `__aexit__` still releases and sets the event. Every waiter
  wakes, misses the cache and calls the function itself, without taking the
  lock again. On plain asyncio, 10 concurrent callers whose first
  computation raises run the function 10 times, 9 of them at once, with a
  10 s lease that never expired.
- **Why it is only suspected:** `RedLock`'s docstring says "blocking just
  happens for the first time" and warns against relying on it for real
  exclusion. It describes this for an expired lease; the failure case is not
  documented, but the behaviour follows from the same design. It defeats
  the decorator's stated purpose ("avoids cache stampede effects") exactly
  when a backend is failing. Whether that is a bug is the maintainers' call.
- **Not reported.**

### aiometer 1.0.0: `run_any` returns only after every function has finished

- **Library:** [aiometer](https://pypi.org/project/aiometer/) 1.0.0 (latest
  release on 2026-11-18; `run_any.py` and `amap.py` are unchanged on the
  upstream default branch).
- **Reproduction:** [`findings/aiometer_meters.py`](../findings/aiometer_meters.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** the property "`run_any` returns when the first function
  completes" fails on all 1000 seeds 0-999 with the default settings (every
  run has at least two functions with different durations).
- **What happens:** `run_any` is `amap(...)` plus taking the first item from
  the result stream. Returning from inside `amap`'s context exits its anyio
  task group normally, which waits for every remaining function. The other
  functions are not cancelled, and the first result is only returned once
  the slowest has finished. On plain asyncio, `run_any([fast, slow])` with
  0.05 s and 1 s sleeps returns `"fast"` after 1.0 s.
- **Why it is only suspected:** the README says `run_any` runs the
  functions "until the first one completes, similarly to `Promise.any()`",
  and the API docs say it returns "the first available result". That reads
  as returning at the first completion, but nothing promises that the
  others are cancelled, and waiting for them may be the intended
  structured-concurrency behaviour.
- **Upstream status:** no matching issue or pull request found in searches
  of florimondmanca/aiometer on 2026-11-18. Not reported.

### asyncstdlib 3.14.0: cancelling one `tee` peer silently ends the others

- **Library:** [asyncstdlib](https://pypi.org/project/asyncstdlib/) 3.14.0
  (latest release on 2026-11-18; `TeePeer` is unchanged on the upstream
  default branch).
- **Reproduction:** [`findings/asyncstdlib_tee_cancel.py`](../findings/asyncstdlib_tee_cancel.py)
  (plain-asyncio reproduction plus the SimLoop harness).
- **Seeds:** with two peers over a 6-item async generator, one peer under a
  random `asyncio.timeout`, the other peer misses items on 719 of seeds
  0-999 with `scheduler="fifo"` (first failing seed 0).
- **What happens:** the most advanced peer fetches the next item by awaiting
  the source's `__anext__` in its own task. When that peer is cancelled
  there, the `CancelledError` is thrown into the source async generator,
  which finishes and is closed. The other peers then get
  `StopAsyncIteration` and their `async for` loops end normally, as if the
  stream were complete. On plain asyncio, the peer that was not cancelled
  receives 2 of 6 items and no error.
- **Why it is only suspected:** a bare async generator shared between
  consumers behaves the same way, and `tee`'s documentation ("each providing
  the same items in the same order") does not discuss cancellation. Silent
  truncation for consumers that were not cancelled is the part that seems
  worth raising; a `tee` could fetch in a task no peer owns.
- **Upstream status:** no matching issue found in searches of
  maxfischer2781/asyncstdlib for "tee cancel", "tee cancelled" and "tee
  timeout" on 2026-11-18 (issue #177 is a different `tee` bug, fixed). Not
  reported.

## False positives

### aiolimiter 1.3.0: `AsyncLimiter` spins at a fixed instant

- **Harness:** [`findings/aiolimiter_rate.py`](../findings/aiolimiter_rate.py)
  with `step_time=0` (the default at the time): about 84% of seeds hit the
  step limit under both `random` and `fifo`, with `_wake_next` timers firing
  over and over at one simulated instant.
- **What happened:** after leaking, the bucket level was a float residue
  (`2.8e-16`). A full-bucket `acquire` needs that much more capacity, so
  `_wake_next` sets a timer `2.8e-16 / rate` seconds ahead, which rounds to
  the current time. The timer fires, no time has passed, and it rearms.
- **Why it is not a bug:** a real loop's clock moves between iterations, so
  the next leak clears the residue. On plain asyncio no acquire needed more
  than 4 wakeups, even with a 15.6 ms clock tick and matching
  `_clock_resolution` (Windows before Python 3.13). The residue also needs
  a timer to fire at exactly its scheduled instant, which only a frozen
  clock does. With `SimConfig(step_time=1e-6)` the harness is clean.
- **SimLoop changes found on the way:** `step_time` models the clock moving
  while callbacks run, and a step-limit failure now says when the last
  steps all ran at one instant, pointing to `step_time`.

### aiohttp 3.14.5: `TypeError` in `RequestHandler.start` after a reset

- **Harness:** [`findings/aiohttp_pool.py`](../findings/aiohttp_pool.py) with
  the default `scheduler="random"`: 56 of seeds 0-299 fail with
  `TypeError: 'NoneType' object is not callable` at `web_protocol.py:666`
  (first failing seed 5), raised in the server's per-connection task.
- **What happened:** a request's bytes arrive and `data_received` resolves
  the handler task's waiter; a FIN arrives at the same simulated instant and
  `connection_lost` clears `_request_factory`. The random scheduler then ran
  `connection_lost` before the task wakeup, although the wakeup was scheduled
  first.
- **Why it is not a bug:** asyncio's ready queue is FIFO, and on the selector
  loop `data_received` and the end-of-stream read happen in different loop
  iterations, so the wakeup always runs before `connection_lost` is called.
  With `scheduler="fifo"` (with and without 2 ms timer jitter) none of 300
  seeds fail, and `explore --confirm 300` reports the group as not
  reproduced with asyncio's order. The same group shows up in the httpx
  harness, which uses an aiohttp server. This case is why `explore` now
  checks feasibility by default.
- **SimLoop fix found on the way:** an incoming RST used to call
  `connection_lost` synchronously, so for a while the same `TypeError` also
  "reproduced" under FIFO. Real asyncio reports the loss from a later
  callback (`_force_close`); SimLoop now does the same.

## Tested, nothing found

Harnesses that ran clean. Absence of a failure here is evidence about these
properties and fault mixes only.

- **redis-py 8.1.0 (`redis.asyncio`)**, [`findings/redis_reply_integrity.py`](../findings/redis_reply_integrity.py),
  against a simulated RESP2 server:
  every `ECHO` and pipelined reply matched its own request across 500 seeds
  with random client timeouts (`asyncio.wait_for`), 1% injected TCP resets
  per segment and server process crashes every 0.3-1.5 s; the client
  recovered after faults stopped and the pool had no connections stuck in
  use. (The 2023 cancellation bug, CVE-2023-28858/28859, is fixed.)
- **aiohttp 3.14.5**, [`findings/aiohttp_pool.py`](../findings/aiohttp_pool.py):
  every response matched its request, the pool served `limit` concurrent
  requests after faults stopped, and no connection stayed acquired, across
  300 seeds each with `scheduler="fifo"` and with 2 ms timer jitter,
  random client timeouts and cancellations, 0.5% TCP resets per segment and
  server crashes every 0.3-1.5 s.
- **httpx 0.28.1 / httpcore 1.0.9**, [`findings/httpx_pool.py`](../findings/httpx_pool.py):
  same properties and faults as the aiohttp harness with a 3-connection
  pool, plus cancellation of in-flight requests; 300 seeds each with
  `scheduler="fifo"` and with 2 ms jitter, all clean. Keep-alive reuse is
  exercised (13 connections for 43 requests in a fault-free run), which
  needed SimLoop to answer httpcore's zero-timeout `select.poll` readiness
  check on idle sockets.
- **websockets 17.4**, [`findings/websockets_keepalive.py`](../findings/websockets_keepalive.py):
  an echo server behind random partitions (0.2-4 s, every 0.5-3 s); every
  echo came back in order, and no `recv()` waited longer than ping interval +
  ping timeout + close timeout on a dead link, across 1000 seeds each with
  the random scheduler and with `fifo` plus 2 ms jitter. Keepalive closed
  the connection 68 times in 30 sample runs. An early "lost message" failure
  here was a SimLoop bug (segments in flight when a partition healed could
  overtake the held ones), now fixed.
- **aiostream 0.8.2**, [`findings/aiostream_operators.py`](../findings/aiostream_operators.py):
  `merge` delivered every item once and in per-source order, `map` with
  `task_limit` kept its bound and its order when `ordered=True`, and leaving
  a merged stream early (`break` or a timeout) closed every source. 1000
  seeds each with `random`, `pct` and `fifo` plus 1 ms jitter.
- **aiomisc 18.0.40 (`@aggregate`)**, [`findings/aiomisc_aggregate.py`](../findings/aiomisc_aggregate.py):
  every caller that was not cancelled got its own result and no argument
  went through a completed batch twice, with random windows, `max_count`
  and caller timeouts. A batch whose leading caller is cancelled is
  restarted by the next caller, so work before the first `await` in the
  batch function runs again; that is a design choice, not counted here.
- **tenacity 9.2.1**, [`findings/tenacity_retry.py`](../findings/tenacity_retry.py):
  with `stop_before_delay(T)` no attempt started after `T` (plus timer
  lateness), and cancelling the caller stopped retrying. `stop_after_delay`
  does start attempts after its delay, as its docstring says.
- **asyncstdlib 3.14.0 (`tee` with a lock)**, [`findings/asyncstdlib_tee_cancel.py`](../findings/asyncstdlib_tee_cancel.py):
  1 to 4 peers consumed concurrently through `lock=asyncio.Lock()` each saw
  every item in order, 1000 seeds each with `random`, `pct` and `fifo` plus
  1 ms jitter (`strict_cancellation=True`).
- **aiometer 1.0.0 (meters)**, [`findings/aiometer_meters.py`](../findings/aiometer_meters.py):
  `amap` never ran more than `max_at_once` jobs, spaced starts by at least
  `1 / max_per_second` and yielded every result; a timed-out or failing
  `run_on_each` left no job running. 1000 seeds each with the `random`,
  `pct` and `fifo` plus 1 ms jitter schedulers, all with
  `strict_cancellation=True`.
- **aiolimiter 1.3.0**, [`findings/aiolimiter_rate.py`](../findings/aiolimiter_rate.py):
  the leaky-bucket bound (at most `max_rate + rate * window` capacity
  granted in any window) held and no waiter was left unwoken, across 1000
  seeds each with the `random`, `fifo` and `pct` schedulers, with mixed
  acquire amounts and random `wait_for` timeouts (`step_time=1e-6`).
- **anyio 4.16.0 (asyncio backend)**, [`findings/anyio_primitives.py`](../findings/anyio_primitives.py):
  memory object streams delivered every successfully sent item exactly once
  and in order per producer (buffer sizes 0, 1, 3; two consumers);
  `CapacityLimiter` never exceeded its total, woke waiters when the total
  was raised and left nothing borrowed; `Semaphore` and `Lock` kept their
  bounds in both `fast_acquire` modes. 1000 seeds each with the `random`,
  `fifo` and `pct` schedulers, with random `move_on_after` cancellations.
  The first attempt flagged every failing seed as unreproducible: anyio
  keeps child cancel scopes in a set, which iterated in address order.
  SimLoop now hashes such classes by creation order (`stable_hash`).
- **uvicorn 0.54.0 + Starlette 1.10.0 + httpx 0.28.1** (h11),
  [`findings/uvicorn_starlette.py`](../findings/uvicorn_starlette.py), all
  running unmodified on the simulated network: every response carried its
  own request's id and body across keep-alive reuse, client timeouts and a
  fault schedule (server crashes and restarts, 2% injected resets,
  partitions), and a fresh request succeeded after the faults; running
  handlers never exceeded `limit_concurrency`; a graceful shutdown in the
  middle of traffic answered every request whose handler had started.
  300 seeds each with `random`, `pct` and `fifo` plus 2 ms jitter.
- **aiorwlock 1.6.0**, [`findings/aiorwlock_exclusion.py`](../findings/aiorwlock_exclusion.py): mutual exclusion (no writer with readers or another
  writer), no deadlock and no lock left held, across 3000 seeds with random
  acquire timeouts, in both `fast` modes.
