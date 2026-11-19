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

### 10. aiomcache 0.8.2: a cancelled command leaves its reply for the next one; a dropped connection strands pool waiters

- **Library:** [aiomcache](https://pypi.org/project/aiomcache/) 0.8.2
  (latest release on 2026-11-19, from May 2024), `Client` and
  `MemcachePool`.
- **Reproduction:** [`findings/aiomcache_cancel_desync.py`](../findings/aiomcache_cancel_desync.py)
  (two plain-asyncio reproductions against a local memcached-protocol
  server, plus two SimLoop harnesses against a simulated one).
- **Seeds:** "a `get` of a worker's own key returns a value it may have
  stored", 3 workers, 2 pooled connections, random client timeouts: fails
  on 348 of seeds 0-999 with the default scheduler (first failing seed 2)
  and on 328 with `scheduler="fifo", timer_jitter=0.001` (first seed 0).
  "every request finishes" with 1-2 pooled connections and 5% or 20% TCP
  resets per segment: deadlocks on 83 of seeds 0-999 (first seed 2) and on
  96 with fifo plus jitter (first seed 14).
- **Property violated:** a reply belongs to the command that asked for it,
  and a caller waiting for a pooled connection gets one once the pool has
  room.
- **Explanation:** the `acquire` decorator around every command catches
  `Exception` only, and always returns the connection to the pool. A
  command cancelled after it was written but before its reply was read
  (a `wait_for` timeout, a client disconnect) leaves that reply on the
  connection; the next command on it reads it as its own, and every later
  command reads the reply of the one before, until a reply fails to parse.
  `get(key)` then returns `None` for a key that exists, or an older value of
  the same key (the harness's `get(b'w2') returned b'2-4', stored {b'2-6'}`);
  `add` and `delete` can report another command's outcome. Separately,
  `MemcachePool.release` closes a connection whose reader hit EOF or an
  error without putting anything in the queue, so a caller already waiting
  in `acquire` on `Queue.get()` is never woken; when every connection is
  lost that way, the waiter blocks forever although the pool size is 0. On
  plain asyncio: after one `get(b"a")` times out, `get(b"b")` and the next
  `get(b"a")` both return `None` with both keys stored; and with
  `pool_size=1`, a second caller is still waiting 2 s after the first
  caller's connection was dropped by the server.
- **Feasibility:** both reproduce with `asyncio.run` and fixed sleeps over
  real localhost sockets.
- **Upstream status:** already known and fixed on the default branch, not
  released. The reply mix-up is issue aio-libs/aiomcache#489, fixed by pull
  request #510 (merged 2026-11-18); the default branch's `release` also
  queues a `None` placeholder that wakes a waiter when it discards a
  connection (no separate issue about the hang found). Listed as a
  rediscovery; users of the PyPI release are still affected.

### 11. coredis 6.9.0: cancellation leaks pool slots and loses pool wakeups, so commands hang forever

- **Library:** [coredis](https://pypi.org/project/coredis/) 6.9.0 (latest
  release on 2026-11-19; `pool/_basic.py` and `_concurrency.py` are
  unchanged on the upstream default branch), `Redis` with its default
  `ConnectionPool`.
- **Reproduction:** [`findings/coredis_pool_hangs.py`](../findings/coredis_pool_hangs.py)
  (two plain-asyncio reproductions against a local RESP3 server, plus the
  SimLoop harness against the simulated one in `findings/_resp.py`).
- **Seeds:** 4 workers sending `ECHO`s and 2-command pipelines, 30% of them
  under a random `wait_for` timeout, `max_connections` 1-3, no network
  faults; "every request finishes within 30 s" fails on 450 of seeds 0-499
  with the default scheduler (235 with a free connection in the pool, first
  seed 0; 215 with every slot gone, first seed 4) and on 433 with
  `scheduler="fifo", timer_jitter=0.001` (230, first seed 3; 203, first
  seed 4).
- **Property violated:** a command gets a connection once one is free, and
  the pool keeps `max_connections` slots.
- **Explanation:** two separate defects.
  1. *Slot leak.* The pool's queue starts with `max_connections` `None`
     placeholders, each a slot. A pipeline keeps its connection while it
     waits for the replies; cancelling it invalidates the connection (so a
     half-read reply is never reused), and the pipeline's cleanup calls
     `release`, which only puts back a connection that is still usable. The
     connection's own exit path restores a placeholder only for a
     connection that is sitting in the queue, which this one is not. Each
     cancelled pipeline therefore removes a slot for good; after
     `max_connections` of them (64 by default) every command waits forever,
     since the pool's `timeout` defaults to `None`. Blocking commands are
     sent with `disconnect_on_cancellation=should_block` and look exposed
     to the same leak when cancelled (from reading the code; not exercised
     here).
  2. *Lost wakeup.* `coredis._concurrency.Queue.get` takes the queue's lock,
     appends an `Event` to `_getters` and waits on it. A caller cancelled
     while waiting (its own timeout, a disconnect) leaves its event in
     `_getters`. The next `put` pops and sets that stale event; the caller
     now waiting holds the lock and is never woken, and every later caller
     queues behind the lock, while a free connection sits in the pool.

  On plain asyncio: with `max_connections=2`, two pipelines cancelled by a
  10 ms timeout leave 0 slots and the next `ECHO` is still waiting 2 s
  later; with `max_connections=1`, one caller that gives up after 10 ms
  waiting for the connection leaves the next caller waiting 2 s later with
  1 free connection in the pool. A related effect seen once (fifo seed 236
  with `max_connections=64`): a cancelled pipeline also fails an `ECHO`
  multiplexed on the same connection with `ConnectionError: Connection
  lost!`, with no network fault.
- **Feasibility:** both reproduce with `asyncio.run` and fixed sleeps over
  real localhost sockets.
- **Upstream status:** searched alisaifee/coredis issues and pull requests
  on 2026-11-19. For the slot leak, pull request #436 ("fix: restore pool
  slot when releasing an unusable connection") was opened and closed by its
  author 29 seconds later without comments or a merge, and no issue exists;
  the default branch still has the bug. Nothing found for the lost wakeup.
  Not reported.

### 12. asyncpg 0.32.0: a connection lost during `release` raises `InternalClientError`

- **Library:** [asyncpg](https://pypi.org/project/asyncpg/) 0.32.0 (latest
  release on 2026-11-19; `pool.py` is unchanged on the upstream default
  branch), `Pool` with `pool.fetchval` and `async with pool.acquire()`.
- **Reproduction:** [`findings/asyncpg_pool.py`](../findings/asyncpg_pool.py)
  (two plain-asyncio reproductions against a local server, plus the SimLoop
  harness). The server is [`findings/_pg.py`](../findings/_pg.py), a small
  PostgreSQL wire-protocol simulator that asyncpg drives unmodified: startup,
  prepared `SELECT $1::text` that echoes its parameter after a random delay,
  the pool's reset query, and `CancelRequest`.
- **Seeds:** 4 workers, 10 operations each, half `pool.fetchval` and half a
  two-query `async with pool.acquire()` block, 30% under a random
  `wait_for` timeout (asyncpg answers each with a `CancelRequest`),
  `max_size` 1-3, 1% TCP resets and random server crashes, then a recovery
  check; `strict_cancellation=True`. Of seeds 0-499: default scheduler 2
  failures (seeds 55 and 397, both the cosmetic log below); pct 0; `scheduler="fifo",
  timer_jitter=0.001` 4 failures (seed 440 the `InternalClientError`;
  seeds 61, 81 and 253 the log). Reply integrity held on every seed (each
  query got its own parameter back), and the pool recovered after the
  faults on every seed.
- **Property violated:** releasing a connection whose socket has died
  either succeeds or raises one of asyncpg's documented connection errors.
- **Explanation:** `Pool.release` checks that the proxy is still attached
  (`connection._con is not None`) and then awaits
  `asyncio.shield(holder.release(timeout))`, which runs the holder's
  `release` in a new task. If the socket fails in between (a reset seen by
  the transport, or the server going away), asyncio has already queued the
  protocol's `connection_lost`, which runs before the shielded task starts:
  `Connection._cleanup` calls `holder._release_on_close()`, which marks the
  holder free (`_in_use = None`) and returns it to the pool. The task then
  hits the first check in `PoolConnectionHolder.release` and raises
  `InternalClientError: PoolConnectionHolder.release() called on a free
  connection holder`. The very next check in that method handles a closed
  connection gracefully (`if self._con.is_closed(): terminate(); return`),
  so the abort path is anticipated but the order of the two checks lets
  this one through. `InternalClientError` is documented as an asyncpg bug
  class; it subclasses neither `PostgresError` nor `InterfaceError`, so the
  usual `except` clauses for a dropped connection miss it and it reaches
  the application from the `async with` exit. The pool itself stays
  consistent: the holder was already returned and the next acquire works.

  A second, cosmetic effect: when the caller is cancelled while `release`
  runs the reset query and that query then fails because the connection
  dropped, nothing retrieves the shielded task's exception, and asyncio
  logs "Task exception was never retrieved" with
  `ConnectionDoesNotExistError`.

  On plain asyncio: aborting the transport inside `async with
  pool.acquire()` (exactly what asyncio's selector transport does on a
  socket error: it schedules `connection_lost` for the next iteration)
  makes the block's exit raise the `InternalClientError`, and the next
  `pool.fetchval` succeeds. With a server that stalls the reset query and
  then drops the connection, `wait_for(block, 0.02)` timing out during the
  release produces the "never retrieved" log.
- **Impact:** low. A dropped connection surfaces as an unexpected
  exception type instead of a `ConnectionDoesNotExistError`, which can
  turn a retryable failure into a crash in code that catches asyncpg's
  documented errors. No data is mixed up and the pool recovers.
- **Feasibility:** both reproduce with `asyncio.run` over real localhost
  sockets; the simulated run hits the free-holder case only under fifo
  scheduling with timer jitter, where a reset lands between the block's
  last query and its exit.
- **Upstream status:** searched MagicStack/asyncpg issues and pull requests
  for "free connection holder" and `InternalClientError` release on
  2026-11-19; no matching report (#1307 is a different pool problem). Not
  reported.

### 13. nats-py 2.17.0: `flush()` reports success during a reconnect, and `rtt()` raises `AssertionError`

- **Library:** [nats-py](https://pypi.org/project/nats-py/) 2.17.0 (latest
  release, 2026-11-17; `flush()` and `_send_ping()` are unchanged on the
  upstream default branch), `nats.aio.client.Client`.
- **Reproduction:** [`findings/nats_flush.py`](../findings/nats_flush.py)
  (two plain-asyncio reproductions against a local server, plus the SimLoop
  harness). The server is [`findings/_nats.py`](../findings/_nats.py), a
  small NATS server (INFO/CONNECT, PING/PONG, SUB/UNSUB, PUB/HPUB,
  MSG/HMSG, routing between connections) that keeps each connection's
  commands in the order it processed them.
- **Seeds:** a client with 4 workers, 10 operations each: requests (new
  and old style, some under `wait_for`), `flush()` and `rtt()` under random
  `wait_for` timeouts, and `publish` + `flush()` pairs; a second client
  answers the requests and collects the publishes; 1% TCP resets and
  random server crashes, `reconnect_time_wait=0.05`;
  `strict_cancellation=True`. Of seeds 0-299, default scheduler: 28 fail
  with a flush that succeeded during a reconnect (first seed 12), 4 with
  the `rtt()` assertion (first seed 34), and 125 with the known ordering
  issue below (first seed 0); `scheduler="fifo", timer_jitter=0.001`: 17
  (first seed 29), 8 (first seed 74) and 139 (first seed 5). A run stops
  at its first failure, so the commoner ordering issue hides some of the
  others. Request/reply integrity, per-publisher ordering without
  duplicates, and recovery after the faults held on every seed.
- **Property violated:** `flush()` "sends a ping to the server expecting a
  pong back ensuring what we have written so far has made it to the
  server" (its docstring), else it raises a timeout; `rtt()` raises one of
  nats-py's errors.
- **Explanation:**
  1. *flush during a reconnect.* `flush()` checks whether the read loop or
     the flusher task has finished, and if so flushes the pending buffer
     inline and returns without a PING. That fallback came with PR #853,
     for tasks killed from outside (SIGINT handling on Python < 3.11). But
     a reconnect cancels both tasks on purpose, so for the whole reconnect
     `flush()` takes the fallback, `_flush_pending()` sees the client is not
     connected and does nothing, and `flush()` returns success with the
     data still in the client's buffer. If the client then reconnects the
     data goes out later; if it gives up (`max_reconnect_attempts`) it is
     never sent, after a `flush()` that said it was.
  2. *rtt during an outage.* `rtt()` has no such check: it calls
     `_send_ping`, which starts with `assert self._transport, "Client.connect
     must be called first"`. After a failed reconnect attempt the client
     holds a fresh, unconnected transport, which is falsy, so `rtt()`
     raises a bare `AssertionError` (under `python -O` the assert is
     skipped and the write fails with `AttributeError` on the missing
     stream writer).
  3. *Known upstream (#1085, open PR #1063):* `_send_ping` writes PING
     straight to the socket while earlier commands still wait in the
     pending buffer for the flusher task, so the PONG can come back before
     the server has processed a PUB or SUB issued before `flush()`. The
     simulation shows it when the PUB's segment is delayed behind the PING
     (per-segment jitter, as with a lost and retransmitted segment); #1085
     reproduces it against a real server.

  On plain asyncio, with the only server gone and `max_reconnect_attempts=3`:
  `publish()` then `flush(timeout=2)` returns in microseconds, the client
  then closes after its reconnect attempts, and the message never reached
  the server; `rtt(timeout=1)` called after the first failed attempt raises
  `AssertionError: Client.connect must be called first`.
- **Impact:** moderate for (1): code that uses `flush()` as a delivery
  checkpoint (before acknowledging upstream work, before shutting down)
  gets a false success during an outage. Low for (2): an unexpected
  exception type from a health-check call during an outage.
- **Feasibility:** both reproduce with `asyncio.run` over real localhost
  sockets.
- **Upstream status:** searched nats-io/nats.py issues and pull requests
  on 2026-11-19 (flush with reconnect/reconnecting/outage, rtt,
  `AssertionError`, the assertion text). Nothing matches (1) or (2):
  #1061 and PR #1075 cover a PING written before CONNECT during a
  reconnect, and PR #1063 reroutes `_send_ping` through the pending
  buffer, but `flush()` returns before reaching `_send_ping` here; the
  PR's diff could not be fetched to check further. (3) is #1085. Not
  reported.

### 14. circuitbreaker 2.1.3 and aiocircuitbreaker 2.0.0: a cancelled call counts as a success

- **Library:** [circuitbreaker](https://pypi.org/project/circuitbreaker/)
  2.1.3 (latest release; `__exit__` is unchanged on the upstream default
  branch) and its asyncio fork
  [aiocircuitbreaker](https://pypi.org/project/aiocircuitbreaker/) 2.0.0
  (latest release, 2022), `CircuitBreaker.decorate` on a coroutine function.
- **Reproduction:** [`findings/circuit_breakers.py`](../findings/circuit_breakers.py)
  (a model-based harness shared by four breaker libraries, plus a
  plain-asyncio reproduction for these two).
- **Seeds:** 4 workers, 30 calls each, every call under a random
  `wait_for` timeout (5-100 ms), a backend that switches between failing in
  1-10 ms, hanging and working, `failure_threshold=3`,
  `recovery_timeout=1`. "Once 3 calls in a row have failed, calls in the
  next 0.5 s do not reach the backend" fails on seeds 4, 63, 74, 80 and 94
  of 0-99 for both libraries in every simulated time zone; on 23 of seeds
  0-499 with the default scheduler (first seed 4) and on 30 with
  `scheduler="fifo", timer_jitter=0.001` (first seed 5). With `__exit__`
  patched to ignore `CancelledError`, all 1000 pass.
- **Property violated:** a circuit breaker counts failures and opens after
  `failure_threshold` of them; a call that never completed is not evidence
  that the backend works.
- **Explanation:** the guarded call runs inside `with self:`, and
  `__exit__` records a failure only for an exception matching
  `expected_exception` (an `Exception` subclass) and calls `reset()` for
  everything else. `CancelledError` is a `BaseException`, so when the
  caller's timeout (`wait_for`, `asyncio.timeout`, a cancelled `TaskGroup`)
  cancels a guarded call, the breaker resets: the failure count goes to 0
  and the state to closed. Two consequences: a timeout in between failures
  keeps the breaker from ever reaching the threshold, and a half-open trial
  call that times out (the usual symptom of a backend that is still sick)
  closes the circuit as if the trial had succeeded, sending full traffic
  to the backend.

  On plain asyncio: two failures open a breaker with `failure_threshold=2`;
  after the recovery timeout it is half-open; the trial call, cancelled by
  `wait_for(..., 0.05)` against a backend that hangs, leaves it `closed`.
- **Feasibility:** reproduces with `asyncio.run`; the simulated failures
  also occur under `fifo` with timer jitter.
- **Upstream status:** searched fabfuel/circuitbreaker issues and pull
  requests for cancel, timeout and half-open on 2026-11-19: #55 asks about
  half-open behaviour (concurrent trial calls) and PR #71 adds consecutive
  half-open successes, neither about cancellation. The aiocircuitbreaker
  repository listed on PyPI could not be searched (the search API rejects
  it). Not reported.

### 15. aiobreaker 1.2.0: the reopen time mixes UTC and local time

- **Library:** [aiobreaker](https://pypi.org/project/aiobreaker/) 1.2.0
  (latest release, 2021; the same code is on the upstream default branch).
- **Reproduction:** [`findings/circuit_breakers.py`](../findings/circuit_breakers.py),
  run with `SimConfig(timezone=...)`.
- **Seeds:** the harness of #14 passes on all 100 seeds with
  `timezone=0` and fails on all 100 with `timezone=-5` ("reported a
  time to reopen of 18000.99885 s (recovery timeout 1 s)") and with
  `timezone=1` (`TypeError: unsupported operand type(s) for -:
  'NoneType' and 'datetime.datetime'`), on the first rejection.
- **Property violated:** a rejection reports when the breaker will try
  again (`CircuitBreakerError.reopen_time`, `time_remaining`,
  `CircuitBreaker.time_until_open`, `sleep_until_open`).
- **Explanation:** `open()` stores `opened_at = datetime.utcnow()`, and the
  open state's timeout check also uses `utcnow()`, so the breaker itself
  reopens on time. But `opens_at` compares `opened_at + timeout` with
  `datetime.now()`, local time, and `time_remaining`/`time_until_open`
  subtract `datetime.now()`. West of UTC (UTC-5) the reopen time is five
  hours late and `sleep_until_open()` sleeps five hours too long; east of
  UTC (UTC+1) `opens_at` decides the timeout has already passed and returns
  `None`, so the error carries `reopen_time=None` and `time_remaining`
  raises `TypeError`. Only hosts running in UTC see correct values.

  On plain asyncio with `TZ=America/New_York`, the first rejection's
  `time_remaining` is `5:00:01.99`; with `TZ=Europe/Berlin` it raises the
  `TypeError`.
- **Impact:** low to moderate: code that backs off using the reported
  time either stalls for hours or crashes, depending on the host's zone.
- **Feasibility:** deterministic; depends only on the host's time zone.
- **Upstream status:** arlyon/aiobreaker has 11 issues and 7 pull requests;
  none mention time zones, `utcnow` or the reopen time (#2 added the
  remaining time). Not reported.

### 16. purgatory 3.0.2: every rejection grows the same exception's traceback

- **Library:** [purgatory](https://pypi.org/project/purgatory/) 3.0.2
  (latest release, 2026-11-11; `raise self` is unchanged on the upstream
  default branch), `AsyncCircuitBreakerFactory`.
- **Reproduction:** [`findings/circuit_breakers.py`](../findings/circuit_breakers.py).
- **Seeds:** the harness of #14 fails on every seed of 0-99 in every
  simulated zone: "a rejection's traceback has 50 frames", ten rejections
  after the circuit opened.
- **Property violated:** an exception raised to a caller describes that
  call; memory use does not grow with the number of rejected calls.
- **Explanation:** the open state is itself an exception (`class
  OpenedState(State, Exception)`), and `handle_new_request` rejects a call
  with `raise self`. Raising an exception instance again prepends the new
  frames to its existing `__traceback__`, so every rejection while the
  circuit is open adds five frames to the one shared object. Outside a
  simulation, 2,000 rejections give a 10,000-frame traceback. The chain
  keeps every frame, and their local variables, alive until the circuit
  closes, and logging the exception prints all of it.
- **Impact:** moderate under load: a busy service with a long `ttl`
  accumulates frames (and the requests they reference) for every rejected
  call while a dependency is down, which is when it can least afford it.
- **Feasibility:** deterministic, on any event loop.
- **Upstream status:** mardiros/purgatory has no issues and 62 pull
  requests, none about this. Not reported.

### 17. aio-throttle 1.7.0: cancelled waiters still count as queued; the random-reject quota is inverted

- **Library:** [aio-throttle](https://pypi.org/project/aio-throttle/) 1.7.0
  (latest release, 2022; `internals.py` and `quotas.py` are unchanged on the
  upstream default branch, anna-money/aio-throttle).
- **Reproduction:** [`findings/aio_throttle_queue.py`](../findings/aio_throttle_queue.py)
  (the SimLoop harness, a plain-asyncio reproduction and a direct check of
  the quota).
- **Seeds:** 6 workers, 15 requests each, random priorities, each request
  under a random `asyncio.timeout` (5-200 ms), `Throttler(capacity_limit=2,
  queue_limit=2)`, `strict_cancellation=True`. "A request is rejected for a
  full queue only if `queue_limit` callers really wait (or, for `LOW`
  priority, if any caller waits)" fails on 298 of seeds 0-299 with the
  default scheduler (first seed 0) and on 299 with `scheduler="fifo",
  timer_jitter=0.001` (first seed 0).
- **Property violated:** `queue_limit` bounds how many callers wait for a
  slot; a request is rejected for a full queue only when that many wait.
- **Explanation:**
  1. *Stale waiters.* `LifoSemaphore.acquire` appends a future to
     `_waiters` and awaits it; a waiter that is cancelled (its caller's
     timeout, a client disconnect) cancels its future but leaves it in the
     list. `waiting` is `len(_waiters)`, and `Throttler._check_queue`
     rejects when `waiting >= queue_limit` with no free slot, and rejects
     every `LOW` priority request when `waiting > 0`. A release pops
     futures from the top of the stack until it finds a live one, so stale
     futures below a live waiter stay counted while the throttler is busy:
     requests are turned away with `REJECTED_DUE_TO_FULL_QUEUE` while fewer
     callers wait than the limit allows (and `stats.queue_size` reports
     them). In the failing runs a request was rejected with 1 live waiter
     and 2 counted.
  2. *Inverted quota.* `RandomRejectThrottleQuota.can_be_accepted` returns
     `self._random.random() < self._reject_probability`, which accepts with
     the reject probability: `RandomRejectThrottleQuota(0.1)` rejected
     89.6% of 10,000 requests. (Its constructor's error message also names
     `MaxFractionCapacityQuota`.)

  On plain asyncio: with `capacity_limit=1, queue_limit=1` and the slot
  held, a second caller that waits 10 ms and is cancelled leaves the next
  request rejected with "rejected due to full queue", with 0 live waiters
  and 1 counted.
- **Impact:** moderate for (1) in a server that uses the throttler as
  admission control (its aiohttp middleware): after client timeouts it
  sheds load it has room for, and starves `LOW` priority traffic. (2)
  makes the quota do the opposite of its name.
- **Feasibility:** both reproduce on plain asyncio.
- **Upstream status:** anna-money/aio-throttle has 2 issues and 242 pull
  requests (dependency bumps, CI, CODEOWNERS); none about either problem.
  Not reported.

### 18. aiochannel 1.4.1: `put()` on a closed, full channel raises `ChannelFull`

- **Library:** [aiochannel](https://pypi.org/project/aiochannel/) 1.4.1
  (latest release, 2026-07-18; `put_nowait` is unchanged on the upstream
  default branch).
- **Reproduction:** [`findings/aiochannel_close.py`](../findings/aiochannel_close.py).
- **Seeds:** 3 producers and 3 consumers on a `Channel(maxsize=1-3)`, every
  `put`/`get` under a random `asyncio.timeout`, the channel closed at a
  random time while producers run. "A put after the close raises
  `ChannelClosed`" fails on 88 of seeds 0-499 with the default scheduler
  (first seed 10) and on 94 with `scheduler="fifo", timer_jitter=0.001`
  (first seed 11). With the harness also accepting `ChannelFull` after a
  close, every item put was consumed exactly once and `join()` returned on
  all 1000 seeds.
- **Property violated:** `put()`'s docstring: "If the channel is closed or
  closing, raise ChannelClosed."
- **Explanation:** `put()` waits while `full() and not closed`, so once the
  channel is closed it falls through to `put_nowait()`, which checks
  `full()` before the closed flag and raises `ChannelFull` while undrained
  items fill the buffer. A blocking `put` never raises `ChannelFull`
  otherwise, so a producer written as `except ChannelClosed: break` crashes
  instead of stopping. On plain asyncio: `Channel(1)`, one `put`, `close()`,
  then `await put(...)` raises `ChannelFull`.
- **Impact:** low; the error type is wrong, nothing is lost.
- **Feasibility:** deterministic on plain asyncio.
- **Upstream status:** tudborg/aiochannel has 9 issues and 26 pull
  requests; #3 (an `InvalidStateError` during close) is fixed and nothing
  matches this. Not reported.

### 19. arq 0.28.0: abort requests never expire; a shutdown mid-job locks the job and skips cleanup

- **Library:** [arq](https://pypi.org/project/arq/) 0.28.0 (latest release;
  `worker.py` is identical on the upstream default branch,
  python-arq/arq), with redis-py 5.3.1 (arq requires `redis<6`).
- **Reproduction:** [`findings/arq_worker.py`](../findings/arq_worker.py)
  (two SimLoop harnesses against the simulated Redis server in
  `findings/_resp.py`, plus plain-asyncio reproductions that run
  `Worker.run()` against the same server on its own thread). It pins arq's
  redis-py in `findings/requirements-arq_worker.txt`, which CI installs in a
  separate environment. The harness applies redis-py's later fix to
  `Pipeline.reset` (see the note below) so that bug does not mask arq's.
- **Seeds:**
  1. A worker with `allow_abort_jobs=True` and `keep_result=0` runs a job
     with `_job_id="report"`; the client then calls `abort()` on it (which
     returns `False`), waits 61-600 s and enqueues a new job with the same
     id. "The new job runs" fails on all of seeds 0-19 with the default
     scheduler and with `scheduler="fifo", timer_jitter=0.001`. With the
     cleanup range corrected (`min=-inf, max=now_ms - 60_000`) it passes on
     all 40.
  2. Three workers (`max_jobs=2`) run 20 short jobs while one of them is
     stopped and replaced every 0.1-1 s, the way arq stops on a signal.
     "`Worker.close()` does not raise, and every job finishes within 30 s"
     fails on 22 of seeds 0-99 with the default scheduler (first seed 0, a
     job locked for 280 more seconds) and on 15 with `scheduler="fifo",
     timer_jitter=0.001` (first seed 7, `close()` raised
     `CancelledError`).
- **Property violated:** (1) an abort request expires after
  `abort_job_max_age` ("age of items in the abort_key sorted set after which
  they're deleted"); (2) arq's documented "pessimistic execution": "If the
  worker shuts down, the job will be cancelled immediately and will remain
  in the queue to be run again when the worker starts up again (or run by
  another worker which is still running)", and a clean shutdown runs
  `on_shutdown`.
- **Explanation:**
  1. *Abort requests never expire.* `Job.abort()` adds the job id to the
     `arq:abort` sorted set, scored with the current time in milliseconds.
     Each poll, `_cancel_aborted_jobs` means to drop old entries but calls
     `zremrangebyscore(abort_jobs_ss, min=timestamp_ms() + abort_job_max_age,
     max=inf)`: the range lies in the future (and adds 60 to a millisecond
     timestamp, where the constant is commented as seconds), so it never
     matches. Entries only leave the set when a job with that id finishes.
     An abort for a job that already finished, or was never found, stays
     forever: a job enqueued later under the same `_job_id` is "aborted
     before start", minutes or days later, and every worker reads the whole
     growing set on every poll. On plain asyncio a real worker kept an
     abort request scored ten minutes in the past after several polls.
  2. *Shutdown mid-job.* On a signal, `handle_sig` cancels the worker's
     tasks, and only the `wait_for` around the job function itself catches
     the cancellation (to re-queue the job). Three other windows don't:
     - cancelled while the reply to the `in-progress` claim
       (`PSETEX arq:in-progress:<id>` in `start_jobs`) is on its way, the
       worker never starts the job but leaves the claim in place;
     - cancelled during the job's first round trip in `run_job` (`GET`
       job, `INCR` retries), the job task raises `CancelledError` and also
       leaves the claim (with the try counted);
     - cancelled while the result is being recorded, inside
       `asyncio.shield(self.finish_job(...))`, the job task raises
       `CancelledError` and the recording task is left pending ("Task was
       destroyed but it is pending!" when `run()` returns).

     A job task that raised makes `Worker.close()` raise as well, since its
     `asyncio.gather(*self.tasks.values())` re-raises: `Worker.run()` then
     raises `CancelledError`, `on_shutdown` never runs, the pool is not
     closed and the health-check key stays. A claim left in place stops
     every worker from running the job until it expires, after
     `job_timeout + 10` s (310 s by default).

     On plain asyncio, `Worker.run()` with SIGINT sent while each reply is
     in flight: during the claim, `run()` returned but the job was not run
     and stayed claimed for 310 s; during the first round trip, `run()`
     raised `CancelledError`, `on_shutdown` did not run, the health-check
     key stayed and the job stayed claimed for 310 s; while recording the
     result, `run()` raised `CancelledError` and `on_shutdown` did not run.
- **Also seen (redis-py 5.3.1, fixed upstream):** `Pipeline.reset`, when
  cancelled while reading its `UNWATCH` reply, catches only
  `ConnectionError`, so the connection is never returned to the pool and
  the pipeline is garbage-collected holding it ("Unclosed Pipeline client").
  arq hits this when a worker stops during `start_jobs`. redis-py 8.1.0
  handles the cancellation, but arq's `redis<6` pin keeps its users on the
  old code. Not counted as an arq bug.
- **Impact:** (1) moderate: re-enqueueing a job id after aborting it (for
  example, to replace a job) silently cancels the new job, at any time
  later; the set also grows without bound. (2) moderate: rolling restarts
  (Kubernetes, deploys) stop workers while jobs start and finish all the
  time; affected jobs are delayed by about five minutes by default (longer
  with a larger `job_timeout`), and shutdown hooks do not run.
- **Feasibility:** all of it reproduces on plain asyncio; (1) is
  deterministic.
- **Upstream status:** issue #485 ("How to safely re-enqueue an aborted
  job?", open) reports the symptom of (1): a re-enqueued job is aborted
  "if the aborted job is not found"; it does not identify the cause. Nothing
  found for (2) (#343 is about workers that crash). Not reported.

### 20. grpclib 0.4.9: a stream reset before its handler starts leaks the stream and its flow-control window (known upstream)

- **Library:** [grpclib](https://pypi.org/project/grpclib/) 0.4.9 (latest
  release), server and client, with a JSON codec instead of protobuf.
- **Reproduction:** [`findings/grpclib_reset_leak.py`](../findings/grpclib_reset_leak.py)
  (the SimLoop harness and a plain-asyncio reproduction).
- **Seeds:** 4 clients, 15 unary calls each with 0, 100 or 70,000-byte
  payloads, under the call's own timeout (1-100 ms or none) or an outer
  `asyncio.wait_for` (0-50 ms). "Once every call has finished, the server
  holds none of their HTTP/2 streams" fails on 26 of seeds 0-99 with the
  default scheduler (first seed 15) and on 16 with `scheduler="fifo",
  timer_jitter=0.001` (first seed 0). With the stream released for a
  handler task cancelled before its first step, it passes on all 200.
- **Property violated:** a finished call releases its stream, and the
  server acknowledges the DATA it received, so the connection's receive
  window is not used up.
- **Explanation:** when `HEADERS`, `DATA` and `RST_STREAM` for one stream
  are handled in the same `data_received` call (a client that sends a
  request and gives up straight away), `Handler.accept` creates the request
  handler task and `Handler.cancel` cancels it before it has run. asyncio
  never enters a coroutine cancelled that early, so `request_handler`'s
  `finally: release_stream()` does not run: the stream stays in
  `EventsProcessor.streams` and the DATA it buffered is never acknowledged.
  Each such call takes its request size out of the connection's receive
  window for good. On plain asyncio, 64 calls that each sent 60 KB and gave
  up at once all left their stream registered, and the window fell from
  4,194,272 to 351,776 bytes; once it reaches zero the connection carries
  no more request data.
- **Impact:** high for long-lived connections with clients that cancel
  calls: the server's memory grows and the connection ends up accepting
  streams but no data.
- **Feasibility:** deterministic on plain asyncio.
- **Upstream status:** already known: vmagamedov/grpclib#217 (open,
  2026-08-10) describes exactly this, and PR #218 (open) fixes it.
  Rediscovered here by a generic check (no stream outlives its call).

### 21. aiomysql 0.3.2: releasing a closed connection wakes no pool waiter, so `acquire()` hangs

- **Library:** [aiomysql](https://pypi.org/project/aiomysql/) 0.3.2 (latest
  release, 2025-10-22; `Pool.release` is unchanged on the upstream default
  branch), with PyMySQL 1.2.4.
- **Reproduction:** [`findings/aiomysql_pool_wakeup.py`](../findings/aiomysql_pool_wakeup.py)
  (the SimLoop harness and a plain-asyncio reproduction), against the
  simulated MySQL server in [`findings/_mysql.py`](../findings/_mysql.py)
  (handshake, `COM_QUERY` result sets, `COM_PING`, transaction status).
- **Seeds:** 6 workers share `create_pool(minsize=0, maxsize=2)` and run 10
  `SELECT SLEEP(t), '<token>'` queries each, 30% of them under a 0-50 ms
  `asyncio.wait_for`, 5% leaving a transaction open. "Every worker finishes
  within 60 s" (a fault-free run takes about 1.5 s) fails on 4 of seeds
  0-199 with the default scheduler (first seed 36) and on 9 with
  `scheduler="fifo", timer_jitter=0.001` (first seed 14); in each, a
  worker waits in `acquire()` while the pool holds 0 of 2 connections.
  With `release` waking a waiter in those cases too, all 400 runs pass, and
  so does asyncmy 0.2.18 (whose pool notifies on every release) on the same
  harness. Every reply matched its token and the pool never exceeded
  `maxsize`.
- **Property violated:** an `acquire()` waiting for a full pool gets a
  connection once the pool has room.
- **Explanation:** `Pool.release` schedules `_wakeup()` (a `notify()` on
  the pool's condition) only when it returns the connection to the free
  list. Two other paths remove the connection from `_used` and return
  without notifying: a connection that is already closed, which is what a
  cancelled query leaves (aiomysql closes the connection when a read is
  cancelled) as well as a broken one; and a connection still in a
  transaction, which `release` closes. The pool shrinks, but a caller
  blocked in `Condition.wait()` inside `_acquire` is not told, and only the
  next normal release wakes it. On plain asyncio, with `maxsize=1`, a
  second caller waiting in `acquire()` was still waiting 2 s later, with
  the pool empty, after the holder's query was cancelled by `wait_for`, and
  likewise after the holder released a connection with a transaction open.
- **Impact:** high where queries run under timeouts (any web handler with
  a request timeout): once the cancelled holders outnumber normal releases
  at the right moment, callers without their own timeout hang forever and
  the others time out, although the pool is empty.
- **Feasibility:** deterministic on plain asyncio.
- **Upstream status:** no issue describes this cause. #553 ("Pool.acquire()
  hanging after a while", closed, 2020) reports the symptom without a
  cause. Open PR #982 (2024) proposes to "notify connection pool in any
  case, even if an exception happened during the connection", which is
  about failed connects in `_fill_free_pool`; we could not see whether its
  diff touches `release`. Not reported.

### 22. aiohttp 3.14.5: graceful shutdown cancels handlers one `shutdown_timeout` late and does not wait for them

- **Library:** [aiohttp](https://pypi.org/project/aiohttp/) 3.14.5 (latest
  release; `RequestHandler.shutdown` is unchanged on the upstream default
  branch).
- **Reproduction:** [`findings/aiohttp_graceful_shutdown.py`](../findings/aiohttp_graceful_shutdown.py)
  (the SimLoop harness and a plain-asyncio reproduction).
- **Seeds:** an `AppRunner(app, shutdown_timeout=0.5)` server; 4 clients
  send 10 requests each, whose handlers take 0-0.2 s or 0-3 s and spend
  50 ms cleaning up when cancelled; `runner.cleanup()` starts at a random
  time (0.3-1.5 s). "Handlers still running `shutdown_timeout` after
  shutdown starts are cancelled then, and `on_cleanup` runs after they
  have finished" fails on 95 of seeds 0-99 with the default scheduler
  (first seed 0) and on 94 with `scheduler="fifo", timer_jitter=0.001`
  (first seed 0): handlers were cancelled 1.0 s after shutdown began and
  `on_cleanup` ran while 2-3 of them were still running. (The passing seeds
  had no long handler in flight.)
- **Property violated:** the documented graceful shutdown: "4. Wait a
  short time for running handlers to complete ... adjustable with
  `shutdown_timeout`. 5. Close any remaining connections and cancel their
  handlers. It will wait on the canceling handlers for a short time, again
  adjustable with `shutdown_timeout`. 6. Call the `Application.on_cleanup`
  signal."
- **Explanation:** `RequestHandler.shutdown(timeout)` first waits up to
  `timeout` for the handler. Its "cancel" step then calls
  `self._current_request._cancel(CancelledError())`, which only sets an
  exception on the request's body stream: a handler that is not reading the
  body (most handlers, once they have the request) keeps running, and
  `shutdown` waits for it, shielded, up to `timeout` again. Only then does
  it call `self._task_handler.cancel()`, and it returns without waiting
  for the task, so `AppRunner.cleanup()` goes on to `on_cleanup` and
  returns while the cancelled handlers are still running their `except` and
  `finally` blocks. On plain asyncio with `shutdown_timeout=0.5`, a handler
  sleeping 10 s was cancelled 1.0 s after `cleanup()` was called;
  `on_cleanup` ran and `cleanup()` returned at 1.0 s, and the handler
  finished its 0.1 s of cleanup at 1.1 s.
- **Impact:** moderate: a server takes twice the configured grace period
  before cancelling handlers (120 s with the default of 60 s), and
  `on_cleanup` closes database pools and sessions while cancelled handlers
  are still using them to roll back or release.
- **Feasibility:** deterministic on plain asyncio.
- **Upstream status:** PR #7718 (2023) introduced this shutdown sequence;
  no issue found about the late cancellation or the missing wait (#8179 and
  #8448 are about background tasks and cleanup contexts). Not reported.

### 23. aiomisc 18.0.40: `PoolBase` loses capacity for good and hands out recycled instances

- **Library:** [aiomisc](https://pypi.org/project/aiomisc/) 18.0.40 (latest
  release), `aiomisc.PoolBase`, the base class for user-defined pools.
- **Reproduction:** [`findings/aiomisc_pool.py`](../findings/aiomisc_pool.py)
  (the SimLoop harness, a patched pool as a control, and plain-asyncio
  reproductions).
- **Seeds:** a `PoolBase` subclass with `maxsize=2` hands fake connections
  to 6 workers doing 10 acquire/use/release rounds each. One workload per
  kind of trouble; seeds 0-99, default scheduler / `scheduler="fifo",
  timer_jitter=0.001`:
  - 30% of connects fail: 5 / 11 runs hang (first seeds 74 / 2) with every
    worker waiting in `acquire()` and no connection open;
  - 5% of health checks raise: 94 / 92 runs fail (first seed 0 / 0):
    `acquire()` returns a closed connection, a connection is closed while
    in use, or the pool holds 3-4 connections;
  - 30% of rounds under a 0-20 ms `asyncio.wait_for`: 4 / 11 runs hang
    (first seeds 31 / 34) with every worker waiting and both slots taken by
    connections nobody holds;
  - `recycle=0.05`: 5 / 9 runs hang (first seeds 63 / 16) with a worker
    waiting and no connection open.

  The same workloads without faults pass on all 200 runs, and with the four
  fixes below applied to `PoolBase` every workload passes all 200.
- **Property violated:** an acquired instance is usable and not shared, the
  pool holds at most `maxsize` instances, and an `acquire()` is served once
  the pool has room.
- **Explanation:** four separate defects in `PoolBase.__acquire` and its
  helpers:
  - `__create_new_instance` takes a semaphore slot and then awaits
    `_create_instance()`. If that raises, the slot is never given back. After
    `maxsize` failed connects (a database that is down at startup) the
    semaphore stays locked with no instances, and every later `acquire()`
    waits on the empty instance queue for good, even after the database is
    back.
  - When `_check_instance()` raises, `__acquire` logs it and recycles the
    instance (frees its slot and queues it for `_destroy_instance`), then
    falls through and returns it to the caller anyway. The recycler closes it
    under the caller, and the caller's release puts it back in the queue,
    where the next caller gets the closed instance; meanwhile the freed slot
    has been used to create a replacement, so the pool holds more than
    `maxsize`. (The `False` branch does it right: it recycles and retries.)
  - A caller cancelled during `_check_instance()` has already taken the
    instance off the queue, and nothing puts it back or recycles it; one
    cancelled during `_create_instance()` keeps the slot. Each such
    cancellation permanently removes one instance from circulation.
  - `__release` recycles an instance past its `recycle` deadline, which
    frees a slot, but a caller already waiting in `self._instances.get()`
    only checked for a free slot before it started waiting, so it is not
    woken: it waits until some later caller creates an instance.

  On plain asyncio with `maxsize=1`: after one failed connect, the next
  `acquire()` is not served within 1 s although connects now succeed; when
  the check raises once, the caller holds an instance that is closed 10 ms
  later, and the next caller gets that same closed instance; after one
  acquire that times out during a 1 s check, the next `acquire()` is not
  served within 1 s; with `recycle=0.1`, a caller waiting when the holder
  releases the expired instance is not served within 1.3 s.
- **Impact:** moderate to high for code built on `PoolBase`: a brief outage
  at startup, or `maxsize` timed-out acquires, leaves a pool that never
  serves again, and a flaky health check hands out connections that are
  being closed.
- **Feasibility:** each is deterministic on plain asyncio (the scripts
  above), and each hang also reproduces with `scheduler="fifo"` plus timer
  jitter.
- **Upstream status:** searches of aiokitchen/aiomisc issues and pull
  requests for "pool" and "recycle" on 2026-11-19 found nothing about these
  (the only pool pull requests are the original implementation, #64, and
  #65, "pool fixes", both closed). Not reported.

### 24. generic-connection-pool 0.8.1: the asyncio pool never disposes dead connections, leaks slots to cancelled acquires, and `close()` spins

- **Library:** [generic-connection-pool](https://pypi.org/project/generic-connection-pool/)
  0.8.1 (latest release), `generic_connection_pool.asyncio.ConnectionPool`.
- **Reproduction:** [`findings/gcpool_leaks.py`](../findings/gcpool_leaks.py)
  (the SimLoop harness, a patched pool as a control, and plain-asyncio
  reproductions).
- **Seeds:** a pool with `min_idle=1, max_size=2, total_max_size=4` hands
  fake connections to 6 workers doing 10 acquire/use/release rounds each.
  Seeds 0-49, default scheduler / `scheduler="fifo", timer_jitter=0.001`
  (both with `step_time=1e-6`, see below):
  - no faults, two endpoints, 30% failed connects, 10 ms acquire timeouts,
    short idle and lifetime limits, and the background collector: all pass;
  - aliveness checks that fail 10% of the time: 50 / 50 runs fail (first
    seed 0 / 0): up to 10 connections are open at once, and connections are
    still open after `close()`;
  - two endpoints, 30% of rounds under a 0-20 ms `asyncio.wait_for`: 43 / 47
    runs fail (first seed 0 / 0): every worker waits in `acquire()` for 60 s,
    or slots are still acquired or reserved after every worker finished.

  With the fixes below applied, both failing workloads pass all 100 runs.
- **Property violated:** every connection the pool drops is disposed, at
  most `total_max_size` connections exist, a cancelled acquire gives back
  what it took, and `close(timeout)` closes every connection; its
  documented `timeout` is the time "after which the pool closes all
  connection despite they are released or not".
- **Explanation:**
  - When `check_aliveness()` returns `False`, `_acquire_connection` detaches
    the connection, decrements the pool size and calls
    `on_connection_dead()`, but never `dispose()`. The connection stays
    open unless the manager's `on_connection_dead()` closes it, which the
    documentation does not ask for and none of the bundled managers (socket,
    SSL, Unix socket, psycopg2, asyncpg) does, and the pool, which no longer counts it, opens a
    replacement.
  - `_acquire_connection` undoes its bookkeeping in `except Exception`
    blocks, and `CancelledError` is not an `Exception`. An acquire cancelled
    during `check_aliveness()` leaves the connection in the endpoint's
    acquired set; one cancelled during `create()` leaves the endpoint slot
    reserved and the pool-wide size incremented (`_create_connection` has
    the same `except Exception`). Each such cancellation removes a slot for
    good; with `max_size` of them, every acquire waits until its timeout.
  - `_close_connections` loops `while self._pools.get_size() != 0`, taking
    only free connections. A connection that is held, or a slot leaked as
    above, keeps the size above zero, so it loops without sleeping; the
    timeout only bounds lock acquisition, so it eventually raises
    `TimeoutError` with the held connections left open, and with no
    timeout it never returns.

  On plain asyncio with `max_size=1`: a connection whose check failed was
  still open after `close()`; after one acquire cancelled during a 1 s
  aliveness check, and after one cancelled during a 1 s connect, the next
  acquire was not served within 1 s; `close(timeout=0.5)` with a connection
  held used 0.48 s of CPU in 0.5 s and raised `TimeoutError` without closing
  it, and `close()` used 0.99 s of CPU in its first second and did not
  return.
- **Impact:** moderate: connections that fail a health check leak (each an
  open socket or database session), timed-out requests drain the pool
  until it stops serving, and shutdown with a connection checked out burns
  a core.
- **Feasibility:** each is deterministic on plain asyncio (the scripts
  above). `step_time=1e-6` is needed because the pool polls with
  `Timer(0.0)`, which only expires once the clock has moved (`elapsed >
  timeout`); a real clock always has.
- **How it was found:** the harness first passed the idle and lifetime
  workloads vacuously: `ConnectionInfo` stamps `created_at` with
  `dataclasses.field(default_factory=time.monotonic)`, which kept the real
  clock under simulation, so nothing ever expired. SimLoop now also swaps
  clocks held in dataclass fields, class attributes, default arguments and
  closures; the expiry workloads then ran for real (and pass).
- **Upstream status:** the repository (dapper91/generic-connection-pool)
  has one issue ever, unrelated; its pull requests are the author's release
  merges ("fix: socket leak", #30, is in 0.8.1). Not reported.

### 25. databases 0.9.0: a transaction that fails or is cancelled never gives its connection back

- **Library:** [databases](https://pypi.org/project/databases/) 0.9.0
  (latest release, March 2024), tested with the asyncmy 0.2.18 backend; the
  code at fault is in `databases/core.py` and backend-independent.
- **Reproduction:** [`findings/databases_transaction_leak.py`](../findings/databases_transaction_leak.py)
  (the SimLoop harness, a patched `Transaction` as a control, and
  plain-asyncio reproductions).
- **Seeds:** `Database("mysql+asyncmy://...", max_size=2)` against the
  simulated MySQL server; 5 workers run 8 `SELECT SLEEP(t), '<token>'`
  queries each. Seeds 0-49, default scheduler / `scheduler="fifo",
  timer_jitter=0.001`:
  - without transactions, 30% of queries under a 0-30 ms `wait_for`, or
    0.3% TCP resets per segment: all pass;
  - the same inside `async with db.transaction()`: timeouts fail 47 / 43
    runs (first seed 0 / 0), resets fail 36 / 38 (first seed 1 / 0): a pool
    connection is still in use after every worker finished, or workers wait
    for a connection for 60 s.

  With `Transaction.start`, `commit` and `rollback` releasing the
  connection in a `finally`, both transaction workloads pass all 100 runs.
- **Property violated:** leaving `async with db.transaction()`, by any
  path, returns the connection to the pool.
- **Explanation:** `Transaction.commit()` and `rollback()` pop the
  transaction off the connection's stack, await the driver's commit or
  rollback, and only then call `Connection.__aexit__()`, which releases the
  connection when its counter reaches zero. If the driver call raises (the
  connection broke, or a cancelled query left it unusable) the release is
  skipped: the pool loses the slot for good, and the task's `Connection`
  (kept per task) keeps a counter that never returns to zero. `start()`
  has the same shape: it increments the counter (acquiring the connection)
  before awaiting `BEGIN`, and a cancellation or error there never undoes
  it. As a side effect, a caller that times out inside a transaction gets
  the driver's `InterfaceError` from the rollback instead of `TimeoutError`.

  On plain asyncio with `max_size=1`: when the connection drops inside a
  transaction (`OperationalError` 2006), when a 0.7 s `wait_for` expires
  during a 1 s query inside one (the caller gets `InterfaceError: Cancelled
  during execution`), and when a 0.1 s `wait_for` expires during a slow
  `BEGIN` (`TimeoutError`), another task's query is not served within 1 s
  afterwards.
- **Impact:** high for applications using it: every request that times out
  or loses its connection inside a transaction permanently removes a pool
  connection, until the application stops serving.
- **Feasibility:** deterministic on plain asyncio (the scripts above).
- **Upstream status:** the commit/rollback half is known: open pull
  request encode/databases#498 (2022), "Fix connection leak when
  transaction commit/rollback raise an exception"; it does not mention
  `start()` or cancellation. No release since 0.9.0. Rediscovered here from
  a generic "no connection stays in use" check. Not reported.

### 26. tortoise-orm 1.1.9: `in_transaction()` leaks its connection when `BEGIN` fails or is cancelled

- **Library:** [tortoise-orm](https://pypi.org/project/tortoise-orm/) 1.1.9
  (latest release), MySQL backend with asyncmy 0.2.18; the code at fault
  (`TransactionContextPooled`) is shared by the pooled backends.
- **Reproduction:** [`findings/tortoise_transaction_leak.py`](../findings/tortoise_transaction_leak.py)
  (the SimLoop harness, a guarded `begin()` as a control, and plain-asyncio
  reproductions).
- **Seeds:** `maxsize=2` against the simulated MySQL server; 5 workers run
  8 `SELECT SLEEP(t), '<token>'` queries each, each in its own task. Seeds
  0-49, default scheduler / `scheduler="fifo", timer_jitter=0.001`: without
  transactions, timeouts and 0.3% TCP resets both pass; inside
  `in_transaction()`, timeouts fail 29 / 18 runs (first seed 0 / 0) and
  resets 15 / 13 (first seed 0 / 9): a pool connection is still in use after
  every worker finished, or workers wait 60 s for one. With `begin()`
  guarded (release the connection and reset the context variable on any
  exception), all 200 transaction runs pass, so `BEGIN` is the only leaking
  path.
- **Property violated:** leaving `in_transaction()`, by any path, returns
  the connection to the pool.
- **Explanation:** `TransactionContextPooled.__aenter__` acquires a pool
  connection, points the `connections` context variable at the
  transaction, and awaits `begin()` with nothing around it. If `BEGIN`
  raises (the connection dropped) or the task is cancelled during it,
  `__aenter__` fails, so `__aexit__`, which releases the connection and
  resets the context variable, never runs. On plain asyncio with
  `maxsize=1`, after a 0.1 s timeout during a slow `BEGIN`, and after the
  connection drops on `BEGIN`: the failed task still sees a
  `TransactionWrapper` as its `default` connection, another task's query is
  not served within 1 s, and `Tortoise.close_connections()` does not return
  within 1 s (it waits for the lost connection).
- **Impact:** high under request deadlines: each deadline that lands in a
  `BEGIN` round trip loses a connection for good (the ORM uses
  `in_transaction()` internally too, e.g. `get_or_create`).
- **Feasibility:** deterministic on plain asyncio (the scripts above).
- **Upstream status:** known: open issue tortoise/tortoise-orm#2391
  (2026-11-01, cancellation during `begin()`), fix in open PR #2392.
  Rediscovered here from a generic "no connection stays in use" check; the
  failing-`BEGIN` trigger, the stale context variable and the hanging
  `close_connections()` are the same defect. Not reported.

### 27. broadcaster 0.3.1: concurrent first subscribers to a channel lose one of them

- **Library:** [broadcaster](https://pypi.org/project/broadcaster/) 0.3.1
  (latest release, August 2024), shown with the shipped `redis-stream://`
  backend over redis-py 8.1.0; the code at fault is `Broadcast.subscribe`
  and backend-independent.
- **Reproduction:** [`findings/broadcaster_subscribe_race.py`](../findings/broadcaster_subscribe_race.py)
  (the SimLoop harness, a locked `subscribe` as a control, and a
  plain-asyncio reproduction against a local `_resp.py` server).
- **Seeds:** `Broadcast("redis-stream://...")` against the simulated Redis
  server (which now has streams and a blocking `XREAD`). A publisher sends a
  numbered message to channels `a` and `b` every 5 ms; subscribers enter
  `broadcast.subscribe(ch)` four times each, wait for a message published
  after they joined, and leave; one more stays on channel `keep`
  throughout so the stream backend never runs out of streams (that is the
  separate, known encode/broadcaster#149). Seeds 0-49, default scheduler /
  `scheduler="fifo", timer_jitter=0.001`, `strict_cancellation=True`: one
  subscriber per channel passes all runs; three per channel fails 50 / 50
  (first seed 0 / 0), e.g. seed 0: `s0 round 0 on 'a': KeyError('a')`. With
  the bookkeeping in `subscribe` held under one `asyncio.Lock`, all 100 runs
  pass.
- **Property violated:** every task inside `async with
  broadcast.subscribe(ch)` receives what is published to `ch`, and leaves
  without an exception of the library's making.
- **Explanation:** `subscribe` checks `if not
  self._subscribers.get(channel)`, awaits `self._backend.subscribe(channel)`,
  and only then assigns `self._subscribers[channel] = {queue}`. A second
  task that arrives during that await also sees no subscribers, subscribes
  the backend again, and its assignment replaces the first task's set. The
  first task's queue is in no set, so it never receives an event; whichever
  of the two leaves second gets `KeyError` from the `finally` block, either
  from `set.remove` or because the first to leave already deleted the
  channel and unsubscribed the backend while the other was still inside.
  Any backend whose `subscribe` waits on I/O is affected: redis-stream
  (`XINFO STREAM`), Postgres (`LISTEN`; there the shared, unlocked asyncpg
  connection additionally raises "another operation is in progress", known
  as encode/broadcaster#22) and Kafka (waits for partition assignment), as
  well as custom `BroadcastBackend`s. The plain `redis://` backend happens
  not to suspend in `subscribe` once connected, so it does not show it.

  On plain asyncio, with a 10 ms server delay: two tasks enter
  `subscribe("news")` together and `"hello"` is published once both are
  in. Result: `{'first': "got nothing, then KeyError('news')", 'second':
  "got 'hello'"}`.
- **Impact:** medium: the typical use is one subscription per WebSocket
  client, so two clients joining a quiet channel within one round trip of
  each other is ordinary; one of them silently receives nothing and both
  can crash on disconnect.
- **Feasibility:** deterministic on plain asyncio (the script above); it
  needs only two tasks calling `subscribe` on the same new channel within
  one backend round trip.
- **Upstream status:** searched issues and pull requests for subscribe
  races, `KeyError` and concurrency: #142 (open) reports a `KeyError` from
  the same `finally` when the backend's `subscribe` raises, a different
  trigger; #22 (open) is the Postgres connection sharing. No report of
  this race found. Not reported.

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

### uvicorn 0.54.0: `lifespan.shutdown` is sent while cancelled request tasks and their connections are still open

- **Library:** [uvicorn](https://pypi.org/project/uvicorn/) 0.54.0 (latest
  release; `Server.shutdown` is unchanged on the upstream default branch),
  with Starlette 1.10.0.
- **Reproduction:** `plain_uvicorn()` in
  [`findings/aiohttp_graceful_shutdown.py`](../findings/aiohttp_graceful_shutdown.py)
  (plain asyncio).
- **Observed:** with `timeout_graceful_shutdown=0.5` and a handler that
  sleeps 10 s and spends 0.1 s cleaning up when cancelled, uvicorn cancels
  the request task 0.7 s after `should_exit` (on time, allowing for its
  0.1 s polling), then immediately runs the lifespan shutdown and returns
  from `serve()`. The handler finishes cleaning up 0.1 s later, and only
  then does the client get its 500 response, over a connection that was
  still open during the lifespan shutdown.
- **Why it is only suspected:** uvicorn documents only that after the
  timeout "the server will start terminating requests". The ASGI lifespan
  spec says `lifespan.shutdown` is "sent to the application when the server
  has stopped accepting connections and closed all active connections",
  which this contradicts, but whether uvicorn should wait for cancelled
  tasks (and how long) is a design question. The effect is the same as in
  aiohttp's #22: shutdown code that closes pools can run while cancelled
  handlers still use them.
- **Upstream status:** nothing found in Kludex/uvicorn issues or PRs about
  waiting for cancelled tasks before the lifespan shutdown. Not reported.

### pymongo 4.20.0: a `getMore` cut by a client-side timeout leaves its server cursor open

- **Library:** [pymongo](https://pypi.org/project/pymongo/) 4.20.0
  (latest release), `AsyncMongoClient`; the synchronous `Cursor` has the
  same code (it is generated from the async one) but was not run here.
- **Reproduction:** [`findings/pymongo_async_pool.py`](../findings/pymongo_async_pool.py)
  (`cursor_after_timeout()`, plus the cursor check in the pool harness),
  against the simulated MongoDB server in `findings/_mongo.py`.
- **Seeds:** one cursor, `batch_size=2`, a server that answers `getMore`
  after 0.2 s, and a 0.1 s limit, then `await cursor.close()`:
  `asyncio.wait_for` raises `TimeoutError` and `close()` sends
  `killCursors` (no server cursor left); `pymongo.timeout(0.1)` and
  `socketTimeoutMS=100` raise `NetworkTimeout` and `close()` sends nothing,
  leaving the cursor open on the server. Deterministic (seed 0). In the
  pool harness, server cursors the client had received were left open in
  11 of 100 runs with `pymongo.timeout()` jobs and 26 of 100 with 0.3% TCP
  resets (first seed 1 for both, default scheduler), and in none without
  faults or with `wait_for` timeouts.
- **What happens:** `AsyncCursor._send_message` handles `ConnectionFailure`
  (which `NetworkTimeout` subclasses) by setting `self._killed = True`
  before `close()`, and `_prepare_to_die` skips `killCursors` for a killed
  cursor. Every other exception, including `CancelledError`, goes through
  `close()` with the cursor id intact.
- **Why it is only suspected:** the CSOT specification says that when a
  cursor times out client-side "it's imperative that drivers make a
  good-faith effort to close the server-side cursor", and its spec test
  "timeoutMS is refreshed for close" (a `getMore` blocked past `timeoutMS`,
  then `close()`, expecting `killCursors`) is this case. pymongo's unified
  test runner skips that file ("CSOT not implemented for cursors"), so the
  gap is known to the maintainers in that sense. Whether the server cursor
  survives depends on the server: the simulated server keeps a
  non-exhausted cursor after its connection closes, which is what I expect
  of `mongod` for a `getMore` that completes after the client gave up, but
  that was not checked against a real server here. Where it does survive
  it lasts until the server's idle cursor timeout (10 minutes by default).
  For a reset connection, treating the cursor as dead is a long-standing,
  apparently deliberate choice.
- **Not reported.**

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
  use. (The 2023 cancellation bug, CVE-2023-28858/28859, is fixed.) The
  same mix against `BlockingConnectionPool` (`max_connections=2`, six
  workers, pool timeout none, 50 ms or 1 s) was clean too: never more than
  2 connections, none stuck in use, 200 seeds each, random and fifo.
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
- **taskiq 0.13.0 (worker `Receiver`)**, [`findings/taskiq_receiver.py`](../findings/taskiq_receiver.py):
  with ackable deliveries, `max_async_tasks=3`, `max_prefetch` 0-3, task
  `timeout` labels and a stop at a random time, at most `max_async_tasks`
  tasks ran at once, unacked deliveries stayed within `max_async_tasks +
  max_prefetch`, no task ran twice and no message was acked twice, every
  finished task was acked, and a stop without `wait_tasks_timeout` waited for
  every running task (200 seeds each, random and fifo).
- **CPython 3.13.18 and 3.14.6 `asyncio.Queue.shutdown()`**, [`findings/asyncio_queue_shutdown.py`](../findings/asyncio_queue_shutdown.py):
  with producers, consumers and `join()` waiters on `Queue`, `LifoQueue`
  and `PriorityQueue` (blocking, `*_nowait` and `wait_for` calls) and a
  graceful or immediate shutdown at a random time, the documented behaviour
  held: no `QueueShutDown` before shutdown or from `get` on a non-empty
  queue (graceful), every blocked caller woke, no item was lost or
  duplicated beyond what an immediate shutdown drains, and `join()`
  returned once nothing was unfinished (1000 seeds each, random, fifo and
  PCT, `strict_cancellation=True`). A mutated `shutdown()` that wakes no
  one fails 143 of 200 seeds.
- **SQLAlchemy 2.1.9 asyncio engine (aiomysql 0.3.2 driver)**, [`findings/sqlalchemy_async_pool.py`](../findings/sqlalchemy_async_pool.py):
  against the simulated MySQL server (which now answers the dialect's
  startup queries), with `engine.connect()`/`engine.begin()` jobs, raw
  `BEGIN`s left open, caller timeouts, `pool_timeout=0.02`,
  `pool_pre_ping`, TCP resets and server crashes: replies always matched,
  at most `pool_size + max_overflow` connections were open, no connection
  was checked out while still in a server-side transaction, nothing stayed
  checked out or hung, and `dispose()` closed every server session (100
  seeds per workload, random and fifo, `strict_cancellation=True`).
  SQLAlchemy uses its own pool, so the aiomysql pool bug above does not
  apply. (A first version counted server-side sessions and flagged 4 open
  with a limit of 3: an invalidated connection's slot is reused before the
  server sees it close. That is network latency, not a leak; the harness
  counts client connections instead.)
- **pymongo 4.20.0 `AsyncMongoClient`**, [`findings/pymongo_async_pool.py`](../findings/pymongo_async_pool.py):
  unmodified against the simulated MongoDB server in `findings/_mongo.py`
  (OP_MSG, standalone, polled monitoring), `maxPoolSize=2`, five workers
  doing inserts with read-back, cursor iteration (stopping early and
  closing) and `find_one_and_update`, under `wait_for` timeouts,
  `pymongo.timeout()`, 0.3% TCP resets and server crashes. Every reply
  matched its request, the pool never had more than two connections (CMAP
  events), no worker hung, nothing stayed checked out, the client served
  a query once faults stopped and `close()` closed every server
  connection; 100 seeds per workload, random and fifo,
  `strict_cancellation=True`. Its checkout path restores counters on any
  `BaseException`. Running it needed `fcntl.fcntl` on simulated sockets in
  SimLoop (pymongo sets close-on-exec itself). The cursor cleanup gap is a
  suspected issue above.
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
- **psycopg-pool 3.3.3 (`AsyncConnectionPool`)**, [`findings/psycopg_pool_clients.py`](../findings/psycopg_pool_clients.py),
  against a stand-in connection class (connects in 0-20 ms, fails 5% of the
  time), `max_size` 1-3, five clients with random `wait_for` timeouts: no
  connection went to two clients at once, no more than `max_size` stayed
  open, and the pool could hand out `max_size` connections afterwards. 1000
  seeds each with `random`, `pct` and `fifo` plus 1 ms jitter, with
  `strict_cancellation=True`. An earlier run failed on 23 of 300 seeds and
  did not replay: psycopg_pool imports `monotonic` by name, which kept the
  real clock. SimLoop now swaps such module globals for the run.
- **aio-pika 10.2.3 (`aio_pika.pool.Pool`)**, [`findings/aio_pika_pool.py`](../findings/aio_pika_pool.py):
  no item held by two users, never more than `max_size` items created,
  every user finished, with a constructor that fails 10% of the time and
  random `wait_for` timeouts. 1000 seeds each with `random`, `pct` and
  `fifo` plus 1 ms jitter, with `strict_cancellation=True`.
- **aiologic 0.17.1**, [`findings/aiologic_primitives.py`](../findings/aiologic_primitives.py):
  `Lock` mutual exclusion, the `Semaphore` bound and permit return, and
  exactly-once delivery through bounded and unbounded `Queue`s, with random
  `wait_for` timeouts on every side. 1000 seeds per primitive each with
  `random`, `pct` and `fifo` plus 1 ms jitter, with `strict_cancellation=True`.
- **aiocron 2.1 and APScheduler 3.11.3 (`AsyncIOScheduler`)**, [`findings/cron_dst.py`](../findings/cron_dst.py),
  in a simulated `America/New_York` zone (`SimConfig(timezone=...)`) over
  the spring-forward and fall-back nights of 2024, `fifo` with 10 ms timer
  jitter: a `*/15` job fired every 15 minutes of real time at second 0
  through the gap and the repeated hour, and daily jobs at 01:30 and 02:30
  fired once per night. The one exception is documented: APScheduler runs
  a daily 01:30 job twice on the night the clocks go back (its cron docs
  say so; #1199 discusses the wording). APScheduler's DST handling has many
  recent upstream issues and PRs (#1144, #1183, #1198, #1200); this
  harness did not reach those cases.
- **pyrate-limiter 4.5.1 (`Limiter`, in-memory bucket)**, [`findings/pyrate_limiter_window.py`](../findings/pyrate_limiter_window.py):
  with two rates (5/s and 12/3 s), weights 1-2, the limiter's own timeout
  and outer `wait_for` cancellations, no window of either rate held more
  than its limit; 300 seeds each with `random` and `fifo` plus 1 ms jitter,
  `strict_cancellation=True`. The bucket's leak ran in an event-loop task
  (the bucket marked async) instead of its usual background thread, so
  races with that thread were not covered.
- **grpclib 0.4.9 (server and client)**, [`findings/grpclib_reset_leak.py`](../findings/grpclib_reset_leak.py),
  with the #217 leak fixed in the harness: unary, server-streaming and
  bidirectional calls under per-call deadlines, 0.2% TCP resets per
  segment and server crashes every 0.3-1.5 s. Every reply matched its
  request, no call outlived its deadline by more than 50 ms, the channel
  recovered after faults stopped and every server handler exited; 100
  seeds each with `random` and `fifo` plus 1 ms jitter. Calls without a
  deadline were left out (open issue #216 covers a hang there).
- **asyncmy 0.2.18 (`Pool`)**, [`findings/aiomysql_pool_wakeup.py`](../findings/aiomysql_pool_wakeup.py):
  the aiomysql pool harness (6 workers, `maxsize=2`, queries cancelled by
  `wait_for`, transactions left open) against the simulated MySQL server:
  every reply matched its query, the pool stayed within `maxsize` and every
  worker finished; 200 seeds each with `random` and `fifo` plus 1 ms jitter.
  Running it needed `asyncio.BufferedProtocol` support in SimLoop's
  transports.
