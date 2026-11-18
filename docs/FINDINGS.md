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

## Suspected issues

None yet.

## False positives

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
- **aiorwlock 1.6.0**, [`findings/aiorwlock_exclusion.py`](../findings/aiorwlock_exclusion.py): mutual exclusion (no writer with readers or another
  writer), no deadlock and no lock left held, across 3000 seeds with random
  acquire timeouts, in both `fast` modes.
