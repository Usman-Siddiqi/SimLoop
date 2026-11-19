# Progress log

Newest session first. Each entry: done, in progress, known issues, next steps.

## Session 5 (2026-11-19): cancellation through shared futures, DataLoaders

### Done
- `strict_cancellation`'s stray-cancel check now follows cancellation
  through a shared plain `Future`: the loop records which tasks wait on each
  future, and a task that ends cancelled after awaiting a future that was
  cancelled on behalf of another waiter is reported, naming that waiter. A
  future a library cancels to wake its own task (aiohttp) is still accepted.
  Every existing harness was re-swept with the check on, random and fifo:
  no new hits besides the recorded bugs.
- **Captured clocks**: third-party modules that imported `time` or
  `os.urandom` functions by name (`from time import monotonic`) get those
  module globals swapped for the run and restored after; a module first
  imported inside a run is pointed back at the real functions on exit. The
  standard library keeps the real functions (`threading` and `queue` time
  real threads). Found through psycopg_pool, whose timers read the real
  clock and made 23 of 300 seeds fail without replaying.
- **Findings**: singleflight 0.1.3 `SingleFlightAsync`: a cancelled leader
  hands concurrent callers `None` and leaves the key returning `None` for
  good (confirmed, not reported). aiodataloader 0.4.3: one caller's
  cancellation cancels the cached future every caller of the key shares, and
  the cancelled future stays cached so later loads fail (confirmed, not
  reported). strawberry-graphql 0.341.2 `DataLoader`: the same spread,
  without the cache poisoning (known upstream, open PR #4803; rediscovered
  by the generic check). aiomcache 0.8.2: a cancelled command leaves its
  reply for the next command on the pooled connection, and a connection
  dropped mid-command strands a caller waiting in the pool (both fixed on
  the upstream default branch, unreleased; rediscovered). coredis 6.9.0:
  each cancelled pipeline leaks a connection-pool slot, and a caller that
  gives up waiting for a connection swallows the next caller's wakeup;
  either way commands hang forever (confirmed on real asyncio, unfixed
  upstream, not reported). `findings/_resp.py` now answers `HELLO 3` and
  takes a fixed `delay` for plain-asyncio repros. A scratch run of the
  redis-py reply-integrity harness against valkey-py 6.2.0 was clean (500
  seeds, random and fifo, with `strict_cancellation`); not kept as a
  harness.
- asyncpg 0.32.0 under a PostgreSQL wire-protocol simulator
  (`findings/_pg.py`: startup, prepared statements, reset query,
  `CancelRequest`; asyncpg runs on it unmodified) with timeouts, resets and
  crashes: replies always matched their queries and the pool always
  recovered, but a connection lost as `async with pool.acquire()` exits
  makes release raise `InternalClientError` ("called on a free connection
  holder"), and a caller cancelled during a failing release leaves a
  "Task exception was never retrieved" log (confirmed on real asyncio, not
  reported; low impact).
- nats-py 2.17.0 under a NATS server simulator (`findings/_nats.py`):
  `flush()` returns success at once during a reconnect (a fallback for
  externally killed tasks also fires when a reconnect cancels them), so a
  message can be lost after a successful flush; `rtt()` during an outage
  raises a bare `AssertionError` (both confirmed on real asyncio, not
  reported). The harness also rediscovers #1085 (PING written ahead of
  queued commands). Request/reply and per-publisher ordering held.
- **Simulated time zone**: `SimConfig(utc_offset=hours)`. Local-time
  functions (`time.localtime`, `mktime`, `strftime`, `ctime`, `asctime`, the
  `time.timezone` constants, `datetime.now()`, `fromtimestamp()`,
  `date.today()`) use a fixed simulated zone, UTC by default, so runs no
  longer depend on the host's `TZ` (before, `datetime.now()` converted with
  the host zone). A non-zero offset found the aiobreaker bug below.
- Circuit breakers, one model-based harness for four libraries:
  circuitbreaker 2.1.3 and aiocircuitbreaker 2.0.0 count a cancelled call as
  a success (a timed-out half-open trial closes the circuit); aiobreaker
  1.2.0 mixes `utcnow()` and `now()` in its reopen time; purgatory 3.0.2
  re-raises one exception object per rejection, growing its traceback
  (all confirmed, not reported).
- Harness fix: `set_link(..., latency=...)` keeps earlier keyword settings,
  so the redis-py and asyncpg harnesses kept injecting resets during their
  recovery phase; they now pass `reset=0.0` (results unchanged), and the
  `set_link` docstring says so.
- Clean harnesses: psycopg-pool 3.3.3 `AsyncConnectionPool` (stand-in
  connection class), aio-pika 10.2.3 `Pool`, aiologic 0.17.1 `Lock`,
  `Semaphore` and `Queue`.
- 242 tests.

### In progress
- Nothing half-done.

### Known issues
- Same as session 4, plus: on Python 3.11, `wait_for(future, t)` cancels
  the future itself rather than the awaiting task, so a shared future
  cancelled that way is not attributed and `stray_cancel` stays silent
  (3.12+ is covered; the findings CI job runs on 3.13).

### Next steps
1. More shared-future/shared-task patterns: other DataLoader ports,
   request-coalescing helpers in HTTP clients and ORMs, `asyncio.shield`
   misuse.
2. Run every findings harness with `strict_cancellation=True` in CI.
3. Coverage- or novelty-guided seed exploration (M8, optional).

## Session 4 (2026-11-18): timeline, fault schedules, cancellation checks, more findings

### Done
- **HTML timeline** (`simloop.save_html`, `--html` on `run`/`replay`/
  `explore`, `--simloop-html DIR` in pytest): per-host and per-task lanes,
  message arrows from send to `recv`, crash/pause spans, kind filters, zoom
  and a click-through event list, self-contained. **README GIF**
  (`scripts/render_gif.py`) drawn from a real shrunk run with the same model.
- **Declarative fault schedules** (`faults.schedule(at/during/between/every
  ...)`) with seeded ranges and `any_of` targets; partitions are reference
  counted so overlapping windows compose.
- **`stable_hash`**: identity-hashed classes kept in sets hash by first-use
  order inside a run (anyio's `CancelScope` is patched automatically). Fixed
  anyio runs that never reproduced.
- **`SimConfig(strict_cancellation=True)`**: fails a run when a task keeps
  running with a cancel request nobody `uncancel()`ed and then finishes
  normally. No hits on SimLoop's own machinery or any existing harness except
  the aiotools bug below; catch-alls that end the task in the same step are
  exempt (uvicorn's `run_asgi` does that).
- **Shrinking**: candidates get a step budget of 10x the failing run, are cut
  off once they spend far longer at one instant than the failing run did,
  and skip the trace digest. A CPython 3.11 deadlock that took 500 s of
  zero-time spinning candidates now shrinks in 17 s; the test suite went from
  15 s to 8 s.
- Tasks that cleanup cannot finish are closed while the run is active, so
  their `finally` blocks no longer print "Exception ignored" noise into later
  runs.
- **Findings**: CPython 3.11 `wait_for` + `Condition` lets a second task into
  the lock (confirmed on real 3.11.1-3.11.18, fixed in 3.12 by the
  `wait_for` rewrite; CI checks it on 3.11). aiotools 2.2.4 `TaskScope`
  timeouts leak a cancel request, which makes a later failing `TaskGroup`
  cancel the task on 3.13+ (confirmed, not reported). cashews 7.6.0: with
  the default `@cache(protected=True)`, one caller's timeout cancels every
  concurrent caller for the key (confirmed, not reported). backoff 2.2.1
  `max_time` uses a stale, wall-clock elapsed time (known upstream,
  rediscovered; the clock half shown with a SimLoop clock jump). Suspected:
  aiometer 1.0.0 `run_any` waits for every function; asyncstdlib 3.14.0
  `tee` peers silently end when another peer is cancelled. Clean: anyio
  primitives, uvicorn + Starlette + httpx, aiometer meters, asyncstdlib
  `tee` with a lock, aiostream, aiomisc `@aggregate`, tenacity.
- `strict_cancellation` also flags a `CancelledError` that reached a task
  from another task's cancellation (how the cashews bug shows up).
- 231 tests on 3.11 to 3.13.

### In progress
- Nothing half-done.

### Known issues
- `strict_cancellation` cannot tell a leak from code that deliberately
  swallows `CancelledError` and keeps going; that is why it is opt-in.
- `RandomScheduler.pick` and `PCTScheduler.pick` are O(batch).
- TLS and Unix sockets are not simulated.

### Next steps
1. More M9 harnesses: aiohttp and websockets server shutdown, other
   "singleflight" caches that share a task between callers.
2. Coverage- or novelty-guided seed exploration (M8, optional).
3. Run every findings harness with `strict_cancellation=True` in CI.

## Session 3 (2026-11-18): fidelity, PCT, parallel runs, more findings

### Done
- **Network fidelity fixes found while hunting** (each with a regression
  test that fails on the old code): an incoming RST reports
  `connection_lost` from a later callback, like asyncio's `_force_close`;
  data held by a partition is replayed in order after a heal, with segments
  in flight queued behind it; simulated sockets have virtual file
  descriptors, and `select.select`/`select.poll` answer zero-timeout
  readiness checks (httpcore uses them to expire idle connections).
- **Feasibility confirmation**: `explore(confirm=N)` (default 200 in the CLI
  and pytest) re-searches each failure found under a non-FIFO scheduler with
  `fifo` plus timer jitter and labels it. This is what separated the aiohttp
  false positive from real bugs.
- **PCT scheduler** (`scheduler="pct"` / `"pct:DEPTH"`, `PCTScheduler`):
  per-task priorities with `DEPTH - 1` demotion points drawn from the
  choice source; all-zero choices are FIFO, so shrinking still works. Finds
  a "loses every round" ordering in most seeds where `random` finds none.
- **Parallel exploration**: `workers=` / `-j` / `--simloop-workers`; forked
  workers (closures work), outcomes consumed in seed order so the report
  matches a sequential one. 4x on 4 cores.
- **`datetime.now()`, `utcnow()`, `date.today()` simulated** via stand-in
  classes, including modules that did `from datetime import datetime`.
- **`SimConfig.step_time`** and a step-limit message that detects spinning
  at one instant (from the aiolimiter false positive).
- **Findings**: aiocache 0.12.3 `cached_stampede` lets every waiter compute
  after a failed first call (suspected issue; reproduces on plain asyncio;
  not reported). aiolimiter 1.3.0 zero-time spin (false positive: frozen
  clock). aiolimiter, aiohttp, httpx/httpcore and websockets clean under
  their properties; redis-py rerun clean after the transport fixes.
- 197 tests on 3.11 to 3.13.

### In progress
- Nothing half-done.

### Known issues
- Sets of user objects with identity hashes still iterate in address order.
- `RandomScheduler.pick` and `PCTScheduler.pick` are O(batch).
- TLS and Unix sockets are not simulated.
- On platforms without `fork`, parallel exploration needs a picklable target.

### Next steps
1. HTML timeline (per-task lanes, network messages, faults) and an animated
   GIF generated from real runs (M7).
2. Declarative fault schedules (M5).
3. More M9 harnesses: tenacity/backoff retry timing, anyio primitives
   (CapacityLimiter, memory streams) under cancellation, aiohttp/httpx with
   richer properties.
4. Coverage- or novelty-guided seed exploration (M8, optional).

## Session 2 (2026-11-18): network, faults, shrinking, pytest, first findings

### Done
- **M4 simulated network** (`simloop.network()`): hosts with addresses,
  per-host tasks and ports, ordered/reliable simulated TCP (latency ranges,
  drops as retransmission delay, injected resets, SYN retries with backoff),
  UDP with drop/duplicate/reorder, partitions (symmetric and one-way) that
  hold data until healed, DNS via `getaddrinfo`, `VirtualSocket` for code
  that builds its own sockets (aiohttp's happy eyeballs), proxy env vars
  stripped during runs. aiohttp, httpx and websockets client/server pairs
  run deterministically under it.
- **M5 faults**: host crash (process crash with RSTs, or power loss) and
  restart with a persistent `disk`, host pause/resume, per-host clock offset
  and drift, FoundationDB-style `faults.buggify()`, Jepsen-style nemesis
  tasks (partitions, crashes, pauses, clock jumps, flaky links).
- **M6 shrinking** (`simloop.shrink`, `explore --shrink`): chunk deletion,
  chunk zeroing and per-value binary search over the recorded choices;
  trailing zeros are ignored when comparing, so the all-FIFO run is the
  simplest. `Reproduction` files (seed, config, choices) via `--save`, and
  `simloop replay FILE`. `SimConfig.to_dict/from_dict`.
- **pytest plugin**: `@simloop.test(seeds=...)`, `SimulationFailed` with
  seed, shrunk summary, reproduce command and trace tail, framework frames
  hidden from tracebacks, example database in `.simloop/examples/`,
  `--simloop-seeds/-start/-seed/-no-shrink/-no-db/-trace-last`.
- **Determinism self-check**: `check_determinism()`, `simloop check`, and
  `explore` now shows the first divergent event for unreproducible groups.
- **Framework bug fixed while hunting**: simulated tasks and futures now hash
  by creation order. `TaskGroup` cancels children by iterating a set, so
  reruns diverged after any TaskGroup abort.
- **M9 findings** (`docs/FINDINGS.md`, `findings/`, CI job): aiojobs 1.4.0
  pending-close limit violation (no upstream report found, not reported);
  async-lru 2.4.0 stale-entry eviction (known upstream, open PRs); redis-py
  8.1.0 and aiorwlock 1.6.0 ran clean.
- `examples/counter.py` (crash loses acknowledged writes); README rewritten
  for all of the above. 152 tests on 3.11 to 3.13.

### In progress
- Nothing half-done.

### Known issues
- Sets of user objects with identity hashes still iterate in address order
  (aiojobs keeps jobs in a set); `explore` flags the divergence but cannot
  remove it.
- The same assertion can land in two failure groups when it surfaces through
  different frames (seen with aiojobs: `_job.py:124` vs the job body).
- `RandomScheduler.pick` is O(batch); fine so far.
- `datetime.now()` not virtualized; TLS and Unix sockets not simulated.

### Next steps
1. More M9 harnesses with properties: aiohttp connector pool under resets
   and cancellations, httpcore pool, websockets close/keepalive under
   partitions, aiolimiter, tenacity/backoff, aiocache.
2. PCT scheduler (M8) and parallel exploration (M3).
3. HTML timeline and GIF (M7); `datetime` virtualization (M2).
4. Declarative fault schedules (M5); deterministic hashing opt-in for user
   classes.

## Session 1 (2026-11-18): foundation

### Done
- Project rules (`CLAUDE.md`), roadmap with acceptance criteria, findings log,
  MIT license, packaging (`src/` layout, hatchling, `py.typed`).
- `SimEventLoop`: a from-scratch `AbstractEventLoop` with a virtual clock,
  batch-based ready queue like asyncio's `_run_once`, timer heap with FIFO
  tie-breaking, optional non-negative timer jitter, deadlock detection with a
  list of blocked tasks and where they wait, step and simulated-time limits,
  per-run deterministic task names, inline `run_in_executor`, async generator
  finalization in creation order, and fail-closed stubs for all I/O methods.
- `ChoiceSource`: seeded, recorded bounded-integer choices; replay from a
  recorded sequence; 0 is always the simplest choice. `ChoiceRandom` exposes
  it as a `random.Random` (`simloop.rng()`).
- Scheduling policies: `fifo` (asyncio's exact order, verified against
  `asyncio.run`) and `random` (uniform within a batch, preserving per-origin
  FIFO order).
- `Sandbox`: simulated `time.*`, `time.sleep` blocks simulated time, seeded
  global `random`, seeded `os.urandom`/`uuid4`/`secrets`; audit hook plus a
  `Thread.start` patch reject real sockets, DNS, threads and subprocesses.
- Properties: `always` (checked after every step), `eventually` (with
  deadline or at end). Unhandled task/callback exceptions fail the run.
- `run()`, `explore()` (failure grouping by signature, traced rerun of the
  first failing seed with digest comparison), CLI `simloop run|explore`.
- Trace with running digest; text timeline; trace freezes at the failure.
- Example `examples/inventory.py` (lost-update race: 100/1000 seeds fail,
  locked version 0/1000) and SVG visuals generated by
  `scripts/render_visuals.py`.
- 95 tests, passing on Python 3.11, 3.12 and 3.13; ruff and `mypy --strict`
  clean; GitHub Actions CI.

### In progress
- Nothing half-done. CI is green on the working branch (lint, mypy, tests on 3.11 to 3.13).

### Known issues
- `RandomScheduler.pick` is O(batch) per pick and deque deletion from the
  middle is O(n), so huge batches (thousands of simultaneously ready
  callbacks) are quadratic. Fine for now; revisit with per-origin queues.
- `datetime.now()` is not virtualized; references to `time` functions
  captured before a run keep the real clock (caught after the fact by
  `explore`'s rerun check).
- `asyncio.all_tasks()` iteration order is address-based.
- Sandbox patches are process-wide while a run is active.
- Exploration is single-process.

### Next steps
1. M4 simulated network: nodes with addresses, `open_connection` /
   `start_server` / `getaddrinfo` over simulated streams, seeded latency,
   drops, duplication, reordering, partitions. This unblocks testing real
   client libraries.
2. M5 fault injection (crash/restart of nodes, I/O errors, slow operations).
3. M6 shrinking on the recorded choice sequence.
4. pytest plugin, `datetime` virtualization, determinism self-check that
   reports the first divergent trace event.
5. Start M9 harnesses once the network exists; candidates in `ROADMAP.md`.
