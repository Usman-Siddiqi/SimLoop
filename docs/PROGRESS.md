# Progress log

Newest session first. Each entry: done, in progress, known issues, next steps.

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
