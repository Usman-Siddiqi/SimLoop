"""arq 0.28.0: abort requests never expire; Ctrl-C mid-job locks the job and skips cleanup.

Runs arq workers and a client against the simulated Redis server in
``_resp.py`` (strings, sorted sets, expiry, ``WATCH``/``MULTI``/``EXEC``).

1. Abort requests never expire. ``Job.abort()`` adds the job id to the
   ``arq:abort`` sorted set, scored with the current time in milliseconds.
   Every poll, the worker means to drop entries older than
   ``abort_job_max_age`` (60, commented as seconds) but calls
   ``zremrangebyscore(min=now_ms + 60, max=inf)``: a range in the future,
   which never matches. An abort for a job that already finished, or was
   never found, therefore stays forever, and a job enqueued later under the
   same ``_job_id`` is "aborted before start", however long after.
2. A shutdown while a job is being claimed, starting or finishing is not
   clean. On a signal the worker cancels its tasks, and only the call of the
   job function itself handles that cancellation:

   * cancelled while the reply to its ``in-progress`` claim is on its way,
     the worker never starts the job but leaves the claim in place;
   * cancelled during the job's first round trip, the job task raises
     ``CancelledError`` and also leaves the claim;
   * cancelled while the result is being recorded (inside
     ``asyncio.shield``), the job task raises ``CancelledError`` and the
     recording task is destroyed while pending.

   A job task that raised makes ``Worker.close()`` raise too (its
   ``gather`` re-raises), so ``Worker.run()`` raises ``CancelledError``,
   ``on_shutdown`` never runs and the health-check key is left behind. A
   claim left in place stops every worker from running the job until it
   expires (``job_timeout + 10`` s, 310 s by default), although arq
   documents that a job cancelled by a shutdown "will remain in the queue to
   be run again when the worker starts up again (or run by another worker
   which is still running)".

Run: python findings/arq_worker.py
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import threading
import time
from typing import Any

from _resp import RespServer, Session, encode, read_command
from arq import create_pool
from arq.connections import RedisSettings
from arq.jobs import Job, JobStatus
from arq.worker import Worker, func
from redis.asyncio.client import Pipeline

import simloop

SETTINGS = RedisSettings(host="db", conn_retries=0)

_reset = Pipeline.reset


async def _reset_releasing(self: Pipeline) -> None:
    # arq requires redis<6, whose Pipeline.reset keeps the connection when it
    # is cancelled during UNWATCH (fixed in later redis-py). Apply that fix so
    # the leak does not mask arq's own behaviour.
    try:
        await _reset(self)
    except asyncio.CancelledError:
        if self.connection:
            await self.connection.disconnect(nowait=True)
            await self.connection_pool.release(self.connection)
            self.connection = None
        self.watching = False
        raise


Pipeline.reset = _reset_releasing  # type: ignore[method-assign]


async def stop(worker: Worker, task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await worker.close()


def start(worker: Worker) -> asyncio.Task[None]:
    return asyncio.create_task(worker.async_run())


async def abort_harness() -> None:
    """A job enqueued long after an abort of an earlier job with its id still runs."""
    net = simloop.network()
    net.add_host("db", entry=RespServer().serve)
    net.set_link("main", "db", latency=(0.0005, 0.005))
    await asyncio.sleep(0.01)
    rng = simloop.rng()
    calls: list[int] = []

    async def work(ctx: dict[str, Any], n: int) -> int:
        calls.append(n)
        await asyncio.sleep(rng.uniform(0, 0.2))
        return n

    worker = Worker(
        [func(work, name="work")],
        redis_settings=SETTINGS,
        handle_signals=False,
        allow_abort_jobs=True,
        poll_delay=rng.uniform(0.5, 1),
        keep_result=0,
    )
    task = start(worker)
    pool = await create_pool(SETTINGS)
    job = await pool.enqueue_job("work", 1, _job_id="report")
    assert job is not None
    while await job.status() != JobStatus.not_found:  # until the job has finished
        await asyncio.sleep(0.1)
    await asyncio.sleep(rng.uniform(0, 1))
    # Too late, or for a job that was never there: nothing to abort.
    assert await job.abort(timeout=5, poll_delay=0.1) is False
    later = rng.uniform(61, 600)
    await asyncio.sleep(later)
    again = await pool.enqueue_job("work", 2, _job_id="report")
    assert again is not None
    await asyncio.sleep(5)
    await stop(worker, task)
    await pool.aclose()
    assert calls == [1, 2], (
        f"the job enqueued {later:.0f} s after an abort of its id was aborted before start"
    )


async def shutdown_harness() -> None:
    """Restarting workers neither raises in close() nor leaves jobs locked."""
    net = simloop.network()
    server = RespServer()
    net.add_host("db", entry=server.serve)
    net.set_link("main", "db", latency=(0.0005, 0.005))
    await asyncio.sleep(0.01)
    rng = simloop.rng()

    async def work(ctx: dict[str, Any], n: int) -> int:
        await asyncio.sleep(rng.uniform(0, 0.3))
        return n

    def worker() -> tuple[Worker, asyncio.Task[None]]:
        w = Worker(
            [func(work, name="work")],
            redis_settings=SETTINGS,
            handle_signals=False,
            poll_delay=rng.uniform(0.01, 0.1),
            max_jobs=2,
            max_tries=100,
        )
        return w, start(w)

    workers = [worker() for _ in range(3)]
    errors: list[str] = []
    restarting = True

    async def restarts() -> None:
        while restarting:
            await asyncio.sleep(rng.uniform(0.1, 1.0))
            i = rng.randrange(len(workers))
            try:
                await stop(*workers[i])
            except asyncio.CancelledError:
                # Worker.close() raised: cleanup after the gather was skipped.
                errors.append("Worker.close() raised CancelledError")
            workers[i] = worker()

    restarter = asyncio.create_task(restarts())
    pool = await create_pool(SETTINGS)
    jobs: list[Job] = []
    for n in range(20):
        job = await pool.enqueue_job("work", n)
        assert job is not None
        jobs.append(job)
        await asyncio.sleep(rng.uniform(0, 0.05))
    deadline = 30.0
    try:
        for n, job in enumerate(jobs):
            try:
                assert await job.result(timeout=deadline, poll_delay=0.05) == n
            except TimeoutError:
                left = await pool.pttl(f"arq:in-progress:{job.job_id}")
                errors.append(
                    f"job {n} did not finish in {deadline:.0f} s:"
                    f" no worker runs it, its in-progress key expires in {left / 1000:.0f} s"
                )
                break
    finally:
        restarting = False
        await restarter
        for w, t in workers:
            with contextlib.suppress(asyncio.CancelledError):
                await stop(w, t)
        await pool.aclose()
    assert not errors, errors[0]


class SignallingServer(RespServer):
    """Sends this process SIGINT once a transaction containing ``trigger`` arrives."""

    def __init__(self, trigger: bytes = b"") -> None:
        super().__init__()
        self.trigger = trigger

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = Session()
        with contextlib.suppress(ConnectionError):
            while (command := await read_command(reader)) is not None:
                queued = [b" ".join(c) for c in session.queued or []]
                claimed = command[0].upper() == b"EXEC" and any(self.trigger in q for q in queued)
                if self.trigger and claimed:
                    self.trigger = b""
                    os.kill(os.getpid(), signal.SIGINT)  # Ctrl-C while the reply is on its way
                    await asyncio.sleep(0.1)
                writer.write(encode(self.dispatch(session, command)))
                await writer.drain()


def serve_in_thread(server: RespServer) -> int:
    """Run ``server`` on its own loop and thread, like a separate Redis process."""
    ready = threading.Event()
    port: list[int] = []

    async def run() -> None:
        listener = await asyncio.start_server(server.handle, "127.0.0.1", 0)
        port.append(listener.sockets[0].getsockname()[1])
        ready.set()
        await asyncio.Event().wait()

    threading.Thread(target=asyncio.run, args=(run(),), daemon=True).start()
    ready.wait()
    return port[0]


def plain_ctrl_c(trigger: bytes) -> dict[str, object]:
    """``Worker.run()`` on a real loop, with Ctrl-C at the transaction holding ``trigger``."""
    server = SignallingServer(trigger)
    settings = RedisSettings(host="127.0.0.1", port=serve_in_thread(server), conn_retries=0)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    shutdown: list[bool] = []
    calls: list[bool] = []

    async def work(ctx: dict[str, Any]) -> None:
        calls.append(True)

    async def on_shutdown(ctx: dict[str, Any]) -> None:
        shutdown.append(True)

    async def enqueue() -> None:
        pool = await create_pool(settings)
        await pool.enqueue_job("work", _job_id="j")
        await pool.aclose()

    loop.run_until_complete(enqueue())
    worker = Worker(
        [func(work, name="work")], redis_settings=settings, on_shutdown=on_shutdown, poll_delay=0.05
    )
    try:
        worker.run()
        outcome = "returned"
    except asyncio.CancelledError:
        outcome = "raised CancelledError"
    time.sleep(0.3)  # let the server finish what it received
    deadline = server.expires.get(b"arq:in-progress:j")
    locked = deadline is not None and b"arq:in-progress:j" in server.data
    return {
        "run": outcome,
        "on_shutdown": bool(shutdown),
        "job ran": bool(calls),
        "in-progress ttl s": round(deadline - time.time()) if locked and deadline else None,
        "health key left": any(k.endswith(b":health-check") for k in server.data),
    }


def plain_stale_abort() -> bool:
    """Whether a real worker keeps an abort request made ten minutes ago."""
    server = RespServer()
    settings = RedisSettings(host="127.0.0.1", port=serve_in_thread(server))

    async def work(ctx: dict[str, Any]) -> None:
        pass

    async def main() -> bool:
        pool = await create_pool(settings)
        await pool.zadd("arq:abort", {"gone": time.time() * 1000 - 600_000})
        worker = Worker(
            [func(work, name="work")],
            redis_settings=settings,
            handle_signals=False,
            allow_abort_jobs=True,
            poll_delay=0.05,
        )
        task = start(worker)
        await asyncio.sleep(0.3)  # several polls
        await stop(worker, task)
        kept = await pool.zscore("arq:abort", "gone") is not None
        await pool.aclose()
        return kept

    return asyncio.run(main())


def main() -> int:
    shutdown_messages = []
    for scheduler in ("random", "fifo"):
        config = simloop.SimConfig(scheduler=scheduler)
        if scheduler == "fifo":
            config = config.replace(timer_jitter=0.001)
        abort = simloop.explore(abort_harness, seeds=range(20), config=config)
        print(f"{scheduler}, abort then re-enqueue: {abort.summary()}")
        assert abort.groups
        assert all("aborted before start" in g.message for g in abort.groups)
        shutdown = simloop.explore(shutdown_harness, seeds=range(100), config=config)
        print(f"{scheduler}, restarting workers: {shutdown.summary()}")
        shutdown_messages += [g.message for g in shutdown.groups]
    assert any("Worker.close() raised CancelledError" in m for m in shutdown_messages)
    assert any("in-progress key expires" in m for m in shutdown_messages)
    kept = plain_stale_abort()
    print(f"real worker, abort request made 10 minutes ago: {'kept' if kept else 'dropped'}")
    assert kept
    cases = (
        ("being claimed", b"PSETEX arq:in-progress:", False, True),
        ("starting", b"GET arq:job:", True, True),
        ("finishing", b"SET arq:result:", True, False),
    )
    for label, trigger, raises, locked in cases:
        outcome = plain_ctrl_c(trigger)
        print(f"real Worker.run(), Ctrl-C while a job is {label}: {outcome}")
        assert (outcome["run"] == "raised CancelledError") is raises
        assert outcome["on_shutdown"] is not raises
        ttl = outcome["in-progress ttl s"]
        assert (isinstance(ttl, int) and ttl > 300) is locked
    return 0


if __name__ == "__main__":
    sys.exit(main())
