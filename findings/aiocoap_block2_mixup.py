"""aiocoap 0.4.17: concurrent block-wise responses from one resource get mixed.

An aiocoap server (``simplesocketserver``) and client (``simple6``) on the
simulated network, over UDP with 1-20 ms latency. Four tasks send six
POSTs each to one resource, which answers with the request's payload
repeated. Checked: every response is exactly the answer to its own
request, the handler runs at most once per request (and exactly once when
a response came back), and nobody hangs. With small responses this held
with 10% datagram loss, 10% duplication and ``wait_for`` cancellations;
with 2 KB responses (two Block2 blocks) it held one request at a time,
under the same faults, random and fifo.

With 2 KB responses and four tasks at once it fails without any fault.
``Block2Cache`` keeps the rendered response under a key made of the
client's address, the request code and its options, which is the same
for two POSTs to one resource, so the second render replaces the first;
the first request's later blocks are then cut from the second request's
response. The server sends no ETag, so the client's assembly check cannot
notice, and the caller gets a body that is half another request's answer.
The client could tell its operations apart with a Request-Tag (RFC 9175),
but sets none; giving each request its own Request-Tag passes. Related
open upstream issues: #58 (2017, concurrent Block1 requests to one
resource cannot be told apart) and #273 (Request-Tag for simultaneous
requests not implemented).

Run: python findings/aiocoap_block2_mixup.py
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sys
from collections.abc import Callable, Coroutine
from typing import Any

import aiocoap
from aiocoap import resource

import simloop

logging.getLogger("coap").setLevel(logging.CRITICAL)
logging.getLogger("coap-server").setLevel(logging.CRITICAL)
WORKLOADS: dict[str, dict[str, Any]] = {
    "small, 10% loss and duplicates, wait_for": {"drop": 0.1, "dup": 0.1, "cancel": 0.3},
    "2 KB, one at a time, 10% loss and duplicates": {
        "repeat": 400,
        "workers": 1,
        "drop": 0.1,
        "dup": 0.1,
    },
    "2 KB, one at a time, 5% loss and duplicates, wait_for": {
        "repeat": 400,
        "workers": 1,
        "drop": 0.05,
        "dup": 0.05,
        "cancel": 0.2,
    },
    "2 KB, concurrent, no faults": {"repeat": 400},
    "2 KB, concurrent, Request-Tag, 5% loss and duplicates": {
        "repeat": 400,
        "tag": True,
        "drop": 0.05,
        "dup": 0.05,
    },
}
FAILING = ("2 KB, concurrent, no faults",)


def make_harness(knobs: dict[str, Any]) -> Callable[[], Coroutine[Any, Any, None]]:
    async def harness() -> None:
        """Every response answers its own request; handlers run at most once."""
        rng = simloop.rng()
        net = simloop.network()
        runs: dict[str, int] = {}
        repeat = knobs.get("repeat", 1)

        class Echo(resource.Resource):
            async def render_post(self, request: aiocoap.Message) -> aiocoap.Message:
                key = request.payload.decode()
                runs[key] = runs.get(key, 0) + 1
                await asyncio.sleep(rng.uniform(0, 0.01))
                return aiocoap.Message(code=aiocoap.CHANGED, payload=(key + "|").encode() * repeat)

        async def server() -> None:
            site = resource.Site()
            site.add_resource(["echo"], Echo())
            address = socket.gethostbyname(socket.gethostname())
            await aiocoap.Context.create_server_context(
                site, bind=(address, 5683), transports=["simplesocketserver"]
            )
            await asyncio.Future()

        net.add_host("coap", entry=server)
        net.set_link("main", "coap", latency=(0.001, 0.02))
        await asyncio.sleep(0.1)
        client = await aiocoap.Context.create_client_context(transports=["simple6"])
        net.set_link(
            "main",
            "coap",
            latency=(0.001, 0.02),
            drop=knobs.get("drop", 0.0),
            duplicate=knobs.get("dup", 0.0),
        )
        answered: list[str] = []
        wrong: list[str] = []

        async def post(tag: str) -> None:
            request = aiocoap.Message(
                code=aiocoap.POST, uri="coap://coap/echo", payload=tag.encode()
            )
            if knobs.get("tag"):
                request.opt.request_tag = [tag.encode()]
            try:
                if rng.random() < knobs.get("cancel", 0):
                    try:
                        response = await asyncio.wait_for(
                            client.request(request).response, rng.uniform(0, 3)
                        )
                    except TimeoutError:
                        return
                else:
                    response = await client.request(request).response
            except aiocoap.error.Error as exc:
                simloop.log(f"{tag}: {type(exc).__name__}")
                return
            answered.append(tag)
            body = bytes(response.payload)
            if body != (tag + "|").encode() * repeat:
                others = sorted({part for part in body.decode().split("|") if part} - {tag})
                wrong.append(f"{tag}: the response also holds the answer to {others}")

        async def worker(n: int) -> None:
            for i in range(6):
                await post(f"c{n}-{i}")
                await asyncio.sleep(rng.uniform(0, 0.05))

        tasks = [asyncio.create_task(worker(n)) for n in range(knobs.get("workers", 4))]
        done, pending = await asyncio.wait(tasks, timeout=600)
        assert not pending, f"{len(pending)} worker(s) stuck"
        for task in done:
            task.result()
        assert not wrong, wrong[:3]
        twice = {tag: count for tag, count in runs.items() if count > 1}
        assert not twice, f"handler ran more than once: {twice}"
        for tag in answered:
            assert runs.get(tag) == 1, (
                f"{tag}: answered, but the handler ran {runs.get(tag, 0)} times"
            )
        await client.shutdown()

    return harness


async def plain() -> list[str]:
    """On a real loop: two concurrent POSTs whose answers need two blocks each."""

    class Echo(resource.Resource):
        async def render_post(self, request: aiocoap.Message) -> aiocoap.Message:
            await asyncio.sleep(0.01)
            return aiocoap.Message(code=aiocoap.CHANGED, payload=request.payload * 2000)

    site = resource.Site()
    site.add_resource(["echo"], Echo())
    server = await aiocoap.Context.create_server_context(
        site, bind=("127.0.0.1", 56830), transports=["simplesocketserver"]
    )
    client = await aiocoap.Context.create_client_context(transports=["simple6"])

    async def post(body: bytes) -> str:
        request = aiocoap.Message(
            code=aiocoap.POST, uri="coap://127.0.0.1:56830/echo", payload=body
        )
        response = await client.request(request).response
        payload = bytes(response.payload)
        return f"{body.decode()}: {payload.count(b'A')} x A, {payload.count(b'B')} x B"

    outcome = list(await asyncio.gather(post(b"A"), post(b"B")))
    await client.shutdown()
    await server.shutdown()
    return outcome


def main() -> int:
    configs = [("random", simloop.SimConfig(step_time=1e-6))]
    configs.append(("fifo", configs[0][1].replace(scheduler="fifo", timer_jitter=0.001)))
    for workload, knobs in WORKLOADS.items():
        for name, config in configs:
            report = simloop.explore(make_harness(knobs), seeds=range(30), config=config)
            print(f"{workload}, {name}: {report.summary().splitlines()[0]}")
            if workload not in FAILING:
                assert report.ok, report.summary()
                continue
            assert report.groups, report.summary()
            for group in report.groups:
                assert "also holds the answer to" in group.message, group.message
    outcome = asyncio.run(plain())
    print(f"real asyncio loop, two 2000-byte answers: {'; '.join(outcome)}")
    assert outcome[0] != "A: 2000 x A, 0 x B", outcome
    return 0


if __name__ == "__main__":
    sys.exit(main())
