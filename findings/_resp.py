"""A tiny RESP2 server for driving Redis clients inside a simulation."""

from __future__ import annotations

import asyncio

import simloop


def encode(value: object) -> bytes:
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, Exception):
        return f"-ERR {value}\r\n".encode()
    if isinstance(value, int):
        return f":{value}\r\n".encode()
    if isinstance(value, str) and value in ("OK", "PONG"):
        return f"+{value}\r\n".encode()
    if isinstance(value, list):
        return f"*{len(value)}\r\n".encode() + b"".join(encode(v) for v in value)
    data = value if isinstance(value, bytes) else str(value).encode()
    return b"$%d\r\n%s\r\n" % (len(data), data)


async def read_command(reader: asyncio.StreamReader) -> list[bytes] | None:
    line = await reader.readline()
    if not line:
        return None
    if not line.startswith(b"*"):
        return line.strip().split()
    args = []
    for _ in range(int(line[1:])):
        size = int((await reader.readline())[1:])
        args.append((await reader.readexactly(size + 2))[:-2])
    return args


class RespServer:
    """GET/SET/INCR/DEL/ECHO/PING plus enough handshake commands for clients.

    ``slow`` adds a random processing delay per command, which is what turns a
    client timeout into a late reply on a reused connection.
    """

    def __init__(self, slow: float = 0.0) -> None:
        self.slow = slow
        self.data: dict[bytes, bytes] = {}

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while (command := await read_command(reader)) is not None:
                if self.slow:
                    await asyncio.sleep(simloop.rng().uniform(0, self.slow))
                writer.write(encode(self.execute(command)))
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    def execute(self, command: list[bytes]) -> object:
        name = command[0].upper()
        args = command[1:]
        if name == b"PING":
            return "PONG"
        if name == b"ECHO":
            return args[0]
        if name == b"GET":
            return self.data.get(args[0])
        if name == b"SET":
            self.data[args[0]] = args[1]
            return "OK"
        if name == b"INCR":
            value = int(self.data.get(args[0], b"0")) + 1
            self.data[args[0]] = str(value).encode()
            return value
        if name == b"DEL":
            return sum(self.data.pop(k, None) is not None for k in args)
        if name in (b"CLIENT", b"SELECT", b"AUTH"):
            return "OK"
        if name == b"HELLO":
            return Exception("unknown command 'HELLO'")
        return Exception(f"unknown command '{name.decode()}'")

    async def serve(self, port: int = 6379) -> None:
        server = await asyncio.start_server(self.handle, port=port)
        async with server:
            await server.serve_forever()
