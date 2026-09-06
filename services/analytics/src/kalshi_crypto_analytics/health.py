from __future__ import annotations

import asyncio
from collections.abc import Callable


class HealthServer:
    def __init__(self, port: int, ready: Callable[[], bool]) -> None:
        self.port, self.ready = port, ready
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "0.0.0.0", self.port)

    async def close(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await reader.read(1024)
            path = request.split(b" ", 2)[1] if b" " in request else b"/"
            healthy = path == b"/healthz" or (path == b"/readyz" and self.ready())
            status, body = (200, b"ok\n") if healthy else (503, b"not ready\n")
            phrase = "OK" if status == 200 else "Service Unavailable"
            writer.write(f"HTTP/1.1 {status} {phrase}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
