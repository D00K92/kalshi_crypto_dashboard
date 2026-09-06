from __future__ import annotations

import asyncio


class HealthServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.ready = False
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "0.0.0.0", self.port)

    async def close(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.read(1024)
            status = 200 if self.ready else 503
            body = b"ok\n" if self.ready else b"not ready\n"
            response = f"HTTP/1.1 {status} {'OK' if status == 200 else 'Service Unavailable'}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
