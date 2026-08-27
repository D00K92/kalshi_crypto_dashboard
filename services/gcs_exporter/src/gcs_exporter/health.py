"""Minimal HTTP health endpoints for Kubernetes probes."""

from __future__ import annotations

import asyncio


class HealthServer:
    def __init__(self, port: int) -> None:
        self._port = port
        self._ready = False
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, host="0.0.0.0", port=self._port
        )

    def mark_ready(self) -> None:
        self._ready = True

    def mark_not_ready(self) -> None:
        self._ready = False

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=2)
            parts = request_line.split()
            path = parts[1] if len(parts) >= 2 else b""
            healthy = path == b"/healthz" or (
                path == b"/readyz" and self._ready
            )
            status = b"200 OK" if healthy else b"503 Service Unavailable"
            body = b"ok\n" if healthy else b"not ready\n"
            writer.write(
                b"HTTP/1.1 "
                + status
                + b"\r\nContent-Type: text/plain\r\nContent-Length: "
                + str(len(body)).encode("ascii")
                + b"\r\nConnection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
