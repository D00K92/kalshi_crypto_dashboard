"""Bounded in-process queue between venue adapters and Redis."""

from __future__ import annotations

import asyncio

from ingestion.models import MarketEvent
from ingestion.pipeline.redis_publisher import RedisPublisher


class EventPipeline:
    def __init__(self, publisher: RedisPublisher, maxsize: int) -> None:
        self._publisher = publisher
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=maxsize)

    @property
    def queued_events(self) -> int:
        return self._queue.qsize()

    async def put(self, event: MarketEvent) -> None:
        await self._queue.put(event)

    async def run(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                await self._publisher.publish(event)
            finally:
                self._queue.task_done()

    async def drain(self) -> None:
        await self._queue.join()
