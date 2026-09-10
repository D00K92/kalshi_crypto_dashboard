"""Bounded in-process queue between venue adapters and Redis."""

from __future__ import annotations

import asyncio
import itertools
import logging
import time

from ingestion.models import MarketEvent
from ingestion.pipeline.redis_publisher import RedisPublisher

LOGGER = logging.getLogger(__name__)


class EventPipeline:
    def __init__(self, publisher: RedisPublisher, maxsize: int, *, batch_size: int = 200,
                 flush_ms: int = 5, delay_warning_ms: int = 5_000) -> None:
        self._publisher = publisher
        self._queue: asyncio.PriorityQueue[tuple[int, int, MarketEvent]] = asyncio.PriorityQueue(maxsize=maxsize)
        self._sequence = itertools.count()
        self._batch_size, self._flush_seconds = batch_size, flush_ms / 1_000
        self._delay_warning_ms = delay_warning_ms

    @property
    def queued_events(self) -> int:
        return self._queue.qsize()

    async def put(self, event: MarketEvent) -> None:
        # Book snapshots dominate message volume but must never delay price discovery.
        priority = 1 if event.stream_name in {"stream:orderbook_snapshots", "stream:kalshi_orderbook"} else 0
        await self._queue.put((priority, next(self._sequence), event))

    async def run(self) -> None:
        while True:
            _, _, first = await self._queue.get()
            batch = [first]
            try:
                deadline = time.monotonic() + self._flush_seconds
                while len(batch) < self._batch_size:
                    try:
                        _, _, event = await asyncio.wait_for(self._queue.get(), max(0, deadline - time.monotonic()))
                    except TimeoutError:
                        break
                    batch.append(event)
                now_ms = time.time_ns() // 1_000_000
                oldest_delay_ms = max(0, now_ms - min(event.received_ts_ms for event in batch))
                if oldest_delay_ms > self._delay_warning_ms:
                    LOGGER.warning("ingestion_queue_delayed", extra={"delay_ms": oldest_delay_ms, "queued_events": self.queued_events})
                await self._publisher.publish_many(batch)
            finally:
                for _ in batch:
                    self._queue.task_done()

    async def drain(self) -> None:
        await self._queue.join()
