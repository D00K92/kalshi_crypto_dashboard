"""Separate lossless price discovery from coalesced book publication."""

from __future__ import annotations

import asyncio
import logging
import time

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ingestion.models import MarketEvent
from ingestion.pipeline.redis_publisher import RedisPublisher

LOGGER = logging.getLogger(__name__)
BOOK_STREAMS = frozenset({"stream:orderbook_snapshots", "stream:kalshi_orderbook"})


class EventPipeline:
    """Keep trades/Kalshi quotes lossless; retain only latest book state."""

    def __init__(self, publisher: RedisPublisher, maxsize: int, *,
                 book_publisher: RedisPublisher | None = None, batch_size: int = 200,
                 flush_ms: int = 5, book_flush_ms: int = 100,
                 delay_warning_ms: int = 5_000) -> None:
        self._publisher = publisher
        self._book_publisher = book_publisher or publisher
        self._critical_queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=maxsize)
        self._book_queue: asyncio.Queue[tuple[str, str, str, str]] = asyncio.Queue(maxsize=maxsize)
        self._latest_books: dict[tuple[str, str, str, str], MarketEvent] = {}
        self._queued_books: set[tuple[str, str, str, str]] = set()
        self._batch_size, self._flush_seconds = batch_size, flush_ms / 1_000
        self._book_flush_seconds = book_flush_ms / 1_000
        self._delay_warning_ms = delay_warning_ms

    @property
    def queued_events(self) -> int:
        return self._critical_queue.qsize() + self._book_queue.qsize()

    async def put(self, event: MarketEvent) -> None:
        if event.stream_name not in BOOK_STREAMS:
            await self._critical_queue.put(event)
            return
        key = self._book_key(event)
        self._latest_books[key] = event
        if key in self._queued_books:
            return
        try:
            self._book_queue.put_nowait(key)
        except asyncio.QueueFull:
            LOGGER.warning("ingestion_book_queue_full", extra={"queued_events": self._book_queue.qsize()})
            return
        self._queued_books.add(key)

    async def run(self) -> None:
        await asyncio.gather(self._run_critical(), self._run_books())

    async def _run_critical(self) -> None:
        while True:
            first = await self._critical_queue.get()
            batch = [first]
            published = False
            try:
                deadline = time.monotonic() + self._flush_seconds
                while len(batch) < self._batch_size:
                    try:
                        event = await asyncio.wait_for(self._critical_queue.get(), max(0, deadline - time.monotonic()))
                    except TimeoutError:
                        break
                    batch.append(event)
                self._warn_if_delayed(batch, "critical")
                await self._publisher.publish_many(batch)
                published = True
            finally:
                if published:
                    for _ in batch:
                        self._critical_queue.task_done()

    async def _run_books(self) -> None:
        while True:
            first = await self._book_queue.get()
            keys = [first]
            try:
                await asyncio.sleep(self._book_flush_seconds)
                while len(keys) < self._batch_size:
                    try:
                        keys.append(self._book_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                events: list[MarketEvent] = []
                for key in keys:
                    event = self._latest_books.pop(key, None)
                    self._queued_books.discard(key)
                    if event is not None:
                        events.append(event)
                self._warn_if_delayed(events, "book")
                try:
                    await self._book_publisher.publish_many(events)
                except (RedisConnectionError, RedisTimeoutError):
                    # Books are replaceable snapshots. Drop this stale batch
                    # after bounded retries so newer coalesced state can flow.
                    LOGGER.warning(
                        "ingestion_book_batch_dropped",
                        extra={"event_count": len(events)},
                    )
            finally:
                for _ in keys:
                    self._book_queue.task_done()

    def _warn_if_delayed(self, events: list[MarketEvent], stream_class: str) -> None:
        if not events:
            return
        now_ms = time.time_ns() // 1_000_000
        oldest_delay_ms = max(0, now_ms - min(event.received_ts_ms for event in events))
        if oldest_delay_ms > self._delay_warning_ms:
            LOGGER.warning("ingestion_queue_delayed", extra={"stream_class": stream_class, "delay_ms": oldest_delay_ms, "queued_events": self.queued_events})

    @staticmethod
    def _book_key(event: MarketEvent) -> tuple[str, str, str, str]:
        return (event.stream_name, event.venue, event.instrument, str(getattr(event, "market_ticker", "")))

    async def drain(self) -> None:
        await self._critical_queue.join()
        await self._book_queue.join()
