"""Ingestion process lifecycle."""

from __future__ import annotations

import asyncio
import logging

from ingestion.adapters.binance import BinanceFeed
from ingestion.config import Settings
from ingestion.pipeline.event_pipeline import EventPipeline
from ingestion.pipeline.redis_publisher import RedisPublisher


LOGGER = logging.getLogger(__name__)


class IngestionService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._publisher = RedisPublisher(
            settings.redis_url,
            stream_maxlen=settings.stream_maxlen,
        )
        self._pipeline = EventPipeline(
            self._publisher,
            maxsize=settings.queue_maxsize,
        )
        self._feed = BinanceFeed(settings.binance_ws_url, self._pipeline)

    async def run(self, stop_event: asyncio.Event) -> None:
        await self._publisher.ready()
        LOGGER.info("redis_ready")

        pipeline_task = asyncio.create_task(
            self._pipeline.run(), name="redis-publisher"
        )
        feed_task = asyncio.create_task(self._feed.run(), name="binance-feed")
        stop_task = asyncio.create_task(stop_event.wait(), name="shutdown-signal")
        try:
            done, _ = await asyncio.wait(
                {pipeline_task, feed_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task not in done:
                failed_task = next(iter(done))
                if failed_task.cancelled():
                    raise RuntimeError(f"{failed_task.get_name()} stopped unexpectedly")
                failure = failed_task.exception()
                if failure is not None:
                    raise failure
                raise RuntimeError(f"{failed_task.get_name()} exited unexpectedly")
        finally:
            stop_task.cancel()
            feed_task.cancel()
            await asyncio.gather(stop_task, feed_task, return_exceptions=True)

            try:
                async with asyncio.timeout(self._settings.shutdown_grace_seconds):
                    await self._pipeline.drain()
            except TimeoutError:
                LOGGER.error(
                    "pipeline_drain_timed_out",
                    extra={"queued_events": self._pipeline.queued_events},
                )
            finally:
                pipeline_task.cancel()
                await asyncio.gather(pipeline_task, return_exceptions=True)
                await self._publisher.close()
                LOGGER.info("ingestion_stopped")
