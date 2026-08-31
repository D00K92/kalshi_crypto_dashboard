"""Ingestion process lifecycle."""

from __future__ import annotations

import asyncio
import logging

from ingestion.adapters.binance import BinanceFeed
from ingestion.adapters.gemini import GeminiFeed
from ingestion.adapters.crypto_com import CryptoComFeed
from ingestion.adapters.bitstamp import BitstampFeed
from ingestion.adapters.coinbase import CoinbaseFeed
from ingestion.adapters.deribit import DeribitFeed
from ingestion.adapters.bybit import BybitFeed
from ingestion.adapters.kraken import KrakenFeed
from ingestion.kalshi_feed import KalshiFeed
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
        self._gemini = GeminiFeed(settings.gemini_ws_url, settings.gemini_symbol, self._pipeline)
        self._crypto_com = CryptoComFeed(settings.crypto_com_ws_url, settings.crypto_com_symbol, self._pipeline)
        self._bitstamp = BitstampFeed(settings.bitstamp_ws_url, settings.bitstamp_symbol, self._pipeline)
        self._coinbase = CoinbaseFeed(settings.coinbase_ws_url, settings.coinbase_product_id, self._pipeline, settings.coinbase_api_key, settings.coinbase_secret)
        self._deribit = DeribitFeed(settings.deribit_ws_url, settings.deribit_instrument, self._pipeline)
        self._bybit = BybitFeed(settings.bybit_ws_url, settings.bybit_symbol, self._pipeline)
        self._kraken = KrakenFeed(settings.kraken_ws_url, settings.kraken_symbol, self._pipeline)
        self._kalshi = (
            KalshiFeed(settings, self._pipeline)
            if settings.kalshi_api_key and settings.kalshi_private_key
            else None
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        await self._publisher.ready()
        LOGGER.info("redis_ready")

        pipeline_task = asyncio.create_task(
            self._pipeline.run(), name="redis-publisher"
        )
        feed_task = asyncio.create_task(self._feed.run(), name="binance-feed")
        gemini_task = asyncio.create_task(self._gemini.run(), name="gemini-feed")
        crypto_com_task = asyncio.create_task(self._crypto_com.run(), name="crypto-com-feed")
        bitstamp_task = asyncio.create_task(self._bitstamp.run(), name="bitstamp-feed")
        coinbase_task = asyncio.create_task(self._coinbase.run(), name="coinbase-feed")
        deribit_task = asyncio.create_task(self._deribit.run(), name="deribit-feed")
        bybit_task = asyncio.create_task(self._bybit.run(), name="bybit-feed")
        kraken_task = asyncio.create_task(self._kraken.run(), name="kraken-feed")
        kalshi_task = asyncio.create_task(self._kalshi.run(stop_event), name="kalshi-feed") if self._kalshi else None
        stop_task = asyncio.create_task(stop_event.wait(), name="shutdown-signal")
        feed_tasks = {feed_task, gemini_task, crypto_com_task, bitstamp_task, coinbase_task, deribit_task, bybit_task, kraken_task}
        if kalshi_task:
            feed_tasks.add(kalshi_task)
        try:
            done, _ = await asyncio.wait(
                {pipeline_task, *feed_tasks, stop_task},
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
            gemini_task.cancel()
            crypto_com_task.cancel()
            bitstamp_task.cancel()
            coinbase_task.cancel()
            deribit_task.cancel()
            bybit_task.cancel()
            kraken_task.cancel()
            if kalshi_task:
                kalshi_task.cancel()
            await asyncio.gather(stop_task, *feed_tasks, return_exceptions=True)

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
