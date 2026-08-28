from __future__ import annotations

import asyncio
import logging

import orjson
import redis.asyncio as redis
from redis.exceptions import ResponseError

from market_aggregator.aggregation import MarketAggregator
from market_aggregator.config import Settings
from market_aggregator.health import HealthServer

LOGGER = logging.getLogger(__name__)


class AggregatorService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = redis.Redis.from_url(settings.redis_url, decode_responses=False, health_check_interval=30)
        self.state = MarketAggregator(settings.price_tick, settings.book_depth, settings.freshness_ms)
        self.health = HealthServer(settings.health_port)

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.health.start()
        try:
            await self.client.ping()
            await self._ensure_group(self.settings.book_stream, self.settings.book_group)
            await self._ensure_group(self.settings.trade_stream, self.settings.trade_group)
            self.health.ready = True
            LOGGER.info("market_aggregator_ready")
            book_task = asyncio.create_task(self._consume(self.settings.book_stream, self.settings.book_group, self._handle_book))
            trade_task = asyncio.create_task(self._consume(self.settings.trade_stream, self.settings.trade_group, self._handle_trade))
            await stop_event.wait()
        finally:
            self.health.ready = False
            for task in (locals().get("book_task"), locals().get("trade_task")):
                if task:
                    task.cancel()
            await asyncio.gather(*(task for task in (locals().get("book_task"), locals().get("trade_task")) if task), return_exceptions=True)
            await self.health.close()
            await self.client.aclose()

    async def _consume(self, stream: str, group: str, handler) -> None:
        while True:
            rows = await self.client.xreadgroup(group, self.settings.consumer_name, {stream: ">"}, count=self.settings.read_count, block=self.settings.read_block_ms)
            for _, entries in rows:
                for redis_id, fields in entries:
                    try:
                        payload = fields.get(b"payload")
                        if payload is None:
                            raise ValueError("missing payload")
                        event = orjson.loads(payload)
                        published_ts_ms = int(redis_id.split(b"-", 1)[0] if isinstance(redis_id, bytes) else str(redis_id).split("-", 1)[0])
                        await handler(event, published_ts_ms)
                        await self.client.xack(stream, group, redis_id)
                    except (ValueError, TypeError, orjson.JSONDecodeError) as exc:
                        LOGGER.warning("event_rejected", extra={"stream": stream, "redis_id": redis_id, "error": str(exc)})
                        await self.client.xack(stream, group, redis_id)

    async def _handle_book(self, event: dict, published_ts_ms: int | None = None) -> None:
        if event.get("event_type") != "book_snapshot":
            return
        snapshot = self.state.apply_book(event, published_ts_ms=published_ts_ms)
        encoded = orjson.dumps(snapshot)
        prefix = self.settings.output_prefix
        await self.client.set(f"{prefix}:book:BTCUSDT:latest", encoded)
        await self.client.publish(f"{prefix}:aggregated_orderbook", encoded)

    async def _handle_trade(self, event: dict, published_ts_ms: int | None = None) -> None:
        if event.get("event_type") != "trade":
            return
        spot = self.state.apply_trade(event)
        encoded = orjson.dumps(spot)
        prefix = self.settings.output_prefix
        await self.client.set(f"{prefix}:spot:BTCUSDT:latest", encoded)
        await self.client.publish(f"{prefix}:aggregated_spot", encoded)
        candles = orjson.dumps(self.state.candle_snapshot("BTCUSDT"))
        cvd = orjson.dumps(self.state.cvd_snapshot("BTCUSDT"))
        await self.client.set(f"{prefix}:candles:BTCUSDT:5s", candles)
        await self.client.set(f"{prefix}:cvd:BTCUSDT:5s", cvd)
        await self.client.publish(f"{prefix}:aggregated_candles", candles)
        await self.client.publish(f"{prefix}:aggregated_cvd", cvd)

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self.client.xgroup_create(stream, group, id=self.settings.group_start_id, mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
