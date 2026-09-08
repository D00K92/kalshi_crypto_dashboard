from __future__ import annotations

import asyncio
import logging

import orjson
import redis.asyncio as redis
from redis.exceptions import ResponseError

from aggregator.aggregation import MarketAggregator
from aggregator.config import Settings
from aggregator.health import HealthServer

LOGGER = logging.getLogger(__name__)


class AggregatorService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = redis.Redis.from_url(settings.redis_url, decode_responses=False, health_check_interval=30)
        self.state = MarketAggregator(settings.price_tick, settings.book_depth, settings.freshness_ms, settings.aggregation_venues, dict(settings.taker_fees), settings.trade_freshness_ms, settings.history_ms)
        self.health = HealthServer(settings.health_port)
        self._last_candle_publish_bucket: int | None = None
        self._published_bars: set[tuple[str, int]] = set()

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.health.start()
        try:
            await self.client.ping()
            await self._restore_candles()
            await self._ensure_group(self.settings.book_stream, self.settings.book_group)
            await self._ensure_group(self.settings.trade_stream, self.settings.trade_group)
            self.health.ready = True
            LOGGER.info("aggregator_ready")
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
                await self._process_entries(stream, group, entries, handler)

    async def _process_entries(self, stream: str, group: str, entries, handler) -> None:
        acknowledged = []
        for redis_id, fields in entries:
            try:
                payload = fields.get(b"payload")
                if payload is None:
                    raise ValueError("missing payload")
                event = orjson.loads(payload)
                published_ts_ms = int(redis_id.split(b"-", 1)[0] if isinstance(redis_id, bytes) else str(redis_id).split("-", 1)[0])
                await handler(event, published_ts_ms)
            except (ValueError, TypeError, orjson.JSONDecodeError) as exc:
                LOGGER.warning("event_rejected", extra={"stream": stream, "redis_id": redis_id, "error": str(exc)})
            acknowledged.append(redis_id)
        if acknowledged:
            await self.client.xack(stream, group, *acknowledged)

    async def _handle_book(self, event: dict, published_ts_ms: int | None = None) -> None:
        if event.get("event_type") != "book_snapshot":
            return
        snapshot = self.state.apply_book(event, published_ts_ms=published_ts_ms)
        if snapshot is None:
            return
        encoded = orjson.dumps(snapshot)
        prefix = self.settings.output_prefix
        pipe = self.client.pipeline(transaction=False)
        pipe.set(f"{prefix}:book:BTCUSDT:latest", encoded)
        pipe.xadd(getattr(self.settings, "orderbook_stream", "stream:orderbook:v1"), {"payload": encoded}, maxlen=getattr(self.settings, "orderbook_maxlen", 10_000), approximate=True)
        pipe.publish(f"{prefix}:aggregated_orderbook", encoded)
        await pipe.execute()

    async def _handle_trade(self, event: dict, published_ts_ms: int | None = None) -> None:
        if event.get("event_type") != "trade":
            return
        spot = self.state.apply_trade(event)
        if spot is None:
            return
        encoded = orjson.dumps(spot)
        prefix = self.settings.output_prefix
        pipe = self.client.pipeline(transaction=False)
        pipe.set(f"{prefix}:spot:BTCUSDT:latest", encoded)
        pipe.publish(f"{prefix}:aggregated_spot", encoded)
        frequencies = dict(getattr(self.settings, "bar_frequencies", (("1m", 60_000), ("5m", 300_000), ("10m", 600_000), ("15m", 900_000), ("30m", 1_800_000), ("1h", 3_600_000))))
        published_bars = getattr(self, "_published_bars", None)
        if published_bars is None:
            published_bars = self._published_bars = set()
        for bar in self.state.resampled_bars("BTCUSDT", frequencies):
            key = (bar["frequency"], bar["bucket_start_ts_ms"])
            if key in published_bars:
                continue
            encoded_bar = orjson.dumps(bar)
            pipe.xadd(getattr(self.settings, "bars_stream", "stream:bars:v1"), {"payload": encoded_bar}, maxlen=getattr(self.settings, "bars_maxlen", 50_000), approximate=True)
            pipe.set(f"{prefix}:bars:BTCUSDT:{bar['frequency']}:latest", encoded_bar)
            published_bars.add(key)
        bucket = spot["bucket_start_ts_ms"]
        if bucket != self._last_candle_publish_bucket:
            candles = orjson.dumps(self.state.candle_snapshot("BTCUSDT"))
            pipe.set(f"{prefix}:candle_state:BTCUSDT:10s", orjson.dumps(self.state.export_candle_state()))
            pipe.set(f"{prefix}:candles:BTCUSDT:10s", candles)
            pipe.publish(f"{prefix}:aggregated_candles", candles)
            self._last_candle_publish_bucket = bucket
        await pipe.execute()

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self.client.xgroup_create(stream, group, id=self.settings.group_start_id, mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _restore_candles(self) -> None:
        prefix = self.settings.output_prefix
        state_key = f"{prefix}:candle_state:BTCUSDT:10s"
        raw_state = await self.client.get(state_key)
        try:
            if raw_state:
                loaded = self.state.restore_candle_state(orjson.loads(raw_state))
            else:
                raw_candles = await self.client.get(f"{prefix}:candles:BTCUSDT:10s")
                loaded = self.state.restore_candle_snapshot(orjson.loads(raw_candles)) if raw_candles else 0
                if loaded:
                    await self.client.set(state_key, orjson.dumps(self.state.export_candle_state()))
            if loaded:
                LOGGER.info("candles_restored", extra={"buckets": loaded})
        except (ValueError, TypeError, KeyError, orjson.JSONDecodeError) as exc:
            LOGGER.warning("candle_state_restore_failed", extra={"error": str(exc)})
