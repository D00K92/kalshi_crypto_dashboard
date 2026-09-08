from __future__ import annotations

import asyncio
import logging
import time

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
        self._published_primitives: set[tuple[str, str, int]] = set()

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
        pipeline = self.client.pipeline(transaction=False)
        has_writes = False
        for redis_id, fields in entries:
            try:
                payload = fields.get(b"payload")
                if payload is None:
                    raise ValueError("missing payload")
                event = orjson.loads(payload)
                published_ts_ms = int(redis_id.split(b"-", 1)[0] if isinstance(redis_id, bytes) else str(redis_id).split("-", 1)[0])
                has_writes = (await handler(event, published_ts_ms, pipeline)) or has_writes
            except (ValueError, TypeError, orjson.JSONDecodeError) as exc:
                LOGGER.warning(
                    "event_rejected stream=%s redis_id=%s error_type=%s error=%s",
                    stream,
                    redis_id,
                    type(exc).__name__,
                    str(exc),
                )
            acknowledged.append(redis_id)
        if has_writes:
            await pipeline.execute()
        if acknowledged:
            await self.client.xack(stream, group, *acknowledged)

    async def _handle_book(self, event: dict, published_ts_ms: int | None = None, pipeline=None) -> bool:
        if event.get("event_type") != "book_snapshot":
            return False
        if self.state.apply_book(event, published_ts_ms=published_ts_ms) is None:
            return False
        snapshot = self.state.primitive_book_snapshot(event, published_ts_ms or int(time.time() * 1000))
        encoded = orjson.dumps(snapshot)
        prefix = self.settings.output_prefix
        pipe = pipeline or self.client.pipeline(transaction=False)
        pipe.set(f"{prefix}:book:BTCUSDT:latest", encoded)
        pipe.xadd(getattr(self.settings, "orderbook_stream", "stream:orderbook:v1"), {"payload": encoded}, maxlen=getattr(self.settings, "orderbook_maxlen", 10_000), approximate=True)
        if pipeline is None:
            await pipe.execute()
        return True

    async def _handle_trade(self, event: dict, published_ts_ms: int | None = None, pipeline=None) -> bool:
        if event.get("event_type") != "trade":
            return False
        spot = self.state.apply_trade(event)
        if spot is None:
            return False
        encoded = orjson.dumps(spot)
        prefix = self.settings.output_prefix
        pipe = pipeline or self.client.pipeline(transaction=False)
        pipe.set(f"{prefix}:spot:BTCUSDT:latest", encoded)
        pipe.publish(f"{prefix}:aggregated_spot", encoded)
        bucket = spot["bucket_start_ts_ms"]
        if bucket != self._last_candle_publish_bucket:
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
            published_primitives = getattr(self, "_published_primitives", None)
            if published_primitives is None:
                published_primitives = self._published_primitives = set()
            for primitive in self.state.primitive_bars("BTCUSDT", frequencies):
                key = (primitive["venue"], primitive["frequency"], primitive["bucket_start_ts_ms"])
                if key in published_primitives:
                    continue
                published_primitives.add(key)
                encoded_primitive = orjson.dumps(primitive)
                pipe.xadd(getattr(self.settings, "primitive_stream", "stream:primitives:v1"), {"payload": encoded_primitive}, maxlen=getattr(self.settings, "primitive_maxlen", 50_000), approximate=True)
                pipe.set(f"{prefix}:primitive:{primitive['venue']}:{primitive['frequency']}:latest", encoded_primitive)
            candles = orjson.dumps(self.state.candle_snapshot("BTCUSDT"))
            pipe.set(f"{prefix}:candle_state:BTCUSDT:30s", orjson.dumps(self.state.export_candle_state()))
            pipe.set(f"{prefix}:candles:BTCUSDT:30s", candles)
            pipe.publish(f"{prefix}:aggregated_candles", candles)
            self._last_candle_publish_bucket = bucket
        if pipeline is None:
            await pipe.execute()
        return True

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self.client.xgroup_create(stream, group, id=self.settings.group_start_id, mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _restore_candles(self) -> None:
        prefix = self.settings.output_prefix
        state_key = f"{prefix}:candle_state:BTCUSDT:30s"
        raw_state = await self.client.get(state_key)
        try:
            if raw_state:
                loaded = self.state.restore_candle_state(orjson.loads(raw_state))
            else:
                raw_candles = await self.client.get(f"{prefix}:candles:BTCUSDT:30s")
                loaded = self.state.restore_candle_snapshot(orjson.loads(raw_candles)) if raw_candles else 0
                if loaded:
                    await self.client.set(state_key, orjson.dumps(self.state.export_candle_state()))
            if loaded:
                LOGGER.info("candles_restored", extra={"buckets": loaded})
                frequencies = dict(getattr(self.settings, "bar_frequencies", (("1m", 60_000), ("5m", 300_000), ("10m", 600_000), ("15m", 900_000), ("30m", 1_800_000), ("1h", 3_600_000))))
                self._published_bars.update(
                    (bar["frequency"], bar["bucket_start_ts_ms"])
                    for bar in self.state.resampled_bars("BTCUSDT", frequencies)
                )
        except (ValueError, TypeError, KeyError, orjson.JSONDecodeError) as exc:
            LOGGER.warning("candle_state_restore_failed", extra={"error": str(exc)})
