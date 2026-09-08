from __future__ import annotations

import asyncio
import logging
import time

import orjson
import redis.asyncio as redis
from redis.exceptions import ResponseError

from aggregator.aggregation import CANDLE_INTERVAL_MS, MarketAggregator
from aggregator.config import Settings
from aggregator.health import HealthServer

LOGGER = logging.getLogger(__name__)


class AggregatorService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = redis.Redis.from_url(settings.redis_url, decode_responses=False, health_check_interval=30)
        self.state = MarketAggregator(settings.price_tick, settings.book_depth, settings.freshness_ms, settings.aggregation_venues, dict(settings.taker_fees), settings.trade_freshness_ms, settings.history_ms)
        self.health = HealthServer(settings.health_port)
        self._max_trade_event_ts_ms: int | None = None
        self._last_finalized_primitive_bucket: int | None = None
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
            stop_task = asyncio.create_task(stop_event.wait())
            done, _ = await asyncio.wait(
                {book_task, trade_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if stop_task not in done:
                # Redis transactions and acknowledgements are atomic below.
                # Exit on a consumer failure so the pod restarts from the last
                # committed checkpoint instead of continuing with dirty RAM.
                next(task for task in done if task is not stop_task).result()
        finally:
            self.health.ready = False
            for task in (locals().get("book_task"), locals().get("trade_task"), locals().get("stop_task")):
                if task:
                    task.cancel()
            await asyncio.gather(*(task for task in (locals().get("book_task"), locals().get("trade_task"), locals().get("stop_task")) if task), return_exceptions=True)
            await self.health.close()
            await self.client.aclose()

    async def _consume(self, stream: str, group: str, handler) -> None:
        while True:
            claimed = await self.client.xautoclaim(
                stream,
                group,
                self.settings.consumer_name,
                min_idle_time=self.settings.pending_idle_ms,
                start_id="0-0",
                count=self.settings.read_count,
            )
            if len(claimed) > 1 and claimed[1]:
                await self._process_entries(stream, group, claimed[1], handler)
            rows = await self.client.xreadgroup(group, self.settings.consumer_name, {stream: ">"}, count=self.settings.read_count, block=self.settings.read_block_ms)
            for _, entries in rows:
                await self._process_entries(stream, group, entries, handler)

    async def _process_entries(self, stream: str, group: str, entries, handler) -> None:
        acknowledged = []
        pipeline = self.client.pipeline(transaction=True)
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
        if stream == self.settings.trade_stream and has_writes:
            # Checkpoint the open event-time buckets in the same Redis
            # transaction as their outputs and acknowledgement. A restart can
            # therefore never acknowledge trades that were not persisted.
            pipeline.set(
                f"{self.settings.output_prefix}:candle_state:BTCUSDT:10s",
                orjson.dumps(self.state.export_candle_state()),
            )
            pipeline.set(
                f"{self.settings.output_prefix}:primitive_watermark:BTCUSDT:10s",
                orjson.dumps(
                    {
                        "schema_version": 1,
                        "max_event_ts_ms": self._max_trade_event_ts_ms,
                        "finalized_through_ms": self._last_finalized_primitive_bucket,
                    }
                ),
            )
        if acknowledged:
            pipeline.xack(stream, group, *acknowledged)
            has_writes = True
        if has_writes:
            await pipeline.execute()

    async def _handle_book(self, event: dict, published_ts_ms: int | None = None, pipeline=None) -> bool:
        if event.get("event_type") != "book_snapshot":
            return False
        if self.state.apply_book(event, published_ts_ms=published_ts_ms) is None:
            return False
        snapshot = self.state.primitive_book_snapshot(event, published_ts_ms or int(time.time() * 1000))
        encoded = orjson.dumps(snapshot)
        prefix = self.settings.output_prefix
        pipe = pipeline or self.client.pipeline(transaction=True)
        pipe.set(f"{prefix}:book:BTCUSDT:latest", encoded)
        pipe.xadd(getattr(self.settings, "orderbook_stream", "stream:orderbook:v1"), {"payload": encoded}, maxlen=getattr(self.settings, "orderbook_maxlen", 10_000), approximate=True)
        if pipeline is None:
            await pipe.execute()
        return True

    async def _handle_trade(self, event: dict, published_ts_ms: int | None = None, pipeline=None) -> bool:
        if event.get("event_type") != "trade":
            return False
        event_ts_ms = int(event.get("exchange_ts_ms") or event.get("received_ts_ms") or published_ts_ms or time.time() * 1000)
        event_bucket = (event_ts_ms // CANDLE_INTERVAL_MS) * CANDLE_INTERVAL_MS
        if (
            self._last_finalized_primitive_bucket is not None
            and event_bucket <= self._last_finalized_primitive_bucket
        ):
            LOGGER.warning(
                "late_trade_dropped event_id=%s venue=%s bucket=%s finalized_through=%s",
                event.get("event_id"),
                event.get("venue"),
                event_bucket,
                self._last_finalized_primitive_bucket,
            )
            return False
        spot = self.state.apply_trade(event)
        if spot is None:
            return False
        self._max_trade_event_ts_ms = max(
            event_ts_ms, self._max_trade_event_ts_ms or event_ts_ms
        )
        encoded = orjson.dumps(spot)
        prefix = self.settings.output_prefix
        pipe = pipeline or self.client.pipeline(transaction=True)
        pipe.set(f"{prefix}:spot:BTCUSDT:latest", encoded)
        pipe.publish(f"{prefix}:aggregated_spot", encoded)
        watermark_ms = self._max_trade_event_ts_ms - self.settings.allowed_lateness_ms
        closed_through = (
            (watermark_ms // CANDLE_INTERVAL_MS) * CANDLE_INTERVAL_MS
            - CANDLE_INTERVAL_MS
        )
        first_bucket = min(self.state.trade_buckets, default=None)
        can_advance = first_bucket is not None and closed_through >= first_bucket
        if self._last_finalized_primitive_bucket is not None:
            can_advance = closed_through > self._last_finalized_primitive_bucket
        if can_advance:
            lower_bound = (
                self._last_finalized_primitive_bucket + CANDLE_INTERVAL_MS
                if self._last_finalized_primitive_bucket is not None
                else first_bucket
            )
            closed_buckets = sorted(
                start
                for start in self.state.trade_buckets
                if lower_bound <= start <= closed_through
            )
            for start in closed_buckets:
                for primitive in self.state.primitive_bars_for_bucket("BTCUSDT", start):
                    encoded_primitive = orjson.dumps(primitive)
                    pipe.xadd(getattr(self.settings, "primitive_stream", "stream:primitives:v1"), {"payload": encoded_primitive}, maxlen=getattr(self.settings, "primitive_maxlen", 50_000), approximate=True)
                    pipe.set(f"{prefix}:primitive:{primitive['venue']}:{primitive['frequency']}:latest", encoded_primitive)
                self.state.mark_primitive_bucket_finalized(start)
            # Empty intervals are finalized too; otherwise a later delayed
            # trade could reopen a gap and make the output stream regress.
            self._last_finalized_primitive_bucket = closed_through
            # Keep the dashboard's public 30-second contract while deriving it
            # from the 10-second canonical state.
            candles = orjson.dumps(self.state.candle_snapshot("BTCUSDT", interval_ms=30_000))
            pipe.set(f"{prefix}:candles:BTCUSDT:30s", candles)
            pipe.publish(f"{prefix}:aggregated_candles", candles)
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
        state_key = f"{prefix}:candle_state:BTCUSDT:10s"
        raw_state, raw_watermark = await self.client.mget(
            state_key, f"{prefix}:primitive_watermark:BTCUSDT:10s"
        )
        try:
            if raw_state:
                loaded = self.state.restore_candle_state(orjson.loads(raw_state))
            else:
                raw_candles = await self.client.get(f"{prefix}:candles:BTCUSDT:10s")
                loaded = self.state.restore_candle_snapshot(orjson.loads(raw_candles)) if raw_candles else 0
                if loaded:
                    await self.client.set(state_key, orjson.dumps(self.state.export_candle_state()))
            if loaded:
                progress = orjson.loads(raw_watermark) if raw_watermark is not None else None
                if isinstance(progress, dict):
                    max_event_ts_ms = progress.get("max_event_ts_ms")
                    finalized_through_ms = progress.get("finalized_through_ms")
                    self._max_trade_event_ts_ms = None if max_event_ts_ms is None else int(max_event_ts_ms)
                    self._last_finalized_primitive_bucket = None if finalized_through_ms is None else int(finalized_through_ms)
                else:
                    # Legacy deployments stored only an integer watermark.
                    self._max_trade_event_ts_ms = max(self.state.trade_buckets)
                    self._last_finalized_primitive_bucket = (
                        int(progress)
                        if progress is not None
                        else max(self.state.trade_buckets) - CANDLE_INTERVAL_MS
                    )
                LOGGER.info("candles_restored", extra={"buckets": loaded})
                frequencies = dict(getattr(self.settings, "bar_frequencies", (("10s", 10_000),)))
                self._published_bars.update(
                    (bar["frequency"], bar["bucket_start_ts_ms"])
                    for bar in self.state.resampled_bars("BTCUSDT", frequencies)
                )
        except (ValueError, TypeError, KeyError, orjson.JSONDecodeError) as exc:
            LOGGER.warning("candle_state_restore_failed", extra={"error": str(exc)})
