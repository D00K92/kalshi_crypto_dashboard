from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import orjson
import redis.asyncio as redis
from redis.exceptions import ResponseError

from .computation import V1FeatureComputer
from .config import Settings
from .health import HealthServer

LOGGER = logging.getLogger(__name__)


class LiveFeatureService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = redis.Redis.from_url(settings.redis_url, decode_responses=False, health_check_interval=30)
        self.computer = V1FeatureComputer(
            ewma_decay=settings.ewma_decay,
            max_bar_age_ms=settings.max_bar_age_ms,
            history_bars=settings.history_bars,
        )
        self.health = HealthServer(settings.health_port)
        self._processed = 0

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.health.start()
        try:
            await self.client.ping()
            await self._restore_state()
            await self._replay_history()
            await self._ensure_group()
            self.health.ready = True
            LOGGER.info("live_feature_service_ready history_bars=%s", self.computer.history_count)
            while not stop_event.is_set():
                rows = await self.client.xreadgroup(
                    self.settings.consumer_group, self.settings.consumer_name,
                    {self.settings.bars_stream: ">"}, count=100, block=1_000,
                )
                for _, entries in rows:
                    await self._process_entries(entries)
        finally:
            self.health.ready = False
            await self.health.close()
            await self.client.aclose()

    async def _ensure_group(self) -> None:
        try:
            await self.client.xgroup_create(self.settings.bars_stream, self.settings.consumer_group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _replay_history(self) -> None:
        """Warm the rolling window from Redis before consuming new entries."""
        entries = await self.client.xrevrange(self.settings.bars_stream, max="+", min="-", count=self.settings.replay_count)
        for entry_id, fields in reversed(entries):
            raw = fields.get(b"payload") or fields.get("payload")
            if raw is None:
                continue
            try:
                self.computer.compute(orjson.loads(raw), now_ms=time.time_ns() // 1_000_000, replay=True)
            except (ValueError, TypeError, KeyError, orjson.JSONDecodeError):
                continue
        await self.client.set(self.settings.state_key, orjson.dumps(self.computer.snapshot()))
        if self.computer.history_count < 361:
            LOGGER.warning("live_feature_warmup_incomplete history_bars=%s required=361", self.computer.history_count)

    async def _process_entries(self, entries: list[tuple[Any, Any]]) -> None:
        for entry_id, fields in entries:
            try:
                raw = fields.get(b"payload") or fields.get("payload")
                if raw is None:
                    raise ValueError("missing payload")
                bar = orjson.loads(raw)
                row = self.computer.compute(bar, now_ms=time.time_ns() // 1_000_000)
                encoded = orjson.dumps(row.payload()) if row is not None else None
                pipe = self.client.pipeline(transaction=False)
                if encoded is not None:
                    pipe.set(self.settings.feature_key, encoded)
                    pipe.xadd(self.settings.feature_stream, {"payload": encoded}, maxlen=50_000, approximate=True)
                pipe.set(self.settings.state_key, orjson.dumps(self.computer.snapshot()))
                await pipe.execute()
                self._processed += 1
                if self._processed % 1000 == 0:
                    await self.client.xtrim(self.settings.bars_stream, maxlen=self.settings.primitive_maxlen, approximate=True)
                await self.client.xack(self.settings.bars_stream, self.settings.consumer_group, entry_id)
            except (ValueError, TypeError, KeyError, orjson.JSONDecodeError) as exc:
                LOGGER.warning("live_feature_rejected entry_id=%s error=%s", entry_id, exc)
                await self.client.xack(self.settings.bars_stream, self.settings.consumer_group, entry_id)
            except Exception:
                LOGGER.exception("live_feature_publish_failed entry_id=%s", entry_id)
                # Leave transient failures pending for a later recovery pass.

    async def _restore_state(self) -> None:
        raw = await self.client.get(self.settings.state_key)
        if raw:
            self.computer.restore(orjson.loads(raw))


async def run() -> None:
    service = LiveFeatureService(Settings.from_env())
    stop_event = asyncio.Event()
    await service.run(stop_event)
