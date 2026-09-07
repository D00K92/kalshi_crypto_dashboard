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
        self.computer = V1FeatureComputer(ewma_decay=settings.ewma_decay)
        self.health = HealthServer(settings.health_port)

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.health.start()
        try:
            await self.client.ping()
            await self._restore_state()
            await self._ensure_group()
            self.health.ready = True
            LOGGER.info("live_feature_service_ready")
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

    async def _process_entries(self, entries: list[tuple[Any, Any]]) -> None:
        for entry_id, fields in entries:
            try:
                raw = fields.get(b"payload") or fields.get("payload")
                if raw is None:
                    raise ValueError("missing payload")
                bar = orjson.loads(raw)
                row = self.computer.compute(bar, now_ms=time.time_ns() // 1_000_000)
                if row is not None:
                    encoded = orjson.dumps(row.payload())
                    pipe = self.client.pipeline(transaction=False)
                    pipe.set(self.settings.feature_key, encoded)
                    pipe.set(self.settings.state_key, orjson.dumps(self.computer.snapshot()))
                    pipe.xadd(self.settings.feature_stream, {"payload": encoded}, maxlen=50_000, approximate=True)
                    await pipe.execute()
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
