"""Exporter orchestration and ACK-after-upload lifecycle."""

from __future__ import annotations

import asyncio
from collections import defaultdict
import logging
import time
from typing import Protocol

import orjson

from gcs_exporter.config import Settings
from gcs_exporter.gcs_uploader import GCSUploader, UploadResult
from gcs_exporter.health import HealthServer
from gcs_exporter.models import RawStreamEntry, TradeRow
from gcs_exporter.object_names import dead_letter_object_name, trade_object_name
from gcs_exporter.parquet_writer import write_trades
from gcs_exporter.redis_consumer import RedisStreamConsumer


LOGGER = logging.getLogger(__name__)


class Consumer(Protocol):
    async def ready(self) -> None: ...
    async def ensure_group(self) -> None: ...
    async def read_new(self, count: int, block_ms: int) -> list[RawStreamEntry]: ...
    async def reclaim(self, min_idle_ms: int, count: int) -> list[RawStreamEntry]: ...
    async def ack(self, redis_ids: list[str]) -> int: ...
    async def close(self) -> None: ...


class Uploader(Protocol):
    async def upload(
        self, object_name: str, data: bytes, content_type: str
    ) -> UploadResult: ...


class GCSExporterService:
    def __init__(
        self,
        settings: Settings,
        consumer: Consumer | None = None,
        uploader: Uploader | None = None,
        health: HealthServer | None = None,
    ) -> None:
        self._settings = settings
        self._consumer = consumer or RedisStreamConsumer(
            settings.redis_url,
            settings.stream_name,
            settings.consumer_group,
            settings.consumer_name,
        )
        self._uploader = uploader or GCSUploader(settings.bucket_name)
        self._health = health or HealthServer(settings.health_port)
        self._buffer: list[TradeRow] = []
        self._buffer_started_at: float | None = None
        self._last_reclaim_at = 0.0

    @property
    def buffered_rows(self) -> int:
        return len(self._buffer)

    async def run(self, stop_event: asyncio.Event) -> None:
        await self._health.start()
        try:
            await self._consumer.ready()
            await self._consumer.ensure_group()
            self._health.mark_ready()
            LOGGER.info(
                "gcs_exporter_ready",
                extra={
                    "stream": self._settings.stream_name,
                    "group": self._settings.consumer_group,
                    "consumer": self._settings.consumer_name,
                    "bucket": self._settings.bucket_name,
                },
            )
            while not stop_event.is_set():
                now = time.monotonic()
                if now - self._last_reclaim_at >= self._settings.reclaim_interval_seconds:
                    reclaimed = await self._consumer.reclaim(
                        self._settings.reclaim_min_idle_ms,
                        self._settings.read_count,
                    )
                    self._last_reclaim_at = now
                    await self._ingest(reclaimed)

                capacity = max(1, self._settings.flush_size - len(self._buffer))
                entries = await self._consumer.read_new(
                    min(self._settings.read_count, capacity),
                    self._settings.read_block_ms,
                )
                await self._ingest(entries)
                if self._should_flush(time.monotonic()):
                    await self.flush()
        finally:
            self._health.mark_not_ready()
            try:
                async with asyncio.timeout(self._settings.shutdown_grace_seconds):
                    await self.flush()
            except TimeoutError:
                LOGGER.error(
                    "shutdown_flush_timed_out",
                    extra={"buffered_rows": len(self._buffer)},
                )
            finally:
                await self._consumer.close()
                await self._health.close()
                LOGGER.info(
                    "gcs_exporter_stopped",
                    extra={"uncommitted_rows": len(self._buffer)},
                )

    async def _ingest(self, entries: list[RawStreamEntry]) -> None:
        for entry in entries:
            try:
                row = TradeRow.from_entry(entry)
            except (UnicodeDecodeError, ValueError) as exc:
                await self._archive_malformed(entry, str(exc))
                continue
            if not self._buffer:
                self._buffer_started_at = time.monotonic()
            self._buffer.append(row)
            if len(self._buffer) >= self._settings.flush_size:
                await self.flush()

    def _should_flush(self, now: float) -> bool:
        if not self._buffer or self._buffer_started_at is None:
            return False
        return (
            len(self._buffer) >= self._settings.flush_size
            or now - self._buffer_started_at >= self._settings.flush_interval_seconds
        )

    async def flush(self) -> None:
        if not self._buffer:
            return
        groups: dict[tuple[str, str, str, str], list[TradeRow]] = defaultdict(list)
        for row in self._buffer:
            groups[row.partition].append(row)

        for rows in groups.values():
            started_at = time.monotonic()
            parquet_bytes = write_trades(rows)
            object_name = trade_object_name(rows)
            result = await self._uploader.upload(
                object_name,
                parquet_bytes,
                "application/vnd.apache.parquet",
            )
            redis_ids = [row.redis_id for row in rows]
            await self._consumer.ack(redis_ids)
            committed = set(redis_ids)
            self._buffer = [
                row for row in self._buffer if row.redis_id not in committed
            ]
            LOGGER.info(
                "parquet_batch_committed",
                extra={
                    "object": result.object_name,
                    "already_existed": result.already_existed,
                    "rows": len(rows),
                    "bytes": len(parquet_bytes),
                    "elapsed_seconds": time.monotonic() - started_at,
                },
            )

        self._buffer_started_at = time.monotonic() if self._buffer else None

    async def _archive_malformed(
        self, entry: RawStreamEntry, error: str
    ) -> None:
        try:
            payload = entry.payload_bytes()
        except ValueError:
            payload = b""
        raw_fields = {
            self._decode_lossy(key): self._decode_lossy(value)
            for key, value in entry.fields.items()
        }
        document = orjson.dumps(
            {
                "redis_id": entry.redis_id,
                "error": error,
                "fields": raw_fields,
            },
            option=orjson.OPT_SORT_KEYS,
        )
        object_name = dead_letter_object_name(entry.redis_id, payload)
        await self._uploader.upload(object_name, document, "application/json")
        await self._consumer.ack([entry.redis_id])
        LOGGER.warning(
            "malformed_entry_archived",
            extra={"redis_id": entry.redis_id, "object": object_name, "error": error},
        )

    @staticmethod
    def _decode_lossy(value: bytes | str) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value
