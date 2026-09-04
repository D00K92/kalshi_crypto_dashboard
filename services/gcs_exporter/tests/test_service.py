from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
import orjson
from redis.exceptions import TimeoutError as RedisTimeoutError

from gcs_exporter.gcs_uploader import UploadResult
from gcs_exporter.models import RawStreamEntry
from gcs_exporter.service import GCSExporterService
from test_models import make_entry


class FakeConsumer:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.acked: list[list[str]] = []
        self.trimmed: list[tuple[str, int]] = []

    async def ready(self) -> None:
        self.events.append("ready")

    async def ensure_group(self) -> None:
        self.events.append("ensure_group")

    async def read_new(self, count: int, block_ms: int) -> list[RawStreamEntry]:
        return []

    async def reclaim(self, min_idle_ms: int, count: int) -> list[RawStreamEntry]:
        return []

    async def ack(self, redis_ids: list[str]) -> int:
        self.events.append("ack")
        self.acked.append(redis_ids)
        return len(redis_ids)

    async def trim_exported(
        self, latest_exported_id: str, retention_seconds: int
    ) -> int:
        self.events.append("trim")
        self.trimmed.append((latest_exported_id, retention_seconds))
        return 0

    async def close(self) -> None:
        self.events.append("close")


class FakeUploader:
    def __init__(self, events: list[str], failure: Exception | None = None) -> None:
        self.events = events
        self.failure = failure
        self.uploads: list[tuple[str, bytes, str]] = []

    async def upload(
        self, object_name: str, data: bytes, content_type: str
    ) -> UploadResult:
        self.events.append("upload")
        if self.failure:
            raise self.failure
        self.uploads.append((object_name, data, content_type))
        return UploadResult(object_name, already_existed=False)


class FakeHealth:
    async def start(self) -> None:
        pass

    def mark_ready(self) -> None:
        pass

    def mark_not_ready(self) -> None:
        pass

    async def close(self) -> None:
        pass


def make_service(settings, failure: Exception | None = None):
    events: list[str] = []
    consumer = FakeConsumer(events)
    uploader = FakeUploader(events, failure)
    service = GCSExporterService(
        settings, consumer=consumer, uploader=uploader, health=FakeHealth()  # type: ignore[arg-type]
    )
    return service, consumer, uploader, events


async def test_flush_uploads_before_acknowledging(settings) -> None:
    service, consumer, uploader, events = make_service(settings)
    await service._ingest([make_entry()])

    await service.flush()

    assert events == ["upload", "ack"]
    assert consumer.acked == [["1724677200000-0"]]
    assert len(uploader.uploads) == 1
    assert service.buffered_rows == 0


async def test_upload_failure_leaves_message_unacked_and_buffered(settings) -> None:
    service, consumer, _, events = make_service(settings, RuntimeError("GCS down"))
    await service._ingest([make_entry()])

    with pytest.raises(RuntimeError, match="GCS down"):
        await service.flush()

    assert events == ["upload"]
    assert consumer.acked == []
    assert service.buffered_rows == 1


async def test_malformed_entry_is_dead_lettered_before_ack(settings) -> None:
    service, consumer, uploader, events = make_service(settings)
    malformed = RawStreamEntry("99-0", {b"payload": b"not-json"})

    await service._ingest([malformed])

    assert events == ["upload", "ack"]
    assert uploader.uploads[0][0].startswith("dead-letter/stream=ticks/")
    assert consumer.acked == [["99-0"]]
    assert service.buffered_rows == 0


async def test_excluded_venue_is_acknowledged_without_upload(settings) -> None:
    settings = replace(settings, excluded_venues=frozenset({"bybit"}))
    service, consumer, uploader, events = make_service(settings)
    entry = make_entry()
    payload = orjson.loads(entry.payload_bytes())
    payload["venue"] = "bybit"
    entry = RawStreamEntry(entry.redis_id, {b"payload": orjson.dumps(payload)})

    await service._ingest([entry])

    assert events == ["ack"]
    assert consumer.acked == [[entry.redis_id]]
    assert uploader.uploads == []
    assert service.buffered_rows == 0


async def test_flush_splits_rows_across_utc_hour_partitions(settings) -> None:
    service, consumer, uploader, _ = make_service(settings)
    first_hour = 1_724_677_200_000
    second_hour = first_hour + 3_600_000
    await service._ingest(
        [
            make_entry("1-0", first_hour),
            make_entry("2-0", second_hour),
        ]
    )

    await service.flush()

    assert len(uploader.uploads) == 2
    assert len(consumer.acked) == 2
    assert "/hour=13/" in uploader.uploads[0][0]
    assert "/hour=14/" in uploader.uploads[1][0]


async def test_count_trigger_flushes_at_threshold(settings) -> None:
    settings = replace(settings, flush_size=2)
    service, consumer, uploader, events = make_service(settings)

    await service._ingest([make_entry("1-0"), make_entry("2-0")])

    assert len(uploader.uploads) == 1
    assert consumer.acked == [["1-0", "2-0"]]
    assert events == ["upload", "ack"]


async def test_time_trigger_uses_age_of_oldest_buffered_row(settings) -> None:
    service, _, _, _ = make_service(settings)
    await service._ingest([make_entry()])
    assert service._buffer_started_at is not None

    # Add a tiny epsilon: monotonic timestamps are floats, so subtracting an
    # exact boundary can round just below the configured interval.
    assert service._should_flush(
        service._buffer_started_at + settings.flush_interval_seconds + 1e-6
    )


async def test_transient_redis_timeout_does_not_stop_service(
    settings, monkeypatch
) -> None:
    events: list[str] = []
    stop_event = asyncio.Event()

    class TransientConsumer(FakeConsumer):
        calls = 0

        async def read_new(self, count: int, block_ms: int) -> list[RawStreamEntry]:
            self.calls += 1
            if self.calls == 1:
                raise RedisTimeoutError("temporary timeout")
            stop_event.set()
            return []

    consumer = TransientConsumer(events)
    service = GCSExporterService(
        settings,
        consumer=consumer,
        uploader=FakeUploader(events),
        health=FakeHealth(),  # type: ignore[arg-type]
    )

    async def no_wait(*args) -> None:
        return None

    monkeypatch.setattr(service, "_wait_for_retry", no_wait)
    await service.run(stop_event)

    assert consumer.calls == 2
