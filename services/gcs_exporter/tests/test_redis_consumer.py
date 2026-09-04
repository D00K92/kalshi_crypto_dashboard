from __future__ import annotations

from gcs_exporter.redis_consumer import RedisStreamConsumer


class FakeRedis:
    def __init__(self, *, pending: int = 0, pending_min: str | None = None) -> None:
        self.pending = pending
        self.pending_min = pending_min
        self.trim_calls: list[tuple[str, str, bool]] = []

    async def xinfo_groups(self, stream: str):
        return [
            {
                "name": "gcs_archiver_group",
                "last-delivered-id": "1900000-0",
            }
        ]

    async def xpending(self, stream: str, group: str):
        return {"pending": self.pending, "min": self.pending_min}

    async def xtrim(self, stream: str, *, minid: str, approximate: bool):
        self.trim_calls.append((stream, minid, approximate))
        return 42


def test_flatten_normalizes_binary_redis_ids() -> None:
    response = [
        (
            b"stream:ticks",
            [(b"1724677200000-0", {b"payload": b"{}"})],
        )
    ]

    entries = RedisStreamConsumer._flatten(response)

    assert entries[0].redis_id == "1724677200000-0"
    assert entries[0].fields == {b"payload": b"{}"}


async def test_trim_exported_keeps_retention_window() -> None:
    consumer = RedisStreamConsumer(
        "redis://unused", "stream:ticks", "gcs_archiver_group", "consumer"
    )
    fake = FakeRedis()
    consumer._client = fake  # type: ignore[assignment]

    removed = await consumer.trim_exported("2000000-4", 900)

    assert removed == 42
    assert fake.trim_calls == [("stream:ticks", "1100000-0", True)]


async def test_trim_exported_does_not_cross_oldest_pending_entry() -> None:
    consumer = RedisStreamConsumer(
        "redis://unused", "stream:ticks", "gcs_archiver_group", "consumer"
    )
    fake = FakeRedis(pending=3, pending_min="1050000-2")
    consumer._client = fake  # type: ignore[assignment]

    await consumer.trim_exported("2000000-4", 900)

    assert fake.trim_calls == [("stream:ticks", "1050000-2", True)]
