from __future__ import annotations

from gcs_exporter.redis_consumer import RedisStreamConsumer


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
