from __future__ import annotations

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

from ingestion.models import Trade
from ingestion.pipeline.redis_publisher import RedisPublisher


class RecordingRedis:
    def __init__(self, failures: int = 0) -> None:
        self.calls: list[tuple[str, object]] = []
        self.failures = failures

    def xadd(self, stream: str, fields: dict[str, str], **kwargs: object) -> "RecordingRedis":
        self.calls.append(("xadd", stream))
        return self

    def pipeline(self, **kwargs: object) -> "RecordingRedis":
        self.calls.append(("pipeline", kwargs))
        return self

    async def execute(self) -> list[object]:
        self.calls.append(("execute", None))
        if self.failures:
            self.failures -= 1
            raise RedisTimeoutError("temporary Redis timeout")
        return []

    def publish(self, channel: str, payload: bytes) -> "RecordingRedis":
        self.calls.append(("publish", channel))
        return self


async def test_stream_append_precedes_best_effort_pubsub() -> None:
    publisher = RedisPublisher("redis://unused", stream_maxlen=100)
    fake = RecordingRedis()
    publisher._client = fake  # type: ignore[assignment]
    event = Trade(
        event_id="binance:BTCUSDT:trade:1",
        event_type="trade",
        venue="binance",
        instrument="BTCUSDT",
        trade_id="1",
        price="100000",
        quantity="0.1",
        taker_side="buy",
        exchange_ts_ms=1,
        received_ts_ms=2,
    )

    await publisher.publish(event)

    assert fake.calls == [
        ("pipeline", {"transaction": False}),
        ("xadd", "stream:ticks"),
        ("publish", "pub:btc_ticks"),
        ("execute", None),
    ]


async def test_timeout_rebuilds_pipeline_and_retries_same_batch() -> None:
    publisher = RedisPublisher(
        "redis://unused",
        stream_maxlen=100,
        retry_base_seconds=0,
    )
    fake = RecordingRedis(failures=1)
    publisher._client = fake  # type: ignore[assignment]
    event = Trade(
        event_id="binance:BTCUSDT:trade:1",
        event_type="trade",
        venue="binance",
        instrument="BTCUSDT",
        trade_id="1",
        price="100000",
        quantity="0.1",
        taker_side="buy",
        exchange_ts_ms=1,
        received_ts_ms=2,
    )

    await publisher.publish(event)

    assert [call[0] for call in fake.calls].count("pipeline") == 2
    assert [call[0] for call in fake.calls].count("xadd") == 2
    assert [call[0] for call in fake.calls].count("execute") == 2


async def test_retry_limit_raises_after_bounded_attempts() -> None:
    publisher = RedisPublisher(
        "redis://unused",
        stream_maxlen=100,
        retry_limit=3,
        retry_base_seconds=0,
    )
    fake = RecordingRedis(failures=3)
    publisher._client = fake  # type: ignore[assignment]
    event = Trade(
        event_id="binance:BTCUSDT:trade:1",
        event_type="trade",
        venue="binance",
        instrument="BTCUSDT",
        trade_id="1",
        price="100000",
        quantity="0.1",
        taker_side="buy",
        exchange_ts_ms=1,
        received_ts_ms=2,
    )

    with pytest.raises(RedisTimeoutError):
        await publisher.publish(event)

    assert [call[0] for call in fake.calls].count("pipeline") == 3
    assert [call[0] for call in fake.calls].count("execute") == 3
