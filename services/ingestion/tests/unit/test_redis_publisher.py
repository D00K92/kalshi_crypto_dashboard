from __future__ import annotations

from ingestion.models import Trade
from ingestion.pipeline.redis_publisher import RedisPublisher


class RecordingRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def xadd(self, stream: str, fields: dict[str, str], **kwargs: object) -> str:
        self.calls.append(("xadd", stream))
        return "1-0"

    async def publish(self, channel: str, payload: bytes) -> int:
        self.calls.append(("publish", channel))
        return 1


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
        ("xadd", "stream:ticks"),
        ("publish", "pub:btc_ticks"),
    ]
