from __future__ import annotations

import asyncio

from ingestion.models import BookSnapshot, Trade
from ingestion.pipeline.event_pipeline import EventPipeline


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def publish(self, event: Trade) -> None:
        self.events.append(event)

    async def publish_many(self, events: list[object]) -> None:
        self.events.extend(events)


async def test_pipeline_delivers_and_drains() -> None:
    publisher = RecordingPublisher()
    pipeline = EventPipeline(publisher, maxsize=1)  # type: ignore[arg-type]
    worker = asyncio.create_task(pipeline.run())
    event = Trade(
        event_id="binance:BTCUSDT:trade:1",
        event_type="trade",
        venue="binance",
        instrument="BTCUSDT",
        trade_id="1",
        price="100000",
        quantity="0.1",
        taker_side="sell",
        exchange_ts_ms=1,
        received_ts_ms=2,
    )

    try:
        await pipeline.put(event)
        await pipeline.drain()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert publisher.events == [event]
    assert pipeline.queued_events == 0


async def test_pipeline_prioritizes_trade_over_book_snapshot() -> None:
    publisher = RecordingPublisher()
    pipeline = EventPipeline(publisher, maxsize=4)  # type: ignore[arg-type]
    low = BookSnapshot(event_id="low", event_type="book_snapshot", venue="x", instrument="x", sequence=1, bids=(), asks=(), exchange_ts_ms=1, received_ts_ms=2, depth=1)
    high = Trade(event_id="high", event_type="trade", venue="x", instrument="x", trade_id="high", price="1", quantity="1", taker_side="buy", exchange_ts_ms=1, received_ts_ms=2)
    await pipeline.put(low)
    await pipeline.put(high)
    worker = asyncio.create_task(pipeline.run())
    try:
        await pipeline.drain()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    assert publisher.events == [high, low]
