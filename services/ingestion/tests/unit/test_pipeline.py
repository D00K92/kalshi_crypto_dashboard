from __future__ import annotations

import asyncio

from ingestion.models import Trade
from ingestion.pipeline.event_pipeline import EventPipeline


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[Trade] = []

    async def publish(self, event: Trade) -> None:
        self.events.append(event)


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
