from gcs_exporter.kalshi_service import (
    KalshiOrderBookExporterService,
    KalshiTickerExporterService,
    KalshiTradeExporterService,
)
from test_models import kalshi_orderbook_entry, kalshi_ticker_entry, kalshi_trade_entry
from test_service import FakeConsumer, FakeHealth, FakeUploader


async def test_kalshi_ticker_flush_uploads_before_ack(settings) -> None:
    events: list[str] = []
    consumer = FakeConsumer(events)
    uploader = FakeUploader(events)
    service = KalshiTickerExporterService(settings, consumer=consumer, uploader=uploader, health=FakeHealth())

    await service._ingest([kalshi_ticker_entry()])
    await service.flush()

    assert events == ["upload", "ack"]
    assert consumer.acked == [["1724677200000-0"]]
    assert uploader.uploads[0][0].startswith("kalshi/tickers/series=KXBTCD/")


async def test_kalshi_trade_flush_uploads_before_ack(settings) -> None:
    events: list[str] = []
    consumer = FakeConsumer(events)
    uploader = FakeUploader(events)
    service = KalshiTradeExporterService(settings, consumer=consumer, uploader=uploader, health=FakeHealth())

    await service._ingest([kalshi_trade_entry()])
    await service.flush()

    assert events == ["upload", "ack"]
    assert consumer.acked == [["1724677200001-0"]]
    assert uploader.uploads[0][0].startswith("kalshi/trades/series=KXBTCD/")


async def test_kalshi_orderbook_flush_uploads_before_ack(settings) -> None:
    events: list[str] = []
    consumer = FakeConsumer(events)
    uploader = FakeUploader(events)
    service = KalshiOrderBookExporterService(settings, consumer=consumer, uploader=uploader, health=FakeHealth())

    await service._ingest([kalshi_orderbook_entry()])
    await service.flush()

    assert events == ["upload", "ack"]
    assert consumer.acked == [["1724677200002-0"]]
    assert uploader.uploads[0][0].startswith("kalshi/orderbooks/series=KXBTCD/")
