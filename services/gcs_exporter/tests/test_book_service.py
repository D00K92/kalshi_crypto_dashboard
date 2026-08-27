import orjson

from gcs_exporter.book_service import OrderBookExporterService
from gcs_exporter.models import RawStreamEntry
from test_service import FakeConsumer, FakeHealth, FakeUploader


def make_book_entry() -> RawStreamEntry:
    payload = {
        "event_id": "bybit:BTCUSDT:book:42", "event_type": "book_snapshot",
        "venue": "bybit", "instrument": "BTCUSDT", "sequence": 42,
        "bids": [{"price": "100", "quantity": "1"}],
        "asks": [{"price": "101", "quantity": "2"}],
        "exchange_ts_ms": 1724677200000, "received_ts_ms": 1724677200001,
        "depth": 1, "schema_version": 1,
    }
    return RawStreamEntry("42-0", {b"payload": orjson.dumps(payload)})


async def test_orderbook_flush_uploads_before_ack(settings) -> None:
    events: list[str] = []
    consumer = FakeConsumer(events)
    uploader = FakeUploader(events)
    service = OrderBookExporterService(settings, consumer=consumer, uploader=uploader, health=FakeHealth())

    await service._ingest([make_book_entry()])
    await service.flush()

    assert events == ["upload", "ack"]
    assert consumer.acked == [["42-0"]]
    assert uploader.uploads[0][0].startswith("books/venue=bybit/instrument=BTCUSDT/")
