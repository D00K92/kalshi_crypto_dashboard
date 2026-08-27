import io
import json

import pyarrow.parquet as pq

from gcs_exporter.models import OrderBookRow
from gcs_exporter.parquet_writer import BOOK_SCHEMA, write_books


def _row() -> OrderBookRow:
    return OrderBookRow(
        redis_id="1-0", event_id="bybit:BTCUSDT:book:1", venue="bybit",
        instrument="BTCUSDT", sequence=1, bids=(("100.00000001", "1.25"),),
        asks=(("100.00000002", "2.5"),), exchange_ts_ms=1_724_677_200_000,
        received_ts_ms=1_724_677_200_001, depth=1, schema_version=1,
    )


def test_write_books_schema_and_round_trip() -> None:
    data = write_books([_row()])
    table = pq.read_table(io.BytesIO(data))
    assert table.schema == BOOK_SCHEMA
    record = table.to_pylist()[0]
    assert json.loads(record["bids"])[0]["price"] == "100.00000001"
    assert json.loads(record["asks"])[0]["quantity"] == "2.5"
    assert table.schema.metadata[b"dataset"] == b"crypto_orderbooks"


def test_write_books_rejects_empty_batch() -> None:
    try:
        write_books([])
    except ValueError as exc:
        assert "empty" in str(exc)
    else:
        raise AssertionError("expected ValueError")
