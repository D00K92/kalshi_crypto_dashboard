from __future__ import annotations

from datetime import datetime, timezone

import orjson
import pytest

from gcs_exporter.models import OrderBookRow, RawStreamEntry, TradeRow


def make_entry(
    redis_id: str = "1724677200000-0", timestamp_ms: int = 1_724_677_200_000
) -> RawStreamEntry:
    payload = {
        "event_id": "binance:trade:123",
        "event_type": "trade",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "trade_id": "123",
        "price": "64234.12000000",
        "quantity": "0.00123000",
        "taker_side": "sell",
        "exchange_ts_ms": timestamp_ms,
        "received_ts_ms": timestamp_ms + 3,
        "schema_version": 1,
    }
    return RawStreamEntry(redis_id, {b"payload": orjson.dumps(payload)})


def test_trade_row_decodes_actual_nested_payload() -> None:
    row = TradeRow.from_entry(make_entry())

    assert row.redis_id == "1724677200000-0"
    assert row.venue == "binance"
    assert row.instrument == "BTCUSDT"
    assert row.price == 64234.12
    assert row.quantity == 0.00123
    assert row.partition == ("binance", "BTCUSDT", "2024-08-26", "13")


def test_invalid_json_is_rejected() -> None:
    entry = RawStreamEntry("1-0", {b"payload": b"not-json"})

    with pytest.raises(ValueError, match="valid JSON"):
        TradeRow.from_entry(entry)


def test_unknown_schema_version_is_rejected() -> None:
    payload = orjson.loads(make_entry().payload_bytes())
    payload["schema_version"] = 2

    with pytest.raises(ValueError, match="unsupported schema_version"):
        TradeRow.from_entry(RawStreamEntry("1-0", {b"payload": orjson.dumps(payload)}))


def test_order_book_row_decodes_snapshot() -> None:
    payload = {
        "event_id": "bybit:BTCUSDT:book:42", "event_type": "book_snapshot",
        "venue": "bybit", "instrument": "BTCUSDT", "sequence": 42,
        "bids": [{"price": "100", "quantity": "1"}],
        "asks": [{"price": "101", "quantity": "2"}],
        "exchange_ts_ms": 1_724_677_200_000, "received_ts_ms": 1_724_677_200_001,
        "depth": 1, "schema_version": 1,
    }
    row = OrderBookRow.from_entry(RawStreamEntry("1-0", {b"payload": orjson.dumps(payload)}))
    assert row.partition == ("bybit", "BTCUSDT", "2024-08-26", "13")
    assert row.bids == (("100", "1"),)


def test_order_book_row_rejects_more_than_15_levels() -> None:
    payload = {"event_type": "book_snapshot", "venue": "binance", "instrument": "BTCUSDT",
               "event_id": "x", "sequence": 1, "bids": [{"price": str(100-i), "quantity": "1"} for i in range(16)],
               "asks": [{"price": "200", "quantity": "1"}], "received_ts_ms": 2, "depth": 16, "schema_version": 1}
    with pytest.raises(ValueError, match="maximum depth"):
        OrderBookRow.from_entry(RawStreamEntry("1-0", {b"payload": orjson.dumps(payload)}))
