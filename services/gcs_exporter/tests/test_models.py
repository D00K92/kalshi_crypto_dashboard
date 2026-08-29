from __future__ import annotations

from datetime import datetime, timezone

import orjson
import pytest

from gcs_exporter.models import (
    KalshiOrderBookRow,
    KalshiTickerRow,
    KalshiTradeRow,
    OrderBookRow,
    RawStreamEntry,
    TradeRow,
)


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


def test_order_book_row_truncates_more_than_15_levels() -> None:
    payload = {"event_type": "book_snapshot", "venue": "binance", "instrument": "BTCUSDT",
               "event_id": "x", "sequence": 1, "bids": [{"price": str(100-i), "quantity": "1"} for i in range(16)],
               "asks": [{"price": "200", "quantity": "1"}], "received_ts_ms": 2, "depth": 16, "schema_version": 1}
    row = OrderBookRow.from_entry(RawStreamEntry("1-0", {b"payload": orjson.dumps(payload)}))
    assert len(row.bids) == 15
    assert row.depth == 15


def kalshi_ticker_entry(redis_id: str = "1724677200000-0") -> RawStreamEntry:
    payload = {
        "event_id": "kalshi:KXBTCD-TEST-1:ticker:abc",
        "event_type": "kalshi_ticker",
        "venue": "kalshi",
        "instrument": "BTCUSD",
        "series_ticker": "KXBTCD",
        "event_ticker": "KXBTCD-TEST",
        "market_ticker": "KXBTCD-TEST-1",
        "yes_bid_dollars": "0.42",
        "yes_ask_dollars": "0.43",
        "last_price_dollars": None,
        "volume": "123.00",
        "open_interest": "456.00",
        "exchange_ts_ms": 1_724_677_200_000,
        "received_ts_ms": 1_724_677_200_001,
        "schema_version": 1,
    }
    return RawStreamEntry(redis_id, {b"payload": orjson.dumps(payload)})


def kalshi_trade_entry(redis_id: str = "1724677200001-0") -> RawStreamEntry:
    payload = {
        "event_id": "kalshi:KXBTCD-TEST-1:trade:trade-7",
        "event_type": "kalshi_trade",
        "venue": "kalshi",
        "instrument": "BTCUSD",
        "series_ticker": "KXBTCD",
        "event_ticker": "KXBTCD-TEST",
        "market_ticker": "KXBTCD-TEST-1",
        "trade_id": "trade-7",
        "yes_price_dollars": "0.51",
        "count": "3.00",
        "taker_side": "no",
        "exchange_ts_ms": 1_724_677_200_000,
        "received_ts_ms": 1_724_677_200_001,
        "schema_version": 1,
    }
    return RawStreamEntry(redis_id, {b"payload": orjson.dumps(payload)})


def kalshi_orderbook_entry(redis_id: str = "1724677200002-0") -> RawStreamEntry:
    payload = {
        "event_id": "kalshi:KXBTCD-TEST-1:book:8",
        "event_type": "kalshi_orderbook_snapshot",
        "venue": "kalshi",
        "instrument": "BTCUSD",
        "series_ticker": "KXBTCD",
        "event_ticker": "KXBTCD-TEST",
        "market_ticker": "KXBTCD-TEST-1",
        "sequence": 8,
        "yes_bids": [{"price": "0.42", "quantity": "10.00"}],
        "no_bids": [{"price": "0.58", "quantity": "4.00"}],
        "exchange_ts_ms": None,
        "received_ts_ms": 1_724_677_200_001,
        "schema_version": 1,
    }
    return RawStreamEntry(redis_id, {b"payload": orjson.dumps(payload)})


def test_kalshi_rows_decode_stream_payloads() -> None:
    ticker = KalshiTickerRow.from_entry(kalshi_ticker_entry())
    trade = KalshiTradeRow.from_entry(kalshi_trade_entry())
    book = KalshiOrderBookRow.from_entry(kalshi_orderbook_entry())

    assert ticker.partition == ("KXBTCD", "KXBTCD-TEST", "KXBTCD-TEST-1", "BTCUSD", "2024-08-26", "13")
    assert trade.taker_side == "no"
    assert book.yes_bids == (("0.42", "10.00"),)
    assert book.partition == ("KXBTCD", "KXBTCD-TEST", "KXBTCD-TEST-1", "BTCUSD", "2024-08-26", "13")
