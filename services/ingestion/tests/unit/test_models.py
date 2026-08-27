from __future__ import annotations

import json

import pytest

from ingestion.models import BookLevel, BookSnapshot, Trade


def test_trade_serializes_without_numeric_precision_loss() -> None:
    event = Trade(
        event_id="binance:BTCUSDT:trade:42",
        event_type="trade",
        venue="binance",
        instrument="BTCUSDT",
        trade_id="42",
        price="123456.12345678",
        quantity="0.00000001",
        taker_side="buy",
        exchange_ts_ms=1_700_000_000_000,
        received_ts_ms=1_700_000_000_001,
    )

    payload = json.loads(event.to_json())

    assert isinstance(event.to_json(), bytes)
    assert payload["price"] == "123456.12345678"
    assert payload["quantity"] == "0.00000001"
    assert payload["schema_version"] == 1


def test_negative_quantity_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        BookLevel(price="100", quantity="-1")


def test_crossed_book_is_rejected() -> None:
    with pytest.raises(ValueError, match="crossed or locked"):
        BookSnapshot(
            event_id="book-1",
            event_type="book_snapshot",
            venue="binance",
            instrument="BTCUSDT",
            sequence=1,
            bids=(BookLevel("101", "1"),),
            asks=(BookLevel("100", "1"),),
            exchange_ts_ms=None,
            received_ts_ms=1,
            depth=1,
        )
