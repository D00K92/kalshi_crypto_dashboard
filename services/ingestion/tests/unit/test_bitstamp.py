from __future__ import annotations

import json

from ingestion.adapters.bitstamp import parse_bitstamp_message
from ingestion.models import BookSnapshot, Trade


def test_parse_trade() -> None:
    event = parse_bitstamp_message(json.dumps({
        "event": "trade", "channel": "live_trades_btcusd",
        "data": {"id": "123", "amount_str": "0.001", "price_str": "100000.10",
                 "type": 0, "timestamp": "1700000000", "microtimestamp": "1700000000123456"},
    }), received_ts_ms=2)
    assert isinstance(event, Trade)
    assert event.event_id == "bitstamp:BTCUSD:trade:123"
    assert event.exchange_ts_ms == 1_700_000_000_123
    assert event.taker_side == "buy"


def test_parse_order_book_snapshot() -> None:
    event = parse_bitstamp_message(json.dumps({
        "event": "data", "channel": "order_book_btcusd",
        "data": {"timestamp": "1700000000", "microtimestamp": "1700000000123456",
                 "bids": [["100000.00", "1.5"]], "asks": [["100001.00", "1.2"]]},
    }), received_ts_ms=2)
    assert isinstance(event, BookSnapshot)
    assert event.event_id == "bitstamp:BTCUSD:book:1700000000123456"
    assert event.bids[0].price == "100000.00"


def test_ignore_subscription_ack() -> None:
    assert parse_bitstamp_message(
        '{"event":"bts:subscription_succeeded","channel":"live_trades_btcusd","data":{}}',
        received_ts_ms=2,
    ) is None
