from __future__ import annotations

import json

from ingestion.adapters.crypto_com import parse_crypto_com_message
from ingestion.models import BookSnapshot, Trade


def test_parse_trade() -> None:
    events = parse_crypto_com_message(json.dumps({"result": {"channel": "trade.BTC_USD", "instrument_name": "BTC_USD", "data": [{"s": "BUY", "p": "100000.10", "q": "0.001", "t": 1700000000000, "d": "123"}]}}), received_ts_ms=2)
    assert isinstance(events[0], Trade)
    assert events[0].instrument == "BTCUSD"
    assert events[0].taker_side == "buy"


def test_parse_book_snapshot() -> None:
    events = parse_crypto_com_message(json.dumps({"result": {"channel": "book.BTC_USD.50", "instrument_name": "BTC_USD", "u": 160, "data": [{"bids": [["100000.00", "1.5", "0"]], "asks": [["100001.00", "1.2", "0"]]}]}}), received_ts_ms=2)
    assert isinstance(events[0], BookSnapshot)
    assert events[0].event_id == "crypto.com:BTCUSD:book:160"


def test_heartbeat_is_ignored() -> None:
    assert parse_crypto_com_message(json.dumps({"id": 42, "method": "public/heartbeat", "code": 0}), received_ts_ms=2) == []
