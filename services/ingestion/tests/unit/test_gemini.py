from __future__ import annotations

import json

from ingestion.adapters.gemini import parse_gemini_message
from ingestion.models import BookSnapshot, Trade


def test_parse_trade() -> None:
    event = parse_gemini_message(json.dumps({"E": 1_700_000_000_000_000_000, "symbol": "btcusd", "t": 123, "p": "100000.10", "q": "0.001", "m": False}), received_ts_ms=2)
    assert isinstance(event, Trade)
    assert event.instrument == "BTCUSD"
    assert event.exchange_ts_ms == 1_700_000_000_000
    assert event.taker_side == "buy"


def test_parse_depth_snapshot() -> None:
    event = parse_gemini_message(json.dumps({"lastUpdateId": 160, "symbol": "btcusd", "bids": [["100000.00", "1.5"]], "asks": [["100001.00", "1.2"]]}), received_ts_ms=2)
    assert isinstance(event, BookSnapshot)
    assert event.event_id == "gemini:BTCUSD:book:160"
    assert event.bids[0].price == "100000.00"
