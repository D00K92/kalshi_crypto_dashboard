from __future__ import annotations

import json

import pytest

from ingestion.adapters.binance import BinanceMessageError, parse_binance_message
from ingestion.models import BookSnapshot, Trade


def test_parse_trade() -> None:
    raw = json.dumps(
        {
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1_700_000_000_000,
                "s": "BTCUSDT",
                "t": 12345,
                "p": "100000.10",
                "q": "0.001",
                "T": 1_700_000_000_001,
                "m": False,
            },
        }
    )

    event = parse_binance_message(raw, received_ts_ms=1_700_000_000_002)

    assert isinstance(event, Trade)
    assert event.event_id == "binance:BTCUSDT:trade:12345"
    assert event.taker_side == "buy"
    assert event.price == "100000.10"


def test_parse_partial_depth_snapshot() -> None:
    raw = json.dumps(
        {
            "stream": "btcusdt@depth20@100ms",
            "data": {
                "lastUpdateId": 160,
                "bids": [["100000.00", "1.5"], ["99999.00", "2"]],
                "asks": [["100001.00", "1.2"], ["100002.00", "3"]],
            },
        }
    )

    event = parse_binance_message(raw, received_ts_ms=1_700_000_000_002)

    assert isinstance(event, BookSnapshot)
    assert event.sequence == 160
    assert event.instrument == "BTCUSDT"
    assert event.exchange_ts_ms is None
    assert event.bids[0].price == "100000.00"
    assert event.asks[0].quantity == "1.2"


def test_unknown_stream_is_ignored() -> None:
    raw = json.dumps({"stream": "btcusdt@bookTicker", "data": {}})
    assert parse_binance_message(raw, received_ts_ms=1) is None


@pytest.mark.parametrize("raw", ["not-json", "[]", '{"stream":"btcusdt@trade"}'])
def test_malformed_frames_are_rejected(raw: str) -> None:
    with pytest.raises(BinanceMessageError):
        parse_binance_message(raw, received_ts_ms=1)
