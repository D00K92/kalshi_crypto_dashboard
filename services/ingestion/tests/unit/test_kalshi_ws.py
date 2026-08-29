from __future__ import annotations

import orjson
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from ingestion.adapters.kalshi import KalshiMessageError, KalshiWebSocket, parse_kalshi_message


def _pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()


def test_subscription_contains_all_public_market_channels() -> None:
    ws = KalshiWebSocket("wss://example.invalid", "key", _pem())
    message = orjson.loads(ws.subscribe_message(["ticker", "trade", "orderbook_delta"], ["KXBTCD-TEST-1"]))
    assert message["cmd"] == "subscribe"
    assert message["params"]["market_tickers"] == ["KXBTCD-TEST-1"]


def test_update_subscription_rejects_unknown_action() -> None:
    ws = KalshiWebSocket("wss://example.invalid", "key", _pem())
    with pytest.raises(ValueError, match="add_markets"):
        ws.update_subscription_message(2, "replace", ["KXBTCD-TEST-1"])


def test_normalize_ticker_preserves_partial_fields() -> None:
    event = parse_kalshi_message(
        orjson.dumps({"type": "ticker", "msg": {"market_ticker": "KXBTCD-TEST-1", "yes_bid_dollars": "0.42", "ts_ms": 1700000000000}}),
        series_ticker="KXBTCD", event_ticker="KXBTCD-TEST", received_ts_ms=1700000000123,
    )
    assert event is not None
    assert event.market_ticker == "KXBTCD-TEST-1"
    assert event.yes_bid_dollars == "0.42"
    assert event.yes_ask_dollars is None
    assert event.exchange_ts_ms == 1700000000000


def test_normalize_trade_uses_stable_trade_id() -> None:
    event = parse_kalshi_message(
        orjson.dumps({"type": "trade", "msg": {"market_ticker": "KXBTCD-TEST-1", "trade_id": "trade-7", "yes_price_dollars": "0.51", "no_price_dollars": "0.49", "count_fp": "3.00", "taker_side": "no", "ts": 1700000000}}),
        series_ticker="KXBTCD", event_ticker="KXBTCD-TEST", received_ts_ms=1700000000123,
    )
    assert event is not None
    assert event.event_id.endswith("trade:trade-7")
    assert event.taker_side == "no"
    assert event.exchange_ts_ms == 1700000000000


def test_normalize_snapshot_and_delta_keep_binary_book_semantics() -> None:
    snapshot = parse_kalshi_message(
        orjson.dumps({"type": "orderbook_snapshot", "seq": 8, "msg": {"market_ticker": "KXBTCD-TEST-1", "yes_dollars_fp": [["0.42", "10.00"]], "no_dollars_fp": [["0.58", "4.00"]]}}),
        series_ticker="KXBTCD", event_ticker="KXBTCD-TEST", received_ts_ms=1700000000123,
    )
    delta = parse_kalshi_message(
        orjson.dumps({"type": "orderbook_delta", "seq": 9, "msg": {"market_ticker": "KXBTCD-TEST-1", "price_dollars": "0.42", "delta_fp": "-2.00", "side": "yes", "ts_ms": 1700000001000}}),
        series_ticker="KXBTCD", event_ticker="KXBTCD-TEST", received_ts_ms=1700000000123,
    )
    assert snapshot is not None and snapshot.yes_bids[0].quantity == "10.00"
    assert delta is not None and delta.delta_side == "yes" and delta.delta_fp == "-2.00"


def test_malformed_trade_is_rejected() -> None:
    with pytest.raises(KalshiMessageError, match="trade_id"):
        parse_kalshi_message(
            orjson.dumps({"type": "trade", "msg": {"market_ticker": "KXBTCD-TEST-1"}}),
            series_ticker="KXBTCD", event_ticker="KXBTCD-TEST", received_ts_ms=1700000000123,
        )
