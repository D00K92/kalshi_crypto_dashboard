import pytest

from ingestion.adapters.coinbase import CoinbaseBook, CoinbaseMessageError, parse_coinbase_message


def test_parse_coinbase_match() -> None:
    event = parse_coinbase_message(
        b'{"type":"match","trade_id":"42","product_id":"BTC-USD","price":"100000.10","size":"0.01","side":"buy","time":"2026-08-27T10:00:00.123456Z"}',
        received_ts_ms=2,
    )
    assert event is not None
    assert event.event_id == "coinbase:BTC-USD:trade:42"
    assert event.exchange_ts_ms == 1787824800123


def test_ignore_non_match() -> None:
    assert parse_coinbase_message('{"type":"subscriptions"}', received_ts_ms=2) is None


def test_parse_coinbase_numeric_trade_id_and_last_match() -> None:
    event = parse_coinbase_message(
        '{"type":"last_match","trade_id":43,"product_id":"BTC-USD","price":"100000","size":"0.01","side":"sell","time":"2026-08-27T10:00:00Z"}',
        received_ts_ms=2,
    )
    assert event is not None
    assert event.trade_id == "43"


def test_reject_invalid_side() -> None:
    with pytest.raises(CoinbaseMessageError):
        parse_coinbase_message(
            '{"type":"match","trade_id":"1","product_id":"BTC-USD","price":"1","size":"1","side":"hold","time":"2026-08-27T10:00:00Z"}',
            received_ts_ms=2,
        )


def test_parse_advanced_trade_message() -> None:
    event = parse_coinbase_message(
        '{"channel":"market_trades","timestamp":"2026-08-27T10:00:00Z","events":[{"trades":[{"product_id":"BTC-USD","trade_id":"99","price":"100001","size":"0.02","side":"BUY","time":"2026-08-27T10:00:00.123456Z"}]}]}',
        received_ts_ms=2,
    )
    assert event is not None
    assert event.trade_id == "99"
    assert event.taker_side == "buy"


def test_advanced_l2_snapshot_and_update_are_capped() -> None:
    book = CoinbaseBook("BTC-USD")
    snapshot = book.apply_advanced(
        {"type": "snapshot", "product_id": "BTC-USD", "updates": [
            {"side": "bid", "price_level": "100", "new_quantity": "1"},
            {"side": "offer", "price_level": "101", "new_quantity": "1"},
        ]},
        2,
    )
    assert snapshot is not None
    assert snapshot.bids[0].price == "100"
    updated = book.apply_advanced(
        {"type": "update", "product_id": "BTC-USD", "event_time": "2026-08-27T10:00:00Z", "updates": [
            {"side": "bid", "price_level": "100", "new_quantity": "0"},
            {"side": "bid", "price_level": "99", "new_quantity": "2"},
        ]},
        3,
    )
    assert updated is not None
    assert updated.bids[0].price == "99"
