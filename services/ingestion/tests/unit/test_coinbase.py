import pytest

from ingestion.adapters.coinbase import CoinbaseMessageError, parse_coinbase_message


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


def test_reject_invalid_side() -> None:
    with pytest.raises(CoinbaseMessageError):
        parse_coinbase_message(
            '{"type":"match","trade_id":"1","product_id":"BTC-USD","price":"1","size":"1","side":"hold","time":"2026-08-27T10:00:00Z"}',
            received_ts_ms=2,
        )
