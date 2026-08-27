import pytest

from ingestion.adapters.deribit import DeribitMessageError, parse_deribit_message


def test_parse_deribit_trade_notification() -> None:
    event = parse_deribit_message(
        '{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC_USDT.raw","data":{"trade_id":"abc-1","timestamp":1787824800123,"instrument_name":"BTC_USDT","direction":"sell","amount":0.25,"price":100001.5}}}',
        received_ts_ms=2,
    )
    assert event is not None
    assert event.event_id == "deribit:BTC_USDT:trade:abc-1"
    assert event.quantity == "0.25"


def test_ignore_non_subscription() -> None:
    assert parse_deribit_message('{"jsonrpc":"2.0","id":1,"result":[]}', received_ts_ms=2) is None


def test_reject_invalid_direction() -> None:
    with pytest.raises(DeribitMessageError):
        parse_deribit_message(
            '{"method":"subscription","params":{"data":{"trade_id":"1","timestamp":1,"instrument_name":"BTC_USDT","direction":"x","amount":1,"price":1}}}',
            received_ts_ms=2,
        )
