import pandas as pd

from scripts.load_raw_to_bigquery import normalize_books, normalize_trades


def test_normalize_trades_converts_epoch_and_metadata():
    result = normalize_trades(pd.DataFrame([{
        "price": 10.0, "quantity": 2.0, "taker_side": "buy",
        "exchange_ts_ms": 1_728_249_600_000,
    }]), venue="binance", instrument="BTCUSDT", source_object="gs://x")
    assert result.loc[0, "event_timestamp"].isoformat() == "2024-10-06T21:20:00+00:00"
    assert result.loc[0, "venue"] == "binance"


def test_normalize_books_expands_ten_levels_per_side():
    frame = pd.DataFrame([{
        "bids": '[{"price":"10","quantity":"2"}]',
        "asks": '[{"price":"11","quantity":"3"}]',
        "exchange_ts_ms": 1_728_249_600_000,
    }])
    result = normalize_books(frame, venue="binance", instrument="BTCUSDT", source_object="gs://x")
    assert len(result) == 2
    assert set(result["side"]) == {"bid", "ask"}
    assert set(result["level"]) == {1}
