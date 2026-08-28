from decimal import Decimal
import time

from market_aggregator.aggregation import MarketAggregator


def trade(venue, price, quantity, side="buy", ts=10_000):
    return {"event_type": "trade", "venue": venue, "instrument": "BTCUSDT", "price": str(price), "quantity": str(quantity), "taker_side": side, "exchange_ts_ms": ts, "received_ts_ms": ts}


def test_trade_vwap_and_cvd():
    agg = MarketAggregator()
    first = trade("binance", 100, 1)
    first["event_id"] = "one"
    second = trade("coinbase", 110, 3, "sell")
    second["event_id"] = "two"
    agg.apply_trade(first)
    spot = agg.apply_trade(second)
    assert spot["price"] == "107.5"
    assert spot["total_volume"] == "4"
    assert spot["used_venues"] == ["binance", "coinbase"]
    assert agg.cvd == Decimal("-2")


def test_book_preserves_venue_contributions_and_depth():
    agg = MarketAggregator(depth=1)
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({"event_type": "book_snapshot", "venue": "binance", "instrument": "BTCUSDT", "received_ts_ms": now, "bids": [{"price": "100.001", "quantity": "2"}], "asks": [{"price": "101.001", "quantity": "3"}]})
    assert snapshot["bids"][0]["price"] == "100.00"
    assert snapshot["bids"][0]["venues"] == {"binance": "2"}
