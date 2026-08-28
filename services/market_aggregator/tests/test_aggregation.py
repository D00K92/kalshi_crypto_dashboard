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
    agg = MarketAggregator(price_tick="1", depth=1)
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({"event_type": "book_snapshot", "venue": "binance", "instrument": "BTCUSDT", "received_ts_ms": now, "bids": [{"price": "100.001", "quantity": "2"}], "asks": [{"price": "101.001", "quantity": "3"}]})
    assert snapshot["bids"][0]["price"] == "100"
    assert snapshot["bids"][0]["venues"] == {"binance": "2"}
    assert snapshot["asks"][0]["price"] == "102"


def test_price_bucketing_is_side_aware_and_removes_crossing_bids():
    agg = MarketAggregator(price_tick="1", depth=10)
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({
        "event_type": "book_snapshot",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [
            {"price": "100.99", "quantity": "1"},
            {"price": "99.99", "quantity": "2"},
        ],
        "asks": [
            {"price": "100.00", "quantity": "3"},
            {"price": "100.01", "quantity": "4"},
        ],
    })
    # Bid 100.99 floors to 100, while the exact 100 ask remains at 100 and
    # the 100.01 ask ceils to 101. The overlapping 100 bid is removed.
    assert [level["price"] for level in snapshot["bids"]] == ["99"]
    assert [level["price"] for level in snapshot["asks"]] == ["100", "101"]
    assert snapshot["bucket_method"] == "bid_floor_ask_ceiling"
