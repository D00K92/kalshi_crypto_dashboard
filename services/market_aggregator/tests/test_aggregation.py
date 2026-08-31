from decimal import Decimal
import time

from market_aggregator.aggregation import MarketAggregator
from market_aggregator.config import Settings


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
    assert spot["price"] == "105"
    assert spot["method"] == "ten_second_trade_average"
    assert spot["total_volume"] == "4"
    assert spot["used_venues"] == ["binance", "coinbase"]
    assert agg.cvd == Decimal("-2")


def test_candle_state_round_trips_across_restart():
    original = MarketAggregator()
    first = trade("coinbase", 100, 2, "sell", ts=10_000)
    first["event_id"] = "persisted-one"
    original.apply_trade(first)
    persisted = original.export_candle_state()

    restored = MarketAggregator()
    assert restored.restore_candle_state(persisted) == 1
    assert restored.candle_snapshot("BTCUSDT") == original.candle_snapshot("BTCUSDT")
    assert restored.cvd_snapshot("BTCUSDT") == original.cvd_snapshot("BTCUSDT")

    next_trade = trade("coinbase", 110, 1, "buy", ts=10_001)
    next_trade["event_id"] = "persisted-two"
    restored.apply_trade(next_trade)
    assert restored.candle_snapshot("BTCUSDT")[0]["close"] == "110"


def test_book_preserves_venue_contributions_and_depth():
    agg = MarketAggregator(price_tick="1", depth=1)
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({"event_type": "book_snapshot", "venue": "binance", "instrument": "BTCUSDT", "received_ts_ms": now, "bids": [{"price": "100.001", "quantity": "2"}], "asks": [{"price": "101.001", "quantity": "3"}]})
    assert snapshot["bids"][0]["price"] == "100"
    assert snapshot["bids"][0]["venues"] == {"binance": "2"}
    assert snapshot["asks"][0]["price"] == "102"


def test_price_bucketing_is_side_aware():
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
    # The 100 bid and ask are crossed after bucketing and are volume-netted.
    assert [level["price"] for level in snapshot["bids"]] == ["99"]
    assert [level["price"] for level in snapshot["asks"]] == ["100", "101"]
    assert snapshot["asks"][0]["total_quantity"] == "2"
    assert snapshot["bucket_method"] == "effective_price_bid_floor_ask_ceiling_uncrossed"


def test_cross_venue_price_difference_does_not_empty_bid_side():
    agg = MarketAggregator(price_tick="1", depth=10)
    now = int(time.time() * 1000)
    agg.apply_book({
        "event_id": "venue-a-book",
        "event_type": "book_snapshot",
        "venue": "venue-a",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [{"price": "110", "quantity": "1"}],
        "asks": [{"price": "111", "quantity": "1"}],
    })
    snapshot = agg.apply_book({
        "event_id": "venue-b-book",
        "event_type": "book_snapshot",
        "venue": "venue-b",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [{"price": "100", "quantity": "2"}],
        "asks": [{"price": "101", "quantity": "2"}],
    })

    assert snapshot["bids"]
    assert snapshot["asks"]
    assert snapshot["bids"][0]["price"] == "100"
    assert snapshot["asks"][0]["price"] == "101"


def test_crossed_levels_are_volume_netted():
    agg = MarketAggregator(price_tick="1", depth=10)
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({
        "event_id": "crossed-book",
        "event_type": "book_snapshot",
        "venue": "venue-a",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [{"price": "110", "quantity": "1"}],
        "asks": [{"price": "101", "quantity": "2"}, {"price": "111", "quantity": "3"}],
    })

    assert [level["price"] for level in snapshot["bids"]] == []
    assert [level["price"] for level in snapshot["asks"]] == ["101", "111"]
    assert snapshot["asks"][0]["total_quantity"] == "1"


def test_cross_venue_book_keeps_a_coherent_reference_pair():
    agg = MarketAggregator(price_tick="1", depth=10)
    now = int(time.time() * 1000)
    agg.apply_book({
        "event_id": "low-venue-book",
        "event_type": "book_snapshot",
        "venue": "venue-a",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [{"price": "100", "quantity": "1"}],
        "asks": [{"price": "101", "quantity": "1"}],
    })
    snapshot = agg.apply_book({
        "event_id": "high-venue-book",
        "event_type": "book_snapshot",
        "venue": "venue-b",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [{"price": "110", "quantity": "1"}],
        "asks": [{"price": "111", "quantity": "1"}],
    })

    assert snapshot["bids"][0]["price"] == "100"
    assert snapshot["asks"][0]["price"] == "111"
    assert Decimal(snapshot["asks"][0]["price"]) > Decimal(snapshot["bids"][0]["price"])


def test_book_freshness_uses_redis_publication_time():
    agg = MarketAggregator(freshness_ms=5_000)
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({
        "event_id": "delayed-book",
        "event_type": "book_snapshot",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "received_ts_ms": now - 60_000,
        "bids": [{"price": "100", "quantity": "1"}],
        "asks": [{"price": "101", "quantity": "1"}],
    }, published_ts_ms=now)
    assert snapshot["venues"] == ["binance"]


def test_adaptive_tick_uses_finest_common_price_precision():
    agg = MarketAggregator(depth=10)
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({
        "event_id": "adaptive-tick",
        "event_type": "book_snapshot",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [{"price": "100.01", "quantity": "1"}],
        "asks": [{"price": "100.03", "quantity": "1"}],
    })
    assert snapshot["price_tick"] == "0.01"
    assert snapshot["bids"][0]["price"] == "100.01"
    assert snapshot["asks"][0]["price"] == "100.03"


def test_auto_price_tick_config_enables_inference(monkeypatch):
    monkeypatch.setenv("AGGREGATION_PRICE_TICK", "auto")
    assert Settings.from_env().price_tick is None
    assert dict(Settings.from_env().taker_fees)["coinbase"] == "0.006"


def test_taker_fees_are_applied_before_bucketing():
    agg = MarketAggregator(price_tick="1", taker_fees={"venue-a": "0.01"})
    now = int(time.time() * 1000)
    snapshot = agg.apply_book({
        "event_id": "fee-adjusted-book",
        "event_type": "book_snapshot",
        "venue": "venue-a",
        "instrument": "BTCUSDT",
        "received_ts_ms": now,
        "bids": [{"price": "100", "quantity": "2"}],
        "asks": [{"price": "100", "quantity": "3"}],
    })
    assert snapshot["bids"][0]["price"] == "99"
    assert snapshot["asks"][0]["price"] == "101"
    assert snapshot["taker_fees"] == {"venue-a": "0.01"}


def test_configured_venues_exclude_binance_and_bybit():
    agg = MarketAggregator(venues=("bitstamp", "crypto.com", "gemini", "coinbase", "kraken"))
    excluded = trade("binance", 100, 1)
    excluded["event_id"] = "binance-excluded"
    assert agg.apply_trade(excluded) is None
    assert agg.latest_trades == {}

    included = trade("coinbase", 110, 1)
    included["event_id"] = "coinbase-included"
    assert agg.apply_trade(included)["used_venues"] == ["coinbase"]
