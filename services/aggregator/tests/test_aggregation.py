from decimal import Decimal
import time

from aggregator.aggregation import MarketAggregator
from aggregator.config import Settings


def trade(venue, price, quantity, side="buy", ts=None):
    ts = ts or int(time.time() * 1000)
    return {"event_type": "trade", "venue": venue, "instrument": "BTCUSDT", "price": str(price), "quantity": str(quantity), "taker_side": side, "exchange_ts_ms": ts, "received_ts_ms": ts}


def test_trade_vwap():
    agg = MarketAggregator()
    first = trade("binance", 100, 1)
    first["event_id"] = "one"
    second = trade("coinbase", 110, 3, "sell")
    second["event_id"] = "two"
    agg.apply_trade(first)
    spot = agg.apply_trade(second)
    assert spot["price"] == "105"
    assert spot["method"] == "simple_average_fresh_venues"
    assert spot["total_volume"] == "4"
    assert spot["used_venues"] == ["binance", "coinbase"]
    assert spot["venue_count"] == 2


def test_resampled_bars_include_one_hour_context():
    agg = MarketAggregator(history_ms=2 * 60 * 60 * 1000)
    first = trade("binance", 100, 1, ts=3_600_000)
    first["event_id"] = "hour-one"
    agg.apply_trade(first)
    latest = trade("binance", 110, 2, ts=7_200_000)
    latest["event_id"] = "hour-two"
    agg.apply_trade(latest)

    bars = agg.resampled_bars("BTCUSDT", {"1h": 3_600_000})
    assert len(bars) == 1
    assert {key: bars[0][key] for key in ("schema_version", "event_type", "frequency", "open", "close", "volume")} == {
        "schema_version": 1, "event_type": "market_bar", "frequency": "1h", "open": "100", "close": "100", "volume": "1",
    }
    assert bars[0]["primitive_schema_version"] == 2


def test_resampled_bar_exposes_canonical_trade_primitives():
    agg = MarketAggregator()
    first = trade("binance", 100, 2, "buy", ts=60_000)
    first["event_id"] = "primitive-one"
    second = trade("binance", 110, 1, "sell", ts=61_000)
    second["event_id"] = "primitive-two"
    boundary = trade("binance", 110, 1, "sell", ts=120_000)
    boundary["event_id"] = "primitive-boundary"
    agg.apply_trade(first)
    agg.apply_trade(second)
    agg.apply_trade(boundary)

    bar = agg.resampled_bars("BTCUSDT", {"1m": 60_000})[0]
    assert bar["primitive_schema_version"] == 2
    assert bar["p_open"] == "100"
    assert bar["p_trade"] == "110"
    assert bar["p_trade_mean"] == "105"
    assert bar["v_trade"] == "3"
    assert bar["v_buy"] == "2"
    assert bar["v_sell"] == "1"
    assert bar["cnt_trade"] == 2
    assert bar["dt_fill_mean_ms"] == "1000"
    assert bar["dt_fill_min_ms"] == "1000"
    assert bar["dt_fill_max_ms"] == "1000"


def test_synthetic_price_uses_fresh_venue_prices_not_trade_volume():
    agg = MarketAggregator(trade_freshness_ms=60_000)
    first = trade("binance", 100, 100)
    first["event_id"] = "fresh-one"
    second = trade("coinbase", 110, 1, ts=first["received_ts_ms"] + 1)
    second["event_id"] = "fresh-two"
    spot = agg.apply_trade(first)
    spot = agg.apply_trade(second)
    assert spot["price"] == "105"
    assert spot["log_return"] is not None


def test_stale_venue_is_excluded_from_synthetic_price():
    now = int(time.time() * 1000)
    agg = MarketAggregator(trade_freshness_ms=60_000)
    old = trade("binance", 100, 1, ts=now - 61_000)
    old["event_id"] = "stale"
    current = trade("coinbase", 110, 1, ts=now)
    current["event_id"] = "current"
    agg.apply_trade(old)
    spot = agg.apply_trade(current)
    assert spot["price"] == "110"
    assert spot["used_venues"] == ["coinbase"]
    assert spot["stale_venues"] == ["binance"]


def test_candle_state_round_trips_across_restart():
    original = MarketAggregator()
    first = trade("coinbase", 100, 2, "sell", ts=10_000)
    first["event_id"] = "persisted-one"
    original.apply_trade(first)
    persisted = original.export_candle_state()

    restored = MarketAggregator()
    assert restored.restore_candle_state(persisted) == 1
    assert restored.candle_snapshot("BTCUSDT") == original.candle_snapshot("BTCUSDT")

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
    assert snapshot["best_bid"] == "100"
    assert snapshot["best_ask"] == "102"
    assert snapshot["mid_price"] == "101"


def test_primitive_book_snapshot_is_per_venue_and_not_aggregated():
    agg = MarketAggregator(depth=2)
    payload = agg.primitive_book_snapshot({
        "event_type": "book_snapshot",
        "venue": "Coinbase",
        "instrument": "BTCUSDT",
        "exchange_ts_ms": 123,
        "bids": [{"price": "100", "quantity": "2"}],
        "asks": [{"price": "101", "quantity": "3"}],
    }, 456)
    assert payload["event_type"] == "market_book"
    assert payload["primitive_schema_version"] == 2
    assert payload["venue"] == "coinbase"
    assert payload["generated_ts_ms"] == 456
    assert payload["bids"] == [{"price": "100", "quantity": "2"}]
    assert payload["asks"] == [{"price": "101", "quantity": "3"}]


def test_primitive_bars_are_per_venue_and_forward_fill_empty_intervals():
    agg = MarketAggregator()
    agg.apply_book({"event_type": "book_snapshot", "venue": "binance", "instrument": "BTCUSDT", "exchange_ts_ms": 60_000, "received_ts_ms": 60_000, "bids": [{"price": "99", "quantity": "2"}], "asks": [{"price": "101", "quantity": "3"}]})
    one = trade("binance", 100, 2, "buy", ts=60_000)
    one["event_id"] = "primitive-bar-one"
    boundary = trade("binance", 100, 1, "buy", ts=180_000)
    boundary["event_id"] = "primitive-bar-boundary"
    agg.apply_trade(one)
    agg.apply_trade(boundary)

    rows = [row for row in agg.primitive_bars("BTCUSDT", {"1m": 60_000}) if row["venue"] == "binance"]
    assert [row["bucket_start_ts_ms"] for row in rows] == [60_000, 120_000]
    assert rows[0]["cnt_trade"] == 1
    assert rows[1]["cnt_trade"] == 0
    assert rows[1]["p_trade"] == "100"
    assert rows[0]["p_bid_1"] == "99"
    assert rows[0]["q_ask_1"] == "3"


def test_default_primitive_frequency_is_ten_seconds():
    agg = MarketAggregator()
    first = trade("binance", 100, 1, ts=10_000)
    boundary = trade("binance", 101, 1, ts=20_000)
    agg.apply_trade(first)
    agg.apply_trade(boundary)

    rows = agg.primitive_bars("BTCUSDT")
    assert [(row["frequency"], row["interval_ms"], row["bucket_start_ts_ms"]) for row in rows] == [("10s", 10_000, 10_000)]


def test_dashboard_thirty_second_candles_are_resampled_from_ten_second_state():
    agg = MarketAggregator()
    for timestamp, price in ((10_000, 100), (20_000, 110), (30_000, 120)):
        agg.apply_trade(trade("binance", price, 1, ts=timestamp))

    candles = agg.candle_snapshot("BTCUSDT", interval_ms=30_000)
    assert [(candle["bucket_start_ts_ms"], candle["open"], candle["close"], candle["volume"]) for candle in candles] == [
        (0, "100", "110", "2"),
        (30_000, "120", "120", "1"),
    ]


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
