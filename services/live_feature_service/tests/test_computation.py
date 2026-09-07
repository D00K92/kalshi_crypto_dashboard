from __future__ import annotations

import math

import pytest

from live_feature_service.computation import V1FeatureComputer


def bar(ts, prices, frequency="1m"):
    return {"event_type": "market_bar", "frequency": frequency, "bucket_start_ts_ms": ts, "venue_prices": prices}


def test_matches_offline_synthetic_and_log_return_formula():
    computer = V1FeatureComputer()
    first = computer.compute(bar(60_000, {"a": 100, "b": 102}), now_ms=61_000)
    second = computer.compute(bar(120_000, {"a": 101, "b": 103}), now_ms=121_000)
    assert first.synthetic_price == 101
    assert first.log_return is None
    assert first.venue_count == 2
    assert second.synthetic_price == 102
    assert second.log_return == pytest.approx(math.log(102 / 101))
    assert second.ewma_variance is None


def test_ewma_state_uses_prior_return_before_update():
    computer = V1FeatureComputer()
    computer.compute(bar(60_000, {"a": 100}), now_ms=61_000)
    second = computer.compute(bar(120_000, {"a": 101}), now_ms=121_000)
    third = computer.compute(bar(180_000, {"a": 102}), now_ms=181_000)
    first_return = math.log(101 / 100)
    assert second.ewma_variance is None
    assert third.ewma_variance == pytest.approx(first_return * first_return)


def test_ignores_other_frequencies_and_rejects_out_of_order():
    computer = V1FeatureComputer()
    assert computer.compute(bar(60_000, {"a": 100}, frequency="5m"), now_ms=61_000) is None
    computer.compute(bar(60_000, {"a": 100}), now_ms=61_000)
    with pytest.raises(ValueError, match="out-of-order"):
        computer.compute(bar(30_000, {"a": 99}), now_ms=61_000)


def test_requires_per_venue_prices():
    with pytest.raises(ValueError, match="venue_prices"):
        V1FeatureComputer().compute({"event_type": "market_bar", "frequency": "1m", "bucket_start_ts_ms": 1}, now_ms=2)


def test_state_round_trip_preserves_lag_and_ewma():
    original = V1FeatureComputer()
    original.compute(bar(60_000, {"a": 100}), now_ms=61_000)
    original.compute(bar(120_000, {"a": 101}), now_ms=121_000)
    restored = V1FeatureComputer()
    restored.restore(original.snapshot())
    row = restored.compute(bar(180_000, {"a": 102}), now_ms=181_000)
    assert row.log_return == pytest.approx(math.log(102 / 101))
    assert row.ewma_variance == pytest.approx(math.log(101 / 100) ** 2)
