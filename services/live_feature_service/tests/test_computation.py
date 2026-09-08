from __future__ import annotations

import math

import pytest

from live_feature_service.computation import V2TenSecondFeatureComputer


def bar(ts, venue, price, frequency="10s"):
    return {
        "event_type": "primitive_bar", "frequency": frequency, "venue": venue,
        "bucket_start_ts_ms": ts, "bucket_end_ts_ms": ts + 10_000, "p_trade_mean": price,
    }


def test_completed_10s_bucket_uses_equal_weight_synthetic_price():
    computer = V2TenSecondFeatureComputer()
    assert computer.compute(bar(0, "a", 100), now_ms=10_000) is None
    assert computer.compute(bar(0, "b", 102), now_ms=10_000) is None
    row = computer.compute(bar(10_000, "a", 101), now_ms=20_000)
    assert row.synthetic_price == 101
    assert row.available_timestamp_ms == 10_000
    assert row.log_return is None
    assert row.venue_count == 2


def test_ewma_state_uses_prior_return_before_update():
    computer = V2TenSecondFeatureComputer()
    computer.compute(bar(0, "a", 100), now_ms=10_000)
    computer.compute(bar(10_000, "a", 101), now_ms=20_000)
    computer.compute(bar(20_000, "a", 102), now_ms=30_000)
    row = computer.compute(bar(30_000, "a", 103), now_ms=40_000)
    first_return = math.log(101 / 100)
    assert row.ewma_variance == pytest.approx(first_return * first_return)


def test_ignores_other_frequencies_and_rejects_out_of_order():
    computer = V2TenSecondFeatureComputer()
    assert computer.compute(bar(0, "a", 100, frequency="1m"), now_ms=10_000) is None
    computer.compute(bar(0, "a", 100), now_ms=10_000)
    computer.compute(bar(10_000, "a", 101), now_ms=20_000)
    with pytest.raises(ValueError, match="out-of-order"):
        computer.compute(bar(0, "a", 99), now_ms=20_000)


def test_requires_per_venue_price():
    with pytest.raises(ValueError, match="price"):
        V2TenSecondFeatureComputer().compute({"event_type": "primitive_bar", "frequency": "10s", "venue": "a", "bucket_start_ts_ms": 0}, now_ms=1)


def test_rejects_stale_replayed_bar_before_mutating_state():
    computer = V2TenSecondFeatureComputer(max_bar_age_ms=120_000)
    with pytest.raises(ValueError, match="stale feature bar"):
        computer.compute(bar(0, "a", 100), now_ms=300_001)
    assert computer.snapshot()["last_timestamp_ms"] is None


def test_state_round_trip_preserves_history_and_lag():
    original = V2TenSecondFeatureComputer()
    original.compute(bar(0, "a", 100), now_ms=10_000)
    original.compute(bar(10_000, "a", 101), now_ms=20_000)
    original.compute(bar(20_000, "a", 102), now_ms=30_000)
    restored = V2TenSecondFeatureComputer()
    restored.restore(original.snapshot())
    assert restored.history_count == 3
    restored.compute(bar(30_000, "a", 103), now_ms=40_000)
    restored.compute(bar(40_000, "a", 104), now_ms=50_000)
    row = restored.compute(bar(50_000, "a", 105), now_ms=60_000)
    assert row.log_return == pytest.approx(math.log(104 / 103))


def test_state_round_trip_preserves_pending_bucket_across_restart():
    original = V2TenSecondFeatureComputer()
    original.compute(bar(0, "a", 100), now_ms=10_000)
    original.compute(bar(0, "b", 102), now_ms=10_000)

    restored = V2TenSecondFeatureComputer()
    restored.restore(original.snapshot())
    row = restored.compute(bar(10_000, "a", 101), now_ms=20_000)

    assert row.event_timestamp_ms == 0
    assert row.synthetic_price == 101
    assert row.venue_count == 2


def test_lagging_new_venue_cannot_revise_completed_global_state():
    computer = V2TenSecondFeatureComputer()
    computer.compute(bar(0, "a", 100), now_ms=10_000)
    computer.compute(bar(10_000, "a", 101), now_ms=20_000)

    with pytest.raises(ValueError, match="out-of-order"):
        computer.compute(bar(0, "new-venue", 50), now_ms=20_000)

    computer.compute(bar(20_000, "a", 102), now_ms=30_000)
    row = computer.compute(bar(30_000, "a", 103), now_ms=40_000)
    assert row.log_return == pytest.approx(math.log(102 / 101))


def test_v2_checkpoint_reconstructs_omitted_pending_bucket():
    original = V2TenSecondFeatureComputer()
    original.compute(bar(0, "a", 100), now_ms=10_000)
    original.compute(bar(10_000, "a", 101), now_ms=20_000)
    legacy = original.snapshot()
    legacy["schema_version"] = 2
    legacy.pop("pending")

    restored = V2TenSecondFeatureComputer()
    restored.restore(legacy)
    row = restored.compute(bar(20_000, "a", 102), now_ms=30_000)

    assert row.event_timestamp_ms == 10_000
    assert row.synthetic_price == 101


def test_zero_ewma_variance_restores_after_flat_prices():
    original = V2TenSecondFeatureComputer()
    original.compute(bar(0, "a", 100), now_ms=10_000)
    original.compute(bar(10_000, "a", 100), now_ms=20_000)
    original.compute(bar(20_000, "a", 100), now_ms=30_000)
    assert original.snapshot()["ewma_variance"] == 0

    restored = V2TenSecondFeatureComputer()
    restored.restore(original.snapshot())
    assert restored.snapshot()["ewma_variance"] == 0


def test_payload_declares_v2_10s_contract():
    computer = V2TenSecondFeatureComputer()
    computer.compute(bar(0, "a", 100), now_ms=10_000)
    row = computer.compute(bar(10_000, "a", 101), now_ms=20_000)

    assert row.payload()["feature_set"] == "market_features"
    assert row.payload()["feature_version"] == "v2_10s"
