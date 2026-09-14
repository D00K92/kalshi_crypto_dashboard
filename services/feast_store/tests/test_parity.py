from datetime import datetime, timezone

import pytest

from jobs.parity import (
    FeaturePoint,
    compare_points,
    point_from_payload,
)


def payload(*, timestamp_ms=1_000, price=100.0, log_return=0.01, venue_count=2, version="v4_10s"):
    return {
        "feature_set": "market_features",
        "feature_version": version,
        "event_timestamp_ms": timestamp_ms,
        "event_timestamp": datetime.fromtimestamp(timestamp_ms / 1_000, timezone.utc).isoformat(),
        "values": {
            "synthetic_price": price,
            "log_return": log_return,
            "venue_count": venue_count,
        },
    }


def test_payload_requires_selected_contract():
    with pytest.raises(ValueError, match="v4_10s"):
        point_from_payload(payload(version="v2_10s"), feature_version="v4_10s")


def test_payload_accepts_selected_contract():
    assert point_from_payload(
        payload(version="v4_10s"), feature_version="v4_10s"
    ) == FeaturePoint(1_000, 100.0, 0.01, 2)


def test_parity_accepts_small_float_rounding_difference():
    expected = [FeaturePoint(1_000, 100.0, 0.01, 2)]
    actual = {1_000: FeaturePoint(1_000, 100.0 + 1e-10, 0.01 + 1e-12, 2)}

    missing, mismatched, errors = compare_points(
        expected, actual, relative_tolerance=1e-9, absolute_tolerance=1e-10
    )

    assert (missing, mismatched, errors) == (0, 0, [])


def test_parity_reports_missing_and_field_level_drift():
    expected = [
        FeaturePoint(1_000, 100.0, None, 2),
        FeaturePoint(2_000, 101.0, 0.01, 2),
    ]
    actual = {1_000: FeaturePoint(1_000, 105.0, 0.02, 1)}

    missing, mismatched, errors = compare_points(
        expected, actual, relative_tolerance=1e-9, absolute_tolerance=1e-10
    )

    assert missing == 1
    assert mismatched == 1
    assert errors[0]["fields"] == {
        "venue_count": {"offline": 2, "online": 1},
        "synthetic_price": {"offline": 100.0, "online": 105.0},
        "log_return": {"offline": None, "online": 0.02},
    }
    assert errors[1] == {"timestamp_ms": 2_000, "error": "missing_online"}
