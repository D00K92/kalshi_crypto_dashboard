from datetime import datetime, timezone

import pytest

from jobs.parity import (
    FeaturePoint,
    compare_feast_online,
    compare_points,
    load_feast_online_point,
    point_from_payload,
)


def payload(*, timestamp_ms=1_000, price=100.0, log_return=0.01, venue_count=2, version="v2_10s"):
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


def test_payload_requires_v2_10s_contract():
    with pytest.raises(ValueError, match="v2_10s"):
        point_from_payload(payload(version="v1"))


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


def test_feast_online_point_uses_registered_v2_feature_view(monkeypatch):
    calls = []

    class Response:
        def to_dict(self):
            return {"synthetic_price": [100.5], "log_return": [None], "venue_count": [3]}

    class Store:
        def __init__(self, repo_path):
            calls.append(("init", repo_path))

        def get_online_features(self, **kwargs):
            calls.append(("get", kwargs))
            return Response()

    monkeypatch.setattr("jobs.parity.FeatureStore", Store)
    point = load_feast_online_point(repo_path="/repo")

    assert point == FeaturePoint(0, 100.5, None, 3)
    assert calls[1][1]["features"] == [
        "v2_10s_market_features:synthetic_price",
        "v2_10s_market_features:log_return",
        "v2_10s_market_features:venue_count",
    ]


def test_feast_online_matches_recent_stream_point_without_inventing_timestamp():
    online = {
        1_000: FeaturePoint(1_000, 100.0, 0.01, 2),
        2_000: FeaturePoint(2_000, 101.0, 0.02, 3),
    }

    mismatched, errors = compare_feast_online(
        FeaturePoint(0, 100.0, 0.01, 2),
        online,
        max_lag_ms=1_000,
        relative_tolerance=1e-9,
        absolute_tolerance=1e-10,
    )

    assert (mismatched, errors) == (0, [])


def test_feast_online_rejects_value_older_than_allowed_lag():
    online = {
        1_000: FeaturePoint(1_000, 100.0, 0.01, 2),
        3_000: FeaturePoint(3_000, 101.0, 0.02, 3),
    }

    mismatched, errors = compare_feast_online(
        FeaturePoint(0, 100.0, 0.01, 2),
        online,
        max_lag_ms=1_000,
        relative_tolerance=1e-9,
        absolute_tolerance=1e-10,
    )

    assert mismatched == 1
    assert errors[0]["detail"] == "stale_feast_online"
    assert errors[0]["lag_ms"] == 2_000
