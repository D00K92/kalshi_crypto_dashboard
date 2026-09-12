from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from feast_repo.validation import validate_feature_freshness, validate_feature_schema
from jobs import backfill, materialize


def feature_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "asset": ["BTCUSD"],
        "event_timestamp": ["2026-09-05T00:00:00Z"],
        "synthetic_price": [100.0],
        "log_return": [0.0],
        "venue_count": [6],
    })


def test_feature_contract_rejects_missing_columns_and_future_rows():
    with pytest.raises(ValueError, match="missing columns"):
        validate_feature_schema(feature_frame().drop(columns=["venue_count"]), version="v1")

    validate_feature_schema(feature_frame(), version="v1")
    with pytest.raises(ValueError, match="after the requested"):
        validate_feature_freshness(feature_frame(), as_of=datetime(2026, 9, 4, tzinfo=timezone.utc))


def test_v2_10s_contract_has_independent_feast_resources():
    from registry import resolve_feature_spec

    legacy = resolve_feature_spec("market_features", "v1")
    current = resolve_feature_spec("market_features", "v2_10s")

    assert current.fields == legacy.fields
    assert current.feature_view == "v2_10s_market_features"
    assert current.push_source == "v2_10s_market_features_push"
    assert current.feature_view != legacy.feature_view
    assert current.push_source != legacy.push_source

    validate_feature_schema(feature_frame(), version="v2_10s")


def test_v3_contract_registers_all_har_components():
    from registry import resolve_feature_spec

    spec = resolve_feature_spec("market_features", "v3_10s")
    assert spec.feature_view == "v3_10s_market_features"
    assert spec.push_source == "v3_10s_market_features_push"
    assert {
        "realized_vol_30s", "realized_vol_1m", "realized_vol_5m", "realized_vol_15m",
        "realized_vol_30m", "realized_vol_1h", "realized_vol_3h",
    }.issubset(spec.required_fields)


def test_backfill_materializes_only_the_registered_feature_view(monkeypatch):
    calls = []

    class Store:
        def __init__(self, repo_path):
            calls.append(("init", repo_path))

        def materialize(self, **kwargs):
            calls.append(("materialize", kwargs))

    monkeypatch.setattr(backfill, "FeatureStore", Store)
    start = datetime.now(timezone.utc) - timedelta(hours=2)
    end = datetime.now(timezone.utc) - timedelta(hours=1)
    backfill.backfill_features(repo_path="/repo", start_time=start, end_time=end, feature_version="v1")

    assert calls[1] == ("materialize", {
        "start_date": start, "end_date": end, "feature_views": ["v1_market_features"],
    })


def test_v2_10s_backfill_does_not_materialize_v1(monkeypatch):
    calls = []

    class Store:
        def __init__(self, repo_path):
            calls.append(("init", repo_path))

        def materialize(self, **kwargs):
            calls.append(("materialize", kwargs))

    monkeypatch.setattr(backfill, "FeatureStore", Store)
    start = datetime.now(timezone.utc) - timedelta(hours=2)
    end = datetime.now(timezone.utc) - timedelta(hours=1)
    backfill.backfill_features(
        repo_path="/repo", start_time=start, end_time=end, feature_version="v2_10s"
    )

    assert calls[1] == ("materialize", {
        "start_date": start, "end_date": end, "feature_views": ["v2_10s_market_features"],
    })


def test_incremental_materialization_uses_bounded_end_time(monkeypatch):
    calls = []

    class Store:
        def __init__(self, repo_path):
            calls.append(("init", repo_path))

        def materialize_incremental(self, **kwargs):
            calls.append(("incremental", kwargs))

    monkeypatch.setattr(materialize, "FeatureStore", Store)
    end = datetime.now(timezone.utc) - timedelta(hours=1)
    materialize.materialize_incremental(repo_path="/repo", end_time=end)
    assert calls[1] == ("incremental", {"end_date": end})
