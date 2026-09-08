from __future__ import annotations

import time

from fastapi.testclient import TestClient

from model_serving.api import create_app
from model_serving.config import Settings


def payload(**overrides):
    value = {
        "feature_set": "market_features",
        "feature_version": "v1",
        "event_timestamp_ms": int(time.time() * 1000),
        "values": {"synthetic_price": 70_000.0, "venue_count": 6, "ewma_state": {"frequency": "1m", "variance": 1e-6}},
        "source_timestamps_ms": {"bar_1m": int(time.time() * 1000)},
    }
    value.update(overrides)
    return value


def test_health_and_ready():
    client = TestClient(create_app(Settings()))
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_forecast_returns_complete_term_structure():
    client = TestClient(create_app(Settings()))
    response = client.post("/v1/forecast", json=payload())
    assert response.status_code == 200
    body = response.json()
    assert set(body["annualized_volatility"]) == {"1m", "5m", "15m", "30m", "1h"}
    assert len(set(body["annualized_volatility"].values())) == 1
    assert body["model_version"] == "v1"
    assert body["feature_available_ts_ms"] == body["feature_asof_ts_ms"]


def test_forecast_rejects_missing_ewma_state():
    client = TestClient(create_app(Settings()))
    response = client.post("/v1/forecast", json=payload(values={"synthetic_price": 70_000.0}))
    assert response.status_code == 422


def test_forecast_rejects_missing_required_feature():
    client = TestClient(create_app(Settings()))
    response = client.post("/v1/forecast", json=payload(values={"ewma_state": {"frequency": "1m", "variance": 1e-6}}))
    assert response.status_code == 422


def test_forecast_rejects_stale_features():
    client = TestClient(create_app(Settings(max_feature_age_ms=10)))
    old = int(time.time() * 1000) - 100
    response = client.post("/v1/forecast", json=payload(event_timestamp_ms=old))
    assert response.status_code == 422


def test_forecast_uses_availability_timestamp_for_freshness():
    client = TestClient(create_app(Settings(max_feature_age_ms=10)))
    now = int(time.time() * 1000)
    response = client.post(
        "/v1/forecast",
        json=payload(event_timestamp_ms=now - 100_000, available_timestamp_ms=now),
    )
    assert response.status_code == 200


def test_forecast_rejects_non_finite_variance():
    client = TestClient(create_app(Settings()))
    response = client.post("/v1/forecast", json=payload(values={"ewma_state": {"frequency": "1m", "variance": "nan"}}))
    assert response.status_code == 422
