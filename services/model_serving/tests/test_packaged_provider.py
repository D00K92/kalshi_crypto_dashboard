from __future__ import annotations

import hashlib
import json
import time

import joblib
import numpy as np
import pytest
from fastapi.testclient import TestClient

from model_serving.api import create_app
from model_serving.config import Settings
from model_serving.inference_service import ForecastRequest
from model_serving.packaged_provider import PackagedHybridProvider


class ConstantModel:
    def predict(self, frame):
        assert list(frame.columns) == ["log_return", "venue_count"]
        return np.array([0.42])


class HorizonModel:
    def __init__(self, columns, prediction):
        self.columns = columns
        self.prediction = prediction

    def predict(self, frame):
        assert list(frame.columns) == self.columns
        return np.array([self.prediction])


def build_bundle(tmp_path, *, checksum="valid"):
    model_dir = tmp_path / "1h"
    model_dir.mkdir()
    model_path = model_dir / "model.joblib"
    metadata_path = model_dir / "metadata.json"
    joblib.dump(ConstantModel(), model_path)
    metadata_path.write_text(json.dumps({
        "horizon": "1h",
        "feature_set": "market_features",
        "feature_version": "v2_10s",
        "feature_columns": ["log_return", "venue_count"],
    }))
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": 1,
        "model_version": "v2_10s",
        "feature_version": "v2_10s",
        "horizons": {
            horizon: {"kind": "ewma", "resource": f"ewma/v2_10s/{horizon}"}
            for horizon in ("5m", "15m", "30m")
        } | {
            "1h": {
                "kind": "xgboost",
                "resource": "projects/1/locations/test/models/2@1",
                "artifact_uri": "gs://bucket/model",
                "model_path": "1h/model.joblib",
                "metadata_path": "1h/metadata.json",
                "model_sha256": digest(model_path) if checksum == "valid" else "0" * 64,
                "metadata_sha256": digest(metadata_path),
            }
        },
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path


def build_v3_bundle(tmp_path):
    columns = {
        "5m": ["realized_vol_30s", "realized_vol_1m", "realized_vol_5m"],
        "15m": ["realized_vol_5m", "realized_vol_15m", "realized_vol_1h"],
        "30m": ["realized_vol_5m", "realized_vol_30m", "realized_vol_1h"],
        "1h": ["realized_vol_15m", "realized_vol_1h", "realized_vol_3h"],
    }
    entries = {}
    for index, horizon in enumerate(("5m", "15m", "30m", "1h"), start=1):
        model_dir = tmp_path / horizon
        model_dir.mkdir()
        model_path = model_dir / "model.joblib"
        metadata_path = model_dir / "metadata.json"
        joblib.dump(HorizonModel(columns[horizon], index / 10), model_path)
        metadata_path.write_text(json.dumps({
            "horizon": horizon,
            "feature_set": "market_features",
            "feature_version": "v3_10s",
            "feature_columns": columns[horizon],
            "architecture": "har",
        }))
        digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        entries[horizon] = {
            "kind": "har",
            "resource": f"projects/1/locations/test/models/{index}@1",
            "artifact_uri": f"gs://bucket/{horizon}",
            "model_path": f"{horizon}/model.joblib",
            "metadata_path": f"{horizon}/metadata.json",
            "model_sha256": digest(model_path),
            "metadata_sha256": digest(metadata_path),
        }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({
        "schema_version": 1,
        "model_version": "v3_har_1",
        "feature_version": "v3_10s",
        "horizons": entries,
    }))
    return manifest_path


def test_packaged_provider_routes_only_1h_to_model(tmp_path):
    provider = PackagedHybridProvider.load(
        build_bundle(tmp_path), model_version="v2_10s", feature_version="v2_10s", decay=0.96
    )
    now = int(time.time() * 1000)
    result = provider.forecast(
        ForecastRequest(
            "market_features", "v2_10s", now - 10_000, now - 5_000,
            {"synthetic_price": 70_000.0, "log_return": 0.001, "venue_count": 6,
             "ewma_states": {h: {"frequency": h, "variance": variance} for h, variance in {"5m": 1e-8, "15m": 2e-8, "30m": 3e-8}.items()}},
            {"features": now - 5_000},
        ),
        now,
        max_age_ms=90_000,
        future_skew_ms=2_000,
    )
    assert result.annualized_volatility["1h"] == pytest.approx(0.42)
    assert len({result.annualized_volatility[h] for h in ("5m", "15m", "30m")}) == 3
    assert result.model_resources["1h"] == "projects/1/locations/test/models/2@1"
    assert result.model_resources["30m"] == "ewma/v2_10s/30m"


def test_packaged_provider_routes_all_v3_horizons_to_har_models(tmp_path):
    provider = PackagedHybridProvider.load(
        build_v3_bundle(tmp_path), model_version="v3_har_1", feature_version="v3_10s", decay=0.96
    )
    now = int(time.time() * 1000)
    realized = {
        f"realized_vol_{window}": 0.2
        for window in ("30s", "1m", "5m", "15m", "30m", "1h", "3h")
    }
    result = provider.forecast(
        ForecastRequest(
            "market_features", "v3_10s", now - 10_000, now - 5_000,
            {"synthetic_price": 70_000.0, "venue_count": 6, **realized,
             "ewma_states": {h: {"frequency": h, "variance": 1e-8} for h in ("5m", "15m", "30m")}},
            {"features": now - 5_000},
        ),
        now,
        max_age_ms=90_000,
        future_skew_ms=2_000,
    )
    assert result.annualized_volatility == pytest.approx({"5m": 0.1, "15m": 0.2, "30m": 0.3, "1h": 0.4})
    assert all(resource.startswith("projects/") for resource in result.model_resources.values())


def test_packaged_provider_rejects_modified_model(tmp_path):
    with pytest.raises(ValueError, match="checksum mismatch"):
        PackagedHybridProvider.load(
            build_bundle(tmp_path, checksum="invalid"),
            model_version="v2_10s",
            feature_version="v2_10s",
            decay=0.96,
        )


def test_api_becomes_ready_only_after_packaged_bundle_loads(tmp_path):
    manifest = build_bundle(tmp_path)
    settings = Settings(model_bundle_manifest=str(manifest))
    now = int(time.time() * 1000)
    payload = {
        "feature_set": "market_features",
        "feature_version": "v2_10s",
        "event_timestamp_ms": now - 10_000,
        "available_timestamp_ms": now - 5_000,
        "values": {
            "synthetic_price": 70_000.0,
            "log_return": 0.001,
            "venue_count": 6,
            "ewma_states": {h: {"frequency": h, "variance": variance} for h, variance in {"5m": 1e-8, "15m": 2e-8, "30m": 3e-8}.items()},
        },
        "source_timestamps_ms": {"features": now - 5_000},
    }

    with TestClient(create_app(settings)) as api:
        assert api.get("/readyz").json() == {"status": "ready"}
        response = api.post("/v1/forecast", json=payload)

    assert response.status_code == 200
    assert response.json()["model_resources"]["1h"] == "projects/1/locations/test/models/2@1"
