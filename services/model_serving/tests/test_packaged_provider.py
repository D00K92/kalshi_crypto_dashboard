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


def test_packaged_provider_routes_only_1h_to_model(tmp_path):
    provider = PackagedHybridProvider.load(
        build_bundle(tmp_path), model_version="v2_10s", feature_version="v2_10s", decay=0.96
    )
    now = int(time.time() * 1000)
    result = provider.forecast(
        ForecastRequest(
            "market_features", "v2_10s", now - 10_000, now - 5_000,
            {"synthetic_price": 70_000.0, "log_return": 0.001, "venue_count": 6,
             "ewma_state": {"frequency": "10s", "variance": 1e-8}},
            {"features": now - 5_000},
        ),
        now,
        max_age_ms=90_000,
        future_skew_ms=2_000,
    )
    assert result.annualized_volatility["1h"] == pytest.approx(0.42)
    assert len({result.annualized_volatility[h] for h in ("5m", "15m", "30m")}) == 1
    assert result.model_resources["1h"] == "projects/1/locations/test/models/2@1"
    assert result.model_resources["30m"] == "ewma/v2_10s/30m"


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
            "ewma_state": {"frequency": "10s", "variance": 1e-8},
        },
        "source_timestamps_ms": {"features": now - 5_000},
    }

    with TestClient(create_app(settings)) as api:
        assert api.get("/readyz").json() == {"status": "ready"}
        response = api.post("/v1/forecast", json=payload)

    assert response.status_code == 200
    assert response.json()["model_resources"]["1h"] == "projects/1/locations/test/models/2@1"
