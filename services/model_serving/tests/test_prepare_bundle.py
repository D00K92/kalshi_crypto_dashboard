from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_resource_map_accepts_compatible_training_contracts(tmp_path):
    artifacts = tmp_path / "artifacts"
    output = tmp_path / "bundle"
    contracts = {"5m": "v4_10s", "15m": "v3_10s", "30m": "v3_10s", "1h": "v2_10s"}
    resources = {}
    for index, (horizon, feature_version) in enumerate(contracts.items(), start=1):
        source = artifacts / horizon
        source.mkdir(parents=True)
        (source / "model.joblib").write_bytes(b"model")
        metadata = {
            "horizon": horizon,
            "feature_set": "market_features",
            "feature_version": feature_version,
            "feature_columns": ["input"],
            "architecture": "har" if horizon != "1h" else "xgboost",
        }
        if horizon == "1h":
            metadata.pop("architecture")
        (source / "metadata.json").write_text(json.dumps(metadata))
        resources[horizon] = {
            "resource": f"projects/1/locations/test/models/{index}@1",
            "artifact_uri": f"gs://bucket/{horizon}",
            "trained_feature_version": feature_version,
        }
        if horizon == "1h":
            resources[horizon]["architecture"] = "xgboost"
    resource_map = tmp_path / "resources.json"
    resource_map.write_text(json.dumps(resources))

    subprocess.run([
        sys.executable,
        str(Path(__file__).parents[1] / "scripts" / "prepare_bundle.py"),
        "--artifact-dir", str(artifacts),
        "--output-dir", str(output),
        "--model-version", "v4_volume_har_1",
        "--feature-version", "v4_10s",
        "--resource-map", str(resource_map),
    ], check=True)

    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["horizons"]["5m"].get("trained_feature_version") is None
    assert manifest["horizons"]["15m"]["trained_feature_version"] == "v3_10s"
    assert manifest["horizons"]["30m"]["trained_feature_version"] == "v3_10s"
    assert manifest["horizons"]["1h"]["trained_feature_version"] == "v2_10s"
