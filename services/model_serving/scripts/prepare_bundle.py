from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

HORIZONS = ("1m", "5m", "15m", "30m", "1h")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare an immutable packaged model bundle.")
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--resource-name", required=True)
    parser.add_argument("--artifact-uri", required=True)
    args = parser.parse_args()

    if not args.resource_name.startswith("projects/") or "@" not in args.resource_name:
        parser.error("resource-name must identify an immutable Vertex model version")
    if not args.artifact_uri.startswith("gs://"):
        parser.error("artifact-uri must use gs://")
    source_model = args.artifact_dir / "model.joblib"
    source_metadata = args.artifact_dir / "metadata.json"
    if not source_model.is_file() or not source_metadata.is_file():
        parser.error("artifact-dir must contain model.joblib and metadata.json")
    metadata = json.loads(source_metadata.read_text(encoding="utf-8"))
    if metadata.get("horizon") != "1h":
        parser.error("artifact horizon must be 1h")
    if metadata.get("feature_set") != "market_features" or metadata.get("feature_version") != "v2_10s":
        parser.error("artifact must use market_features/v2_10s")
    if metadata.get("feature_columns") != ["log_return", "venue_count"]:
        parser.error("artifact has unexpected feature columns")

    target = args.output_dir / "1h"
    target.mkdir(parents=True, exist_ok=True)
    target_model = target / "model.joblib"
    target_metadata = target / "metadata.json"
    shutil.copy2(source_model, target_model)
    shutil.copy2(source_metadata, target_metadata)
    horizons = {
        horizon: {"kind": "ewma", "resource": f"ewma/v2_10s/{horizon}"}
        for horizon in HORIZONS[:-1]
    }
    horizons["1h"] = {
        "kind": "xgboost",
        "resource": args.resource_name,
        "artifact_uri": args.artifact_uri.rstrip("/"),
        "model_path": "1h/model.joblib",
        "metadata_path": "1h/metadata.json",
        "model_sha256": sha256(target_model),
        "metadata_sha256": sha256(target_metadata),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "model_version": "v2_10s",
        "feature_version": "v2_10s",
        "horizons": horizons,
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"manifest": str(args.output_dir / "manifest.json"), "resource_1h": args.resource_name}))


if __name__ == "__main__":
    main()
