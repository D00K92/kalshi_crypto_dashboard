from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

HORIZONS = ("5m", "15m", "30m", "1h")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_resources(path: Path) -> dict[str, dict[str, str]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != set(HORIZONS):
        raise ValueError("resource map must configure exactly 5m, 15m, 30m, and 1h")
    return value


def _copy_model(
    *, source: Path, target_root: Path, horizon: str, resource: str,
    artifact_uri: str, feature_version: str, default_architecture: str | None = None,
) -> dict[str, str]:
    source_model = source / "model.joblib"
    source_metadata = source / "metadata.json"
    if not source_model.is_file() or not source_metadata.is_file():
        raise ValueError(f"{horizon} artifact must contain model.joblib and metadata.json")
    metadata = json.loads(source_metadata.read_text(encoding="utf-8"))
    if metadata.get("horizon") != horizon:
        raise ValueError(f"{horizon} artifact horizon mismatch")
    if metadata.get("feature_set") != "market_features" or metadata.get("feature_version") != feature_version:
        raise ValueError(f"{horizon} artifact feature contract mismatch")
    columns = metadata.get("feature_columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError(f"{horizon} artifact has invalid feature columns")
    architecture = metadata.get("architecture", default_architecture)
    if architecture not in {"har", "xgboost"}:
        raise ValueError(f"{horizon} artifact has unsupported architecture")
    if not resource.startswith("projects/") or "@" not in resource:
        raise ValueError(f"{horizon} resource must identify an immutable Vertex version")
    if not artifact_uri.startswith("gs://"):
        raise ValueError(f"{horizon} artifact URI must use gs://")

    target = target_root / horizon
    target.mkdir(parents=True, exist_ok=True)
    target_model = target / "model.joblib"
    target_metadata = target / "metadata.json"
    shutil.copy2(source_model, target_model)
    shutil.copy2(source_metadata, target_metadata)
    return {
        "kind": architecture,
        "resource": resource,
        "artifact_uri": artifact_uri.rstrip("/"),
        "model_path": f"{horizon}/model.joblib",
        "metadata_path": f"{horizon}/metadata.json",
        "model_sha256": sha256(target_model),
        "metadata_sha256": sha256(target_metadata),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare an immutable packaged model bundle.")
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--model-version", default="v2_10s")
    parser.add_argument("--feature-version", default="v2_10s")
    parser.add_argument("--resource-map", type=Path,
                        help="JSON mapping each horizon to resource and artifact_uri")
    # Compatibility path for the existing v2 one-model release workflow.
    parser.add_argument("--resource-name")
    parser.add_argument("--artifact-uri")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.resource_map:
        resources = _load_resources(args.resource_map)
        horizons = {
            horizon: _copy_model(
                source=args.artifact_dir / horizon,
                target_root=args.output_dir,
                horizon=horizon,
                resource=resources[horizon]["resource"],
                artifact_uri=resources[horizon]["artifact_uri"],
                feature_version=args.feature_version,
            )
            for horizon in HORIZONS
        }
    else:
        if not args.resource_name or not args.artifact_uri:
            parser.error("legacy packaging requires --resource-name and --artifact-uri")
        horizons = {
            horizon: {"kind": "ewma", "resource": f"ewma/{args.model_version}/{horizon}"}
            for horizon in HORIZONS[:-1]
        }
        horizons["1h"] = _copy_model(
            source=args.artifact_dir,
            target_root=args.output_dir,
            horizon="1h",
            resource=args.resource_name,
            artifact_uri=args.artifact_uri,
            feature_version=args.feature_version,
            default_architecture="xgboost",
        )

    manifest = {
        "schema_version": 1,
        "model_version": args.model_version,
        "feature_version": args.feature_version,
        "horizons": horizons,
    }
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"manifest": str(manifest_path), "resources": {
        horizon: entry["resource"] for horizon, entry in horizons.items()
    }}))


if __name__ == "__main__":
    main()
