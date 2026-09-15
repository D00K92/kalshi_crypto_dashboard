from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

HORIZONS = ("5m", "15m", "30m", "1h")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def feature_contract_hash(*, feature_set: str, feature_version: str,
                          horizon: str, feature_columns: list[str]) -> str:
    payload = {
        "feature_columns": feature_columns,
        "feature_set": feature_set,
        "feature_version": feature_version,
        "horizon": horizon,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _load_resources(path: Path) -> dict[str, dict[str, str]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != set(HORIZONS):
        raise ValueError("resource map must configure exactly 5m, 15m, 30m, and 1h")
    return value


def _copy_model(
    *, source: Path, target_root: Path, horizon: str, resource: str,
    artifact_uri: str, feature_version: str, default_architecture: str | None = None,
    artifact_feature_version: str | None = None,
) -> dict[str, str]:
    source_model = source / "model.joblib"
    source_metadata = source / "metadata.json"
    if not source_model.is_file() or not source_metadata.is_file():
        raise ValueError(f"{horizon} artifact must contain model.joblib and metadata.json")
    metadata = json.loads(source_metadata.read_text(encoding="utf-8"))
    if metadata.get("horizon") != horizon:
        raise ValueError(f"{horizon} artifact horizon mismatch")
    trained_feature_version = artifact_feature_version or feature_version
    if metadata.get("feature_set") != "market_features" or metadata.get("feature_version") != trained_feature_version:
        raise ValueError(f"{horizon} artifact feature contract mismatch")
    columns = metadata.get("feature_columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError(f"{horizon} artifact has invalid feature columns")
    contract_hash = feature_contract_hash(
        feature_set="market_features",
        feature_version=trained_feature_version,
        horizon=horizon,
        feature_columns=columns,
    )
    metadata_hash = metadata.get("feature_contract_hash")
    if metadata_hash is not None and metadata_hash != contract_hash:
        raise ValueError(f"{horizon} artifact feature contract hash mismatch")
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
    entry = {
        "kind": architecture,
        "resource": resource,
        "artifact_uri": artifact_uri.rstrip("/"),
        "model_path": f"{horizon}/model.joblib",
        "metadata_path": f"{horizon}/metadata.json",
        "model_sha256": sha256(target_model),
        "metadata_sha256": sha256(target_metadata),
        "feature_contract_hash": contract_hash,
    }
    if trained_feature_version != feature_version:
        entry["trained_feature_version"] = trained_feature_version
    return entry


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
    parser.add_argument("--artifact-feature-version")
    parser.add_argument("--five-minute-artifact-dir", type=Path)
    parser.add_argument("--five-minute-resource-name")
    parser.add_argument("--five-minute-artifact-uri")
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
                default_architecture=resources[horizon].get("architecture"),
                artifact_feature_version=resources[horizon].get("trained_feature_version"),
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
            artifact_feature_version=args.artifact_feature_version,
        )
        five_minute_args = (
            args.five_minute_artifact_dir,
            args.five_minute_resource_name,
            args.five_minute_artifact_uri,
        )
        if any(five_minute_args) and not all(five_minute_args):
            parser.error("5m packaging requires artifact dir, resource name, and artifact URI")
        if all(five_minute_args):
            horizons["5m"] = _copy_model(
                source=args.five_minute_artifact_dir,
                target_root=args.output_dir,
                horizon="5m",
                resource=args.five_minute_resource_name,
                artifact_uri=args.five_minute_artifact_uri,
                feature_version=args.feature_version,
                default_architecture="har",
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
