"""Production training entrypoint for v1 realized-volatility models."""
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import date
from pathlib import Path

import gcsfs
import joblib
from google.cloud import aiplatform, storage
from xgboost import XGBRegressor

from src.common.data_io import load_training_table
from src.common.modeling import train_horizon

HORIZONS = ("1m", "5m", "15m", "30m", "1h")

def upload_to_vertex(model: XGBRegressor, metadata: dict, *, project: str, location: str,
                     bucket: str, model_version: str) -> str:
    """Upload a model bundle to GCS and register it in Vertex Model Registry."""
    horizon = metadata["horizon"]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / horizon
        root.mkdir()
        joblib.dump(model, root / "model.joblib")
        (root / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        client = storage.Client(project=project)
        prefix = f"models/{model_version}/{horizon}"
        for file in root.iterdir():
            client.bucket(bucket).blob(f"{prefix}/{file.name}").upload_from_filename(file)
    aiplatform.init(project=project, location=location, staging_bucket=f"gs://{bucket}")
    resource = aiplatform.Model.upload(
        display_name=f"crypto-volatility-{model_version}-{horizon}", artifact_uri=f"gs://{bucket}/{prefix}",
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest",
        labels={"version": model_version, "horizon": horizon})
    return resource.resource_name


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--project", required=True)
    parser.add_argument("--location", default="asia-northeast3")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--feature-root", default="gs://kalshi-crypto-tick-data/features/v1")
    parser.add_argument("--target-root", default="gs://kalshi-crypto-tick-data/processed/future_realized_volatility")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()
    fs = gcsfs.GCSFileSystem(project=args.project)
    table = load_training_table(fs, args.feature_root, args.target_root, args.start_date, args.end_date)
    for horizon in HORIZONS:
        model, metadata = train_horizon(table, horizon)
        metadata["training_cutoff"] = table.attrs.get("training_cutoff")
        print(json.dumps(metadata["metrics"] | {"horizon": horizon}), flush=True)
        if args.upload:
            print(upload_to_vertex(model, metadata, project=args.project, location=args.location,
                                   bucket=args.bucket, model_version=args.model_version), flush=True)


if __name__ == "__main__":
    main()
