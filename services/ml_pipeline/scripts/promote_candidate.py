"""Evaluate a candidate bundle and register only models that pass promotion gates."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from google.cloud import aiplatform as vertex

from src.common.evaluation import evaluate_frame, retrain_decision

HORIZONS = ("1m", "5m", "15m", "30m", "1h")
INFERENCE_PREDICTIONS_ROOT = "gs://kalshi-crypto-tick-data/inference_predictions"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Joined labeled feature/target parquet")
    parser.add_argument("--candidate-root", required=True, help="Directory containing <horizon>/model.joblib")
    parser.add_argument("--champion-metrics", required=True, help="JSON mapping horizons to champion metrics")
    parser.add_argument("--output", required=True, help="Promotion report JSON")
    parser.add_argument("--project", required=True)
    parser.add_argument("--location", default="asia-northeast3")
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--upload", action="store_true", help="Register passing candidates in Vertex AI")
    args = parser.parse_args()

    table = pd.read_parquet(args.dataset)
    champions = json.loads(Path(args.champion_metrics).read_text(encoding="utf-8"))
    report = {"evaluated_at": datetime.now(timezone.utc).isoformat(), "model_version": args.model_version,
              "benchmark": {"name": "ewma", "decay": 0.96}, "horizons": {}}
    for horizon in HORIZONS:
        model_path = Path(args.candidate_root) / horizon / "model.joblib"
        result = evaluate_frame(table, joblib.load(model_path), horizon)
        champion = champions.get(horizon, champions)
        decision = retrain_decision(result, champion)
        # Promotion requires both champion non-deterioration and a material EWMA edge.
        promoted = bool(decision["beats_benchmark"] and not decision["degraded"])
        item = {"metrics": result["metrics"], "benchmark_metrics": result["benchmark"]["metrics"],
                "decision": decision, "promoted": promoted}
        if promoted and args.upload:
            item["vertex_resource"] = _register(model_path.parent, args)
        report["horizons"][horizon] = item
    report["inference_predictions_root"] = INFERENCE_PREDICTIONS_ROOT
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({h: v["promoted"] for h, v in report["horizons"].items()}), flush=True)


def _register(bundle: Path, args) -> str:
    """Upload a passing bundle to GCS and register it as a Vertex model."""
    from google.cloud import storage

    client = storage.Client(project=args.project)
    prefix = f"models/{args.model_version}/candidate"
    for file in bundle.iterdir():
        client.bucket(args.bucket).blob(f"{prefix}/{bundle.name}/{file.name}").upload_from_filename(file)
    vertex.init(project=args.project, location=args.location, staging_bucket=f"gs://{args.bucket}")
    model = vertex.Model.upload(
        display_name=f"crypto-volatility-{args.model_version}-{bundle.name}",
        artifact_uri=f"gs://{args.bucket}/{prefix}/{bundle.name}",
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest",
        labels={"version": args.model_version, "stage": "candidate", "horizon": bundle.name},
    )
    return model.resource_name


if __name__ == "__main__":
    main()
