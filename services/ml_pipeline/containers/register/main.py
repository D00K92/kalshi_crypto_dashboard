"""Container entrypoint for registering an approved model bundle."""
from __future__ import annotations

import argparse
from pathlib import Path

from google.cloud import aiplatform


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--artifact-uri", required=True)
    p.add_argument("--project", required=True)
    p.add_argument("--location", default="asia-northeast3")
    p.add_argument("--bucket", default="kalshi-crypto-tick-data")
    p.add_argument("--model-version", default="v1")
    p.add_argument("--horizon", required=True)
    p.add_argument("--promote-file")
    a = p.parse_args()
    if a.promote_file and Path(a.promote_file).read_text(encoding="utf-8").strip().lower() != "true":
        print("candidate rejected by promotion gate", flush=True)
        return
    aiplatform.init(project=a.project, location=a.location, staging_bucket=f"gs://{a.bucket}")
    model = aiplatform.Model.upload(
        display_name=f"crypto-volatility-{a.model_version}-{a.horizon}",
        artifact_uri=a.artifact_uri,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest",
        labels={"version": a.model_version, "stage": "champion", "horizon": a.horizon},
    )
    print(model.resource_name, flush=True)


if __name__ == "__main__":
    main()
