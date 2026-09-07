"""Compile the Vertex/Kubeflow volatility training pipeline."""

import argparse
import os
from pathlib import Path

from kfp import compiler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="pipeline.yaml")
    parser.add_argument("--image-tag", default=os.getenv("GITHUB_SHA", "v1"))
    parser.add_argument("--image-registry", default=os.getenv("ML_PIPELINE_REGISTRY", "asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/ml-pipeline"))
    args = parser.parse_args()
    os.environ["ML_PIPELINE_IMAGE_TAG"] = args.image_tag
    os.environ["ML_PIPELINE_REGISTRY"] = args.image_registry
    from src.pipelines.pipeline_dag import volatility_training_pipeline

    output = Path(args.output)
    compiler.Compiler().compile(volatility_training_pipeline, str(output))
    print(output)


if __name__ == "__main__":
    main()
