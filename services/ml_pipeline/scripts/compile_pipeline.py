"""Compile the Vertex/Kubeflow volatility training pipeline."""

from pathlib import Path

from kfp import compiler

from src.pipelines.pipeline_dag import volatility_training_pipeline


def main() -> None:
    output = Path("pipeline.yaml")
    compiler.Compiler().compile(volatility_training_pipeline, str(output))
    print(output)


if __name__ == "__main__":
    main()
