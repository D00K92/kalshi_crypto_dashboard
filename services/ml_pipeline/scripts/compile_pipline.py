"""Compile the Vertex/Kubeflow volatility training pipeline."""
from pathlib import Path

from kfp import compiler
from src.pipelines.training_pipeline import volatility_training_pipeline


if __name__ == "__main__":
    output = Path("volatility_training_pipeline.json")
    compiler.Compiler().compile(volatility_training_pipeline, str(output))
    print(f"compiled {output}")
