"""KFP container components for the production Vertex pipeline."""

import os

from kfp import dsl
from kfp.dsl import Dataset, Input, Model, Output

REGISTRY = os.getenv("ML_PIPELINE_REGISTRY", "asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/ml-pipeline")
IMAGE_TAG = os.getenv("ML_PIPELINE_IMAGE_TAG", "v1")
LOAD_IMAGE = f"{REGISTRY}/ml-load:{IMAGE_TAG}"
TRAIN_IMAGE = f"{REGISTRY}/ml-train:{IMAGE_TAG}"
EVALUATE_IMAGE = f"{REGISTRY}/ml-evaluate:{IMAGE_TAG}"
REGISTER_IMAGE = f"{REGISTRY}/ml-register:{IMAGE_TAG}"


@dsl.container_component
def load_container(
    feast_repo: str, target_table: str, start_date: str, end_date: str,
    project: str, output_dataset: Output[Dataset],
):
    return dsl.ContainerSpec(
        image=LOAD_IMAGE, command=["python", "/app/main.py"],
        args=["--feast-repo", feast_repo, "--target-table", target_table,
              "--start-date", start_date, "--end-date", end_date,
              "--project", project, "--output", output_dataset.path],
    )


@dsl.container_component
def train_container(dataset: Input[Dataset], horizon: str, model: Output[Model]):
    return dsl.ContainerSpec(
        image=TRAIN_IMAGE, command=["python", "/app/main.py"],
        args=["--dataset", dataset.path, "--horizon", horizon, "--output", model.path],
    )


@dsl.container_component
def evaluate_container(
    dataset: Input[Dataset], model: Input[Model], horizon: str, champion_metrics_uri: str,
    report: Output[Dataset], promote: Output[Dataset],
):
    return dsl.ContainerSpec(
        image=EVALUATE_IMAGE, command=["python", "/app/main.py"],
        args=["--dataset", dataset.path, "--model", model.path, "--horizon", horizon,
              "--champion-metrics", champion_metrics_uri, "--report", report.path,
              "--promote", promote.path],
    )


@dsl.container_component
def register_container(
    model: Input[Model], promote: Input[Dataset], project: str, location: str,
    bucket: str, model_version: str, horizon: str,
):
    return dsl.ContainerSpec(
        image=REGISTER_IMAGE, command=["python", "/app/main.py"],
        args=["--artifact-uri", model.uri, "--promote-file", promote.path,
              "--project", project, "--location", location, "--bucket", bucket,
              "--model-version", model_version, "--horizon", horizon],
    )
