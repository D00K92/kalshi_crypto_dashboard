"""Kubeflow Pipeline components for volatility-model training."""

from kfp import dsl
from kfp.dsl import Dataset, Input, Metrics, Model, Output


@dsl.component(
    base_image="python:3.12-slim",
    packages_to_install=["gcsfs>=2025.9,<2027", "pandas>=2.2,<3", "pyarrow>=20,<24"],
)
def load_training_data(
    feature_root: str,
    target_root: str,
    start_date: str,
    end_date: str,
    output_dataset: Output[Dataset],
) -> None:
    """Join feature daily partitions to target hourly partitions."""
    import datetime
    import gcsfs
    import pandas as pd

    def days(start, end):
        while start <= end:
            yield start
            start += datetime.timedelta(days=1)

    fs = gcsfs.GCSFileSystem()
    start, end = datetime.date.fromisoformat(start_date), datetime.date.fromisoformat(end_date)
    features = [pd.read_parquet(f"{feature_root.rstrip('/')}/date={day}/features.parquet", filesystem=fs)
                for day in days(start, end)
                if fs.exists(f"{feature_root.rstrip('/')}/date={day}/features.parquet")]
    targets = [pd.read_parquet(f"gs://{path}" if not path.startswith("gs://") else path, filesystem=fs)
               for day in days(start, end)
               for path in fs.glob(f"{target_root.rstrip('/')}/date={day}/hour=*/targets.parquet")]
    if not features or not targets:
        raise RuntimeError("feature or target partitions are missing")
    left, right = pd.concat(features, ignore_index=True), pd.concat(targets, ignore_index=True)
    left["timestamp"] = pd.to_datetime(left["timestamp"], utc=True)
    right["timestamp"] = pd.to_datetime(right["timestamp"], utc=True)
    joined = left.merge(right, on=["timestamp", "frequency"], how="inner", suffixes=("", "_target"))
    joined.to_parquet(output_dataset.path, index=False, compression="snappy")


@dsl.component(
    base_image="python:3.12-slim",
    packages_to_install=["pandas>=2.2,<3", "pyarrow>=20,<24", "numpy>=2,<3", "xgboost>=2.1,<3", "joblib>=1.4,<2"],
)
def train_horizon(
    dataset: Input[Dataset],
    horizon: str,
    model: Output[Model],
    metrics: Output[Metrics],
) -> None:
    """Train one horizon with a chronological split and persist its bundle."""
    import json
    from pathlib import Path
    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from xgboost import XGBRegressor

    target = f"target_rv_{horizon}"
    table = pd.read_parquet(dataset.path).dropna(subset=[target]).sort_values("timestamp").reset_index(drop=True)
    excluded = {"timestamp", "frequency", "asset", "venue", "synthetic_price"}
    columns = [c for c in table.select_dtypes(include=np.number).columns
               if c not in excluded and not c.startswith("target_")]
    n = len(table)
    train_end, valid_end = int(n * .70), int(n * .85)
    if not columns or train_end < 2 or valid_end <= train_end or n <= valid_end:
        raise RuntimeError("insufficient training data or numeric features")
    model_impl = XGBRegressor(n_estimators=500, max_depth=6, learning_rate=.05, subsample=.8,
                              colsample_bytree=.8, objective="reg:squarederror", eval_metric="rmse",
                              random_state=42, n_jobs=-1)
    model_impl.fit(table[columns].iloc[:train_end], table[target].iloc[:train_end],
                   eval_set=[(table[columns].iloc[train_end:valid_end], table[target].iloc[train_end:valid_end])],
                   verbose=False)
    # Realized volatility cannot be negative; apply the same production
    # output constraint used by the standalone trainer before scoring.
    pred = np.maximum(model_impl.predict(table[columns].iloc[valid_end:]), 0.0)
    result = {"horizon": horizon, "target": target, "feature_columns": columns,
              "rows": {"total": n, "train": train_end, "validation": valid_end-train_end, "test": n-valid_end},
              "prediction_floor": 0.0,
              "metrics": {"rmse": float(np.sqrt(mean_squared_error(table[target].iloc[valid_end:], pred))),
                          "mae": float(mean_absolute_error(table[target].iloc[valid_end:], pred)),
                          "r2": float(r2_score(table[target].iloc[valid_end:], pred))}}
    Path(model.path).mkdir(parents=True, exist_ok=True)
    joblib.dump(model_impl, Path(model.path) / "model.joblib")
    (Path(model.path) / "metadata.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    for key, value in result["metrics"].items():
        metrics.log_metric(key, value)


@dsl.component(
    base_image="python:3.12-slim",
    packages_to_install=["google-cloud-aiplatform>=1.60,<2"],
)
def register_vertex_model(
    model: Input[Model], project: str, location: str, display_name: str, model_version: str,
) -> str:
    """Register a trained model artifact in Vertex AI Model Registry."""
    from google.cloud import aiplatform
    aiplatform.init(project=project, location=location)
    registered = aiplatform.Model.upload(
        display_name=display_name, artifact_uri=model.uri,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest",
        labels={"version": model_version},
    )
    return registered.resource_name
