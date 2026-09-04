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
    yesterday = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=1)
    if end > yesterday:
        raise ValueError(f"training end date {end} exceeds policy cutoff {yesterday}")
    cutoff = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(hours=1)
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
    left, right = left[left["timestamp"] <= cutoff], right[right["timestamp"] <= cutoff]
    joined = left.merge(right, on=["timestamp", "frequency"], how="inner", suffixes=("", "_target"))
    if joined.empty:
        raise RuntimeError(f"no eligible feature/target rows at or before {cutoff.isoformat()}")
    required_freqs = {"1s", "5s", "1m", "5m", "10m", "30m", "1h"}
    required_targets = {"target_rv_1m", "target_rv_5m", "target_rv_15m", "target_rv_30m", "target_rv_1h"}
    if not required_freqs.issubset(set(joined["frequency"].dropna().unique())):
        raise RuntimeError("training data is missing one or more required frequencies")
    if not required_targets.issubset(joined.columns) or joined[list(required_targets)].notna().sum().min() == 0:
        raise RuntimeError("training data has incomplete target coverage")
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
    actual = table[target].iloc[valid_end:].to_numpy()
    qlike = float(np.mean(np.log(np.maximum(pred * pred, 1e-18)) +
                         np.maximum(actual * actual, 1e-18) / np.maximum(pred * pred, 1e-18)))
    result = {"horizon": horizon, "target": target, "feature_columns": columns,
              "rows": {"total": n, "train": train_end, "validation": valid_end-train_end, "test": n-valid_end},
              "prediction_floor": 0.0,
              "metrics": {"qlike": qlike,
                          "rmse": float(np.sqrt(mean_squared_error(table[target].iloc[valid_end:], pred))),
                          "mae": float(mean_absolute_error(table[target].iloc[valid_end:], pred)),
                          "r2": float(r2_score(table[target].iloc[valid_end:], pred))}}
    Path(model.path).mkdir(parents=True, exist_ok=True)
    joblib.dump(model_impl, Path(model.path) / "model.joblib")
    (Path(model.path) / "metadata.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    for key, value in result["metrics"].items():
        metrics.log_metric(key, value)


@dsl.component(
    base_image="python:3.12-slim",
    packages_to_install=["pandas>=2.2,<3", "pyarrow>=20,<24", "numpy>=2,<3", "xgboost>=2.1,<3", "joblib>=1.4,<2", "gcsfs>=2025.9,<2027"],
)
def evaluate_candidate(
    dataset: Input[Dataset],
    model: Input[Model],
    horizon: str,
    champion_metrics_uri: str,
) -> bool:
    """Return true only when candidate QLIKE beats EWMA and the champion gate."""
    import json
    from pathlib import Path
    import gcsfs
    import joblib
    import numpy as np
    import pandas as pd

    table = pd.read_parquet(dataset.path).dropna(subset=[f"target_rv_{horizon}"]).sort_values("timestamp").reset_index(drop=True)
    candidate = joblib.load(Path(model.path) / "model.joblib")
    columns = list(getattr(candidate, "feature_names_in_", []))
    test_start = int(len(table) * .85)
    if len(table) <= test_start:
        raise RuntimeError("insufficient rows for promotion holdout")
    prediction = np.maximum(candidate.predict(table[columns]), 0.0)[test_start:]
    actual = np.maximum(table[f"target_rv_{horizon}"].to_numpy()[test_start:] ** 2, 1e-18)
    forecast = np.maximum(prediction ** 2, 1e-18)
    candidate_qlike = float(np.mean(np.log(forecast) + actual / forecast))
    seconds = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}[horizon]
    returns = pd.to_numeric(table["trade_log_return"], errors="coerce").fillna(0.0)
    variance = returns.pow(2).ewm(alpha=.04, adjust=False, min_periods=1).mean().shift(1).fillna(returns.pow(2))
    periods = np.maximum(1.0, seconds / table["frequency_seconds"].astype(float))
    benchmark = np.sqrt(np.maximum(variance.to_numpy() * periods.to_numpy() * (365 * 24 * 60 * 60 / seconds), 1e-18))
    benchmark_var = np.maximum(benchmark[test_start:] ** 2, 1e-18)
    benchmark_qlike = float(np.mean(np.log(benchmark_var) + actual / benchmark_var))
    beats_benchmark = candidate_qlike <= benchmark_qlike - abs(benchmark_qlike) * .02
    champion_qlike = None
    if champion_metrics_uri:
        fs = gcsfs.GCSFileSystem()
        try:
            with fs.open(champion_metrics_uri, "r") as handle:
                champion = json.load(handle)
            champion_qlike = champion.get(horizon, champion).get("metrics", {}).get("qlike")
        except FileNotFoundError:
            # Bootstrap run: EWMA is the only incumbent until v1 is promoted.
            champion_qlike = None
    not_degraded = champion_qlike is None or candidate_qlike <= champion_qlike + abs(champion_qlike) * .05
    Path(model.path, "promotion.json").write_text(json.dumps({"candidate_qlike": candidate_qlike,
        "benchmark_qlike": benchmark_qlike, "champion_qlike": champion_qlike,
        "beats_benchmark": beats_benchmark, "not_degraded": not_degraded,
        "promoted": beats_benchmark and not_degraded}, indent=2), encoding="utf-8")
    return bool(beats_benchmark and not_degraded)


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
        labels={"version": model_version, "stage": "champion"},
    )
    return registered.resource_name
