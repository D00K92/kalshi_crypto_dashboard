"""Compile-time Kubeflow pipeline for five parallel volatility models."""

from kfp import dsl

from src.components.kfp_components import load_training_data, register_vertex_model, train_horizon

HORIZONS = ("1m", "5m", "15m", "30m", "1h")


@dsl.pipeline(name="crypto-volatility-training-v1")
def volatility_training_pipeline(
    feature_root: str = "gs://kalshi-crypto-tick-data/features/v1",
    target_root: str = "gs://kalshi-crypto-tick-data/processed/future_realized_volatility",
    start_date: str = "2026-08-31",
    end_date: str = "2026-09-02",
    project: str = "kalshi-crypto-506614",
    location: str = "asia-northeast3",
    model_version: str = "v1",
) -> None:
    data = load_training_data(feature_root=feature_root, target_root=target_root,
                              start_date=start_date, end_date=end_date)
    with dsl.ParallelFor(items=list(HORIZONS), parallelism=5) as horizon:
        trained = train_horizon(dataset=data.outputs["output_dataset"], horizon=horizon)
        register_vertex_model(
            model=trained.outputs["model"], project=project, location=location,
            display_name=f"crypto-volatility-{model_version}-{horizon}", model_version=model_version,
        )
