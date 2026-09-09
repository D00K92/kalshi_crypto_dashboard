"""Production Vertex AI/Kubeflow training DAG."""

from kfp import dsl

from src.components.kfp_components import evaluate_container, load_container, register_container, train_container

HORIZONS = ("5m", "15m", "30m", "1h")


@dsl.pipeline(name="crypto-volatility-training-v2-10s")
def volatility_training_pipeline(
    feast_repo: str = "/app/feast_store",
    target_table: str = "kalshi-crypto-506614.training_labels.future_realized_volatility_v2_10s",
    start_date: str = "2026-08-31",
    end_date: str = "2026-09-02",
    project: str = "kalshi-crypto-506614",
    location: str = "asia-northeast3",
    model_version: str = "v2_10s",
    feature_version: str = "v2_10s",
    champion_metrics_uri: str = "gs://kalshi-crypto-tick-data/models/v2_10s/champion_metrics.json",
    bucket: str = "kalshi-crypto-tick-data",
) -> None:
    data = load_container(feast_repo=feast_repo, target_table=target_table,
                          start_date=start_date, end_date=end_date, project=project,
                          feature_version=feature_version)
    with dsl.ParallelFor(items=list(HORIZONS), parallelism=4) as horizon:
        trained = train_container(
            dataset=data.outputs["output_dataset"], horizon=horizon,
            feature_version=feature_version,
        )
        evaluated = evaluate_container(dataset=data.outputs["output_dataset"], model=trained.outputs["model"],
                                       horizon=horizon, champion_metrics_uri=champion_metrics_uri)
        register_container(model=trained.outputs["model"], promote=evaluated.outputs["promote"],
                           project=project, location=location, bucket=bucket,
                           model_version=model_version, feature_version=feature_version,
                           horizon=horizon)
