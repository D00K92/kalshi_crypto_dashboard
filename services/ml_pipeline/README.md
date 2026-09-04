# ML Pipeline

`ml_pipeline` owns model training, evaluation, retraining decisions, Kubeflow
Pipelines, and Vertex AI model lifecycle operations. Feature and target
generation, Feast definitions, and online-store materialization belong to
`services/batch_etl`.

## Inputs

```text
gs://kalshi-crypto-tick-data/features/v1/date=YYYY-MM-DD/features.parquet
gs://kalshi-crypto-tick-data/processed/future_realized_volatility/date=YYYY-MM-DD/hour=HH/targets.parquet
```

Training is point-in-time safe: the loader rejects current-day data and limits
samples to `end_date 23:00 UTC` because the longest target horizon is one hour.

## Structure

```text
src/common/       Shared loading, EWMA benchmark, QLIKE, and model logic
src/components/   KFP components and local training/evaluation entrypoints
src/pipelines/    Kubeflow pipeline DAG definition (`pipeline_dag.py`)
containers/       Four task images: load, train, evaluate, and register
scripts/          Compile, submit, and event-trigger training workflows
```

## Training and retraining

The five horizon models use chronological splits and non-negative predictions.
Evaluation compares model QLIKE against an annualized EWMA benchmark (`lambda=0.96`).
`scripts/evaluate_and_trigger.py` records state and can submit the KFP pipeline
after sustained champion deterioration. The KFP DAG evaluates each candidate on
the same 15% holdout used for training and registers it only when it beats EWMA
by 2% and is no more than 5% worse than the current champion. The champion
metrics artifact is expected at
`gs://kalshi-crypto-tick-data/models/v1/champion_metrics.json`.

Inference prediction records should be written under
`gs://kalshi-crypto-tick-data/inference_predictions/date=YYYY-MM-DD/`.

Compile and submit from the unified repository environment:

```bash
uv run --directory services/ml_pipeline python scripts/compile_pipeline.py
uv run --directory services/ml_pipeline python scripts/run_pipeline.py ...
```

Historical feature retrieval through Feast remains available via
`src/components/fetch_historical_features.py`; Feast configuration is owned by
`services/feast_store`.

Container images are published to Artifact Registry under
`asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/ml-pipeline/` and referenced
directly by the Vertex pipeline.
