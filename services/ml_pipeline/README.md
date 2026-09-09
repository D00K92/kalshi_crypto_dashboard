# ML Pipeline

`ml_pipeline` owns model training, evaluation, retraining decisions, Kubeflow
Pipelines, and Vertex AI model lifecycle operations. Feature and target
generation, Feast definitions, and online-store materialization belong to
`services/batch_etl`.

## Inputs

Training labels and exact-timestamp features are joined directly from the
partitioned BigQuery tables declared by the immutable Feast contract. This
avoids an expensive range join across the feature-view TTL; Feast remains the
owner of the corresponding offline and online feature definitions.

Training is point-in-time safe: the loader rejects current-day ranges and only
uses labels whose future window has completed.

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
uv run --directory services/ml_pipeline python scripts/compile_pipeline.py \
  --image-tag <immutable-git-sha>
uv run --directory services/ml_pipeline python scripts/run_pipeline.py ...
```

`ML Pipeline CI` runs the Feast and ML test suites and, on a `main` change to
either service, publishes `ml-load`, `ml-train`, `ml-evaluate`, and
`ml-register` with the immutable commit SHA. It also uploads a compiled Vertex
template that references those exact images. Image publication does not submit
or promote a model; use `run_pipeline.py` with a completed training range and
record the resulting Vertex resources before changing analytics variables.

Historical feature retrieval is centralized in
`src/common/data_io.py::load_training_table_from_feast`; it resolves the model
contract and performs an exact-timestamp BigQuery join. Feast configuration is
owned by `services/feast_store`. The legacy GCS loader remains available for
rollback only.

Container images are published to Artifact Registry under
`asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/ml-pipeline/` and referenced
directly by the Vertex pipeline.
