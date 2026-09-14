# ML pipeline service

Owns training-data assembly, horizon-specific model training, evaluation,
promotion decisions, and Vertex AI model registration. It consumes the existing
BigQuery contracts and does not compute live features or serve inference.

## Model contract

| Item | Current value |
|---|---|
| Feature set/version | Training default `market_features/v3_10s`; live 5m candidate uses `v4_10s` |
| Model inputs | Horizon-specific realized volatility; v4 5m also uses buyer-volume windows |
| Label table | `training_labels.future_realized_volatility_v2_10s` |
| Horizons | `5m`, `15m`, `30m`, `1h` |
| Split | Chronological 70% train / 15% validation / 15% test |
| Candidate model | Non-negative HAR linear regression per horizon; XGBoost remains selectable |
| Benchmark | Annualized EWMA, decay `0.96` |
| Primary metric | QLIKE |
| Champion metrics | `gs://kalshi-crypto-tick-data/models/v3_har_1/champion_metrics.json` |

Training rejects current-day ranges and exact-timestamp joins BigQuery labels to
the immutable feature contract. Feast is not part of the training-data path.
Promotion requires a candidate to beat
EWMA by 2% and be no more than 5% worse than the current champion.

The pipeline can train and register all four horizons. The online
`model-serving` bundle packages the v4 5m HAR and the approved 1h XGBoost;
15m/30m use EWMA.

`v4_10s` adds
`log(1 + buyer volume)` over 30-second, 5-minute, and 10-minute trailing windows
to the existing v3 5m HAR inputs. The live producer publishes this version on
separate Redis keys while v2 remains warm for rollback; Feast is not on the
inference path.

## Pipeline DAG

```text
load point-in-time table
        |
        +-- for each 5m / 15m / 30m / 1h in parallel
                train -> evaluate vs EWMA/champion -> conditional register
```

Task images are:

- `ml-load`
- `ml-train`
- `ml-evaluate`
- `ml-register`

Registered artifacts contain `model.joblib` and `metadata.json`; metadata
records the exact horizon, feature set/version, ordered feature columns, label
version, architecture, row split, and metrics. The `architecture` pipeline
parameter selects `har` or `xgboost` without changing the DAG.

## Run locally

Python 3.12 and GCP credentials are required for BigQuery, GCS, and Vertex
operations.

```bash
uv sync --locked
uv run --locked pytest

uv run --locked python scripts/compile_pipeline.py \
  --output pipeline.yaml --image-tag <immutable-git-sha>

uv run --locked python scripts/run_pipeline.py \
  --template pipeline.yaml \
  --project kalshi-crypto-506614 \
  --location asia-northeast3 \
  --pipeline-root gs://kalshi-crypto-tick-data/pipeline-root \
  --start-date 2026-08-31 --end-date 2026-09-02 \
  --model-version v3_har_1 --feature-version v3_10s
```

Use a service account with the necessary Vertex, BigQuery, GCS, and Artifact
Registry permissions when submitting production runs. Do not point production
at a display name or mutable alias; record the exact versioned Vertex resource
and immutable artifact URI from an approved promotion.

## Configuration

| Variable | Default / purpose |
|---|---|
| `ML_PIPELINE_REGISTRY` | `asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/ml-pipeline` |
| `ML_PIPELINE_IMAGE_TAG` | Component image tag used by the compiled DAG |
| `GITHUB_SHA` | Default immutable tag for compilation in CI |
| Pipeline parameters | Project, location, date range, feature/model version, bucket, and champion metrics URI |

## CI and release relationship

`.github/workflows/ml-pipeline.yml` tests this service and
`feast_store`. On a main-branch change it publishes all four task images with
the commit SHA and uploads a compiled Vertex template. Publishing the template
does not submit a run or promote a model.

Production release variables identify the exact 5m v4 and 1h resources and
artifact URIs. The services CD workflow validates and packages those bytes
into the model-serving image.
