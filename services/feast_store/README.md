# Feast feature repository (`services/feast_store`)

This directory is the single ownership boundary for offline and online Feast
features. It is intentionally a skeleton: feature computation and schemas are
not implemented yet.

## Current GCP contract

| Resource | Current value | Role |
|---|---|---|
| Project | `kalshi-crypto-506614` | GCP project |
| Region | `asia-northeast3` | GKE, Artifact Registry, and planned Cloud Run region |
| GCS bucket | `gs://kalshi-crypto-tick-data` | raw data, resampled data, features, targets, and Feast registry |
| GKE cluster | `quant-cluster` | current batch ETL and Redis workloads |
| Artifact Registry | `asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/quant-repo` | production images |
| Memorystore Redis | discovered by CI with `gcloud redis instances list` | Feast online store |

GCS inputs are raw `ticks/` and `books/` partitions. Batch ETL writes
`processed/resampled_market_data/`, `features/v1/`, and
`processed/future_realized_volatility/`. The Feast offline store will read the
feature Parquet contract; the online store will use the private Memorystore
endpoint. The Kubernetes DNS address currently in `feature_store.yaml` is not
valid from Cloud Run and must be replaced during deployment.

## Layout

```text
feast_store/
├── feature_store.yaml       # Feast project/provider/registry configuration
├── requirements.txt         # isolated Feast runtime dependencies
├── entities.py              # entity declarations
├── data_sources.py          # offline source declarations
├── feature_views/           # versioned FeatureView declarations
├── feature_services/        # model-facing feature-service declarations
├── transformations.py       # placeholder feature computation boundary
├── validation.py            # placeholder schema/freshness checks
├── jobs/                    # apply, materialize, and backfill entrypoints
├── src/feast_repo/           # shared config and interfaces
└── tests/                   # repository-level contract tests
```

`definitions/` and the root `entities.py` remain temporarily as the current
working definition while the new layout is filled in. They should be removed
only after the new declarations pass `feast apply` and retrieval tests.
