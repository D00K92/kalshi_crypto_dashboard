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

GCS inputs are raw `ticks/` and `books/` partitions. Batch ETL writes canonical
bars and v1 realized-volatility features to BigQuery; the active Feast source
is `kalshi-crypto-506614.feature_store.realized_volatility_v1`. Targets remain
outside Feast. The online store uses the private Memorystore endpoint. The
Kubernetes DNS address currently in `feature_store.yaml` is not valid from
Cloud Run and must be replaced during deployment.

## Layout

```text
feast_store/
├── feature_store.yaml       # Feast project/provider/registry configuration
├── requirements.txt         # isolated Feast runtime dependencies
├── entities.py              # entity declarations
├── data_sources.py          # offline source declarations
├── definitions/             # entities, sources, views, and services
├── jobs/                    # apply, materialize, and backfill entrypoints
├── src/feast_repo/           # shared config and validation interfaces
├── src/feast_repo/           # shared config and interfaces
└── tests/                   # repository-level contract tests
```

The low-latency bridge is `jobs/live_features.py`. It consumes the
`stream:features:v1` stream emitted by `aggregator` and writes
`v1_market_features` with `FeatureStore.write_to_online_store`. BigQuery remains
the offline source; hourly materialization is retained for reconciliation and
recovery, not for the real-time inference path.

Feast configuration files (`feature_store.yaml`, `pyproject.toml`, and the
Dockerfile) remain at the root because Feast and the container build expect
them there. All Python declarations live under `definitions/`; operational
commands live under `jobs/`.
