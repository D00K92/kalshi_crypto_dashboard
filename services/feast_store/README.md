# Feast feature repository (`services/feast_store`)

This directory is the single ownership boundary for offline and online Feast
features. Feature formulas remain in `batch_etl`; this service owns versioned
contracts, Feast registration, materialization, and low-latency online writes.

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
├── definitions/             # entities, sources, views, and services
├── registry/                # immutable live feature contracts
├── jobs/                    # apply, materialize, and live-push entrypoints
├── src/feast_repo/           # shared config and validation interfaces
└── tests/                   # repository-level contract tests
```

The low-latency bridge is `jobs/live_push.py`. It consumes the
`stream:features:v1` stream emitted by `aggregator`, resolves the immutable
`feature_set`/`feature_version` contract in `registry/feature_specs.py`, and
pushes validated rows to the corresponding Feast PushSource. BigQuery remains
the offline source; hourly materialization is retained for reconciliation and
recovery, not for the real-time inference path. The old `live_features.py`
entrypoint remains as a compatibility wrapper.

The internal Kubernetes service `feast-server:6566` runs `feast serve` against
the same repository and Redis online store. Inference workloads should call
this service for online feature reads; it is intentionally a `ClusterIP` and
is not exposed publicly.

To add a feature version, register an immutable `FeatureSpec`, define its
PushSource/FeatureView and model-facing FeatureService, then apply the Feast
repository before deploying a producer that emits the new version. Producers
must include entity data, event and creation timestamps, and a `values` object.

Feast configuration files (`feature_store.yaml`, `pyproject.toml`, and the
Dockerfile) remain at the root because Feast and the container build expect
them there. All Python declarations live under `definitions/`; operational
commands live under `jobs/`.
