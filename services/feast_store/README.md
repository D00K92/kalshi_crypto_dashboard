# Feast feature store

This directory is the ownership boundary for feature contracts and Feast
operations. `batch_etl` computes offline values and
`live_feature_service` computes online values. Production parity compares the
active Redis stream directly with BigQuery; Feast remains available for
offline definitions, registry operations, and research materialization.

## Current contract

| Item | Value |
|---|---|
| Entity | BTC asset, currently `BTCUSD` |
| Active parity contract | `market_features/v4_10s` |
| Fields | `synthetic_price`, `log_return`, `venue_count` |
| Offline source | `kalshi-crypto-506614.feature_store.realized_volatility_v4_10s` |
| Online source | Memorystore Redis immutable feature stream |
| Registry | `gs://kalshi-crypto-tick-data/feature_store/registry.db` |
| Live input | `stream:features:v4_10s` |

Targets are intentionally outside Feast. EWMA state is an inference-only field
in the live envelope and is not a registered Feast feature.

The Feast v2 and v3 definitions remain registered for research and historical
reproducibility. They are not part of production inference.

## Components

- `definitions/`: entities, BigQuery sources, PushSources, FeatureViews, and
  FeatureServices.
- `registry/feature_specs.py`: immutable producer/consumer validation specs.
- `jobs/apply.py`: apply definitions to the GCS registry.
- `jobs/live_push.py`: retained offline utility for explicit Feast push work.
- `jobs/materialize.py`: materialize completed offline intervals.
- `jobs/parity.py`: compare recent online and BigQuery values.
- `feature_store.yaml`: shared provider, registry, offline, and online stores.

## Runtime flow

The `feast-server:6566` ClusterIP Service and Deployment are retained as a
reversible compatibility shell, but the Deployment is intentionally set to
zero replicas because there are no live HTTP feature clients. Analytics reads
the latest feature snapshot and forwards it to model-serving directly.

`cronjob/feature-parity-v4-10s` runs every 15 minutes. Exit code 0 means
online/offline parity passed, 1 means drift or missing data, and 2 means the
check itself could not run. CD also launches an immediate parity Job as a
release gate.

## Local commands

Python 3.12 is required.

```bash
uv sync --locked
uv run --locked pytest

uv run --locked python -m jobs.apply --repo-path .
uv run --locked python -m jobs.materialize \
  --repo-path . --end-time 2026-09-07T04:00:00Z
uv run --locked python -m jobs.parity \
  --redis-url redis://127.0.0.1:6379/0
```

Running apply, materialization, or parity requires GCP credentials and access to
the GCS registry, BigQuery source, and Redis endpoint.

## Configuration

| Variable | Used by | Default / purpose |
|---|---|---|
| `FEAST_REPO_PATH` | Apply/materialize | Repository directory |
| `FEATURE_VERSION` | Parity contract | `v4_10s` |
| `FEATURE_STREAM` | Parity | `stream:features:v4_10s` |
| `FEATURE_KEY` | Parity | `market:features:v4_10s:BTCUSD:latest` |
| `REDIS_URL` | Parity | Required production Memorystore URL |
| `GCP_PROJECT_ID`, `GCP_REGION` | Parity/config | Project and `asia-northeast3` |
| `PARITY_OFFLINE_TABLE` | Parity | Active v4_10s BigQuery feature table |
| `GCS_BUCKET` | Shared config | `kalshi-crypto-tick-data` |

## Change and deployment rules

Add a new version by registering an immutable `FeatureSpec`, defining its
BigQuery/PushSource/FeatureView/FeatureService, applying the registry, and only
then deploying a producer. Do not edit a deployed contract in place.

CD builds `feast-store`, preserves the existing registry, applies the suspended
server compatibility manifest, and applies the v4 parity CronJob. These
resources use the `batch-etl` Kubernetes service account and are not publicly
exposed.
