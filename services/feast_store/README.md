# Feast feature store

This directory is the ownership boundary for feature contracts and Feast
operations. `batch_etl` computes offline values and
`live_feature_service` computes online values; Feast declares how those values
are registered, retrieved, pushed, materialized, and compared.

## Current contract

| Item | Value |
|---|---|
| Entity | BTC asset, currently `BTCUSD` |
| Active feature set/version | `market_features/v2_10s` |
| Fields | `synthetic_price`, `log_return`, `venue_count` |
| Offline source | `kalshi-crypto-506614.feature_store.realized_volatility_v2_10s` |
| Online store | Memorystore Redis configured in `feature_store.yaml` |
| Registry | `gs://kalshi-crypto-tick-data/feature_store/registry.db` |
| Live input | `stream:features:v2_10s` |

Targets are intentionally outside Feast. EWMA state is an inference-only field
in the live envelope and is not a registered Feast feature.

The additive `v3_10s` HAR contract is also registered for research and
migration work. Production live writes and parity remain on `v2_10s`.

## Components

- `definitions/`: entities, BigQuery sources, PushSources, FeatureViews, and
  FeatureServices.
- `registry/feature_specs.py`: immutable producer/consumer validation specs.
- `jobs/apply.py`: apply definitions to the GCS registry.
- `jobs/live_push.py`: entrypoint for the live Redis Stream-to-Feast bridge.
- `jobs/materialize.py`: materialize completed offline intervals.
- `jobs/parity.py`: compare recent online and BigQuery values.
- `feature_store.yaml`: shared provider, registry, offline, and online stores.

## Runtime flow

`deployment/feast-live-bridge` consumes `stream:features:v2_10s` with group
`feast-live-features-v2-10s`. It validates each envelope, pushes it to the
versioned Feast PushSource, and ACKs only success or a permanent validation
failure. Transient failures remain pending for reclamation.

The `feast-server:6566` ClusterIP Service and Deployment are retained as a
reversible compatibility shell, but the Deployment is intentionally set to
zero replicas because there are no live HTTP feature clients. Analytics reads
the latest feature snapshot and forwards it to model-serving directly.

`cronjob/feature-parity-v2-10s` runs every 15 minutes. Exit code 0 means
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
  --repo-path . --redis-url redis://127.0.0.1:6379/0
```

Running apply, materialization, or parity requires GCP credentials and access to
the GCS registry, BigQuery source, and Redis endpoint.

## Configuration

| Variable | Used by | Default / purpose |
|---|---|---|
| `FEAST_REPO_PATH` | Apply, bridge, parity | Repository directory |
| `FEATURE_VERSION` | Contract resolution | `v2_10s` |
| `FEATURE_STREAM` | Bridge/parity | `stream:features:v2_10s` |
| `FEATURE_KEY` | Parity | `market:features:v2_10s:BTCUSD:latest` |
| `FEAST_LIVE_GROUP` | Bridge | Deployment uses `feast-live-features-v2-10s` |
| `FEAST_PENDING_IDLE_MS` | Bridge | `120000` |
| `REDIS_URL` | Bridge/parity | Required production Memorystore URL |
| `GCP_PROJECT_ID`, `GCP_REGION` | Parity/config | Project and `asia-northeast3` |
| `PARITY_OFFLINE_TABLE` | Parity | Active v2_10s BigQuery feature table |
| `GCS_BUCKET` | Shared config | `kalshi-crypto-tick-data` |

## Change and deployment rules

Add a new version by registering an immutable `FeatureSpec`, defining its
BigQuery/PushSource/FeatureView/FeatureService, applying the registry, and only
then deploying a producer. Do not edit a deployed contract in place.

CD builds `feast-store`, runs the one-shot high-memory apply Job, deploys the
bridge, applies the suspended server compatibility manifest, and applies the
parity CronJob. These resources use the `batch-etl` Kubernetes service account
and are not publicly exposed.
