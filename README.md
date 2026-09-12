# Kalshi crypto pricing platform

[한국어 문서](RENAME_KR.md)

A deployed market-data and volatility-pricing system for live BTC prediction
markets. It ingests crypto and Kalshi feeds, maintains a real-time Redis state,
builds offline training data in BigQuery, serves a hybrid volatility term
structure, and displays model probabilities and quote-relative edges in Dash.

The system is decision support only: it does not submit orders or manage
positions.

## Data flow

```text
Crypto + Kalshi APIs
        |
        v
    ingestion ---------------------------> gcs_exporter -> GCS
        |                                      |
        v                                      v
      Redis -> aggregator -> live_feature_service     batch_etl -> BigQuery
        |                         |                         |
        |                         +-> feast-live-bridge     v
        |                                                   ml_pipeline
        |                                                        |
        |                                                  Vertex AI + GCS
        |                                                        |
        |            analytics -> model-serving <----------------+
        |                |
        +<---------------+-- volatility and Kalshi pricing
        |
        v
     dashboard -> https://crypto-dashboard.kairos-trading.com
```

Redis Streams and latest-state keys are the real-time integration boundary.
The one synchronous business API is `analytics -> model-serving` over the
internal `POST /v1/forecast` endpoint. See [ARCHITECTURE.md](ARCHITECTURE.md)
for the complete service and interface catalog.

## Services

| Directory | Responsibility | Runtime |
|---|---|---|
| `services/ingestion` | Normalize exchange and Kalshi events into Redis Streams. Omitted Kalshi book sides become empty sides; malformed values remain rejected. | GKE Deployment `ingestion-service` |
| `services/aggregator` | Build synthetic BTC spot state, canonical per-venue books, and completed 10-second venue primitives. | GKE Deployment `aggregator` |
| `services/live_feature_service` | Turn primitives into the live `market_features/v2_10s` contract and EWMA state. | GKE Deployment `live-feature-service` |
| `services/gcs_exporter` | Archive five normalized Redis Streams to partitioned Parquet in GCS. | GKE Deployment `gcs-exporter` |
| `services/batch_etl` | Land raw data, build 10-second bars, features, and future-volatility labels in BigQuery. | GKE CronJobs |
| `services/feast_store` | Own Feast definitions, registry, online writes, optional feature serving, and parity checks. | GKE `feast-live-bridge` and Jobs/CronJobs; `feast-server` is retained at zero replicas |
| `services/ml_pipeline` | Train, evaluate, and register four horizon volatility candidates. | Vertex AI Pipelines task images |
| `services/model_serving` | Serve 5m/15m/30m EWMA forecasts plus the packaged promoted 1h XGBoost model. | GKE Deployment and Service `model-serving` |
| `services/analytics` | Price active KXBTCD contracts from live spot, features, forecasts, and Kalshi metadata. | GKE Deployment and Service `analytics` |
| `services/dashboard` | Render live BTC state, the Kalshi market/model curves, fair values, midpoint edges, and trade-aware activity age. | GKE Deployment, Service, and public Ingress `dashboard` |

Each service README documents its exact inputs, outputs, configuration,
development commands, and deployment boundary.

## Canonical v2_10s contract

The active model contract is genuinely versioned as `v2_10s`; it does not
reuse the legacy v1 feature definition.

| Layer | Contract |
|---|---|
| Raw archive | Partitioned crypto and Kalshi Parquet in GCS |
| Canonical bars | BigQuery `market_data.bars`, frequency `10s` |
| Offline features | BigQuery `feature_store.realized_volatility_v2_10s` |
| Offline targets | BigQuery `training_labels.future_realized_volatility_v2_10s` |
| Live primitives | Redis `stream:primitives:v1` |
| Live features | Redis `market:features:v2_10s:BTCUSD:latest` |
| Feast feature view | `market_features`, version `v2_10s` |
| Volatility output | Redis `market:volatility:v2_10s:BTCUSD:latest` |
| Contract pricing | Redis `market:pricing:v1:<KXBTCD market ticker>` |

Offline and online features share the same event-time meaning, 10-second
cadence, names, and model-facing values. The parity job compares recent
BigQuery rows with both the live calculation and Feast online values. The
offline label table retains `target_rv_1m` for compatibility, but 1m is not an
active training or serving horizon.

The additive `v3_10s` HAR research contract is implemented in offline, live,
Feast, and ML code, but production manifests and serving remain on `v2_10s`.

## Forecasting and Kalshi pricing

The supported horizons are `5m`, `15m`, `30m`, and `1h`. The first three use
deterministic EWMA from the live 10-second variance state. The 1h forecast uses
the immutable promoted XGBoost artifact packaged into the model-serving image
after CI validates its metadata and checksums. The running model-serving pod
therefore needs no Vertex AI, GCS, Redis, Feast, or Kalshi access.

For each active KXBTCD contract, analytics computes time to expiry in minutes,
locates it in `[0,5)`, `[5,15)`, `[15,30)`, or `[30,60)`, linearly
interpolates annualized volatility, and passes that value to the Gaussian
above-strike pricer. It publishes probability, fair value, and quote edges; it
never places an order.

The dashboard refreshes the Kalshi monitor every second and the contract table
every two seconds, and plots the model curve in yellow. `EDGE MID` is always
`Kalshi YES midpoint - model probability`. Before a contract trades, `AGE`
uses ticker or orderbook activity; afterward it measures time since the latest
distinct trade price/quantity/side fingerprint, so repeated snapshots do not
reset it.

Analytics also fits one KXBTCD midpoint implied volatility from five to seven
near-ATM contracts. Contract weights are `(open interest + 1)` normalized over
the selected set. The volatility cone displays this estimate as a dotted
yellow line alongside the model forecasts.

## Local development

Install the shared editable development environment from the repository root:

```bash
uv sync
source .venv/bin/activate
```

Service lockfiles remain the deployment source of truth when present. Validate
an individual locked service exactly as CI does with:

```bash
uv sync --directory services/<service> --locked
uv run --directory services/<service> --locked pytest
```

`live_feature_service` currently has no lockfile, so CI uses `uv sync` and
`uv run pytest` there without `--locked`.

Most long-running services require a local Redis instance. Cloud jobs additionally
require Application Default Credentials and access to the project resources named
in their README.

## Delivery

- `.github/workflows/ci.yml` tests and builds every deployed service.
- `.github/workflows/integration.yml` exercises the isolated
  ingestion/aggregator/live-feature path and restart recovery.
- `.github/workflows/cd.yml` builds immutable commit-SHA images, packages the
  approved 1h model into `model-serving`, deploys GKE resources, verifies
  rollouts, gates on offline/online parity, and smoke-tests live pricing.
- `.github/workflows/ml-pipeline.yml` tests Feast/ML code and publishes the four
  Vertex pipeline task images plus a compiled pipeline template.

The production manifests are under `k8s/`. Never deploy `latest`; CD uses the
Git commit SHA as the image tag.

## Current production contract

- Feature contract: `market_features/v2_10s`
- Forecast horizons: `5m`, `15m`, `30m`, `1h`
- The offline-only `target_rv_1m` column is retained for compatibility; 1m is
  not served.
- Live feature key: `market:features:v2_10s:BTCUSD:latest`
- Volatility key: `market:volatility:v2_10s:BTCUSD:latest`
- Kalshi IV key: `market:implied_volatility:v1:BTCUSD:latest`
- Pricing keys: `market:pricing:v1:<KXBTCD market ticker>`
- Dashboard: yellow model curve; `EDGE MID = midpoint - model`; pre-trade age
  includes orderbook activity and post-trade age follows distinct trades.
- Region: `asia-northeast3`

Volatility and pricing outputs fail closed when their upstream data or model
response is missing, stale, incompatible, or non-finite.
