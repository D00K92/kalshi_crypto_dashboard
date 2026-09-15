# Batch ETL service

Builds the offline data used for model training and feature parity. Production
lands archived GCS Parquet in BigQuery, replaces canonical 10-second bar
partitions, and computes the canonical `v4_10s` features plus
future-volatility labels.
It does not own live features, Feast declarations, or model selection.

## Production pipeline

| Schedule (UTC) | Kubernetes resource | Command | Output |
|---|---|---|---|
| Minute 15 hourly | `cronjob/batch-etl` | `scripts/run_bigquery_hourly.py` | Raw landing tables and `market_data.bars` |
| Minute 30 hourly | `cronjob/batch-etl-features` | `scripts/run_bigquery_features.py --feature-version active` | `feature_store.realized_volatility_v4_10s` |
| Minute 45 hourly | `cronjob/batch-etl-targets` | `scripts/run_bigquery_targets.py` | `training_labels.future_realized_volatility_v2_10s` |

The target job defaults to a two-hour delay so each forward window has completed.
All jobs operate on an explicit UTC hour and use partition/slice replacement or
MERGE semantics so retries are idempotent.

Raw sources:

```text
gs://<bucket>/ticks/venue=<venue>/instrument=<instrument>/date=<date>/hour=<hour>/*.parquet
gs://<bucket>/books/venue=<venue>/instrument=<instrument>/date=<date>/hour=<hour>/*.parquet
```

Current production venues are Binance, Bitstamp, Coinbase, Crypto.com, Gemini,
and Kraken. The canonical live/offline cadence is 10 seconds.

## Data semantics

`market_data.bars` contains one row per event timestamp, venue, instrument,
and frequency. Trade columns include open/last/mean/high/low, buy/sell/total
volume, count, and fill intervals. Book columns retain the latest ten bid/ask
prices and aggregated quantities. The resampler reads the prior hour for
boundary continuity but writes only the requested target hour.

The canonical `v4_10s` job derives price, return, seven causal trailing
realized-volatility inputs, and three buyer-volume inputs directly from the
10-second primitive bars. It is the only active physical-table writer.
Read-only `v2_10s` and `v3_10s` compatibility views expose their original
column subsets for rollback models without duplicating feature computation.
The label job creates `target_rv_1m`, `target_rv_5m`,
`target_rv_15m`, `target_rv_30m`, and `target_rv_1h`. The 1m column is
retained in the offline table for compatibility; the active training and
serving horizons are 5m, 15m, 30m, and 1h.

Legacy Dask/GCS scripts remain available for bounded validation and rollback;
they are not the active production feature/label path.

## Run and test locally

Cloud commands require Application Default Credentials with GCS and BigQuery
access.

```bash
uv sync --locked
uv run --locked pytest

uv run --locked python scripts/run_bigquery_hourly.py \
  --target-hour 2026-09-01T08:00:00Z \
  --venues binance --frequencies 10s

uv run --locked python scripts/run_bigquery_features.py \
  --target-hour 2026-09-01T08:00:00Z --feature-version active

uv run --locked python scripts/run_bigquery_targets.py \
  --target-hour 2026-09-01T08:00:00Z --label-version v2_10s
```

Use `--dry-run` on feature/target jobs to validate SQL without writing.
For a resumable range repair:

```bash
uv run --locked python scripts/backfill_bigquery.py \
  --start-hour 2026-08-31T08:00:00Z \
  --end-hour 2026-09-02T23:00:00Z
```

The targeted v2 feature/label repair utility is
`scripts/backfill_v2_10s_features_targets.py` (now defaults to canonical
`v4_10s`; pass an older version only for rollback repair).

## Configuration

| Variable | Default / purpose |
|---|---|
| `GCP_PROJECT_ID` | `kalshi-crypto-506614` |
| `GCS_BUCKET_NAME` | `kalshi-crypto-tick-data` |
| `BATCH_ETL_TARGET_HOUR` | Previous complete hour when omitted |
| `BATCH_ETL_VENUES` | Six production crypto venues |
| `BATCH_ETL_FREQUENCIES` | `10s` |
| `BATCH_ETL_PARALLELISM` | `1`; raw BigQuery writes are intentionally serialized |
| `FEATURE_VERSION` | Script default `v4_10s`; production CronJob uses `active` to build only canonical v4 |
| `LABEL_VERSION` | `v2_10s` |
| `BATCH_ETL_BACKFILL_STATE` | Local resume-state path for range backfills |
| `BATCH_ETL_BACKFILL_LOCK` | `/tmp/kalshi-bigquery-resample.lock` |

## Deployment

Before Feast registration, CD idempotently applies
`sql/017_create_v2_v3_contract_tables.sql`; hourly feature and target queries
perform data generation only. CD then builds one immutable `batch-etl` image and applies
`k8s/batch-etl-cronjob.yaml` plus
`k8s/batch-etl-feature-cronjobs.yaml`. The `batch-etl` Kubernetes service
account needs GCS read access and BigQuery job/table permissions. CronJobs use
`concurrencyPolicy: Forbid`; inspect Job status and logs rather than an HTTP
health endpoint.
