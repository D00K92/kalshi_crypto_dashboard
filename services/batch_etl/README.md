# Batch ETL

`batch_etl` owns expensive Dask jobs that read raw tick and order-book Parquet
from GCS, resample them onto regular time grids, and write processed Parquet
back to GCS. Modeling transformations belong in `ml_pipeline`.

Raw input layout:

```text
gs://<bucket>/ticks/venue=<venue>/instrument=<instrument>/date=<yyyy-mm-dd>/hour=<hh>/*.parquet
gs://<bucket>/books/venue=<venue>/instrument=<instrument>/date=<yyyy-mm-dd>/hour=<hh>/*.parquet
```

Default processed output layout:

```text
gs://<bucket>/processed/resampled_market_data/frequency=<1s|5s|1m|5m|10m|30m|1h>/date=<yyyy-mm-dd>/hour=<hh>/venue=<venue>/*.parquet
```

The resampling job applies these data semantics:

- one row per `timestamp` and `venue`
- trade prices: `last` for `p_trade`, `max` for `p_high`, `min` for `p_low`, then forward-fill
- trade volumes: `sum` for `v_trade`, `v_buy`, and `v_sell`, then zero-fill
- trade count: `cnt_trade` counts positive-volume trades in each period, then zero-fill
- fill timing: `dt_fill_mean_ms`, `dt_fill_max_ms`, and `dt_fill_min_ms`
  aggregate milliseconds between consecutive positive-volume trades whose
  current trade lands in the period
- order-book prices: `last` for `p_bid_1` through `p_bid_10` and `p_ask_1` through `p_ask_10`, then forward-fill
- order-book quote volumes: `sum` for `q_bid_1` through `q_bid_10` and `q_ask_1` through `q_ask_10`, then zero-fill
- multi-venue jobs concatenate venue rows instead of prefixing venue names into columns

## Hourly Kubernetes job

The production CronJob runs at minute 15 of every UTC hour and processes the
previous complete hour for all configured venues and frequencies. Each task
also reads the preceding raw hour so the first fill interval and forward-filled
prices at the target-hour boundary are correct. Only rows from the target hour
are written.

Each `date/hour/venue` partition is replaced independently and compacted to one
Parquet file. A retry is therefore idempotent and cannot delete sibling hours,
venues, or frequencies.

```text
schedule: 15 * * * * (Etc/UTC)
target at 09:15 UTC: 08:00:00 through 08:59:59 UTC
source context: 07:00:00 through 08:59:59 UTC
```

Run a specific hour manually from the unified development environment:

```bash
../../.venv/bin/python scripts/run_hourly_resampling.py \
  --target-hour 2026-09-01T08:00:00Z \
  --venues binance \
  --frequencies 1s
```

`BATCH_ETL_VENUES`, `BATCH_ETL_FREQUENCIES`, `GCS_BUCKET_NAME`, and
`BATCH_ETL_OUTPUT_DATASET` provide the corresponding container configuration.

### One-time Workload Identity setup

The dedicated `batch-etl` Kubernetes service account needs bucket-scoped object
read/write/delete access. This is required because retries replace only their
target partition.

```bash
export PROJECT_ID="kalshi-crypto-506614"
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
export BUCKET="kalshi-crypto-tick-data"
export NAMESPACE="default"
export KSA_NAME="batch-etl"

gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --role="roles/storage.objectUser" \
  --member="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT_ID.svc.id.goog/subject/ns/$NAMESPACE/sa/$KSA_NAME" \
  --condition=None
```

The CD workflow builds an immutable `batch-etl` image, applies
`k8s/batch-etl-cronjob.yaml`, and confirms that the CronJob exists. The manifest
uses `concurrencyPolicy: Forbid`; a run that exceeds 55 minutes is terminated so
jobs cannot overlap.

Physical Parquet columns:

```text
timestamp
p_open
p_trade
p_close
p_trade_mean
p_high
p_low
v_trade
v_buy
v_sell
cnt_trade
dt_fill_mean_ms
dt_fill_max_ms
dt_fill_min_ms
p_bid_1 ... p_bid_10
p_ask_1 ... p_ask_10
q_bid_1 ... q_bid_10
q_ask_1 ... q_ask_10
```

`date`, `hour`, and `venue` are Hive partition keys encoded only in directory
names. They are not duplicated in the Parquet file payload. Reading from the
dataset root with Hive partition discovery reconstructs all three as logical
columns.

Build one cadence:

```bash
UV_CACHE_DIR=/tmp/kalshi-batch-etl-uv-cache uv run \
  python scripts/build_resampled_market_data.py \
  --start-date 2026-09-01 --end-date 2026-09-01 \
  --venue binance --frequency 1s --overwrite
```

Run a bounded local validation output:

```bash
UV_CACHE_DIR=/tmp/kalshi-batch-etl-uv-cache uv run \
  python scripts/build_resampled_market_data.py \
  --start-date 2026-09-01 --end-date 2026-09-01 \
  --venue binance --hour 08 --frequency 5s \
  --output /tmp/kalshi-resampled-binance-5s --overwrite
```

Validate a live GCS slice before a backfill:

```bash
UV_CACHE_DIR=/tmp/kalshi-batch-etl-uv-cache uv run \
  python scripts/validate_gcs_dask.py \
  --date 2026-09-01 --dataset all --venue binance --sample-rows 1
```

Run tests:

```bash
UV_CACHE_DIR=/tmp/kalshi-batch-etl-uv-cache uv run pytest
```

Open data structure questions:

- Whether `frequency` should remain a path component or become a Hive partition
  column at the same level as `date` and `hour`.

### Hourly features and Feast sync

After resampling completes, build the single venue-agnostic feature partition:

```bash
python scripts/build_features.py --target-hour 2026-09-01T08:00:00Z
```

The job writes `features/v1/date=YYYY-MM-DD/features.parquet`. Feast
definitions and materialization are owned by this service under `feature_store/`
and should run in a Feast-compatible image using `feature_store/requirements.txt`.

### Resumable backfill

Backfill any bounded UTC range; `--resume` skips partitions already present:

```bash
python scripts/backfill_features_targets.py \
  --start-hour 2026-08-31T08:00:00Z \
  --end-hour 2026-09-02T23:00:00Z \
  --resume
```

Use `--features-only` or `--targets-only` to retry one side independently.
