# BigQuery migration boundary

The current Dask jobs remain the production writer while BigQuery contracts are
validated. The SQL files in `sql/` provision the target datasets and tables:

```text
market_data.bars
feature_store.market_features_v1
training_labels.future_realized_volatility_v1
```

The intended migration is incremental:

1. Provision the datasets and tables with `bq query --use_legacy_sql=false`.
2. Load a bounded historical GCS partition into `market_data.bars`.
3. Compare BigQuery aggregates against the existing Parquet output.
4. Implement `bigquery.io.write_frame` with partition replacement and retries.
5. Add a BigQuery writer flag to resampling, feature, and target jobs.
6. Switch Feast `FileSource` definitions to BigQuery sources only after parity.

SQL is appropriate for bucketing, OHLCV aggregation, latest-book selection,
venue averages, lag/lead, and rolling volatility. Keep tick ordering,
deduplication, late-arrival repair, and complex stateful OFI logic in Python
until equivalent SQL tests prove parity.

## Migration validation status

The datasets and tables have been provisioned in `asia-northeast3`. A bounded
Binance `1m` partition (`2026-09-01 08:00–08:59 UTC`) was loaded and compared
against the source Parquet: 60/60 rows joined with zero absolute error for
timestamp, trade price, volume, and trade count. The temporary staging table
was deleted after validation. Production writers remain unchanged pending
implementation of partition replacement in `bigquery.io`.

The parameterized `sql/010_resample_bars_1m.sql` now projects all ten book
levels and was dry-run and executed against the normalized Binance hour. All
60 timestamps and tested book prices/quantities matched the Parquet bars. One
last-trade value differed where several trades shared an identical exchange
timestamp; the SQL path intentionally uses deterministic `trade_id` ordering,
which is safer than relying on Parquet file order.

The hourly resampler now writes BigQuery `market_data.bars` by default while
retaining the GCS Parquet copy. Set `BATCH_ETL_BIGQUERY_TABLE=` to disable it.
The following replaces only the requested BigQuery hour:

```bash
PYTHONPATH=.:src uv run python scripts/run_hourly_resampling.py \
  --target-hour 2026-09-01T08:00:00Z \
  --venues binance --frequencies 1m --bigquery-table bars
```

### BigQuery realized-volatility features

Provision the feature table once, then run the hourly job (the feature CronJob
uses this command):

```bash
bq query --location=asia-northeast3 --use_legacy_sql=false \
  < sql/012_create_realized_volatility_table.sql
uv run python scripts/run_bigquery_features.py \
  --target-hour 2026-09-03T08:00:00Z
```

`011_compute_realized_volatility.sql` computes an equal-weight mean of all
available venue `1m` trade prices, log returns, and annualized realized
volatility over 1-hour and 3-hour rolling windows. It requires at least 45 and
135 observations respectively, so sparse input produces `NULL` instead of a
misleading value. BigQuery performs the aggregation and window functions;
Dask is not needed for this path.
