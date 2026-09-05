# Analytics

`analytics` owns lightweight Kalshi contract pricing and simple price
indicators. It should stay small and should not depend on Dask.

Planned scope:

- port pricing logic from the v1 code snippets
- keep small price indicators that can run in process
- consume processed Parquet outputs from `batch_etl` when heavier market
  features are needed

Out of scope:

- Dask jobs
- raw GCS tick/book backfills
- expensive feature generation

Run tests:

```bash
  UV_CACHE_DIR=/tmp/kalshi-analytics-uv-cache uv run pytest
```
