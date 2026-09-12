# GCS exporter service

Archives the five normalized ingestion streams as Snappy-compressed Parquet in
Google Cloud Storage. It is an independent consumer and does not block
ingestion, aggregation, or live pricing.

## Delivery guarantees

- Creates each Redis consumer group at ID `0` and reclaims abandoned pending
  entries with `XAUTOCLAIM`.
- Flushes at 10,000 rows or 60 seconds after the oldest buffered row by default.
- Uploads objects with a create-only precondition and verifies CRC32C.
- ACKs a Stream entry only after its batch is safely stored.
- Writes invalid records to a stream-specific dead-letter prefix before ACK.
- Flushes buffered records during bounded graceful shutdown.
- Provides at-least-once delivery; offline readers should deduplicate by
  `event_id` or `redis_id`.

## Stream and object contracts

| Dataset | Redis stream / group | GCS prefix |
|---|---|---|
| Crypto trades | `stream:ticks` / `gcs_archiver_group` | `ticks/venue=.../instrument=.../date=.../hour=...` |
| Crypto books | `stream:orderbook_snapshots` / `gcs_orderbook_archiver_group` | `books/venue=.../instrument=.../date=.../hour=...` |
| Kalshi tickers | `stream:kalshi_tickers` / `gcs_kalshi_ticker_archiver_group` | `kalshi/tickers/series=.../event=.../market=.../instrument=.../date=.../hour=...` |
| Kalshi trades | `stream:kalshi_trades` / `gcs_kalshi_trade_archiver_group` | `kalshi/trades/series=.../event=.../market=.../instrument=.../date=.../hour=...` |
| Kalshi books | `stream:kalshi_orderbook` / `gcs_kalshi_orderbook_archiver_group` | `kalshi/orderbooks/series=.../event=.../market=.../instrument=.../date=.../hour=...` |

Partition fields live in the object path and are not duplicated in Parquet
columns. Malformed entries are stored under
`dead-letter/stream=<stream>/...`.

Kalshi streams retain a short post-export window for dashboard reads and
restarts. Crypto streams are not exporter-trimmed because `aggregator` also
owns consumer groups on them. Bybit records are excluded by default because
Bybit is not an active ingestion source.

## Run and test locally

Application Default Credentials must be able to create objects in the selected
bucket.

```bash
uv sync --locked
GCS_EXPORTER_REDIS_URL=redis://localhost:6379/0 \
GCS_BUCKET_NAME=kalshi-crypto-tick-data \
  uv run --locked gcs-exporter
uv run --locked pytest
```

The Binance historical archive utility is separate from the deployed worker:

```bash
uv run --locked python scripts/backfill_binance_trades.py \
  --start-date 2026-07-01 --end-date 2026-08-25 --dry-run
```

## Configuration

| Variable | Default |
|---|---|
| `GCS_EXPORTER_REDIS_URL` | Optional full URL; otherwise `REDIS_HOST:REDIS_PORT` |
| `GCS_BUCKET_NAME` | `kalshi-crypto-tick-data` |
| `STREAM_NAME`, `CONSUMER_GROUP` | Trade stream/group; sibling consumers are created by the entrypoint |
| `CONSUMER_NAME` | `pod-$HOSTNAME` |
| `GCS_EXCLUDED_VENUES` | `bybit` |
| `FLUSH_SIZE` | `10000` |
| `FLUSH_INTERVAL_SEC` | `60` |
| `READ_COUNT`, `READ_BLOCK_MS` | `500`, `1000` |
| `RECLAIM_INTERVAL_SEC`, `RECLAIM_MIN_IDLE_MS` | `30`, `120000` |
| `POST_EXPORT_RETENTION_SEC` | `900` |
| `SHUTDOWN_GRACE_SECONDS` | `30` |
| `HEALTH_PORT` | `8080`; sibling workers use 8081-8084 |
| `GCS_EXPORTER_LOG_LEVEL` | `INFO` |

## Deployment and health

Kubernetes runs `deployment/gcs-exporter` with service account
`gcs-exporter` from `k8s/gcs-exporter-deployment.yaml`. Workload Identity
must grant bucket-scoped object creation. `GET /healthz` is liveness and
`GET /readyz` reports Redis/group readiness for the primary worker. The
endpoint is pod-local.
