# GCS Exporter

Consumes normalized crypto and Kalshi market-data Redis Streams and archives
them as in-memory, Snappy-compressed Parquet objects in GCS.

## Delivery contract

- Redis consumer groups are created at ID `0`
- Flush: 10,000 rows or 60 seconds after the oldest buffered row
- ACK: only after a create-only, CRC32C-checked GCS upload
- Recovery: stale pending entries are reclaimed with `XAUTOCLAIM`
- Malformed data: written to `dead-letter/stream=ticks/` before ACK
- Object partitions: venue/instrument/date/hour for crypto; series/event/market/instrument/date/hour for Kalshi

The pipeline is at-least-once. Backtests should use `event_id` or `redis_id` to
deduplicate records if a process dies between a successful upload and its ACK.

## Local tests

```bash
uv sync --directory services/gcs_exporter
uv run --directory services/gcs_exporter pytest
```

## Container

The platform is selected by the build command, not hard-coded in the
Dockerfile:

```bash
docker buildx build \
  --platform linux/amd64 \
  --tag asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/quant-repo/gcs-exporter:<immutable-tag> \
  --push \
  services/gcs_exporter
```

Use an immutable version or commit tag. Do not deploy `latest`.

## One-time GCP setup

The bucket and Workload Identity IAM binding are external GCP resources and
cannot be created by the Kubernetes Deployment manifest.

```bash
export PROJECT_ID="kalshi-crypto-506614"
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
export REGION="asia-northeast3"
export BUCKET="kalshi-crypto-tick-data"
export NAMESPACE="default"
export KSA_NAME="gcs-exporter"

gcloud storage buckets create "gs://$BUCKET" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --default-storage-class=STANDARD \
  --uniform-bucket-level-access \
  --public-access-prevention

gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --role="roles/storage.objectCreator" \
  --member="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT_ID.svc.id.goog/subject/ns/$NAMESPACE/sa/$KSA_NAME" \
  --condition=None
```

If the bucket already exists in this project, inspect it instead of running the
create command again. Bucket names are globally unique.

## Render and deploy

```bash
export GCS_EXPORTER_IMAGE="asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/quant-repo/gcs-exporter:<immutable-tag>"
export REDIS_HOST="<memorystore-private-ip>"
export REDIS_PORT="6379"
export GCS_BUCKET_NAME="kalshi-crypto-tick-data"

envsubst < k8s/gcs-exporter-deployment.yaml | kubectl apply -f -
kubectl rollout status deployment/gcs-exporter --timeout=5m
kubectl logs deployment/gcs-exporter --tail=100
```

## Runtime configuration

| Variable | Default |
|---|---|
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `GCS_EXPORTER_REDIS_URL` | constructed from host and port |
| `GCS_BUCKET_NAME` | `kalshi-crypto-tick-data` |
| `STREAM_NAME` | `stream:ticks` |
| `CONSUMER_GROUP` | `gcs_archiver_group` |
| `CONSUMER_NAME` | `pod-$HOSTNAME` |
| `FLUSH_SIZE` | `10000` |
| `FLUSH_INTERVAL_SEC` | `60` |
| `RECLAIM_MIN_IDLE_MS` | `120000` |
| `RECLAIM_INTERVAL_SEC` | `30` |
| `HEALTH_PORT` | `8080` |

The default entry point runs five consumers in one process:

| Dataset | Stream | Consumer group |
|---|---|---|
| Crypto trades | `stream:ticks` | `gcs_archiver_group` |
| Crypto order books | `stream:orderbook_snapshots` | `gcs_orderbook_archiver_group` |
| Kalshi tickers | `stream:kalshi_tickers` | `gcs_kalshi_ticker_archiver_group` |
| Kalshi trades | `stream:kalshi_trades` | `gcs_kalshi_trade_archiver_group` |
| Kalshi order books | `stream:kalshi_orderbook` | `gcs_kalshi_orderbook_archiver_group` |

`GET /healthz` is the liveness endpoint. `GET /readyz` succeeds after Redis is
reachable and the consumer group is available.

## Operational constraint

The ingestion stream must retain at least 24 hours of peak traffic. The current
count-based approximate trim cannot guarantee that duration until peak events
per second are measured. Do not treat Redis as the archive of record.
