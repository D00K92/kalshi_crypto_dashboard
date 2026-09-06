# Kalshi Crypto Dashboard

A production-oriented, cloud-native market-data platform built as a portfolio project. The repository currently ingests live Binance BTC/USDT market data into Redis Streams and archives trade ticks to partitioned Parquet objects in Google Cloud Storage.

## Current data flow

```text
Binance WebSocket
        |
        v
Python asyncio ingestion service
        |
        +--> Redis Pub/Sub (live consumers)
        |
        +--> Redis Streams (durable handoff)
                    |
                    v
             GCS exporter
                    |
                    v
       Snappy-compressed Parquet in GCS
```

Both services are independently containerized and deployed to Google Kubernetes Engine (GKE). The GCS exporter uses a Redis consumer group, acknowledges records only after upload, and uses create-only GCS writes for retry-safe delivery.

## Implemented

- Binance `BTCUSDT` trade and depth WebSocket ingestion
- Normalized tick events published to Redis
- Redis Streams consumer-group processing
- Size- or time-based Parquet batching with PyArrow
- Hourly UTC GCS partitioning by venue and instrument
- Dead-letter storage for malformed records
- Kubernetes manifests, health probes, and Workload Identity access
- Unit tests for ingestion and archival behavior

## Roadmap

- Coinbase and Deribit WebSocket adapters
- GitHub Actions CI/CD for Artifact Registry and GKE
- Real-time Plotly Dash interface
- Analytics and backtesting pipelines

## Repository layout

```text
services/ingestion/      Live WebSocket ingestion and Redis publishing
services/gcs_exporter/   Redis-to-Parquet archival service
k8s/                     Kubernetes deployment manifests
docs/                    Architecture and deployment plans
```

Each service has its own README with local development, test, container, and deployment details.

## Repository-wide development environment

The root environment installs every service as an editable package and adds
JupyterLab, plotting, and test tooling. Service-specific environments and
lockfiles remain the source of truth for deployment.

```bash
uv sync
source .venv/bin/activate
jupyter lab
```

Select the `.venv/bin/python` kernel when opening notebooks. Local batch ETL
validation data is under `services/batch_etl/tmp/` and is ignored by Git.

Use this root environment for local development across all services. Running
`uv run --directory services/<service> ...` creates a separate service-level
`.venv`; reserve that workflow for validating an individual production lock.
To run a service's tests with the shared environment, invoke the root pytest
executable from that service directory, for example:

```bash
cd services/batch_etl
../../.venv/bin/pytest
```

## Technology

Python, asyncio, uvloop, orjson, Redis Streams, PyArrow, Parquet, Docker, Kubernetes, Google Kubernetes Engine, Google Cloud Storage, and Workload Identity Federation.

## Status

The Binance-to-Redis-to-GCS path is deployed and verified. Other exchanges and downstream dashboard/analytics services remain planned work.
