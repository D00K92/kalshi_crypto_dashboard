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

## Technology

Python, asyncio, uvloop, orjson, Redis Streams, PyArrow, Parquet, Docker, Kubernetes, Google Kubernetes Engine, Google Cloud Storage, and Workload Identity Federation.

## Status

The Binance-to-Redis-to-GCS path is deployed and verified. Other exchanges and downstream dashboard/analytics services remain planned work.
