# High-Frequency Quant & Prediction Market Analytics Platform

## System Architecture
```
[Live Exchange Feeds]
├─ Binance  (BTC/USDT Tick & Depth)
├─ Coinbase (BTC/USD  Tick & Depth)
├─ Deribit  (BTC/USD  Tick & Options IV)
└─ Kalshi   (Market Contracts)
│
│ WebSocket Connection (Asyncio / websockets)
▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Data Ingestion & Aggregation Engine (Python Asyncio / uv)               │
│ - Tick Data Normalization                                               │
│ - In-Memory Multi-Exchange Orderbook Aggregation (Binance+Coinbase+Deribit)│
└───────┬─────────────────────────────────────────────────┬───────────────┘
│                                                 │
│ (1) Ultra-Fast Pub/Sub Push                     │ (2) Persistent Stream Ingestion
▼                                                 ▼
┌───────────────────────────────┐                 ┌──────────────────────┐
│ Redis Pub/Sub                 │                 │ Redis Streams        │
│ Channel: pub:orderbook      │                 │ Key: stream:ticks  │
│ Channel: pub:btc_ticks      │                 └───────┬──────────────┘
└───────┬───────────────────────┘                         │
│                                                 │ (Consumer Groups)
│ (Ephemeral Broadcasting                         │
│  via WebSockets)                                ├───────────────────────────────┐
│                                                 ▼                               ▼
│                                 ┌───────────────────────────────┐ ┌─────────────┐
│                                 │ ML & Analytics Engine         │ │ DB Ingestion│
│                                 │ - Gram-Charlier Model         │ │ Worker      │
│                                 │ - ML Inference (FastAPI/ONNX) │ │ - PyArrow   │
│                                 │ - Fair Value = Model + ML Adj │ └──────┬──────┘
│                                 └───────┬───────────────────────┘        │
│                                         │                                │ (Micro-batch)
│                                         │ (Computed Fair Value / Edge)   │
▼                                         ▼                                ▼
┌──────────────────────────────────────────────────────────────────────────┐ ┌─────────────┐
│ Real-Time Trading Dashboard (Plotly Dash)                                │ │ Storage     │
│                                                                          │ │ & Batch     │
│ ┌──────────────────────┐ ┌───────────────────┐ ┌───────────────────────┐ │ │ Layer       │
│ │ Aggregated Orderbook │ │ Real-Time Chart   │ │ Kalshi Contract       │ │ │             │
│ │ (Binance+CB+Deribit) │ │ (BTC Synthetic)   │ │ Monitor               │ │ │ ClickHouse  │
│ └──────────┬───────────┘ └─────────┬─────────┘ │ - Fair Value & Edge   │ │ │ (Tick TSDB) │
│            │                       │           └───────────┬───────────┘ │ │      ▲      │
│            └───────────────────────┼───────────────────────┘             │ │      │      │
│                                    ▼                                     │ │      │      │
│                    Dash Clientside Callbacks (JS/Plotly.js)              │ │ PySpark/Dask│
│                    - Zero Python I/O Overhead                            │ │ (Batch ETL) │
│                    - DOM Reflow Minimization & WebGL                     │ └─────────────┘
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Project Overview

Designed and built an end-to-end, ultra-low latency quantitative data pipeline and real-time visualization platform. The system ingests tick-level order book data from major cryptocurrency exchanges (Binance, Coinbase, Deribit) and prediction markets (Kalshi), synthesizes multi-exchange L2 depth in memory, and evaluates contract fair values using a hybrid statistical (Gram-Charlier Expansion) and Machine Learning (ONNX Runtime) engine.

---

## Tech Stack

* **Languages & Core Runtimes:** Python 3.11+, JavaScript/ES6, `uv` Package Manager
* **Concurrency & Messaging:** Python `asyncio`, `websockets`, Redis 7.x (Hybrid: Streams for persistent worker ingestion, Pub/Sub for sub-50ms UI broadcasting)
* **Distributed Processing & Storage:** PySpark / Dask, ClickHouse (Columnar Time-Series DB), GCP Cloud Storage (Parquet Lakehouse), PyArrow
* **Machine Learning & Quantitative Analytics:** ONNX Runtime (C++ backend wrapper), Gram-Charlier Expansion Model, NumPy, SciPy, Evidently AI (Model Drift Monitoring)
* **Visualization & Frontend Engine:** Plotly Dash, Clientside Callbacks (JavaScript/Plotly.js), WebGL Canvas Rendering

---

## Key Achievements & Technical Impact

* **Decoupled Messaging Infrastructure for Zero Data Loss & Low Latency:**
  Replaced high-overhead polling queues with a hybrid Redis messaging bus. Utilized **Redis Pub/Sub** for ephemeral 50ms UI streaming while leveraging **Redis Streams** with Consumer Groups for reliable, asynchronous database persistence and downstream inference.

* **High-Throughput In-Memory Orderbook Aggregation:**
  Engineered a multi-threaded Python Asyncio ingestion worker that normalizes and aggregates raw Level 2 tick/depth updates across Binance, Coinbase, and Deribit in real time, serving unified market depth visuals without blocking event loops.

* **Bypassed Python GIL Bottlenecks in Dashboard Rendering:**
  Eliminated server-side I/O contention in Plotly Dash by converting chart updates into client-side JavaScript callbacks (`ClientsideCallbacks`). Pushed direct WebSocket payloads straight into browser-side WebGL buffers, achieving smooth 60 FPS real-time rendering.

* **Scalable Feature Engineering with PySpark/Dask:**
  Implemented a distributed offline ETL pipeline processing tens of millions of daily tick records stored in Parquet/ClickHouse format. Accelerated high-dimensional feature extractions (Parkinson Volatility, Hurst Exponents, Order Flow Imbalance) by over 80%.

* **Hybrid Quant + ML Pricing Engine Integration:**
  Constructed a FastAPI-based inference service wrapping C++ ONNX Runtime. Combined higher-moment statistical skewness/kurtosis (Gram-Charlier model) with real-time ML adjustments to dynamically calculate prediction contract fair values and market edge.


## Repo Structure (subject to change)
Mono-repository (Monorepo) or distinct repositories—and containerizing/deploying each service independently via Docker is the industry standard for high-throughput quantitative and distributed systems.

kalshi_crypto/
├── .github/
│   └── workflows/                # CI/CD pipelines per service
│       ├── deploy-ingestion.yml
│       ├── deploy-analytics.yml
│       └── deploy-dashboard.yml
├── k8s/                          # Kubernetes manifests per service
│   ├── ingestion-deployment.yaml
│   ├── analytics-deployment.yaml
│   └── dashboard-deployment.yaml
│
├── shared/                       # Shared models, constants, protobuf/schemas
│   ├── __init__.py
│   └── schemas.py                # Data types (Tick, Orderbook, FairValue)
│
├── services/
│   ├── ingestion/                # Ingestion Service (Asyncio + WebSocket)
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── src/
│   │
│   ├── analytics/                # ML & Pricing Engine (FastAPI + ONNX)
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── src/
│   │
│   ├── dashboard/                # Real-Time UI (Plotly Dash)
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── src/
│   │
│   └── batch_etl/                # Offline Spark/Dask Feature Pipeline
│       ├── Dockerfile
│       ├── pyproject.toml
│       └── src/
│
├── docker-compose.yml            # For local multi-container development
└── README.md