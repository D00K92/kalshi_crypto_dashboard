# Aggregator service

Consumes normalized crypto trades and book snapshots and publishes reusable
market primitives. It fits the existing ingestion schema and Redis topology; it
does not connect to exchanges or compute model-specific features.

## Inputs

| Stream | Consumer group |
|---|---|
| `stream:ticks` | Production: `aggregator_trades_v5` |
| `stream:orderbook_snapshots` | Production: `aggregator_books_v5` |

The manifest pins these v5 group names as persistent delivery identities.
The code-level `market_aggregator_*` defaults remain for compatibility only;
changing a deployed group creates a new group and can replay retained entries.

## Outputs

| Redis interface | Meaning |
|---|---|
| `market:book:BTCUSDT:latest` | Most recently processed venue's canonical depth-limited book; last writer wins |
| `stream:orderbook:v1` | Bounded stream of canonical per-venue book snapshots |
| `market:spot:BTCUSDT:latest` | Equal-weight mean of fresh per-venue trade prices |
| `market:aggregated_spot` | Best-effort spot Pub/Sub notification |
| `stream:primitives:v1` | Completed per-venue 10-second primitive bars |
| `market:primitive:<venue>:10s:latest` | Latest primitive for diagnostics |
| `market:candles:BTCUSDT:30s` | Dashboard-facing 30-second candle snapshot |
| `market:aggregated_candles` | Best-effort candle Pub/Sub notification |
| `market:candle_state:BTCUSDT:10s` | Atomic restart checkpoint |
| `market:primitive_watermark:BTCUSDT:10s` | Event-time finalization watermark |

The aggregator does not publish `market_features/v2_10s`. That contract and
its EWMA state belong to `live_feature_service`.

Trade buckets close using event time and a configurable allowed-lateness
watermark. State/output writes and source ACKs share one Redis transaction so a
restart cannot acknowledge unpersisted work.

## Run and test locally

```bash
uv sync --locked
AGGREGATOR_REDIS_URL=redis://localhost:6379/0 uv run --locked aggregator
uv run --locked pytest
```

The integration harness can exercise isolated stream names against Redis:

```bash
uv run --locked python scripts/integration_test.py
```

## Configuration

| Variable | Default / purpose |
|---|---|
| `AGGREGATOR_REDIS_URL` | Full Redis URL; legacy `MARKET_AGGREGATOR_REDIS_URL` remains a fallback |
| `REDIS_HOST`, `REDIS_PORT` | `localhost`, `6379` |
| `BOOK_STREAM`, `TRADE_STREAM` | `stream:orderbook_snapshots`, `stream:ticks` |
| `BOOK_CONSUMER_GROUP`, `TRADE_CONSUMER_GROUP` | Persistent group IDs above |
| `AGGREGATOR_GROUP_START_ID` | `0` |
| `AGGREGATOR_OUTPUT_PREFIX` | `market` |
| `PRIMITIVE_STREAM` | `stream:primitives:v1` |
| `AGGREGATED_ORDERBOOK_STREAM` | `stream:orderbook:v1` |
| `AGGREGATION_VENUES` | `binance,bitstamp,crypto.com,gemini,coinbase,kraken` |
| `AGGREGATION_TAKER_FEES` | Comma-separated `venue=rate` map |
| `AGGREGATION_PRICE_TICK` | `auto`; in-memory consolidated-book tick override; production uses `0.01` |
| `AGGREGATION_BOOK_DEPTH` | `10` |
| `AGGREGATION_FRESHNESS_MS` | `500` for book inputs |
| `FEATURE_TRADE_FRESHNESS_MS` | `60000` for trade state |
| `AGGREGATOR_ALLOWED_LATENESS_MS` | `5000` |
| `AGGREGATOR_REPLAY_MAX_AGE_MS` | `5000` |
| `AGGREGATOR_PENDING_IDLE_MS` | `60000` |
| `AGGREGATION_HISTORY_MS` | Two hours |
| `HEALTH_PORT` | `8080` |

`AGGREGATED_BARS_STREAM` remains in configuration for rollout compatibility
but the current service contract publishes primitives on
`stream:primitives:v1` and dashboard candles as a latest-state key.

## Deployment and health

Kubernetes runs `deployment/aggregator` from
`k8s/aggregator-deployment.yaml`. `GET /healthz` is liveness;
`GET /readyz` becomes ready after Redis, checkpoint restoration, and consumer
group setup. The probe server is pod-local and has no Kubernetes Service.
