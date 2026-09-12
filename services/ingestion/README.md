# Ingestion service

Connects to live crypto venues and Kalshi, normalizes external messages, and
publishes versioned events to Redis. It owns transport and schema adaptation
only; downstream services own aggregation, storage, features, and pricing.

## Active inputs

| Source | Data |
|---|---|
| Binance Spot | BTCUSDT trades and partial-depth snapshots |
| Gemini Spot | BTCUSD trades and books |
| Crypto.com Exchange | BTC_USD trades and books |
| Bitstamp | BTCUSD trades and books |
| Coinbase Advanced Trade | BTC-USD trades and level-2 snapshots |
| Deribit | BTC_USDT spot trades |
| Kraken v2 | BTC/USD trades and books |
| Kalshi | Authenticated KXBTCD discovery plus ticker, trade, and book updates |

Kalshi starts only when both credentials are present. The repository includes a
Bybit adapter and settings, but the service does not instantiate it; Bybit is
not a production input.

## Redis API

Every Stream entry carries the normalized JSON document in its `payload`
field. Stable `event_id` values let consumers deduplicate at-least-once
delivery.

| Event | Stream | Best-effort Pub/Sub |
|---|---|---|
| Crypto trade | `stream:ticks` | `pub:btc_ticks` |
| Crypto book snapshot | `stream:orderbook_snapshots` | `pub:orderbook` |
| Kalshi ticker | `stream:kalshi_tickers` | `pub:kalshi_tickers` |
| Kalshi trade | `stream:kalshi_trades` | `pub:kalshi_trades` |
| Kalshi book update | `stream:kalshi_orderbook` | `pub:kalshi_orderbook` |

Redis Streams are the durable interface. Pub/Sub is only a notification path
and is not required by the current consumers.

## Run and test locally

Start Redis on `localhost:6379`, then:

```bash
uv sync --locked
uv run --locked ingestion
uv run --locked pytest
```

A local `.env` is loaded for convenience and must not be committed. Kubernetes
injects settings directly.

## Configuration

| Variable | Default / purpose |
|---|---|
| `INGESTION_REDIS_URL` | Optional full Redis URL; otherwise `REDIS_HOST:REDIS_PORT` |
| `REDIS_HOST`, `REDIS_PORT` | `localhost`, `6379` |
| `BINANCE_SYMBOL` | `btcusdt`; builds the default combined trade/depth URL |
| `*_WS_URL`, `*_SYMBOL` | Per-venue URL and instrument overrides |
| `COINBASE_PRODUCT_ID` | `BTC-USD` |
| `COINBASE_API_KEY`, `COINBASE_SECRET` | Optional Coinbase JWT credentials |
| `DERIBIT_INSTRUMENT` | `BTC_USDT` |
| `KALSHI_REST_URL`, `KALSHI_WS_URL` | Kalshi API endpoints |
| `KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY` | Kalshi credentials; both required to enable the feed |
| `KALSHI_RSA_PATH` | Local alternative path to a Kalshi private-key file |
| `KALSHI_SERIES_TICKER` | `KXBTCD` |
| `INGESTION_QUEUE_MAXSIZE` | `10000` |
| `INGESTION_STREAM_MAXLEN` | `1000000`; production manifest uses `250000` |
| `INGESTION_SHUTDOWN_GRACE_SECONDS` | `10` |
| `INGESTION_LOG_LEVEL` | `INFO` |

## Deployment and health

The Docker image runs the `ingestion` entrypoint. Kubernetes deploys it as
`deployment/ingestion-service` from `k8s/ingestion-deployment.yaml`.
Coinbase and Kalshi credentials come from Kubernetes Secrets.

The service exposes no HTTP API. Kubernetes process state and logs are the
health signal; any feed or publisher task exiting unexpectedly terminates the
process so the pod can restart. Shutdown drains the in-memory publication queue
within the configured grace period.
