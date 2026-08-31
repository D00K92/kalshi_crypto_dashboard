# Ingestion service

The ingestion service consumes Binance Spot `BTCUSDT`, Gemini Spot `BTCUSD`,
Crypto.com Spot `BTC_USD`, Bitstamp Spot `BTC/USD`, Coinbase Advanced Trade `BTC-USD`, Deribit spot
`BTC_USDT`, and Kraken Spot `BTC/USD` trades over WebSocket. Binance, Gemini,
Crypto.com, Bitstamp, Coinbase, Bybit, and Kraken provide top-15 order-book
snapshots. Normalized events are appended to Redis
Streams before a best-effort Redis Pub/Sub broadcast.

Hot-path JSON encoding and decoding uses `orjson`. On supported CPython Unix
platforms, the process runs on `uvloop`; other platforms fall back to the
standard asyncio event loop.

## Outputs

| Event | Redis Stream | Pub/Sub channel |
|---|---|---|
| Trade | `stream:ticks` | `pub:btc_ticks` |
| Top-15 book snapshot | `stream:orderbook_snapshots` | `pub:orderbook` |

Consumers must deduplicate Stream events using the stable `event_id` field.

## Run locally

Start Redis on `localhost:6379`, then run:

```bash
uv sync --group dev
uv run ingestion
```

Run tests with:

```bash
uv run pytest
```

To inspect live Coinbase Level 2 data locally, export the CDP credentials and run:

```bash
export COINBASE_API_KEY='organizations/.../apiKeys/...'
export COINBASE_SECRET="$(<coinbase_private_key.pem)"
uv run python scripts/test_coinbase_ws.py --seconds 30
```

The diagnostic prints connection status and top-15 snapshots only; it never prints
the API key, private key, or JWT. Use `--seconds 0` to run until interrupted.

## Configuration

| Variable | Default |
|---|---|
| `INGESTION_REDIS_URL` | unset; overrides `REDIS_HOST` and `REDIS_PORT` |
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `BINANCE_SYMBOL` | `btcusdt` |
| `BINANCE_WS_URL` | Binance combined trade/depth stream |
| `GEMINI_WS_URL` | `wss://ws.gemini.com` |
| `GEMINI_SYMBOL` | `btcusd` |
| `CRYPTO_COM_WS_URL` | `wss://stream.crypto.com/exchange/v1/market` |
| `CRYPTO_COM_SYMBOL` | `BTC_USD` |
| `BITSTAMP_WS_URL` | `wss://ws.bitstamp.net` |
| `BITSTAMP_SYMBOL` | `btcusd` |
| `COINBASE_WS_URL` | `wss://advanced-trade-ws.coinbase.com` |
| `COINBASE_PRODUCT_ID` | `BTC-USD` |
| `COINBASE_API_KEY` | unset; CDP API key name used for JWT authentication |
| `COINBASE_SECRET` | unset; CDP ES256 private-key PEM (preserve newlines) |
| `DERIBIT_WS_URL` | `wss://www.deribit.com/ws/api/v2` |
| `DERIBIT_INSTRUMENT` | `BTC_USDT` |
| `KRAKEN_WS_URL` | `wss://ws.kraken.com/v2` |
| `KRAKEN_SYMBOL` | `BTC/USD` |
| `INGESTION_QUEUE_MAXSIZE` | `10000` |
| `INGESTION_STREAM_MAXLEN` | `1000000` locally; production deployment uses `250000` |
| `INGESTION_SHUTDOWN_GRACE_SECONDS` | `10` |
| `INGESTION_LOG_LEVEL` | `INFO` |

This slice deliberately uses Binance's WebSocket-only partial-depth stream.
Full-depth reconstruction requires a REST snapshot followed by diff-depth
updates and is outside this slice.
