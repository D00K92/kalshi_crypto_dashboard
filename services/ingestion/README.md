# Ingestion service

The ingestion service consumes Binance Spot `BTCUSDT`, Coinbase Advanced Trade
`BTC-USD`, and Deribit spot `BTC_USDT` trades over WebSocket. Binance, Coinbase,
and Bybit provide top-15 order-book snapshots. Normalized events are appended to Redis
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
| `COINBASE_WS_URL` | `wss://advanced-trade-ws.coinbase.com` |
| `COINBASE_PRODUCT_ID` | `BTC-USD` |
| `COINBASE_API_KEY` | unset; CDP API key name used for JWT authentication |
| `COINBASE_SECRET` | unset; CDP ES256 private-key PEM (preserve newlines) |
| `DERIBIT_WS_URL` | `wss://www.deribit.com/ws/api/v2` |
| `DERIBIT_INSTRUMENT` | `BTC_USDT` |
| `INGESTION_QUEUE_MAXSIZE` | `10000` |
| `INGESTION_STREAM_MAXLEN` | `1000000` |
| `INGESTION_SHUTDOWN_GRACE_SECONDS` | `10` |
| `INGESTION_LOG_LEVEL` | `INFO` |

This slice deliberately uses Binance's WebSocket-only partial-depth stream.
Full-depth reconstruction requires a REST snapshot followed by diff-depth
updates and is outside this slice.
