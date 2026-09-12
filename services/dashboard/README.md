# Dashboard service

Read-only Plotly Dash terminal for current BTC market state and Kalshi model
pricing. It polls Redis and joins analytics-owned pricing keys onto normalized
Kalshi contract rows; it does not calculate forecasts or alter producer
schemas.

## Inputs and display

| Redis interface | Display use |
|---|---|
| `market:spot:BTCUSDT:latest` | Live synthetic BTC price |
| `market:candles:BTCUSDT:30s` | Canonical candle and volume charts |
| `stream:kalshi_tickers` | Current KXBTCD quotes |
| `stream:kalshi_trades` | Recent contract trades |
| `stream:kalshi_orderbook` | Pre-trade contract activity age |
| `market:pricing:v1:<market_ticker>` | Model value, probability, and bid/mid/ask edges |
| `market:implied_volatility:v1:BTCUSD:latest` | Dotted Kalshi-IV line in the volatility cone |

The browser refreshes spot every 250 ms, candles every 500 ms, the Kalshi
monitor every second, the contract table every two seconds, and the volatility
cone every ten seconds. A display-only forming candle moves between canonical
30-second publications; it is never written back to Redis.

Only fresh pricing records are attached. Missing or older-than-60-second
analytics values render as `-`. The contract panel selects the active event
and an at-the-money window around current synthetic spot.

Before a contract trades, `AGE` follows ticker or orderbook activity. After a
trade, it follows the first timestamp of the latest distinct trade
price/quantity/side fingerprint, so repeated snapshots do not reset it.

The data adapter also supports the latest canonical per-venue book key
`market:book:BTCUSDT:latest` for diagnostics/legacy views, but the current UI
does not render an order-book panel.

## Run and test locally

```bash
uv sync --locked
REDIS_URL=redis://localhost:6379/0 uv run --locked python -m dashboard
uv run --locked pytest
```

Open <http://localhost:8050>. To test only Redis connectivity:

```bash
REDIS_URL=redis://localhost:6379/0 \
  uv run --locked python scripts/test_redis_connector.py
```

## Configuration

| Variable | Default |
|---|---|
| `REDIS_URL` | Optional full Redis URL |
| `REDIS_HOST`, `REDIS_PORT` | `localhost`, `6379` |
| `AGGREGATOR_OUTPUT_PREFIX` | `market` |
| `DASHBOARD_INSTRUMENT` | `BTCUSDT` |
| `PORT` | `8050` |

## Deployment and health

Kubernetes runs `deployment/dashboard` and
`service/dashboard:8050` from `k8s/dashboard-deployment.yaml`.
`k8s/dashboard-public-ingress.yaml` exposes the UI at
<https://crypto-dashboard.kairos-trading.com> through the configured GCE
Ingress, static IP, managed certificate, and backend policy.

`GET /healthz` reports process liveness. `GET /readyz` pings Redis and
returns 503 when the data store is unavailable.
