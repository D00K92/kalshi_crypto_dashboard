# Dashboard

Plotly Dash desktop terminal for the `market-aggregator` latest-state keys.

## Run locally

```bash
uv sync
REDIS_URL=redis://localhost:6379/0 uv run python -m dashboard
```

Open <http://localhost:8050>. The app reads these keys on each one-second
refresh: `market:book:BTCUSDT:latest`, `market:spot:BTCUSDT:latest`,
`market:candles:BTCUSDT:10s`, `market:cvd:BTCUSDT:10s`,
`stream:kalshi_tickers`, and `stream:kalshi_trades`.

The dashboard uses latest-state polling for recovery. Kalshi rows are limited
to fresh data from the most recent event and the monitor displays the ATM
window around the current synthetic spot price.

To smoke-test only the connector:

```bash
REDIS_URL=redis://localhost:6379/0 uv run python scripts/test_redis_connector.py
```

For GCP Memorystore, run the dashboard inside GKE or use a private-network
tunnel; its private IP is not directly reachable from a laptop. The Kubernetes
Service is `ClusterIP` by default, so external access must be provided through
an approved authenticated access layer.
