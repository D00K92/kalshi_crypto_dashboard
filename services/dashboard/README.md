# Dashboard

Plotly Dash desktop terminal for the `market-aggregator` latest-state keys.

## Run locally

```bash
uv sync
REDIS_URL=redis://localhost:6379/0 uv run python -m dashboard
```

Open <http://localhost:8050>. The app reads these keys on each one-second
refresh: `market:book:BTCUSDT:latest`, `market:spot:BTCUSDT:latest`,
`market:candles:BTCUSDT:5s`, and `market:cvd:BTCUSDT:5s`.

The current slice intentionally uses latest-state polling for recovery and
keeps Kalshi panels as placeholders. Pub/Sub delivery and Kalshi data are
subsequent phases.

To smoke-test only the connector:

```bash
REDIS_URL=redis://localhost:6379/0 uv run python scripts/test_redis_connector.py
```

For GCP Memorystore, use a private-network tunnel or run the smoke test from
inside GKE; its private IP is not directly reachable from a laptop.
