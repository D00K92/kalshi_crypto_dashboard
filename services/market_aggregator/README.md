# Market aggregator

Consumes normalized trade and top-15 order-book events from Redis Streams and
publishes dashboard-ready state. The service maintains per-venue books, an
aggregated 10-level book, a fresh-venue equal-weight synthetic trade price,
ten-second OHLCV candles, and CVD. Each live synthetic-price update also emits a
Feast-compatible v1 feature row on `stream:features:v1` and
`pub:features:v1`; a Feast bridge can consume that stream and write the row to
the Feast Redis online store without waiting for BigQuery materialization.

Run locally with Redis available:

```bash
uv sync --group dev
uv run aggregator
```

Configuration is environment-based; see `src/market_aggregator/config.py`.
Set `AGGREGATOR_OUTPUT_PREFIX` for isolated staging runs; it defaults to
`market`. Live feature freshness defaults to 60 seconds and can be configured
with `FEATURE_TRADE_FRESHNESS_MS`.

Health endpoints listen on `HEALTH_PORT` (default `8080`): `/healthz` reports
the process and `/readyz` reports Redis/group readiness.

Inspect the latest state and watch live updates:

```bash
uv run python scripts/inspect_aggregator.py
uv run python scripts/inspect_aggregator.py --watch 30
```
