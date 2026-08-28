# Market aggregator

Consumes normalized trade and top-15 order-book events from Redis Streams and
publishes dashboard-ready state. The service maintains per-venue books, an
aggregated 10-level book, a five-second cross-venue trade VWAP, five-second
OHLCV candles, and CVD.

Run locally with Redis available:

```bash
uv sync --group dev
uv run market-aggregator
```

Configuration is environment-based; see `src/market_aggregator/config.py`.
Set `AGGREGATOR_OUTPUT_PREFIX` for isolated staging runs; it defaults to
`market`.

Health endpoints listen on `HEALTH_PORT` (default `8080`): `/healthz` reports
the process and `/readyz` reports Redis/group readiness.

Inspect the latest state and watch live updates:

```bash
uv run python scripts/inspect_aggregator.py
uv run python scripts/inspect_aggregator.py --watch 30
```
