# Aggregator

Consumes normalized trade and top-15 order-book events from Redis Streams and
publishes reusable market state for the dashboard and downstream ML services.
The service maintains per-venue books, a consolidated 10-level order book with
spread/depth/imbalance metrics, a fresh-venue equal-weight synthetic trade
price, thirty-second OHLCV candles, and completed bars at 1m/5m/10m/15m/30m
and 1h frequencies. Bars are retained for two hours by default so
`live_feature_service` has enough context for 1-hour predictions. Aggregator
does not compute ML feature rows; `live_feature_service` consumes
`stream:bars:v1` and `stream:orderbook:v1`, computes features/EWMA state, and
publishes the Feast-compatible feature stream.

`stream:orderbook:v1` is event-driven rather than fixed-frequency: one record
is emitted for each accepted input order-book snapshot. Its cadence depends on
the venue feeds and ingestion rate. The `500ms` freshness setting controls
which venue books are included; it does not throttle publication.

Run locally with Redis available:

```bash
uv sync --group dev
uv run aggregator
```

Configuration is environment-based; see `src/aggregator/config.py`.
Set `AGGREGATOR_OUTPUT_PREFIX` for isolated staging runs; it defaults to
`market`. Set `AGGREGATION_HISTORY_MS` to increase the retained bar context
(the default is two hours). Stream names and retention are configurable with
`AGGREGATED_BARS_STREAM`, `AGGREGATED_ORDERBOOK_STREAM`, and their `*_MAXLEN`
settings.

Health endpoints listen on `HEALTH_PORT` (default `8080`): `/healthz` reports
the process and `/readyz` reports Redis/group readiness.

Inspect the latest state and watch live updates:

```bash
uv run python scripts/inspect_aggregator.py
uv run python scripts/inspect_aggregator.py --watch 30
```
