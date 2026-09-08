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

The canonical per-venue primitive dataset is published on
`stream:primitives:v1`. Each `primitive_bar` row uses the offline field names,
one venue/frequency/time bucket, zero-fills flow for empty buckets, forward
fills the last trade state, and carries the latest order-book levels available
at the bucket boundary. Configure the stream with `PRIMITIVE_STREAM` and
`PRIMITIVE_STREAM_MAXLEN`.

`stream:orderbook:v1` is an event-driven primitive stream: one per-venue
`market_book` record is emitted for each accepted input order-book snapshot.
Its cadence depends on the venue feeds and ingestion rate. The `500ms`
freshness setting is retained for internal state quality and does not throttle
publication. The old cross-venue `aggregated_orderbook` publication is no
longer emitted.

Completed records on `stream:bars:v1` retain the existing bar fields and also
expose additive canonical trade primitives used by the offline resampler
(`primitive_schema_version=2`): `p_open`, `p_high`, `p_low`, `p_trade`,
`p_close`, `p_trade_mean`, `v_trade`, `v_buy`, `v_sell`, `cnt_trade`, and
fill-time statistics. Order-book primitives remain on `stream:orderbook:v1`;
time-bucketed per-venue book columns are the next compatibility-safe
expansion.

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

Primitive buckets close on an event-time watermark. Configure the tolerated
delay with `AGGREGATOR_ALLOWED_LATENESS_MS` (default `5000`); trades older than
the finalized watermark are acknowledged and dropped. Unacknowledged Redis
consumer entries are reclaimed after `AGGREGATOR_PENDING_IDLE_MS` (default
`60000`). Keep both values explicit in production manifests.

Health endpoints listen on `HEALTH_PORT` (default `8080`): `/healthz` reports
the process and `/readyz` reports Redis/group readiness.

Inspect the latest state and watch live updates:

```bash
uv run python scripts/inspect_aggregator.py
uv run python scripts/inspect_aggregator.py --watch 30
```
