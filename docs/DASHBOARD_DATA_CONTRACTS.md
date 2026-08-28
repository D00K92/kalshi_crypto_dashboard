# Dashboard and Market Aggregator Data Contracts

Version: 1

The market aggregator consumes normalized ingestion events and publishes
dashboard-ready state. JSON numeric values are encoded as strings where price
or quantity precision matters.

## Redis inputs

| Event | Stream | Pub/Sub |
|---|---|---|
| Trade | `stream:ticks` | `pub:btc_ticks` |
| Book snapshot | `stream:orderbook_snapshots` | `pub:orderbook` |

The aggregator must deduplicate by `event_id` and treat `received_ts_ms` as the
freshness clock. Existing ingestion payloads use `schema_version: 1`.

## Aggregated order book

Latest-state key:

```text
market:book:BTCUSDT:latest
```

Live channel:

```text
pub:aggregated_orderbook
```

Payload shape:

```json
{
  "schema_version": 1,
  "event_type": "aggregated_book",
  "instrument": "BTCUSDT",
  "generated_ts_ms": 0,
  "depth": 10,
  "price_tick": "1.00",
  "bucket_method": "bid_floor_ask_ceiling",
  "venues": ["binance", "coinbase", "bybit"],
  "stale_venues": [],
  "bids": [
    {
      "price": "65000.00",
      "total_quantity": "12.4",
      "venues": {"binance": "5.1", "coinbase": "4.8", "bybit": "2.5"}
    }
  ],
  "asks": []
}
```

There are at most 10 levels per side. Levels are grouped into fixed price
buckets using side-aware quantization: bids are rounded down to the bucket
boundary and asks are rounded up. Any bid bucket at or above the lowest ask
bucket is removed, guaranteeing a non-crossed ladder despite small venue price
discrepancies. The configured bucket size is reported in `price_tick` (the
production default is `$1.00`). Venue quantity is preserved so the dashboard
can render stacked absolute-volume bars.

## Synthetic spot price

Latest-state key:

```text
market:spot:BTCUSDT:latest
```

Live channel:

```text
pub:aggregated_spot
```

The first implementation uses a cross-venue trade-volume-weighted average over
the active five-second bucket. For every fresh trade, the aggregator adds
`price * quantity` to the numerator and `quantity` to the denominator:

```text
synthetic_price = Σ(price × quantity) / Σ(quantity)
```

This is equivalent to weighting each venue's five-second VWAP by that venue's
five-second traded quantity. USD and USDT are treated as equivalent by explicit
configuration. The aggregator must retain the per-venue volume and VWAP so the
dashboard can explain the result and expose venue dominance.

```json
{
  "schema_version": 1,
  "event_type": "aggregated_spot",
  "instrument": "BTCUSDT",
  "price": "65000.25",
  "method": "five_second_trade_vwap",
  "generated_ts_ms": 0,
  "bucket_start_ts_ms": 0,
  "bucket_end_ts_ms": 0,
  "total_volume": "25.0",
  "venues": {
    "binance": {"vwap": "65000.10", "volume": "10.0", "last_received_ts_ms": 0},
    "coinbase": {"vwap": "65000.40", "volume": "8.0", "last_received_ts_ms": 0},
    "bybit": {"vwap": "65000.25", "volume": "7.0", "last_received_ts_ms": 0}
  },
  "used_venues": ["binance", "coinbase", "bybit"],
  "stale_venues": []
}
```

The spot payload represents the current five-second bucket and is updated as
trades arrive. If a venue has no trades in the bucket, it contributes zero
volume. If a venue connection is stale beyond the configured freshness limit,
it is reported in `stale_venues` and excluded from the active calculation.
When the bucket has no trades, the aggregator should publish a null price
instead of manufacturing a value.

## Five-second candles and CVD

Latest-state keys:

```text
market:candles:BTCUSDT:5s
market:cvd:BTCUSDT:5s
```

Live channels:

```text
pub:aggregated_candles
pub:aggregated_cvd
```

Each candle contains `bucket_start_ts_ms`, `open`, `high`, `low`, `close`, and
`volume`. Candle OHLC values are built from normalized trades across all venues;
the synthetic spot line uses the bucket's volume-weighted price. CVD uses taker
direction from normalized trades and contains the bucket's delta plus the
cumulative value needed by the chart.

## Recovery and freshness

The dashboard first reads latest-state keys, then subscribes to live channels.
Pub/Sub is an update transport, not the source of truth. Every published
payload includes `generated_ts_ms`; the UI must visibly indicate stale or
missing venue data.
