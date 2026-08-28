# Dashboard v2 Development Plan

The dashboard keeps the v1 visual organization while adding a live, multi-venue
market-data view. It is desktop-only.

## Layout

The top row uses a `2.5 : 1 : 3.5` width ratio:

1. BTCUSDT market panel: five-second OHLC candles with CVD below.
2. Aggregated order book: 10 ask levels above the spread and 10 bid levels
   below it. Volumes are absolute and stacked by venue (Binance, Coinbase,
   and Bybit).
3. Kalshi monitor: summary of the active KXBTCD chain.

The bottom section contains the detailed KXBTCD contract table. Kalshi chart
areas remain present in the layout but are intentionally empty placeholders for
now.

## Delivery phases

1. Define shared market-data contracts for the aggregator and dashboard.
2. Build the `market-aggregator` service for books, synthetic spot, candles,
   and CVD.
3. Add unit tests for aggregation, freshness, price calculation, candle
   boundaries, CVD, and malformed events.
4. Create the `services/dashboard` Dash service and Redis reader.
5. Implement the v2 layout and real-time market panels.
6. Add dashboard WebSocket/SSE delivery with latest-state recovery.
7. Add Kubernetes deployment, health checks, observability, CI, and CD.
8. Validate the complete venue-to-browser data path end to end.
