# Live feature service

Consumes completed 1-minute bars from `stream:bars:v1`, computes the exact
offline v1 contract, and publishes a versioned row to `stream:features:v1`
plus `market:features:BTCUSD:latest`.

The v1 parity formula is:

```text
synthetic_price = mean(per-venue p_trade_mean)
log_return = ln(synthetic_price / previous_synthetic_price)
venue_count = count(non-null per-venue prices)
```

The `ewma_state` field is an inference-only extension and is ignored by Feast's
registered v1 fields. Feast's existing live-push bridge consumes the feature
stream asynchronously.
