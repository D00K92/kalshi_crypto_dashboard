# Live feature service

Consumes per-venue completed 10-second primitives from `stream:primitives:v1`,
retains a 450-bar rolling history per venue, and publishes a versioned row to `stream:features:v1`
plus `market:features:BTCUSD:latest`.

The rolling state is checkpointed in Redis. On startup the service replays the
recent primitive stream before consuming new entries, so a restart does not
lose the one-hour feature warm-up window. The primitive stream is retained to
approximately 100,000 entries (about 48 hours for six venues).

The v1 parity formula is:

```text
synthetic_price = mean(per-venue p_trade_mean)
log_return = ln(synthetic_price / previous_synthetic_price)
venue_count = count(non-null per-venue prices)
```

The `ewma_state` field is an inference-only extension and is ignored by Feast's
registered v1 fields. Feast's existing live-push bridge consumes the feature
stream asynchronously.
