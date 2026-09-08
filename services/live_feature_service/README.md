# Live feature service

Consumes per-venue completed 10-second primitives from `stream:primitives:v1`,
retains a 450-bar rolling history per venue, and publishes the `market_features/v2_10s`
contract to `stream:features:v2_10s` plus `market:features:v2_10s:BTCUSD:latest`.

The rolling state, including the in-flight bucket, is checkpointed in Redis in
the same transaction as feature publication and consumer acknowledgement. On
first startup the service replays the recent primitive stream to warm its
history; restarts restore the checkpoint and reclaim unacknowledged entries.
Configure the reclaim threshold with `LIVE_FEATURE_PENDING_IDLE_MS` (default
`60000`). The primitive stream is retained to approximately 100,000 entries
(about 48 hours for six venues).

The v2_10s checkpoint uses `market:features:BTCUSD:state:v2_10s` and consumer
group `live-features-v2-10s` by default. The former v1 key and group are left
intact so an emergency rollback can restore the previous deployment.

The v2_10s parity formula is:

```text
synthetic_price = mean(per-venue p_trade_mean)
log_return = ln(synthetic_price / previous_synthetic_price)
venue_count = count(non-null per-venue prices)
```

The `ewma_state` field is an inference-only extension and is ignored by Feast's
registered v2_10s fields. Feast's existing live-push bridge consumes the feature
stream asynchronously.
