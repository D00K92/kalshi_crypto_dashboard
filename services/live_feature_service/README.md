# Live feature service

Owns the low-latency `market_features/v4_10s` calculation. It consumes the
aggregator's completed per-venue primitives, maintains rolling state, and
publishes an inference-ready feature envelope without requiring changes to
upstream event schemas.

Window sizes and version capabilities come from the repository-level
`feature_contracts/market_features.json` manifest through a generated module.
Feature formulas remain explicit in `computation.py` for parity review.

## Data contract

Input:

- `stream:primitives:v1`
- consumer group `live-features-v4-10s`

Outputs:

- `stream:features:v4_10s`
- `market:features:v4_10s:BTCUSD:latest`
- restart checkpoint `market:features:BTCUSD:state:v4_10s`

The feature formula is:

```text
synthetic_price = mean(non-null per-venue p_trade_mean)
log_return      = ln(synthetic_price / previous_synthetic_price)
venue_count     = count(non-null per-venue prices)
```

The payload also includes source timestamps, event/availability timestamps,
HAR realized-volatility windows, buyer-volume windows, and sampled-candle EWMA
state used by packaged model fallbacks.

The optional `v3_10s` contract adds annualized trailing realized volatility at
30s, 1m, 5m, 15m, 30m, 1h, and 3h. It retains 1,100 bars and publishes only after
the complete three-hour lookback is warm. Select it with `FEATURE_VERSION=v3_10s`;
versioned Redis keys and defaults are derived automatically.

The active `v4_10s` contract retains those HAR inputs and adds causal
`log1p(sum(v_buy))` windows over 30s, 5m, and 10m. Production runs v4 as a
single consumer with its own stream, latest key, checkpoint, and consumer
group. The implementation can still reproduce v2 explicitly for offline tests,
but no v2 producer is deployed.

State, an optional feature publication, and the source ACK commit in one Redis
transaction. First startup replays recent primitives; restarts restore the
checkpoint and reclaim abandoned pending entries. Until enough history exists,
the service warms up rather than inventing values.

## Run and test locally

```bash
uv sync
REDIS_URL=redis://localhost:6379/0 uv run python -m live_feature_service
uv run pytest
```

## Configuration

| Variable | Default |
|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379/0` |
| `BARS_STREAM` | `stream:primitives:v1` |
| `FEATURE_STREAM` | `stream:features:v2_10s` |
| `FEATURE_KEY` | `market:features:v2_10s:BTCUSD:latest` |
| `FEATURE_STATE_KEY` | `market:features:BTCUSD:state:v2_10s` |
| `LIVE_FEATURE_GROUP` | `live-features-v2-10s` |
| `LIVE_FEATURE_PENDING_IDLE_MS` | `60000` |
| `EWMA_DECAY` | `0.96` |
| `MAX_BAR_AGE_MS` | `120000` |
| `HISTORY_BARS` | `450` |
| `REPLAY_COUNT` | `5000` |
| `PRIMITIVE_STREAM_MAXLEN` | `100000` |
| `HEALTH_PORT` | `8080` |

## Consumers and deployment

Analytics reads the latest feature key, and the v4 parity job compares recent
immutable stream history directly with BigQuery.

Kubernetes runs only `deployment/live-feature-service-v4`.
`GET /healthz` is liveness and
`GET /readyz` becomes ready after Redis setup and state restoration/replay.
The probe endpoint is pod-local.
