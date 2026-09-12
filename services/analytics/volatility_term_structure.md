# Analytics volatility and Kalshi pricing design

## Goal and compatibility rule

`services/analytics` is a consumer and publisher around the current
architecture. Existing ingestion, aggregator, live-feature, Feast, ML, GCS
exporter, model-serving, and dashboard contracts remain independent.

Analytics adapts the data that those services already expose. It must not
require another service to add a field, rename a Redis key, publish a new
message, or adopt the analytics output schema. A future upstream improvement
may replace an analytics adapter, but it is not a prerequisite for this plan.

The first pricing model covers active `KXBTCD` above-strike markets with a
positive remaining lifetime of at most 60 minutes:

```text
Pay $1 when the Kalshi settlement value is strictly greater than strike K.
```

The result is a model probability, not an executable order or a claim that the
model is calibrated to the market.

## Current service connections

```text
crypto venues -> ingestion -> Redis -> aggregator
                                         |
                                         +-> spot latest state
                                         +-> 10s primitives -> live_feature_service
                                                                  |
                                                                  +-> v2_10s latest features
                                                                  +-> feature stream -> Feast bridge

Kalshi WebSocket -> ingestion -> stream:kalshi_tickers -----------+
Kalshi REST -------------------------------------------------------+-> analytics
live spot + features ---------------------------------------------+
analytics -> model-serving /v1/forecast -> four forecasts --------+
                                                                  |
                                                                  v
                              volatility + pricing keys -> Redis -> dashboard
```

Analytics uses the same private Redis instance and Kubernetes network as the
existing services. Each dependency has a narrow purpose:

| Dependency | Existing interface used by analytics | Purpose | Required upstream change |
|---|---|---|---|
| Aggregator | `GET market:spot:BTCUSDT:latest` | Synthetic spot and spot timestamp | None |
| Live feature service | `GET market:features:v2_10s:BTCUSD:latest` | One timestamped v2_10s model-feature observation and EWMA state | None |
| Ingestion | `stream:kalshi_tickers` | Bid, ask, ticker freshness, event and market identifiers | None |
| Kalshi REST | Existing authenticated event/market endpoints | Authoritative market definition, strike, settlement/expiry time, and status | None |
| Model serving | `POST http://model-serving:8080/v1/forecast` | Complete 5m/15m/30m/1h term structure | None |
| Dashboard | Existing Redis connection and analytics-owned pricing keys | Display model value and edge | None |

The v2_10s analytics path reads the versioned timestamped latest-feature
envelope directly and forwards it to model-serving. This guarantees that all
four forecasts use exactly one observation and lets both services validate its
age. The existing Feast live bridge continues to reconcile the online store.
The `feast-server:6566` compatibility Service is retained with its Deployment
at zero replicas; analytics does not put Feast HTTP serving on its critical
path. A future `ForecastProvider` may use Feast without changing the pricing
core or any producer.

Analytics uses its own Redis consumer group, `analytics-pricing-v1`, for
`stream:kalshi_tickers`; it never acknowledges entries for another consumer.
On startup it bootstraps recent ticker state with `XREVRANGE`, refreshes market
metadata from Kalshi REST, and then consumes new entries with its own group.
This matches the dashboard's ability to reconstruct recent state without
changing ingestion.

## Ownership boundaries

### Existing services continue to own

- Ingestion owns exchange and Kalshi WebSocket connections and normalized raw
  Redis streams.
- Aggregator owns the synthetic spot and reusable completed primitives.
- Live feature service owns the v2_10s feature envelope and EWMA state.
- Feast store owns feature definitions, the live bridge, online storage, and
  the currently suspended optional HTTP feature-serving boundary.
- ML pipeline owns training, evaluation, promotion, and registration of one
  model per supported horizon.
- Dashboard owns presentation. It is not required for analytics readiness.

### Analytics owns

- Reading and freshness-checking the existing inputs.
- Fetching Kalshi market metadata because the existing ticker stream does not
  contain expiry and settlement fields.
- Adapting the feature envelope to the forecast API and validating the complete
  response. The retained direct provider owns its own rollback-only artifact
  loading.
- Combining the four forecasts into one atomic internal term structure.
- Interpolating annualized volatility, fitting Kalshi midpoint IV, computing
  model probabilities, and publishing the analytics-owned Redis outputs.
- Reporting unavailable states without emitting a tradable-looking stale
  value.

Analytics must not import another service's private Python package at runtime.
Redis JSON, Kalshi REST, and the model-serving HTTP API are its production
service boundaries.

## Live inference adapter

The model-serving service owns inference and publishes no Redis data itself.
Analytics retains a `ForecastProvider` boundary so HTTP can be rolled back to
the direct Vertex/GCS provider during parity testing.

The production provider calls the private model-serving API for exactly four
horizons:

```text
5m, 15m, 30m, 1h
```

Analytics reads one `market:features:v2_10s:BTCUSD:latest` envelope and forwards its
feature contract, timestamp, and values to `/v1/forecast`. It rejects timeouts,
malformed responses, version mismatches, incomplete horizons, invalid values,
or timestamp mismatches. Set `FORECAST_PROVIDER=direct` only for rollback or
parity testing with the legacy Vertex/GCS artifact path.

All four results must use the same latest-feature observation and complete
successfully. Partial results are not published. The assembled snapshot is
written by analytics, not by the ML pipeline:

```text
market:volatility:v2_10s:BTCUSD:latest
```

Example:

```json
{
  "schema_version": 1,
  "event_type": "volatility_term_structure",
  "asset": "BTCUSD",
  "model_version": "v2_10s",
  "model_resources": {
    "5m": "ewma/v2_10s/5m",
    "15m": "ewma/v2_10s/15m",
    "30m": "ewma/v2_10s/30m",
    "1h": "projects/.../models/...@1"
  },
  "feature_asof_ts_ms": 1788593790000,
  "generated_ts_ms": 1788593790116,
  "annualized_volatility": {
    "5m": 0.34,
    "15m": 0.37,
    "30m": 0.41,
    "1h": 0.45
  }
}
```

`BTCUSD` is the live entity emitted by the live feature service. Analytics
does not ask either producer to rename it, and `asset` is not passed as a
numeric model feature.

A deterministic fixture provider is used in tests. Production model-serving
explicitly identifies EWMA resources for 5m/15m/30m and the immutable XGBoost
resource for 1h; it never silently substitutes one provider after a failure.

## Forecast validation and freshness

Every volatility must be finite and strictly positive. Analytics rejects the
whole snapshot if a horizon is missing, a feature is missing, a model resource
does not match its configured horizon/version, or inference fails.

The following are initial configurable limits:

```text
spot maximum age             = 5 seconds
Kalshi ticker maximum age    = 60 seconds
live feature maximum age     = 90 seconds
volatility snapshot max age  = 60 seconds
Kalshi metadata refresh      = 15 seconds while an event is active
allowed future clock skew    = 2 seconds
```

Freshness is calculated from payload timestamps, not Redis key existence. The
aggregator spot key currently has no TTL, so `generated_ts_ms` must always be
validated. The volatility key has a 60-second TTL as cleanup in addition to
the timestamp check.

## Market metadata and lifetime

The ticker stream supplies `series_ticker`, `event_ticker`, and
`market_ticker`, but not expiry. Analytics queries Kalshi REST for the active
event and markets and caches the response for at most 15 seconds.

The market response is authoritative for:

- market status and supported above-strike contract type;
- the numeric strike;
- the settlement source/rules retained for audit;
- the timestamp at which the settlement observation is determined.

Use the contract's settlement/determination timestamp for `expiry_ts_ms`.
`close_time` is only acceptable when the market definition establishes that it
is the same instant. If the correct timestamp or strike cannot be established,
pricing is unavailable. Parsing `-T<strike>` from `market_ticker` is a checked
fallback: it must agree with REST metadata when both exist.

Define:

```text
tau_seconds = (expiry_ts_ms - pricing_asof_ts_ms) / 1000
```

Only `0 < tau_seconds <= 3600` is supported. Expired markets and markets beyond
one hour are removed from the active pricing set.

## Annualization and interpolation

The ML targets are annualized log-return volatility. Analytics never
annualizes a model output again. It converts remaining time once using the same
365-day convention as training:

```text
seconds_per_year = 365 * 24 * 60 * 60
T = tau_seconds / seconds_per_year
```

Interpolation is deterministic and linear in annualized volatility. Let `h0`
and `h1` be adjacent supported horizons in seconds and let their annualized
forecasts be `sigma0` and `sigma1`:

```text
w = (tau_seconds - h0) / (h1 - h0)
sigma_tau = (1 - w) * sigma0 + w * sigma1
```

Selection is:

| Remaining lifetime | Rule |
|---|---|
| `0 < tau < 5m` | Flat short-end volatility: `sigma_tau = sigma_5m` |
| `5m <= tau < 15m` | Interpolate `sigma_5m` and `sigma_15m` |
| `15m <= tau < 30m` | Interpolate `sigma_15m` and `sigma_30m` |
| `30m <= tau < 60m` | Interpolate `sigma_30m` and `sigma_1h` |
| `tau = 60m` | Use `sigma_1h` |

Descending and ascending volatility curves are both interpolated as supplied;
analytics does not repair or flatten model predictions between knots.

The output enum is:

```text
interpolation_method = "linear_annualized_volatility_v1"
```

## Initial probability model

The baseline model is explicit. It assumes the future log return over `T` is
normal with zero expected log return:

```text
log(S_T / S0) ~ Normal(0, sigma_tau^2 * T)
z = log(S0 / K) / (sigma_tau * sqrt(T))
model_probability = Phi(z)
```

This is equivalent to `P(S_T > K)` under the stated statistical assumption.
It is not described as Black-Scholes fair value, does not apply risk-neutral
drift, and does not discount the $1 settlement payoff. The output records:

```text
distribution_model = "zero_log_return_gaussian_v1"
```

Skewness, kurtosis, and Gram-Charlier terms are outside v1. They must not
appear as zero-valued inputs because they are not used. A later distribution
model requires a new version and calibration tests rather than modifying this
formula in place.

## Kalshi midpoint implied volatility

Analytics also publishes one market-implied annualized volatility for the
active hourly KXBTCD event. It filters to valid two-sided mids, orders contracts
by log-moneyness distance from spot, and uses the nearest seven; at least five
valid contracts are required. Each selected contract receives normalized
weight:

```text
weight_i = (open_interest_i + 1) / sum(open_interest_j + 1)
```

A bounded grid search followed by golden-section refinement minimizes a robust
Huber loss between observed mids and zero-rate Black-digital probabilities.
Only the midpoint IV is published; separate bid and ask IVs are not computed.
When calibration is unavailable, analytics deletes the latest key rather than
retaining a stale value:

```text
market:implied_volatility:v1:BTCUSD:latest    # TTL 60 seconds
method = "robust_black_digital_mid_iv_v1"
```

## Kalshi quote and edge definitions

Kalshi ticker values ending in `_dollars` are parsed as dollar probabilities
in `[0, 1]`.

```text
market_mid_probability = (kalshi_yes_bid_dollars + kalshi_yes_ask_dollars) / 2
edge_vs_mid_probability = model_probability - market_mid_probability
buy_yes_edge_probability = model_probability - kalshi_yes_ask_dollars
sell_yes_edge_probability = kalshi_yes_bid_dollars - model_probability
```

The midpoint and all edge fields are `null` unless both bid and ask are
present, finite, ordered, and inside `[0, 1]`. No trade decision is emitted.
Values in cents remain floating point for analysis:

```text
model_value_dollars = model_probability
model_value_cents = 100 * model_probability
```

Integer quote rounding, fees, slippage, and minimum-edge policy belong to a
future execution plan and are not part of this pricing service.

## Analytics Redis outputs

For each successfully priced active market, publish:

```text
market:pricing:v1:<market_ticker>    # latest state, TTL 60 seconds
market:pricing:v1:active             # sorted set, score = generated_ts_ms
stream:pricing:v1                    # durable audit stream, MAXLEN ~ 5,000
pub:pricing:v1                       # optional wake-up notification
market:implied_volatility:v1:BTCUSD:latest  # active-event midpoint IV, TTL 60 seconds
```

Latest-state payload:

```json
{
  "schema_version": 1,
  "event_type": "kalshi_model_price",
  "status": "available",
  "market_ticker": "KXBTCD-...-T70000",
  "event_ticker": "KXBTCD-...",
  "asset": "BTCUSD",
  "spot_price": 70125.5,
  "spot_generated_ts_ms": 1788593790000,
  "strike": 70000.0,
  "expiry_ts_ms": 1788597390000,
  "pricing_asof_ts_ms": 1788593790000,
  "time_to_expiry_seconds": 3600.0,
  "time_to_expiry_minutes": 60.0,
  "annualized_volatility": 0.45,
  "volatility_bracket": ["1h", "1h"],
  "volatility_generated_ts_ms": 1788593790116,
  "interpolation_method": "linear_annualized_volatility_v1",
  "distribution_model": "zero_log_return_gaussian_v1",
  "model_probability": 0.61,
  "model_value_dollars": 0.61,
  "model_value_cents": 61.0,
  "kalshi_yes_bid_dollars": 0.58,
  "kalshi_yes_ask_dollars": 0.60,
  "market_mid_probability": 0.59,
  "edge_vs_mid_probability": 0.02,
  "buy_yes_edge_probability": 0.01,
  "sell_yes_edge_probability": -0.03,
  "model_version": "v2_10s",
  "generated_ts_ms": 1788593790200
}
```

Publishing the latest key, sorted-set member, stream entry, and notification is
one analytics-owned Redis pipeline. Pub/Sub failure does not invalidate the
durable/latest writes.

Before each publish cycle, analytics removes sorted-set members older than 60
seconds. Readers must still tolerate a member whose latest key has expired.

## Unavailable behavior

On any invalid or stale required input, analytics must not preserve a
tradable-looking latest price. It performs the following analytics-owned
operations:

1. Delete `market:pricing:v1:<market_ticker>`.
2. Remove the ticker from `market:pricing:v1:active`.
3. Write a diagnostic record with a short TTL to
   `market:pricing:v1:status:<market_ticker>`.
4. Log a bounded reason enum without secrets or raw credentials.

Initial reason enums include:

```text
stale_spot
stale_ticker
stale_features
stale_volatility
missing_market_metadata
unsupported_contract
invalid_strike
outside_supported_lifetime
model_inference_failed
incomplete_term_structure
invalid_quote
```

Invalid or stale quotes prevent edge calculation but do not prevent publishing
the standalone model probability when every model input is valid. All other
reasons above make the model price unavailable.

## Current implementation

- Pure validation, linear annualized-volatility interpolation, Gaussian
  probability, and
  boundary tests are implemented in the analytics package.
- Redis adapters recover spot, live features, and Kalshi ticker state without
  relying on Pub/Sub.
- The Kalshi REST cache resolves active-market strike and expiry semantics.
- Production uses the bounded HTTP provider and validates an all-or-nothing
  four-horizon response. Direct Vertex/GCS inference remains a rollback path.
- Redis publication, health/readiness, stale cleanup, GKE deployment, dashboard
  integration, and CD smoke checks are active.
- Historical probability calibration and any execution policy remain separate
  future work. The service remains non-trading.

## Acceptance checks

1. No existing service schema, key, channel, consumer group, or deployment must
   change for analytics to run.
2. Analytics can recover current spot and Kalshi ticker state after restart
   without relying on Pub/Sub delivery.
3. Exactly the `5m`, `15m`, `30m`, and `1h` forecasts produce one
   atomic volatility snapshot from one feature observation; production
   identifies EWMA short horizons and the approved immutable 1h model.
4. Annualization occurs in training only; pricing converts lifetime to year
   fraction once.
5. Interpolation and pricing are deterministic at `0`, `5m`, `15m`, `30m`,
   and `60m` boundaries.
6. Missing, partial, stale, non-finite, or semantically ambiguous inputs cannot
   leave a live price key behind.
7. Output fields have explicit units and readers can distinguish model value,
   midpoint edge, and executable-side edge.
8. Redis restart, analytics restart, missed Pub/Sub, and Kalshi event rollover
   tests demonstrate recovery without interfering with other consumers.
