# Analytics Volatility and Kalshi Pricing Plan

## Goal and compatibility rule

Build `services/analytics` as a new consumer and publisher around the current
architecture. Existing ingestion, aggregator, Feast, ML-training, GCS
exporter, and dashboard contracts remain unchanged.

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
crypto exchanges
      |
      v
ingestion (unchanged) ---> Redis trade/book streams
      |                              |
      |                              v
      |                    aggregator (unchanged)
      |                       |                 |
      |                       |                 +--> stream:features:v1
      |                       |                          |
      |                       |                          v
      |                       |                 Feast live bridge/server
      |                       |                          |
      |                       |                    (unchanged; remains
      |                       |                     available to other clients)
      |                       v
      |              market:spot:BTCUSDT:latest
      |              market:features:BTCUSD:latest
      |                       |
      +--> stream:kalshi_tickers
      |              |
      v              v
Kalshi REST ----> analytics <---- configured Vertex model artifacts
                      |
                      +--> market:volatility:v1:BTCUSD:latest
                      +--> market:pricing:v1:<market_ticker>
                      +--> stream:pricing:v1
                      +--> pub:pricing:v1
                                |
                                v
                     dashboard or other readers (optional)
```

Analytics uses the same private Redis instance and Kubernetes network as the
existing services. Each dependency has a narrow purpose:

| Dependency | Existing interface used by analytics | Purpose | Required upstream change |
|---|---|---|---|
| Aggregator | `GET market:spot:BTCUSDT:latest` | Synthetic spot and spot timestamp | None |
| Live feature service | `GET market:features:BTCUSD:latest` | One timestamped v1 model-feature observation | None |
| Aggregator | `SUBSCRIBE market:aggregated_spot` | Low-latency wake-up only | None |
| Ingestion | `stream:kalshi_tickers` | Bid, ask, ticker freshness, event and market identifiers | None |
| Kalshi REST | Existing authenticated event/market endpoints | Authoritative market definition, strike, settlement/expiry time, and status | None |
| Vertex AI Model Registry/GCS | Five configured model resource names and their artifact URIs | Load the approved horizon models and `metadata.json` | None |
| Dashboard | Existing Redis connection; optional read of analytics-owned keys | Display model value and edge later | None |

The v1 analytics path reads the aggregator's existing timestamped latest-feature
envelope directly. This guarantees that all five local model calls use exactly
one observation and lets analytics validate its age. The existing Feast live
bridge and `feast-server:6566` continue unchanged for their current consumers;
analytics does not need to put the Feast server on its critical path merely to
reread the same v1 row. A future `ForecastProvider` may use Feast without
changing the pricing core or any producer.

Pub/Sub is never the source of truth. A spot notification only tells analytics
to reread `market:spot:BTCUSDT:latest`. A periodic read provides recovery after
missed notifications.

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
- Aggregator owns the synthetic spot calculation and live feature
  publication.
- Feast store owns feature definitions, the live bridge, and online feature
  serving.
- ML pipeline owns training, evaluation, promotion, and registration of one
  model per supported horizon.
- Dashboard owns presentation. It is not required for analytics readiness.

### Analytics owns

- Reading and freshness-checking the existing inputs.
- Fetching Kalshi market metadata because the existing ticker stream does not
  contain expiry and settlement fields.
- Loading configured approved model artifacts, ordering features according to
  each artifact's `metadata.json`, and running online inference.
- Combining the five forecasts into one atomic internal term structure.
- Interpolating variance, computing model probabilities, and publishing the
  analytics-owned Redis outputs.
- Reporting unavailable states without emitting a tradable-looking stale
  value.

Analytics must not import another service's private Python package at runtime.
Redis JSON, Kalshi REST, and Vertex/GCS APIs are the service boundaries. Shared
code can be extracted later, but is not required here.

## Live inference adapter

The model-serving service owns inference and publishes no Redis data itself.
Analytics retains a `ForecastProvider` boundary so the HTTP migration can be
rolled back safely to the direct provider during parity testing.

The production provider calls the private model-serving API for exactly five
horizons:

```text
1m, 5m, 15m, 30m, 1h
```

Analytics reads one `market:features:BTCUSD:latest` envelope and forwards its
feature contract, timestamp, and values to `/v1/forecast`. It rejects timeouts,
malformed responses, version mismatches, incomplete horizons, invalid values,
or timestamp mismatches. Set `FORECAST_PROVIDER=direct` only for rollback or
parity testing with the legacy Vertex/GCS artifact path.

All five results must use the same latest-feature observation and complete
successfully. Partial results are not published. The assembled snapshot is
written by analytics, not by the ML pipeline:

```text
market:volatility:v1:BTCUSD:latest
```

Example:

```json
{
  "schema_version": 1,
  "event_type": "volatility_term_structure",
  "asset": "BTCUSD",
  "model_version": "v1",
  "model_resources": {
    "1m": "projects/.../models/...",
    "5m": "projects/.../models/...",
    "15m": "projects/.../models/...",
    "30m": "projects/.../models/...",
    "1h": "projects/.../models/..."
  },
  "feature_asof_ts_ms": 1788593790000,
  "generated_ts_ms": 1788593790116,
  "annualized_volatility": {
    "1m": 0.31,
    "5m": 0.34,
    "15m": 0.37,
    "30m": 0.41,
    "1h": 0.45
  }
}
```

`BTCUSD` is the live entity emitted by the aggregator. Historical label
rows currently use `BTC`; analytics does not ask either producer to rename it.
The mapping is explicit in the adapter, and `asset` is not passed as a numeric
model feature.

A deterministic fixture provider may be used in tests. An EWMA provider may be
used for an explicitly labeled non-production benchmark, but it must not be
reported as an ML model or silently selected when a configured model fails.

## Forecast validation and freshness

Every volatility must be finite and strictly positive. Analytics rejects the
whole snapshot if a horizon is missing, a feature is missing, a model resource
does not match its configured horizon/version, or inference fails.

The following are initial configurable limits:

```text
spot maximum age             = 5 seconds
Kalshi ticker maximum age    = 60 seconds
live feature maximum age     = 60 seconds
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

Interpolation is deterministic and uses cumulative total variance, not linear
volatility interpolation. Let `h0` and `h1` be adjacent supported horizons in
seconds and let their annualized forecasts be `sigma0` and `sigma1`:

```text
V0 = sigma0^2 * h0 / seconds_per_year
V1 = sigma1^2 * h1 / seconds_per_year
w = (tau_seconds - h0) / (h1 - h0)
Vtau = (1 - w) * V0 + w * V1
sigma_tau = sqrt(Vtau / T)
```

Selection is:

| Remaining lifetime | Rule |
|---|---|
| `0 < tau < 1m` | Flat short-end variance: `sigma_tau = sigma_1m` |
| `1m <= tau < 5m` | Interpolate `V1m` and `V5m` |
| `5m <= tau < 15m` | Interpolate `V5m` and `V15m` |
| `15m <= tau < 30m` | Interpolate `V15m` and `V30m` |
| `30m <= tau < 60m` | Interpolate `V30m` and `V1h` |
| `tau = 60m` | Use `sigma_1h` |

For the selected bracket, `V1` must be greater than or equal to `V0`. Because
the horizons are trained independently, a violation is possible. The initial
implementation rejects that market calculation with reason
`non_monotone_total_variance`; it does not silently repair the model outputs.

The output enum is:

```text
interpolation_method = "linear_total_variance_v1"
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
  "annualized_volatility": 0.45,
  "volatility_bracket": ["1h", "1h"],
  "volatility_generated_ts_ms": 1788593790116,
  "interpolation_method": "linear_total_variance_v1",
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
  "model_version": "v1",
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
non_monotone_total_variance
invalid_quote
```

Invalid quotes prevent edge calculation but do not prevent publishing the
standalone model probability when every model input is valid. All other
reasons above make the model price unavailable.

## Delivery sequence

1. Implement pure validation, total-variance interpolation, Gaussian
   probability, and output-schema modules with boundary tests.
2. Add read-only Redis adapters for aggregator spot and Kalshi ticker state.
3. Add the Kalshi REST metadata cache and verify strike/expiry semantics
   against recorded active market responses.
4. Add the live-feature adapter and configured Vertex/GCS artifact loader; test
   feature ordering from `metadata.json` and all-or-nothing five-horizon
   inference.
5. Add the pricing loop, analytics-owned Redis publisher, health endpoints,
   and stale-state cleanup.
6. Deploy analytics into the existing GKE and private Redis network with its
   own service account permissions and consumer group.
7. Validate shadow outputs against historical settlements and check probability
   calibration. Keep the service non-trading during this phase.
8. Optionally extend the dashboard to read analytics outputs. This is a
   dashboard enhancement, not a dependency of analytics deployment.

## Acceptance checks

1. No existing service schema, key, channel, consumer group, or deployment must
   change for analytics to run.
2. Analytics can recover current spot and Kalshi ticker state after restart
   without relying on Pub/Sub delivery.
3. Exactly the configured `1m`, `5m`, `15m`, `30m`, and `1h` artifacts produce
   one atomic volatility snapshot from one feature observation.
4. Annualization occurs in training only; pricing converts lifetime to year
   fraction once.
5. Interpolation and pricing are deterministic at `0`, `1m`, `5m`, `15m`,
   `30m`, and `60m` boundaries.
6. Missing, partial, stale, non-finite, or semantically ambiguous inputs cannot
   leave a live price key behind.
7. Output fields have explicit units and readers can distinguish model value,
   midpoint edge, and executable-side edge.
8. Redis restart, analytics restart, missed Pub/Sub, and Kalshi event rollover
   tests demonstrate recovery without interfering with other consumers.
