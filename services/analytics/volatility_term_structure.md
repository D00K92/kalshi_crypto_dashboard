# Volatility Term-Structure Contract

## Purpose

The ML pipeline predicts future realized volatility at fixed horizons. The
analytics pricing service uses those predictions to price Kalshi Bitcoin
digital contracts whose remaining lifetime is no more than one hour.

The contract is a digital payoff:

```text
Pay $1 when BTC at expiry is greater than strike K.
```

## Supported ML horizons

The existing ML pipeline is the source of truth:

```text
1m, 5m, 15m, 30m, 1h
```

The analytics service must not invent a 10-minute model output or silently
substitute a different horizon.

## Annualization rule

Every ML output is already an annualized volatility. The pricing service must
not annualize it again or apply a second square-root-of-time scaling to the
forecast input. The consumed values are directly interpreted as:

```text
sigma_1m, sigma_5m, sigma_15m, sigma_30m, sigma_1h
```

The interpolated result remains annualized.

## Lifetime selection and interpolation

Let `tau` be the contract's remaining lifetime in minutes.

| Remaining lifetime | Source or bracket |
|---|---|
| `tau < 1` | Use `1m` forecast as the lower bound |
| `1 <= tau < 5` | Interpolate `1m` and `5m` |
| `5 <= tau < 15` | Interpolate `5m` and `15m` |
| `15 <= tau < 30` | Interpolate `15m` and `30m` |
| `30 <= tau < 60` | Interpolate `30m` and `1h` |
| `tau = 60` | Use `1h` forecast |
| `tau > 60` | Reject; outside supported contract lifetime |

For lower horizon `h0`, upper horizon `h1`, and annualized forecasts `sigma0`,
`sigma1`:

```text
w       = (tau - h0) / (h1 - h0)
sigma_t = (1 - w) * sigma0 + w * sigma1
```

At 50 minutes:

```text
w       = (50 - 30) / (60 - 30) = 2/3
sigma50 = (1/3 * sigma30) + (2/3 * sigma1h)
```

### Variance-consistent option

If review determines linear volatility interpolation is insufficient, the
implementation may interpolate horizon total variance internally:

```text
Vh = sigma_h^2 * horizon_years
Vtau = (1 - w) * Vh0 + w * Vh1
sigma_tau = sqrt(Vtau / tau_years)
```

This is interpolation, not a second annualization of the ML output. The
selected method must be recorded as `interpolation_method`.

## Redis input contract

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
  "model_name": "ewma",
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

The record has a 60-second freshness limit. Missing, negative, non-finite, or
stale values make pricing unavailable.

## Pricing inputs

For each active Kalshi ticker, analytics combines:

```text
S0 = current synthetic BTC price
K = strike parsed from market_ticker
tau = time remaining until Kalshi close_time
sigma_tau = selected/interpolated annualized volatility
mu = 0 for the initial toy model
skewness = 0 for the initial toy model
excess_kurtosis = 0 for the initial toy model
```

The initial model is a normal digital-option calculation. Skewness and excess
kurtosis are reserved for a later Gram–Charlier extension.

## Redis output

Publish one latest-state record per active market:

```text
market:pricing:v1:<market_ticker>
```

TTL is 120 seconds. The payload must include:

```text
market_ticker, event_ticker, asset, spot_price, strike, expiry_ts_ms,
time_to_expiry_seconds, annualized_volatility, volatility_bracket,
interpolation_method, fair_probability, fair_value_dollars, fair_value_cents,
kalshi_yes_bid, kalshi_yes_ask, market_mid_probability, edge_probability,
edge_cents, model_version, generated_ts_ms
```

For a `$1` binary contract:

```text
fair_value_dollars = fair_probability
fair_value_cents   = round(100 * fair_probability)
```

Also maintain:

```text
market:pricing:v1:active       # sorted set, score = generated_ts_ms
stream:pricing:v1              # bounded audit stream, maxlen about 5,000
pub:pricing:v1                 # optional non-durable notification
```

## Failure and safety behavior

- Reject stale spot, volatility, or ticker data.
- Reject `tau > 60m`.
- Never treat missing data as zero volatility or zero probability.
- Clamp numerical probabilities to `[0, 1]`.
- Mark pricing unavailable when interpolation inputs are missing.
- Include model version and interpolation method in every output.
- Use TTLs so expired contract prices cannot remain live indefinitely.

## Review checklist

1. ML emits exactly `1m, 5m, 15m, 30m, 1h`.
2. Values are annualized exactly once at model-output creation.
3. `tau` uses actual Kalshi `close_time` when available.
4. Interpolation is deterministic and tested at every boundary.
5. Redis input/output keys have explicit TTL and freshness checks.
6. Dashboard displays cents consistently with Kalshi values.
7. Missing model output cannot silently produce a tradable price.
