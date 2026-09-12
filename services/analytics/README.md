# Analytics pricing service

Prices active Kalshi KXBTCD contracts from existing live market data. Analytics
reads Redis, obtains authoritative Kalshi metadata, calls model-serving, and
publishes model probabilities, fair values, and quote-relative edges. It does
not modify upstream schemas and never submits orders.

## Inputs

| Interface | Owner | Use |
|---|---|---|
| `market:spot:BTCUSDT:latest` | `aggregator` | Current synthetic BTC spot |
| `market:features:v2_10s:BTCUSD:latest` | `live_feature_service` | Current model feature envelope and EWMA state |
| `stream:kalshi_tickers` | `ingestion` | Current KXBTCD quotes |
| Kalshi REST events/markets | Kalshi | Strike, expiry, event, and open status |
| `POST model-serving:8080/v1/forecast` | `model-serving` | Four-horizon annualized volatility |

The service owns Redis group `analytics-pricing-v1`, bootstraps recent tickers
with `XREVRANGE`, and reclaims abandoned pending entries. Pub/Sub is not
required for recovery.

## Outputs

| Redis interface | Retention / consumer |
|---|---|
| `market:volatility:v2_10s:BTCUSD:latest` | 60-second TTL; operations and CD smoke check |
| `market:implied_volatility:v1:BTCUSD:latest` | 60-second TTL; dashboard volatility cone |
| `market:pricing:v1:<market_ticker>` | 60-second TTL; dashboard |
| `market:pricing:v1:status:<market_ticker>` | Short-lived unavailable reason |
| `market:pricing:v1:active` | Sorted set used for expiry/cleanup |
| `stream:pricing:v1` | Bounded publication history |
| `pub:pricing:v1` | Best-effort notification |

Analytics linearly interpolates annualized volatility to the contract expiry, applies the
documented Gaussian above-strike model, and calculates fair value plus
bid/mid/ask edges. See `volatility_term_structure.md` for the pricing rules.

For the active hourly event, analytics selects the five to seven valid KXBTCD
contracts nearest spot and fits one midpoint Black-digital implied volatility.
Weights are `(open interest + 1)` normalized across the selected contracts;
the robust fit limits the influence of one dislocated quote. The IV key is
deleted when fewer than five valid contracts are available.

Missing, stale, future-skewed, malformed, or incomplete model inputs fail
closed: the available price is removed and a status record is written. Invalid
or stale quotes leave quote/edge fields unavailable while preserving a valid
standalone model probability and fair value.

## Forecast providers

`FORECAST_PROVIDER=http` is production. It forwards the current v2_10s
observation to model-serving and validates response horizons, timestamps,
version, values, and resources.

`FORECAST_PROVIDER=direct` is retained only for rollback/parity. It loads four
exact Vertex/GCS artifacts in-process and therefore requires the
`VOLATILITY_MODEL_5M`, `15M`, `30M`, and `1H` resource variables plus
artifact-read IAM. Do not configure display names or mutable aliases.

## Run and test locally

Production-style HTTP mode requires Redis, model-serving, GCP project metadata,
and Kalshi credentials:

```bash
uv sync --locked
GCP_PROJECT_ID=kalshi-crypto-506614 \
ANALYTICS_REDIS_URL=redis://localhost:6379/0 \
MODEL_SERVING_URL=http://127.0.0.1:8080 \
KALSHI_API_KEY=... KALSHI_PRIVATE_KEY=... \
  uv run --locked analytics

uv run --locked pytest
uv run --locked ruff check src tests
```

## Configuration

| Variable | Default / purpose |
|---|---|
| `ANALYTICS_REDIS_URL` | Optional full URL; otherwise `REDIS_HOST:REDIS_PORT` |
| `KALSHI_REST_URL` | `https://external-api.kalshi.com` |
| `KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY` | Authenticated REST credentials |
| `GCP_PROJECT_ID` | Required by current settings in both provider modes |
| `GCP_REGION` | `asia-northeast3` |
| `FORECAST_PROVIDER` | `http` |
| `MODEL_SERVING_URL` | `http://model-serving:8080` |
| `MODEL_SERVING_TIMEOUT_MS` | `1000` |
| `FEATURE_VERSION`, `VOLATILITY_MODEL_VERSION` | `v2_10s` |
| `CONSUMER_NAME` | `analytics-$HOSTNAME` |
| `SPOT_MAX_AGE_MS` | `5000` |
| `TICKER_MAX_AGE_MS` | `60000` |
| `FEATURE_MAX_AGE_MS` | `90000` |
| `VOLATILITY_MAX_AGE_MS` | `60000` |
| `ALLOWED_FUTURE_SKEW_MS` | `2000` |
| `KALSHI_METADATA_REFRESH_MS` | `15000` |
| `HEALTH_PORT` | `8080` |

## Deployment and verification

Kubernetes runs `deployment/analytics` and internal
`service/analytics:8080` from `k8s/analytics-deployment.yaml`. Kalshi
credentials come from `secret/kalshi-credentials`.
`ANALYTICS_GCP_SERVICE_ACCOUNT` supplies the Workload Identity annotation
needed by the retained direct provider.

`GET /healthz` means the process is alive. `GET /readyz` succeeds only when
the forecast provider is ready, Redis is reachable, and at least one supported
market has a fresh published price.

```bash
kubectl rollout status deployment/analytics --timeout=5m
kubectl port-forward service/analytics 18080:8080
curl --fail http://127.0.0.1:18080/readyz
```

CD additionally asserts a fresh v2_10s volatility snapshot, the exact approved
1h resource, EWMA resources for 5m/15m/30m, positive values, and at least one
fresh KXBTCD pricing key.
