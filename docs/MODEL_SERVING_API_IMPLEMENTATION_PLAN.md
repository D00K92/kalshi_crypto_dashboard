# Model-Serving API Implementation Plan

## Objective

Separate live market-state aggregation, ML feature computation, volatility-model inference, and Analytics pricing. The model-serving API owns approved model artifact loading and five-horizon inference; Analytics owns Kalshi contract pricing and edge calculation.

```text
Venue feeds → Aggregator → bars/order-book state → live_feature_service
                                     ↓                    ↓
                              Redis market state      Redis features + Feast push
                                                            ↓
                              Analytics → model-serving API → five forecasts
                                           ↓
                              probability / price / edge → Redis
```

The Aggregator owns reusable market primitives: normalized trades, resampled
bars, consolidated order-book state, best bid/ask, spread, depth, imbalance,
mid-price, source freshness, and sequence-gap handling. It does not own
model-specific feature definitions.

The `live_feature_service` combines those primitives into the
inference-ready feature contract. It owns rolling windows, realized volatility,
EWMA state, feature timestamps, validity checks, idempotent publication, and
publishing both a Redis stream event and an atomic latest-feature snapshot.

Feast is an offline/online feature contract and consistency layer. Its online
store is updated asynchronously from the live feature stream; it should not be
on the synchronous dashboard pricing critical path.

## 1. Service boundary

### `model-serving` owns

- Loading the five approved Vertex/GCS model artifacts at startup.
- Validating horizon, model version, artifact metadata, and ordered feature columns.
- Running inference and returning one complete volatility term structure.

### `model-serving` does not own

- Redis consumption or Feast writes.
- Kalshi metadata, contract probability, price, or edge calculations.
- Dashboard outputs or order execution.

Analytics continues to own Redis feature/spot reads, Kalshi ticker and REST metadata handling, freshness checks, total-variance interpolation, probability/edge calculation, and Redis pricing publication.

Analytics should read the low-latency Redis latest-feature snapshot and call
model-serving. Feast remains available for historical retrieval, parity checks,
and online-store reconciliation.

## 2. API contract

### Request

`POST /v1/forecast`

```json
{
  "feature_set": "market_features",
  "feature_version": "v1",
  "event_timestamp_ms": 1760000000000,
  "values": {
    "synthetic_price": 70000.0,
    "log_return": 0.0002,
    "venue_count": 6,
    "ewma_state": {
      "frequency": "1m",
      "variance": 0.000001
    }
  },
  "source_timestamps_ms": {
    "bar_1m": 1760000000000,
    "order_book": 1760000000000
  }
}
```

### Response

```json
{
  "annualized_volatility": {
    "5m": 0.34,
    "15m": 0.36,
    "30m": 0.38,
    "1h": 0.40
  },
  "feature_asof_ts_ms": 1760000000000,
  "generated_ts_ms": 1760000000100,
  "model_resources": {
    "5m": "projects/.../models/...@1"
  },
  "model_version": "v1"
}
```

Rules:

- Exactly four horizons are required: `5m`, `15m`, `30m`, `1h`.
- Feature version and names must match the artifact metadata.
- `event_timestamp_ms` identifies the feature observation, not request arrival time.
- EWMA inference receives validated rolling state; a single latest return is
  insufficient to reconstruct EWMA after a restart.
- Partial, invalid, stale, or non-finite predictions are rejected.
- The API never fabricates a fallback forecast.

## 3. New service

Create `services/model_serving/` with:

```text
src/model_serving/
├─ artifact_loader.py
├─ inference_service.py
├─ api.py
├─ config.py
└─ health.py
tests/
Dockerfile
pyproject.toml
uv.lock
```

Behavior:

- Resolve the five immutable Vertex resources at startup.
- Download each `model.joblib` and `metadata.json` artifact from its GCS URI.
- Retain validated models in memory.
- `GET /healthz` means the process is alive.
- `GET /readyz` succeeds only when all five models are loaded and valid.
- `POST /v1/forecast` validates the feature payload and returns the complete term structure.

The service must be an internal Kubernetes `ClusterIP` API, not a public endpoint.

For the current EWMA champion path, implement a deterministic `EWMAProvider`
inside model-serving. It consumes the validated EWMA state produced by Live
Feature Computation and returns the same five-horizon forecast contract as a
future learned-model provider. It must not consume Redis directly.

Create `services/live_feature_service/` with ownership of the live feature
contract, rolling state, Redis stream consumer, latest-feature publication, and
asynchronous Feast online-store push. The service must fail closed for missing,
stale, out-of-order, or non-finite market inputs.

## 4. Analytics refactor

Replace the local `ConfiguredForecastProvider` with an HTTP forecast client.

Configuration:

```text
MODEL_SERVING_URL=http://model-serving:8080
MODEL_SERVING_TIMEOUT_MS=<bounded timeout>
```

Analytics must treat a timeout, malformed response, incomplete horizon set, invalid metadata, or unavailable model-serving API as `model_inference_failed`. It must not publish a price in that case.

After migration, Analytics no longer needs the Vertex AI, GCS artifact, `joblib`, pandas, or XGBoost runtime dependencies.

## 5. Kubernetes and IAM

```text
model-serving ServiceAccount
        ↓ Workload Identity
Vertex AI Model Registry + GCS read permissions
        ↓
model-serving Deployment → ClusterIP Service
        ↓
Analytics Deployment
```

- The model-serving service account needs Vertex model read and GCS object read access.
- Analytics should not retain model-artifact IAM permissions after the switch.
- `live_feature_service` needs Redis stream/read-write and Feast online-push
  permissions; it does not need Vertex model-artifact access.
- Deployment order: model-serving readiness before Analytics rollout.

## 6. CI/CD

Add model-serving to CI for locked dependency installation, tests, API-contract tests, and Linux Docker image builds.

Add model-serving to CD:

```text
CI success
  → build/test live-feature-service
  → commit-SHA model-serving image
  → Artifact Registry
  → deploy live-feature-service
  → deploy model-serving
  → wait for /readyz
  → deploy/restart Analytics
  → verify fresh Redis volatility and KXBTCD price outputs
```

## 7. Safe migration

### Phase A — parity testing

- Keep direct in-process inference as the active path.
- Deploy the API in parallel.
- Send the same feature payload to both providers.
- Compare all five forecasts, model version, feature timestamp, and horizon mapping.
- Compare live-computed features and EWMA state against the existing batch/Feast
  definitions over the same bounded timestamps.

### Phase B — controlled switch

- Add `FORECAST_PROVIDER=direct|http`.
- Test `http` in staging first.
- Verify API readiness, response latency, Redis volatility, and KXBTCD pricing outputs.
- Switch production only after output parity is established.

### Phase C — cleanup

- Remove local model loading and artifact credentials from Analytics.
- Retain contract tests that reject malformed API responses.

## Definition of done

- Model-serving exposes validated five-horizon inference through `/v1/forecast`.
- Analytics calls only the internal API for active inference.
- Aggregator publishes canonical bars and order-book state.
- `live_feature_service` publishes valid, timestamped inference features and
  asynchronously reconciles them into Feast.
- Failure remains fail-closed.
- Direct and API inference match during migration.
- CI and CD test/build/deploy model-serving before Analytics.
- Analytics no longer has Vertex/GCS model-read permissions.

## Target architecture

The intended ownership model is:

```text
Aggregator            = live market-state aggregation
live_feature_service  = live feature calculation
Feast                 = feature contract / offline-online consistency
Model-serving API     = model artifact loading and inference
Analytics             = KXBTCD pricing and edge analysis
```
