# Model-serving API

Internal forecast service for the complete four-horizon volatility term
structure. It accepts one validated live feature observation and returns
annualized forecasts for `5m`, `15m`, `30m`, and `1h`.

Production is a hybrid provider:

- 5m, 15m, and 30m: deterministic EWMAs from matching 5-, 15-, and
  30-minute synthetic-candle return states.
- 1h: the immutable promoted XGBoost artifact packaged into the image.

The `v3_10s` bundle format can instead package one HAR or XGBoost artifact for
each horizon. The provider routes each horizon through the ordered feature
columns recorded in that artifact's metadata; legacy EWMA entries remain valid
for rollback bundles.

The service does not read Redis or Feast, fetch Kalshi metadata, calculate
contract probabilities, or expose a public endpoint.

## HTTP API

| Endpoint | Behavior |
|---|---|
| `GET /healthz` | Process is alive |
| `GET /readyz` | Bundle is loaded and the provider is ready |
| `POST /v1/forecast` | Validate one `market_features/v2_10s` observation and return all four forecasts |

Example request:

```json
{
  "feature_set": "market_features",
  "feature_version": "v2_10s",
  "event_timestamp_ms": 1760000000000,
  "available_timestamp_ms": 1760000000100,
  "values": {
    "synthetic_price": 70000.0,
    "log_return": 0.0002,
    "venue_count": 6,
    "ewma_states": {
      "5m": {"frequency": "5m", "variance": 0.000001},
      "15m": {"frequency": "15m", "variance": 0.000002},
      "30m": {"frequency": "30m", "variance": 0.000003}
    }
  },
  "source_timestamps_ms": {
    "binance": 1760000000000,
    "coinbase": 1760000000000
  }
}
```

A successful response contains `annualized_volatility`, feature
event/availability timestamps, `generated_ts_ms`, `model_resources`, and
`model_version`. Contract mismatch, stale/future timestamps, missing sources,
invalid EWMA state, or non-finite values produce 422. An incomplete internal
term structure produces 500.

## Immutable bundle

`scripts/prepare_bundle.py` retains the approved v2 1h-artifact interface. For
v3, pass an artifact root containing `5m/`, `15m/`, `30m/`, and `1h/` directories
plus `--resource-map`, a JSON mapping each horizon to its immutable Vertex
`resource` and `artifact_uri`. It validates metadata and writes checksums for
all four models.

Startup rechecks the manifest, paths, checksums, metadata, model interface, and
a smoke prediction. The running pod needs no Vertex/GCS IAM because release CI
downloads and packages the artifact before building the image.

## Run and test locally

```bash
uv sync --locked
uv run --locked pytest

uv run --locked python scripts/prepare_bundle.py \
  --artifact-dir /tmp/approved-1h \
  --output-dir model_bundle \
  --resource-name projects/.../locations/asia-northeast3/models/...@1 \
  --artifact-uri gs://bucket/immutable/path

MODEL_BUNDLE_MANIFEST=model_bundle/manifest.json \
  uv run --locked uvicorn model_serving.api:app --host 127.0.0.1 --port 8080
```

A normal local Docker build may omit a model to validate image construction.
Production passes `--build-arg REQUIRE_MODEL_BUNDLE=true`, which fails the
build when the approved bundle is absent or incompatible.

## Configuration

| Variable | Default |
|---|---|
| `MODEL_VERSION` | `v2_10s` |
| `FEATURE_VERSION` | `v2_10s` |
| `MODEL_MAX_FEATURE_AGE_MS` | `90000` |
| `MODEL_ALLOWED_FUTURE_SKEW_MS` | `2000` |
| `EWMA_DECAY` | `0.96` |
| `MODEL_BUNDLE_MANIFEST` | `/models/manifest.json` |

## Deployment and consumer

Kubernetes runs `deployment/model-serving` and internal ClusterIP
`service/model-serving` from `k8s/model-serving-deployment.yaml`. Analytics
calls `http://model-serving:8080/v1/forecast` with a bounded timeout.
Readiness prevents analytics rollout verification from succeeding with an
unusable bundle.
