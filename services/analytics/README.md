# Analytics pricing service

Analytics consumes the existing aggregator latest keys and Kalshi ticker stream,
loads five approved volatility models, and publishes model probabilities and
quote-relative edges. It never emits orders or trading decisions and has no EWMA
or other production fallback.

## Inputs and outputs

Inputs are `market:spot:BTCUSDT:latest`,
`market:features:v1:BTCUSD:latest`, and `stream:kalshi_tickers`. The service owns
consumer group `analytics-pricing-v1`; startup uses `XREVRANGE` and abandoned
pending entries are recovered with `XAUTOCLAIM`, so Pub/Sub is not required for
recovery. Kalshi REST supplies authoritative above-strike market metadata.

Successful cycles write:

- `market:volatility:v1:BTCUSD:latest` (60-second TTL)
- `market:pricing:v1:<market_ticker>` (60-second TTL)
- `market:pricing:v1:active`, `stream:pricing:v1`, and `pub:pricing:v1`

Invalid or stale inputs delete the latest price and write a short-lived
`market:pricing:v1:status:<market_ticker>` diagnostic. Invalid quotes only make
the quote/edge fields null; the standalone model probability remains available.

## Required runtime configuration

| Variable | Meaning |
|---|---|
| `REDIS_HOST`, `REDIS_PORT` | Existing private Redis endpoint; `ANALYTICS_REDIS_URL` may override both locally. |
| `KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY` | Existing authenticated REST credentials. |
| `GCP_PROJECT_ID`, `GCP_REGION` | Vertex/GCS project and region. |
| `VOLATILITY_MODEL_1M`, `VOLATILITY_MODEL_5M`, `VOLATILITY_MODEL_15M`, `VOLATILITY_MODEL_30M`, `VOLATILITY_MODEL_1H` | Exact immutable Vertex resource names, such as `projects/123/locations/asia-northeast3/models/456@1`. |
| `VOLATILITY_MODEL_VERSION` | Expected artifact/label version; defaults to `v1`. |
| `CONSUMER_NAME` | Redis consumer identity; defaults to the pod hostname. |
| `HEALTH_PORT` | HTTP probe port; defaults to `8080`. |
| `SPOT_MAX_AGE_MS`, `TICKER_MAX_AGE_MS`, `FEATURE_MAX_AGE_MS`, `VOLATILITY_MAX_AGE_MS` | Freshness limits; defaults are 5, 60, 60, and 60 seconds. |
| `ALLOWED_FUTURE_SKEW_MS`, `KALSHI_METADATA_REFRESH_MS` | Clock-skew allowance and active metadata refresh; defaults are 2 and 15 seconds. |

Do not configure a display name, alias such as `latest`, or a model-family name.
Each exact resource must resolve to an artifact directory containing
`model.joblib` and `metadata.json`. Metadata must contain the matching `horizon`
and non-empty ordered `feature_columns`; Vertex labels `horizon` and `version`
are checked when present. All five artifacts must load and infer from one feature
observation or readiness stays false.

The training pipeline serializes an `xgboost.XGBRegressor` with joblib. The
analytics image therefore includes joblib, XGBoost, pandas, NumPy (transitive),
and scikit-learn using the same supported major formats. Promotion must upload
both files from each horizon bundle. A rollout using artifacts created by an
incompatible future XGBoost/joblib major version deliberately remains not ready;
retrain or align the analytics dependency bounds instead of substituting a
fallback.

### Champion-selection deployment prerequisite

The repository does not contain the five promoted champion resource IDs, so CD
cannot infer them safely. Copy the `vertex_resource` values from an approved
promotion report into the five GitHub variables. Before doing so, verify that
each model's `artifactUri` is horizon-specific and immutable.

The current promotion script uploads to
`gs://<bucket>/models/<version>/candidate/<horizon>` before registration. A later
promotion using the same version can overwrite that path, so an old numeric
Vertex resource ID alone does not prove that its backing bytes are still the
approved champion. Production deployment therefore requires either five models
already registered from immutable per-promotion artifact paths, or an operator
copy/re-registration of each approved bundle at such a path. Analytics validates
the exact configured resource, horizon/version labels, metadata, and loadability,
but cannot reconstruct the intended champion from a mutable bucket prefix. Until
this prerequisite is satisfied, leave the model variables unset; CD will fail
with a prerequisite message and analytics readiness will not be claimed.

## Kubernetes and CI/CD prerequisites

Create the existing secret (never commit either value):

```bash
kubectl create secret generic kalshi-credentials \
  --from-literal=api-key="$KALSHI_API_KEY" \
  --from-file=private-key=/secure/path/kalshi-private.pem
```

Configure these GitHub repository variables:

- `ANALYTICS_GCP_SERVICE_ACCOUNT`: GCP service-account email used by the
  Kubernetes `analytics` service account through Workload Identity.
- All five `VOLATILITY_MODEL_*` exact resource names listed above.

The GCP service account needs `aiplatform.models.get` on the configured Vertex
models and `storage.objects.get` on their artifact objects. Grant
`roles/iam.workloadIdentityUser` on that GCP account to
`serviceAccount:<project>.svc.id.goog[default/analytics]`. Prefer resource- or
bucket-scoped custom roles/grants over project-wide access.

CI tests analytics on Python 3.12 and builds its linux/amd64 image. CD checks out
the exact successful CI commit, builds/pushes the image, renders the manifest,
waits for readiness, then verifies the HTTP endpoint and fresh Redis volatility
and pricing records. A running pod that is not ready is reported with logs and
the likely missing live/model/IAM prerequisites; it is never reported as a
successful deployment.

## Verification

```bash
UV_CACHE_DIR=/tmp/kalshi-analytics-uv-cache uv sync --locked
UV_CACHE_DIR=/tmp/kalshi-analytics-uv-cache uv run --locked pytest
UV_CACHE_DIR=/tmp/kalshi-analytics-uv-cache uv run --locked ruff check src tests

kubectl rollout status deployment/analytics --timeout=5m
kubectl port-forward service/analytics 18080:8080
curl --fail http://127.0.0.1:18080/readyz
```

`/healthz` proves the process is alive. `/readyz` succeeds only after models are
loaded and at least one supported market has a fresh published price. Inspect
`market:volatility:v1:BTCUSD:latest`, `market:pricing:v1:active`, and matching
`market:pricing:v1:KXBTCD-*` keys for post-deploy evidence.
