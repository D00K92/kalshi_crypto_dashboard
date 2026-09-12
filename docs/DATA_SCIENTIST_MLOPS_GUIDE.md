# Data scientist guide to model research and production MLOps

This guide is for researchers improving the BTC volatility forecasts used by
the live Kalshi pricing system. It identifies the files a data scientist can
change directly, the extra work required when a feature or model contract
changes, and how to give a coding agent an explicit, safe MLOps task.

The authoritative service boundaries remain in [`ARCHITECTURE.md`](../ARCHITECTURE.md).
Use this document as the change workflow, not as permission to bypass those
boundaries.

## Current production contract

Before starting an experiment, write these facts at the top of the experiment
report:

| Contract item | Production value |
|---|---|
| Feature contract | `market_features/v2_10s` |
| Ordered ML inputs | `log_return`, `venue_count` |
| Entity | `BTCUSD` |
| Offline features | `feature_store.realized_volatility_v2_10s` |
| Labels | `training_labels.future_realized_volatility_v2_10s` |
| Forecast horizons | `5m`, `15m`, `30m`, `1h` |
| Production learned model | Approved 1h model packaged into `model-serving` |
| Short-horizon production model | EWMA for 5m, 15m, and 30m |
| Primary promotion metric | QLIKE against EWMA and the current champion |
| Serving output | Positive, finite, annualized volatility |

A successful training run is **not** a production deployment. The training
pipeline can conditionally register passing models for all four horizons, but
it does not change the production GitHub model variables. The current release
path packages only the separately approved 1h artifact. Production does not
follow a mutable model name, `latest` alias, or display name.

Never change the meaning, order, or formula of a deployed feature version in
place. The additive `v3_10s` HAR research contract already exists; another
feature definition requires a new immutable version such as `v4_10s`, new
Redis names, a new BigQuery table, and a controlled migration.

## Choose the size of the change first

| Intended change | Scope | Normal owner |
|---|---|---|
| Try new hyperparameters or another joblib-compatible regressor with the same two inputs and one positive volatility output | `ml_pipeline` only until promotion | Data scientist |
| Change evaluation metrics or promotion thresholds | `ml_pipeline`, tests, and an explicit risk review | Data scientist plus reviewer |
| Add, remove, reorder, or redefine an input feature | Versioned offline + live + Feast + training + parity + serving migration | Data scientist specifies; coding agent implements integration |
| Change artifact format, preprocessing runtime, or inference framework | Training containers + bundle schema + model-serving runtime + CD | Coding agent with data scientist acceptance criteria |
| Add/remove horizons or change the response meaning | ML, model-serving, analytics interpolation, smoke checks, and possibly UI | System-level change; not a model-only edit |
| Change probability, fair-value, or edge calculations | `analytics`, not the model-training service | Quant/pricing change |

If the research idea can be evaluated without changing the live contract, keep
it model-only. That is the fastest and safest path.

## Files data scientists can modify

### Normal model-research surface

These are the primary files for a same-contract model experiment:

| File | What may change | Required proof |
|---|---|---|
| [`services/ml_pipeline/src/common/modeling.py`](../services/ml_pipeline/src/common/modeling.py) | Estimator, hyperparameters, fitting procedure, causal preprocessing, prediction floor | Chronological split remains intact; metadata and serialization tests pass |
| [`services/ml_pipeline/src/common/data_io.py`](../services/ml_pipeline/src/common/data_io.py) | Training population, point-in-time join, and eligibility filters | Tests prove label maturity, exact timestamp alignment, and no future rows |
| [`services/ml_pipeline/src/common/contracts.py`](../services/ml_pipeline/src/common/contracts.py) | Add a new immutable feature contract | Never reorder or redefine the deployed `v2_10s` entry |
| [`services/ml_pipeline/src/common/evaluation.py`](../services/ml_pipeline/src/common/evaluation.py) | Metrics and candidate/champion decision policy | Tests cover better, worse, equal, missing, and non-finite cases |
| [`services/ml_pipeline/src/common/benchmarks.py`](../services/ml_pipeline/src/common/benchmarks.py) | Research benchmarks | Existing EWMA remains available for comparison unless an approved policy change replaces it |
| [`services/ml_pipeline/containers/train/main.py`](../services/ml_pipeline/containers/train/main.py) | Training entrypoint and artifact metadata | Still writes `model.joblib` and `metadata.json`, or declares a versioned artifact migration |
| [`services/ml_pipeline/containers/evaluate/main.py`](../services/ml_pipeline/containers/evaluate/main.py) | Candidate evaluation report | Evaluation uses the held-out chronological test rows and cannot train on them |
| [`services/ml_pipeline/src/pipelines/pipeline_dag.py`](../services/ml_pipeline/src/pipelines/pipeline_dag.py) | Training stages or parameters | Compiled pipeline uses immutable commit-SHA component images |
| [`services/ml_pipeline/tests/`](../services/ml_pipeline/tests) | Model, leakage, metric, metadata, and artifact regression tests | All tests pass locally and in `ML Pipeline CI` |
| [`services/ml_pipeline/pyproject.toml`](../services/ml_pipeline/pyproject.toml) and `uv.lock` | New training dependency | Lockfile committed; serving dependency added separately if inference imports it |

Exploratory notebooks or scripts may be created under
`services/ml_pipeline/research/`, but production code must not import from that
directory. Record the feature version, date range, Git commit, seed, metrics,
benchmark, and artifact URI for every experiment worth comparing.

### Files involved in a feature change

A data scientist should define the formula, availability time, required
history, null behavior, and expected predictive value. A coding agent should
then implement the feature consistently across these owners:

| Layer | Files | Responsibility |
|---|---|---|
| Offline computation | `services/batch_etl/sql/<new-version>.sql`, [`scripts/run_bigquery_features.py`](../services/batch_etl/scripts/run_bigquery_features.py) | Write a new versioned BigQuery table using only information available at the feature event time |
| Labels, if changed | `services/batch_etl/sql/<new-target-version>.sql`, [`scripts/run_bigquery_targets.py`](../services/batch_etl/scripts/run_bigquery_targets.py) | Keep future information exclusively in labels; preserve `label_window_end` |
| Live computation | [`services/live_feature_service/src/live_feature_service/computation.py`](../services/live_feature_service/src/live_feature_service/computation.py), `service.py`, and tests | Produce the same formula from completed primitives, persist all state needed for restart, and publish versioned keys/streams |
| Feast contract | [`services/feast_store/registry/feature_specs.py`](../services/feast_store/registry/feature_specs.py), `definitions/data_sources.py`, and a new versioned definition module | Register the immutable field list, BigQuery source, PushSource, FeatureView, and FeatureService |
| Offline/live parity | [`services/feast_store/jobs/parity.py`](../services/feast_store/jobs/parity.py) and tests | Compare every model input, timestamps, missing rows, and Feast online lag |
| Training contract | [`services/ml_pipeline/src/common/contracts.py`](../services/ml_pipeline/src/common/contracts.py), `data_io.py`, and `modeling.py` | Select exactly the versioned fields in a stable order and record them in metadata |
| Serving contract | [`services/model_serving/scripts/prepare_bundle.py`](../services/model_serving/scripts/prepare_bundle.py), `packaged_provider.py`, and tests | Allow only the expected version and ordered columns; reject incompatible artifacts at build/startup |
| Pricing adapter | [`services/analytics/src/kalshi_crypto_analytics/schemas.py`](../services/analytics/src/kalshi_crypto_analytics/schemas.py), `forecast.py`, configuration, and tests | Forward and validate the new feature/model version without moving feature formulas into analytics |
| Deployment | `k8s/live-feature-service-deployment.yaml`, Feast/ parity manifests, model-serving and analytics manifests, [`.github/workflows/cd.yml`](../.github/workflows/cd.yml) | Deploy versioned names in the correct order and gate the switch on parity and smoke checks |

Do not modify `ingestion` or `aggregator` merely to make a downstream feature
easier. The new live feature implementation should consume the existing
`stream:primitives:v1` contract unless the research requirement truly needs a
new upstream measurement.

### Files usually outside the data scientist edit boundary

- `services/ingestion/**`: venue connections and normalized source events.
- `services/aggregator/**`: completed primitives, synthetic spot, and
  canonical per-venue book publication.
- `services/analytics/core.py` and `kalshi.py`: probability, pricing, and
  Kalshi market rules.
- `services/dashboard/**`: presentation only.
- `k8s/**` and `.github/workflows/**`: deployment mechanics. A data scientist
  supplies requirements; a coding agent changes these with rollout proof.
- `services/ml_pipeline/pipeline.yaml`: generated output. Change the DAG or
  compiler and regenerate it; do not hand-edit compiled KFP YAML.

## Production requirements for every learned model

A drop-in candidate for the current serving path must satisfy all of these:

1. Accept a pandas `DataFrame` with the exact ordered columns recorded in
   `metadata.json`.
2. Implement `predict(frame)` and return one scalar per row.
3. Serialize and load with `joblib` using dependencies available in both
   `ml_pipeline` and `model_serving`.
4. Produce finite, strictly positive annualized volatility in production.
5. Use no data newer than each row's feature availability time.
6. Preserve chronological train/validation/test splits. Never shuffle time
   series rows across these boundaries.
7. Record model family, parameters, random seed, feature contract, ordered
   columns, label version, horizon, row counts, date cutoff, and metrics.
8. Beat the required EWMA margin without exceeding the champion degradation
   limit on held-out data.
9. Load and smoke-predict inside the production model-serving image before the
   image is deployed.

Preprocessing needed at inference time must be inside the serialized estimator
(for example, an sklearn `Pipeline`) or inside a new versioned bundle contract.
Do not fit a scaler in a notebook and omit it from the artifact.

## Template: same-contract production trainer

Use this pattern when editing `services/ml_pipeline/src/common/modeling.py`.
Adapt it rather than creating a second unconnected training path.

```python
from __future__ import annotations

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

from src.common.contracts import resolve_contract


def build_candidate(seed: int) -> XGBRegressor:
    # Keep parameters explicit so metadata and reviews show what was trained.
    return XGBRegressor(
        n_estimators=750,
        max_depth=4,
        learning_rate=0.03,
        min_child_weight=10,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        eval_metric="rmse",
        random_state=seed,
        n_jobs=-1,
    )


def train_horizon(
    table: pd.DataFrame,
    horizon: str,
    seed: int = 42,
    *,
    feature_version: str = "v2_10s",
):
    contract = resolve_contract(feature_version)
    columns = list(contract.feature_columns)
    target = f"target_rv_{horizon}"
    required = ["timestamp", target, *columns]
    missing = [name for name in required if name not in table]
    if missing:
        raise ValueError(f"training table missing columns: {missing}")

    usable = (
        table.dropna(subset=[target, *columns])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if not usable["timestamp"].is_monotonic_increasing:
        raise ValueError("training rows must be chronological")

    row_count = len(usable)
    train_end = int(row_count * 0.70)
    validation_end = int(row_count * 0.85)
    if train_end < 2 or validation_end <= train_end or validation_end >= row_count:
        raise ValueError("insufficient rows for chronological split")

    x = usable[columns]
    y = usable[target].astype(float)
    model = build_candidate(seed)
    model.fit(
        x.iloc[:train_end],
        y.iloc[:train_end],
        eval_set=[(x.iloc[train_end:validation_end], y.iloc[train_end:validation_end])],
        verbose=False,
    )

    # Test rows are used for reporting, never fitting or hyperparameter choice.
    prediction = np.asarray(model.predict(x.iloc[validation_end:]), dtype=float)
    if prediction.shape != (row_count - validation_end,):
        raise ValueError("model returned an unexpected prediction shape")
    if not np.isfinite(prediction).all():
        raise ValueError("model returned a non-finite prediction")
    if (prediction <= 0).any():
        raise ValueError("candidate cannot produce non-positive volatility")

    actual = y.iloc[validation_end:].to_numpy()
    forecast_variance = np.maximum(prediction**2, 1e-18)
    actual_variance = np.maximum(actual**2, 1e-18)
    metadata = {
        "artifact_schema_version": 1,
        "model_family": "xgboost",
        "horizon": horizon,
        "target": target,
        "feature_set": contract.feature_set,
        "feature_version": contract.feature_version,
        "feature_view": contract.feature_view,
        "feature_service": contract.feature_service,
        "feature_columns": columns,
        "label_version": contract.label_version,
        "entity": "BTCUSD",
        "random_seed": seed,
        "git_sha": os.getenv("GITHUB_SHA", "local"),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "training_cutoff": table.attrs.get("training_cutoff"),
        "rows": {
            "total": row_count,
            "train": train_end,
            "validation": validation_end - train_end,
            "test": row_count - validation_end,
        },
        "metrics": {
            "qlike": float(np.mean(np.log(forecast_variance) + actual_variance / forecast_variance)),
            "rmse": float(np.sqrt(mean_squared_error(actual, prediction))),
            "mae": float(mean_absolute_error(actual, prediction)),
        },
    }
    return model, metadata
```

The repository's existing evaluator independently recomputes test metrics and
the EWMA comparison. Do not let the trainer write a hard-coded promotion flag.

## Template: model artifact regression tests

Add tests beside `services/ml_pipeline/tests/test_train.py`.

```python
import joblib
import numpy as np
import pandas as pd

from src.common.modeling import train_horizon


def training_frame(rows: int = 200) -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=rows, freq="10s", tz="UTC"),
        "log_return": np.sin(np.arange(rows) / 10_000),
        "venue_count": np.full(rows, 6),
        "target_rv_1h": np.linspace(0.30, 0.45, rows),
    })


def test_candidate_round_trips_with_exact_contract(tmp_path):
    model, metadata = train_horizon(
        training_frame(), "1h", seed=7, feature_version="v2_10s"
    )
    assert metadata["feature_columns"] == ["log_return", "venue_count"]
    assert metadata["feature_version"] == "v2_10s"
    assert metadata["rows"]["test"] > 0

    path = tmp_path / "model.joblib"
    joblib.dump(model, path)
    restored = joblib.load(path)
    frame = training_frame().tail(3)[metadata["feature_columns"]]
    prediction = np.asarray(restored.predict(frame), dtype=float)
    assert prediction.shape == (3,)
    assert np.isfinite(prediction).all()
    assert (prediction > 0).all()


def test_future_rows_do_not_enter_training_metadata():
    frame = training_frame()
    _, metadata = train_horizon(frame, "1h", feature_version="v2_10s")
    assert metadata["rows"]["train"] < metadata["rows"]["total"]
    assert metadata["rows"]["validation"] > 0
    assert metadata["rows"]["test"] > 0
```

Also retain tests for missing inputs, NaN/inf predictions, wrong column order,
too few rows, artifact load failure, and deterministic behavior for a fixed
seed.

## Template: a new versioned feature

The names below are illustrative. Replace `squared_return` with the researched
feature, but keep the version isolation.

### 1. Live formula

In the new version's live computer, keep feature math pure and explicit:

```python
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class V4FeatureValues:
    synthetic_price: float
    log_return: float
    venue_count: int
    squared_return: float


def compute_v4_values(*, current_price: float, previous_price: float, venue_count: int) -> V4FeatureValues:
    if not all(math.isfinite(value) and value > 0 for value in (current_price, previous_price)):
        raise ValueError("prices must be positive and finite")
    if venue_count < 1:
        raise ValueError("venue_count must be positive")
    log_return = math.log(current_price / previous_price)
    return V4FeatureValues(
        synthetic_price=current_price,
        log_return=log_return,
        venue_count=venue_count,
        squared_return=log_return * log_return,
    )
```

The stateful service must additionally snapshot and restore every lag/window
needed by the formula, publish an `available_timestamp_ms`, and atomically
commit output, state, and source acknowledgement as the current service does.

### 2. Matching offline formula

Create a new table rather than altering `realized_volatility_v2_10s`:

```sql
CREATE TABLE IF NOT EXISTS `${project}.feature_store.market_features_v4_10s`
(
  asset STRING NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  created_timestamp TIMESTAMP NOT NULL,
  feature_version STRING NOT NULL,
  synthetic_price FLOAT64,
  log_return FLOAT64,
  venue_count INT64,
  squared_return FLOAT64
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY asset, feature_version;

-- Build synthetic_price and log_return from completed 10-second bars exactly
-- as the live computer does. Only then derive:
SELECT
  'BTCUSD' AS asset,
  event_timestamp,
  CURRENT_TIMESTAMP() AS created_timestamp,
  'v4_10s' AS feature_version,
  synthetic_price,
  log_return,
  venue_count,
  POW(log_return, 2) AS squared_return
FROM causal_returns;
```

Use an idempotent `MERGE` in the real implementation. Window frames must end at
the current event timestamp, never in the future.

### 3. Feast and training contracts

Add, do not replace, version entries:

```python
# services/feast_store/registry/feature_specs.py
FEATURE_REGISTRY = {
    # Keep the existing v1, v2_10s, and v3_10s entries here too.
    ("market_features", "v4_10s"): FeatureSpec(
        feature_set="market_features",
        version="v4_10s",
        feature_view="v4_10s_market_features",
        push_source="v4_10s_market_features_push",
        fields=("synthetic_price", "log_return", "venue_count", "squared_return"),
        required_fields=("synthetic_price", "venue_count", "squared_return"),
    ),
}

# services/ml_pipeline/src/common/contracts.py
CONTRACTS = {
    # Keep the existing v1, v2_10s, and v3_10s entries here too.
    "v4_10s": ModelFeatureContract(
        feature_set="market_features",
        feature_version="v4_10s",
        feature_view="v4_10s_market_features",
        feature_service="volatility_v4_10s",
        offline_table="kalshi-crypto-506614.feature_store.market_features_v4_10s",
        label_version="v2_10s",  # Change only if the target definition changed.
        feature_columns=("log_return", "venue_count", "squared_return"),
    ),
}
```

The BigQuery loader must explicitly select `squared_return`; the Feast
FeatureView must declare it as `Float64`; parity must compare it; bundle
validation must require the exact ordered list. A contract entry alone does not
make the feature production-ready.

## Workflow: model architecture changes with unchanged features

1. State the hypothesis, horizon, baseline, primary metric, secondary metrics,
   date range, and minimum acceptable improvement.
2. Keep `market_features/v2_10s`, its ordered columns, the labels, and the
   annualized-volatility output unchanged.
3. Modify the model builder/trainer and add serialization and held-out tests.
4. If adding a library, update both training and inference dependencies when
   the loaded object imports that library. Regenerate and commit each `uv.lock`.
5. Run the local gates:

   ```bash
   cd services/ml_pipeline
   uv sync --locked
   uv run --locked pytest
   uv run --locked python scripts/compile_pipeline.py \
     --output /tmp/volatility-pipeline.yaml --image-tag <git-sha>

   cd ../model_serving
   uv sync --locked
   uv run --locked pytest
   ```

6. Open a PR with the candidate metrics and explicit statement that production
   is unchanged. Merge only after review.
7. `ML Pipeline CI` publishes commit-SHA task images and a compiled Vertex
   template. It does not launch training.
8. Submit the immutable template with a date range ending no later than
   yesterday UTC. Record the Vertex Pipeline resource name. The current DAG
   conditionally registers candidates that pass its gate; this is still not a
   live deployment because it does not update the release variables.
9. Review the held-out report, EWMA comparison, champion comparison, row counts,
   and exact 1h `model.joblib`/`metadata.json` artifact.
10. Promotion is a separate authorized action. Set the exact versioned Vertex
    resource and matching immutable GCS artifact URI in GitHub, then dispatch
    `Deploy Services`.
11. Verify model-serving readiness, analytics readiness, the exact live 1h
    resource, fresh volatility/pricing keys, restart counts, and latency/errors.

If the new architecture is not joblib-compatible or does not expose
`predict(DataFrame)`, stop treating it as a drop-in model. Version the bundle
schema and change the training container, model-serving loader/runtime,
dependency locks, Docker build, preparation script, and tests together. Keep
the HTTP forecast response unchanged when possible so analytics need not
change.

## Workflow: feature-set changes

Use this release order. Do not switch analytics immediately after writing a
feature formula.

1. **Specify:** choose a new version; document formula, units, null policy,
   history length, availability timestamp, ordered model columns, source
   fields, and leakage argument.
2. **Offline first:** create the new BigQuery table and idempotent SQL; dry-run,
   backfill a bounded research range, and inspect coverage/distributions.
3. **Live shadow:** deploy a new stream, latest key, state key, and consumer
   group while leaving `v2_10s` untouched. Consume the current primitives; do
   not require aggregator schema changes without evidence.
4. **Register:** add the versioned Feast source/view/service/spec and apply the
   registry only after offline schema exists.
5. **Prove parity:** compare all fields and event timestamps over enough rows;
   test restart recovery, late/out-of-order data, NaN/inf/nulls, and warmup.
6. **Train:** add the ML contract and loader fields, train candidates on the new
   version, and compare to both EWMA and the current production champion.
7. **Package:** update allowlisted serving contracts and bundle validation;
   smoke-load the exact artifact in the production image.
8. **Switch consumers:** deploy model-serving, then analytics configuration,
   with readiness and fresh-output gates. Dashboard should not need a change if
   pricing output remains `stream:pricing:v1`/`market:pricing:v1:*`.
9. **Observe:** keep both versions available through a defined observation
   window. Roll back by switching exact version/resource configuration, not by
   deleting data or models.
10. **Retire later:** remove the old version only in a separate reviewed change
    after rollback is no longer required.

## Deploying an approved 1h model

The two values below must describe the same immutable artifact:

- `VOLATILITY_MODEL_1H`: exact Vertex resource including a version suffix such
  as `@3`.
- `VOLATILITY_MODEL_1H_ARTIFACT_URI`: immutable GCS directory containing
  `model.joblib` and `metadata.json`.

After explicit production authorization, the operator or coding agent updates
both GitHub variables and manually dispatches `.github/workflows/cd.yml` when
there is no new code commit. CD downloads the artifact, verifies metadata and
checksums, packages it into the image, rolls out GKE, runs feature parity, and
smoke-checks live analytics output.

A concrete model-only release looks like this; replace every placeholder with
an observed immutable value:

```bash
# 1. Find and download the compiled template produced for the merged SHA.
gh run list --workflow "ML Pipeline CI" --commit <MERGED_SHA>
gh run download <ML_CI_RUN_ID> \
  --name vertex-pipeline-<MERGED_SHA> \
  --dir /tmp/vertex-pipeline-<MERGED_SHA>

# 2. Submit training. This may conditionally register passing candidates but
#    does not deploy them to the pricing system.
cd services/ml_pipeline
uv run --locked python scripts/run_pipeline.py \
  --template /tmp/vertex-pipeline-<MERGED_SHA>/pipeline-<MERGED_SHA>.yaml \
  --project kalshi-crypto-506614 \
  --location asia-northeast3 \
  --pipeline-root gs://kalshi-crypto-tick-data/pipeline-root \
  --start-date <YYYY-MM-DD> \
  --end-date <YYYY-MM-DD> \
  --model-version <MODEL_VERSION> \
  --feature-version <FEATURE_VERSION> \
  --service-account <VERTEX_PIPELINE_SERVICE_ACCOUNT>

# 3. Only after approval: record rollback values, update the matching pair,
#    and release the approved 1h model.
gh variable get VOLATILITY_MODEL_1H
gh variable get VOLATILITY_MODEL_1H_ARTIFACT_URI
gh variable set VOLATILITY_MODEL_1H --body '<PROJECTS/.../MODELS/...@N>'
gh variable set VOLATILITY_MODEL_1H_ARTIFACT_URI --body '<GS://IMMUTABLE/DIRECTORY>'
gh workflow run cd.yml --ref main
```

Monitor the resulting workflow and verify the live resource rather than
assuming dispatch means success:

```bash
gh run list --workflow "Deploy Services" --limit 5
gh run watch <DEPLOY_RUN_ID> --exit-status
kubectl rollout status deployment/model-serving --timeout=5m
kubectl rollout status deployment/analytics --timeout=5m
```

Never update only one of the two variables. Record the previous pair before
changing them; rollback restores that pair and reruns the same deployment.

## What monitoring exists and what does not

Current production safeguards:

- Kubernetes startup/readiness/liveness probes.
- Offline/live/Feast parity every 15 minutes.
- Immediate parity and analytics smoke gates during deployment.
- Freshness, version, positivity, complete-horizon, and exact-resource checks.
- Service logs and Kubernetes restart/event history.

The repository contains
[`services/ml_pipeline/scripts/evaluate_and_trigger.py`](../services/ml_pipeline/scripts/evaluate_and_trigger.py)
for delayed-label evaluation and retraining decisions, but it is not currently
wired to a durable scheduled production monitor. Application-level model
latency, feature distributions, and prediction-distribution metrics are also
not yet a complete alerting system. Do not tell an agent to “check monitoring”
and assume these exist.

A useful monitoring implementation should separately measure:

- **Feature operations:** latest-key age, stream lag/pending count, rejected
  rows, warmup/readiness, missing fields, NaN/inf, and parity mismatch by field.
- **Feature statistics:** missing rate, quantiles, mean/std, out-of-range count,
  venue-count distribution, and comparison with the training reference window.
- **Inference operations:** request count, 422/5xx rate, latency percentiles,
  model resource/version, pod restarts, and stale/incomplete outputs.
- **Model behavior:** forecast quantiles by horizon, zero/clipped rate, abrupt
  changes, curve inversions or kinks, and EWMA disagreement.
- **Delayed quality:** QLIKE/RMSE/MAE after labels mature, compared with EWMA and
  champion over rolling windows. Retraining may be suggested after consecutive
  failures; promotion must remain gated.

Every alert needs a numeric threshold, evaluation window, consecutive-failure
rule, destination, runbook, and false-positive policy.

## How to ask a coding agent explicitly

Good MLOps prompts contain seven things:

1. **Outcome:** research report, code change, training run, promotion, deploy,
   monitoring, or rollback.
2. **Exact version:** current and proposed feature/model versions.
3. **Scope:** files/services the agent may change and boundaries it must keep.
4. **Invariants:** point-in-time safety, immutable names, ordered columns,
   output units/shape, no upstream schema forcing, and fail-closed behavior.
5. **Authority:** whether it may commit, push, submit Vertex jobs, change GitHub
   variables, deploy GKE, or only inspect.
6. **Proof:** tests, parity, metrics, smoke checks, and observation duration.
7. **Stop conditions:** missing data/IAM, failed gates, worse metrics, contract
   ambiguity, or any need to broaden scope.

### Prompt: implement a same-contract candidate, no deployment

```text
Implement and evaluate <MODEL IDEA> for the 1h volatility horizon.

Keep the production input/output contract unchanged:
- feature contract market_features/v2_10s
- ordered inputs [log_return, venue_count]
- target target_rv_1h
- output is one finite, positive annualized-volatility value per row
- chronological 70/15/15 split; no shuffled or future data

Work only in services/ml_pipeline plus model_serving tests if needed for
serialization compatibility. Preserve EWMA and the current champion. Add tests
for deterministic training, missing/non-finite data, joblib round-trip, exact
column order, and held-out metrics. Run the ML tests and compile the pipeline
with an immutable test image tag.

Do not submit Vertex jobs, register/promote a model, change GitHub variables,
push, or deploy. Report changed files, candidate vs EWMA/champion metrics, risks,
and the exact next command that would submit training. Stop if the idea requires
a feature-contract or artifact-format change and explain the larger scope.
```

### Prompt: implement a versioned live feature safely

```text
Implement a new immutable feature contract market_features/<NEW_VERSION> with
these ordered model fields: <FIELDS>. The feature formulas, units, required
history, null behavior, and availability rules are: <SPECIFICATION>.

Do not edit v2_10s in place. Keep ingestion and aggregator contracts unchanged;
consume stream:primitives:v1. Implement matching offline BigQuery SQL and live
computation, new versioned Redis stream/latest/state/group names, Feast
source/view/service/spec, ML contract/loading, and field-by-field parity. Keep
v2_10s running as rollback. Do not switch analytics or production model-serving
yet.

Add tests for causal computation, offline/live formula equivalence, timestamp
alignment, restart state, duplicate/out-of-order input, warmup, null/NaN/inf,
and parity failures. Run all affected service tests and BigQuery dry-run. Give
me a migration plan with deployment order and rollback before any external
write. You may edit local files only; do not push, backfill production, apply
Feast, change GitHub variables, or deploy GKE without a second explicit request.
```

### Prompt: submit and evaluate training, but do not deploy

```text
Using the already merged commit <SHA> and feature contract <VERSION>, locate the
immutable ML Pipeline CI template, submit one Vertex pipeline for UTC dates
<START> through <END>, and monitor it to completion. The end date must satisfy
the label-maturity cutoff. This authorizes creation of the Vertex pipeline run,
candidate artifacts, and the pipeline's existing conditional Vertex
registration for candidates that pass its gate.

Do not modify source, GitHub variables, the champion metrics file, or GKE. A
registered candidate must not be treated as production approval. Report the
pipeline resource, any registered Vertex resources, exact artifact URIs, row
counts/splits, QLIKE/RMSE/MAE by horizon, EWMA comparison, champion comparison,
and gate result. Stop on missing data, IAM, or contract mismatch rather than
changing another service. If I prohibit even conditional registration, do not
submit the current DAG; run or implement an evaluation-only path first.
```

### Prompt: promote and deploy an approved model

```text
Promote and deploy this already approved 1h model:
- Vertex versioned resource: <PROJECTS/.../MODELS/...@N>
- immutable artifact directory: <GS://...>
- feature/model version: <VERSION>
- approved evaluation report: <URI OR RUN ID>

Confirm the resource and artifact metadata match before changing anything.
Record the current GitHub variable pair for rollback. This explicitly
authorizes updating VOLATILITY_MODEL_1H and
VOLATILITY_MODEL_1H_ARTIFACT_URI, dispatching Deploy Services from main, and
monitoring the GKE rollout. Do not change service APIs or feature formulas.

Require model bundle validation, all CI/staging gates, feature parity,
model-serving and analytics readiness, the exact live model resource, fresh
positive four-horizon volatility, at least one fresh Kalshi pricing key, and
zero new crash/OOM loops. If a gate fails, diagnose and report; do not bypass it.
Rollback to the recorded variable pair if the new artifact causes a confirmed
production regression. Report final live image SHA, resource, rollout status,
and rollback state.
```

### Prompt: implement model monitoring without automatic promotion

```text
Implement production monitoring for model version <VERSION>. Start by auditing
what telemetry already exists; do not invent metrics that are not emitted.

Add a scheduled delayed-label evaluation using the existing ML evaluation
functions. Persist reports and consecutive-failure state durably in a versioned
GCS path. Measure rolling QLIKE/RMSE/MAE by horizon against EWMA and champion,
plus feature freshness/missingness, parity status, prediction quantiles,
inference latency/errors, exact resource/version, and pod restarts.

Use these thresholds/windows: <NUMERIC THRESHOLDS>. Alerts go to <DESTINATION>
with a runbook link. Retraining may be recommended only after <N> consecutive
failures and a <HOURS> cooldown. Do not automatically promote, alter GitHub
model variables, or deploy a candidate.

Add unit tests for thresholds, delayed labels, idempotency, persisted state,
missing data, and alert recovery. Add Kubernetes/workflow resources only where
the current architecture requires them. Run focused tests and a non-alerting
staging execution, then report sample output, cost/resource impact, and the
commands for an authorized production enablement. Do not enable production
notifications or schedules without my explicit approval.
```

### Prompt: read-only live MLOps audit

```text
Perform a read-only production audit for the currently deployed volatility
model. Do not edit files, restart/scale pods, submit jobs, change variables, or
deploy.

Report with UTC sample times:
- live model-serving image, model version, and exact 1h Vertex resource
- readiness, restarts, CPU/memory, and recent warning events
- feature latest-key age, stream consumer lag/pending count, and 15-minute
  parity results
- forecast freshness/positivity/completeness and analytics unavailable reasons
- latest delayed-label QLIKE/RMSE/MAE report if one actually exists
- differences from the documented production contract

Separate observed facts from inference. If metrics do not exist, say so and
identify the narrowest instrumentation needed; do not implement it in this
task.
```

## Review checklist before handing work to production

- [ ] Experiment identifies Git SHA, data dates, feature version, label version,
  seed, and exact artifact URI.
- [ ] No future or availability-late data enters a feature row.
- [ ] Train/validation/test are chronological and test data did not influence
  tuning.
- [ ] Ordered training columns exactly match bundle and serving validation.
- [ ] Model and preprocessing round-trip in the production inference runtime.
- [ ] Predictions are finite, positive, correctly annualized, and one per row.
- [ ] Candidate is compared with EWMA and champion on held-out data.
- [ ] New features have offline/live/Feast parity and versioned storage names.
- [ ] Existing contracts remain available for rollback.
- [ ] Promotion and deployment were separately authorized.
- [ ] Exact previous resource/artifact pair is recorded for rollback.
- [ ] Staging, production rollouts, parity, and analytics smoke checks pass.
- [ ] Live resource/version, freshness, restarts, and error logs are verified.

When any box is unresolved, the correct production result is “not ready,” not a
silent fallback or a bypassed gate.
