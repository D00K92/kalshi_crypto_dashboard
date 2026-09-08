# Live ML pricing implementation TODO

This is the execution handoff for getting ML volatility predictions into live
Kalshi prices on the dashboard. It is based on `ARCHITECTURE.md`, the repository
at commit `422c366`, and read-only checks of the GKE deployment and BigQuery backfill on 2026-09-07.

Follow the tasks in the stated order. Do not redesign service contracts and do
not make upstream services change their payloads to accommodate a consumer.
Redis remains the real-time integration boundary.

## What “done” means

The work is complete only when all of these conditions are true at the same
time:

- GitHub discovers and runs CI, staging integration, and deployment workflows.
- `ingestion-service`, `aggregator`, `analytics`, and `dashboard` are Ready in
  GKE.
- `market:spot:BTCUSDT:latest` and
  `market:features:v1:BTCUSD:latest` are fresh.
- `market:volatility:v1:BTCUSD:latest` exists, has all five horizons, and is
  less than 60 seconds old.
- `market:pricing:v1:active` contains at least one currently supported KXBTCD
  market and its `market:pricing:v1:<ticker>` record is fresh.
- The dashboard displays numeric model value and edge columns instead of `-`
  for at least one active contract.
- The latest CI, staging integration, and deployment runs all pass.

Offline feature/label jobs and automated retraining are required for a
sustainable system, but they are not required to prove the already-registered
bootstrap models can produce live prices. Complete Phase A first.

## Current verified state

| Component | Current state | Blocking live display? |
|---|---|---|
| GitHub Actions | Workflows were moved to `docs/.github/workflows`; GitHub does not discover workflows there. Commit `422c366` triggered no CI run. | **Yes, for normal deployment** |
| `ingestion` | GKE Ready `1/1`; crypto and Kalshi streams are populated and advancing. | No |
| `aggregator` | GKE Ready `1/1`; spot and v1 feature records are fresh. | No |
| Aggregator staging test | Test expects legacy candle/CVD keys, while the service publishes `30s` candles. | **Yes, CI failure** |
| `analytics` | Running but Ready `0/1`; `/healthz` is 200 and `/readyz` is 503. | **Yes, immediate blocker** |
| Model registry | Five exact Vertex resources exist; the 1m bundle and metadata were verified in GCS. | No |
| Live model inference | All pricing status records say `model_inference_failed`; no volatility snapshot or active price exists. | **Yes** |
| `dashboard` | GKE Ready `1/1`; code already joins analytics keys by Kalshi ticker. | Waiting on analytics output |
| `gcs_exporter` | GKE Ready `1/1`. | No |
| `batch_etl` raw job | Production CronJob resumed with canonical GCS → BigQuery landing → `market_data.raw_*` loading; target-hour replacement is partition-safe. | No |
| `batch_etl` feature/label jobs | Feature and target MERGE jobs succeed; same-hour idempotency verified. Repaired window contains 15,060 feature/target rows, with 14,720 complete target rows. | No |
| Feast live bridge/server | Bridge Ready `1/1`; server Ready `2/2`. Analytics intentionally uses the direct Redis feature key. | No |
| `ml_pipeline` | Tests pass and bootstrap models are registered, but there is no complete automated image-build/run/promotion release path. | No for Phase A; yes for lifecycle |

Backfill verification: `market_data.bars` contains unique repaired rows for 1m (67,800), 5m (13,560), 15m (4,520), 30m (2,260), and 1h (1,130) over the available venue-hour intersection. `backfill_raw` was temporary staging and is no longer part of the production dependency chain.

The exact analytics failure was reproduced inside the running pod. Aggregator
publishes `log_return` as a JSON string, so pandas infers an `object` column.
XGBoost rejects it:

```text
ValueError: DataFrame.dtypes for data must be int, float, bool or category.
Invalid columns: log_return: object
```

Casting the ordered model inputs to `float` allowed all five deployed models to
predict successfully. The observed predictions were positive and had a valid
total-variance term structure. Do not change the aggregator schema to fix this;
analytics owns input adaptation.

## Execution order

### Phase A — show live ML prices now

~~1. Restore GitHub workflow discovery.~~

~~2. Fix analytics numeric feature adaptation and add a regression test.~~

~~3. Fix the aggregator staging integration test’s legacy candle-frequency mismatch.~~

~~4. Run local service tests and container builds.~~
~~5. Commit and push Phase A changes.~~
~~6. Watch CI, staging integration, and deployment through completion.~~
~~7. Verify live Redis outputs, analytics readiness, and dashboard rendering.~~

### Phase B — make model training sustainable

~~8. Repair BigQuery write authorization for the batch feature and target jobs.~~
~~9. Prove hourly feature/label generation and close any data gaps.~~
~~10. Finish Feast operational jobs and tests needed by historical retrieval.~~
11. Build, publish, and execute the ML pipeline; promote immutable models.
12. Update analytics model resource variables and repeat Phase A verification.

## Repository and CI/CD TODO

### P0. Restore workflows to GitHub’s executable location

- [x] Move `docs/.github` back to the repository root as `.github`:

  ```bash
  git mv docs/.github .github
  ```

- [x] Confirm these files exist:

  ```text
  .github/workflows/ci.yml
  .github/workflows/integration.yml
  .github/workflows/cd.yml
  ```

- [x] Confirm no workflow remains under `docs/.github`.
- [x] Confirm all renamed paths use `services/aggregator` and
  `k8s/aggregator-deployment.yaml`.
- [x] Keep the workflow trigger chain `CI -> Staging Integration / Deploy
  Services`; do not deploy a commit whose CI failed.

Acceptance:

```bash
test -f .github/workflows/ci.yml
test -f .github/workflows/integration.yml
test -f .github/workflows/cd.yml
rg 'services/market_aggregator|market-aggregator-deployment' .github
```

The final `rg` command must return no matches. After pushing, `gh run list`
must show a CI run for the new commit.

### P1. Expand CI coverage after Phase A is green

- [ ] Add `services/feast_store` and `services/ml_pipeline` to Python test
  coverage. They are currently absent from the CI matrix.
- [ ] Add validation that required workflow files are under
  `.github/workflows`, not a documentation directory.
- [ ] Resolve the root dependency conflict before requiring root `uv lock` in
  CI: `gcs_exporter` requires `google-cloud-storage >=3.4,<4`, while
  `ml_pipeline` requires `>=2.10,<3`. Prefer aligning the ML pipeline to the
  supported 3.x range after its tests pass; do not weaken both ranges blindly.

## `ingestion` TODO

Current status: live inputs are healthy. `stream:ticks`,
`stream:orderbook_snapshots`, and `stream:kalshi_tickers` contain data. No code
change is required for Phase A.

### P0. Verify rather than modify

- [x] Confirm `deployment/ingestion-service` is Ready.
- [x] Confirm recent crypto events and KXBTCD ticker events exist in Redis.
- [x] Confirm the Kalshi discovery/subscription logs do not show repeated 401,
  discovery, or connection-loop errors.
- [x] Do not add Bybit or change normalized event schemas during this task.

Acceptance:

- `stream:ticks` and `stream:kalshi_tickers` lengths increase across two checks.
- The newest ticker’s exchange timestamp is less than 60 seconds old.
- Analytics must adapt to the existing ticker schema.

## `aggregator` TODO

Current status: live service is healthy and publishes the exact spot/feature
inputs analytics needs.

### P0. Fix the staging integration contract

File: `services/aggregator/scripts/integration_test.py`

- [x] Change only these expected keys:

  ```python
  f"{prefix}:candles:BTCUSDT:30s"
  f"{prefix}:cvd:BTCUSDT:30s"
  ```

  The current assertions incorrectly expect legacy keys.

- [x] Keep the production implementation at 30-second candles. Do not create
  duplicate 5-second keys merely to satisfy the stale test.
- [x] Keep `values.log_return` and other numeric JSON representation compatible
  with the current contract. Analytics will coerce model inputs.
- [x] Preserve existing Redis consumer-group IDs during deployment; changing a
  group can replay historical streams.

Acceptance:

```bash
PYTHONPATH=services/aggregator/src .venv/bin/pytest -q services/aggregator/tests
docker build --platform linux/amd64 -t aggregator:local services/aggregator
```

The staging integration job must print `aggregator integration test passed`.

## `analytics` TODO

Current status: model input adaptation is fixed and all five models produce volatility.
The remaining deployment issue was Kalshi expiry parsing: the service selected the
administrative `expiration_time` instead of `expected_expiration_time`, making active
hourly contracts appear outside the supported lifetime; this is now fixed and tested.

### P0. Coerce model inputs at the consumer boundary

File: `services/analytics/src/kalshi_crypto_analytics/forecast.py`

- [x] In `ConfiguredForecastProvider.forecast`, retain metadata-defined feature
  ordering and convert every value to a finite float before constructing the
  DataFrame. A suitable implementation shape is:

  ```python
  row = [float(observation.values[column]) for column in columns]
  if not all(math.isfinite(value) for value in row):
      raise ValueError("non-finite feature")
  frame = pd.DataFrame([row], columns=columns, dtype=float)
  prediction = bundle.model.predict(frame)
  ```

- [x] Do not sort feature names. `metadata.json.feature_columns` defines the
  model input order.
- [x] Continue rejecting missing, nonnumeric, NaN, infinite, zero, or negative
  predictions as `model_inference_failed`.
- [x] Do not introduce fallback volatility. Unavailable inputs must remain
  unavailable.

### P0. Add regression and diagnostic coverage

File: `services/analytics/tests/test_forecast.py`

- [x] Add a test using the real live representation:

  ```python
  FeatureObservation({"log_return": "0", "venue_count": 6}, timestamp)
  ```

- [x] Make the fake model assert that both DataFrame columns have numeric
  dtypes. The test must fail on the current implementation and pass after the
  cast.
- [x] Retain the existing feature-order and atomic-five-model tests.
- [x] Log the exception detail when inference fails, including the horizon but
  not full payloads or credentials. Currently Redis records only the broad
  reason, making production diagnosis unnecessarily difficult.

Acceptance:

```bash
PYTHONPATH=services/analytics/src .venv/bin/pytest -q services/analytics/tests
docker build --platform linux/amd64 -t analytics:local services/analytics
```

After deployment:

- `/healthz` returns 200.
- `/readyz` returns 200.
- `market:volatility:v1:BTCUSD:latest` contains exactly `1m`, `5m`, `15m`,
  `30m`, and `1h` positive values.
- `market:pricing:v1:active` is nonempty while a supported contract with no more
  than one hour to expiry is open.
- No fresh status record reports `model_inference_failed`.

### P1. Make model compatibility explicit

- [ ] Add artifact metadata fields for training Python, XGBoost, pandas, and
  scikit-learn versions.
- [ ] Validate supported major versions during `load()` and emit a precise
  startup error.
- [ ] Add a fixture containing a real serialized training bundle to the
  analytics container compatibility test, or run a cross-image contract test
  in CI.

## `dashboard` TODO

Current status: the Redis join and display fields already exist. The dashboard
shows `-` because analytics has published no available pricing records.

### P0. Verify the completed consumer path

- [x] Do not calculate volatility or probabilities in the dashboard.
- [x] Keep joining `market:pricing:v1:<market_ticker>` by exact ticker.
- [x] Confirm records older than 60 seconds still render as unavailable.
- [x] After analytics is Ready, open the dashboard and verify at least one row
  displays `model_value`, `edge_mid`, `buy_yes_edge`, and `sell_yes_edge`.
- [x] Capture one screenshot or a short verification note with the ticker,
  pricing timestamp, and model version. Do not treat Redis connectivity alone
  as proof that pricing works.

Acceptance:

```bash
PYTHONPATH=services/dashboard/src .venv/bin/pytest -q services/dashboard/tests
kubectl port-forward service/dashboard 8050:8050
```

Open `http://127.0.0.1:8050` and inspect an active KXBTCD row.

### P1. Improve operator diagnosis

- [ ] When a pricing key is absent, optionally read the matching
  `market:pricing:v1:status:<ticker>` record and display its reason in a tooltip
  or status column. Keep this diagnostic-only; do not transform producer data.

## `gcs_exporter` TODO

Current status: Ready and not on the immediate pricing path.

### P1. Prove offline continuity

- [ ] Verify recent crypto and Kalshi Parquet objects continue arriving for all
  enabled stream types.
- [x] Check exporter consumer-group pending counts and dead-letter growth.
- [x] Do not block Phase A on historical export if live Redis inputs are fresh.

Acceptance: recent GCS objects exist, exporter remains Ready, and pending
entries do not grow continuously.

Observed 2026-09-06: exporter is Ready `1/1` and recent tick, book, and
Kalshi Parquet partitions exist. However, pending entries grew over 15 seconds:
`stream:ticks` 1,194 -> 1,915; `stream:orderbook_snapshots` 1,984 -> 4,339;
`stream:kalshi_tickers` 49 -> 219; `stream:kalshi_trades` 172 -> 279; and
`stream:kalshi_orderbook` 3,747 -> 7,910. The dead-letter prefix was empty, but
backlog growth means the continuity acceptance condition is not met; investigate
exporter throughput or upstream stream volume before marking this TODO complete.

## `batch_etl` TODO

Current status: raw hourly loading succeeds, but feature and target CronJobs
fail. Cloud Logging shows the authenticated principal is
`serviceAccount:kalshi-crypto-506614.svc.id.goog[default/batch-etl]`.

Observed failures:

```text
feature_store.realized_volatility_v1:
Permission bigquery.tables.updateData denied

training_labels.future_realized_volatility_v1:
Permission bigquery.tables.updateData denied
```

### P1. Repair effective BigQuery write authorization

- [x] Inspect the effective IAM policy for the exact principal above.
- [x] The project policy was inspected: `roles/bigquery.dataEditor` was conditional
  to `market_data`, so duplicate unconditional project roles were not added.
- [x] Grant least-privilege write access scoped to `market_data`, `feature_store`,
  and `training_labels` for the same KSA principal; project-level
  `roles/bigquery.jobUser` was preserved.
- [x] Run one manual feature job and one manual target job from their CronJobs.
- [x] Query the written target hours and confirm rerunning the same hour is
  idempotent through `MERGE` (2026-09-06 14:00 UTC; counts and hashes stable).
- [x] Confirm the next scheduled executions complete without retries.

Acceptance:

- Latest `batch-etl-features-*` and `batch-etl-targets-*` Jobs are `Complete`.
- BigQuery contains recent rows in both output tables.
- No new `bigquery.tables.updateData denied` message appears.

### P1. Validate model-facing data

- [x] Verify `log_return` and `venue_count` are non-null for enough 1-minute
  rows to train each horizon (14,884 usable rows in the repaired window).
- [x] Verify all five target columns have usable rows through the chosen
  training cutoff.
- [x] Record row counts, minimum timestamp, maximum timestamp, and null counts
  before starting an ML pipeline run.

**DATA QUALITY STATUS - REPAIRED BACKFILL:** The canonical raw tables and 1-minute bars were rebuilt from intact GCS staging data. The repaired feature window contains 15,060 rows, with 14,884 rows having non-null `log_return` and positive `venue_count`; 14,720 target rows have all five horizons populated. Venue-specific source gaps and boundary nulls remain expected and are now monitored rather than treated as the former failed backfill state.

**SINGLE-HOUR TRACE (2026-09-06 11:00-11:59 UTC):** 358 tick Parquet objects and
360 book Parquet objects (60 per crypto venue) were present in GCS. The same
hour contained 72,298 raw trade rows, 4,009,840 raw book-level rows, 360
resampled 1-minute bar rows (6 venues x 60 minutes), and 60 feature rows.
This recent interval is complete end-to-end; the observed historical loss is
therefore a backfill or resampling-history problem, not a live-path loss for
this hour.

## `feast_store` TODO

Current status: the live bridge and server are healthy. Analytics intentionally
reads `market:features:v1:BTCUSD:latest` directly, so Feast is not a Phase A
dependency.

### P1. Prove historical retrieval used by training

- [x] Run `feast apply` through the existing apply job and verify the registry
  at `gs://kalshi-crypto-tick-data/feature_store/registry.db` is current.
- [x] Execute a bounded point-in-time historical retrieval for several known
  label timestamps and confirm no future feature enters a row.
- [x] Confirm returned columns include `synthetic_price`, `log_return`, and
  `venue_count` with numeric dtypes.

### P2. Remove placeholder operational code

These files still raise `NotImplementedError`:

- `services/feast_store/src/feast_repo/config.py`
- `services/feast_store/jobs/backfill.py`
- `services/feast_store/jobs/materialize.py`

- [x] Either implement each supported command with bounded, resumable behavior
  and tests, or delete it if the declarative apply/live-push path supersedes it.
  Do not leave callable placeholder entrypoints.
- [x] Replace the placeholder contract test with assertions against the actual
  entity, FeatureView, FeatureService, and source definitions.

## `ml_pipeline` TODO

Current status: local tests pass and the five bootstrap Vertex models are enough
for Phase A. The repeatable production training/release path is incomplete.

### P1. Build and publish the four KFP images

- [x] Add reproducible build steps for `ml-load`, `ml-train`, `ml-evaluate`, and
  `ml-register` from `services/ml_pipeline/containers`.
- [x] Stop relying on mutable `:v1` alone. Publish immutable commit-SHA tags and
  pass those image references into pipeline compilation.
- [x] Add all four image builds and ML tests to CI without coupling deployment
  of unrelated live services to model promotion.

### P1. Run the end-to-end training contract

- [x] After batch tables are healthy, load a bounded completed date range using
  Feast point-in-time retrieval.
- [x] Assert the assembled table has `log_return`, `venue_count`, and all five
  target columns before training.
- [ ] Train and evaluate every horizon against EWMA and the current champion.
- [ ] Upload each promoted bundle to a unique immutable GCS prefix containing
  both `model.joblib` and `metadata.json`.
- [ ] Register the exact GCS prefix in Vertex with `horizon`, `version`, and
  `stage` labels.
- [ ] Save a promotion report mapping each horizon to its exact Vertex resource
  name. Never configure analytics with a display name or a floating `latest`
  alias.

### P1. Close release-path gaps

- [ ] Update the five GitHub repository variables only after all five promoted
  resources pass an analytics compatibility smoke test.
- [ ] Deploy all five resources atomically. Do not mix horizons from different
  model versions unless an explicit tested manifest records that combination.
- [ ] Preserve the previous five-resource set for rollback.

Acceptance:

- A Vertex pipeline run completes for all five horizons.
- Promotion output is auditable and points to immutable artifacts.
- A disposable analytics container loads those exact resources and predicts on
  `{"log_return": "0", "venue_count": 6}` before production rollout.

## Final deployment and live verification

Perform this only after the Phase A code changes pass locally.

- [x] Commit by notable component so failures are easy to locate:

  1. `fix(ci): restore workflows and align aggregator integration contract`
  2. `fix(analytics): coerce live model features to numeric inputs`

- [x] Push to `main` and watch the new runs:

  ```bash
  gh run list --limit 10
  gh run watch <ci-run-id>
  gh run watch <integration-run-id>
  gh run watch <deploy-run-id>
  ```

- [x] Verify deployments:

  ```bash
  kubectl rollout status deployment/ingestion-service --timeout=5m
  kubectl rollout status deployment/aggregator --timeout=5m
  kubectl rollout status deployment/analytics --timeout=5m
  kubectl rollout status deployment/dashboard --timeout=5m
  ```

- [x] From the analytics pod, verify:

  ```text
  GET /readyz                                      -> HTTP 200
  GET market:volatility:v1:BTCUSD:latest           -> fresh five-horizon JSON
  ZRANGE market:pricing:v1:active 0 -1              -> at least one ticker
  GET market:pricing:v1:<active-ticker>             -> status=available
  XLEN stream:pricing:v1                            -> greater than zero
  ```

- [x] Verify the selected price record contains finite values for spot, strike,
  time to expiry, annualized volatility, model probability, model value, and
  quote-relative edges.
- [x] Verify the dashboard shows that exact ticker and model version.
- [x] If analytics remains unready, inspect
  `market:pricing:v1:status:<ticker>` first. Fix the reported dependency; do not
  weaken readiness or publish fabricated fallback prices.

## Stop conditions

Do not expand this task into order execution, strategy decisions, public
ingress, Bybit activation, a feature schema v2, or a synchronous RPC between
services. The handoff is complete when live ML-derived prices are visible and
the offline path can reliably produce the next auditable model set.
