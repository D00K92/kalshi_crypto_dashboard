# Feature development guide

This is the starting point for a Data Scientist changing model features.
`market_features.json` is the only human-edited feature registry. It owns names,
types, rolling-window sizes, version membership, offline-table bindings, and
the ordered model input subset for every forecast horizon.

## Which files should I edit?

Choose the smallest matching change:

| Change | Files to edit |
|---|---|
| Change which existing features a model uses | `feature_contracts/market_features.json` only |
| Add a calculated feature to the current contract | Manifest, canonical BigQuery SQL, live `computation.py`, BigQuery schema migration, and their tests |
| Change only training or model architecture | ML pipeline code; do not change the feature contract unless inputs change |
| Introduce a new feature-contract version | Manifest plus new offline/live formula code; MLOps handles deployment configuration and migration |

### 1. Model-subset-only change

Edit `contracts.<version>.model_inputs.<horizon>` in
`feature_contracts/market_features.json`. List columns in the exact order the
model must receive them. This order becomes part of the model contract hash.

No SQL, live feature, Feast, or model-serving edit is required when every named
feature already exists in the contract.

### 2. New calculated feature

Edit these files:

1. `feature_contracts/market_features.json`
   - Add the feature name, type, calculation kind, window, and `window_bars`.
   - Add it to the canonical contract's `fields` and `required_fields`.
   - Add it to each applicable horizon under `model_inputs`.
2. `services/batch_etl/sql/018_compute_v4_10s_volume_features.sql`
   - Implement the offline formula using only data available at the feature's
     `event_timestamp`.
3. `services/live_feature_service/src/live_feature_service/computation.py`
   - Implement the identical causal formula for Redis primitives.
4. `services/batch_etl/sql/017_create_v2_v3_contract_tables.sql`
   - Add the BigQuery column when the canonical physical schema changes.
5. Tests:
   - `services/batch_etl/tests/test_v2_10s_contract.py`
   - `services/live_feature_service/tests/test_computation.py`
   - `services/feast_store/tests/test_parity.py`

The production parity checker currently compares price, return, and venue
count. If the new feature must become a deployment gate, also extend
`services/feast_store/jobs/parity.py` to select and compare it.

### 3. New contract version

Add the version under `contracts`, set `canonical_version`, and set
`canonical_sql` in the manifest. Implement its offline and online calculations
as above. A new calculation family may also require a live computer or routing
change in:

- `services/live_feature_service/src/live_feature_service/computation.py`
- `services/live_feature_service/src/live_feature_service/service.py`

Do not edit Kubernetes manifests, Redis keys, model packaging, or deployment
workflows as part of Data Science work. Hand the tested contract version to
MLOps for compatibility migration, parity validation, and release.

## Generate registrations

After editing the manifest, run from the repository root:

```bash
python tools/generate_feature_contracts.py
python tools/generate_feature_contracts.py --check
```

The generator updates the batch runner, Feast registration, live window
configuration, and ML contract modules. Commit the generated files together
with the manifest. CI runs `--check` and rejects stale generated output.

Never edit these files directly:

- `services/batch_etl/scripts/generated_feature_contracts.py`
- `services/feast_store/registry/generated_feature_contracts.py`
- `services/live_feature_service/src/live_feature_service/generated_feature_contracts.py`
- `services/ml_pipeline/src/common/generated_feature_contracts.py`
- `services/ml_pipeline/src/common/contracts.py` for ordinary feature or subset changes
- `services/feast_store/definitions/canonical_market_features.py` for ordinary schema changes

## Required validation

```bash
python tools/generate_feature_contracts.py --check
uv run --directory services/batch_etl --locked pytest
uv run --directory services/feast_store --locked pytest
uv run --directory services/ml_pipeline --locked pytest
uv run --directory services/live_feature_service pytest
```

Before deployment, MLOps will dry-run the BigQuery SQL, apply schema changes,
confirm offline/online parity, package model artifacts with their contract
hashes, and verify the live feature payload contains every required input.
