# Feast Pipeline Architecture Recommendation

The overall structure is sound. The most important adjustment is to treat `feast_store` as a **feature repository and serving/materialization component**, not as the owner of feature computation.

This design uses **Option A**: `batch_etl` computes and stores targets alongside bars and features. Targets remain logically and physically separate from production features, while `ml_pipeline` consumes the precomputed target data when assembling training datasets.

## Recommended structure

```text
services/
├── batch_etl/
│   ├── resampling/
│   ├── feature_generation/
│   ├── target_generation/
│   ├── quality_checks/
│   ├── jobs/
│   │   ├── process_incremental.py
│   │   ├── backfill_bars.py
│   │   ├── backfill_features.py
│   │   └── backfill_targets.py
│   ├── Dockerfile
│   └── pyproject.toml
│
├── feast_store/
│   ├── feature_store.yaml
│   ├── entities.py
│   ├── data_sources.py
│   ├── feature_views/
│   ├── feature_services/
│   ├── jobs/
│   │   ├── apply.py
│   │   ├── materialize_incremental.py
│   │   └── materialize_range.py
│   ├── tests/
│   ├── Dockerfile
│   └── pyproject.toml
│
├── ml_pipeline/
│   ├── target_specs/
│   ├── dataset_generation/
│   ├── training/
│   ├── evaluation/
│   ├── model_registry/
│   ├── Dockerfile
│   └── pyproject.toml
│
└── model_serving/
    ├── inference/
    ├── feature_retrieval/
    ├── Dockerfile
    └── pyproject.toml
```

## Service responsibilities

### `batch_etl`

This service owns data transformation:

```text
GCS ticks
  → validated ticks
  → resampled bars
  ├→ calculated features → BigQuery feature tables
  └→ calculated targets  → BigQuery target tables
```

It should handle:

- Incremental source discovery
- Tick deduplication and ordering
- Late-arriving ticks
- Resampling
- Feature calculation
- Target calculation
- Idempotent backfills
- Schema and quality validation
- Writing physical BigQuery feature and target tables

Using Python and Dask here is valid. Simple operations can gradually move to BigQuery SQL without changing the service boundary:

```text
batch_etl/
├── resampling/
│   ├── bigquery_resampler.py
│   └── dask_resampler.py
└── feature_generation/
    ├── sql/
    └── python/
```

The implementation technology is less important than the output contract.

Targets should be written to a separate dataset or table namespace and must never be materialized into the online feature store. A typical split is:

```text
BigQuery:
├── market_data.bars_1m
├── feature_store.market_features_1m
└── training_labels.forward_returns_5m
```

### `feast_store`

This component answers:

- What is an entity?
- Which columns are features?
- Where is each feature table?
- What is the feature TTL?
- Which features belong to a model-facing feature service?
- Which features should be available online?
- How are historical features retrieved without leakage?

Its inputs should already look like:

```text
market_id
event_timestamp
created_timestamp
return_5m
realized_volatility_1h
relative_volume_24h
```

Feast registers and retrieves those fields. It should not need to understand raw trade records or reconstruct bars.

### `ml_pipeline`

This service owns:

```text
Load precomputed targets
  + retrieve prediction-time features from Feast
  → training dataset
  → train
  → evaluate
  → register model
```

The ML pipeline owns target **selection and interpretation**, while `batch_etl` owns scalable target computation. `target_specs/` records which stored target definition, horizon, and version a model uses.

Targets:

- Are not used at live inference
- Often require looking forward in time
- Must be carefully aligned with prediction timestamps
- Can easily introduce leakage
- May change independently of production feature definitions

For example:

```text
prediction time: 10:00
features: information available at or before 10:00
target: return from 10:00 through 10:05
```

Feast should retrieve the features as of 10:00. The ML pipeline should load the previously calculated 10:00–10:05 target and join it using `market_id` and `prediction_timestamp`.

For example:

```python
labels = load_target_table("training_labels.forward_returns_5m")

training_data = feast.get_historical_features(
    entity_df=labels[
        ["market_id", "prediction_timestamp", "forward_return_5m"]
    ],
    features=[
        "market_features_1m:return_5m",
        "market_features_1m:realized_volatility_1h",
    ],
).to_df()
```

The target column can be carried through the entity dataframe while Feast attaches point-in-time-correct features.

### `model_serving`

Online inference needs an explicit boundary:

```text
Prediction request
    ↓
Model service
    ↓
Feast online feature retrieval
    ↓
Redis / Bigtable
    ↓
Model prediction
```

This could be a standalone inference service or part of an existing API, but it should be represented in the architecture.

## Separate feature backfills from Feast materialization

There are two distinct operations:

### Feature-generation backfill

```text
Raw ticks → bars → calculated features and targets
```

This belongs in `batch_etl`.

### Feast online-store materialization

```text
Existing BigQuery feature rows → Redis/Bigtable
```

This belongs in `feast_store`.

Using the name `materialize_range.py` instead of a generic `backfill.py` makes it clear that Feast is not recalculating feature history.

## ETL-to-Feast data contract

Every Feast source table should contain at least:

```text
entity key(s)
event_timestamp
created_timestamp
feature columns
```

The fields should have explicit meanings:

- `event_timestamp`: when the feature was valid in the real world
- `created_timestamp`: when that version of the feature was produced or became available
- Entity key: stable identity used by both training and inference
- Feature columns: immutable definitions for a given version

For a market model:

```text
market_id = "coinbase:BTC-USD:1m"
event_timestamp = 2026-09-04 10:05:00 UTC
created_timestamp = 2026-09-04 10:05:12 UTC
```

That 12-second difference matters. If the production pipeline would not have known the feature until `10:05:12`, the training pipeline should not pretend it existed at `10:05:00`.

## Target data contract and leakage protection

Each stored target row should include:

```text
market_id
prediction_timestamp
label_window_end
label_created_timestamp
target value(s)
label_version
```

For example:

```text
prediction_timestamp    = 10:00
feature cutoff          = 10:00
label window            = 10:00–10:05
label_created_timestamp = 10:05
```

The target is keyed by the prediction timestamp even though it becomes observable later. When assembling a training row, Feast must use `prediction_timestamp`—not `label_created_timestamp`—as the historical feature lookup time.

Precomputing features and targets in the same service does not by itself prevent leakage. Leakage protection requires all of the following:

- Feature calculations use only information available by the prediction cutoff.
- Targets are stored separately from production features.
- Target tables are never exposed as online Feast feature views.
- Feast historical retrieval uses the prediction timestamp.
- Training and evaluation splits respect chronological order.

## Feature versioning

Avoid changing the meaning of an existing feature while retaining its name.

Bad:

```text
realized_volatility_1h
```

For example, it originally uses one-minute returns and later silently changes to tick returns.

Better:

```text
realized_volatility_1h_v1
realized_volatility_1h_v2
```

Alternatively, create versioned feature views:

```text
market_features_1m_v1
market_features_1m_v2
```

Feature services can then represent explicit model contracts:

```text
direction_model_v3_features
volatility_model_v2_features
```

This allows an older production model to continue retrieving exactly the features it was trained on.

## Final recommendation

- Keep resampling and feature computation in `batch_etl`.
- Compute and store targets in `batch_etl` under a separate label namespace.
- Let `ml_pipeline` select and consume precomputed target versions.
- Keep feature-data and target-data backfills in `batch_etl`.
- Limit Feast backfills to materializing already-computed features.
- Add an explicit `model_serving` boundary.
- Define a strict timestamped table contract between ETL and Feast.
- Use BigQuery SQL for straightforward transformations and Dask for genuinely complex or stateful features.

The resulting ownership model is:

```text
batch_etl owns bar, feature, and target values
feast_store owns feature definitions and access
ml_pipeline owns target selection, training datasets, and models
model_serving owns predictions
```
