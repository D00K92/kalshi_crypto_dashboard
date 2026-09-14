-- Idempotent schema migration for the active 10-second feature contracts.
-- Deployment runs this before Feast validates its BigQuery data sources.
CREATE TABLE IF NOT EXISTS `${project}.feature_store.realized_volatility_v2_10s`
(
  asset STRING NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  created_timestamp TIMESTAMP NOT NULL,
  source_frequency STRING NOT NULL,
  feature_version STRING NOT NULL,
  synthetic_price FLOAT64,
  log_return FLOAT64,
  venue_count INT64,
  realized_vol_1h FLOAT64,
  realized_vol_3h FLOAT64
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY source_frequency, asset;

CREATE TABLE IF NOT EXISTS `${project}.feature_store.realized_volatility_v3_10s`
(
  asset STRING NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  created_timestamp TIMESTAMP NOT NULL,
  source_frequency STRING NOT NULL,
  feature_version STRING NOT NULL,
  synthetic_price FLOAT64,
  log_return FLOAT64,
  venue_count INT64,
  realized_vol_30s FLOAT64,
  realized_vol_1m FLOAT64,
  realized_vol_5m FLOAT64,
  realized_vol_15m FLOAT64,
  realized_vol_30m FLOAT64,
  realized_vol_1h FLOAT64,
  realized_vol_3h FLOAT64
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY source_frequency, asset;

CREATE TABLE IF NOT EXISTS `${project}.feature_store.realized_volatility_v4_10s`
(
  asset STRING NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  created_timestamp TIMESTAMP NOT NULL,
  source_frequency STRING NOT NULL,
  feature_version STRING NOT NULL,
  synthetic_price FLOAT64,
  log_return FLOAT64,
  venue_count INT64,
  realized_vol_30s FLOAT64,
  realized_vol_1m FLOAT64,
  realized_vol_5m FLOAT64,
  realized_vol_15m FLOAT64,
  realized_vol_30m FLOAT64,
  realized_vol_1h FLOAT64,
  realized_vol_3h FLOAT64,
  log_buy_volume_30s FLOAT64,
  log_buy_volume_5m FLOAT64,
  log_buy_volume_10m FLOAT64
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY source_frequency, asset;

CREATE TABLE IF NOT EXISTS `${project}.training_labels.future_realized_volatility_v2_10s`
(
  market_id STRING NOT NULL,
  prediction_timestamp TIMESTAMP NOT NULL,
  label_window_end TIMESTAMP NOT NULL,
  label_created_timestamp TIMESTAMP NOT NULL,
  target_rv_1m FLOAT64,
  target_rv_5m FLOAT64,
  target_rv_15m FLOAT64,
  target_rv_30m FLOAT64,
  target_rv_1h FLOAT64,
  label_version STRING NOT NULL
)
PARTITION BY DATE(prediction_timestamp)
CLUSTER BY market_id, label_version;
