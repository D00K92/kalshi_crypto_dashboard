CREATE TABLE IF NOT EXISTS `kalshi-crypto-506614.feature_store.realized_volatility_v1`
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
