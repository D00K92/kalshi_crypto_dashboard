-- Normalized BigQuery landing tables for SQL resampling.
-- The GCS exporter remains the source of truth during migration.
CREATE TABLE IF NOT EXISTS `kalshi-crypto-506614.market_data.raw_trades`
(
  event_timestamp TIMESTAMP NOT NULL,
  received_timestamp TIMESTAMP,
  venue STRING NOT NULL,
  instrument STRING NOT NULL,
  trade_id STRING,
  price FLOAT64 NOT NULL,
  quantity FLOAT64 NOT NULL,
  taker_side STRING NOT NULL,
  source_object STRING,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY venue, instrument;

-- One row per book level in a snapshot. Keeping levels normalized makes
-- latest-snapshot selection and level aggregation straightforward in SQL.
CREATE TABLE IF NOT EXISTS `kalshi-crypto-506614.market_data.raw_book_levels`
(
  event_timestamp TIMESTAMP NOT NULL,
  received_timestamp TIMESTAMP,
  venue STRING NOT NULL,
  instrument STRING NOT NULL,
  side STRING NOT NULL,
  level INT64 NOT NULL,
  price FLOAT64 NOT NULL,
  quantity FLOAT64 NOT NULL,
  source_object STRING,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY venue, instrument, side, level;
