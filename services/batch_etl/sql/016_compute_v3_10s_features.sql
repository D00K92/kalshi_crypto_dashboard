-- Point-in-time-safe HAR inputs for market_features/v3_10s.
-- Every window ends at event_timestamp and therefore uses only information
-- available when the forecast is made.
DECLARE seconds_per_year FLOAT64 DEFAULT 365 * 24 * 60 * 60;

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

MERGE `${project}.feature_store.realized_volatility_v3_10s` AS target
USING (
  WITH venue_prices AS (
    SELECT
      event_timestamp,
      AVG(COALESCE(p_trade_mean, p_trade)) AS synthetic_price,
      COUNTIF(COALESCE(p_trade_mean, p_trade) IS NOT NULL) AS venue_count
    FROM `${project}.market_data.bars`
    WHERE frequency = '10s'
      -- Three hours of returns need one additional price observation.
      AND event_timestamp >= TIMESTAMP_SUB(@target_start, INTERVAL 10810 SECOND)
      AND event_timestamp < @target_end
    GROUP BY event_timestamp
  ),
  returns AS (
    SELECT
      event_timestamp,
      synthetic_price,
      venue_count,
      IF(previous_price > 0,
         LN(synthetic_price / previous_price), NULL) AS log_return
    FROM (
      SELECT
        event_timestamp,
        synthetic_price,
        venue_count,
        LAG(synthetic_price) OVER (ORDER BY event_timestamp) AS previous_price
      FROM venue_prices
    )
  ),
  windows AS (
    SELECT
      *,
      COUNT(log_return) OVER w30s AS observations_30s,
      COUNT(log_return) OVER w1m AS observations_1m,
      COUNT(log_return) OVER w5m AS observations_5m,
      COUNT(log_return) OVER w15m AS observations_15m,
      COUNT(log_return) OVER w30m AS observations_30m,
      COUNT(log_return) OVER w1h AS observations_1h,
      COUNT(log_return) OVER w3h AS observations_3h,
      SUM(POW(log_return, 2)) OVER w30s AS sum_sq_30s,
      SUM(POW(log_return, 2)) OVER w1m AS sum_sq_1m,
      SUM(POW(log_return, 2)) OVER w5m AS sum_sq_5m,
      SUM(POW(log_return, 2)) OVER w15m AS sum_sq_15m,
      SUM(POW(log_return, 2)) OVER w30m AS sum_sq_30m,
      SUM(POW(log_return, 2)) OVER w1h AS sum_sq_1h,
      SUM(POW(log_return, 2)) OVER w3h AS sum_sq_3h
    FROM returns
    WINDOW
      w30s AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 20 PRECEDING AND CURRENT ROW),
      w1m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 50 PRECEDING AND CURRENT ROW),
      w5m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 290 PRECEDING AND CURRENT ROW),
      w15m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 890 PRECEDING AND CURRENT ROW),
      w30m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 1790 PRECEDING AND CURRENT ROW),
      w1h AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 3590 PRECEDING AND CURRENT ROW),
      w3h AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 10790 PRECEDING AND CURRENT ROW)
  )
  SELECT
    'BTCUSD' AS asset,
    event_timestamp,
    CURRENT_TIMESTAMP() AS created_timestamp,
    '10s' AS source_frequency,
    'v3_10s' AS feature_version,
    synthetic_price,
    log_return,
    venue_count,
    IF(observations_30s = 3, SQRT(sum_sq_30s * seconds_per_year / 30), NULL) AS realized_vol_30s,
    IF(observations_1m = 6, SQRT(sum_sq_1m * seconds_per_year / 60), NULL) AS realized_vol_1m,
    IF(observations_5m = 30, SQRT(sum_sq_5m * seconds_per_year / 300), NULL) AS realized_vol_5m,
    IF(observations_15m = 90, SQRT(sum_sq_15m * seconds_per_year / 900), NULL) AS realized_vol_15m,
    IF(observations_30m = 180, SQRT(sum_sq_30m * seconds_per_year / 1800), NULL) AS realized_vol_30m,
    IF(observations_1h = 360, SQRT(sum_sq_1h * seconds_per_year / 3600), NULL) AS realized_vol_1h,
    IF(observations_3h = 1080, SQRT(sum_sq_3h * seconds_per_year / 10800), NULL) AS realized_vol_3h
  FROM windows
  WHERE event_timestamp >= @target_start
    AND event_timestamp < @target_end
) AS source
ON target.asset = source.asset
AND target.event_timestamp = source.event_timestamp
AND target.feature_version = source.feature_version
WHEN MATCHED THEN UPDATE SET
  created_timestamp = source.created_timestamp,
  source_frequency = source.source_frequency,
  synthetic_price = source.synthetic_price,
  log_return = source.log_return,
  venue_count = source.venue_count,
  realized_vol_30s = source.realized_vol_30s,
  realized_vol_1m = source.realized_vol_1m,
  realized_vol_5m = source.realized_vol_5m,
  realized_vol_15m = source.realized_vol_15m,
  realized_vol_30m = source.realized_vol_30m,
  realized_vol_1h = source.realized_vol_1h,
  realized_vol_3h = source.realized_vol_3h
WHEN NOT MATCHED THEN INSERT (
  asset, event_timestamp, created_timestamp, source_frequency,
  feature_version, synthetic_price, log_return, venue_count,
  realized_vol_30s, realized_vol_1m, realized_vol_5m, realized_vol_15m,
  realized_vol_30m, realized_vol_1h, realized_vol_3h
) VALUES (
  source.asset, source.event_timestamp, source.created_timestamp,
  source.source_frequency, source.feature_version, source.synthetic_price,
  source.log_return, source.venue_count, source.realized_vol_30s, source.realized_vol_1m,
  source.realized_vol_5m, source.realized_vol_15m,
  source.realized_vol_30m, source.realized_vol_1h, source.realized_vol_3h
);
