-- BigQuery-native venue-agnostic realized-volatility features.
-- Input is the canonical 1-minute bars table.  The synthetic price is the
-- equal-weight mean of available venue trade prices at each minute.
-- @target_start and @target_end bound the output hour; the three-hour
-- lookback is included so the 3h window is complete at the first output row.
DECLARE annualization_factor FLOAT64 DEFAULT 365 * 24 * 60;

MERGE `${project}.feature_store.realized_volatility_v1` AS target
USING (
  WITH venue_prices AS (
    SELECT
      event_timestamp,
      AVG(COALESCE(p_trade_mean, p_trade)) AS synthetic_price,
      COUNTIF(COALESCE(p_trade_mean, p_trade) IS NOT NULL) AS venue_count
    FROM `${project}.market_data.bars`
    WHERE frequency = '1m'
      AND event_timestamp >= TIMESTAMP_SUB(@target_start, INTERVAL 3 HOUR)
      AND event_timestamp < @target_end
    GROUP BY event_timestamp
  ),
  returns AS (
    SELECT
      event_timestamp,
      synthetic_price,
      venue_count,
      SAFE_DIVIDE(
        synthetic_price,
        LAG(synthetic_price) OVER (ORDER BY event_timestamp)
      ) AS price_ratio
    FROM venue_prices
  ),
  with_returns AS (
    SELECT
      event_timestamp,
      synthetic_price,
      venue_count,
      IF(price_ratio > 0, LN(price_ratio), NULL) AS log_return
    FROM returns
  ),
  windows AS (
    SELECT
      event_timestamp,
      synthetic_price,
      venue_count,
      log_return,
      COUNT(log_return) OVER (
        ORDER BY UNIX_SECONDS(event_timestamp)
        RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
      ) AS observations_1h,
      COUNT(log_return) OVER (
        ORDER BY UNIX_SECONDS(event_timestamp)
        RANGE BETWEEN 10800 PRECEDING AND CURRENT ROW
      ) AS observations_3h,
      SUM(POW(log_return, 2)) OVER (
        ORDER BY UNIX_SECONDS(event_timestamp)
        RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
      ) AS sum_sq_1h,
      SUM(POW(log_return, 2)) OVER (
        ORDER BY UNIX_SECONDS(event_timestamp)
        RANGE BETWEEN 10800 PRECEDING AND CURRENT ROW
      ) AS sum_sq_3h
    FROM with_returns
  )
  SELECT
    'BTC' AS asset,
    event_timestamp,
    CURRENT_TIMESTAMP() AS created_timestamp,
    '1m' AS source_frequency,
    'v1' AS feature_version,
    synthetic_price,
    log_return,
    venue_count,
    IF(observations_1h >= 45,
       SQRT(sum_sq_1h * annualization_factor), NULL) AS realized_vol_1h,
    IF(observations_3h >= 135,
       SQRT(sum_sq_3h * annualization_factor), NULL) AS realized_vol_3h
  FROM windows
  WHERE event_timestamp >= @target_start
    AND event_timestamp < @target_end
) AS source
ON target.event_timestamp = source.event_timestamp
WHEN MATCHED THEN UPDATE SET
  asset = source.asset,
  created_timestamp = source.created_timestamp,
  source_frequency = source.source_frequency,
  feature_version = source.feature_version,
  synthetic_price = source.synthetic_price,
  log_return = source.log_return,
  venue_count = source.venue_count,
  realized_vol_1h = source.realized_vol_1h,
  realized_vol_3h = source.realized_vol_3h
WHEN NOT MATCHED THEN INSERT (
  asset, event_timestamp, created_timestamp, source_frequency,
  feature_version, synthetic_price, log_return, venue_count,
  realized_vol_1h, realized_vol_3h
) VALUES (
  source.asset, source.event_timestamp, source.created_timestamp,
  source.source_frequency, source.feature_version, source.synthetic_price,
  source.log_return, source.venue_count, source.realized_vol_1h,
  source.realized_vol_3h
);
