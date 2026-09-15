-- Canonical 10-second model features. This is the only active feature writer.
-- It computes price, realized-volatility, and buyer-volume features directly
-- from primitive bars so no versioned intermediate tables are required.
DECLARE seconds_per_year FLOAT64 DEFAULT 365 * 24 * 60 * 60;

MERGE `${project}.feature_store.realized_volatility_v4_10s` AS target
USING (
  WITH primitives AS (
    SELECT
      event_timestamp,
      AVG(COALESCE(p_trade_mean, p_trade)) AS synthetic_price,
      COUNTIF(COALESCE(p_trade_mean, p_trade) IS NOT NULL) AS venue_count,
      SUM(COALESCE(v_buy, 0.0)) AS buy_volume
    FROM `${project}.market_data.bars`
    WHERE frequency = '10s'
      AND event_timestamp >= TIMESTAMP_SUB(@target_start, INTERVAL 3 HOUR)
      AND event_timestamp < @target_end
    GROUP BY event_timestamp
  ),
  returns AS (
    SELECT
      *,
      IF(
        SAFE_DIVIDE(synthetic_price, LAG(synthetic_price) OVER (ORDER BY event_timestamp)) > 0,
        LN(SAFE_DIVIDE(synthetic_price, LAG(synthetic_price) OVER (ORDER BY event_timestamp))),
        NULL
      ) AS log_return
    FROM primitives
  ),
  windows AS (
    SELECT
      *,
      COUNT(log_return) OVER w30s AS observations_rv_30s,
      COUNT(log_return) OVER w1m AS observations_rv_1m,
      COUNT(log_return) OVER w5m AS observations_rv_5m,
      COUNT(log_return) OVER w15m AS observations_rv_15m,
      COUNT(log_return) OVER w30m AS observations_rv_30m,
      COUNT(log_return) OVER w1h AS observations_rv_1h,
      COUNT(log_return) OVER w3h AS observations_rv_3h,
      SUM(POW(log_return, 2)) OVER w30s AS sum_sq_30s,
      SUM(POW(log_return, 2)) OVER w1m AS sum_sq_1m,
      SUM(POW(log_return, 2)) OVER w5m AS sum_sq_5m,
      SUM(POW(log_return, 2)) OVER w15m AS sum_sq_15m,
      SUM(POW(log_return, 2)) OVER w30m AS sum_sq_30m,
      SUM(POW(log_return, 2)) OVER w1h AS sum_sq_1h,
      SUM(POW(log_return, 2)) OVER w3h AS sum_sq_3h,
      COUNT(*) OVER w30s AS observations_30s,
      COUNT(*) OVER w5m AS observations_5m,
      COUNT(*) OVER w10m AS observations_10m,
      SUM(buy_volume) OVER w30s AS buy_volume_30s,
      SUM(buy_volume) OVER w5m AS buy_volume_5m,
      SUM(buy_volume) OVER w10m AS buy_volume_10m
    FROM returns
    WINDOW
      w30s AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 20 PRECEDING AND CURRENT ROW),
      w5m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 290 PRECEDING AND CURRENT ROW),
      w10m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 590 PRECEDING AND CURRENT ROW),
      w1m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 50 PRECEDING AND CURRENT ROW),
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
    'v4_10s' AS feature_version,
    synthetic_price,
    log_return,
    venue_count,
    IF(observations_rv_30s = 3, SQRT(sum_sq_30s * seconds_per_year / 30), NULL) AS realized_vol_30s,
    IF(observations_rv_1m = 6, SQRT(sum_sq_1m * seconds_per_year / 60), NULL) AS realized_vol_1m,
    IF(observations_rv_5m = 30, SQRT(sum_sq_5m * seconds_per_year / 300), NULL) AS realized_vol_5m,
    IF(observations_rv_15m = 90, SQRT(sum_sq_15m * seconds_per_year / 900), NULL) AS realized_vol_15m,
    IF(observations_rv_30m = 180, SQRT(sum_sq_30m * seconds_per_year / 1800), NULL) AS realized_vol_30m,
    IF(observations_rv_1h = 360, SQRT(sum_sq_1h * seconds_per_year / 3600), NULL) AS realized_vol_1h,
    IF(observations_rv_3h = 1080, SQRT(sum_sq_3h * seconds_per_year / 10800), NULL) AS realized_vol_3h,
    IF(observations_30s = 3, LN(1 + buy_volume_30s), NULL) AS log_buy_volume_30s,
    IF(observations_5m = 30, LN(1 + buy_volume_5m), NULL) AS log_buy_volume_5m,
    IF(observations_10m = 60, LN(1 + buy_volume_10m), NULL) AS log_buy_volume_10m
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
  realized_vol_3h = source.realized_vol_3h,
  log_buy_volume_30s = source.log_buy_volume_30s,
  log_buy_volume_5m = source.log_buy_volume_5m,
  log_buy_volume_10m = source.log_buy_volume_10m
WHEN NOT MATCHED THEN INSERT ROW;
