-- BigQuery-native future realized-volatility labels from the 1m synthetic price.
-- For prediction timestamp t, each horizon sums squared returns from t+1
-- through t+horizon. The descending window makes this a forward-looking
-- window without exporting data to Python/Dask.
DECLARE seconds_per_year FLOAT64 DEFAULT 365 * 24 * 60 * 60;

MERGE `${project}.training_labels.future_realized_volatility_v1` AS target
USING (
  WITH synthetic_prices AS (
    SELECT
      event_timestamp,
      AVG(COALESCE(p_trade_mean, p_trade)) AS synthetic_price
    FROM `${project}.market_data.bars`
    WHERE frequency = '1m'
      AND event_timestamp >= TIMESTAMP_SUB(@target_start, INTERVAL 1 MINUTE)
      AND event_timestamp < TIMESTAMP_ADD(@target_end, INTERVAL 1 HOUR)
    GROUP BY event_timestamp
  ),
  returns AS (
    SELECT
      event_timestamp,
      synthetic_price,
      IF(synthetic_price > 0 AND previous_price > 0,
         LN(synthetic_price / previous_price), NULL) AS log_return
    FROM (
      SELECT
        event_timestamp,
        synthetic_price,
        LAG(synthetic_price) OVER (ORDER BY event_timestamp) AS previous_price
      FROM synthetic_prices
    )
  ),
  future_windows AS (
    SELECT
      event_timestamp,
      COUNT(log_return) OVER w1m AS observations_1m,
      COUNT(log_return) OVER w5 AS observations_5m,
      COUNT(log_return) OVER w15 AS observations_15m,
      COUNT(log_return) OVER w30 AS observations_30m,
      SUM(POW(log_return, 2)) OVER w1m AS sum_sq_1m,
      SUM(POW(log_return, 2)) OVER w5 AS sum_sq_5m,
      SUM(POW(log_return, 2)) OVER w15 AS sum_sq_15m,
      SUM(POW(log_return, 2)) OVER w30 AS sum_sq_30m,
      SUM(POW(log_return, 2)) OVER w1h AS sum_sq_1h,
      COUNT(log_return) OVER w1h AS observations_1h
    FROM returns
    WINDOW
      w1m AS (ORDER BY UNIX_SECONDS(event_timestamp) DESC RANGE BETWEEN 60 PRECEDING AND 1 PRECEDING),
      w5 AS (ORDER BY UNIX_SECONDS(event_timestamp) DESC RANGE BETWEEN 300 PRECEDING AND 1 PRECEDING),
      w15 AS (ORDER BY UNIX_SECONDS(event_timestamp) DESC RANGE BETWEEN 900 PRECEDING AND 1 PRECEDING),
      w30 AS (ORDER BY UNIX_SECONDS(event_timestamp) DESC RANGE BETWEEN 1800 PRECEDING AND 1 PRECEDING),
      w1h AS (ORDER BY UNIX_SECONDS(event_timestamp) DESC RANGE BETWEEN 3600 PRECEDING AND 1 PRECEDING)
  )
  SELECT
    'BTC' AS market_id,
    event_timestamp AS prediction_timestamp,
    TIMESTAMP_ADD(event_timestamp, INTERVAL 1 HOUR) AS label_window_end,
    CURRENT_TIMESTAMP() AS label_created_timestamp,
    IF(observations_1m >= 1, SQRT(sum_sq_1m * seconds_per_year / 60), NULL) AS target_rv_1m,
    IF(observations_5m >= 4, SQRT(sum_sq_5m * seconds_per_year / 300), NULL) AS target_rv_5m,
    IF(observations_15m >= 12, SQRT(sum_sq_15m * seconds_per_year / 900), NULL) AS target_rv_15m,
    IF(observations_30m >= 24, SQRT(sum_sq_30m * seconds_per_year / 1800), NULL) AS target_rv_30m,
    IF(observations_1h >= 45, SQRT(sum_sq_1h * seconds_per_year / 3600), NULL) AS target_rv_1h,
    'v1' AS label_version
  FROM future_windows
  WHERE event_timestamp >= @target_start
    AND event_timestamp < @target_end
) AS source
ON target.market_id = source.market_id
AND target.prediction_timestamp = source.prediction_timestamp
AND target.label_version = source.label_version
WHEN MATCHED THEN UPDATE SET
  label_window_end = source.label_window_end,
  label_created_timestamp = source.label_created_timestamp,
  target_rv_1m = source.target_rv_1m,
  target_rv_5m = source.target_rv_5m,
  target_rv_15m = source.target_rv_15m,
  target_rv_30m = source.target_rv_30m,
  target_rv_1h = source.target_rv_1h
WHEN NOT MATCHED THEN INSERT (
  market_id, prediction_timestamp, label_window_end, label_created_timestamp,
  target_rv_1m, target_rv_5m, target_rv_15m, target_rv_30m, target_rv_1h,
  label_version
) VALUES (
  source.market_id, source.prediction_timestamp, source.label_window_end,
  source.label_created_timestamp, source.target_rv_1m, source.target_rv_5m,
  source.target_rv_15m, source.target_rv_30m, source.target_rv_1h,
  source.label_version
);
