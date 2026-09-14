-- Buyer-volume features for the promoted 5m HAR model.
-- Volatility inputs come from v3; buyer volume is summed across venues and
-- log-transformed to limit the influence of extreme prints.
MERGE `${project}.feature_store.realized_volatility_v4_10s` AS target
USING (
  WITH buy_volume_10s AS (
    SELECT event_timestamp, SUM(v_buy) AS buy_volume
    FROM `${project}.market_data.bars`
    WHERE frequency = '10s'
      AND event_timestamp >= TIMESTAMP_SUB(@target_start, INTERVAL 590 SECOND)
      AND event_timestamp < @target_end
    GROUP BY event_timestamp
  ),
  base AS (
    SELECT
      features.*,
      COALESCE(volume.buy_volume, 0.0) AS buy_volume
    FROM `${project}.feature_store.realized_volatility_v3_10s` AS features
    LEFT JOIN buy_volume_10s AS volume USING (event_timestamp)
    WHERE features.asset = 'BTCUSD'
      AND features.feature_version = 'v3_10s'
      AND features.event_timestamp >= TIMESTAMP_SUB(@target_start, INTERVAL 590 SECOND)
      AND features.event_timestamp < @target_end
  ),
  windows AS (
    SELECT
      *,
      COUNT(*) OVER w30s AS observations_30s,
      COUNT(*) OVER w5m AS observations_5m,
      COUNT(*) OVER w10m AS observations_10m,
      SUM(buy_volume) OVER w30s AS buy_volume_30s,
      SUM(buy_volume) OVER w5m AS buy_volume_5m,
      SUM(buy_volume) OVER w10m AS buy_volume_10m
    FROM base
    WINDOW
      w30s AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 20 PRECEDING AND CURRENT ROW),
      w5m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 290 PRECEDING AND CURRENT ROW),
      w10m AS (ORDER BY UNIX_SECONDS(event_timestamp) RANGE BETWEEN 590 PRECEDING AND CURRENT ROW)
  )
  SELECT
    asset,
    event_timestamp,
    CURRENT_TIMESTAMP() AS created_timestamp,
    source_frequency,
    'v4_10s' AS feature_version,
    synthetic_price,
    log_return,
    venue_count,
    realized_vol_30s,
    realized_vol_1m,
    realized_vol_5m,
    realized_vol_15m,
    realized_vol_30m,
    realized_vol_1h,
    realized_vol_3h,
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
