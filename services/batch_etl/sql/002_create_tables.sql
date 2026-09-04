-- Canonical resampled market table. Keep event and production timestamps
-- separate so Feast and training can enforce point-in-time correctness.
CREATE TABLE IF NOT EXISTS `kalshi-crypto-506614.market_data.bars`
(
  event_timestamp TIMESTAMP NOT NULL,
  created_timestamp TIMESTAMP NOT NULL,
  venue STRING NOT NULL,
  instrument STRING NOT NULL,
  frequency STRING NOT NULL,
  p_open FLOAT64,
  p_high FLOAT64,
  p_low FLOAT64,
  p_close FLOAT64,
  p_trade FLOAT64,
  p_trade_mean FLOAT64,
  v_trade FLOAT64,
  v_buy FLOAT64,
  v_sell FLOAT64,
  cnt_trade INT64,
  dt_fill_mean_ms FLOAT64,
  dt_fill_max_ms FLOAT64,
  dt_fill_min_ms FLOAT64,
  p_bid_1 FLOAT64, p_ask_1 FLOAT64, q_bid_1 FLOAT64, q_ask_1 FLOAT64,
  p_bid_2 FLOAT64, p_ask_2 FLOAT64, q_bid_2 FLOAT64, q_ask_2 FLOAT64,
  p_bid_3 FLOAT64, p_ask_3 FLOAT64, q_bid_3 FLOAT64, q_ask_3 FLOAT64,
  p_bid_4 FLOAT64, p_ask_4 FLOAT64, q_bid_4 FLOAT64, q_ask_4 FLOAT64,
  p_bid_5 FLOAT64, p_ask_5 FLOAT64, q_bid_5 FLOAT64, q_ask_5 FLOAT64,
  p_bid_6 FLOAT64, p_ask_6 FLOAT64, q_bid_6 FLOAT64, q_ask_6 FLOAT64,
  p_bid_7 FLOAT64, p_ask_7 FLOAT64, q_bid_7 FLOAT64, q_ask_7 FLOAT64,
  p_bid_8 FLOAT64, p_ask_8 FLOAT64, q_bid_8 FLOAT64, q_ask_8 FLOAT64,
  p_bid_9 FLOAT64, p_ask_9 FLOAT64, q_bid_9 FLOAT64, q_ask_9 FLOAT64,
  p_bid_10 FLOAT64, p_ask_10 FLOAT64, q_bid_10 FLOAT64, q_ask_10 FLOAT64
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY frequency, venue, instrument;

-- One venue-agnostic feature table. Add columns only with an explicit feature
-- version; never change the meaning of an existing column in place.
CREATE TABLE IF NOT EXISTS `kalshi-crypto-506614.feature_store.market_features_v1`
(
  market_id STRING NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  created_timestamp TIMESTAMP NOT NULL,
  asset STRING NOT NULL,
  frequency STRING NOT NULL,
  feature_version STRING NOT NULL,
  trade_log_return FLOAT64,
  wap_1 FLOAT64, microprice_1 FLOAT64, obi_1 FLOAT64,
  spread FLOAT64, relative_spread FLOAT64,
  book_slope_bid FLOAT64, book_slope_ask FLOAT64,
  liquidity_consumption FLOAT64, ofi FLOAT64, aggressor_imbalance FLOAT64,
  rv_30s FLOAT64, rv_60s FLOAT64, rv_300s FLOAT64, rv_900s FLOAT64,
  rv_1800s FLOAT64, rv_3600s FLOAT64,
  bv_30s FLOAT64, bv_60s FLOAT64, bv_300s FLOAT64, bv_900s FLOAT64,
  bv_1800s FLOAT64, bv_3600s FLOAT64,
  gk_vol_30s FLOAT64, gk_vol_60s FLOAT64, gk_vol_300s FLOAT64,
  gk_vol_900s FLOAT64, gk_vol_1800s FLOAT64, gk_vol_3600s FLOAT64
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY frequency, asset, market_id;

-- Labels are deliberately not a Feast online FeatureView.
CREATE TABLE IF NOT EXISTS `kalshi-crypto-506614.training_labels.future_realized_volatility_v1`
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
