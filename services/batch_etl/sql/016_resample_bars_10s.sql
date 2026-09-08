-- Canonical 10-second bar backfill.
-- Parameters: @source_start (target_start - 1 hour), @target_start, @target_end.
-- The source predicates are on the partition column, so one invocation reads
-- only one UTC day plus the small state lookback. The MERGE is idempotent.
MERGE `kalshi-crypto-506614.market_data.bars` AS target
USING (
  WITH
  keys AS (
    SELECT DISTINCT venue, instrument
    FROM (
      SELECT venue, instrument
      FROM `kalshi-crypto-506614.market_data.raw_trades`
      WHERE event_timestamp >= @target_start AND event_timestamp < @target_end
      UNION ALL
      SELECT venue, instrument
      FROM `kalshi-crypto-506614.market_data.raw_book_levels`
      WHERE event_timestamp >= @target_start AND event_timestamp < @target_end
    )
  ),
  buckets AS (
    SELECT k.venue, k.instrument, ts AS event_timestamp
    FROM keys k
    CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(
      @target_start, TIMESTAMP_SUB(@target_end, INTERVAL 10 SECOND), INTERVAL 10 SECOND
    )) AS ts
  ),
  trades_with_lag AS (
    SELECT
      event_timestamp, received_timestamp, trade_id, venue, instrument, price,
      quantity, taker_side,
      LAG(event_timestamp) OVER (
        PARTITION BY venue, instrument
        ORDER BY event_timestamp, received_timestamp, trade_id
      ) AS previous_trade_timestamp
    FROM `kalshi-crypto-506614.market_data.raw_trades`
    WHERE event_timestamp >= @source_start AND event_timestamp < @target_end
  ),
  trades AS (
    SELECT
      TIMESTAMP_BUCKET(event_timestamp, INTERVAL 10 SECOND) AS event_timestamp,
      venue, instrument,
      ARRAY_AGG(price ORDER BY event_timestamp, received_timestamp, trade_id LIMIT 1)[OFFSET(0)] AS p_open,
      ARRAY_AGG(price ORDER BY event_timestamp DESC, received_timestamp DESC, trade_id DESC LIMIT 1)[OFFSET(0)] AS p_trade,
      MAX(price) AS p_high,
      MIN(price) AS p_low,
      AVG(price) AS p_trade_mean,
      SUM(quantity) AS v_trade,
      SUM(IF(taker_side = 'buy', quantity, 0)) AS v_buy,
      SUM(IF(taker_side = 'sell', quantity, 0)) AS v_sell,
      COUNTIF(quantity > 0) AS cnt_trade,
      AVG(TIMESTAMP_DIFF(event_timestamp, previous_trade_timestamp, MILLISECOND)) AS dt_fill_mean_ms,
      MAX(TIMESTAMP_DIFF(event_timestamp, previous_trade_timestamp, MILLISECOND)) AS dt_fill_max_ms,
      MIN(TIMESTAMP_DIFF(event_timestamp, previous_trade_timestamp, MILLISECOND)) AS dt_fill_min_ms
    FROM trades_with_lag
    GROUP BY 1, 2, 3
  ),
  latest_book AS (
    SELECT
      TIMESTAMP_BUCKET(event_timestamp, INTERVAL 10 SECOND) AS event_timestamp,
      venue, instrument, side, level, price, quantity
    FROM `kalshi-crypto-506614.market_data.raw_book_levels`
    WHERE event_timestamp >= @source_start AND event_timestamp < @target_end
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY TIMESTAMP_BUCKET(event_timestamp, INTERVAL 10 SECOND), venue, instrument, side, level
      ORDER BY event_timestamp DESC, received_timestamp DESC
    ) = 1
  ),
  book_grid AS (
    SELECT b.venue, b.instrument, b.event_timestamp, side, level,
      lb.price, lb.quantity
    FROM buckets b
    CROSS JOIN UNNEST(['bid', 'ask']) AS side
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, 10)) AS level
    LEFT JOIN latest_book lb
      ON lb.venue = b.venue AND lb.instrument = b.instrument
      AND lb.event_timestamp = b.event_timestamp
      AND lb.side = side AND lb.level = level
  ),
  book_filled AS (
    SELECT
      venue, instrument, event_timestamp, side, level,
      LAST_VALUE(price IGNORE NULLS) OVER w AS price,
      LAST_VALUE(quantity IGNORE NULLS) OVER w AS quantity
    FROM book_grid
    WINDOW w AS (
      PARTITION BY venue, instrument, side, level
      ORDER BY event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
  ),
  book_wide AS (
    SELECT
      venue, instrument, event_timestamp,
      MAX(IF(side = 'bid' AND level = 1, price, NULL)) AS p_bid_1,
      MAX(IF(side = 'ask' AND level = 1, price, NULL)) AS p_ask_1,
      MAX(IF(side = 'bid' AND level = 1, quantity, NULL)) AS q_bid_1,
      MAX(IF(side = 'ask' AND level = 1, quantity, NULL)) AS q_ask_1,
      MAX(IF(side = 'bid' AND level = 2, price, NULL)) AS p_bid_2,
      MAX(IF(side = 'ask' AND level = 2, price, NULL)) AS p_ask_2,
      MAX(IF(side = 'bid' AND level = 2, quantity, NULL)) AS q_bid_2,
      MAX(IF(side = 'ask' AND level = 2, quantity, NULL)) AS q_ask_2,
      MAX(IF(side = 'bid' AND level = 3, price, NULL)) AS p_bid_3,
      MAX(IF(side = 'ask' AND level = 3, price, NULL)) AS p_ask_3,
      MAX(IF(side = 'bid' AND level = 3, quantity, NULL)) AS q_bid_3,
      MAX(IF(side = 'ask' AND level = 3, quantity, NULL)) AS q_ask_3,
      MAX(IF(side = 'bid' AND level = 4, price, NULL)) AS p_bid_4,
      MAX(IF(side = 'ask' AND level = 4, price, NULL)) AS p_ask_4,
      MAX(IF(side = 'bid' AND level = 4, quantity, NULL)) AS q_bid_4,
      MAX(IF(side = 'ask' AND level = 4, quantity, NULL)) AS q_ask_4,
      MAX(IF(side = 'bid' AND level = 5, price, NULL)) AS p_bid_5,
      MAX(IF(side = 'ask' AND level = 5, price, NULL)) AS p_ask_5,
      MAX(IF(side = 'bid' AND level = 5, quantity, NULL)) AS q_bid_5,
      MAX(IF(side = 'ask' AND level = 5, quantity, NULL)) AS q_ask_5,
      MAX(IF(side = 'bid' AND level = 6, price, NULL)) AS p_bid_6,
      MAX(IF(side = 'ask' AND level = 6, price, NULL)) AS p_ask_6,
      MAX(IF(side = 'bid' AND level = 6, quantity, NULL)) AS q_bid_6,
      MAX(IF(side = 'ask' AND level = 6, quantity, NULL)) AS q_ask_6,
      MAX(IF(side = 'bid' AND level = 7, price, NULL)) AS p_bid_7,
      MAX(IF(side = 'ask' AND level = 7, price, NULL)) AS p_ask_7,
      MAX(IF(side = 'bid' AND level = 7, quantity, NULL)) AS q_bid_7,
      MAX(IF(side = 'ask' AND level = 7, quantity, NULL)) AS q_ask_7,
      MAX(IF(side = 'bid' AND level = 8, price, NULL)) AS p_bid_8,
      MAX(IF(side = 'ask' AND level = 8, price, NULL)) AS p_ask_8,
      MAX(IF(side = 'bid' AND level = 8, quantity, NULL)) AS q_bid_8,
      MAX(IF(side = 'ask' AND level = 8, quantity, NULL)) AS q_ask_8,
      MAX(IF(side = 'bid' AND level = 9, price, NULL)) AS p_bid_9,
      MAX(IF(side = 'ask' AND level = 9, price, NULL)) AS p_ask_9,
      MAX(IF(side = 'bid' AND level = 9, quantity, NULL)) AS q_bid_9,
      MAX(IF(side = 'ask' AND level = 9, quantity, NULL)) AS q_ask_9,
      MAX(IF(side = 'bid' AND level = 10, price, NULL)) AS p_bid_10,
      MAX(IF(side = 'ask' AND level = 10, price, NULL)) AS p_ask_10,
      MAX(IF(side = 'bid' AND level = 10, quantity, NULL)) AS q_bid_10,
      MAX(IF(side = 'ask' AND level = 10, quantity, NULL)) AS q_ask_10
    FROM book_filled
    GROUP BY 1, 2, 3
  ),
  assembled AS (
    SELECT
      b.*, t.* EXCEPT(event_timestamp, venue, instrument), bw.* EXCEPT(event_timestamp, venue, instrument)
    FROM buckets b
    LEFT JOIN trades t USING (event_timestamp, venue, instrument)
    LEFT JOIN book_wide bw USING (event_timestamp, venue, instrument)
  )
  SELECT
    event_timestamp, CURRENT_TIMESTAMP() AS created_timestamp, venue, instrument, '10s' AS frequency,
    p_open, p_high, p_low, p_trade AS p_close, p_trade, p_trade_mean,
    COALESCE(v_trade, 0) AS v_trade, COALESCE(v_buy, 0) AS v_buy,
    COALESCE(v_sell, 0) AS v_sell, COALESCE(cnt_trade, 0) AS cnt_trade,
    dt_fill_mean_ms, dt_fill_max_ms, dt_fill_min_ms,
    p_bid_1, p_ask_1, q_bid_1, q_ask_1, p_bid_2, p_ask_2, q_bid_2, q_ask_2,
    p_bid_3, p_ask_3, q_bid_3, q_ask_3, p_bid_4, p_ask_4, q_bid_4, q_ask_4,
    p_bid_5, p_ask_5, q_bid_5, q_ask_5, p_bid_6, p_ask_6, q_bid_6, q_ask_6,
    p_bid_7, p_ask_7, q_bid_7, q_ask_7, p_bid_8, p_ask_8, q_bid_8, q_ask_8,
    p_bid_9, p_ask_9, q_bid_9, q_ask_9, p_bid_10, p_ask_10, q_bid_10, q_ask_10
  FROM assembled
) AS source
ON target.event_timestamp = source.event_timestamp
AND target.venue = source.venue
AND target.instrument = source.instrument
AND target.frequency = source.frequency
WHEN MATCHED THEN UPDATE SET
  created_timestamp = source.created_timestamp,
  p_open = source.p_open, p_high = source.p_high, p_low = source.p_low,
  p_close = source.p_close, p_trade = source.p_trade, p_trade_mean = source.p_trade_mean,
  v_trade = source.v_trade, v_buy = source.v_buy, v_sell = source.v_sell,
  cnt_trade = source.cnt_trade, dt_fill_mean_ms = source.dt_fill_mean_ms,
  dt_fill_max_ms = source.dt_fill_max_ms, dt_fill_min_ms = source.dt_fill_min_ms,
  p_bid_1 = source.p_bid_1, p_ask_1 = source.p_ask_1, q_bid_1 = source.q_bid_1, q_ask_1 = source.q_ask_1,
  p_bid_2 = source.p_bid_2, p_ask_2 = source.p_ask_2, q_bid_2 = source.q_bid_2, q_ask_2 = source.q_ask_2,
  p_bid_3 = source.p_bid_3, p_ask_3 = source.p_ask_3, q_bid_3 = source.q_bid_3, q_ask_3 = source.q_ask_3,
  p_bid_4 = source.p_bid_4, p_ask_4 = source.p_ask_4, q_bid_4 = source.q_bid_4, q_ask_4 = source.q_ask_4,
  p_bid_5 = source.p_bid_5, p_ask_5 = source.p_ask_5, q_bid_5 = source.q_bid_5, q_ask_5 = source.q_ask_5,
  p_bid_6 = source.p_bid_6, p_ask_6 = source.p_ask_6, q_bid_6 = source.q_bid_6, q_ask_6 = source.q_ask_6,
  p_bid_7 = source.p_bid_7, p_ask_7 = source.p_ask_7, q_bid_7 = source.q_bid_7, q_ask_7 = source.q_ask_7,
  p_bid_8 = source.p_bid_8, p_ask_8 = source.p_ask_8, q_bid_8 = source.q_bid_8, q_ask_8 = source.q_ask_8,
  p_bid_9 = source.p_bid_9, p_ask_9 = source.p_ask_9, q_bid_9 = source.q_bid_9, q_ask_9 = source.q_ask_9,
  p_bid_10 = source.p_bid_10, p_ask_10 = source.p_ask_10, q_bid_10 = source.q_bid_10, q_ask_10 = source.q_ask_10
WHEN NOT MATCHED THEN INSERT ROW;
