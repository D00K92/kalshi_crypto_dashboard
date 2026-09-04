-- Parameterized 1m resampler. Parameters: @source_start, @target_start,
-- @target_end, @venue, @instrument. Includes preceding-hour book state.
MERGE `kalshi-crypto-506614.market_data.bars` target
USING (
  WITH buckets AS (
    SELECT ts AS event_timestamp FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
      @source_start, TIMESTAMP_SUB(@target_end, INTERVAL 1 MINUTE), INTERVAL 1 MINUTE)) ts
  ),
  t AS (
    SELECT TIMESTAMP_TRUNC(event_timestamp, MINUTE) bucket, venue, instrument,
      ARRAY_AGG(price ORDER BY event_timestamp, COALESCE(CAST(trade_id AS STRING), FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', received_timestamp)) LIMIT 1)[OFFSET(0)] p_open,
      ARRAY_AGG(price ORDER BY event_timestamp DESC, COALESCE(CAST(trade_id AS STRING), FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', received_timestamp)) DESC LIMIT 1)[OFFSET(0)] p_trade,
      MAX(price) p_high, MIN(price) p_low, AVG(price) p_trade_mean,
      SUM(quantity) v_trade, SUM(IF(taker_side='buy',quantity,0)) v_buy,
      SUM(IF(taker_side='sell',quantity,0)) v_sell, COUNTIF(quantity>0) cnt_trade,
      AVG(TIMESTAMP_DIFF(event_timestamp, previous_trade_timestamp, MILLISECOND)) dt_fill_mean_ms,
      MAX(TIMESTAMP_DIFF(event_timestamp, previous_trade_timestamp, MILLISECOND)) dt_fill_max_ms,
      MIN(TIMESTAMP_DIFF(event_timestamp, previous_trade_timestamp, MILLISECOND)) dt_fill_min_ms
    FROM (
      SELECT *, LAG(event_timestamp) OVER (
        PARTITION BY venue, instrument ORDER BY event_timestamp, trade_id, received_timestamp
      ) previous_trade_timestamp
      FROM `kalshi-crypto-506614.market_data.raw_trades`
    )
    WHERE event_timestamp >= @source_start AND event_timestamp < @target_end
      AND venue=@venue AND instrument=@instrument
    GROUP BY 1,2,3
  ),
  latest AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT TIMESTAMP_TRUNC(event_timestamp, MINUTE) bucket, venue, instrument, side, level, price, quantity,
        ROW_NUMBER() OVER (PARTITION BY TIMESTAMP_TRUNC(event_timestamp,MINUTE),venue,instrument,side,level
          ORDER BY event_timestamp DESC, received_timestamp DESC) rn
      FROM `kalshi-crypto-506614.market_data.raw_book_levels`
      WHERE event_timestamp >= @source_start AND event_timestamp < @target_end
        AND venue=@venue AND instrument=@instrument
    ) WHERE rn=1
  ),
  b AS (
    SELECT bucket, venue, instrument,
      ARRAY_AGG(STRUCT(side,level,price,quantity) ORDER BY side,level) levels
    FROM latest GROUP BY 1,2,3
  ),
  assembled AS (
    SELECT k.event_timestamp, @venue venue, @instrument instrument,
      t.p_open,t.p_high,t.p_low,t.p_trade,t.p_trade_mean,t.v_trade,t.v_buy,t.v_sell,t.cnt_trade,
      t.dt_fill_mean_ms,t.dt_fill_max_ms,t.dt_fill_min_ms,
      b.levels
    FROM buckets k
    LEFT JOIN t ON t.bucket=k.event_timestamp
    LEFT JOIN b ON b.bucket=k.event_timestamp AND b.venue=@venue AND b.instrument=@instrument
  )
  SELECT event_timestamp, CURRENT_TIMESTAMP() created_timestamp, venue, instrument, '1m' frequency,
    p_open,p_high,p_low,p_trade p_close,p_trade,p_trade_mean,
    COALESCE(v_trade,0) v_trade,COALESCE(v_buy,0) v_buy,COALESCE(v_sell,0) v_sell,COALESCE(cnt_trade,0) cnt_trade,
    dt_fill_mean_ms,dt_fill_max_ms,dt_fill_min_ms,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=1) p_bid_1,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=1) p_ask_1,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=1) q_bid_1,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=1) q_ask_1,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=2) p_bid_2,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=2) p_ask_2,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=2) q_bid_2,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=2) q_ask_2,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=3) p_bid_3,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=3) p_ask_3,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=3) q_bid_3,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=3) q_ask_3,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=4) p_bid_4,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=4) p_ask_4,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=4) q_bid_4,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=4) q_ask_4,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=5) p_bid_5,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=5) p_ask_5,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=5) q_bid_5,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=5) q_ask_5,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=6) p_bid_6,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=6) p_ask_6,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=6) q_bid_6,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=6) q_ask_6,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=7) p_bid_7,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=7) p_ask_7,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=7) q_bid_7,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=7) q_ask_7,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=8) p_bid_8,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=8) p_ask_8,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=8) q_bid_8,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=8) q_ask_8,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=9) p_bid_9,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=9) p_ask_9,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=9) q_bid_9,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=9) q_ask_9,
    (SELECT price FROM UNNEST(levels) WHERE side='bid' AND level=10) p_bid_10,
    (SELECT price FROM UNNEST(levels) WHERE side='ask' AND level=10) p_ask_10,
    (SELECT quantity FROM UNNEST(levels) WHERE side='bid' AND level=10) q_bid_10,
    (SELECT quantity FROM UNNEST(levels) WHERE side='ask' AND level=10) q_ask_10
  FROM assembled WHERE event_timestamp>=@target_start AND event_timestamp<@target_end
) source
ON target.event_timestamp=source.event_timestamp AND target.venue=source.venue AND target.instrument=source.instrument AND target.frequency=source.frequency
WHEN MATCHED THEN UPDATE SET p_open=source.p_open,p_high=source.p_high,p_low=source.p_low,p_close=source.p_close,p_trade=source.p_trade,p_trade_mean=source.p_trade_mean,v_trade=source.v_trade,v_buy=source.v_buy,v_sell=source.v_sell,cnt_trade=source.cnt_trade
WHEN NOT MATCHED THEN INSERT ROW;
