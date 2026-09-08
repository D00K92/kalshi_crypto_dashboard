"""Load one GCS raw-data hour into canonical BigQuery raw tables."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from uuid import uuid4
import pandas as pd
from google.cloud import bigquery, storage

INSTRUMENTS={"binance":"BTCUSDT","coinbase":"BTC-USD","kraken":"BTC_USD"}

def _timestamp(value):
    return pd.to_datetime(value, unit="ms", utc=True)

def normalize_trades(frame, *, venue, instrument, source_object):
    """Normalize a raw trade frame for callers that need local transformation."""
    result = frame.copy()
    result["event_timestamp"] = _timestamp(result["exchange_ts_ms"])
    result["received_timestamp"] = _timestamp(result.get("received_ts_ms", result["exchange_ts_ms"]))
    result["venue"] = venue
    result["instrument"] = instrument
    result["source_object"] = source_object
    return result

def normalize_books(frame, *, venue, instrument, source_object):
    """Expand bid/ask JSON arrays into canonical level rows."""
    rows = []
    for record in frame.to_dict("records"):
        timestamp = _timestamp(record["exchange_ts_ms"])
        received = _timestamp(record.get("received_ts_ms", record["exchange_ts_ms"]))
        for side, field in (("bid", "bids"), ("ask", "asks")):
            values = record.get(field) or []
            if isinstance(values, str):
                values = json.loads(values)
            for level, quote in enumerate(values, start=1):
                rows.append({"event_timestamp": timestamp, "received_timestamp": received,
                             "venue": venue, "instrument": instrument, "side": side,
                             "level": level, "price": float(quote["price"]),
                             "quantity": float(quote["quantity"]), "source_object": source_object})
    return pd.DataFrame(rows)

def _uris(bucket,kind,venue,instrument,day,hour,project):
    client=storage.Client(project=project); prefix=f"{kind}/venue={venue}/instrument={instrument}/date={day}/hour={hour}/"
    return sorted(f"gs://{bucket}/{b.name}" for b in client.list_blobs(bucket,prefix=prefix) if b.name.endswith(".parquet"))

def _load(client,uris,table):
    if not uris: return 0
    cfg=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,autodetect=True,write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    client.load_table_from_uri(uris,table,job_config=cfg).result(); return len(uris)

def _replace_trades(client,landing,target,venue,instrument,start,end):
    client.query(f"DELETE FROM `{target}` WHERE event_timestamp>=@start AND event_timestamp<@end AND venue=@venue AND instrument=@instrument",job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("start","TIMESTAMP",start),bigquery.ScalarQueryParameter("end","TIMESTAMP",end),bigquery.ScalarQueryParameter("venue","STRING",venue),bigquery.ScalarQueryParameter("instrument","STRING",instrument)])).result()
    sql=f"""INSERT INTO `{target}` (event_timestamp,received_timestamp,venue,instrument,trade_id,price,quantity,taker_side,source_object,ingested_at) SELECT TIMESTAMP_MILLIS(exchange_ts_ms),TIMESTAMP_MILLIS(COALESCE(received_ts_ms,exchange_ts_ms)),@venue,@instrument,COALESCE(trade_id,event_id,redis_id),price,quantity,taker_side,@source,CURRENT_TIMESTAMP() FROM `{landing}` WHERE TIMESTAMP_MILLIS(exchange_ts_ms)>=@start AND TIMESTAMP_MILLIS(exchange_ts_ms)<@end"""
    client.query(sql,job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("start","TIMESTAMP",start),bigquery.ScalarQueryParameter("end","TIMESTAMP",end),bigquery.ScalarQueryParameter("venue","STRING",venue),bigquery.ScalarQueryParameter("instrument","STRING",instrument),bigquery.ScalarQueryParameter("source","STRING",landing)])).result()

def _replace_books(client,landing,target,venue,instrument,start,end):
    client.query(f"DELETE FROM `{target}` WHERE event_timestamp>=@start AND event_timestamp<@end AND venue=@venue AND instrument=@instrument",job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("start","TIMESTAMP",start),bigquery.ScalarQueryParameter("end","TIMESTAMP",end),bigquery.ScalarQueryParameter("venue","STRING",venue),bigquery.ScalarQueryParameter("instrument","STRING",instrument)])).result()
    parts=[]
    for side,col in (("bid","bids"),("ask","asks")):
        parts.append(f"SELECT TIMESTAMP_MILLIS(exchange_ts_ms),TIMESTAMP_MILLIS(COALESCE(received_ts_ms,exchange_ts_ms)),@venue,@instrument,@side,off+1,SAFE_CAST(JSON_VALUE(x,'$.price') AS FLOAT64),SAFE_CAST(JSON_VALUE(x,'$.quantity') AS FLOAT64),@source,CURRENT_TIMESTAMP() FROM `{landing}`,UNNEST(JSON_QUERY_ARRAY({col})) x WITH OFFSET off WHERE TIMESTAMP_MILLIS(exchange_ts_ms)>=@start AND TIMESTAMP_MILLIS(exchange_ts_ms)<@end")
    sql=f"INSERT INTO `{target}` (event_timestamp,received_timestamp,venue,instrument,side,level,price,quantity,source_object,ingested_at) " + " UNION ALL ".join(parts)
    cfg=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("start","TIMESTAMP",start),bigquery.ScalarQueryParameter("end","TIMESTAMP",end),bigquery.ScalarQueryParameter("venue","STRING",venue),bigquery.ScalarQueryParameter("instrument","STRING",instrument),bigquery.ScalarQueryParameter("side","STRING","bid"),bigquery.ScalarQueryParameter("source","STRING",landing)])
    # side is embedded as a literal per branch to avoid parameter type ambiguity.
    sql=sql.replace("@side,", "'bid',", 1).replace("@side,", "'ask',", 1)
    cfg.query_parameters=[x for x in cfg.query_parameters if x.name!="side"]
    client.query(sql,job_config=cfg).result()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--date",required=True); ap.add_argument("--hour",required=True); ap.add_argument("--venue",required=True); ap.add_argument("--instrument"); ap.add_argument("--bucket",default="kalshi-crypto-tick-data"); ap.add_argument("--project",default="kalshi-crypto-506614"); ap.add_argument("--types",default="trades,books"); a=ap.parse_args()
    target=datetime.fromisoformat(f"{a.date}T{a.hour}:00:00+00:00"); end=target.replace(minute=0)+__import__('datetime').timedelta(hours=1); instrument=a.instrument or INSTRUMENTS.get(a.venue,"BTCUSD")
    c=bigquery.Client(project=a.project,location="asia-northeast3"); kinds={x.strip() for x in a.types.split(",")}
    # Retries or an accidentally duplicated invocation must never share a
    # mutable landing table. A unique table also makes cleanup by one worker
    # unable to delete another worker's in-flight destination.
    run_suffix = f"{a.date.replace('-', '')}_{a.hour}_{uuid4().hex}"
    for kind in kinds:
        uris=_uris(a.bucket,"ticks" if kind=="trades" else "books",a.venue,instrument,a.date,a.hour,a.project)
        if not uris: print(f"{kind} source_files=0 rows=0",flush=True); continue
        landing=f"{a.project}.market_data._landing_{kind}_{a.venue.replace('.','_').replace('-','_')}_{run_suffix}"; n=_load(c,uris,landing)
        if kind=="trades": _replace_trades(c,landing,f"{a.project}.market_data.raw_trades",a.venue,instrument,target,end)
        else: _replace_books(c,landing,f"{a.project}.market_data.raw_book_levels",a.venue,instrument,target,end)
        c.delete_table(landing,not_found_ok=True); print(f"{kind} source_files={n} loaded=1",flush=True)
if __name__=="__main__": main()
