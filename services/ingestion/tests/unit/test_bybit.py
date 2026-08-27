from ingestion.adapters.bybit import BybitBook, parse_bybit_message


def test_parse_bybit_trade_batch() -> None:
    events = parse_bybit_message('{"topic":"publicTrade.BTCUSDT","data":[{"T":1700000000000,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100000","i":"trade-1"}]}', received_ts_ms=2)
    assert events and events[0].event_id == "bybit:BTCUSDT:trade:trade-1"


def test_bybit_book_snapshot_and_delta() -> None:
    book = BybitBook("BTCUSDT")
    snapshot = book.apply({'type':'snapshot','ts':1,'data':{'u':1,'b':[['99','1']], 'a':[['101','1']]}}, 2)
    assert snapshot and snapshot.bids[0].price == '99'
    updated = book.apply({'type':'delta','ts':2,'data':{'u':2,'b':[['100','2']], 'a':[]}}, 3)
    assert updated and updated.bids[0].price == '100'
