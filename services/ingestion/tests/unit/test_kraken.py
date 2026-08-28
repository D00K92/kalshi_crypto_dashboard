from ingestion.adapters.kraken import KrakenBook, KrakenMessageError, parse_kraken_message, trade_from_message


def test_parse_kraken_trade() -> None:
    messages = parse_kraken_message(
        '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","price":100.5,"qty":0.2,"side":"buy","trade_id":42,"timestamp":"2024-01-01T00:00:00.000000Z"}]}',
        received_ts_ms=2,
    )
    trade = trade_from_message(messages[0])
    assert trade.event_id == "kraken:BTC/USD:trade:42"
    assert trade.price == "100.5"
    assert trade.taker_side == "buy"


def test_parse_kraken_ignores_subscription_ack() -> None:
    assert parse_kraken_message('{"channel":"book","type":"subscribe","success":true}') == []


def test_parse_kraken_book_frame_keeps_frame_type() -> None:
    messages = parse_kraken_message(
        '{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[],"asks":[]}]}',
        received_ts_ms=2,
    )
    assert messages[0]["type"] == "snapshot"


def test_kraken_book_snapshot_and_update() -> None:
    book = KrakenBook("BTC/USD")
    snapshot = book.apply(
        {"type": "snapshot", "timestamp": "2024-01-01T00:00:00.000000Z", "bids": [{"price": "99", "qty": "1"}], "asks": [{"price": "101", "qty": "2"}]},
        2,
    )
    assert snapshot and snapshot.bids[0].price == "99"
    updated = book.apply(
        {"type": "update", "bids": [{"price": "100", "qty": "3"}], "asks": [{"price": "101", "qty": "0"}, {"price": "102", "qty": "1"}]},
        3,
    )
    assert updated and updated.bids[0].price == "100"
    assert updated.asks[0].price == "102"
    assert updated.depth <= 15


def test_kraken_parser_rejects_malformed_book_levels() -> None:
    book = KrakenBook("BTC/USD")
    try:
        book.apply({"type": "snapshot", "bids": [{"price": "not-a-price", "qty": "1"}], "asks": []}, 2)
    except KrakenMessageError as exc:
        assert "price" in str(exc)
    else:
        raise AssertionError("malformed Kraken price was accepted")
