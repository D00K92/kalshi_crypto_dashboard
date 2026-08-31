from dashboard.orderbook import _rows, orderbook_ladder


def test_orderbook_rows_are_aggregated_and_include_spread():
    rows = _rows({
        "asks": [{"price": "101", "total_quantity": "2", "venues": {"binance": "2"}}],
        "bids": [{"price": "99", "total_quantity": "3", "venues": {"coinbase": "3"}}],
    })
    assert len(rows) == 21
    assert rows[0] == {"side": "ask", "price": "101", "volume": "2.0000", "depth_pct": 2 / 3}
    assert rows[10] == {"side": "spread", "price": "SPREAD", "volume": "—", "depth_pct": 0.0}
    assert rows[11] == {"side": "bid", "price": "99", "volume": "3.0000", "depth_pct": 1.0}
    assert orderbook_ladder({}).rowData[0]["side"] == "ask"


def test_orderbook_volume_rounds_to_four_decimal_places():
    rows = _rows({"asks": [{"price": "101", "total_quantity": "1.234567"}]})
    assert rows[0]["volume"] == "1.2346"
    assert rows[0]["depth_pct"] == 1.0


def test_empty_book_has_safe_state():
    rows = orderbook_ladder({}).rowData
    assert len(rows) == 21
    assert all(row["price"] == "—" for row in rows[:10] + rows[11:])
