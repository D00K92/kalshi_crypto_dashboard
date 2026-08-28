from dashboard.orderbook import orderbook_ladder


def test_orderbook_ladder_has_asks_spread_and_bids():
    component = orderbook_ladder({
        "asks": [{"price": "101", "total_quantity": "2", "venues": {"binance": "2"}}],
        "bids": [{"price": "99", "total_quantity": "3", "venues": {"coinbase": "3"}}],
    })
    rows = component.children[1].children
    assert len(rows) == 3
    assert rows[0].className == "book-row ask"
    assert rows[1].className == "book-spread"
    assert rows[2].className == "book-row bid"


def test_empty_book_has_safe_state():
    component = orderbook_ladder({})
    assert "Waiting for live order book" in component.children[1].children[0].children
