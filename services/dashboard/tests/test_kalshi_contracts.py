from dashboard.kalshi_contracts import contract_rows, contract_table, select_contract_window


def test_contract_rows_use_latest_active_event_and_recent_trade():
    rows = contract_rows(
        [
            {
                "event_ticker": "KXBTCD-NEW",
                "market_ticker": "KXBTCD-NEW-T70199.99",
                "yes_bid_dollars": "0.41",
                "yes_ask_dollars": "0.42",
                "last_price_dollars": "0.41",
                "volume": "115356.88",
                "open_interest": "36599.10",
                "received_ts_ms": 1700000000000,
            },
            {
                "event_ticker": "KXBTCD-OLD",
                "market_ticker": "KXBTCD-OLD-T70199.99",
                "yes_bid_dollars": "0.01",
                "yes_ask_dollars": "0.02",
                "received_ts_ms": 1700000000000,
            },
        ],
        [
            {
                "event_ticker": "KXBTCD-NEW",
                "market_ticker": "KXBTCD-NEW-T70199.99",
                "yes_price_dollars": "0.42",
                "count": "5.46",
                "taker_side": "yes",
                "received_ts_ms": 1700000000000,
            }
        ],
        now_ms=1700000005000,
    )

    assert rows == [
        {
            "event": "KXBTCD-NEW",
            "contract": "BTC > 70,199.99",
            "market_ticker": "KXBTCD-NEW-T70199.99",
            "strike": 70199.99,
            "bid_value": 41.0,
            "ask_value": 42.0,
            "spread_value": 1.0,
            "last_value": 41.0,
            "volume_value": 115356.88,
            "open_interest_value": 36599.10,
            "last_trade_value": 42.0,
            "last_trade_qty_value": 5.46,
            "bid": "41¢",
            "ask": "42¢",
            "spread": "1¢",
            "last": "41¢",
            "volume": "115,357",
            "open_interest": "36,599",
            "last_trade": "42¢",
            "last_trade_qty": "5.46",
            "last_trade_side": "YES",
            "age": "5s",
            "has_trade": True,
        }
    ]


def test_contract_table_builds_ag_grid():
    grid = contract_table([{"contract": "BTC > 70,199.99"}])

    assert grid.id == "kalshi-contract-grid"
    assert grid.rowData == [{"contract": "BTC > 70,199.99"}]
    assert grid.columnDefs[0]["field"] == "contract"


def test_select_contract_window_keeps_eight_strikes_each_side_of_atm():
    rows = [
        {"strike": float(strike), "market_ticker": f"M-T{strike}"}
        for strike in range(90, 111)
    ]

    selected = select_contract_window(rows, spot=100.2)

    assert [row["strike"] for row in selected] == list(range(92, 109))


def test_select_contract_window_uses_probability_when_spot_is_missing():
    rows = [
        {"strike": float(strike), "bid_value": bid, "ask_value": bid + 2, "market_ticker": f"M-T{strike}"}
        for strike, bid in ((90, 20), (100, 49), (110, 70))
    ]

    selected = select_contract_window(rows, spot=None, lower=1, upper=1)

    assert [row["strike"] for row in selected] == [90.0, 100.0, 110.0]


def test_contract_rows_discards_stale_event_before_selecting_active_event():
    rows = contract_rows(
        [
            {
                "event_ticker": "KXBTCD-OLD",
                "market_ticker": "KXBTCD-OLD-T70000",
                "yes_bid_dollars": "0.01",
                "yes_ask_dollars": "0.02",
                "received_ts_ms": 1_000,
            },
            {
                "event_ticker": "KXBTCD-NEW",
                "market_ticker": "KXBTCD-NEW-T70000",
                "yes_bid_dollars": "0.41",
                "yes_ask_dollars": "0.42",
                "received_ts_ms": 299_000,
            },
        ],
        [],
        now_ms=300_000,
    )

    assert [row["event"] for row in rows] == ["KXBTCD-NEW"]


def test_contract_rows_returns_empty_when_all_data_is_stale():
    assert contract_rows(
        [{"event_ticker": "KXBTCD-OLD", "market_ticker": "KXBTCD-OLD-T70000", "received_ts_ms": 1}],
        [],
        now_ms=300_002,
    ) == []
