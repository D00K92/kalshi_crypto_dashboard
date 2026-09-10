from dashboard.app import app, layout, refresh_kalshi_contracts, refresh_kalshi_monitor


def _component_ids(component):
    found = []
    stack = [component]
    while stack:
        current = stack.pop()
        identifier = getattr(current, "id", None)
        if identifier:
            found.append(identifier)
        children = getattr(current, "children", None)
        if isinstance(children, list):
            stack.extend(children)
        elif children is not None:
            stack.append(children)
    return found


def test_kalshi_components_are_present_in_initial_layout():
    ids = _component_ids(layout())

    assert "kalshi-market-structure" in ids
    assert "kalshi-monitor-waiting" in ids
    assert "kalshi-contract-grid" in ids
    assert "kalshi-contracts-waiting" in ids
    assert "kalshi-monitor-data" in ids
    assert "kalshi-table-data" in ids
    assert "kalshi-monitor-refresh" in ids
    assert "kalshi-table-refresh" in ids
    assert "volatility-refresh" in ids


def test_kalshi_callbacks_update_properties_not_component_children():
    callback_outputs = list(app.callback_map)

    assert any("kalshi-market-structure.figure" in output for output in callback_outputs)
    assert any("kalshi-contract-grid.rowData" in output for output in callback_outputs)
    assert "kalshi-monitor-data.data" in callback_outputs
    assert "kalshi-table-data.data" in callback_outputs
    assert not any("kalshi-data.data" in output for output in callback_outputs)


def test_daily_gap_hides_kalshi_monitor_and_contract_table():
    payload = {"contracts": [{"market_ticker": "E-T100"}], "spot": 70_000, "waiting_for_hourly_contract": True}

    _, summary_style, _, chart_style, monitor_waiting_style = refresh_kalshi_monitor(payload)
    _, grid_style, table_waiting_style = refresh_kalshi_contracts(payload)

    assert summary_style == {"display": "none"}
    assert chart_style == {"display": "none"}
    assert monitor_waiting_style == {}
    assert grid_style == {"display": "none"}
    assert table_waiting_style == {}
