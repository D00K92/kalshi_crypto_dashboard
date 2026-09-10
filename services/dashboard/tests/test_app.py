from dashboard.app import app, layout


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
    assert "kalshi-contract-grid" in ids
    assert "kalshi-monitor-data" in ids
    assert "kalshi-table-data" in ids
    assert "kalshi-monitor-refresh" in ids
    assert "kalshi-table-refresh" in ids
    assert "volatility-refresh" in ids


def test_kalshi_callbacks_update_properties_not_component_children():
    callback_outputs = list(app.callback_map)

    assert any("kalshi-market-structure.figure" in output for output in callback_outputs)
    assert "kalshi-contract-grid.rowData" in callback_outputs
    assert "kalshi-monitor-data.data" in callback_outputs
    assert "kalshi-table-data.data" in callback_outputs
    assert not any("kalshi-data.data" in output for output in callback_outputs)
