from __future__ import annotations

from typing import Any

import dash_ag_grid as dag


def _format_volume(value: Any) -> str:
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "0.0000"


def _rows(book: dict[str, Any]) -> list[dict[str, Any]]:
    asks = list(reversed(book.get("asks") or []))[:10]
    bids = list(book.get("bids") or [])[:10]
    asks += [{"price": "—", "total_quantity": "0"}] * (10 - len(asks))
    bids += [{"price": "—", "total_quantity": "0"}] * (10 - len(bids))
    levels = asks + bids
    max_volume = max((float(level.get("total_quantity") or 0) for level in levels), default=0.0)

    def level_row(side: str, level: dict[str, Any]) -> dict[str, Any]:
        volume = float(level.get("total_quantity") or 0)
        return {"side": side, "price": level.get("price", "—"), "volume": _format_volume(level.get("total_quantity")), "depth_pct": volume / max_volume if max_volume else 0.0}

    return ([level_row("ask", level) for level in asks]
            + [{"side": "spread", "price": "SPREAD", "volume": "—", "depth_pct": 0.0}]
            + [level_row("bid", level) for level in bids])


def orderbook_ladder(book: dict[str, Any]) -> dag.AgGrid:
    rows = _rows(book)
    return dag.AgGrid(
        id="orderbook-grid", rowData=rows,
        columnDefs=[
            {"field": "price", "headerName": "PRICE", "flex": 1, "sortable": False, "cellStyle": {"function": "priceColor(params)"}},
            {"field": "volume", "headerName": "VOLUME", "flex": 1, "sortable": False, "cellStyle": {"function": "volumeFill(params)"}},
        ],
        defaultColDef={"resizable": False}, columnSize="responsiveSizeToFit",
        className="ag-theme-quartz-dark orderbook-grid", style={"width": "100%"},
        dashGridOptions={"domLayout": "autoHeight", "headerHeight": 28, "rowHeight": 25, "suppressCellFocus": True, "suppressMovableColumns": True, "suppressRowHoverHighlight": True, "animateRows": False},
    )
