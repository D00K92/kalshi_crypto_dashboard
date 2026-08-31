var dagfuncs = window.dashAgGridFunctions = window.dashAgGridFunctions || {};
dagfuncs.priceColor = function (params) {
    var side = (params.data || {}).side;
    if (side === "ask") return {color: "#f87171"};
    if (side === "bid") return {color: "#34d399"};
    return {color: "#94a3b8"};
};

dagfuncs.volumeFill = function (params) {
    var data = params.data || {};
    if (data.side === "spread") return {backgroundColor: "#172235", color: "#94a3b8", textAlign: "right"};
    if (data.side === "empty") return {color: "#64748b"};
    var pct = Math.max(0, Math.min(1, Number(data.depth_pct) || 0));
    var color = data.side === "ask" ? "rgba(248, 113, 113, 0.24)" : "rgba(52, 211, 153, 0.24)";
    var stop = (pct * 100).toFixed(3) + "%";
    return {background: "linear-gradient(to left, " + color + " 0%, " + color + " " + stop + ", transparent " + stop + ", transparent 100%)", textAlign: "right"};
};

dagfuncs.kalshiTradeSide = function (params) {
    var side = String(params.value || "");
    if (side === "YES") return {color: "#34d399", fontWeight: "700"};
    if (side === "NO") return {color: "#f87171", fontWeight: "700"};
    return {color: "#64748b"};
};
