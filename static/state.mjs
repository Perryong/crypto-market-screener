export const panels = [
  "candles",
  "orderbook",
  "cvd",
  "footprint",
  "heatmap",
  "dom",
  "trades",
  "liquidations",
  "screener",
  "news",
];
export const defaultSettings = {
  symbol: "BTCUSDT",
  interval: "1m",
  tickSize: 10,
  density: "comfortable",
  hidden: [],
};
export function loadSettings(storage) {
  let saved = {};
  try {
    saved = JSON.parse(storage.getItem("cryexc.workspace.v1")) || {};
  } catch {}
  return {
    symbol: /^[A-Z0-9_]{2,30}$/.test(saved.symbol) ? saved.symbol : "BTCUSDT",
    interval: ["1m", "5m", "15m", "1h", "4h"].includes(saved.interval)
      ? saved.interval
      : "1m",
    tickSize:
      Number.isFinite(saved.tickSize) && saved.tickSize > 0
        ? saved.tickSize
        : 10,
    density: saved.density === "compact" ? "compact" : "comfortable",
    hidden: Array.isArray(saved.hidden)
      ? saved.hidden.filter((p) => panels.includes(p))
      : [],
  };
}
export function saveSettings(storage, settings) {
  try {
    storage.setItem("cryexc.workspace.v1", JSON.stringify(settings));
  } catch {}
}
export function safeNewsUrl(value) {
  try {
    const u = new URL(value);
    return ["http:", "https:"].includes(u.protocol) ? u.href : null;
  } catch {
    return null;
  }
}
export function replacePoints(rows) {
  return [...new Map(rows.map((r) => [r.timestamp, r])).values()]
    .sort((a, b) => a.timestamp - b.timestamp)
    .slice(-1000);
}
export function priceDecimals(tick) {
  return Math.min(
    12,
    (Number(tick).toFixed(12).replace(/0+$/, "").split(".")[1] || "").length,
  );
}
export function screenMarkets(rows, query, sort) {
  return rows
    .filter((r) => r.symbol.toLowerCase().includes(query.toLowerCase()))
    .sort((a, b) => (b[sort] || 0) - (a[sort] || 0));
}
