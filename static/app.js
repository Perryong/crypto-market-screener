import {
  panels,
  defaultSettings,
  loadSettings,
  saveSettings,
  safeNewsUrl,
  priceDecimals,
  replacePoints,
  screenMarkets,
} from "./state.mjs";
import { createChart } from "./charts.js";
import { API_BASE } from './config.mjs';
import { backendURLs } from './deployment.mjs';
let endpoints, deploymentError;
try { endpoints = backendURLs(API_BASE, location.origin); }
catch (error) { deploymentError = error.message; }
const $ = (id) => document.getElementById(id);
let storage;
try {
  storage = window.localStorage;
} catch {
  storage = null;
}
let settings = loadSettings(storage),
  metadata = {},
  markets = [],
  socket = null,
  generation = 0,
  subscriptions = [],
  retryTimer,
  retries = 0,
  controller = null;
let candles = [],
  footprint = [],
  heatmap = [],
  cvd = [],
  trades = [],
  liquidations = [],
  news = [],
  book = null,
  dom = [],
  stats = null,
  status = null,
  frame = 0;
let candleReady = false,
  seedPending = false,
  lastSeed = 0,
  lastTradeTime = 0,
  disposed = false,
  pendingCandles = [];
const paused = { trades: false, liquidations: false };
const intervals = {
  "1m": 60000,
  "5m": 300000,
  "15m": 900000,
  "1h": 3600000,
  "4h": 14400000,
};
const format = (n, d = 2) =>
  n == null || !Number.isFinite(+n)
    ? "—"
    : (+n).toLocaleString("en-US", {
        minimumFractionDigits: d,
        maximumFractionDigits: d,
      });
const compact = (n) =>
  n == null || !Number.isFinite(+n)
    ? "—"
    : (+n).toLocaleString("en-US", {
        notation: "compact",
        maximumFractionDigits: 2,
      });
function precision() {
  return priceDecimals(metadata[settings.symbol]?.tickSize || 0.1);
}
const price = (n) => format(n, precision());
const time = (n) => new Date(n).toLocaleTimeString([], { hour12: false });
const text = (id, value) => {
  $(id).textContent = value;
};
function signed(id, n, suffix = "") {
  text(id, n == null ? "—" : `${n >= 0 ? "+" : ""}${format(n)}${suffix}`);
  $(id).classList.toggle("positive", n >= 0);
  $(id).classList.toggle("negative", n < 0);
}
function notice(message = "") {
  text("notice", message);
  $("notice").hidden = !message;
}
function persist() {
  saveSettings(storage, settings);
}
const charts = {};
for (const kind of ["candles", "cvd", "footprint", "heatmap"])
  charts[kind] = createChart($(kind + "-chart"), {
    kind,
    onHover: (r) =>
      text(
        "ohlc",
        `${settings.symbol}  O ${price(r.open)}  H ${price(r.high)}  L ${price(r.low)}  C ${price(r.close)}  V ${compact(r.volume)}`,
      ),
  });
function renderSoon() {
  if (!frame)
    frame = requestAnimationFrame(() => {
      frame = 0;
      render();
    });
}
function empty(el, message) {
  el.replaceChildren();
  const p = document.createElement("p");
  p.className = "empty";
  p.textContent = message;
  el.append(p);
}
function row(values, className = "three", side = "") {
  const el = document.createElement("div");
  el.className = `data-row ${className}`;
  values.forEach((value, i) => {
    const span = document.createElement("span");
    span.textContent = value;
    if (i === 0 && side)
      span.className = side === "buy" ? "positive" : "negative";
    el.append(span);
  });
  return el;
}
function renderBook() {
  for (const side of ["asks", "bids"]) {
    const root = $(side);
    root.replaceChildren();
    if (!book) continue;
    let total = 0;
    let rows = (book[side] || [])
      .slice(0, 11)
      .map((l) => ({ ...l, total: (total += l.quantity) }));
    if (side === "asks") rows.reverse();
    const max = Math.max(1, ...rows.map((l) => l.total));
    for (const l of rows) {
      const el = row(
        [price(l.price), compact(l.quantity), compact(l.total)],
        "three book-row",
        side === "bids" ? "buy" : "sell",
      );
      el.style.setProperty("--bar", `${(l.total / max) * 75}%`);
      el.style.setProperty(
        "--bar-color",
        side === "bids" ? "#53d7ae12" : "#ef7b8312",
      );
      root.append(el);
    }
  }
  const b = book?.bids?.[0]?.price,
    a = book?.asks?.[0]?.price;
  text("book-mid", a && b ? price((a + b) / 2) : "—");
  text("spread", a && b ? price(a - b) : "—");
  if (book) {
    const bid = book.bids.reduce((s, l) => s + l.quantity, 0),
      ask = book.asks.reduce((s, l) => s + l.quantity, 0),
      pct = (bid / (bid + ask || 1)) * 100;
    text("imbalance", `${format(pct, 0)}% / ${format(100 - pct, 0)}%`);
    $("balance-bar").style.width = pct + "%";
  }
  const root = $("dom-list");
  root.replaceChildren();
  const middle = book && a && b ? (a + b) / 2 : 0;
  const levels = [...dom]
    .sort((a, b) => Math.abs(a.price - middle) - Math.abs(b.price - middle))
    .slice(0, 18)
    .sort((a, b) => b.price - a.price);
  if (!book || !levels.length) empty(root, "Waiting for synchronized depth…");
  else
    levels.forEach((l) => {
      const el = row(
        [
          compact(l.bid),
          price(l.price),
          compact(l.ask),
          compact(l.bought),
          compact(l.sold),
          compact(l.delta),
        ],
        "dom-row",
      );
      el.title = `Total traded volume: ${compact(l.volume)}`;
      root.append(el);
    });
}
function renderActivity(kind) {
  if (paused[kind]) return;
  const isTrade = kind === "trades",
    side = $(isTrade ? "trade-side" : "liq-side").value,
    min = Math.max(0, +$(isTrade ? "trade-min" : "liq-min").value || 0);
  const rows = (isTrade ? trades : liquidations).filter(
    (r) =>
      (r.quoteQty ?? r.notionalUsd) >= min &&
      (!side ||
        (isTrade ? (r.isBuyerMaker ? "sell" : "buy") : r.side.toLowerCase()) ===
          side),
  );
  const root = $(kind + "-list");
  root.replaceChildren();
  if (!rows.length) {
    empty(
      root,
      isTrade
        ? "No trades match this filter yet"
        : "No liquidation events match this filter yet",
    );
    return;
  }
  for (const r of rows.slice(0, 100)) {
    const s = isTrade
      ? r.isBuyerMaker
        ? "sell"
        : "buy"
      : r.side.toLowerCase();
    root.append(
      row(
        [
          `${price(r.price)} ${s === "buy" ? "↑" : "↓"}`,
          compact(r.quoteQty ?? r.notionalUsd),
          time(r.timestamp),
        ],
        "three",
        s,
      ),
    );
  }
}
function renderMarkets() {
  const root = $("screener-list");
  root.replaceChildren();
  const rows = screenMarkets(
    markets,
    $("market-search").value,
    $("market-sort").value,
  );
  text("market-count", `${rows.length} MARKETS`);
  if (!rows.length) {
    empty(root, "No matching markets");
    return;
  }
  for (const r of rows.slice(0, 100)) {
    const el = row(
      [
        "",
        format(r.lastPrice, r.lastPrice < 1 ? 6 : 2),
        `${r.priceChangePercent >= 0 ? "+" : ""}${format(r.priceChangePercent)}%`,
        compact(r.quoteVolume24h),
        `${compact(r.low24h)} – ${compact(r.high24h)}`,
      ],
      "screener-row",
    );
    const button = document.createElement("button");
    button.textContent = r.symbol;
    button.onclick = () => selectSymbol(r.symbol);
    el.firstChild.append(button);
    el.children[2].className =
      r.priceChangePercent >= 0 ? "positive" : "negative";
    root.append(el);
  }
}
function renderNews() {
  const root = $("news-list");
  root.replaceChildren();
  if (!news.length) {
    empty(
      root,
      status?.news?.connected
        ? "Waiting for news…"
        : "News feed unavailable · market data continues",
    );
    return;
  }
  for (const item of news.slice(0, 100)) {
    const article = document.createElement("article");
    article.className = "news-item";
    const stamp = document.createElement("time");
    stamp.textContent = `TREE OF ALPHA · ${time(item.timestamp)}`;
    const url = safeNewsUrl(item.url),
      title = document.createElement(url ? "a" : "span");
    title.textContent = item.title || item.body || "News update";
    if (url) {
      title.href = url;
      title.target = "_blank";
      title.rel = "noopener noreferrer";
    }
    article.append(stamp, title);
    root.append(article);
  }
}
function render() {
  charts.candles.setData(candles);
  charts.cvd.setData(cvd);
  charts.footprint.setData(footprint);
  charts.heatmap.setData(heatmap);
  const ticker = markets.find((r) => r.symbol === settings.symbol);
  text(
    "last-price",
    price(trades[0]?.price ?? candles.at(-1)?.close ?? ticker?.lastPrice),
  );
  signed("price-change", ticker?.priceChangePercent, "%");
  text("high", price(ticker?.high24h));
  text("low", price(ticker?.low24h));
  text("volume", compact(ticker?.quoteVolume24h));
  const now = Math.max(status?.serverTime || 0, Date.now());
  text(
    "funding",
    stats?.fundingRate == null
      ? "—"
      : now - stats.timestamp > 15000
        ? "Stale"
        : format(stats.fundingRate * 100, 4) + "%",
  );
  text(
    "oi",
    stats?.openInterestUsd == null
      ? "—"
      : now - stats.oiTimestamp > 360000
        ? "Stale"
        : compact(stats.openInterestUsd),
  );
  signed("cvd-value", cvd.at(-1)?.value);
  renderBook();
  renderActivity("trades");
  renderActivity("liquidations");
  for (const [kind, rows] of Object.entries({ cvd, footprint, heatmap })) {
    const el = document.querySelector(`[data-kind="${kind}"]`);
    el.textContent = rows.length
      ? `Available from ${time(rows[0].timestamp)}`
      : "Collecting history";
  }
}
function applySettings() {
  $("symbol").value = settings.symbol;
  $("tick").value = settings.tickSize;
  $("tick").min = metadata[settings.symbol]?.tickSize || 0.1;
  $("tick").step = metadata[settings.symbol]?.tickSize || 0.1;
  for (const el of document.querySelectorAll("[data-interval]"))
    el.classList.toggle("active", el.dataset.interval === settings.interval);
  for (const el of document.querySelectorAll(".market-name"))
    el.textContent = settings.symbol;
  for (const p of panels) $("panel-" + p).hidden = settings.hidden.includes(p);
  document.body.classList.toggle("compact", settings.density === "compact");
  $("density").value = settings.density;
  for (const el of document.querySelectorAll("[data-panel]"))
    el.checked = !settings.hidden.includes(el.dataset.panel);
}
async function api(path, signal) {
  if (!endpoints) throw new Error(deploymentError);
  const r = await fetch(endpoints.http + path, { signal });
  if (!r.ok) {
    let detail;
    try {
      detail = (await r.json()).detail;
    } catch {}
    throw Error(
      typeof detail === "string" ? detail : `Request failed (${r.status})`,
    );
  }
  return r.json();
}
function send(msg) {
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(msg));
}
function subscribe() {
  for (const sub of subscriptions)
    send({
      type: "stream_unsubscribe",
      stream: sub.stream,
      instanceId: sub.instanceId,
    });
  const config = {
    symbol: settings.symbol,
    exchange: "binancef",
    exchanges: ["binancef"],
    interval: settings.interval,
    tickSize: +settings.tickSize,
    timeRange: "1h",
    depth: 30,
  };
  subscriptions = [
    "trade",
    "kline",
    "orderbook",
    "dom",
    "cvd",
    "footprint",
    "orderbook_heatmap",
    "liquidation",
    "news",
  ].map((stream) => ({
    stream,
    instanceId: `${generation}:${stream}`,
    config: stream === "news" ? {} : config,
  }));
  send({ type: "stream_subscribe_batch", subscriptions });
}
async function seedCandles() {
  if (seedPending) return;
  seedPending = true;
  pendingCandles = [];
  lastSeed = Date.now();
  const gen = generation,
    symbol = settings.symbol,
    interval = settings.interval;
  try {
    // Exchange kline updates replace the current candle, never add volume to a REST seed.
    const snapshot = await api(
      "/api/candles?" + new URLSearchParams({ symbol, interval, limit: 300 }),
      controller?.signal,
    );
    if (gen !== generation) return;
    candles = snapshot.candles;
    candles = replacePoints([...candles, ...pendingCandles]);
    candleReady = true;
    lastSeed = Date.now();
    text("candle-status", `${candles.length} candles · ${interval}`);
    text("ohlc", `${symbol} · ${interval} · Binance Futures`);
    renderSoon();
  } catch (e) {
    if (gen === generation && e.name !== "AbortError") {
      text("candle-status", "History unavailable");
      notice(e.message);
    }
  } finally {
    if (gen === generation) {
      seedPending = false;
      pendingCandles = [];
    }
  }
}
function changeSelection() {
  generation++;
  controller?.abort();
  controller = new AbortController();
  candles = [];
  footprint = [];
  heatmap = [];
  cvd = [];
  trades = [];
  liquidations = [];
  book = null;
  dom = [];
  stats = null;
  candleReady = false;
  seedPending = false;
  pendingCandles = [];
  lastTradeTime = 0;
  for (const kind of ["trades", "liquidations"])
    empty(
      $(kind + "-list"),
      paused[kind]
        ? "Paused · resume to see this market"
        : "Waiting for this market…",
    );
  Object.values(charts).forEach((chart) => chart.reset());
  text("candle-status", "Loading history…");
  text("ohlc", "Loading Binance candles…");
  notice();
  applySettings();
  persist();
  renderSoon();
  subscribe();
  seedCandles();
  refreshStats();
}
function selectSymbol(symbol) {
  symbol = symbol.trim().toUpperCase();
  if (!metadata[symbol]) {
    notice("Choose a valid Binance Futures market");
    $("symbol").value = settings.symbol;
    return;
  }
  if (symbol === settings.symbol) return;
  settings.symbol = symbol;
  const base = +metadata[symbol].tickSize;
  settings.tickSize = base * 100;
  changeSelection();
}
function onMessage(event) {
  let msg;
  try {
    msg = JSON.parse(event.data);
  } catch {
    return;
  }
  if (msg.type === "error") {
    notice(msg.message);
    return;
  }
  if (msg.instanceId && !msg.instanceId.startsWith(`${generation}:`)) return;
  const d = msg.data;
  if (!d) return;
  if (
    (msg.symbol && msg.symbol !== settings.symbol) ||
    (d.symbol && d.symbol !== settings.symbol)
  )
    return;
  switch (msg.type) {
    case "trade":
      trades.unshift(d);
      trades.length = Math.min(200, trades.length);
      lastTradeTime = Math.max(lastTradeTime, d.timestamp);
      break;
    case "kline":
      if (d.interval !== settings.interval) break;
      if (seedPending)
        pendingCandles = replacePoints([...pendingCandles, d.candle]);
      candles = replacePoints([...candles, d.candle]);
      break;
    case "orderbook":
      book = d;
      break;
    case "dom":
      dom = d.levels || [];
      break;
    case "cvd_historical":
      cvd = replacePoints(d);
      break;
    case "footprint_historical":
      footprint = (d.candles || []).slice(-240);
      break;
    case "footprint":
      footprint = replacePoints([...footprint, d.candle]).slice(-240);
      break;
    case "orderbook_heatmap":
      heatmap = d.historical
        ? d.historical.slice(-240)
        : replacePoints([...heatmap, d.live]).slice(-240);
      break;
    case "liquidation":
      liquidations.unshift(d);
      liquidations.length = Math.min(100, liquidations.length);
      break;
    case "news_historical":
      news = [...d].reverse();
      renderNews();
      break;
    case "news":
      news.unshift(d);
      news.length = Math.min(100, news.length);
      renderNews();
      break;
  }
  renderSoon();
}
function connect() {
  clearTimeout(retryTimer);
  if (disposed) return;
  if (!endpoints) { notice(deploymentError); return; }
  const ws = new WebSocket(endpoints.ws);
  socket = ws;
  ws.onopen = () => {
    if (ws !== socket) return;
    retries = 0;
    subscriptions = [];
    changeSelection();
    refreshStatus();
  };
  ws.onmessage = (e) => {
    if (ws === socket) onMessage(e);
  };
  ws.onclose = () => {
    if (ws !== socket || disposed) return;
    text("connection-status", "Disconnected · retrying");
    $("status-dot").classList.remove("live");
    book = null;
    dom = [];
    renderSoon();
    retryTimer = setTimeout(
      connect,
      Math.min(30000, 1000 * 2 ** retries++) + Math.random() * 300,
    );
  };
  ws.onerror = () => ws.close();
}
async function refreshStatus() {
  try {
    status = await api("/api/status");
    const s = status.symbols[settings.symbol],
      now = status.serverTime;
    const live =
      socket?.readyState === WebSocket.OPEN &&
      status.marketConnected &&
      s &&
      now - s.lastTradeAt < 15000;
    text(
      "connection-status",
      live
        ? "Live · " + time(s.lastTradeAt)
        : socket?.readyState === WebSocket.OPEN
          ? s?.lastTradeAt
            ? "Market stale · " + time(s.lastTradeAt)
            : "Connected · waiting for market"
          : "Disconnected",
    );
    $("status-dot").classList.toggle("live", !!live);
    const ready =
      status.depthConnected && s?.bookReady && now - s.lastDepthAt < 10000;
    text("book-status", ready ? "Synchronized" : "Depth unavailable / resync");
    if (!ready) {
      book = null;
      dom = [];
      renderSoon();
    }
    text(
      "news-status",
      !status.news.connected
        ? "UNAVAILABLE"
        : status.news.lastReceivedAt && now - status.news.lastReceivedAt > 30000
          ? "STALE"
          : "CONNECTED",
    );
    if (!news.length) renderNews();
    renderSoon();
  } catch {
    text("connection-status", "Backend unavailable");
    $("status-dot").classList.remove("live");
  }
}
async function refreshStats() {
  const gen = generation;
  try {
    const rows = await api(
      "/api/market-stats?symbol=" + settings.symbol,
      controller?.signal,
    );
    if (gen === generation) {
      stats = rows[0] || null;
      renderSoon();
    }
  } catch {}
}
async function refreshMarkets() {
  try {
    markets = (await api("/screener")).filter((r) => metadata[r.symbol]);
    renderMarkets();
    renderSoon();
  } catch (e) {
    empty($("screener-list"), e.message);
  }
}
$("symbol").addEventListener("change", () => selectSymbol($("symbol").value));
for (const button of document.querySelectorAll("[data-interval]"))
  button.onclick = () => {
    settings.interval = button.dataset.interval;
    changeSelection();
  };
$("tick").onchange = () => {
  const value = +$("tick").value,
    base = +metadata[settings.symbol]?.tickSize;
  if (
    !Number.isFinite(value) ||
    value <= 0 ||
    value < base ||
    Math.abs(value / base - Math.round(value / base)) > 1e-6
  ) {
    notice("Price grouping must be a positive multiple of " + base);
    $("tick").value = settings.tickSize;
    return;
  }
  settings.tickSize = value;
  changeSelection();
};
$("chart-reset").onclick = () => charts.candles.reset();
$("retry").onclick = () => {
  const old = socket;
  socket = null;
  old?.close();
  connect();
};
for (const id of ["market-search", "market-sort"])
  $(id).addEventListener("input", renderMarkets);
for (const id of ["trade-side", "trade-min", "liq-side", "liq-min"])
  $(id).addEventListener("input", () =>
    renderActivity(id.startsWith("trade") ? "trades" : "liquidations"),
  );
for (const button of document.querySelectorAll("[data-pause]"))
  button.onclick = () => {
    const kind = button.dataset.pause;
    paused[kind] = !paused[kind];
    button.textContent = paused[kind] ? "Resume" : "Pause";
    button.classList.toggle("active", paused[kind]);
    renderActivity(kind);
  };
for (const p of panels) {
  const label = document.createElement("label"),
    input = document.createElement("input");
  input.type = "checkbox";
  input.dataset.panel = p;
  input.onchange = () => {
    settings.hidden = panels.filter(
      (p) => !document.querySelector(`[data-panel="${p}"]`).checked,
    );
    applySettings();
    persist();
  };
  label.append(
    input,
    document.createTextNode(p.charAt(0).toUpperCase() + p.slice(1)),
  );
  $("panel-toggles").append(label);
}
$("settings-open").onclick = () => $("settings").showModal();
$("density").onchange = () => {
  settings.density = $("density").value;
  applySettings();
  persist();
};
$("reset-settings").onclick = () => {
  settings = { ...defaultSettings, hidden: [] };
  changeSelection();
};
const timers = [
  setInterval(() => {
    text(
      "clock",
      new Date().toLocaleTimeString([], { hour12: false }) + " LOCAL",
    );
  }, 1000),
  setInterval(() => {
    if (!document.hidden) {
      refreshStatus();
      if (Date.now() - lastSeed > (candleReady ? 15000 : 4000)) seedCandles();
    }
  }, 5000),
  setInterval(() => {
    if (!document.hidden) refreshStats();
  }, 10000),
  setInterval(() => {
    if (!document.hidden) refreshMarkets();
  }, 15000),
];
window.addEventListener("pagehide", () => {
  disposed = true;
  clearTimeout(retryTimer);
  timers.forEach(clearInterval);
  socket?.close();
  controller?.abort();
  Object.values(charts).forEach((c) => c.destroy());
});
applySettings();
async function boot() {
  if (!endpoints) { notice(deploymentError); text('connection-status', 'Backend not configured'); return; }
  try {
    const exchanges = await api("/api/exchanges");
    metadata = exchanges[0].symbolDetails;
    const options = Object.keys(metadata).map((s) => {
      const o = document.createElement("option");
      o.value = s;
      return o;
    });
    $("symbols").replaceChildren(...options);
    if (!metadata[settings.symbol]) settings.symbol = "BTCUSDT";
    const base = +metadata[settings.symbol].tickSize;
    if (
      settings.tickSize < base ||
      Math.abs(
        settings.tickSize / base - Math.round(settings.tickSize / base),
      ) > 1e-6
    )
      settings.tickSize = base * 100;
    applySettings();
    refreshMarkets();
    connect();
  } catch (e) {
    notice(e.message + " · retrying in 5 seconds");
    retryTimer = setTimeout(boot, 5000);
  }
}
boot();
