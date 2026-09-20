# Binance Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a locally running Binance Futures dashboard with real charts, order flow, market screening, news, saved workspace settings, and reliable symbol switching.

**Architecture:** FastAPI serves the dashboard and its existing API on port 8086. Browser ES modules consume one application WebSocket plus existing REST endpoints. Fix the shared Binance ingestion and aggregation paths before connecting the visual panels.

**Tech Stack:** Existing Python/FastAPI/Pydantic/httpx/websockets/DuckDB; native HTML, CSS grid, JavaScript modules, Canvas; Python unittest and Node's built-in test runner for focused checks. Browser automation is development-only, using available tooling.

**Spec:** `docs/superpowers/specs/2026-09-21-binance-dashboard-design.md`

## Global Constraints

- This is a market-data application, with no order execution, exchange credentials, account balances, or authentication.
- Other exchanges, options, whale tracking, and prediction markets are outside the selected scope.
- Use native browser controls, CSS grid, and Canvas for charts, without a frontend build pipeline.
- Keep the existing DuckDB file and 24-hour retention.
- Do not delete the user's collected data.
- TLS verification stays enabled.
- No generated market values or unlabeled demo fallback will be used.
- Render external strings as text; allow only HTTP(S) news links.
- This remains a single-process local application; distributed ingestion, workers, and additional databases are unnecessary.
- This directory is not a Git repository. Do not initialize one just to satisfy commit steps; review local diffs/artifacts at each checkpoint instead.

## Review Focus

1. Snapshot arriving before the bridging update: keep depth unavailable until the bridge arrives, then replay every contiguous update (Task 1).
2. Rapid BTC → ETH → BTC changes and two browser clients: discard obsolete responses without unsubscribing another client's symbol (Tasks 2 and 4).
3. Sub-cent prices and non-power-of-ten tick sizes: preserve distinct footprint keys and volume totals; reject unsupported heatmap precision (Task 3).
4. Corrupted/blocked local storage and malicious news URLs: use safe defaults and inert text, leaving the dashboard operational (Tasks 4 and 5).
5. Backend reachable but upstream stalled, reconnect during candle seed, or no liquidation events: distinguish stale, loading, and legitimately empty data (Tasks 4–6).

## File map and execution order

Modify `binance.py` for exchange ingestion, metadata, subscriptions, synchronization and freshness; `models.py` for stream validation; `main.py` for orchestration, routes and broadcasts; `database.py` for precise aggregations and isolated test database selection; `news.py` for task lifecycle/freshness; `README.md` for launch and limitations.

Create `static/index.html`, `static/styles.css`, `static/app.js` (DOM, transport, orchestration), `static/state.mjs` (pure transformations/settings), and `static/charts.js` (Canvas drawing/interactions). Create `tests/test_backend.py` and `tests/dashboard.test.mjs`; keep regression checks grouped by behavior in these files rather than introducing a testing framework. Add `tests/browser-smoke.mjs` only if browser automation requires a local script.

Tasks 1 → 2 → 3 → 4 → 5 → 6 are sequential because they share protocol and lifecycle behavior. Recommended execution: native in this session, followed by an independent final review.

### Task 1: Correct and test exchange event ingestion

**Files:** Modify `binance.py`, `database.py`; create `tests/test_backend.py`.

**Interfaces:** Preserve `BinanceClient.on_trade/on_depth/on_liquidation`, `get_orderbook(symbol)`, and `Database(path)`. Add `_apply_snapshot(symbol: str, snapshot: dict) -> None` and `_replay_depth(symbol: str) -> bool` to let both snapshot completion and arriving updates advance the same synchronization state. Track last received exchange timestamps separately from wall-clock reads.

- [ ] Read the full ingestion module and every caller of snapshot, orderbook and liquidation functions. Confirm Binance Futures snapshot bridging rules against official Binance documentation before changing the algorithm; do not substitute Spot rules.
- [ ] Isolate test imports from the running database with an environment override, keeping the default unchanged:

```python
db = Database(os.environ.get("CRYEXC_DB_PATH", "cryexc.duckdb"))
```

- [ ] Add `unittest.IsolatedAsyncioTestCase` regressions using captured callbacks and synthetic exchange messages: snapshot ID 100, bridge `U=99,u=102,pu=98`, continuation `U=103,u=105,pu=102`; assert both updates survive, quantities of zero remove levels, and stale updates cannot regress IDs. Also test a snapshot that arrives before the bridge and a continuation with mismatched `pu` that suppresses books until resynchronized.

```python
client._apply_snapshot("BTCUSDT", {
    "lastUpdateId": 100, "bids": [["99", "1"]], "asks": [["101", "1"]]
})
self.assertIsNone(client.get_orderbook("BTCUSDT"))
await client._handle_depth({"s": "BTCUSDT", "U": 99, "u": 102,
    "pu": 98, "b": [["99", "2"]], "a": [], "E": 1000})
self.assertEqual(client.get_orderbook("BTCUSDT")["bids"][0]["quantity"], 2)
```

- [ ] Run `CRYEXC_DB_PATH=:memory: .venv/bin/python -m unittest discover -s tests -p 'test_backend.py' -v`; confirm the new synchronization checks fail before implementing them.
- [ ] Retain all buffered events after the snapshot, drop obsolete events, find the Futures bridge, and then enforce `pu == previous u` for each continuation. Maintain at most one snapshot task per symbol. Bound the buffer at 10,000 events; overflow discards the candidate book and requests a fresh snapshot. Do not mark initialized before a bridge; both `get_orderbook` and depth callbacks reject unsynchronized state. Cancel retries when stopped or unsubscribed.
- [ ] Normalize liquidation notional at ingestion, preserving the existing field for compatibility:

```python
liquidation["quoteQty"] = liquidation["notionalUsd"]
```

Exercise a force-order event through the callback and filter and assert the emitted notional equals price × quantity. Filter invalid non-finite/non-positive trade values before persistence and broadcasting.
- [ ] Run the focused checks again. Review that this change fixes shared ingestion, not only dashboard consumers.

### Task 2: Validated shared symbol subscriptions and lifecycle

**Files:** Modify `models.py`, `main.py`, `binance.py`, `news.py`; extend `tests/test_backend.py`.

**Interfaces:** Add `BinanceClient.set_symbols(symbols: set[str]) -> None` as an async method sending upstream `SUBSCRIBE`/`UNSUBSCRIBE`, retaining combined-stream envelopes. Add `fetch_exchange_metadata() -> dict[str, dict]` with `tickSize` strings from PRICE_FILTER; preserve `fetch_exchange_info() -> list[str]`. Add `validate_subscription(stream: str, config: dict, metadata: dict) -> dict` using the existing Pydantic models. Extend `/api/exchanges` with `symbolDetails` without changing its existing list/symbols fields.

- [ ] Add tests for two clients sharing BTC, one switching to ETH, unsubscribe/disconnect grace handling, repeated updates not adding demand twice, and reconnect restoring exactly the current desired set. Fake the upstream socket; assert outgoing control frames and subscription state rather than opening a real exchange connection.
- [ ] Add malformed JSON, unknown stream/symbol/exchange, non-object config, NaN tick, zero DOM tick, negative notional, oversized batch and invalid interval tests. A bad message returns `{"type":"error","message":...}` and the next ping on that same connection still receives pong.

```python
with self.assertRaises(ValueError):
    validate_subscription("dom", {"symbol": "BTCUSDT", "exchange": "binancef",
        "tickSize": 0, "timeRange": "1h"}, {"BTCUSDT": {"tickSize": "0.1"}})
```

- [ ] Run the backend test command from Task 1 and confirm new checks fail.
- [ ] Use existing models for finite positive tick sizes on aggregations, finite nonnegative notional, side enum buy/sell, depth 0–1000, allowlisted intervals/time ranges, and Binance-only exchanges. Limit batches and subscriptions to 32 per connection, instance IDs to 128 characters, and messages to 64 KiB. Validate before mutating subscriptions. Keep orderbook tick size zero as the existing raw-depth option.
- [ ] Compute symbol demand from the connection manager's subscription maps, not a second refcount structure. Keep BTCUSDT as a default feed. Reconcile once per second; release other symbols after 10 seconds without demand. Iterate snapshots of active sets during broadcasts/polling. Fetch new symbol metadata before validation and immediately poll its market statistics. Maintain desired symbols through upstream reconnects.
- [ ] Track tasks explicitly on the client/lifespan, cancel and await them on shutdown; use disconnect `finally` cleanup. Clear removed symbols' live book/buffer state without deleting historical rows. Track `lastTradeAt`, `lastDepthAt`, `lastMarkAt`, `bookReady`, and news freshness.
- [ ] Run the focused checks; ensure old subscription message shapes still work and news subscriptions require no symbol.

### Task 3: Accurate historical/live aggregates and HTTP errors

**Files:** Modify `main.py`, `database.py`, `binance.py`; extend `tests/test_backend.py`.

**Interfaces:** Retain existing historical envelopes. Add periodic `cvd_historical` window replacements for CVD subscribers with top-level `instanceId`, `symbol`, and `exchange`; clients replace rather than append. Add `/api/status` returning active symbols and per-feed timestamps/readiness. Add an optional validated `symbol` query on `/api/market-stats`, preserving the list response. Existing `/klines` returns timestamp/open/high/low/close/volume.

- [ ] Add a time-frozen in-memory CVD check that inserts a $20 buy and $5 sell, gets 15, inserts a $3 sell and gets 12 on the next replacement. Advance the window and confirm expired trades leave the rolling-window sum. Assert initial and replacement frames have matching routing identifiers.

```python
with patch("database.time.time", return_value=120):
    db.insert_trade("binancef", "BTCUSDT", 10, 2, 20, False, 110000)
    db.insert_trade("binancef", "BTCUSDT", 5, 1, 5, True, 111000)
    self.assertEqual(db.get_cvd_historical("binancef", "BTCUSDT", 60000, 60000)[-1]["value"], 15)
```

- [ ] Add low-price footprint checks with prices 0.012 and 0.013 and tick 0.001: two distinct keys, correct OHLC, and preserved total volume. Add 0.05-tick grouping checks, ask buckets rounded up versus bid buckets rounded down, and heatmap rejection of finer-than-stored buckets. Test snapshots sharing a timestamp but different stored resolutions are not double-counted.
- [ ] Run the backend tests and confirm the newly exposed precision cases fail.
- [ ] Derive decimal formatting from the requested tick using `Decimal(str(tick_size))`, retaining at least one decimal for legacy whole-price keys. Normalize floating boundaries before formatting/grouping. Correct bid/ask rounding in the shared orderbook grouping function so grouped spread cannot invert.
- [ ] Store one-minute snapshots using the symbol's exchange tick as base resolution and existing schema. Filter historical rows by stored tick; aggregate into requested coarser integer multiples. For intervals above one minute, use the last snapshot per interval, not the sum of snapshots. Reject finer/incompatible requests; preserve legacy rows without claiming they have finer resolution.
- [ ] Every two seconds compute a CVD window once per unique subscription configuration and send a replacement frame. Label it rolling-window notional CVD in the UI. Add a `ponytail:` comment explaining the bounded full-window query and that incremental aggregation is justified only if measured load requires it.
- [ ] Remove the ten-minute cache around locally held mark prices. Preserve OI polling cadence while polling newly demanded symbols promptly. Represent missing OI as unavailable rather than zero and report source timestamps. Cache screener results for 10 seconds and exchange metadata for one hour; do not cache upstream errors as empty success.
- [ ] Validate HTTP symbol, interval and limit (1–1000); use HTTP 422 for invalid input and 502/503 for upstream failure/unavailability. `/api/status` reports connection and event freshness independently. Keep Binance REST errors from hiding behind `[]`.
- [ ] Run backend checks and an in-process HTTP check with `httpx.ASGITransport` and mocked upstream responses; verify status codes, empty success and error distinction, and no opening of the live DuckDB in tests.

### Task 4: Dashboard shell, transport and durable workspace state

**Files:** Create `static/index.html`, `static/styles.css`, `static/app.js`, `static/state.mjs`, `tests/dashboard.test.mjs`; modify `main.py`.

**Interfaces:** `state.mjs` exports `defaultSettings`, `loadSettings(storage)`, `saveSettings(storage, settings)`, `safeNewsUrl(value)`, and `mergeCandle(candles, trade, intervalMs)`. `app.js` owns the socket, selection generation, abort controller, panel state, and rendering schedule. Static routes use paths relative to `__file__`.

- [ ] Add Node tests for malformed settings JSON, throwing storage, invalid saved interval/tick, allowed HTTPS news and rejected `javascript:` URLs, and candle merges across interval boundaries. Keep rendering independent from these pure functions.

```javascript
import test from 'node:test';
import assert from 'node:assert/strict';
import {loadSettings, safeNewsUrl} from '../static/state.mjs';
test('corrupt settings and unsafe links are harmless', () => {
  assert.equal(loadSettings({getItem: () => '{'}).symbol, 'BTCUSDT');
  assert.equal(safeNewsUrl('javascript:alert(1)'), null);
});
```

- [ ] Run `node --test tests/dashboard.test.mjs`; confirm failure before implementing the helpers.
- [ ] Serve static assets and `/app`, with `/` redirecting. Build semantic header, labelled search/interval/settings controls, summary strip, panel containers and live-status region. Use dark charcoal surfaces, subdued grid lines, teal buys, red sells, tabular numbers and visible focus rings. Use a desktop 3-column grid and a single column below 800px.

```python
static_dir = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")
@app.get("/app", include_in_schema=False)
async def dashboard():
    return FileResponse(static_dir / "index.html")
```

- [ ] Persist only allowlisted settings under `cryexc.workspace.v1`, catching storage failures. Validate numeric values and panel IDs; reset returns to BTCUSDT/1m/default panel visibility and comfortable density. Use native dialog and form controls for settings.
- [ ] Connect using the page's protocol/host. Reconnect at 1, 2, 4, 8, 16, then 30 seconds with jitter; cancel pending reconnect on explicit teardown. Fetch status every 5 seconds, summaries every 10 seconds and screener every 15 seconds only while visible. Show disconnected/resynchronizing/stale/waiting states separately.
- [ ] On selection changes increment a generation, abort old REST calls, clear all symbol panels and buffers, replace socket subscriptions, and use generation-specific instance IDs for aggregate streams. Reject frames with old instance IDs or symbols. For raw events retain symbol filtering; clear/reseed candles after reconnect. Buffer up to 2,000 trades during the initial REST request; at completion replay only events beyond the REST request-completion watermark and immediately refresh the current candle to reconcile the race. If the buffer overflows, reseed before declaring live.
- [ ] Test BTC→ETH→BTC with deliberately out-of-order mocked REST results and late stream frames in the browser; ensure every visible symbol-specific value belongs to the current generation. Verify local-storage failure doesn't break transport. Run Node tests and HTTP smoke checks for `/`, `/app`, and static assets.

### Task 5: Charts and every working market panel

**Files:** Create `static/charts.js`; extend all frontend files and `tests/dashboard.test.mjs`.

**Interfaces:** Export `createChart(canvas, {kind, onHover})` returning `{setData(data), reset(), destroy()}`. `kind` is `candles`, `cvd`, `footprint`, or `heatmap`; chart instances retain their own viewport. `app.js` supplies canonical backend arrays and schedules updates using `requestAnimationFrame`.

- [ ] Add pure transformation checks for same-candle OHLC/volume updates, late trades not overwriting a newer candle, timestamp-keyed CVD replacement without duplication, numeric screener sorting, and case-insensitive symbol search. Extend `state.mjs` only with functions actually used by the panels.
- [ ] Run Node checks to establish failing cases. Implement small pure transformations with sorted timestamps and bounded arrays: 1,000 candles/CVD points, 240 footprint candles, 240 heatmap frames, 200 trades, 100 liquidations/news items. Do not use a new general state-management layer.
- [ ] Draw Canvas charts at device pixel ratio using `ResizeObserver`, price/time axes and compact labels. Candles include volume, pointer crosshair/OHLC readout, pointer-drag pan, wheel zoom clamped to 10–500 visible bars and a keyboard-accessible reset button. Empty data draws an explicit message; a flat-price range receives nonzero padding. Destroy observers/listeners on teardown.
- [ ] Draw CVD from replacement windows and show window/notional units. Footprints display bidVolume × askVolume with delta coloring at precise price keys. Heatmap shows intensity from resting quantity, bid/ask distinction, one-minute snapshot cadence and earliest available timestamp; select only compatible tick multiples. Limit dense rendered price rows to the visible viewport, not entire book history.
- [ ] Render a grouped orderbook with depth bars and numeric spread, and DOM with bid/ask/bought/sold/delta/volume columns centered near midprice. Tick edits update both subscriptions and invalidate previous aggregate histories. Clamp bars when the maximum is zero. Use precision from exchange metadata throughout.
- [ ] Wire trade/liquidation notional and side filters and pause buttons. Keep the unfiltered trade subscription for candles and apply table filters locally so chart volume remains correct. Pause freezes the visible activity table only and retains a bounded latest buffer; resume shows its latest contents.
- [ ] Render searchable/sortable screener rows with symbol buttons, summary metrics including stale/unavailable states, and safe news cards. Create external text using `textContent`; set validated URLs on anchors with `rel="noopener noreferrer"`.

```javascript
const url = safeNewsUrl(item.url);
title.textContent = item.title;
if (url) { link.href = url; link.rel = 'noopener noreferrer'; }
```

- [ ] Wire panel visibility, density and reset settings. All visible controls must operate; no decorative buttons implying unimplemented functionality. Provide readable chart summaries and controls outside Canvas for keyboard users.
- [ ] Run Node tests and browser checks at 1440×900 and 390×844. Verify every panel, symbol/timeframe/tick controls, pointer interactions, keyboard focus, pause behavior, safe news strings and persistence after reload.

### Task 6: End-to-end verification, documentation and running handoff

**Files:** Update `README.md`; optionally create `tests/browser-smoke.mjs` using available browser tooling.

**Interfaces:** Local entry point `http://127.0.0.1:8086/app`; existing `python main.py` launch retained. Test mode uses an in-memory database and mocked upstreams, never the user's stored database.

- [ ] Run `.venv/bin/python -m compileall -q main.py binance.py database.py models.py news.py`, the complete backend regression command, and `node --test tests/dashboard.test.mjs`. Resolve relevant failures before continuing.
- [ ] Run a browser scenario with deterministic REST/WS interception: load BTC, switch to ETH and back, change interval, edit tick, filter/pause activity, hide/show panels, reload settings, disconnect/reconnect, deliver stale frames and malformed news. Assert correct displayed symbol, meaningful empty states and no uncaught browser errors. Hold exchange timestamps constant while status API succeeds and assert the UI shows upstream stale rather than live.
- [ ] Check actual live REST candles and WebSocket trade/depth reception. Confirm an orderbook remains sequenced beyond startup and that a second browser can select a different symbol without disrupting the first. Do not require a real liquidation to occur during the smoke check; its event path has deterministic coverage.
- [ ] Update README with the new `/app` entry point, single-process launch, settings storage, supported views and controls, 24-hour collected-history limitation, one-minute heatmap cadence, and separate news availability. For this machine document:

```sh
uv venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
SSL_CERT_FILE=/etc/ssl/cert.pem .venv/bin/python main.py
```

Explain that the system CA environment setting is machine-specific, not permission to disable TLS verification. Do not require it on machines whose default trust store works.
- [ ] Run the required final independent review under the selected execution skill. Address correctness, scope, missing-panel and accessibility findings; rerun only checks affected by fixes.
- [ ] Identify the previously started process by executable, directory and port before stopping it; restart the updated server once, preserving the DuckDB file. Verify `/app` returns HTTP 200 and a live subscription produces data. Leave it running and report the URL, passed checks and any unresolved external feed outage.

## Plan self-review

Coverage: Task 1 owns correct ingestion; Task 2 owns shared subscriptions/validation/shutdown; Task 3 owns CVD/precision/heatmap/history/stats/errors; Task 4 owns serving/transport/settings/status; Task 5 owns all specified panels/controls/charts/accessibility; Task 6 owns browser/live verification/docs/running delivery. All five Review Focus cases have an owning regression or browser scenario. Existing message fields are preserved; new routing metadata is additive. No new production dependencies or separate services are planned.
