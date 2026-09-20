# Binance dashboard design

Status: approved by the user on 2026-09-21; implemented and verified. Execution record: ../plans/progress.md.

## Intent and scope

Run this backend locally and build a complete, usable Binance Futures market-analysis dashboard inspired by the Cryexc terminal. The user selected the Binance-only scope using this backend. The dashboard must display real data, have working controls, and handle unavailable feeds honestly.

This is a market-data application, with no order execution, exchange credentials, account balances, or authentication. Other exchanges, options, whale tracking, and prediction markets are outside the selected scope. Visual similarity means a dense dark trading workspace, not a claim of exact visual or feature parity with the reference.

## Approach

Recommended: serve a small HTML/CSS/JavaScript frontend from the existing FastAPI process at `/app`, with `/` redirecting there. Reuse the HTTP endpoints, WebSocket protocol, DuckDB storage, and Binance client. Use native browser controls, CSS grid, and Canvas for charts, without a frontend build pipeline.

Alternatives considered: a separate React application would add a build toolchain and another development server; embedding the external reference app would not produce a locally owned dashboard. Neither is needed for this scope. A chart dependency can be considered during planning only if the required chart interactions cannot be implemented cleanly with Canvas.

## Workspace and interactions

- Header: brand, Binance Futures label, searchable symbol selector, interval selector, feed status, last update time, and settings.
- Summary strip: latest price, daily change, daily volume, funding rate, open interest, and spread, with explicit unavailable states.
- Main chart: candlesticks and volume, crosshair with OHLC readout, pan/zoom, and reset-to-live. Offer 1m, 5m, 15m, 1h, and 4h intervals.
- Order-flow views: footprint candles with buy/sell volume per price bucket, liquidity heatmap, and a CVD chart. History comes from locally collected trades/snapshots and clearly displays its available coverage.
- Depth panel: grouped orderbook with depth bars and a DOM ladder showing bids, asks, traded volume, and delta. Tick size is editable and must be positive and finite.
- Activity panels: live trades and liquidations with side and minimum-notional filters, bounded row counts, and pause/resume display controls.
- Market screener: search and sort Binance symbols by daily change, volume, or price; selecting a symbol updates the workspace.
- News panel: existing Tree of Alpha feed with readable timestamps and safe external links; an outage does not block market views.
- Settings: panel visibility, compact/comfortable density, and reset workspace. Persist these settings, selected symbol, interval, and tick size in browser local storage. Invalid stored settings fall back safely.

Desktop uses a chart-centered grid with depth on the right and supporting panels beneath. Smaller screens stack panels without hiding controls. All controls have labels, visible keyboard focus, and sufficient contrast; direction is communicated with signs/text as well as color.

## Data flow and backend changes

1. Load exchange metadata and saved settings, validate the selected symbol, fetch candles and market summaries, and open one application WebSocket.
2. Subscribe to the selected symbol's existing streams. Add validated on-demand Binance subscriptions so selectable symbols actually receive live data; retain BTCUSDT as the initial default. Track shared symbol demand across clients and release unused upstream subscriptions, with a short grace period to avoid churn during switching.
3. On symbol or interval changes, clear incompatible state, cancel obsolete requests, and replace subscriptions. Ignore late responses from the previous selection. Restore subscriptions after reconnect with bounded exponential backoff.
4. Seed candles from Binance REST and update the active candle from live trades. Refresh from REST after gaps so missed trades do not silently produce incorrect candles.
5. Add the missing live CVD path using the same signed-volume and timestamp bucketing rules as historical CVD. Avoid double counting at the history/live boundary; include the selected window's semantics in the chart label.
6. Diagnose and correct orderbook snapshot/buffer synchronization before trusting depth displays. During resynchronization, suppress invalid books and show stale/unavailable status. Replay buffered updates in sequence after the snapshot bridge and retry when a real gap occurs.
7. Keep market statistics fresh enough for the summary strip, including newly selected symbols. Preserve existing public endpoint/message formats where possible; make additions backward compatible.
8. Align heatmap storage and queries with the selected symbol's price precision. The current fixed $10 bucket must not silently erase detail for low-priced symbols. Only offer aggregation settings the stored data can support, and clearly state the one-minute snapshot cadence.
9. Validate externally supplied symbols, intervals, limits, stream names, tick sizes, and filter values. Return useful errors without ending otherwise healthy client connections. Track and cancel background tasks cleanly at shutdown.

REST errors must be distinguishable from valid empty results. Feed health distinguishes browser-to-backend connectivity from upstream freshness. No generated market values or unlabeled demo fallback will be used. Render external strings as text; allow only HTTP(S) news links.

## Storage and performance

Keep the existing DuckDB file and 24-hour retention. Do not delete the user's collected data. State that history begins when this backend collects a symbol; arbitrary historical footprints and historical liquidation backfills are not available.

Bound browser histories and upstream buffers, batch visual updates with animation frames, and limit visible price levels/candles. Reuse existing aggregation queries. This remains a single-process local application; distributed ingestion, workers, and additional databases are unnecessary.

## Verification and delivery

- Add focused runnable regression checks for snapshot bridging/replay/gaps, subscription validation and switching, CVD history/live continuity, and price-bucket behavior.
- Check REST routes and WebSocket subscriptions with deterministic test data; verify malformed inputs and disconnect cleanup.
- Exercise the dashboard in a browser: initial load, symbol and interval switching, filters, panel settings, persistence, chart interactions, narrow viewport, upstream failure, and reconnect. Confirm no uncaught console errors.
- Verify live Binance candles, trades, and depth when the upstream is reachable. Report any news/feed outage separately rather than claiming all data sources work.
- Update README with the launch command, local dashboard URL, system CA bundle guidance for this machine, implemented features, and history limitations.
- Leave the application running locally and report the URL, verification results, and any remaining external limitation.

Acceptance: the local dashboard exposes every panel and interaction above with genuine backend data or an explicit loading/empty/error state; switching symbols never shows the previous symbol's data as current; reconnection restores subscriptions; depth and CVD regression checks pass.

## Current environment

The existing Python dependencies are installed in `.venv`. The backend was started on port 8086 with `SSL_CERT_FILE=/etc/ssl/cert.pem` because the default Python certificate store did not trust the network certificate chain. TLS verification stays enabled. This directory has no Git repository, so the design cannot be committed here without separately initializing version control.
