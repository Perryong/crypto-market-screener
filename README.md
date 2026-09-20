# crypto-market-screener

A local Binance Futures market-analysis dashboard, served by FastAPI with DuckDB history. Open **http://127.0.0.1:8086/app** after starting the server. API documentation is at `/docs`.

## Run

```sh
uv venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python main.py
```

On this Mac, Python needs the system CA bundle to trust the network certificate chain:

```sh
SSL_CERT_FILE=/etc/ssl/cert.pem .venv/bin/python main.py
```

That setting is machine-specific. Keep TLS verification enabled. Only one process may open `cryexc.duckdb`; stop the previous server before restarting. The existing launch command listens on all interfaces. For local-only access:

```sh
SSL_CERT_FILE=/etc/ssl/cert.pem .venv/bin/uvicorn main:app --host 127.0.0.1 --port 8086
```

## Dashboard

- Searchable Binance perpetual market selector; all panels follow the selected symbol.
- Candlestick/volume chart: 1m, 5m, 15m, 1h, 4h, crosshair, drag-to-pan, scroll-to-zoom and reset.
- Grouped orderbook and DOM with bought/sold volume and delta.
- Rolling one-hour USD-notional CVD, footprint charts and resting-liquidity heatmap.
- Trades and liquidations with side/minimum-notional filters and independent pause controls.
- Searchable, sortable market screener with symbol selection.
- Tree of Alpha news with a separate feed indicator.
- Price, daily change/range/volume, funding, open interest and grouped spread.
- Saved panel visibility, density, symbol, interval and tick size; reset workspace.
- Responsive layout, keyboard-accessible controls and explicit waiting/stale/error states.

This application does not execute trades or connect private exchange accounts. There are no simulated market values. Unreachable feeds are reported rather than replaced with invented data.

## Data and limitations

Binance depth uses `/public`; aggregate trades, authoritative candle updates, mark prices and liquidations use `/market`. Depth is exposed only after snapshot bridging and sequence verification. Desired subscriptions restore on reconnect. Symbols are shared across clients and released after ten seconds without demand; BTCUSDT stays active.

Candles are seeded from REST and replaced by exchange candle updates, avoiding additive seed-volume duplication. REST periodically reconciles history. A partial candle may briefly reflect an earlier exchange snapshot while a request is in flight; the next kline update reconciles it.

Trades and snapshots persist in `cryexc.duckdb`. Retention is 24 hours, cleaned hourly. Footprint/CVD/DOM history starts when this backend collects a symbol. Trades before collection or during disconnection cannot be recovered. Coverage labels show the earliest available bucket, not a guarantee of gap-free history.

Heatmaps capture fresh, synchronized depth at one-minute boundaries. Coarser intervals use the last snapshot per interval. Grouping must be a positive multiple of the exchange tick. Old snapshots with coarser stored precision are excluded from incompatible finer queries.

Liquidations are the exchange's sampled public feed, not an exhaustive archive. Empty panels mean no matching events have arrived. Open interest refreshes about every five minutes. News depends independently on Tree of Alpha.

The frontend uses native browser modules and Canvas, with no build or production JavaScript dependency. Web fonts are optional with system fallbacks. This is a single-process local application, not a multi-tenant trading service.

## GitHub Pages

The workflow in `.github/workflows/pages.yml` publishes only `static/` to GitHub Pages. In the repository's **Settings → Pages**, select **GitHub Actions** as the source. Set repository Actions variable `CRYEXC_API_BASE` to the public HTTPS URL of this Python backend, then run **Deploy dashboard to GitHub Pages**.

GitHub Pages cannot execute Python, maintain DuckDB or serve WebSockets. The backend therefore needs a separate always-on host that permits Binance access and supports HTTPS/WSS. Persistent disk is needed to retain DuckDB history across redeployments. Do not put tokens or credentials into `CRYEXC_API_BASE`; it is public browser configuration. Without the variable the published UI explains that its backend is not configured.

Local `/app` redirects to `/app/` so relative assets work both locally and under a GitHub Pages repository path. The Pages workflow excludes databases, local environments, logs and backend source from the published website.

## Verification

```sh
CRYEXC_DB_PATH=:memory: .venv/bin/python -m unittest discover -s tests -v
node --test tests/dashboard.test.mjs
.venv/bin/python -m compileall -q main.py binance.py database.py models.py news.py
```

`tests/browser-smoke.mjs` is a Playwright page-function scenario, runnable using the browser tool's `browser_run_code_unsafe` filename option. It intercepts data only in a separate test page to check saved ticks, paused symbol switching, history retry, stale feeds, old-generation frames, unsafe news and mobile overflow. The normal application uses live feeds.

## API and files

HTTP: `/api/exchanges`, `/api/features`, `/api/status`, `/api/market-stats`, `/screener`, `/klines`, `/api/candles`. Invalid inputs return 422; upstream REST failures return 502/503 rather than an ambiguous empty success.

WebSocket `/ws` accepts `stream_subscribe`, `stream_subscribe_batch`, `stream_update`, `stream_unsubscribe` and `ping`. Streams: `trade`, `kline`, `orderbook`, `orderbook_stats`, `dom`, `cvd`, `footprint`, `orderbook_heatmap`, `liquidation`, `news`. Config fields include `symbol`, `exchange`/`exchanges`, `interval`, `timeRange`, `tickSize`, `depth`, `minNotional` and `side` as applicable. Optional `instanceId` routes frames to the workspace generation. CVD historical messages replace the rolling window rather than append cumulative values.

`main.py`: routes/broadcasts; `binance.py`: exchange ingestion; `database.py`: history/aggregations; `models.py`: validation; `news.py`: news ingestion; `static/`: dashboard; `tests/`: isolated checks; `docs/superpowers/`: approved design, plan and execution decisions.

This extends the original CryExc example backend, whose upstream README marked it deprecated in favor of `jose-donato/cryexc-history`. This local dashboard is a separate implementation inspired by the Cryexc terminal.
