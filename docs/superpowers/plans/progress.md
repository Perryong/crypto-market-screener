# SDD ledger — plan: docs/superpowers/plans/2026-09-21-binance-dashboard.md

Execution: native; spec and plan approved. Tasks 1–6 complete.
Ruling: Work directly in the supplied non-Git directory; Git-only worktree/ledger scripts and commits cannot apply. Preserve this ledger rather than delete it; cost: no Git rollback history.
Pre-flight: Tasks 1–3 produce backwards-compatible message envelopes and precision metadata consumed by Tasks 4–5. Task 6 verifies both. CVD uses replacement windows, not additive live deltas.
Ruling: Use periodic authoritative REST candle replacement plus event-time reconciliation instead of assuming the HTTP completion time is an exchange watermark; cost: periodic REST traffic, but avoids duplicated seed volume.

Task 1: complete — snapshot bridge/buffer/gap, liquidation notional and invalid-trade tests RED→GREEN.
Task 2: complete — validated shared symbol demand, split Binance sockets, tracked shutdown. Shared-client and malformed-message checks pass.
Task 3: complete — precise footprints, compatible heatmap resolutions, CVD replacements, freshness and HTTP errors. Backend checks pass.
Task 4: complete — static serving, responsive shell, generation-safe transport and saved settings. Browser state checks pass.
Task 5: complete — all charts/panels/controls implemented; live BTC/ETH browser checks pass.
Task 6: complete — 12 Python tests, 5 Node tests, 8 deterministic browser checks pass; live BTC/ETH in concurrent tabs, reconnect, settings persistence, chart interactions, and 390px viewport checked. Live Binance and news connected. Server runs with TLS verification at http://127.0.0.1:8086/app; logs /tmp/cryexc-dashboard.log. No Git integration applies to this non-repository workspace; files retained in place.
Ruling: Binance now separates public/market sockets; migrated to documented endpoints and aggregate trades because legacy URLs do not reliably supply market feeds. Cost: two upstream connections per process.
Ruling: Supersedes trade-based candle reconciliation: exchange kline replacements for all five supported intervals plus REST reseeding. Cost: five extra upstream streams per active symbol and a transient older partial snapshot before the next update.
Final review: seven Important findings fixed: saved tick normalization, paused symbol clearing, instance routing, stale depth rejection, heatmap interval alignment, seed retries and source freshness. Browser scenario reproduced failures before fixes, then passed eight checks; stale-depth and heatmap-interval backend checks RED→GREEN.
Final: Ruling: Precision is correctness for low-priced markets, so the reviewer's precision Minor was upgraded and fixed with a Node regression. Cost: longer numeric labels.
Final: Ruling: Reviewer declined live behavior, browser/accessibility execution, news availability, performance under many clients and post-review fixes. Executor owns live/browser checks and regressions; local scope excludes load benchmarking. Cost: throughput ceiling is unmeasured.
Final review disposition: all seven Important findings addressed. No deferred Minor items. Candle reconciliation's transient older partial snapshot is documented in README; precision issue fixed. Regression run emits upstream Starlette/httpx deprecation warnings, not test failures.
