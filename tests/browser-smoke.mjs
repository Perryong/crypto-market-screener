async function browserSmoke(page) {
  const p = await page.context().newPage();
  p.setDefaultTimeout(10000);
  const errors = [],
    requests = [],
    sockets = [];
  p.on("pageerror", (e) => errors.push(e.message));
  let seedFails = true;
  const now = Date.now();
  const candle = {
    timestamp: now - (now % 60000),
    open: 100,
    high: 102,
    low: 99,
    close: 101,
    volume: 5,
  };
  const assert = (ok, message) => {
    if (!ok) throw Error(message);
  };
  await p.addInitScript(() =>
    localStorage.setItem(
      "cryexc.workspace.v1",
      JSON.stringify({ symbol: "BTCUSDT", tickSize: 0.15 }),
    ),
  );
  await p.route("**/api/exchanges", (r) =>
    r.fulfill({
      json: [
        {
          symbols: ["BTCUSDT", "ETHUSDT"],
          symbolDetails: {
            BTCUSDT: { tickSize: "0.1" },
            ETHUSDT: { tickSize: "0.01" },
          },
        },
      ],
    }),
  );
  await p.route("**/screener", (r) => r.fulfill({ json: [] }));
  await p.route("**/api/market-stats?*", (r) => r.fulfill({ json: [] }));
  await p.route("**/api/status", (r) =>
    r.fulfill({
      json: {
        serverTime: Date.now(),
        marketConnected: true,
        depthConnected: true,
        symbols: {
          BTCUSDT: { lastTradeAt: 1, lastDepthAt: 1, bookReady: true },
        },
        news: { connected: false, lastReceivedAt: 0 },
      },
    }),
  );
  await p.route("**/api/candles?*", (r) =>
    seedFails
      ? r.fulfill({ status: 502, json: { detail: "Temporary seed failure" } })
      : r.fulfill({ json: { candles: [candle], asOf: Date.now() } }),
  );
  await p.routeWebSocket("**/ws", (ws) => {
    sockets.push(ws);
    ws.onMessage((raw) => {
      const msg = JSON.parse(raw);
      if (msg.type === "stream_subscribe_batch") {
        requests.push(msg.subscriptions);
        const trade = msg.subscriptions.find((s) => s.stream === "trade");
        ws.send(
          JSON.stringify({
            type: "trade",
            instanceId: trade.instanceId,
            data: {
              symbol: trade.config.symbol,
              price: trade.config.symbol === "BTCUSDT" ? 100 : 200,
              quantity: 1,
              quoteQty: 100,
              isBuyerMaker: false,
              timestamp: Date.now(),
            },
          }),
        );
      }
    });
  });
  try {
    await p.goto("http://127.0.0.1:8086/app");
    await p.waitForFunction(() =>
      document.querySelector("#trades-list").textContent.includes("100"),
    );
    assert(
      Math.abs(
        requests.at(-1)[0].config.tickSize / 0.1 -
          Math.round(requests.at(-1)[0].config.tickSize / 0.1),
      ) < 1e-6,
      "Saved tick must normalize to exchange precision",
    );
    await p.locator('[data-pause="trades"]').click();
    await p.locator("#symbol").fill("ETHUSDT");
    await p.locator("#symbol").press("Tab");
    assert(
      !(await p.locator("#trades-list").innerText()).includes("100.0"),
      "Paused BTC rows must clear on ETH selection",
    );
    await p.locator('[data-pause="trades"]').click();
    await p.waitForFunction(() =>
      document.querySelector("#trades-list").textContent.includes("200"),
    );
    seedFails = false;
    await p.waitForFunction(
      () =>
        document
          .querySelector("#candle-status")
          .textContent.includes("candles ·"),
      {},
      { timeout: 22000 },
    );
    await p.locator("#symbol").fill("BTCUSDT");
    await p.locator("#symbol").press("Tab");
    await p.waitForFunction(() =>
      document
        .querySelector("#connection-status")
        .textContent.toLowerCase()
        .includes("stale"),
    );
    const ws = sockets.at(-1),
      old = requests[0].find((s) => s.stream === "dom");
    ws.send(
      JSON.stringify({
        type: "dom",
        instanceId: old.instanceId,
        data: {
          symbol: "BTCUSDT",
          levels: [
            {
              price: 999,
              bid: 1,
              ask: 1,
              volume: 1,
              bought: 1,
              sold: 0,
              delta: 1,
            },
          ],
        },
      }),
    );
    ws.send(
      JSON.stringify({
        type: "news",
        data: {
          title: "<script>bad()</script>",
          url: "javascript:alert(1)",
          timestamp: Date.now(),
        },
      }),
    );
    await p.waitForFunction(() =>
      document.querySelector("#news-list").textContent.includes("<script>"),
    );
    assert(
      (await p.locator("#news-list a").count()) === 0,
      "Unsafe links must be inert",
    );
    assert(
      !(await p.locator("#dom-list").innerText()).includes("999"),
      "Old DOM generation must be ignored",
    );
    await p.setViewportSize({ width: 390, height: 844 });
    assert(
      await p.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      "Mobile should not overflow horizontally",
    );
    assert(errors.length === 0, errors.join("\n"));
    return {
      passed: true,
      checks: [
        "saved tick precision",
        "paused symbol change",
        "seed retry",
        "stale source",
        "old DOM generation",
        "unsafe news",
        "mobile width",
        "no uncaught errors",
      ],
    };
  } finally {
    await p.close();
  }
}
