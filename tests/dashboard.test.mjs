import test from "node:test";
import assert from "node:assert/strict";
import {
  loadSettings,
  safeNewsUrl,
  priceDecimals,
  screenMarkets,
  replacePoints,
} from "../static/state.mjs";

test("corrupt or blocked storage uses safe defaults", () => {
  for (const storage of [
    { getItem: () => "{" },
    {
      getItem: () => {
        throw Error("blocked");
      },
    },
    { getItem: () => JSON.stringify({ interval: "bad", tickSize: -2 }) },
  ]) {
    const settings = loadSettings(storage);
    assert.equal(settings.symbol, "BTCUSDT");
    assert.equal(settings.interval, "1m");
    assert.equal(settings.tickSize, 10);
  }
});
test("unsafe news links remain inert", () => {
  assert.equal(safeNewsUrl("javascript:alert(1)"), null);
  assert.equal(
    safeNewsUrl("https://example.com/news"),
    "https://example.com/news",
  );
});
test("non decimal-power ticks and sub-cent prices retain precision", () => {
  assert.equal(priceDecimals("0.25000000"), 2);
  assert.equal(priceDecimals("0.00000010"), 7);
});
test("authoritative candle update replaces seeded volume instead of adding", () => {
  const seed = {
    timestamp: 60000,
    open: 10,
    high: 12,
    low: 9,
    close: 11,
    volume: 5,
  };
  const live = { ...seed, close: 12, volume: 7 };
  assert.deepEqual(replacePoints([seed, live]), [live]);
});
test("window replacement deduplicates and screener sorts numbers", () => {
  assert.deepEqual(
    replacePoints([
      { timestamp: 2, value: 5 },
      { timestamp: 1, value: 1 },
      { timestamp: 2, value: 7 },
    ]),
    [
      { timestamp: 1, value: 1 },
      { timestamp: 2, value: 7 },
    ],
  );
  assert.equal(
    screenMarkets(
      [
        { symbol: "BTCUSDT", volume24h: 9 },
        { symbol: "ETHUSDT", volume24h: 100 },
      ],
      "usdt",
      "volume24h",
    )[0].symbol,
    "ETHUSDT",
  );
});
