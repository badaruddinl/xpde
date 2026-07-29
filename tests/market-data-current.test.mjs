import assert from "node:assert/strict";
import test from "node:test";

import {
  isMarketDataCurrent,
  marketDataUnavailableMessage,
} from "../app/market-data-current.js";

const healthyContext = {
  forecastStatus: "CURRENT",
  connectionStatus: "MT5_CONNECTED",
  marketStatus: "OPEN",
  tickAgeMs: 100,
  absoluteTickAgeMs: 100,
  transportTickAgeMs: 100,
  missingFlags: [],
  maximumTickAgeMs: 10_000,
};

test("probabilities require current connected complete market data", () => {
  assert.equal(isMarketDataCurrent(healthyContext), true);
  assert.equal(
    isMarketDataCurrent({
      ...healthyContext,
      connectionStatus: "BRIDGE_DISCONNECTED",
    }),
    false,
  );
  assert.equal(
    isMarketDataCurrent({ ...healthyContext, absoluteTickAgeMs: 10_001 }),
    false,
  );
  assert.equal(
    isMarketDataCurrent({
      ...healthyContext,
      missingFlags: ["FEATURE_WINDOW_EXECUTABLE_HISTORY_INCOMPLETE"],
    }),
    false,
  );
});

test("probability visibility is independent from model warming-up state", () => {
  // Model health is intentionally absent: it gates actions, not diagnostic output.
  assert.equal(isMarketDataCurrent(healthyContext), true);
});

test("disconnected, incomplete, and stale probability states explain the cause", () => {
  assert.equal(
    marketDataUnavailableMessage({
      ...healthyContext,
      connectionStatus: "BRIDGE_DISCONNECTED",
    }),
    "Data market terputus — probabilitas disembunyikan",
  );
  assert.equal(
    marketDataUnavailableMessage({
      ...healthyContext,
      missingFlags: ["FEATURE_WINDOW_EXECUTABLE_HISTORY_INCOMPLETE"],
    }),
    "Data market belum lengkap — probabilitas disembunyikan",
  );
  assert.equal(
    marketDataUnavailableMessage({
      ...healthyContext,
      transportTickAgeMs: 10_001,
    }),
    "Tick market stale — probabilitas disembunyikan",
  );
});
