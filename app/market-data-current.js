/**
 * @typedef {object} MarketDataCurrentInput
 * @property {string} forecastStatus
 * @property {string} connectionStatus
 * @property {string} marketStatus
 * @property {number} tickAgeMs
 * @property {number} absoluteTickAgeMs
 * @property {number} transportTickAgeMs
 * @property {string[]} missingFlags
 * @property {number} maximumTickAgeMs
 */

/**
 * Probability output is displayable only while its market-data context is current.
 * Model health is deliberately not part of this invariant: WARMING_UP probabilities
 * remain useful diagnostics, while actionable decisions have a stricter gate.
 *
 * @param {MarketDataCurrentInput} input
 */
export function isMarketDataCurrent(input) {
  return (
    input.forecastStatus === "CURRENT" &&
    input.connectionStatus === "MT5_CONNECTED" &&
    input.marketStatus === "OPEN" &&
    input.tickAgeMs <= input.maximumTickAgeMs &&
    input.absoluteTickAgeMs <= input.maximumTickAgeMs &&
    input.transportTickAgeMs <= input.maximumTickAgeMs &&
    input.missingFlags.length === 0
  );
}

/**
 * @param {MarketDataCurrentInput} input
 */
export function marketDataUnavailableMessage(input) {
  if (input.connectionStatus !== "MT5_CONNECTED") {
    return "Data market terputus — probabilitas disembunyikan";
  }
  if (input.marketStatus !== "OPEN") {
    return "Market tidak terbuka — probabilitas disembunyikan";
  }
  if (input.missingFlags.length > 0) {
    return "Data market belum lengkap — probabilitas disembunyikan";
  }
  return "Tick market stale — probabilitas disembunyikan";
}
