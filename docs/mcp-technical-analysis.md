# XPDE Read-Only Technical Analysis MCP

## Scope

The MCP is an analysis adapter around data already produced by XPDE. It is not
another predictor and it has no authority over Rust policy.

```text
MetaTrader 5
    |
XPDE Python bridge
    |
XPDE Rust core  ---- exact forecast and core proposal authority
    |
    +-- read-only REST: state, evaluation, models
    +-- SQLite: market/evidence history
    +-- registered artifact manifest
              |
              v
XPDE MCP over STDIO
    +-- fixed GET source adapter
    +-- SQLite mode=ro + query_only
    +-- artifact-root manifest guard
    +-- deterministic TA engine
    +-- TechnicalAnalysisPacket
              |
              v
Agent explanation (never a replacement decision)
```

There is no reverse path from an agent to order execution, feedback, model
promotion, policy configuration, retraining, forecast creation, or database
writes.

## Source boundary

The API adapter can issue GET only to:

- `/api/v1/state`
- `/api/v1/evaluation/summary`
- `/api/v1/models`

`XPDE_API_BASE` must be a loopback origin. Redirects and environment proxies are
disabled.

SQLite is opened per operation using the `file:` URI with `mode=ro`, followed by
`PRAGMA query_only=ON` and `PRAGMA trusted_schema=OFF`. Schema introspection is
used only to make optional fields from older local databases readable; the MCP
never migrates a database.

A manifest can only be selected through a registered model. The resolved
artifact directory and `manifest.json` must remain under the resolved
`XPDE_ARTIFACT_ROOT`, and the manifest has a bounded size.

## Technical contract

`ta_contract_id = xpde-ta-goldm-m5-v1`

Inputs:

- completed chart/Bid M5 bars from XPDE SQLite;
- current M5 from runtime as observation only;
- current XPDE forecast and exact profile proposal;
- current registry, manifest and evaluation evidence.

Derived timeframes:

- M15 requires timestamps `00`, `05`, `10` inside the aligned 15-minute bucket;
- H1 requires every aligned M5 timestamp from minute `00` through `55`;
- an incomplete bucket is rejected, never filled or interpolated.

Indicators:

- EMA20 and EMA50;
- EMA20 price slope across five completed bars;
- Wilder RSI14;
- ROC3 and ROC6;
- Wilder ATR14;
- current ATR percentile rank over up to 100 valid ATR observations.

Rules:

```text
Trend BULLISH = close > EMA20 > EMA50 and EMA20 slope(5) > 0
Trend BEARISH = close < EMA20 < EMA50 and EMA20 slope(5) < 0
Trend MIXED   = otherwise

Momentum BULLISH = RSI14 > 55 and ROC3 > 0
Momentum BEARISH = RSI14 < 45 and ROC3 < 0
Momentum NEUTRAL = otherwise

Structure BULLISH = latest confirmed high/low are HH + HL
Structure BEARISH = latest confirmed high/low are LH + LL
```

Swing points use two completed bars on each side. The nearest support and
resistance are confirmed swing pivots inside the latest 120 bars and their
distance is normalized by ATR when ATR is available.

No indicator or alignment rule emits LONG, SHORT, BUY, or SELL.

## Forecast and decision semantics

- `direction_probability_up` is origin-based `P(UP)`.
- Its complement is `P(NON-UP)`, meaning negative or flat, not strict
  `P(DOWN)`.
- Quantile prices are `origin_close * exp(log_return)`.
- Barrier probabilities are origin-based and not recalculated for current entry.
- LONG exits are represented on Bid; SHORT exits are represented on Ask.
- WARMING_UP does not remove a current forecast. It means live evidence is not
  mature enough for a stability or promotion claim.
- WAIT and NO_PREDICTION are valid exact core outputs.

`forecast_usable_as_guide`, `market_data_current`, `core_actionable`, and
`evidence_stage` are separate fields. The MCP always returns the exact selected
profile proposal with `must_preserve=true`.

## Provenance and interpretation

Packet facts use:

- `XPDE_CORE`
- `XPDE_SQLITE`
- `MODEL_MANIFEST`
- `MCP_DERIVED`

Every confluence or conflict lists `basis_ids` that refer to packet observation
IDs. Technical alignment can be `ALIGNED`, `PARTIALLY_ALIGNED`, `CONFLICTED`,
`NEUTRAL`, or `INSUFFICIENT_DATA`; it remains context and never changes the core
action.

The normative machine-readable files are:

- `mcp/src/xpde_mcp/schemas/technical-analysis-packet.schema.json`
- `mcp/src/xpde_mcp/schemas/tool-contracts.json`
