# XPDE architecture

XPDE is a local, manual-only decision-support system for the MetaTrader 5
symbol `GOLDm#`. It does not expose an order endpoint and the MT5 bridge never
calls `order_send`.

```text
MetaTrader 5 terminal
        |
        | read-only account, symbol, UTC ticks and M5 chart bars
        v
Python MT5 bridge
        |
        +--> exact historical/live Bid and Ask OHLC aggregation
        +--> empirical direct-horizon baseline
        |    (CatBoost artifacts can replace it after validation)
        v
Rust HTTP ingestion
        |
        +--> chart-mode, absolute/transport freshness validation
        +--> executable-side, account-aware Scalper/Sniper policy
        +--> rolling live model-health gate
        +--> append-oriented audit trail
        v
SQLite WAL
        |
        v
Local API / WebSocket --> XPDE dashboard
```

## Why there is no RabbitMQ in the local MVP

The current topology has one MT5 producer, one decision core and one SQLite
writer on the same machine. A durable network broker would add another service
to install, secure, monitor and recover without removing the need for the
existing stale-feed and idempotency gates.

RabbitMQ becomes useful when the topology expands to multiple provider
adapters, remote ML workers, replay consumers or independently deployable
services. At that point the four frozen contracts can become versioned message
payloads and the consumers must add publisher confirms, durable queues,
dead-letter handling and idempotent writes.

## Frozen contracts

- `MarketSnapshot`: provider data and account/symbol specification.
- `ForecastEnvelope`: exact origin candle, direct-horizon quantiles, symmetric
  LONG/SHORT probabilities, dynamic excursions and calibration.
- `DecisionProposal`: cost-aware action, expiry, reasons and warnings.
- `OutcomeRecord`: objective result at the exact completed bar for each horizon.

Rust owns the canonical JSON shape. Python mirrors and validates the parts it
produces. Contract fixtures and golden parity tests should be added before an
ONNX or non-Python inference runtime is promoted.

## Safety boundaries

- The server binds to `127.0.0.1` by default.
- There is no route for placing, modifying or closing an order.
- Stale/closed/disconnected snapshots may update connection telemetry, but can
  never create a forecast or actionable proposal.
- `absolute_tick_age_ms` comes from the provider timestamp after the same
  explicitly configured normalization used by rate bars and executable ticks;
  `transport_tick_age_ms` measures how long the local bridge has seen no change.
- `MARKET_CLOSED`, `FEED_STALE` and `BRIDGE_DISCONNECTED` are separate states.
- Drift, excessive spread or invalid calibration produces abstention.
- Missing exact Bid/Ask bars or a chart mode other than `BID` produces abstention.
- After the live evidence minimum, degraded coverage/Brier/ECE/MAE coverage
  forces `WAIT`; XPDE never auto-retrains or auto-promotes from this signal.
- Leverage changes risk and margin policy only; it does not enter the market
  direction feature set.
- Human feedback is stored separately from objective market outcomes.

## Model lifecycle

The included empirical baseline makes the pipeline testable from day one.
`train_catboost.py` trains direct-horizon MultiQuantile regressors plus calibrated
direction and directional barrier classifiers. Evaluation uses purged
walk-forward folds, a separate calibration window and a final untouched
holdout. Conformal corrections target 80% interval coverage. An eligible
artifact may run in shadow mode but remains a registry `candidate`; champion
promotion is manual after enough objective outcomes have been settled.

## Prediction and downtime semantics

Every prediction stores `origin_bar_timestamp`, `origin_close`,
`origin_bar_index`, and a separate `generated_at`. Outcome settlement selects
the first `h` completed candles strictly after that origin for horizons 1, 3, 6
and 12; wall-clock expiry is not used as a proxy for the outcome candle.

Barrier training and live evaluation use the same explicit three-bar horizon.
For a Bid chart, LONG enters Ask and exits against future Bid OHLC; SHORT enters
Bid and exits against future Ask OHLC constructed from historical ticks.
One-sided OHLC or a spread approximation is not accepted for candidate training.
LONG and SHORT retain `TP_FIRST`, `SL_FIRST`,
`NO_HIT_BEFORE_EXPIRY`, and `AMBIGUOUS_SAME_BAR`. A disagreement between q50,
the direction classifier, and the stronger barrier side yields
`FORECAST_SIDE_CONFLICT` and therefore `WAIT`.

After downtime, market bars are backfilled from the last local completed candle.
Predictions are never generated retroactively: the bridge waits for the next
new completed M5 candle after catch-up.

Dashboard evaluation scopes are deliberately separate:

- offline holdout coverage comes only from the immutable model artifact;
- live rolling metrics come only from settled shadow predictions for the
  currently loaded model;
- current-session metrics reset when the Rust core starts.

Zero live samples are rendered as unavailable rather than as a misleading
zero-percent result.

## Cost and currency semantics

The versioned cost model does not subtract the current spread twice:

- LONG median move = future median Bid − current Ask − slippage − commission.
- SHORT median move = current Bid − (future median Bid + expected exit spread)
  − slippage − commission.

Expected exit spread is the rolling median of recent exact Bid/Ask closes, with
the broker profile value used only when no valid executable bars exist.
Proposal records include account, quote and P&L currency plus every cost
assumption used by the decision.

## Reproducible training boundary

MT5 dataset exports receive a sidecar manifest with symbol, timeframe, UTC
range, row count, gap summary, repository commit and SHA-256. Colab copies the
dataset from Drive to ephemeral `/content`, verifies the hash, checks out the
exact repository commit and runs the test suite before training.

Every candidate includes a model card, machine-readable evaluation report and
SHA-256 checksum list. Local inference accepts only complete schema-v3,
gate-v3 artifacts with the exact executable-side contract. Candidates remain
shadow-only and immutable in Drive.
