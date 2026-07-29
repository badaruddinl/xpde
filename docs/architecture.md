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

Barrier training and live evaluation use the same explicit three-bar horizon
and ordered-tick first-touch implementation. For a Bid chart, LONG enters Ask
and exits against future Bid ticks; SHORT enters Bid and exits against future
Ask ticks.
One-sided OHLC or a spread approximation is not accepted for candidate training.
The dataset gate also requires chart-Bid/tick-Bid parity within tick-size
tolerance, `COPY_TICKS_ALL`, at least 95% tick-volume coverage, a parseable path
for every bar, and exact path-to-OHLC reconstruction. The path is authoritative
for first passage; OHLC is not used as a live ambiguity fallback.
LONG and SHORT retain `TP_FIRST`, `SL_FIRST`,
`NO_HIT_BEFORE_EXPIRY`, `AMBIGUOUS_SAME_BAR`, and
`AMBIGUOUS_SAME_TIMESTAMP`. Multiple executable prices sharing one millisecond
are not assigned an invented order. A disagreement between q50,
the direction classifier, and the stronger barrier side yields
`FORECAST_SIDE_CONFLICT` and therefore `WAIT`.

After downtime, market bars and their ordered `COPY_TICKS_ALL` paths are
backfilled from the last local completed candle. Requests are gzip-compressed
and bounded by bytes, bars and tick points. A reset import accepts strictly
ascending, single-use chunks and is promoted atomically only when every declared
chunk is present, so an interrupted import cannot erase the last usable history.
Predictions are never generated retroactively:
the bridge waits for the next new completed M5 candle after catch-up.

Settlement constructs an expected M5-bucket window. Every bucket must have a
completed path whose JSON count, first/last timestamp, ordering and executable
prices validate. A `LIVE_CURRENT` partial path is never sufficient. Missing
paths remain recoverable as `TICK_PATH_INCOMPLETE`; missing market buckets are
`SESSION_INTERRUPTED`. `NO_HIT_BEFORE_EXPIRY` is legal only for a complete
window, and a prediction cannot become `SETTLED` while either H3 directional
barrier is null. Price-return horizons also require their exact consecutive M5
buckets, so a missing candle is never silently replaced by a later candle.

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

Expected exit spread uses a configured conservative quantile over at least 12
recent exact Bid/Ask closes. Insufficient samples force `WAIT`.
`order_calc_profit()` supplies account-currency profit-per-price-unit factors;
non-matching account/P&L currencies force
`CURRENCY_CONVERSION_UNAVAILABLE` when that authoritative conversion is absent.
The contract distinguishes symbol profit currency from calculated P&L currency;
`order_calc_profit()` results are explicitly denominated in account currency.
Proposal records include every currency, conversion metadata and cost assumption.

## Dynamic proposal evidence

`predictions` owns the immutable forecast. Every material decision change is
stored separately in `decision_proposal_instances` with an exact `proposal_id`,
quote timestamp, entry, action, target, stop, cost assumptions and model-health
state. Instances are persisted by forecast, market snapshot and material
policy-clock events; GET and WebSocket reads never create evidence. Human feedback refers to this ID and is
rejected unless it is still the latest instance within its entry-age and health
gate. A tick-quantized material-change fingerprint suppresses floating-point
churn.

Policy evidence uses explicit, separately reported memberships:
`FIRST_ACTIONABLE`, `HUMAN_ACCEPTED`, `HUMAN_REJECTED`, and `DIAGNOSTIC`.
Settlement begins strictly after the instance `quote_timestamp`, includes the
remaining portion of its current candle, and ends at `outcome_matures_at`.

Decision age is bounded independently of forecast expiry. Warming, degraded or
suspended model health forces `WAIT`; health transitions use persisted
multi-window hysteresis and are recorded as events.
The internal one-second policy clock also advances bridge/tick age, entry expiry,
forecast expiry and market-session transitions when the provider is silent.

## Market and executable storage

The broker weekly calendar is configuration-driven and combined with holiday
overrides, symbol trade mode, absolute tick age and transport freshness. Exact
bar upserts only replace stored Bid/Ask OHLC when the incoming tick count is at
least as complete; first and last tick timestamps make that comparison auditable.
Compact ordered tick paths live in `market_tick_paths`, independent from OHLC,
and are replaced only by a path spanning at least the stored boundaries.
Directional MT5 modes (`FULL`, `LONG_ONLY`, `SHORT_ONLY`, `CLOSE_ONLY`,
`DISABLED`) gate each proposal side independently. Broker DST remains an
explicit operational configuration risk: `fixed_broker_utc_offset` uses an
integer UTC offset rather than pretending to resolve a timezone database
identifier.
Legacy predictions without executable origin sides or the current barrier
contract are quarantined as `LEGACY_UNSETTLEABLE`.

## Reproducible training boundary

MT5 dataset exports default to streaming gzip CSV and receive a sidecar manifest
with symbol, timeframe, UTC
range, row count, gap summary, repository commit and SHA-256. Colab copies the
dataset from Drive to ephemeral `/content`, verifies the hash, checks out the
exact repository commit and runs the test suite before training.

Every candidate includes a model card, machine-readable evaluation report and
SHA-256 checksum list. Local inference accepts only complete schema-v3,
gate-v3 artifacts with the exact executable-side contract. Candidates remain
shadow-only and immutable in Drive.
