# XPDE architecture

XPDE is a local, manual-only decision-support system for the MetaTrader 5
symbol `GOLDm#`. It does not expose an order endpoint and the MT5 bridge never
calls `order_send`.

```text
MetaTrader 5 terminal
        |
        | read-only account, symbol, tick and M5 bars
        v
Python MT5 bridge
        |
        +--> empirical direct-horizon baseline
        |    (CatBoost artifacts can replace it after validation)
        v
Rust HTTP ingestion
        |
        +--> contract and freshness validation
        +--> account-aware Scalper/Sniper policy
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
- `ForecastEnvelope`: direct-horizon quantiles, probabilities and calibration.
- `DecisionProposal`: cost-aware action, expiry, reasons and warnings.
- `OutcomeRecord`: objective result after a prediction expires.

Rust owns the canonical JSON shape. Python mirrors and validates the parts it
produces. Contract fixtures and golden parity tests should be added before an
ONNX or non-Python inference runtime is promoted.

## Safety boundaries

- The server binds to `127.0.0.1` by default.
- There is no route for placing, modifying or closing an order.
- A stale or incomplete feed is rejected before it can update runtime state.
- Drift, excessive spread or invalid calibration produces abstention.
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
