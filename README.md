# XPDE — GOLDm# Probabilistic Decision Engine

Local shadow-mode decision support for the MetaTrader 5 symbol `GOLDm#`.
XPDE forecasts several possible price paths, applies account-aware cost gates,
records outcomes and feedback, then leaves the final decision to a human.

> This repository contains no auto-entry or order-execution capability.

## Current MVP

- MetaTrader 5 read-only bridge for `GOLDm#`.
- M5 direct horizons: 1, 3, 6 and 12 bars.
- Empirical quantile baseline plus CatBoost MultiQuantile trainer.
- Historical MT5 backfill with broker-clock normalization to UTC.
- Purged walk-forward evaluation, conformal interval calibration, calibrated
  direction and TP-before-SL classifiers.
- Scalper and strict Sniper decision policies.
- Dynamic MT5 account and symbol specifications.
- SQLite WAL audit trail, prediction registry and outcome settlement.
- Rust REST/WebSocket service bound to localhost.
- Local dashboard with forecast interval, coverage, cost gate and feedback.

The broker profile starts with leverage `1000:1`, one troy ounce per lot,
minimum `0.1` lot and minimum price fluctuation `$0.01`. Runtime values from
`account_info()`, `symbol_info()` and `symbol_info_tick()` remain authoritative.

## Requirements

- Windows with an installed and logged-in MetaTrader 5 terminal.
- Python 3.11 or newer.
- Rust stable.
- Node.js 22.13 or newer.

## Setup

```powershell
.\scripts\setup.ps1
```

Start the Rust core in terminal one:

```powershell
.\scripts\run-core.ps1
```

Start the dashboard in terminal two:

```powershell
node .\scripts\dashboard-server.mjs
```

Open [http://localhost:3000](http://localhost:3000).

### Realtime mode

With the MT5 terminal already logged in, the simplest option is:

```powershell
.\scripts\start-realtime.ps1
```

This starts any missing local service and keeps the read-only MT5 bridge alive
in the background. Stop only the processes started by that launcher with:

```powershell
.\scripts\stop-realtime.ps1
```

Alternatively, keep the bridge visible in terminal three:

```powershell
.\scripts\run-mt5-bridge.ps1
```

Use `-Once` for a single snapshot and forecast:

```powershell
.\scripts\run-mt5-bridge.ps1 -Once
```

`-Once` is intended only for diagnostics. After it exits, the core deliberately
marks the feed `MT5_STALE` and returns `NO_PREDICTION`.

If multiple MT5 terminals are installed, copy `.env.example` to a local ignored
`.env` or set `MT5_PATH`. Credentials are optional when the selected terminal is
already logged in; never commit them.

## Verification

```powershell
cargo fmt --all -- --check
cargo test --workspace
.\.venv\Scripts\python.exe -m pytest -q ml\tests
npm run lint
npm test
```

## Model workflow

The live bridge posts a transparent empirical baseline so the complete pipeline
can operate before a trained model is promoted. To backfill 50,000 completed M5
bars, train and register a CatBoost candidate:

```powershell
.\scripts\train-candidate.ps1
```

Every training run receives an immutable artifact directory. When its objective
holdout gate passes, the launcher copies it to the local `latest` shadow slot and
loads it on the next realtime bridge start. It remains registered as `candidate`;
promotion to champion is deliberately manual and requires enough settled shadow
predictions. Training artifacts and historical exports are local and ignored by
Git.

The backfill command treats the fetched MT5 window as authoritative and replaces
the local `GOLDm#`/M5 bar cache before importing chunks. Use `--append` only when
an intentional incremental import is required.

Useful read-only endpoints:

```text
GET /api/v1/models
GET /api/v1/evaluation/summary
```

See [architecture](docs/architecture.md) for component and safety boundaries.
