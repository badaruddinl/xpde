# XPDE — GOLDm# Probabilistic Decision Engine

Local shadow-mode decision support for the MetaTrader 5 symbol `GOLDm#`.
XPDE forecasts several possible price paths, applies account-aware cost gates,
records outcomes and feedback, then leaves the final decision to a human.

> This repository contains no auto-entry or order-execution capability.

## Current MVP

- MetaTrader 5 read-only bridge for `GOLDm#`.
- M5 direct horizons: 1, 3, 6 and 12 bars.
- Empirical quantile baseline plus CatBoost MultiQuantile trainer.
- MT5 epoch timestamps stored as UTC, with an explicit provider override only.
- Explicit completed-bar catch-up after downtime without retroactive live forecasts.
- Purged walk-forward evaluation, conformal interval calibration, calibrated
  direction and symmetric LONG/SHORT barrier classifiers.
- Exact prediction origin plus objective settlement for every 1/3/6/12-bar horizon.
- Condition-dependent MFE/MAE models; lot preview remains disabled for the baseline.
- Scalper and strict Sniper decision policies.
- Dynamic MT5 account and symbol specifications.
- SQLite WAL audit trail, prediction registry and outcome settlement.
- Rust REST/WebSocket service bound to localhost.
- Local dashboard with forecast interval, cost gate and feedback.
- Explicitly separated offline holdout, live rolling and current-session metrics.

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

The bridge automatically retries rejected or temporarily unreachable API calls
with capped exponential backoff. The dashboard's **Retry realtime** button
restarts only the local read-only MT5 bridge, prevents duplicate bridge roots,
and waits until a fresh MT5 snapshot reaches the core.

On startup or reconnect, the bridge asks the core for the latest locally stored
completed M5 candle and uses `copy_rates_range()` to append the missing range.
Backfilled bars can settle earlier forecasts, but the bridge deliberately waits
for a genuinely new completed candle before emitting another live forecast.

Alternatively, keep the bridge visible in terminal three:

```powershell
.\scripts\run-mt5-bridge.ps1
```

Use `-Once` for a single connectivity snapshot and catch-up check:

```powershell
.\scripts\run-mt5-bridge.ps1 -Once
```

`-Once` is intended only for diagnostics and deliberately does not manufacture
a live forecast from the last candle seen during startup. After it exits, the
core marks the feed `MT5_STALE` and returns `NO_PREDICTION`.

If multiple MT5 terminals are installed, copy `.env.example` to a local ignored
`.env` or set `MT5_PATH`. Credentials are optional when the selected terminal is
already logged in; never commit them.

MetaTrader 5 Python timestamps are treated as UTC. Only set
`MT5_UTC_OFFSET_OVERRIDE_HOURS` when a provider has been independently verified
to encode a fixed non-UTC offset.

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
an intentional incremental import is required. Every export also creates a
sanitized `.manifest.json` containing timestamps, row count, gap summary,
Git commit and SHA-256.

For CPU training in Google Colab, open
`notebooks/xpde_colab_training.ipynb`, set the exact commit and Drive paths,
then run the cells in order. The notebook copies the dataset to `/content`,
verifies its manifest, runs tests, trains the candidate, verifies artifact
checksums and copies an immutable candidate folder plus ZIP back to Drive.

Candidate schema v2 contains `evaluation.json`, `model_card.md`,
`checksums.sha256`, quantile/direction/barrier models and dynamic MFE/MAE
models. The local bridge verifies every checksum before loading a candidate.

Useful read-only endpoints:

```text
GET /api/v1/market/cursor
GET /api/v1/models
GET /api/v1/evaluation/summary
```

See [architecture](docs/architecture.md) for component and safety boundaries.
