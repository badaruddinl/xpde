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

## One-click Windows launchers

Double-click the launcher required from the repository root:

- `XPDE-Install.cmd` installs or refreshes local dependencies.
- `XPDE-Start.cmd` starts the core, dashboard and MT5 bridge, then opens the dashboard.
- `XPDE-Retry-MT5.cmd` restarts only the MT5 bridge.
- `XPDE-Stop.cmd` stops all XPDE processes tracked by the realtime launcher.

Open and log in to MetaTrader 5 before starting or retrying the bridge.

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
npm run test:ui
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

Candidate training is the promotion path and requires at least 20,000 rows by
default. For a quick pipeline check that cannot be promoted, use
`--training-mode smoke`. Direction and barrier probabilities are calibrated on
a temporal holdout with Platt scaling (or isotonic when enough samples improve
the Brier score), and all CatBoost families use temporal early stopping.

For CPU training in Google Colab, open
`notebooks/xpde_colab_training.ipynb`, set the exact commit and Drive paths,
then run the cells in order. The notebook copies the dataset to `/content`,
installs the exact versions in `ml/requirements-training.lock.txt`, verifies its
manifest, runs Rust/Python/Next.js tests, trains the candidate, verifies artifact
checksums and copies an immutable candidate folder plus ZIP back to Drive. The
artifact records the Git commit, dirty state, runtime and dependency versions.

Candidate schema v3 contains `evaluation.json`, `model_card.md`,
`checksums.sha256`, quantile/direction/barrier models and dynamic MFE/MAE
models. Import a downloaded Colab ZIP through the checked and immutable importer:

```powershell
.\.venv\Scripts\python.exe .\scripts\import-colab-artifact.py `
  .\downloads\xpde-candidate.zip
```

The importer validates the schema, feature and barrier contracts, verifies every
checksum, refuses duplicate run IDs and only promotes eligible candidates to the
local `latest` shadow slot. The bridge repeats checksum and contract validation
before loading a candidate.

Forecast barrier evaluation and actionable proposal evaluation are intentionally
separate. Every settled H3 forecast records counterfactual LONG and SHORT
TP-before-SL outcomes using the exact forecast target/stop. SCALPER and SNIPER
proposal outcomes are stored independently, so a `WAIT` decision does not erase
forecast quality and a proposal metric never masquerades as model coverage.

Useful read-only endpoints:

```text
GET /api/v1/market/cursor
GET /api/v1/models
GET /api/v1/evaluation/summary
```

See [architecture](docs/architecture.md) for component and safety boundaries.
