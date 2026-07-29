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
  direction and executable-side LONG/SHORT barrier classifiers.
- Exact prediction origin plus objective settlement for every 1/3/6/12-bar horizon.
- Deterministic prediction IDs and a database uniqueness contract make retries idempotent.
- Condition-dependent MFE/MAE models; lot preview remains disabled for the baseline.
- Scalper and strict Sniper policies use the current bid/ask, reject already-touched
  barriers and require enough remaining reward/risk after costs.
- Historical ticks are aggregated into separate Bid and Ask OHLC with first/last
  tick timestamps and a compact price-change path. LONG outcomes use Bid exits;
  SHORT outcomes use Ask exits, and same-bar order is resolved from tick order.
- Absolute provider tick age and local transport age are checked independently;
  market closed, stale feed and disconnected bridge remain distinct states.
- Side-aware costs use Ask-entry/Bid-exit for LONG and Bid-entry/estimated
  Ask-exit for SHORT. Exit spread requires at least 12 exact bars and uses the
  configured conservative quantile instead of an optimistic median.
- Every material realtime decision is an immutable proposal instance. Feedback
  and policy outcomes refer to its `proposal_id`, quote, entry and health state.
- Live coverage, absolute and baseline-relative Brier, ECE and MAE-coverage
  gates use persisted hysteresis and stop proposals while warming or unhealthy.
- Dynamic MT5 account, currency and symbol specifications.
- MT5 `order_calc_margin()` and `order_calc_profit()` are authoritative for
  margin and account-currency PnL conversion when available.
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
core eventually marks the bridge `BRIDGE_DISCONNECTED` and returns
`NO_PREDICTION`.

If multiple MT5 terminals are installed, copy `.env.example` to a local ignored
`.env` or set `MT5_PATH`. Credentials are optional when the selected terminal is
already logged in; never commit them.

MetaTrader 5 Python timestamps are treated as UTC. Only set
`MT5_UTC_OFFSET_OVERRIDE_HOURS` when a provider has been independently verified
to encode a fixed non-UTC offset. The same verified normalization is applied to
rate bars, ticks and the absolute freshness calculation. A future tick beyond
the freshness tolerance is rejected instead of being clamped into a fresh tick.

`MT5_MARKET_UTC_OFFSET_HOURS` controls only interpretation of the configured
`GOLDm#` quote window (01:00–23:59). Set it to the timezone used by the broker's
instrument specification; when empty it follows the verified provider timestamp
offset. It does not alter normalized stored timestamps.

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
holdout gate passes, the artifact is loaded, exercised with a deterministic
golden forecast, then atomically promoted to the local `latest` shadow slot.
It remains registered as `candidate`;
promotion to champion is deliberately manual and requires enough settled shadow
predictions. Training artifacts and historical exports are local and ignored by
Git.

Candidate training is intentionally blocked unless the dataset manifest proves
that it came from a Bid chart and historical Bid/Ask ticks, chart Bid matches
aggregated tick Bid within one tick, and tick coverage is sufficient. Training
spread features use exact `ask_close - bid_close` plus rolling median/q75/q90
and spread/ATR. Existing artifacts from the previous feature or barrier
contract must be retired and retrained.

The backfill command treats the fetched MT5 window as authoritative and replaces
the local `GOLDm#`/M5 bar cache before importing chunks. Use `--append` only when
an intentional incremental import is required. Every export also creates a
sanitized `.manifest.json` containing timestamps, row count, gap and chart/tick
parity summaries, tick coverage, Git commit and SHA-256.

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

Only candidate schema v3 with eligibility gate version 3 or newer is executable.
Schema v2 and gate-v2 artifacts belong under
`artifacts/catboost/retired/`; the launcher and retry path fall back to the
empirical baseline rather than loading one. Candidate schema v3 contains
`evaluation.json`, `model_card.md`,
`checksums.sha256`, quantile/direction/barrier models and dynamic MFE/MAE
models. Import a downloaded Colab ZIP through the checked and immutable importer:

```powershell
.\.venv\Scripts\python.exe .\scripts\import-colab-artifact.py `
  .\downloads\xpde-candidate.zip
```

The importer requires the exact artifact file set, validates the schema, feature,
barrier, executable-side and eligibility contracts, verifies every checksum,
loads every model, runs a finite/non-crossing golden forecast, refuses duplicate
run IDs and only promotes eligible candidates to the local `latest` shadow slot.
Promotion uses staging plus rollback. The importer registers the candidate when
the core is available; otherwise the bridge registers it when it is loaded.
The bridge repeats checksum and contract validation before loading a candidate.

Local installation includes a separate inference dependency set (CatBoost,
NumPy and pandas). Scikit-learn remains training-only. If an otherwise valid
candidate cannot load because inference packages are absent, rerun
`XPDE-Install.cmd`; the importer reports this explicitly.

Forecast barrier evaluation and actionable proposal evaluation are intentionally
separate. Every settled H3 forecast records counterfactual LONG and SHORT
TP-before-SL outcomes using the exact forecast target/stop. Realtime SCALPER and
SNIPER changes are stored in `decision_proposal_instances`. The first actionable
instance per profile, plus an explicitly accepted instance, is eligible for
policy settlement over the next three full completed bars. A later `WAIT`
therefore cannot be confused with the earlier proposal a human actually saw.

Broker sessions come from `[market_session]` in `config/default.toml`, with
Sunday/Friday hours, maintenance gaps represented as split sessions, holiday
closures, and the MT5 symbol trade-mode check. `XPDE_MARKET_CLOSED_DATES` can
add emergency closure dates without changing source.
Forecast quality and policy outcome remain separate; a proposal metric never
masquerades as model coverage.
`tp_first_within_horizon_rate` includes no-hit outcomes in its denominator and is
the metric comparable to the trained probability; `tp_vs_sl_conditional_rate`
is reported separately. Live monitoring also exposes direction/barrier Brier,
reliability bins, expected calibration error, and per-horizon coverage, width and
pinball loss. Warming health already forces `WAIT`. Once at least 100 H3
outcomes exist, baseline-relative gates and multi-window hysteresis control
`HEALTHY`, `DEGRADED`, and `SUSPENDED` without retraining or mutating the model.

Rust is the canonical production settlement implementation. The Python
`xpde-settle` command is retained for offline analysis/replay and must not run as
a second production settlement worker.

Useful read-only endpoints:

```text
GET /api/v1/market/cursor
GET /api/v1/models
GET /api/v1/evaluation/summary
```

See [architecture](docs/architecture.md) for component and safety boundaries.
