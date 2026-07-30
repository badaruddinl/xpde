# XPDE — GOLDm# Probabilistic Decision Engine

Local shadow-mode decision support for the MetaTrader 5 symbol `GOLDm#`.
XPDE forecasts several possible price paths, applies account-aware cost gates,
records outcomes and feedback, then leaves the final decision to a human.

> This repository contains no auto-entry or order-execution capability.

## Read-only technical-analysis MCP

The separate package in [`mcp/`](mcp/) exposes XPDE over MCP STDIO without
adding a write path or a second prediction model. Its primary
`xpde_analyze_current` tool builds the versioned `xpde-ta-goldm-m5-v2` packet
from completed M5 bars, strict M15/H1 aggregation, EMA/RSI/ATR/ROC, confirmed
swing structure, support/resistance, the current forecast, exact Rust core
proposal and model evidence.

The MCP opens SQLite with `mode=ro` and `query_only`, calls only fixed local GET
routes, confines manifest reads to `XPDE_ARTIFACT_ROOT`, and runs only through
STDIO. It cannot execute orders, write feedback, promote/retrain models, replace
policy, create forecasts, or change the database. Install it independently with
`XPDE-Install-MCP.cmd`; the existing bridge/inference/training environment is
unchanged.

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
- Live `COPY_TICKS_ALL` paths are persisted separately from OHLC. Forecast and
  proposal settlement use the same ordered first-touch algorithm as training;
  proposal evaluation begins strictly after its exact quote timestamp, including
  the remainder of the current M5 candle.
- Absolute provider tick age and local transport age are checked independently;
  market closed, stale feed and disconnected bridge remain distinct states.
- Side-aware costs use Ask-entry/Bid-exit for LONG and Bid-entry/estimated
  Ask-exit for SHORT. Exit spread requires at least 12 exact bars and uses the
  configured conservative quantile instead of an optimistic median.
- Every material realtime decision is an immutable proposal instance. Feedback
  and policy outcomes refer to its `proposal_id`, quote, entry and health state.
  Instances are emitted only by forecast/snapshot events or the authoritative
  one-second policy clock; HTTP GET and WebSocket serialization are read-only.
- Downtime catch-up restores complete ordered `COPY_TICKS_ALL` paths as well as
  OHLC. Gzip requests are split by encoded bytes, bar count and tick-point count.
  Full resets stage strictly ordered, single-use chunks under an import ID and
  replace live history atomically only after every expected chunk is present.
- Realtime collection recounts the current and previous M5 bucket from
  authoritative raw ticks on every refresh. Overlap is never approximated from
  the compact price-change path, so executable tick coverage cannot freeze.
- Empty, stale or underfilled tracker memory is rehydrated from the timestamps
  of the latest 64 completed trading bars rather than a wall-clock lookback.
  Catch-up paths also hydrate inference memory, and the candidate remains
  fail-closed until its latest 24 completed bars have finite Bid/Ask closes with
  at least 95% executable tick coverage. A failure anywhere in that feature
  window requests a bounded authoritative reload (at most once per M5 bucket)
  until a rebuilt snapshot proves the window healthy.
- Settlement verifies every expected M5 bucket, path metadata and completed-path
  source. Missing or partial windows remain `TICK_PATH_INCOMPLETE` or
  `SESSION_INTERRUPTED`; they can never become a false no-hit or enter
  Brier/ECE/model-health evidence.
- A one-second policy clock keeps disconnect, feed age, session close, forecast
  expiry and entry-window expiry authoritative even when MT5 sends no new event.
- Live coverage, absolute and baseline-relative Brier, ECE and MAE-coverage
  gates use persisted hysteresis and stop proposals while warming or unhealthy.
- Direction evidence uses the training truth (`actual_return > 0`) and scores
  the displayed classifier at `P(UP) >= 0.5`. Evaluation recomputes accuracy
  from probability and return, so legacy stored hit flags cannot bias it.
- The binary complement is named `NON-UP` (negative or flat), never strict
  DOWN. SHORT still requires independent negative q50 and SHORT barrier
  agreement. Offline artifacts and the live 200-settled H3 window publish flat
  samples, denominator and rate using `|log-return| <= 1e-12`.
- Origin direction and barrier probabilities are hidden while market data is
  disconnected, closed, stale or incomplete. They remain visible during model
  `WARMING_UP` when the underlying market data itself is current. Unavailable
  text first reports the actual forecast lifecycle (waiting, mismatch, expired
  or demo) before diagnosing connection or tick freshness.
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
The same range is rebuilt from `COPY_TICKS_ALL`; each completed bar must carry a
valid ordered Bid/Ask path with at least 95% chart tick-volume coverage.
Backfilled bars can settle earlier forecasts, but the bridge deliberately waits
for a genuinely new completed candle before emitting another live forecast.
Incomplete recovery remains pending and can be retried safely.

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
The trainer runs with `--no-register`; only the verified importer may register
the promoted artifact. The legacy manual registration helper carries the same
label contract if it is invoked explicitly.
It remains registered as `candidate`;
promotion to champion is deliberately manual and requires enough settled shadow
predictions. Training artifacts and historical exports are local and ignored by
Git. The default historical export is `data/goldm_m5.csv.gz`; pandas reads it
directly and the compressed stream avoids storing or uploading the large
tick-path CSV uncompressed.

Candidate training is intentionally blocked unless the dataset manifest proves
that it came from a Bid chart and historical Bid/Ask ticks, chart Bid matches
aggregated tick Bid within one tick, and tick coverage is sufficient. Training
requires `COPY_TICKS_ALL`, a parseable path for every bar, exact path-to-OHLC
reconstruction, a 100% valid-path rate and at least 95% tick-volume coverage.
spread features use exact `ask_close - bid_close` plus rolling median/q75/q90
and spread/ATR. Existing artifacts from the previous feature or barrier
contract must be retired and retrained.

The active model contract is `goldm-m5-v5` with
`exact-contiguous-m5-horizons-v1`. Labels for H1/H3/H6/H12 are masked unless
every intervening timestamp is exactly five minutes apart, so weekend and
maintenance gaps cannot become synthetic short-horizon returns. H3 barrier and
MFE/MAE labels use the same continuity rule. TP/SL levels are snapped outward
to the broker tick size, and the artifact records the immutable tick size used
for training.

Live health reports both fully-settled metrics and the completeness of the most
recent 200 predictions whose H12 horizon is due. A model cannot become healthy
unless at least 98% of that due cohort has all four price outcomes and both H3
barrier outcomes. Completed and current live bars must meet the same 95% tick
coverage contract; a valid-looking partial path remains incomplete and can
never become `NO_HIT_BEFORE_EXPIRY`.

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
loads every model, runs a finite/non-crossing golden forecast with a complete
500-bar executable Bid/Ask history and the artifact's immutable broker tick size,
refuses duplicate run IDs and only promotes eligible candidates to the local
`latest` shadow slot.
Promotion uses staging plus rollback. The importer registers the candidate when
the core is available; otherwise the bridge registers it when it is loaded.
The bridge repeats checksum and contract validation before loading a candidate.

Local installation includes a separate inference dependency set (CatBoost,
NumPy and pandas). Scikit-learn remains training-only. If an otherwise valid
candidate cannot load because inference packages are absent, rerun
`XPDE-Install.cmd`; the importer reports this explicitly.

Forecast barrier evaluation and actionable proposal evaluation are intentionally
separate. Every settled H3 forecast records counterfactual LONG and SHORT
TP-before-SL outcomes using ordered executable ticks and the exact forecast
target/stop. Realtime SCALPER and SNIPER changes are stored in
`decision_proposal_instances` from authoritative market or policy-clock events.
Material change
fingerprints quantize prices to the broker tick and reward/risk to policy bands,
so floating-point noise does not create evidence. Evidence memberships are
reported separately as `FIRST_ACTIONABLE`, `HUMAN_ACCEPTED` and
`HUMAN_REJECTED`; they are never blended into one policy rate. Proposal outcome
time starts after `quote_timestamp` and ends at the forecast's exact
`outcome_matures_at`.

Broker sessions come from `[market_session]` in `config/default.toml`, with
Sunday/Friday hours, maintenance gaps represented as split sessions, holiday
closures, and the MT5 symbol trade-mode check. `XPDE_MARKET_CLOSED_DATES` can
add emergency closure dates without changing source.
`market_session.timezone = "fixed_broker_utc_offset"` states the limitation
explicitly. The integer broker UTC offset must be updated operationally for DST
until an authoritative broker timezone source is available.
Because one prediction is a single H1/H3/H6/H12 envelope, publication requires
the session to remain open through `origin + 65 minutes`. The HTTP forecast
endpoint enforces the same boundary before persisting a prediction.
Forecast quality and policy outcome remain separate; a proposal metric never
masquerades as model coverage.

Prediction settlement is an explicit state machine. Price horizons may settle
first, but a prediction is not `SETTLED` until both H3 LONG and SHORT barrier
outcomes exist. Recoverable states (`BARRIER_PENDING`, `TICK_PATH_INCOMPLETE`,
and `SESSION_INTERRUPTED`) remain eligible for later replay after catch-up.
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
