# XPDE Read-Only Technical Analysis MCP

This package exposes XPDE forecasts, core proposals, evidence and deterministic
technical context to an MCP client over **STDIO only**.

It is not a second prediction model. CatBoost remains the only probabilistic
forecast authority and Rust XPDE core remains the only LONG/SHORT/WAIT/
NO_PREDICTION decision authority.

## Safety boundary

The server can only:

- call three fixed local GET endpoints;
- open SQLite using `mode=ro` and `PRAGMA query_only=ON`;
- read `manifest.json` for a registered model whose resolved artifact directory
  stays under `XPDE_ARTIFACT_ROOT`;
- derive versioned indicators from completed bars.

It cannot execute orders, send feedback, register/promote a model, replace
policy, retrain, create forecasts, write SQLite, read arbitrary files, or open
an HTTP listener.

## Windows installation

From the XPDE repository:

```powershell
py -m venv mcp\.venv
.\mcp\.venv\Scripts\python.exe -m pip install --upgrade pip
.\mcp\.venv\Scripts\python.exe -m pip install -e ".\mcp"
```

`XPDE-Install-MCP.cmd` performs the same installation.

Configure the MCP client:

```json
{
  "mcpServers": {
    "xpde": {
      "command": "E:\\path\\to\\xpde\\mcp\\.venv\\Scripts\\python.exe",
      "args": ["-m", "xpde_mcp.server"],
      "env": {
        "XPDE_API_BASE": "http://127.0.0.1:8787",
        "XPDE_DB_PATH": "E:\\path\\to\\xpde\\data\\xpde.sqlite",
        "XPDE_ARTIFACT_ROOT": "E:\\path\\to\\xpde\\artifacts\\catboost"
      }
    }
  }
}
```

`XPDE_API_BASE` is restricted to a loopback HTTP(S) origin. The database and
artifact paths are resolved before use.

## Primary tool

`xpde_analyze_current(profile="SCALPER", depth="STANDARD")` produces a
`TechnicalAnalysisPacket` containing:

- current/source status, including `forecast_usable_as_guide`,
  `core_actionable`, and `evidence_stage`;
- H1/H3/H6/H12 quantile prices derived as
  `origin_close * exp(log_return)`;
- origin-based `P(UP)` and `P(NON-UP)`;
- completed-bar M5/M15/H1 technical views;
- confirmed-pivot support/resistance;
- deterministic forecast–technical confluence/conflict;
- the exact immutable XPDE core proposal and reason codes;
- offline/live evidence;
- observations with provenance IDs.

The TA contract is `xpde-ta-goldm-m5-v1`:

- M5 is native;
- M15 needs all three exact M5 components;
- H1 needs all twelve exact M5 components;
- incomplete aggregation buckets are discarded;
- current M5 is observation-only;
- trend: EMA20, EMA50, five-bar EMA20 slope;
- momentum: Wilder RSI14, ROC3, ROC6;
- volatility: Wilder ATR14 and 100-observation percentile rank;
- structure: confirmed 2-left/2-right swing points and HH/HL/LH/LL;
- levels: nearest confirmed swing support/resistance.

No indicator emits a trade signal.

## Supporting tools

- `xpde_get_current`
- `xpde_get_analysis_bundle`
- `xpde_get_recent_predictions`
- `xpde_get_prediction_evidence`
- `xpde_get_evaluation_summary`
- `xpde_get_model_manifest`
- `xpde_get_market_structure`
- `xpde_explain_prediction`
- `xpde_get_model_evidence`

The prompt `analyze_current_xpde_prediction` instructs an agent to retain the
core decision and treat WARMING_UP correctly: a current forecast remains usable
as a guide while live evidence is not mature enough for stability or promotion
claims.

## Semantic contract

- A forecast is not future truth.
- `direction_probability_up` is `P(UP at forecast origin)`.
- `1 - direction_probability_up` is `P(NON-UP)`: negative or flat, not strict
  `P(DOWN)`.
- Barrier probabilities are origin-based and are not recomputed for current
  entry.
- WARMING_UP does not erase a current forecast.
- WAIT and NO_PREDICTION are valid core outputs.
- An MCP/agent must not invent a replacement BUY/SELL decision.

The machine-readable schema and tool boundary are exposed as MCP resources:

- `xpde://contracts/technical-analysis-packet`
- `xpde://contracts/tools`

## Development checks

```powershell
.\mcp\.venv\Scripts\python.exe -m pip install -e ".\mcp[dev]"
.\mcp\.venv\Scripts\python.exe -m pytest -q mcp\tests
```
