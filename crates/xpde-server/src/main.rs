use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::RwLock};
use tower_http::{
    cors::CorsLayer,
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing::{info, warn};
use uuid::Uuid;
use xpde_domain::{
    AccountSnapshot, BARRIER_HORIZON_BARS, BARRIER_SPEC_ID, CalibrationStatus, ChartMode,
    DataQuality, DecisionAction, DecisionPolicy, DecisionProposal, EXECUTABLE_SIDE_CONTRACT_ID,
    ForecastEnvelope, ForecastPoint, HumanFeedback, MarketBar, MarketSnapshot, MarketStatus,
    SymbolSpec, TradeMode, decide,
};

const MIGRATION: &str = include_str!("../../../migrations/001_init.sql");

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    runtime: Arc<RwLock<RuntimeState>>,
    started_at: DateTime<Utc>,
    policies: PolicySet,
}

#[derive(Debug, Clone)]
struct PolicySet {
    scalper: DecisionPolicy,
    sniper: DecisionPolicy,
    model_health: ModelHealthPolicy,
}

#[derive(Debug, Deserialize)]
struct PolicyFile {
    broker_profile: BrokerPolicyFile,
    policy: ProfilePoliciesFile,
    model_health: ModelHealthPolicy,
}

#[derive(Debug, Deserialize)]
struct BrokerPolicyFile {
    broker_policy_id: String,
    cost_model_id: String,
    commission_usd_per_lot: f64,
    expected_exit_spread_usd: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct ModelHealthPolicy {
    minimum_settled_predictions: usize,
    minimum_interval_coverage: f64,
    maximum_interval_coverage: f64,
    maximum_direction_brier: f64,
    maximum_direction_brier_ratio_to_baseline: f64,
    maximum_barrier_brier_ratio_to_baseline: f64,
    maximum_barrier_ece: f64,
    minimum_mae_q90_coverage: f64,
    maximum_mae_q90_coverage: f64,
    degrade_after_failed_windows: usize,
    suspend_after_severe_windows: usize,
    recover_after_healthy_windows: usize,
    warming_up_forces_wait: bool,
}

#[derive(Debug, Deserialize)]
struct ProfilePoliciesFile {
    scalper: ProfilePolicyFile,
    sniper: ProfilePolicyFile,
}

#[derive(Debug, Deserialize)]
struct ProfilePolicyFile {
    max_spread_usd: f64,
    max_spread_atr_ratio: f64,
    slippage_buffer_usd: f64,
    max_entry_deviation_atr: f64,
    min_reward_risk_ratio: f64,
    min_direction_probability: f64,
    min_barrier_probability: f64,
    exit_spread_min_samples: usize,
    exit_spread_quantile: f64,
    exit_spread_window: usize,
    maximum_decision_age_seconds: u64,
}

fn load_policy_set() -> Result<PolicySet, String> {
    let path = env::var("XPDE_CONFIG_PATH").unwrap_or_else(|_| "config/default.toml".to_owned());
    let contents = fs::read_to_string(&path)
        .map_err(|error| format!("failed to read policy config {path}: {error}"))?;
    let config: PolicyFile = toml::from_str(&contents)
        .map_err(|error| format!("failed to parse policy config {path}: {error}"))?;
    let apply = |mut policy: DecisionPolicy, values: &ProfilePolicyFile| {
        policy.broker_policy_id = config.broker_profile.broker_policy_id.clone();
        policy.cost_model_id = config.broker_profile.cost_model_id.clone();
        policy.commission_usd_per_lot = config.broker_profile.commission_usd_per_lot;
        policy.expected_exit_spread_usd = config.broker_profile.expected_exit_spread_usd;
        policy.max_spread_usd = values.max_spread_usd;
        policy.max_spread_atr_ratio = values.max_spread_atr_ratio;
        policy.slippage_buffer_usd = values.slippage_buffer_usd;
        policy.max_entry_deviation_atr = values.max_entry_deviation_atr;
        policy.min_reward_risk_ratio = values.min_reward_risk_ratio;
        policy.min_direction_probability = values.min_direction_probability;
        policy.min_barrier_probability = values.min_barrier_probability;
        policy.exit_spread_min_samples = values.exit_spread_min_samples;
        policy.exit_spread_quantile = values.exit_spread_quantile;
        policy.exit_spread_window = values.exit_spread_window;
        policy.maximum_decision_age_seconds = values.maximum_decision_age_seconds;
        policy
    };
    Ok(PolicySet {
        scalper: apply(DecisionPolicy::scalper(), &config.policy.scalper),
        sniper: apply(DecisionPolicy::sniper(), &config.policy.sniper),
        model_health: config.model_health.clone(),
    })
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeState {
    mode: &'static str,
    connection_status: &'static str,
    updated_at: DateTime<Utc>,
    forecast_status: ForecastStatus,
    snapshot: MarketSnapshot,
    forecast: ForecastEnvelope,
    proposals: Vec<DecisionProposal>,
    model_health: ModelHealth,
    safety: SafetyStatus,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ForecastStatus {
    Demo,
    WaitingForFirstForecast,
    Current,
    OriginMismatch,
    Expired,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ModelHealthStatus {
    WarmingUp,
    Healthy,
    Degraded,
    Suspended,
}

impl ModelHealthStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::WarmingUp => "WARMING_UP",
            Self::Healthy => "HEALTHY",
            Self::Degraded => "DEGRADED",
            Self::Suspended => "SUSPENDED",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "HEALTHY" => Self::Healthy,
            "DEGRADED" => Self::Degraded,
            "SUSPENDED" => Self::Suspended,
            _ => Self::WarmingUp,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ModelHealth {
    status: ModelHealthStatus,
    sample_size: usize,
    minimum_sample_size: usize,
    interval_coverage: Option<f64>,
    direction_brier: Option<f64>,
    direction_baseline_brier: Option<f64>,
    barrier_brier: Option<f64>,
    barrier_baseline_brier: Option<f64>,
    barrier_ece: Option<f64>,
    mae_q90_coverage: Option<f64>,
    reason_codes: Vec<String>,
    checks: Vec<ModelHealthCheck>,
    consecutive_failures: usize,
    consecutive_successes: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ModelHealthCheck {
    code: String,
    observed: Option<f64>,
    threshold: String,
    passed: bool,
}

impl ModelHealth {
    fn warming_up(minimum_sample_size: usize) -> Self {
        Self {
            status: ModelHealthStatus::WarmingUp,
            sample_size: 0,
            minimum_sample_size,
            interval_coverage: None,
            direction_brier: None,
            direction_baseline_brier: None,
            barrier_brier: None,
            barrier_baseline_brier: None,
            barrier_ece: None,
            mae_q90_coverage: None,
            reason_codes: vec!["MODEL_LIVE_HEALTH_WARMING_UP".to_owned()],
            checks: Vec::new(),
            consecutive_failures: 0,
            consecutive_successes: 0,
        }
    }
}

fn forecast_status(
    snapshot: &MarketSnapshot,
    forecast: &ForecastEnvelope,
    feed_is_demo: bool,
    now: DateTime<Utc>,
) -> ForecastStatus {
    if feed_is_demo {
        return ForecastStatus::Demo;
    }
    if forecast.model_id == "baseline-demo-v1" {
        return ForecastStatus::WaitingForFirstForecast;
    }
    let Some(latest_completed) = snapshot.bars.iter().max_by_key(|bar| bar.timestamp) else {
        return ForecastStatus::OriginMismatch;
    };
    if forecast.origin_bar_timestamp != latest_completed.timestamp
        || (forecast.origin_close - latest_completed.close).abs() > 1e-8
    {
        return ForecastStatus::OriginMismatch;
    }
    let decision_valid_until = forecast.origin_bar_timestamp + chrono::Duration::minutes(10);
    if now > decision_valid_until {
        ForecastStatus::Expired
    } else {
        ForecastStatus::Current
    }
}

#[derive(Debug, Clone, Serialize)]
struct SafetyStatus {
    auto_trading_enabled: bool,
    human_confirmation_required: bool,
    feed_is_demo: bool,
}

#[derive(Debug, Deserialize)]
struct BackfillRequest {
    symbol: String,
    timeframe: String,
    provider: String,
    broker_offset_hours: i32,
    #[serde(default)]
    reset: bool,
    bars: Vec<MarketBar>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelRegistration {
    model_id: String,
    model_type: String,
    status: String,
    feature_version: String,
    schema_version: i64,
    eligibility_gate_version: i64,
    training_mode: String,
    eligible_for_shadow: bool,
    barrier_spec_id: String,
    executable_side_contract_id: String,
    eligibility_gates: serde_json::Map<String, serde_json::Value>,
    artifact_path: String,
    metrics: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum BarrierOutcome {
    TpFirst,
    SlFirst,
    NoHitBeforeExpiry,
    #[cfg(test)]
    AmbiguousSameBar,
}

impl BarrierOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::TpFirst => "TP_FIRST",
            Self::SlFirst => "SL_FIRST",
            Self::NoHitBeforeExpiry => "NO_HIT_BEFORE_EXPIRY",
            #[cfg(test)]
            Self::AmbiguousSameBar => "AMBIGUOUS_SAME_BAR",
        }
    }
}

#[cfg(test)]
fn barrier_outcome(
    action: DecisionAction,
    target: f64,
    stop: f64,
    bars: &[(f64, f64, f64)],
) -> Option<BarrierOutcome> {
    if !target.is_finite() || !stop.is_finite() {
        return None;
    }
    for &(high, low, _) in bars {
        let (tp_hit, sl_hit) = match action {
            DecisionAction::Long => (high >= target, low <= stop),
            DecisionAction::Short => (low <= target, high >= stop),
            _ => return None,
        };
        match (tp_hit, sl_hit) {
            (true, false) => return Some(BarrierOutcome::TpFirst),
            (false, true) => return Some(BarrierOutcome::SlFirst),
            (true, true) => return Some(BarrierOutcome::AmbiguousSameBar),
            (false, false) => {}
        }
    }
    Some(BarrierOutcome::NoHitBeforeExpiry)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TickBarrierOutcome {
    outcome: BarrierOutcome,
    first_touch_time_msc: Option<i64>,
    first_touch_price: Option<f64>,
}

type ExecutableTick = (i64, f64, f64);

fn tick_sequence_barrier_outcome(
    action: DecisionAction,
    target: f64,
    stop: f64,
    ticks: &[ExecutableTick],
    start_exclusive_msc: i64,
    end_exclusive_msc: i64,
) -> Option<TickBarrierOutcome> {
    if !target.is_finite() || !stop.is_finite() || end_exclusive_msc <= start_exclusive_msc {
        return None;
    }
    let mut observed = false;
    for &(time_msc, bid, ask) in ticks {
        if time_msc <= start_exclusive_msc || time_msc >= end_exclusive_msc {
            continue;
        }
        observed = true;
        let executable_price = match action {
            DecisionAction::Long => bid,
            DecisionAction::Short => ask,
            _ => return None,
        };
        let outcome = match action {
            DecisionAction::Long if executable_price >= target => Some(BarrierOutcome::TpFirst),
            DecisionAction::Long if executable_price <= stop => Some(BarrierOutcome::SlFirst),
            DecisionAction::Short if executable_price <= target => Some(BarrierOutcome::TpFirst),
            DecisionAction::Short if executable_price >= stop => Some(BarrierOutcome::SlFirst),
            _ => None,
        };
        if let Some(outcome) = outcome {
            return Some(TickBarrierOutcome {
                outcome,
                first_touch_time_msc: Some(time_msc),
                first_touch_price: Some(executable_price),
            });
        }
    }
    observed.then_some(TickBarrierOutcome {
        outcome: BarrierOutcome::NoHitBeforeExpiry,
        first_touch_time_msc: None,
        first_touch_price: None,
    })
}

#[derive(Debug, Serialize)]
struct ModelRecord {
    model_id: String,
    model_type: String,
    status: String,
    feature_version: String,
    schema_version: i64,
    eligibility_gate_version: i64,
    training_mode: String,
    eligible_for_shadow: bool,
    barrier_spec_id: String,
    executable_side_contract_id: String,
    artifact_path: String,
    metrics: serde_json::Value,
    created_at: String,
    promoted_at: Option<String>,
}

struct Store {
    connection: Mutex<Connection>,
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<(), rusqlite::Error> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|existing| existing == column) {
        connection.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

fn brier_score(probabilities: &[f64], outcomes: &[f64]) -> Option<f64> {
    if probabilities.is_empty() || probabilities.len() != outcomes.len() {
        return None;
    }
    Some(
        probabilities
            .iter()
            .zip(outcomes)
            .map(|(probability, outcome)| (probability - outcome).powi(2))
            .sum::<f64>()
            / probabilities.len() as f64,
    )
}

fn constant_baseline_brier(outcomes: &[f64]) -> Option<f64> {
    if outcomes.is_empty() {
        return None;
    }
    let base_rate = outcomes.iter().sum::<f64>() / outcomes.len() as f64;
    brier_score(&vec![base_rate; outcomes.len()], outcomes)
}

fn expected_calibration_error(probabilities: &[f64], outcomes: &[f64]) -> Option<f64> {
    if probabilities.is_empty() || probabilities.len() != outcomes.len() {
        return None;
    }
    let mut bins = vec![(0usize, 0.0f64, 0.0f64); 10];
    for (&probability, &outcome) in probabilities.iter().zip(outcomes) {
        let index = ((probability.clamp(0.0, 0.999_999) * 10.0) as usize).min(9);
        bins[index].0 += 1;
        bins[index].1 += probability;
        bins[index].2 += outcome;
    }
    Some(
        bins.into_iter()
            .filter(|(count, _, _)| *count > 0)
            .map(|(count, probability_sum, outcome_sum)| {
                let count_f64 = count as f64;
                count_f64 * ((probability_sum / count_f64) - (outcome_sum / count_f64)).abs()
            })
            .sum::<f64>()
            / probabilities.len() as f64,
    )
}

fn apply_model_health_gate(
    proposals: &mut [DecisionProposal],
    health: &ModelHealth,
    warming_up_forces_wait: bool,
) {
    let reason = match health.status {
        ModelHealthStatus::WarmingUp if warming_up_forces_wait => {
            Some("MODEL_LIVE_HEALTH_WARMING_UP")
        }
        ModelHealthStatus::WarmingUp => None,
        ModelHealthStatus::Degraded => Some("MODEL_LIVE_HEALTH_DEGRADED"),
        ModelHealthStatus::Suspended => Some("MODEL_LIVE_HEALTH_SUSPENDED"),
        _ => None,
    };
    for proposal in proposals {
        proposal.model_health_status = health.status.as_str().to_owned();
        let Some(reason) = reason else {
            continue;
        };
        if !proposal.reason_codes.iter().any(|code| code == reason) {
            proposal.reason_codes.push(reason.to_owned());
        }
        if matches!(
            proposal.action,
            DecisionAction::Long | DecisionAction::Short
        ) {
            proposal.action = DecisionAction::Wait;
            proposal.target_price = None;
            proposal.invalidation_price = None;
        }
    }
}

impl Store {
    fn open(path: &Path) -> Result<Self, rusqlite::Error> {
        if let Some(parent) = path.parent()
            && let Err(error) = fs::create_dir_all(parent)
        {
            panic!(
                "failed to create database directory {}: {error}",
                parent.display()
            );
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(MIGRATION)?;
        ensure_column(&connection, "predictions", "origin_bar_timestamp", "TEXT")?;
        ensure_column(&connection, "predictions", "origin_close", "REAL")?;
        ensure_column(&connection, "predictions", "origin_bid", "REAL")?;
        ensure_column(&connection, "predictions", "origin_ask", "REAL")?;
        ensure_column(&connection, "predictions", "origin_bar_index", "INTEGER")?;
        ensure_column(&connection, "predictions", "feature_version", "TEXT")?;
        ensure_column(&connection, "predictions", "barrier_spec_id", "TEXT")?;
        ensure_column(
            &connection,
            "predictions",
            "direction_probability_up",
            "REAL",
        )?;
        ensure_column(
            &connection,
            "predictions",
            "barrier_probability_long",
            "REAL",
        )?;
        ensure_column(
            &connection,
            "predictions",
            "barrier_probability_short",
            "REAL",
        )?;
        ensure_column(
            &connection,
            "predictions",
            "is_duplicate",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &connection,
            "predictions",
            "settlement_status",
            "TEXT NOT NULL DEFAULT 'PENDING'",
        )?;
        ensure_column(&connection, "predictions", "decision_valid_until", "TEXT")?;
        ensure_column(&connection, "predictions", "outcome_matures_at", "TEXT")?;
        ensure_column(
            &connection,
            "prediction_horizon_outcomes",
            "barrier_long_outcome",
            "TEXT",
        )?;
        ensure_column(&connection, "human_feedback", "profile", "TEXT")?;
        ensure_column(&connection, "human_feedback", "proposal_action", "TEXT")?;
        ensure_column(&connection, "human_feedback", "model_id", "TEXT")?;
        ensure_column(&connection, "human_feedback", "forecast_side", "TEXT")?;
        ensure_column(&connection, "human_feedback", "selected_reason", "TEXT")?;
        ensure_column(&connection, "human_feedback", "proposal_id", "TEXT")?;
        ensure_column(
            &connection,
            "model_registry",
            "schema_version",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &connection,
            "model_registry",
            "eligibility_gate_version",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &connection,
            "model_registry",
            "training_mode",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &connection,
            "symbol_specs",
            "chart_mode",
            "TEXT NOT NULL DEFAULT 'UNKNOWN'",
        )?;
        ensure_column(
            &connection,
            "symbol_specs",
            "quote_currency",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &connection,
            "symbol_specs",
            "pnl_currency",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        for (column, definition) in [
            ("profit_per_price_unit_per_lot_buy", "REAL"),
            ("profit_per_price_unit_per_lot_sell", "REAL"),
            ("pnl_calculation_source", "TEXT NOT NULL DEFAULT ''"),
            ("conversion_rate", "REAL"),
            ("conversion_timestamp", "TEXT"),
            ("trade_mode_enabled", "INTEGER NOT NULL DEFAULT 0"),
            ("symbol_profit_currency", "TEXT NOT NULL DEFAULT ''"),
            ("calculated_pnl_currency", "TEXT NOT NULL DEFAULT ''"),
            ("trade_mode", "TEXT NOT NULL DEFAULT 'UNKNOWN'"),
        ] {
            ensure_column(&connection, "symbol_specs", column, definition)?;
        }
        for (column, definition) in [
            ("bid_open", "REAL"),
            ("bid_high", "REAL"),
            ("bid_low", "REAL"),
            ("bid_close", "REAL"),
            ("ask_open", "REAL"),
            ("ask_high", "REAL"),
            ("ask_low", "REAL"),
            ("ask_close", "REAL"),
            ("executable_tick_count", "INTEGER NOT NULL DEFAULT 0"),
            ("first_tick_msc", "INTEGER"),
            ("last_tick_msc", "INTEGER"),
        ] {
            ensure_column(&connection, "market_bars", column, definition)?;
        }
        ensure_column(
            &connection,
            "model_registry",
            "eligible_for_shadow",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &connection,
            "model_registry",
            "barrier_spec_id",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &connection,
            "model_registry",
            "executable_side_contract_id",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &connection,
            "prediction_horizon_outcomes",
            "barrier_short_outcome",
            "TEXT",
        )?;
        ensure_column(
            &connection,
            "decision_proposal_instances",
            "evidence_source",
            "TEXT NOT NULL DEFAULT 'DIAGNOSTIC'",
        )?;
        ensure_column(
            &connection,
            "decision_proposal_outcomes",
            "first_touch_time_msc",
            "INTEGER",
        )?;
        ensure_column(
            &connection,
            "decision_proposal_outcomes",
            "first_touch_price",
            "REAL",
        )?;
        ensure_column(
            &connection,
            "decision_proposal_outcomes",
            "settlement_source",
            "TEXT NOT NULL DEFAULT 'TICK_SEQUENCE'",
        )?;
        connection.execute_batch(
            "INSERT OR IGNORE INTO decision_proposal_evidence
             (proposal_id, evidence_source, created_at)
             SELECT dpi.proposal_id, 'FIRST_ACTIONABLE', dpi.created_at
             FROM decision_proposal_instances dpi
             WHERE dpi.action IN ('LONG','SHORT')
               AND dpi.evidence_eligible=1
               AND dpi.rowid=(
                 SELECT first.rowid
                 FROM decision_proposal_instances first
                 WHERE first.prediction_id=dpi.prediction_id
                   AND first.profile=dpi.profile
                   AND first.action IN ('LONG','SHORT')
                   AND first.evidence_eligible=1
                 ORDER BY first.evaluated_at, first.rowid
                 LIMIT 1
               );
             INSERT OR IGNORE INTO decision_proposal_evidence
             (proposal_id, evidence_source, created_at)
             SELECT hf.proposal_id, 'HUMAN_ACCEPTED', hf.created_at
             FROM human_feedback hf
             WHERE hf.proposal_id IS NOT NULL AND hf.verdict='ACCEPTED'
               AND EXISTS (
                 SELECT 1 FROM decision_proposal_instances dpi
                 WHERE dpi.proposal_id=hf.proposal_id
               );
             INSERT OR IGNORE INTO decision_proposal_evidence
             (proposal_id, evidence_source, created_at)
             SELECT hf.proposal_id, 'HUMAN_REJECTED', hf.created_at
             FROM human_feedback hf
             WHERE hf.proposal_id IS NOT NULL AND hf.verdict='REJECTED'
               AND EXISTS (
                 SELECT 1 FROM decision_proposal_instances dpi
                 WHERE dpi.proposal_id=hf.proposal_id
               );",
        )?;
        connection.execute_batch(
            "UPDATE predictions
             SET feature_version=COALESCE(
               feature_version,
               json_extract(forecast_json, '$.feature_version'),
               'unknown'
             ),
             direction_probability_up=COALESCE(
               direction_probability_up,
               json_extract(forecast_json, '$.direction_probability_up')
             ),
             barrier_probability_long=COALESCE(
               barrier_probability_long,
               json_extract(forecast_json, '$.barrier_probability_long')
             ),
             barrier_probability_short=COALESCE(
               barrier_probability_short,
               json_extract(forecast_json, '$.barrier_probability_short')
             );
             DROP INDEX IF EXISTS ux_predictions_model_origin_contract;
             UPDATE predictions SET is_duplicate=0;
             UPDATE predictions
             SET is_duplicate=1
             WHERE rowid NOT IN (
               SELECT MIN(rowid)
               FROM predictions
               GROUP BY model_id, symbol, timeframe, origin_bar_timestamp,
                        feature_version, barrier_spec_id
             );
             CREATE UNIQUE INDEX ux_predictions_model_origin_contract
             ON predictions(
               model_id, symbol, timeframe, origin_bar_timestamp,
               feature_version, barrier_spec_id
             )
             WHERE is_duplicate=0;",
        )?;
        connection.execute(
            "UPDATE predictions
             SET settlement_status='LEGACY_UNSETTLEABLE'
             WHERE settlement_status='PENDING'
               AND (
                 origin_bid IS NULL OR origin_ask IS NULL
                 OR COALESCE(barrier_spec_id, '') != ?1
               )",
            [BARRIER_SPEC_ID],
        )?;
        connection.execute(
            "DELETE FROM market_bars
             WHERE timeframe='M5'
               AND CAST(strftime('%s', timestamp) AS INTEGER) % 300 != 0",
            [],
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn save_tick_paths<'a>(
        transaction: &rusqlite::Transaction<'_>,
        symbol: &str,
        timeframe: &str,
        bars: impl Iterator<Item = &'a MarketBar>,
        source: &str,
    ) -> Result<(), rusqlite::Error> {
        for bar in bars.filter(|bar| bar.has_valid_tick_path()) {
            let first = bar.first_tick_msc.expect("validated tick path start");
            let last = bar.last_tick_msc.expect("validated tick path end");
            transaction.execute(
                "INSERT INTO market_tick_paths
                 (symbol, timeframe, timestamp, tick_path_json, path_point_count,
                  first_tick_msc, last_tick_msc, path_valid, source, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9)
                 ON CONFLICT(symbol, timeframe, timestamp) DO UPDATE SET
                   tick_path_json=excluded.tick_path_json,
                   path_point_count=excluded.path_point_count,
                   first_tick_msc=excluded.first_tick_msc,
                   last_tick_msc=excluded.last_tick_msc,
                   path_valid=excluded.path_valid,
                   source=excluded.source,
                   updated_at=excluded.updated_at
                 WHERE excluded.first_tick_msc<=market_tick_paths.first_tick_msc
                   AND excluded.last_tick_msc>=market_tick_paths.last_tick_msc
                   AND excluded.path_point_count>=market_tick_paths.path_point_count",
                params![
                    symbol,
                    timeframe,
                    bar.timestamp.to_rfc3339(),
                    serde_json::to_string(&bar.executable_tick_path)
                        .unwrap_or_else(|_| "[]".to_owned()),
                    bar.executable_tick_path.len(),
                    first,
                    last,
                    source,
                    Utc::now().to_rfc3339(),
                ],
            )?;
        }
        Ok(())
    }

    fn load_tick_path(
        transaction: &rusqlite::Transaction<'_>,
        symbol: &str,
        timeframe: &str,
        first_bucket: DateTime<Utc>,
        end_exclusive: DateTime<Utc>,
    ) -> Result<Option<Vec<ExecutableTick>>, rusqlite::Error> {
        let mut statement = transaction.prepare(
            "SELECT tick_path_json
             FROM market_tick_paths
             WHERE symbol=?1 AND timeframe=?2
               AND timestamp>=?3 AND timestamp<?4 AND path_valid=1
             ORDER BY timestamp",
        )?;
        let encoded = statement
            .query_map(
                params![
                    symbol,
                    timeframe,
                    first_bucket.to_rfc3339(),
                    end_exclusive.to_rfc3339()
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        if encoded.is_empty() {
            return Ok(None);
        }
        let mut ticks = Vec::new();
        for path in encoded {
            let Ok(mut parsed) = serde_json::from_str::<Vec<ExecutableTick>>(&path) else {
                return Ok(None);
            };
            ticks.append(&mut parsed);
        }
        if ticks.is_empty() || ticks.windows(2).any(|pair| pair[0].0 > pair[1].0) {
            return Ok(None);
        }
        Ok(Some(ticks))
    }

    fn save_snapshot(&self, snapshot: &MarketSnapshot) -> Result<(), rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO account_profiles
             (login, server, currency, leverage, balance, equity, free_margin, captured_at)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
             WHERE NOT EXISTS (
               SELECT 1 FROM account_profiles
               WHERE id=(SELECT MAX(id) FROM account_profiles)
                 AND login=?1 AND server=?2 AND currency=?3 AND leverage=?4
                 AND balance=?5 AND equity=?6 AND free_margin=?7
                 AND (julianday(?8)-julianday(captured_at))*86400.0 < 30.0
             )",
            params![
                snapshot.account.login,
                snapshot.account.server,
                snapshot.account.currency,
                snapshot.account.leverage,
                snapshot.account.balance,
                snapshot.account.equity,
                snapshot.account.free_margin,
                snapshot.timestamp.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO symbol_specs
             (symbol, description, contract_size, volume_min, volume_max, volume_step,
              tick_size, tick_value, stops_level_points, digits, chart_mode,
              quote_currency, pnl_currency, symbol_profit_currency,
              calculated_pnl_currency, profit_per_price_unit_per_lot_buy,
              profit_per_price_unit_per_lot_sell, pnl_calculation_source,
              conversion_rate, conversion_timestamp, trade_mode_enabled, trade_mode, captured_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                     ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
             ON CONFLICT(symbol) DO UPDATE SET
               description=excluded.description,
               contract_size=excluded.contract_size,
               volume_min=excluded.volume_min,
               volume_max=excluded.volume_max,
               volume_step=excluded.volume_step,
               tick_size=excluded.tick_size,
               tick_value=excluded.tick_value,
               stops_level_points=excluded.stops_level_points,
               digits=excluded.digits,
               chart_mode=excluded.chart_mode,
               quote_currency=excluded.quote_currency,
               pnl_currency=excluded.pnl_currency,
               symbol_profit_currency=excluded.symbol_profit_currency,
               calculated_pnl_currency=excluded.calculated_pnl_currency,
               profit_per_price_unit_per_lot_buy=excluded.profit_per_price_unit_per_lot_buy,
               profit_per_price_unit_per_lot_sell=excluded.profit_per_price_unit_per_lot_sell,
               pnl_calculation_source=excluded.pnl_calculation_source,
               conversion_rate=excluded.conversion_rate,
               conversion_timestamp=excluded.conversion_timestamp,
               trade_mode_enabled=excluded.trade_mode_enabled,
               trade_mode=excluded.trade_mode,
               captured_at=excluded.captured_at
             WHERE description IS NOT excluded.description
                OR contract_size IS NOT excluded.contract_size
                OR volume_min IS NOT excluded.volume_min
                OR volume_max IS NOT excluded.volume_max
                OR volume_step IS NOT excluded.volume_step
                OR tick_size IS NOT excluded.tick_size
                OR tick_value IS NOT excluded.tick_value
                OR stops_level_points IS NOT excluded.stops_level_points
                OR digits IS NOT excluded.digits
                OR chart_mode IS NOT excluded.chart_mode
                OR quote_currency IS NOT excluded.quote_currency
                OR pnl_currency IS NOT excluded.pnl_currency
                OR symbol_profit_currency IS NOT excluded.symbol_profit_currency
                OR calculated_pnl_currency IS NOT excluded.calculated_pnl_currency
                OR profit_per_price_unit_per_lot_buy IS NOT excluded.profit_per_price_unit_per_lot_buy
                OR profit_per_price_unit_per_lot_sell IS NOT excluded.profit_per_price_unit_per_lot_sell
                OR pnl_calculation_source IS NOT excluded.pnl_calculation_source
                OR conversion_rate IS NOT excluded.conversion_rate
                OR conversion_timestamp IS NOT excluded.conversion_timestamp
                OR trade_mode_enabled IS NOT excluded.trade_mode_enabled
                OR trade_mode IS NOT excluded.trade_mode",
            params![
                snapshot.symbol,
                snapshot.symbol_spec.description,
                snapshot.symbol_spec.contract_size,
                snapshot.symbol_spec.volume_min,
                snapshot.symbol_spec.volume_max,
                snapshot.symbol_spec.volume_step,
                snapshot.symbol_spec.tick_size,
                snapshot.symbol_spec.tick_value,
                snapshot.symbol_spec.stops_level_points,
                snapshot.symbol_spec.digits,
                snapshot.symbol_spec.chart_mode.as_str(),
                snapshot.symbol_spec.quote_currency,
                snapshot.symbol_spec.pnl_currency,
                snapshot.symbol_spec.symbol_profit_currency,
                snapshot.symbol_spec.calculated_pnl_currency,
                snapshot.symbol_spec.profit_per_price_unit_per_lot_buy,
                snapshot.symbol_spec.profit_per_price_unit_per_lot_sell,
                snapshot.symbol_spec.pnl_calculation_source,
                snapshot.symbol_spec.conversion_rate,
                snapshot
                    .symbol_spec
                    .conversion_timestamp
                    .map(|value| value.to_rfc3339()),
                snapshot.symbol_spec.trade_mode_enabled,
                snapshot.symbol_spec.trade_mode.as_str(),
                snapshot.timestamp.to_rfc3339(),
            ],
        )?;
        if let Some(bar) = snapshot.bars.iter().max_by_key(|bar| bar.timestamp) {
            transaction.execute(
                "INSERT INTO market_bars
                 (symbol, timeframe, timestamp, open, high, low, close, tick_volume,
                  bid_open, bid_high, bid_low, bid_close,
                  ask_open, ask_high, ask_low, ask_close, executable_tick_count,
                  first_tick_msc, last_tick_msc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                         ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
                 ON CONFLICT(symbol, timeframe, timestamp) DO UPDATE SET
                   open=excluded.open, high=excluded.high, low=excluded.low,
                   close=excluded.close, tick_volume=excluded.tick_volume,
                   bid_open=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.bid_open, market_bars.bid_open) ELSE market_bars.bid_open END,
                   bid_high=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.bid_high, market_bars.bid_high) ELSE market_bars.bid_high END,
                   bid_low=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.bid_low, market_bars.bid_low) ELSE market_bars.bid_low END,
                   bid_close=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.bid_close, market_bars.bid_close) ELSE market_bars.bid_close END,
                   ask_open=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.ask_open, market_bars.ask_open) ELSE market_bars.ask_open END,
                   ask_high=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.ask_high, market_bars.ask_high) ELSE market_bars.ask_high END,
                   ask_low=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.ask_low, market_bars.ask_low) ELSE market_bars.ask_low END,
                   ask_close=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN COALESCE(excluded.ask_close, market_bars.ask_close) ELSE market_bars.ask_close END,
                   first_tick_msc=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.first_tick_msc ELSE market_bars.first_tick_msc END,
                   last_tick_msc=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.last_tick_msc ELSE market_bars.last_tick_msc END,
                   executable_tick_count=MAX(
                     excluded.executable_tick_count,
                     market_bars.executable_tick_count
                   )",
                params![
                    snapshot.symbol,
                    snapshot.timeframe,
                    bar.timestamp.to_rfc3339(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.tick_volume,
                    bar.bid_open,
                    bar.bid_high,
                    bar.bid_low,
                    bar.bid_close,
                    bar.ask_open,
                    bar.ask_high,
                    bar.ask_low,
                    bar.ask_close,
                    bar.executable_tick_count,
                    bar.first_tick_msc,
                    bar.last_tick_msc,
                ],
            )?;
        }
        Self::save_tick_paths(
            &transaction,
            &snapshot.symbol,
            &snapshot.timeframe,
            snapshot
                .bars
                .iter()
                .max_by_key(|bar| bar.timestamp)
                .into_iter()
                .chain(snapshot.current_bar.iter()),
            "LIVE_SNAPSHOT",
        )?;
        transaction.execute(
            "INSERT INTO audit_events(event_type, entity_id, payload_json, created_at)
             VALUES ('MARKET_SNAPSHOT_ACCEPTED', ?1, ?2, ?3)",
            params![
                snapshot.symbol,
                serde_json::json!({
                    "provider": snapshot.provider,
                    "timestamp": snapshot.timestamp,
                    "timeframe": snapshot.timeframe,
                    "bid": snapshot.bid,
                    "ask": snapshot.ask,
                    "completed_bar_count_received": snapshot.bars.len(),
                    "latest_completed_bar_timestamp": snapshot
                        .bars
                        .iter()
                        .max_by_key(|bar| bar.timestamp)
                        .map(|bar| bar.timestamp),
                    "data_quality": snapshot.data_quality,
                })
                .to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;
        transaction.commit()
    }

    fn save_backfill(&self, request: &BackfillRequest) -> Result<usize, rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let transaction = connection.transaction()?;
        if request.reset {
            transaction.execute(
                "DELETE FROM market_bars WHERE symbol=?1 AND timeframe=?2",
                params![request.symbol, request.timeframe],
            )?;
            transaction.execute(
                "DELETE FROM market_tick_paths WHERE symbol=?1 AND timeframe=?2",
                params![request.symbol, request.timeframe],
            )?;
        }
        let inserted = {
            let mut statement = transaction.prepare(
                "INSERT INTO market_bars
                 (symbol, timeframe, timestamp, open, high, low, close, tick_volume,
                  bid_open, bid_high, bid_low, bid_close,
                  ask_open, ask_high, ask_low, ask_close, executable_tick_count,
                  first_tick_msc, last_tick_msc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                         ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
                 ON CONFLICT(symbol, timeframe, timestamp) DO UPDATE SET
                   open=excluded.open, high=excluded.high, low=excluded.low,
                   close=excluded.close, tick_volume=excluded.tick_volume,
                   bid_open=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.bid_open ELSE market_bars.bid_open END,
                   bid_high=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.bid_high ELSE market_bars.bid_high END,
                   bid_low=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.bid_low ELSE market_bars.bid_low END,
                   bid_close=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.bid_close ELSE market_bars.bid_close END,
                   ask_open=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.ask_open ELSE market_bars.ask_open END,
                   ask_high=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.ask_high ELSE market_bars.ask_high END,
                   ask_low=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.ask_low ELSE market_bars.ask_low END,
                   ask_close=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.ask_close ELSE market_bars.ask_close END,
                   first_tick_msc=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.first_tick_msc ELSE market_bars.first_tick_msc END,
                   last_tick_msc=CASE WHEN excluded.executable_tick_count>=market_bars.executable_tick_count
                     THEN excluded.last_tick_msc ELSE market_bars.last_tick_msc END,
                   executable_tick_count=MAX(
                     excluded.executable_tick_count, market_bars.executable_tick_count
                   )",
            )?;
            let mut inserted = 0;
            for bar in &request.bars {
                inserted += statement.execute(params![
                    request.symbol,
                    request.timeframe,
                    bar.timestamp.to_rfc3339(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.tick_volume,
                    bar.bid_open,
                    bar.bid_high,
                    bar.bid_low,
                    bar.bid_close,
                    bar.ask_open,
                    bar.ask_high,
                    bar.ask_low,
                    bar.ask_close,
                    bar.executable_tick_count,
                    bar.first_tick_msc,
                    bar.last_tick_msc,
                ])?;
            }
            inserted
        };
        Self::save_tick_paths(
            &transaction,
            &request.symbol,
            &request.timeframe,
            request.bars.iter(),
            "HISTORICAL_BACKFILL",
        )?;
        transaction.execute(
            "INSERT INTO audit_events(event_type, entity_id, payload_json, created_at)
             VALUES ('MARKET_BACKFILL_ACCEPTED', ?1, ?2, ?3)",
            params![
                request.symbol,
                serde_json::json!({
                    "provider": request.provider,
                    "broker_offset_hours": request.broker_offset_hours,
                    "received": request.bars.len(),
                    "inserted": inserted,
                    "reset": request.reset,
                })
                .to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;
        let maximum_valid_timestamp = (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
        transaction.execute(
            "DELETE FROM market_bars
             WHERE symbol=?1 AND timeframe=?2 AND timestamp>?3",
            params![request.symbol, request.timeframe, maximum_valid_timestamp],
        )?;
        transaction.commit()?;
        Ok(inserted)
    }

    fn latest_bar_timestamp(&self) -> Result<Option<String>, rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        connection.query_row(
            "SELECT MAX(timestamp) FROM market_bars
             WHERE symbol=?1 AND timeframe=?2",
            params![
                xpde_domain::SUPPORTED_SYMBOL,
                xpde_domain::SUPPORTED_TIMEFRAME
            ],
            |row| row.get(0),
        )
    }

    fn save_prediction(
        &self,
        snapshot: &MarketSnapshot,
        forecast: &ForecastEnvelope,
        proposals: &[DecisionProposal],
    ) -> Result<bool, rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        let decision_valid_until = proposals
            .first()
            .map(|proposal| proposal.decision_valid_until)
            .unwrap_or(forecast.generated_at);
        let outcome_matures_at = proposals
            .first()
            .map(|proposal| proposal.outcome_matures_at)
            .unwrap_or(forecast.generated_at);
        let origin_bar = snapshot
            .bars
            .iter()
            .find(|bar| bar.timestamp == forecast.origin_bar_timestamp);
        let origin_bid = origin_bar.and_then(|bar| bar.bid_close);
        let origin_ask = origin_bar.and_then(|bar| bar.ask_close);
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO predictions
             (prediction_id, model_id, feature_version, barrier_spec_id,
              direction_probability_up, barrier_probability_long,
              barrier_probability_short,
              symbol, timeframe, origin_bar_timestamp,
              origin_close, origin_bid, origin_ask, origin_bar_index, generated_at, expires_at,
              decision_valid_until, outcome_matures_at,
              forecast_json, proposal_json, is_duplicate, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, 0, ?21)",
            params![
                forecast.prediction_id.to_string(),
                forecast.model_id,
                forecast.feature_version,
                forecast.barrier_spec_id,
                forecast.direction_probability_up,
                forecast.barrier_probability_long,
                forecast.barrier_probability_short,
                snapshot.symbol,
                snapshot.timeframe,
                forecast.origin_bar_timestamp.to_rfc3339(),
                forecast.origin_close,
                origin_bid,
                origin_ask,
                forecast.origin_bar_index,
                forecast.generated_at.to_rfc3339(),
                outcome_matures_at.to_rfc3339(),
                decision_valid_until.to_rfc3339(),
                outcome_matures_at.to_rfc3339(),
                serde_json::to_string(forecast).unwrap_or_else(|_| "{}".to_owned()),
                serde_json::to_string(proposals).unwrap_or_else(|_| "[]".to_owned()),
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(inserted == 1)
    }

    fn prediction_for_contract(
        &self,
        snapshot: &MarketSnapshot,
        forecast: &ForecastEnvelope,
    ) -> Result<Option<(ForecastEnvelope, Vec<DecisionProposal>)>, rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        let row = connection.query_row(
            "SELECT forecast_json, proposal_json
             FROM predictions
             WHERE model_id=?1 AND symbol=?2 AND timeframe=?3
               AND origin_bar_timestamp=?4 AND feature_version=?5
               AND barrier_spec_id=?6 AND is_duplicate=0
             LIMIT 1",
            params![
                forecast.model_id,
                snapshot.symbol,
                snapshot.timeframe,
                forecast.origin_bar_timestamp.to_rfc3339(),
                forecast.feature_version,
                forecast.barrier_spec_id,
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        );
        match row {
            Ok((forecast_json, proposal_json)) => {
                let saved_forecast = serde_json::from_str(&forecast_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                let saved_proposals = serde_json::from_str(&proposal_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                Ok(Some((saved_forecast, saved_proposals)))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn save_proposal_instances(
        &self,
        proposals: &[DecisionProposal],
    ) -> Result<Vec<DecisionProposal>, rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let transaction = connection.transaction()?;
        let mut persisted = Vec::with_capacity(proposals.len());
        for proposal in proposals {
            let prediction_id = proposal.prediction_id.to_string();
            let prediction_exists: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM predictions WHERE prediction_id=?1)",
                [&prediction_id],
                |row| row.get(0),
            )?;
            if !prediction_exists {
                persisted.push(proposal.clone());
                continue;
            }
            let tick_size = transaction
                .query_row(
                    "SELECT s.tick_size
                     FROM predictions p
                     LEFT JOIN symbol_specs s ON s.symbol=p.symbol
                     WHERE p.prediction_id=?1",
                    [&prediction_id],
                    |row| row.get::<_, Option<f64>>(0),
                )
                .ok()
                .flatten()
                .filter(|value| value.is_finite() && *value > 0.0)
                .unwrap_or(0.01);
            let price_tick =
                |value: Option<f64>| value.map(|price| (price / tick_size).round() as i64);
            let profile = format!("{:?}", proposal.profile).to_uppercase();
            let action = proposal.action.as_str();
            let fingerprint = serde_json::json!({
                "action": action,
                "reference_entry_tick": price_tick(proposal.reference_entry_price),
                "target_tick": price_tick(proposal.target_price),
                "stop_tick": price_tick(proposal.invalidation_price),
                "reward_risk_band": (proposal.reward_risk_ratio / 0.05).round() as i64,
                "cost_model_id": proposal.cost_model_id,
                "entry_spread_ticks": (proposal.entry_spread / tick_size).round() as i64,
                "expected_exit_spread_ticks":
                    (proposal.expected_exit_spread / tick_size).round() as i64,
                "reason_codes": proposal.reason_codes,
                "model_health_status": proposal.model_health_status,
                "pnl_calculation_source": proposal.pnl_calculation_source,
            })
            .to_string();
            let latest = transaction.query_row(
                "SELECT proposal_fingerprint, proposal_json
                 FROM decision_proposal_instances
                 WHERE prediction_id=?1 AND profile=?2
                 ORDER BY evaluated_at DESC, rowid DESC LIMIT 1",
                params![prediction_id, profile],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            );
            if let Ok((latest_fingerprint, proposal_json)) = latest
                && latest_fingerprint == fingerprint
                && let Ok(saved) = serde_json::from_str::<DecisionProposal>(&proposal_json)
            {
                persisted.push(saved);
                continue;
            }
            let directional = matches!(
                proposal.action,
                DecisionAction::Long | DecisionAction::Short
            );
            let has_policy_instance: bool = transaction.query_row(
                "SELECT EXISTS(
                   SELECT 1
                   FROM decision_proposal_evidence dpe
                   JOIN decision_proposal_instances dpi ON dpi.proposal_id=dpe.proposal_id
                   WHERE dpi.prediction_id=?1 AND dpi.profile=?2
                     AND dpe.evidence_source='FIRST_ACTIONABLE'
                 )",
                params![prediction_id, profile],
                |row| row.get(0),
            )?;
            let mut saved = proposal.clone();
            saved.proposal_id = Some(Uuid::new_v4());
            saved.evidence_eligible = directional && !has_policy_instance;
            saved.evidence_source = if saved.evidence_eligible {
                "FIRST_ACTIONABLE".to_owned()
            } else {
                "DIAGNOSTIC".to_owned()
            };
            let evaluated_at = saved.evaluated_at.unwrap_or_else(Utc::now);
            let quote_timestamp = saved.quote_timestamp.unwrap_or(evaluated_at);
            let proposal_json = serde_json::to_string(&saved).unwrap_or_else(|_| "{}".to_owned());
            transaction.execute(
                "INSERT INTO decision_proposal_instances
                 (proposal_id, prediction_id, profile, evaluated_at, quote_timestamp,
                  proposal_fingerprint, reference_entry_price, action, target_price,
                  stop_price, remaining_reward_account, remaining_risk_account,
                  account_currency, cost_model_id, entry_spread, expected_exit_spread,
                  reason_codes_json, model_health_status, proposal_json,
                  evidence_eligible, evidence_source, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                         ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
                params![
                    saved.proposal_id.map(|value| value.to_string()),
                    prediction_id,
                    profile,
                    evaluated_at.to_rfc3339(),
                    quote_timestamp.to_rfc3339(),
                    fingerprint,
                    saved.reference_entry_price,
                    action,
                    saved.target_price,
                    saved.invalidation_price,
                    saved.remaining_reward_account,
                    saved.remaining_risk_account,
                    saved.account_currency,
                    saved.cost_model_id,
                    saved.entry_spread,
                    saved.expected_exit_spread,
                    serde_json::to_string(&saved.reason_codes).unwrap_or_else(|_| "[]".to_owned()),
                    saved.model_health_status,
                    proposal_json,
                    saved.evidence_eligible,
                    saved.evidence_source,
                    Utc::now().to_rfc3339(),
                ],
            )?;
            if saved.evidence_eligible {
                transaction.execute(
                    "INSERT OR IGNORE INTO decision_proposal_evidence
                     (proposal_id, evidence_source, created_at)
                     VALUES (?1, 'FIRST_ACTIONABLE', ?2)",
                    params![
                        saved.proposal_id.map(|value| value.to_string()),
                        Utc::now().to_rfc3339(),
                    ],
                )?;
            }
            persisted.push(saved);
        }
        transaction.execute(
            "DELETE FROM decision_proposal_instances
             WHERE evidence_eligible=0
               AND created_at<?1
               AND NOT EXISTS (
                 SELECT 1 FROM human_feedback hf
                 WHERE hf.proposal_id=decision_proposal_instances.proposal_id
               )
               AND NOT EXISTS (
                 SELECT 1 FROM decision_proposal_outcomes dpo
                 WHERE dpo.proposal_id=decision_proposal_instances.proposal_id
               )",
            [(Utc::now() - chrono::Duration::days(7)).to_rfc3339()],
        )?;
        transaction.execute(
            "DELETE FROM decision_proposal_instances
             WHERE proposal_id IN (
               SELECT proposal_id
               FROM (
                 SELECT proposal_id,
                        ROW_NUMBER() OVER (
                          PARTITION BY prediction_id, profile
                          ORDER BY evaluated_at DESC, rowid DESC
                        ) AS recency_rank
                 FROM decision_proposal_instances
                 WHERE evidence_eligible=0
               ) ranked
               WHERE recency_rank>128
             )
               AND NOT EXISTS (
                 SELECT 1 FROM human_feedback hf
                 WHERE hf.proposal_id=decision_proposal_instances.proposal_id
               )
               AND NOT EXISTS (
                 SELECT 1 FROM decision_proposal_outcomes dpo
                 WHERE dpo.proposal_id=decision_proposal_instances.proposal_id
               )",
            [],
        )?;
        transaction.commit()?;
        Ok(persisted)
    }

    fn save_feedback(&self, feedback: &HumanFeedback) -> Result<(), rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO human_feedback
             (proposal_id, prediction_id, profile, proposal_action, model_id, forecast_side,
              selected_reason, verdict, reason_codes_json, note, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                feedback.proposal_id.to_string(),
                feedback.prediction_id.to_string(),
                format!("{:?}", feedback.profile).to_uppercase(),
                feedback.proposal_action.as_str(),
                feedback.model_id,
                feedback.forecast_side.as_str(),
                feedback.selected_reason,
                format!("{:?}", feedback.verdict).to_uppercase(),
                serde_json::to_string(&feedback.reason_codes).unwrap_or_else(|_| "[]".to_owned()),
                feedback.note,
                feedback.created_at.to_rfc3339(),
            ],
        )?;
        let evidence_source = match feedback.verdict {
            xpde_domain::FeedbackVerdict::Accepted => Some("HUMAN_ACCEPTED"),
            xpde_domain::FeedbackVerdict::Rejected => Some("HUMAN_REJECTED"),
            xpde_domain::FeedbackVerdict::Uncertain => None,
        };
        if let Some(evidence_source) = evidence_source {
            transaction.execute(
                "UPDATE decision_proposal_instances
                 SET evidence_eligible=1,
                     evidence_source=CASE
                       WHEN evidence_source='FIRST_ACTIONABLE' THEN evidence_source
                       ELSE ?2
                     END
                 WHERE proposal_id=?1",
                params![feedback.proposal_id.to_string(), evidence_source],
            )?;
            transaction.execute(
                "INSERT OR IGNORE INTO decision_proposal_evidence
                 (proposal_id, evidence_source, created_at)
                 VALUES (?1, ?2, ?3)",
                params![
                    feedback.proposal_id.to_string(),
                    evidence_source,
                    feedback.created_at.to_rfc3339(),
                ],
            )?;
        }
        transaction.commit()
    }

    fn feedback_matches_proposal(&self, feedback: &HumanFeedback) -> Result<bool, rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        let result = connection.query_row(
            "SELECT dpi.proposal_json, p.model_id
             FROM decision_proposal_instances dpi
             JOIN predictions p ON p.prediction_id=dpi.prediction_id
             WHERE dpi.proposal_id=?1
               AND NOT EXISTS (
                 SELECT 1 FROM decision_proposal_instances newer
                 WHERE newer.prediction_id=dpi.prediction_id
                   AND newer.profile=dpi.profile
                   AND (
                     newer.evaluated_at>dpi.evaluated_at
                     OR (newer.evaluated_at=dpi.evaluated_at AND newer.rowid>dpi.rowid)
                   )
               )",
            [feedback.proposal_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        );
        let Ok((proposal_json, model_id)) = result else {
            return Ok(false);
        };
        let Ok(proposal) = serde_json::from_str::<DecisionProposal>(&proposal_json) else {
            return Ok(false);
        };
        let now = Utc::now();
        let entry_window_end = proposal.generated_at
            + chrono::Duration::seconds(proposal.maximum_decision_age_seconds as i64);
        Ok(proposal.prediction_id == feedback.prediction_id
            && proposal.profile == feedback.profile
            && proposal.action == feedback.proposal_action
            && model_id == feedback.model_id
            && now <= proposal.decision_valid_until
            && (!matches!(feedback.verdict, xpde_domain::FeedbackVerdict::Accepted)
                || now <= entry_window_end))
    }

    fn register_model(&self, model: &ModelRegistration) -> Result<(), rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        connection.execute(
            "INSERT INTO model_registry
             (model_id, model_type, status, feature_version, schema_version,
              eligibility_gate_version, training_mode, eligible_for_shadow,
              barrier_spec_id, executable_side_contract_id, artifact_path,
              metrics_json, created_at, promoted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, NULL)
             ON CONFLICT(model_id) DO UPDATE SET
               model_type=excluded.model_type,
               status=excluded.status,
               feature_version=excluded.feature_version,
               schema_version=excluded.schema_version,
               eligibility_gate_version=excluded.eligibility_gate_version,
               training_mode=excluded.training_mode,
               eligible_for_shadow=excluded.eligible_for_shadow,
               barrier_spec_id=excluded.barrier_spec_id,
               executable_side_contract_id=excluded.executable_side_contract_id,
               artifact_path=excluded.artifact_path,
               metrics_json=excluded.metrics_json",
            params![
                model.model_id,
                model.model_type,
                model.status,
                model.feature_version,
                model.schema_version,
                model.eligibility_gate_version,
                model.training_mode,
                model.eligible_for_shadow,
                model.barrier_spec_id,
                model.executable_side_contract_id,
                model.artifact_path,
                model.metrics.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;
        connection.execute(
            "INSERT INTO audit_events(event_type, entity_id, payload_json, created_at)
             VALUES ('MODEL_REGISTERED', ?1, ?2, ?3)",
            params![
                model.model_id,
                serde_json::to_string(model).unwrap_or_else(|_| "{}".to_owned()),
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn list_models(&self) -> Result<Vec<ModelRecord>, rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT model_id, model_type, status, feature_version, schema_version,
                    eligibility_gate_version, training_mode, eligible_for_shadow,
                    barrier_spec_id, executable_side_contract_id, artifact_path,
                    metrics_json, created_at, promoted_at
             FROM model_registry ORDER BY created_at DESC",
        )?;
        statement
            .query_map([], |row| {
                let metrics_json: String = row.get(11)?;
                Ok(ModelRecord {
                    model_id: row.get(0)?,
                    model_type: row.get(1)?,
                    status: row.get(2)?,
                    feature_version: row.get(3)?,
                    schema_version: row.get(4)?,
                    eligibility_gate_version: row.get(5)?,
                    training_mode: row.get(6)?,
                    eligible_for_shadow: row.get(7)?,
                    barrier_spec_id: row.get(8)?,
                    executable_side_contract_id: row.get(9)?,
                    artifact_path: row.get(10)?,
                    metrics: serde_json::from_str(&metrics_json).unwrap_or(serde_json::Value::Null),
                    created_at: row.get(12)?,
                    promoted_at: row.get(13)?,
                })
            })?
            .collect()
    }

    fn settle_expired_predictions(&self) -> Result<usize, rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let now = Utc::now().to_rfc3339();
        let pending = {
            let mut statement = connection.prepare(
                "SELECT p.prediction_id, p.symbol, p.timeframe,
                        p.origin_bar_timestamp, p.origin_close, p.origin_bid, p.origin_ask,
                        p.forecast_json, p.proposal_json
                 FROM predictions p
                 WHERE p.origin_bar_timestamp IS NOT NULL
                   AND p.origin_close IS NOT NULL
                   AND p.origin_bid IS NOT NULL
                   AND p.origin_ask IS NOT NULL
                   AND p.barrier_spec_id=?1
                   AND p.settlement_status='PENDING'
                   AND p.is_duplicate=0
                   AND EXISTS (
                     SELECT 1 FROM market_bars b
                     WHERE b.symbol=p.symbol
                       AND b.timeframe=p.timeframe
                       AND b.timestamp>p.origin_bar_timestamp
                   )
                   AND (
                     EXISTS (
                       SELECT 1
                       FROM (
                         SELECT 1 AS horizon_bars
                         UNION ALL SELECT 3
                         UNION ALL SELECT 6
                         UNION ALL SELECT 12
                       ) required
                       WHERE NOT EXISTS (
                         SELECT 1 FROM prediction_horizon_outcomes o
                         WHERE o.prediction_id=p.prediction_id
                           AND o.horizon_bars=required.horizon_bars
                       )
                     )
                     OR EXISTS (
                       SELECT 1 FROM prediction_horizon_outcomes o
                       WHERE o.prediction_id=p.prediction_id
                         AND o.horizon_bars=3
                         AND (
                           o.barrier_long_outcome IS NULL
                           OR o.barrier_short_outcome IS NULL
                         )
                     )
                   )
                 ORDER BY p.origin_bar_timestamp",
            )?;
            statement
                .query_map([BARRIER_SPEC_ID], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, f64>(4)?,
                        row.get::<_, Option<f64>>(5)?,
                        row.get::<_, Option<f64>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let transaction = connection.transaction()?;
        let mut settled = 0;
        for (
            prediction_id,
            symbol,
            timeframe,
            origin_bar_timestamp,
            origin_close,
            origin_bid,
            origin_ask,
            forecast_json,
            _proposal_json,
        ) in pending
        {
            let (Some(origin_bid), Some(origin_ask)) = (origin_bid, origin_ask) else {
                continue;
            };
            let outcome_bars = {
                let mut statement = transaction.prepare(
                    "SELECT timestamp, high, low, close,
                            bid_high, bid_low, bid_close,
                            ask_high, ask_low, ask_close
                     FROM market_bars
                     WHERE symbol=?1 AND timeframe=?2 AND timestamp>?3
                     ORDER BY timestamp LIMIT 12",
                )?;
                statement
                    .query_map(params![symbol, timeframe, origin_bar_timestamp], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, f64>(1)?,
                            row.get::<_, f64>(2)?,
                            row.get::<_, f64>(3)?,
                            row.get::<_, Option<f64>>(4)?,
                            row.get::<_, Option<f64>>(5)?,
                            row.get::<_, Option<f64>>(6)?,
                            row.get::<_, Option<f64>>(7)?,
                            row.get::<_, Option<f64>>(8)?,
                            row.get::<_, Option<f64>>(9)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            if outcome_bars.is_empty() || origin_close <= 0.0 {
                continue;
            }
            let Ok(forecast) = serde_json::from_str::<ForecastEnvelope>(&forecast_json) else {
                continue;
            };
            if forecast.origin_bar_timestamp.to_rfc3339() != origin_bar_timestamp
                || (forecast.origin_close - origin_close).abs() > 1e-8
            {
                continue;
            }
            for target in &forecast.points {
                let horizon = target.horizon_bars as usize;
                if horizon == 0 || outcome_bars.len() < horizon {
                    continue;
                }
                let already_settled: bool = transaction.query_row(
                    "SELECT EXISTS(
                       SELECT 1 FROM prediction_horizon_outcomes
                       WHERE prediction_id=?1 AND horizon_bars=?2
                     )",
                    params![prediction_id, target.horizon_bars],
                    |row| row.get(0),
                )?;

                let horizon_bars = &outcome_bars[..horizon];
                if horizon_bars.iter().any(|bar| {
                    bar.4.is_none()
                        || bar.5.is_none()
                        || bar.6.is_none()
                        || bar.7.is_none()
                        || bar.8.is_none()
                        || bar.9.is_none()
                }) {
                    continue;
                }
                let (barrier_long, barrier_short) = if target.horizon_bars == BARRIER_HORIZON_BARS {
                    let Ok(origin_time) = DateTime::parse_from_rfc3339(&origin_bar_timestamp)
                    else {
                        continue;
                    };
                    let first_bucket =
                        origin_time.with_timezone(&Utc) + chrono::Duration::minutes(5);
                    let end_exclusive = origin_time.with_timezone(&Utc)
                        + chrono::Duration::minutes((BARRIER_HORIZON_BARS as i64 + 1) * 5);
                    let tick_path = Self::load_tick_path(
                        &transaction,
                        &symbol,
                        &timeframe,
                        first_bucket,
                        end_exclusive,
                    )?;
                    let long = tick_path.as_deref().and_then(|ticks| {
                        tick_sequence_barrier_outcome(
                            DecisionAction::Long,
                            forecast.target_price_long,
                            forecast.stop_price_long,
                            ticks,
                            first_bucket.timestamp_millis() - 1,
                            end_exclusive.timestamp_millis(),
                        )
                        .map(|result| result.outcome)
                    });
                    let short = tick_path.as_deref().and_then(|ticks| {
                        tick_sequence_barrier_outcome(
                            DecisionAction::Short,
                            forecast.target_price_short,
                            forecast.stop_price_short,
                            ticks,
                            first_bucket.timestamp_millis() - 1,
                            end_exclusive.timestamp_millis(),
                        )
                        .map(|result| result.outcome)
                    });
                    (long, short)
                } else {
                    (None, None)
                };
                if already_settled {
                    if target.horizon_bars == BARRIER_HORIZON_BARS {
                        transaction.execute(
                            "UPDATE prediction_horizon_outcomes
                             SET barrier_long_outcome=COALESCE(barrier_long_outcome, ?3),
                                 barrier_short_outcome=COALESCE(barrier_short_outcome, ?4)
                             WHERE prediction_id=?1 AND horizon_bars=?2",
                            params![
                                prediction_id,
                                target.horizon_bars,
                                barrier_long.map(BarrierOutcome::as_str),
                                barrier_short.map(BarrierOutcome::as_str),
                            ],
                        )?;
                    }
                    continue;
                }

                let final_price = horizon_bars
                    .last()
                    .and_then(|bar| bar.6)
                    .unwrap_or(origin_close);
                let actual_return = (final_price / origin_close).ln();
                let actual_high = horizon_bars
                    .iter()
                    .filter_map(|bar| bar.4)
                    .fold(f64::NEG_INFINITY, f64::max);
                let actual_low = horizon_bars
                    .iter()
                    .filter_map(|bar| bar.5)
                    .fold(f64::INFINITY, f64::min);
                let interval_hit =
                    i64::from(actual_return >= target.q10 && actual_return <= target.q90);
                let direction_hit = i64::from((target.q50 >= 0.0) == (actual_return >= 0.0));
                let (expected_mfe, expected_mae, actual_mfe, actual_mae) = if target.q50 >= 0.0 {
                    (
                        forecast.expected_mfe_long,
                        forecast.expected_mae_long,
                        (actual_high - origin_ask).max(0.0),
                        (origin_ask - actual_low).max(0.0),
                    )
                } else {
                    let actual_ask_high = horizon_bars
                        .iter()
                        .filter_map(|bar| bar.7)
                        .fold(f64::NEG_INFINITY, f64::max);
                    let actual_ask_low = horizon_bars
                        .iter()
                        .filter_map(|bar| bar.8)
                        .fold(f64::INFINITY, f64::min);
                    (
                        forecast.expected_mfe_short,
                        forecast.expected_mae_short,
                        (origin_bid - actual_ask_low).max(0.0),
                        (actual_ask_high - origin_bid).max(0.0),
                    )
                };
                let barrier = if target.q50 >= 0.0 {
                    barrier_long
                } else {
                    barrier_short
                };
                let metrics = serde_json::json!({
                    "median_error": (actual_return - target.q50).abs(),
                    "interval_miss": interval_hit == 0,
                    "direction_error": direction_hit == 0,
                    "mfe_error_usd": if target.horizon_bars == BARRIER_HORIZON_BARS {
                        Some(expected_mfe - actual_mfe)
                    } else {
                        None
                    },
                    "mae_error_usd": if target.horizon_bars == BARRIER_HORIZON_BARS {
                        Some(expected_mae - actual_mae)
                    } else {
                        None
                    },
                });
                transaction.execute(
                    "INSERT INTO prediction_horizon_outcomes
                     (prediction_id, horizon_bars, origin_bar_timestamp,
                      outcome_bar_timestamp, actual_return, actual_high, actual_low,
                      interval_hit, direction_hit, barrier_outcome,
                      barrier_long_outcome, barrier_short_outcome,
                      error_metrics_json, settled_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        prediction_id,
                        target.horizon_bars,
                        origin_bar_timestamp,
                        horizon_bars.last().map(|bar| &bar.0),
                        actual_return,
                        actual_high,
                        actual_low,
                        interval_hit,
                        direction_hit,
                        barrier.map(BarrierOutcome::as_str),
                        barrier_long.map(BarrierOutcome::as_str),
                        barrier_short.map(BarrierOutcome::as_str),
                        metrics.to_string(),
                        now,
                    ],
                )?;
                settled += 1;
            }
            let completed_horizons: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM prediction_horizon_outcomes
                 WHERE prediction_id=?1 AND horizon_bars IN (1,3,6,12)",
                [&prediction_id],
                |row| row.get(0),
            )?;
            if completed_horizons == 4 {
                transaction.execute(
                    "UPDATE predictions SET settlement_status='SETTLED'
                     WHERE prediction_id=?1",
                    [&prediction_id],
                )?;
            }
        }
        transaction.commit()?;
        Ok(settled)
    }

    fn settle_proposal_instances(&self) -> Result<usize, rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let pending = {
            let mut statement = connection.prepare(
                "SELECT dpi.proposal_id, dpi.prediction_id, dpi.profile,
                        dpi.quote_timestamp, dpi.action, dpi.target_price, dpi.stop_price,
                        p.symbol, p.timeframe, dpi.proposal_json
                 FROM decision_proposal_instances dpi
                 JOIN predictions p ON p.prediction_id=dpi.prediction_id
                 WHERE EXISTS (
                     SELECT 1 FROM decision_proposal_evidence dpe
                     WHERE dpe.proposal_id=dpi.proposal_id
                   )
                   AND dpi.action IN ('LONG','SHORT')
                   AND dpi.target_price IS NOT NULL
                   AND dpi.stop_price IS NOT NULL
                   AND p.barrier_spec_id=?1
                   AND NOT EXISTS (
                     SELECT 1 FROM decision_proposal_outcomes dpo
                     WHERE dpo.proposal_id=dpi.proposal_id
                   )
                 ORDER BY dpi.evaluated_at",
            )?;
            statement
                .query_map([BARRIER_SPEC_ID], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, f64>(5)?,
                        row.get::<_, f64>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let transaction = connection.transaction()?;
        let mut settled = 0;
        for (
            proposal_id,
            prediction_id,
            profile,
            quote_timestamp,
            action_text,
            target,
            stop,
            symbol,
            timeframe,
            proposal_json,
        ) in pending
        {
            let Ok(quote_time) = DateTime::parse_from_rfc3339(&quote_timestamp) else {
                continue;
            };
            let Ok(proposal) = serde_json::from_str::<DecisionProposal>(&proposal_json) else {
                continue;
            };
            if Utc::now() < proposal.outcome_matures_at {
                continue;
            }
            let quote_time = quote_time.with_timezone(&Utc);
            let first_bucket =
                DateTime::<Utc>::from_timestamp(quote_time.timestamp().div_euclid(300) * 300, 0)
                    .expect("valid M5 proposal bucket");
            let Some(ticks) = Self::load_tick_path(
                &transaction,
                &symbol,
                &timeframe,
                first_bucket,
                proposal.outcome_matures_at,
            )?
            else {
                continue;
            };
            let action = if action_text == "LONG" {
                DecisionAction::Long
            } else {
                DecisionAction::Short
            };
            let Some(outcome) = tick_sequence_barrier_outcome(
                action,
                target,
                stop,
                &ticks,
                quote_time.timestamp_millis(),
                proposal.outcome_matures_at.timestamp_millis(),
            ) else {
                continue;
            };
            transaction.execute(
                "INSERT OR IGNORE INTO decision_proposal_outcomes
                 (proposal_id, prediction_id, profile, horizon_bars, action,
                  target_price, stop_price, barrier_outcome, first_touch_time_msc,
                  first_touch_price, settlement_source, settled_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                         'TICK_SEQUENCE', ?11)",
                params![
                    proposal_id,
                    prediction_id,
                    profile,
                    BARRIER_HORIZON_BARS,
                    action_text,
                    target,
                    stop,
                    outcome.outcome.as_str(),
                    outcome.first_touch_time_msc,
                    outcome.first_touch_price,
                    Utc::now().to_rfc3339(),
                ],
            )?;
            settled += 1;
        }
        transaction.commit()?;
        Ok(settled)
    }

    fn model_health(
        &self,
        model_id: &str,
        policy: &ModelHealthPolicy,
    ) -> Result<ModelHealth, rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let rows = {
            let mut statement = connection.prepare(
                "SELECT o.interval_hit, o.actual_return, p.direction_probability_up,
                        o.barrier_long_outcome, p.barrier_probability_long,
                        o.barrier_short_outcome, p.barrier_probability_short,
                        o.error_metrics_json, o.settled_at
                 FROM prediction_horizon_outcomes o
                 JOIN predictions p ON p.prediction_id=o.prediction_id
                 WHERE o.horizon_bars=3
                   AND p.model_id=?1
                   AND p.barrier_spec_id=?2
                   AND p.is_duplicate=0
                 ORDER BY o.origin_bar_timestamp DESC
                 LIMIT 200",
            )?;
            statement
                .query_map(params![model_id, BARRIER_SPEC_ID], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<f64>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        if rows.len() < policy.minimum_settled_predictions {
            let mut health = ModelHealth::warming_up(policy.minimum_settled_predictions);
            health.sample_size = rows.len();
            return Ok(health);
        }

        let interval_coverage =
            rows.iter().map(|row| row.0 as f64).sum::<f64>() / rows.len() as f64;
        let mut direction_probabilities = Vec::new();
        let mut direction_outcomes = Vec::new();
        let mut barrier_probabilities = [Vec::new(), Vec::new()];
        let mut barrier_outcomes = [Vec::new(), Vec::new()];
        let mut mae_covered = Vec::new();
        for row in &rows {
            if let Some(probability) = row.2 {
                direction_probabilities.push(probability);
                direction_outcomes.push(if row.1 >= 0.0 { 1.0 } else { 0.0 });
            }
            for (side, (outcome, probability)) in
                [(&row.3, row.4), (&row.5, row.6)].into_iter().enumerate()
            {
                if let (Some(outcome), Some(probability)) = (outcome.as_deref(), probability)
                    && matches!(outcome, "TP_FIRST" | "SL_FIRST" | "NO_HIT_BEFORE_EXPIRY")
                {
                    barrier_probabilities[side].push(probability);
                    barrier_outcomes[side].push(if outcome == "TP_FIRST" { 1.0 } else { 0.0 });
                }
            }
            if let Ok(metrics) = serde_json::from_str::<serde_json::Value>(&row.7)
                && let Some(error) = metrics["mae_error_usd"].as_f64()
            {
                mae_covered.push(if error >= 0.0 { 1.0 } else { 0.0 });
            }
        }
        let direction_brier = brier_score(&direction_probabilities, &direction_outcomes);
        let direction_baseline_brier = constant_baseline_brier(&direction_outcomes);
        let side_barrier_metrics = (0..2)
            .filter_map(|side| {
                let score = brier_score(&barrier_probabilities[side], &barrier_outcomes[side])?;
                let baseline = constant_baseline_brier(&barrier_outcomes[side])?;
                let ratio = if baseline > f64::EPSILON {
                    score / baseline
                } else if score <= f64::EPSILON {
                    1.0
                } else {
                    f64::INFINITY
                };
                Some((score, baseline, ratio))
            })
            .collect::<Vec<_>>();
        let worst_barrier = side_barrier_metrics
            .iter()
            .copied()
            .max_by(|left, right| left.2.total_cmp(&right.2));
        let barrier_brier = worst_barrier.map(|metric| metric.0);
        let barrier_baseline_brier = worst_barrier.map(|metric| metric.1);
        let barrier_ratio = worst_barrier.map(|metric| metric.2);
        let barrier_ece = (0..2)
            .filter_map(|side| {
                expected_calibration_error(&barrier_probabilities[side], &barrier_outcomes[side])
            })
            .max_by(f64::total_cmp);
        let mae_q90_coverage = (!mae_covered.is_empty())
            .then(|| mae_covered.iter().sum::<f64>() / mae_covered.len() as f64);

        let direction_ratio = match (direction_brier, direction_baseline_brier) {
            (Some(score), Some(baseline)) if baseline > f64::EPSILON => Some(score / baseline),
            (Some(score), Some(_)) if score <= f64::EPSILON => Some(1.0),
            (Some(_), Some(_)) => Some(f64::INFINITY),
            _ => None,
        };
        let checks = vec![
            ModelHealthCheck {
                code: "LIVE_INTERVAL_COVERAGE_OUTSIDE_GATE".to_owned(),
                observed: Some(interval_coverage),
                threshold: format!(
                    "{:.3}..={:.3}",
                    policy.minimum_interval_coverage, policy.maximum_interval_coverage
                ),
                passed: interval_coverage >= policy.minimum_interval_coverage
                    && interval_coverage <= policy.maximum_interval_coverage,
            },
            ModelHealthCheck {
                code: "LIVE_DIRECTION_BRIER_OUTSIDE_GATE".to_owned(),
                observed: direction_brier,
                threshold: format!(
                    "<={:.3} and ratio-to-baseline <={:.3}",
                    policy.maximum_direction_brier,
                    policy.maximum_direction_brier_ratio_to_baseline
                ),
                passed: direction_brier
                    .is_some_and(|score| score <= policy.maximum_direction_brier)
                    && direction_ratio.is_some_and(|ratio| {
                        ratio <= policy.maximum_direction_brier_ratio_to_baseline
                    }),
            },
            ModelHealthCheck {
                code: "LIVE_BARRIER_BRIER_OUTSIDE_GATE".to_owned(),
                observed: barrier_ratio,
                threshold: format!(
                    "ratio-to-baseline <={:.3}",
                    policy.maximum_barrier_brier_ratio_to_baseline
                ),
                passed: barrier_ratio
                    .is_some_and(|ratio| ratio <= policy.maximum_barrier_brier_ratio_to_baseline),
            },
            ModelHealthCheck {
                code: "LIVE_BARRIER_ECE_OUTSIDE_GATE".to_owned(),
                observed: barrier_ece,
                threshold: format!("<={:.3}", policy.maximum_barrier_ece),
                passed: barrier_ece.is_some_and(|ece| ece <= policy.maximum_barrier_ece),
            },
            ModelHealthCheck {
                code: "LIVE_MAE_COVERAGE_OUTSIDE_GATE".to_owned(),
                observed: mae_q90_coverage,
                threshold: format!(
                    "{:.3}..={:.3}",
                    policy.minimum_mae_q90_coverage, policy.maximum_mae_q90_coverage
                ),
                passed: mae_q90_coverage.is_some_and(|coverage| {
                    coverage >= policy.minimum_mae_q90_coverage
                        && coverage <= policy.maximum_mae_q90_coverage
                }),
            },
        ];
        let mut reasons = checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| check.code.clone())
            .collect::<Vec<_>>();
        let severe = reasons.len() >= 2
            || direction_brier.is_some_and(|score| score > policy.maximum_direction_brier * 1.25)
            || barrier_ratio
                .is_some_and(|ratio| ratio > policy.maximum_barrier_brier_ratio_to_baseline * 1.25);
        let raw_status = if reasons.is_empty() {
            ModelHealthStatus::Healthy
        } else if severe {
            ModelHealthStatus::Suspended
        } else {
            ModelHealthStatus::Degraded
        };
        let evidence_fingerprint = format!(
            "{}:{}",
            rows.len(),
            rows.iter()
                .map(|row| row.8.as_str())
                .max()
                .unwrap_or("unknown")
        );
        let previous = connection.query_row(
            "SELECT status, consecutive_failures, consecutive_severe_failures,
                    consecutive_successes, evidence_fingerprint
             FROM model_health_state WHERE model_id=?1",
            [model_id],
            |row| {
                Ok((
                    ModelHealthStatus::from_str(&row.get::<_, String>(0)?),
                    row.get::<_, usize>(1)?,
                    row.get::<_, usize>(2)?,
                    row.get::<_, usize>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        );
        let (
            previous_status,
            mut failures,
            mut severe_failures,
            mut successes,
            previous_fingerprint,
        ) = previous.unwrap_or((ModelHealthStatus::WarmingUp, 0, 0, 0, String::new()));
        let status = if previous_fingerprint == evidence_fingerprint {
            previous_status
        } else {
            match raw_status {
                ModelHealthStatus::Healthy => {
                    failures = 0;
                    severe_failures = 0;
                    successes += 1;
                    if successes >= policy.recover_after_healthy_windows {
                        ModelHealthStatus::Healthy
                    } else {
                        previous_status
                    }
                }
                ModelHealthStatus::Degraded => {
                    failures += 1;
                    severe_failures = 0;
                    successes = 0;
                    if failures >= policy.degrade_after_failed_windows {
                        ModelHealthStatus::Degraded
                    } else {
                        previous_status
                    }
                }
                ModelHealthStatus::Suspended => {
                    failures += 1;
                    severe_failures += 1;
                    successes = 0;
                    if severe_failures >= policy.suspend_after_severe_windows {
                        ModelHealthStatus::Suspended
                    } else if failures >= policy.degrade_after_failed_windows {
                        ModelHealthStatus::Degraded
                    } else {
                        previous_status
                    }
                }
                ModelHealthStatus::WarmingUp => previous_status,
            }
        };
        if previous_fingerprint != evidence_fingerprint {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO model_health_state
                 (model_id, status, consecutive_failures, consecutive_severe_failures,
                  consecutive_successes, evidence_fingerprint, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(model_id) DO UPDATE SET
                   status=excluded.status,
                   consecutive_failures=excluded.consecutive_failures,
                   consecutive_severe_failures=excluded.consecutive_severe_failures,
                   consecutive_successes=excluded.consecutive_successes,
                   evidence_fingerprint=excluded.evidence_fingerprint,
                   updated_at=excluded.updated_at",
                params![
                    model_id,
                    status.as_str(),
                    failures,
                    severe_failures,
                    successes,
                    evidence_fingerprint,
                    Utc::now().to_rfc3339(),
                ],
            )?;
            if status != previous_status {
                transaction.execute(
                    "INSERT INTO model_health_events
                     (model_id, previous_status, current_status, evidence_fingerprint,
                      reason_codes_json, metrics_json, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        model_id,
                        previous_status.as_str(),
                        status.as_str(),
                        evidence_fingerprint,
                        serde_json::to_string(&reasons).unwrap_or_else(|_| "[]".to_owned()),
                        serde_json::json!({
                            "interval_coverage": interval_coverage,
                            "direction_brier": direction_brier,
                            "direction_baseline_brier": direction_baseline_brier,
                            "barrier_brier": barrier_brier,
                            "barrier_baseline_brier": barrier_baseline_brier,
                            "barrier_ece": barrier_ece,
                            "mae_q90_coverage": mae_q90_coverage,
                        })
                        .to_string(),
                        Utc::now().to_rfc3339(),
                    ],
                )?;
            }
            transaction.commit()?;
        }
        if status == ModelHealthStatus::WarmingUp {
            reasons.push("MODEL_LIVE_HEALTH_HYSTERESIS_WARMING".to_owned());
        }
        Ok(ModelHealth {
            status,
            sample_size: rows.len(),
            minimum_sample_size: policy.minimum_settled_predictions,
            interval_coverage: Some(interval_coverage),
            direction_brier,
            direction_baseline_brier,
            barrier_brier,
            barrier_baseline_brier,
            barrier_ece,
            mae_q90_coverage,
            reason_codes: reasons,
            checks,
            consecutive_failures: failures,
            consecutive_successes: successes,
        })
    }

    fn evaluation_summary(
        &self,
        current_model_id: &str,
        session_started_at: DateTime<Utc>,
    ) -> Result<serde_json::Value, rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        let overall = connection.query_row(
            "SELECT COUNT(*),
                    COALESCE(AVG(o.interval_hit), 0.0),
                    COALESCE(AVG(o.direction_hit), 0.0),
                    COUNT(CASE WHEN o.barrier_outcome IN ('TP_FIRST','SL_FIRST') THEN 1 END),
                    AVG(CASE
                          WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                          WHEN o.barrier_outcome='SL_FIRST' THEN 0.0
                        END),
                    COUNT(CASE WHEN o.barrier_outcome='NO_HIT_BEFORE_EXPIRY' THEN 1 END),
                    COUNT(CASE WHEN o.barrier_outcome='AMBIGUOUS_SAME_BAR' THEN 1 END),
                    COUNT(CASE WHEN o.barrier_outcome IN
                      ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 1 END),
                    AVG(CASE
                          WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                          WHEN o.barrier_outcome IN
                            ('SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 0.0
                        END),
                    AVG(
                      (p.direction_probability_up
                       - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                      *
                      (p.direction_probability_up
                       - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                    )
             FROM prediction_horizon_outcomes o
             JOIN predictions p ON p.prediction_id=o.prediction_id
             WHERE p.model_id != 'baseline-demo-v1'
               AND p.barrier_spec_id=?1
               AND p.is_duplicate=0
               AND o.horizon_bars=3",
            [BARRIER_SPEC_ID],
            |row| {
                Ok(serde_json::json!({
                    "settled_predictions": row.get::<_, i64>(0)?,
                    "interval_coverage": row.get::<_, f64>(1)?,
                    "direction_accuracy": row.get::<_, f64>(2)?,
                    "tp_before_sl_samples": row.get::<_, i64>(3)?,
                    "tp_before_sl_rate": row.get::<_, Option<f64>>(4)?,
                    "no_hit_samples": row.get::<_, i64>(5)?,
                    "ambiguous_samples": row.get::<_, i64>(6)?,
                    "tp_first_within_horizon_samples": row.get::<_, i64>(7)?,
                    "tp_first_within_horizon_rate": row.get::<_, Option<f64>>(8)?,
                    "tp_vs_sl_conditional_rate": row.get::<_, Option<f64>>(4)?,
                    "direction_brier": row.get::<_, Option<f64>>(9)?,
                }))
            },
        )?;
        let by_model = {
            let mut statement = connection.prepare(
                "SELECT p.model_id, COUNT(*), AVG(o.interval_hit), AVG(o.direction_hit),
                        COUNT(CASE WHEN o.barrier_outcome IN ('TP_FIRST','SL_FIRST') THEN 1 END),
                        AVG(CASE
                              WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                              WHEN o.barrier_outcome='SL_FIRST' THEN 0.0
                            END),
                        COUNT(CASE WHEN o.barrier_outcome='NO_HIT_BEFORE_EXPIRY' THEN 1 END),
                        COUNT(CASE WHEN o.barrier_outcome='AMBIGUOUS_SAME_BAR' THEN 1 END),
                        COUNT(CASE WHEN o.barrier_outcome IN
                          ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 1 END),
                        AVG(CASE
                              WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                              WHEN o.barrier_outcome IN
                                ('SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 0.0
                            END),
                        AVG(
                          (p.direction_probability_up
                           - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                          *
                          (p.direction_probability_up
                           - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                        )
                 FROM prediction_horizon_outcomes o
                 JOIN predictions p ON p.prediction_id=o.prediction_id
                 WHERE o.horizon_bars=3 AND p.barrier_spec_id=?1
                   AND p.is_duplicate=0
                 GROUP BY p.model_id ORDER BY MAX(o.settled_at) DESC",
            )?;
            statement
                .query_map([BARRIER_SPEC_ID], |row| {
                    Ok(serde_json::json!({
                        "model_id": row.get::<_, String>(0)?,
                        "settled_predictions": row.get::<_, i64>(1)?,
                        "interval_coverage": row.get::<_, f64>(2)?,
                        "direction_accuracy": row.get::<_, f64>(3)?,
                        "tp_before_sl_samples": row.get::<_, i64>(4)?,
                        "tp_before_sl_rate": row.get::<_, Option<f64>>(5)?,
                        "no_hit_samples": row.get::<_, i64>(6)?,
                        "ambiguous_samples": row.get::<_, i64>(7)?,
                        "tp_first_within_horizon_samples": row.get::<_, i64>(8)?,
                        "tp_first_within_horizon_rate": row.get::<_, Option<f64>>(9)?,
                        "tp_vs_sl_conditional_rate": row.get::<_, Option<f64>>(5)?,
                        "direction_brier": row.get::<_, Option<f64>>(10)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let current_model = connection.query_row(
            "WITH recent AS (
               SELECT o.*, p.direction_probability_up
               FROM prediction_horizon_outcomes o
               JOIN predictions p ON p.prediction_id=o.prediction_id
               WHERE o.horizon_bars=3
                 AND p.model_id=?1
                 AND p.barrier_spec_id=?2
                 AND p.is_duplicate=0
               ORDER BY o.origin_bar_timestamp DESC
               LIMIT 200
             )
             SELECT COUNT(*),
                    COALESCE(AVG(o.interval_hit), 0.0),
                    COALESCE(AVG(o.direction_hit), 0.0),
                    COUNT(CASE WHEN o.barrier_outcome IN ('TP_FIRST','SL_FIRST') THEN 1 END),
                    AVG(CASE
                          WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                          WHEN o.barrier_outcome='SL_FIRST' THEN 0.0
                        END),
                    COUNT(CASE WHEN o.barrier_outcome='NO_HIT_BEFORE_EXPIRY' THEN 1 END),
                    COUNT(CASE WHEN o.barrier_outcome='AMBIGUOUS_SAME_BAR' THEN 1 END),
                    COUNT(CASE WHEN o.barrier_outcome IN
                      ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 1 END),
                    AVG(CASE
                          WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                          WHEN o.barrier_outcome IN
                            ('SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 0.0
                        END),
                    AVG(
                      (o.direction_probability_up
                       - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                      *
                      (o.direction_probability_up
                       - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                    )
             FROM recent o",
            params![current_model_id, BARRIER_SPEC_ID],
            |row| {
                Ok(serde_json::json!({
                    "model_id": current_model_id,
                    "settled_predictions": row.get::<_, i64>(0)?,
                    "interval_coverage": row.get::<_, f64>(1)?,
                    "direction_accuracy": row.get::<_, f64>(2)?,
                    "tp_before_sl_samples": row.get::<_, i64>(3)?,
                    "tp_before_sl_rate": row.get::<_, Option<f64>>(4)?,
                    "no_hit_samples": row.get::<_, i64>(5)?,
                    "ambiguous_samples": row.get::<_, i64>(6)?,
                    "tp_first_within_horizon_samples": row.get::<_, i64>(7)?,
                    "tp_first_within_horizon_rate": row.get::<_, Option<f64>>(8)?,
                    "tp_vs_sl_conditional_rate": row.get::<_, Option<f64>>(4)?,
                    "direction_brier": row.get::<_, Option<f64>>(9)?,
                }))
            },
        )?;
        let session_started_at_text = session_started_at.to_rfc3339();
        let current_session = connection.query_row(
            "SELECT COUNT(*),
                    COALESCE(AVG(o.interval_hit), 0.0),
                    COALESCE(AVG(o.direction_hit), 0.0),
                    COUNT(CASE WHEN o.barrier_outcome IN ('TP_FIRST','SL_FIRST') THEN 1 END),
                    AVG(CASE
                          WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                          WHEN o.barrier_outcome='SL_FIRST' THEN 0.0
                        END),
                    COUNT(CASE WHEN o.barrier_outcome='NO_HIT_BEFORE_EXPIRY' THEN 1 END),
                    COUNT(CASE WHEN o.barrier_outcome='AMBIGUOUS_SAME_BAR' THEN 1 END),
                    COUNT(CASE WHEN o.barrier_outcome IN
                      ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 1 END),
                    AVG(CASE
                          WHEN o.barrier_outcome='TP_FIRST' THEN 1.0
                          WHEN o.barrier_outcome IN
                            ('SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 0.0
                        END),
                    AVG(
                      (p.direction_probability_up
                       - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                      *
                      (p.direction_probability_up
                       - CASE WHEN o.actual_return>=0.0 THEN 1.0 ELSE 0.0 END)
                    )
             FROM prediction_horizon_outcomes o
             JOIN predictions p ON p.prediction_id=o.prediction_id
             WHERE o.horizon_bars=3
               AND p.model_id=?1
               AND p.generated_at>=?2
               AND p.barrier_spec_id=?3
               AND p.is_duplicate=0",
            params![current_model_id, session_started_at_text, BARRIER_SPEC_ID],
            |row| {
                Ok(serde_json::json!({
                    "model_id": current_model_id,
                    "started_at": session_started_at,
                    "settled_predictions": row.get::<_, i64>(0)?,
                    "interval_coverage": row.get::<_, f64>(1)?,
                    "direction_accuracy": row.get::<_, f64>(2)?,
                    "tp_before_sl_samples": row.get::<_, i64>(3)?,
                    "tp_before_sl_rate": row.get::<_, Option<f64>>(4)?,
                    "no_hit_samples": row.get::<_, i64>(5)?,
                    "ambiguous_samples": row.get::<_, i64>(6)?,
                    "tp_first_within_horizon_samples": row.get::<_, i64>(7)?,
                    "tp_first_within_horizon_rate": row.get::<_, Option<f64>>(8)?,
                    "tp_vs_sl_conditional_rate": row.get::<_, Option<f64>>(4)?,
                    "direction_brier": row.get::<_, Option<f64>>(9)?,
                }))
            },
        )?;
        let forecast_barrier_by_side = {
            let mut statement = connection.prepare(
                "WITH recent AS (
                   SELECT o.barrier_long_outcome, o.barrier_short_outcome,
                          p.barrier_probability_long AS probability_long,
                          p.barrier_probability_short AS probability_short
                   FROM prediction_horizon_outcomes o
                   JOIN predictions p ON p.prediction_id=o.prediction_id
                   WHERE o.horizon_bars=3
                     AND p.model_id=?1
                     AND p.barrier_spec_id=?2
                     AND p.is_duplicate=0
                   ORDER BY o.origin_bar_timestamp DESC
                   LIMIT 200
                 ),
                 sides AS (
                   SELECT 'LONG' AS side, barrier_long_outcome AS outcome,
                          probability_long AS probability FROM recent
                   UNION ALL
                   SELECT 'SHORT' AS side, barrier_short_outcome AS outcome,
                          probability_short AS probability FROM recent
                 )
                 SELECT side,
                        COUNT(outcome),
                        COUNT(CASE WHEN outcome IN ('TP_FIRST','SL_FIRST') THEN 1 END),
                        AVG(CASE
                              WHEN outcome='TP_FIRST' THEN 1.0
                              WHEN outcome='SL_FIRST' THEN 0.0
                            END),
                        COUNT(CASE WHEN outcome='NO_HIT_BEFORE_EXPIRY' THEN 1 END),
                        COUNT(CASE WHEN outcome='AMBIGUOUS_SAME_BAR' THEN 1 END),
                        COUNT(CASE WHEN outcome IN
                          ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 1 END),
                        AVG(CASE
                              WHEN outcome='TP_FIRST' THEN 1.0
                              WHEN outcome IN
                                ('SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 0.0
                            END),
                        AVG(CASE
                              WHEN outcome IN
                                ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY')
                              THEN
                                (probability - CASE WHEN outcome='TP_FIRST' THEN 1.0 ELSE 0.0 END)
                                *
                                (probability - CASE WHEN outcome='TP_FIRST' THEN 1.0 ELSE 0.0 END)
                            END)
                 FROM sides GROUP BY side ORDER BY side",
            )?;
            statement
                .query_map(params![current_model_id, BARRIER_SPEC_ID], |row| {
                    Ok(serde_json::json!({
                        "side": row.get::<_, String>(0)?,
                        "settled_predictions": row.get::<_, i64>(1)?,
                        "tp_before_sl_samples": row.get::<_, i64>(2)?,
                        "tp_before_sl_rate": row.get::<_, Option<f64>>(3)?,
                        "no_hit_samples": row.get::<_, i64>(4)?,
                        "ambiguous_samples": row.get::<_, i64>(5)?,
                        "tp_first_within_horizon_samples": row.get::<_, i64>(6)?,
                        "tp_first_within_horizon_rate": row.get::<_, Option<f64>>(7)?,
                        "tp_vs_sl_conditional_rate": row.get::<_, Option<f64>>(3)?,
                        "brier_score": row.get::<_, Option<f64>>(8)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let barrier_calibration_bins = {
            let mut statement = connection.prepare(
                "WITH recent AS (
                   SELECT o.barrier_long_outcome, o.barrier_short_outcome,
                          p.barrier_probability_long AS probability_long,
                          p.barrier_probability_short AS probability_short
                   FROM prediction_horizon_outcomes o
                   JOIN predictions p ON p.prediction_id=o.prediction_id
                   WHERE o.horizon_bars=3
                     AND p.model_id=?1
                     AND p.barrier_spec_id=?2
                     AND p.is_duplicate=0
                   ORDER BY o.origin_bar_timestamp DESC
                   LIMIT 200
                 ),
                 sides AS (
                   SELECT 'LONG' AS side, barrier_long_outcome AS outcome,
                          probability_long AS probability FROM recent
                   UNION ALL
                   SELECT 'SHORT' AS side, barrier_short_outcome AS outcome,
                          probability_short AS probability FROM recent
                 ),
                 valid AS (
                   SELECT side, outcome, probability,
                          MIN(9, CAST(probability * 10.0 AS INTEGER)) AS bin_index,
                          CASE WHEN outcome='TP_FIRST' THEN 1.0 ELSE 0.0 END AS observed
                   FROM sides
                   WHERE outcome IN
                     ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY')
                     AND probability IS NOT NULL
                 )
                 SELECT side, bin_index, COUNT(*), AVG(probability), AVG(observed),
                        AVG((probability-observed)*(probability-observed))
                 FROM valid
                 GROUP BY side, bin_index
                 ORDER BY side, bin_index",
            )?;
            statement
                .query_map(params![current_model_id, BARRIER_SPEC_ID], |row| {
                    Ok(serde_json::json!({
                        "side": row.get::<_, String>(0)?,
                        "bin_index": row.get::<_, i64>(1)?,
                        "sample_size": row.get::<_, i64>(2)?,
                        "mean_probability": row.get::<_, f64>(3)?,
                        "observed_tp_rate": row.get::<_, f64>(4)?,
                        "brier_score": row.get::<_, f64>(5)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let calibration_sample_size = barrier_calibration_bins
            .iter()
            .filter_map(|bin| bin["sample_size"].as_i64())
            .sum::<i64>();
        let barrier_expected_calibration_error = if calibration_sample_size > 0 {
            Some(
                barrier_calibration_bins
                    .iter()
                    .map(|bin| {
                        let samples = bin["sample_size"].as_i64().unwrap_or(0) as f64;
                        let predicted = bin["mean_probability"].as_f64().unwrap_or(0.0);
                        let observed = bin["observed_tp_rate"].as_f64().unwrap_or(0.0);
                        samples * (predicted - observed).abs()
                    })
                    .sum::<f64>()
                    / calibration_sample_size as f64,
            )
        } else {
            None
        };
        let forecast_quality_by_horizon = {
            let mut statement = connection.prepare(
                "WITH recent_predictions AS (
                   SELECT *
                   FROM predictions
                   WHERE model_id=?1 AND is_duplicate=0
                   ORDER BY origin_bar_timestamp DESC
                   LIMIT 200
                 ),
                 forecast_points AS (
                   SELECT p.prediction_id,
                          CAST(json_extract(point.value, '$.horizon_bars') AS INTEGER) AS horizon_bars,
                          CAST(json_extract(point.value, '$.q10') AS REAL) AS q10,
                          CAST(json_extract(point.value, '$.q50') AS REAL) AS q50,
                          CAST(json_extract(point.value, '$.q90') AS REAL) AS q90
                   FROM recent_predictions p, json_each(p.forecast_json, '$.points') point
                 ),
                 scored AS (
                   SELECT o.horizon_bars, o.actual_return, o.interval_hit,
                          o.error_metrics_json, fp.q10, fp.q50, fp.q90
                   FROM prediction_horizon_outcomes o
                   JOIN forecast_points fp
                     ON fp.prediction_id=o.prediction_id
                    AND fp.horizon_bars=o.horizon_bars
                 )
                 SELECT horizon_bars, COUNT(*), AVG(interval_hit), AVG(q90-q10),
                        AVG(CASE WHEN actual_return>=q10
                                 THEN 0.10*(actual_return-q10)
                                 ELSE -0.90*(actual_return-q10) END),
                        AVG(CASE WHEN actual_return>=q50
                                 THEN 0.50*(actual_return-q50)
                                 ELSE -0.50*(actual_return-q50) END),
                        AVG(CASE WHEN actual_return>=q90
                                 THEN 0.90*(actual_return-q90)
                                 ELSE -0.10*(actual_return-q90) END),
                        AVG(CASE
                              WHEN horizon_bars!=3
                                OR json_extract(error_metrics_json, '$.mae_error_usd') IS NULL
                                THEN NULL
                              WHEN CAST(json_extract(error_metrics_json, '$.mae_error_usd') AS REAL)>=0
                                THEN 1.0
                              ELSE 0.0
                            END)
                 FROM scored
                 GROUP BY horizon_bars
                 ORDER BY horizon_bars",
            )?;
            statement
                .query_map([current_model_id], |row| {
                    Ok(serde_json::json!({
                        "horizon_bars": row.get::<_, i64>(0)?,
                        "sample_size": row.get::<_, i64>(1)?,
                        "interval_coverage": row.get::<_, Option<f64>>(2)?,
                        "mean_interval_width_log_return": row.get::<_, Option<f64>>(3)?,
                        "pinball_q10": row.get::<_, Option<f64>>(4)?,
                        "pinball_q50": row.get::<_, Option<f64>>(5)?,
                        "pinball_q90": row.get::<_, Option<f64>>(6)?,
                        "mae_q90_coverage": row.get::<_, Option<f64>>(7)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let proposal_outcomes_by_profile = {
            let mut statement = connection.prepare(
                "WITH recent_predictions AS (
                   SELECT p.prediction_id
                   FROM predictions p
                   WHERE p.model_id=?1 AND p.barrier_spec_id=?2
                     AND p.is_duplicate=0
                   ORDER BY p.origin_bar_timestamp DESC
                   LIMIT 200
                 )
                 SELECT po.profile, dpe.evidence_source,
                        COUNT(*),
                        COUNT(CASE WHEN po.barrier_outcome IN ('TP_FIRST','SL_FIRST') THEN 1 END),
                        AVG(CASE
                              WHEN po.barrier_outcome='TP_FIRST' THEN 1.0
                              WHEN po.barrier_outcome='SL_FIRST' THEN 0.0
                            END),
                        COUNT(CASE WHEN po.barrier_outcome='NO_HIT_BEFORE_EXPIRY' THEN 1 END),
                        COUNT(CASE WHEN po.barrier_outcome='AMBIGUOUS_SAME_BAR' THEN 1 END),
                        COUNT(CASE WHEN po.barrier_outcome IN
                          ('TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 1 END),
                        AVG(CASE
                              WHEN po.barrier_outcome='TP_FIRST' THEN 1.0
                              WHEN po.barrier_outcome IN
                                ('SL_FIRST','NO_HIT_BEFORE_EXPIRY') THEN 0.0
                            END)
                 FROM decision_proposal_outcomes po
                 JOIN recent_predictions rp ON rp.prediction_id=po.prediction_id
                 JOIN decision_proposal_evidence dpe ON dpe.proposal_id=po.proposal_id
                 WHERE po.horizon_bars=3
                 GROUP BY po.profile, dpe.evidence_source
                 ORDER BY po.profile, dpe.evidence_source",
            )?;
            statement
                .query_map(params![current_model_id, BARRIER_SPEC_ID], |row| {
                    Ok(serde_json::json!({
                        "profile": row.get::<_, String>(0)?,
                        "evidence_source": row.get::<_, String>(1)?,
                        "settled_proposal_instances": row.get::<_, i64>(2)?,
                        "tp_before_sl_samples": row.get::<_, i64>(3)?,
                        "tp_before_sl_rate": row.get::<_, Option<f64>>(4)?,
                        "no_hit_samples": row.get::<_, i64>(5)?,
                        "ambiguous_samples": row.get::<_, i64>(6)?,
                        "tp_first_within_horizon_samples": row.get::<_, i64>(7)?,
                        "tp_first_within_horizon_rate": row.get::<_, Option<f64>>(8)?,
                        "tp_vs_sl_conditional_rate": row.get::<_, Option<f64>>(4)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(serde_json::json!({
            "scope": "LIVE_SHADOW_H3",
            "overall": overall,
            "current_model": current_model,
            "current_session": current_session,
            "forecast_barrier_by_side": forecast_barrier_by_side,
            "barrier_calibration_bins": barrier_calibration_bins,
            "barrier_expected_calibration_error": barrier_expected_calibration_error,
            "forecast_quality_by_horizon": forecast_quality_by_horizon,
            "proposal_outcomes_by_profile": proposal_outcomes_by_profile,
            "by_model": by_model,
            "target_coverage": 0.80,
            "current_model_window": 200,
            "updated_at": Utc::now(),
        }))
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "xpde_server=info,tower_http=info".into()),
        )
        .init();

    let database_path =
        PathBuf::from(env::var("XPDE_DB_PATH").unwrap_or_else(|_| "data/xpde.sqlite".to_owned()));
    let bind = env::var("XPDE_BIND").unwrap_or_else(|_| "127.0.0.1:8787".to_owned());
    let store = Arc::new(Store::open(&database_path).expect("failed to initialize SQLite"));
    let policies = load_policy_set().expect("failed to load broker policy config");
    let runtime = demo_state();
    let state = AppState {
        store,
        runtime: Arc::new(RwLock::new(runtime)),
        started_at: Utc::now(),
        policies,
    };
    tokio::spawn(settlement_loop(state.store.clone()));

    let cors = CorsLayer::new()
        .allow_origin([
            "http://127.0.0.1:3000"
                .parse::<HeaderValue>()
                .expect("valid origin"),
            "http://localhost:3000"
                .parse::<HeaderValue>()
                .expect("valid origin"),
            "http://127.0.0.1:5173"
                .parse::<HeaderValue>()
                .expect("valid origin"),
        ])
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/state", get(get_state))
        .route("/api/v1/market/cursor", get(get_market_cursor))
        .route("/api/v1/market/snapshot", post(post_snapshot))
        .route("/api/v1/market/backfill", post(post_backfill))
        .route("/api/v1/forecast", post(post_forecast))
        .route("/api/v1/feedback", post(post_feedback))
        .route("/api/v1/models", get(get_models))
        .route("/api/v1/models/register", post(post_model_registration))
        .route("/api/v1/evaluation/summary", get(get_evaluation_summary))
        .route("/ws", get(websocket))
        .layer(cors)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(false)),
        )
        .with_state(state);

    let listener = TcpListener::bind(&bind)
        .await
        .unwrap_or_else(|error| panic!("failed to bind {bind}: {error}"));
    info!(%bind, database = %database_path.display(), "XPDE shadow server ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server failure");
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "xpde-server",
        "mode": "shadow",
        "auto_trading": false,
        "timestamp": Utc::now(),
    }))
}

async fn get_state(State(state): State<AppState>) -> Json<RuntimeState> {
    Json(public_runtime_state(&state).await)
}

async fn get_market_cursor(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let last_completed_bar_timestamp = state
        .store
        .latest_bar_timestamp()
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(serde_json::json!({
        "symbol": xpde_domain::SUPPORTED_SYMBOL,
        "timeframe": xpde_domain::SUPPORTED_TIMEFRAME,
        "last_completed_bar_timestamp": last_completed_bar_timestamp,
    })))
}

async fn post_backfill(
    State(state): State<AppState>,
    Json(request): Json<BackfillRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    if request.symbol != xpde_domain::SUPPORTED_SYMBOL {
        return Err(ApiError::bad_request("unsupported backfill symbol"));
    }
    if request.timeframe != xpde_domain::SUPPORTED_TIMEFRAME {
        return Err(ApiError::bad_request("unsupported backfill timeframe"));
    }
    if request.bars.is_empty() || request.bars.len() > 5_000 {
        return Err(ApiError::bad_request(
            "backfill chunk must contain between 1 and 5000 bars",
        ));
    }
    if request.bars.iter().any(|bar| {
        !bar.open.is_finite()
            || !bar.high.is_finite()
            || !bar.low.is_finite()
            || !bar.close.is_finite()
            || bar.high < bar.open.max(bar.close)
            || bar.low > bar.open.min(bar.close)
            || bar.timestamp > Utc::now() + chrono::Duration::minutes(5)
            || bar.timestamp.timestamp() % 300 != 0
            || !bar.has_executable_sides()
    }) {
        return Err(ApiError::bad_request("backfill contains an invalid bar"));
    }
    let received = request.bars.len();
    let inserted = state
        .store
        .save_backfill(&request)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "received": received, "inserted": inserted })),
    ))
}

async fn post_snapshot(
    State(state): State<AppState>,
    Json(snapshot): Json<MarketSnapshot>,
) -> Result<StatusCode, ApiError> {
    snapshot
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    state
        .store
        .save_snapshot(&snapshot)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let current_model_id = state.runtime.read().await.forecast.model_id.clone();
    let model_health = state
        .store
        .model_health(&current_model_id, &state.policies.model_health)
        .map_err(|error| ApiError::internal(error.to_string()))?;

    let mut runtime = state.runtime.write().await;
    runtime.snapshot = snapshot;
    runtime.connection_status = match runtime.snapshot.data_quality.market_status {
        MarketStatus::Open if runtime.snapshot.data_quality.is_valid(10_000) => "MT5_CONNECTED",
        MarketStatus::MarketClosed => "MARKET_CLOSED",
        MarketStatus::BridgeDisconnected => "BRIDGE_DISCONNECTED",
        _ => "MT5_STALE",
    };
    runtime.mode = "LIVE_SHADOW";
    runtime.updated_at = Utc::now();
    runtime.safety.feed_is_demo = false;
    runtime.model_health = model_health;
    runtime.forecast_status = forecast_status(
        &runtime.snapshot,
        &runtime.forecast,
        runtime.safety.feed_is_demo,
        runtime.updated_at,
    );
    let scalper = decide(
        &runtime.snapshot,
        &runtime.forecast,
        &state.policies.scalper,
    );
    let sniper = decide(&runtime.snapshot, &runtime.forecast, &state.policies.sniper);
    runtime.proposals = vec![scalper, sniper];
    let health = runtime.model_health.clone();
    apply_model_health_gate(
        &mut runtime.proposals,
        &health,
        state.policies.model_health.warming_up_forces_wait,
    );
    runtime.proposals = state
        .store
        .save_proposal_instances(&runtime.proposals)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(StatusCode::ACCEPTED)
}

async fn post_forecast(
    State(state): State<AppState>,
    Json(forecast): Json<ForecastEnvelope>,
) -> Result<StatusCode, ApiError> {
    forecast
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let model_health = state
        .store
        .model_health(&forecast.model_id, &state.policies.model_health)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let mut runtime = state.runtime.write().await;
    if !runtime.snapshot.data_quality.is_valid(10_000) {
        return Err(ApiError::bad_request(
            "forecast rejected because the executable market snapshot is not actionable",
        ));
    }
    let latest_completed = runtime
        .snapshot
        .bars
        .iter()
        .max_by_key(|bar| bar.timestamp)
        .ok_or_else(|| ApiError::bad_request("snapshot has no completed origin bar"))?;
    if forecast.origin_bar_timestamp != latest_completed.timestamp
        || (forecast.origin_close - latest_completed.close).abs() > 1e-8
    {
        return Err(ApiError::bad_request(
            "forecast origin does not match the latest completed M5 candle",
        ));
    }
    if runtime.snapshot.symbol_spec.chart_mode != ChartMode::Bid
        || !latest_completed.has_executable_sides()
        || !latest_completed.has_valid_tick_path()
        || !runtime
            .snapshot
            .current_bar
            .as_ref()
            .is_some_and(|bar| bar.has_executable_sides() && bar.has_valid_tick_path())
    {
        return Err(ApiError::bad_request(
            "forecast rejected because exact Bid/Ask executable bars are unavailable",
        ));
    }
    let mut proposals = vec![
        decide(&runtime.snapshot, &forecast, &state.policies.scalper),
        decide(&runtime.snapshot, &forecast, &state.policies.sniper),
    ];
    apply_model_health_gate(
        &mut proposals,
        &model_health,
        state.policies.model_health.warming_up_forces_wait,
    );
    let inserted = state
        .store
        .save_prediction(&runtime.snapshot, &forecast, &proposals)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    if !inserted
        && let Some((saved_forecast, _)) = state
            .store
            .prediction_for_contract(&runtime.snapshot, &forecast)
            .map_err(|error| ApiError::internal(error.to_string()))?
    {
        runtime.forecast = saved_forecast;
        runtime.model_health = model_health;
        runtime.proposals = vec![
            decide(
                &runtime.snapshot,
                &runtime.forecast,
                &state.policies.scalper,
            ),
            decide(&runtime.snapshot, &runtime.forecast, &state.policies.sniper),
        ];
        let health = runtime.model_health.clone();
        apply_model_health_gate(
            &mut runtime.proposals,
            &health,
            state.policies.model_health.warming_up_forces_wait,
        );
        runtime.proposals = state
            .store
            .save_proposal_instances(&runtime.proposals)
            .map_err(|error| ApiError::internal(error.to_string()))?;
        runtime.updated_at = Utc::now();
        runtime.forecast_status = forecast_status(
            &runtime.snapshot,
            &runtime.forecast,
            runtime.safety.feed_is_demo,
            runtime.updated_at,
        );
        return Ok(StatusCode::OK);
    }
    runtime.forecast = forecast;
    runtime.proposals = state
        .store
        .save_proposal_instances(&proposals)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    runtime.model_health = model_health;
    runtime.updated_at = Utc::now();
    runtime.forecast_status = forecast_status(
        &runtime.snapshot,
        &runtime.forecast,
        runtime.safety.feed_is_demo,
        runtime.updated_at,
    );
    Ok(StatusCode::ACCEPTED)
}

async fn post_feedback(
    State(state): State<AppState>,
    Json(feedback): Json<HumanFeedback>,
) -> Result<StatusCode, ApiError> {
    if feedback.note.as_ref().is_some_and(|note| note.len() > 500) {
        return Err(ApiError::bad_request(
            "feedback note exceeds 500 characters",
        ));
    }
    if (Utc::now() - feedback.created_at)
        .num_seconds()
        .unsigned_abs()
        > 30
    {
        return Err(ApiError::bad_request(
            "feedback timestamp is outside the accepted clock window",
        ));
    }
    if feedback.model_id.trim().is_empty()
        || !matches!(
            feedback.forecast_side,
            DecisionAction::Long | DecisionAction::Short
        )
    {
        return Err(ApiError::bad_request("feedback context is invalid"));
    }
    {
        let runtime = state.runtime.read().await;
        let latest_matches = runtime.proposals.iter().any(|proposal| {
            proposal.profile == feedback.profile
                && proposal.proposal_id == Some(feedback.proposal_id)
                && proposal.action == feedback.proposal_action
        });
        if !latest_matches {
            return Err(ApiError::bad_request(
                "feedback rejected because the proposal is not the latest published instance",
            ));
        }
        if matches!(feedback.verdict, xpde_domain::FeedbackVerdict::Accepted)
            && matches!(
                feedback.proposal_action,
                DecisionAction::Long | DecisionAction::Short
            )
            && matches!(
                runtime.model_health.status,
                ModelHealthStatus::Degraded | ModelHealthStatus::Suspended
            )
        {
            return Err(ApiError::bad_request(
                "feedback rejected because live model health no longer permits entry",
            ));
        }
    }
    if !state
        .store
        .feedback_matches_proposal(&feedback)
        .map_err(|error| ApiError::internal(error.to_string()))?
    {
        return Err(ApiError::bad_request(
            "feedback proposal_id does not match a still-valid published instance",
        ));
    }
    state
        .store
        .save_feedback(&feedback)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(StatusCode::CREATED)
}

async fn post_model_registration(
    State(state): State<AppState>,
    Json(model): Json<ModelRegistration>,
) -> Result<StatusCode, ApiError> {
    if model.model_id.trim().is_empty()
        || model.feature_version.trim().is_empty()
        || model.artifact_path.trim().is_empty()
    {
        return Err(ApiError::bad_request(
            "model registration fields are required",
        ));
    }
    if !matches!(
        model.status.as_str(),
        "candidate" | "challenger" | "champion" | "retired"
    ) {
        return Err(ApiError::bad_request("invalid model status"));
    }
    if model.status == "champion" {
        return Err(ApiError::bad_request(
            "champion promotion requires a separate manual approval workflow",
        ));
    }
    let gates_are_booleans = model
        .eligibility_gates
        .values()
        .all(serde_json::Value::is_boolean);
    let all_gates_passed = model
        .eligibility_gates
        .values()
        .all(|value| value.as_bool() == Some(true));
    if model.status != "retired"
        && (model.schema_version != 3
            || model.eligibility_gate_version < 3
            || model.training_mode != "candidate"
            || model.barrier_spec_id != BARRIER_SPEC_ID
            || model.executable_side_contract_id != EXECUTABLE_SIDE_CONTRACT_ID
            || model.eligibility_gates.is_empty()
            || !gates_are_booleans)
    {
        return Err(ApiError::bad_request(
            "active model registration requires a schema-v3 candidate with explicit eligibility gates",
        ));
    }
    if (model.eligible_for_shadow && !all_gates_passed)
        || (model.status == "challenger" && !model.eligible_for_shadow)
    {
        return Err(ApiError::bad_request(
            "eligible or challenger registration requires all shadow eligibility gates to pass",
        ));
    }
    state
        .store
        .register_model(&model)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(StatusCode::CREATED)
}

async fn get_models(State(state): State<AppState>) -> Result<Json<Vec<ModelRecord>>, ApiError> {
    state
        .store
        .list_models()
        .map(Json)
        .map_err(|error| ApiError::internal(error.to_string()))
}

async fn get_evaluation_summary(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let current_model_id = state.runtime.read().await.forecast.model_id.clone();
    let model_health = state.runtime.read().await.model_health.clone();
    let mut summary = state
        .store
        .evaluation_summary(&current_model_id, state.started_at)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    if let Some(object) = summary.as_object_mut() {
        object.insert(
            "model_health".to_owned(),
            serde_json::to_value(model_health)
                .map_err(|error| ApiError::internal(error.to_string()))?,
        );
    }
    Ok(Json(summary))
}

async fn websocket(State(state): State<AppState>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| websocket_loop(socket, state))
}

async fn websocket_loop(mut socket: WebSocket, state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        let payload = match serde_json::to_string(&public_runtime_state(&state).await) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(%error, "failed to serialize websocket state");
                break;
            }
        };
        if socket.send(Message::Text(payload.into())).await.is_err() {
            break;
        }
    }
}

async fn public_runtime_state(state: &AppState) -> RuntimeState {
    let mut runtime = state.runtime.read().await.clone();
    for bar in &mut runtime.snapshot.bars {
        bar.executable_tick_path.clear();
    }
    if let Some(current_bar) = &mut runtime.snapshot.current_bar {
        current_bar.executable_tick_path.clear();
    }
    let now = Utc::now();
    runtime.forecast_status = forecast_status(
        &runtime.snapshot,
        &runtime.forecast,
        runtime.safety.feed_is_demo,
        now,
    );
    if !runtime.safety.feed_is_demo {
        let bridge_age_ms = Utc::now()
            .signed_duration_since(runtime.updated_at)
            .num_milliseconds()
            .max(0) as u64;
        runtime.snapshot.data_quality.transport_tick_age_ms = runtime
            .snapshot
            .data_quality
            .transport_tick_age_ms
            .max(bridge_age_ms);
        runtime.snapshot.data_quality.tick_age_ms = runtime
            .snapshot
            .data_quality
            .absolute_tick_age_ms
            .max(runtime.snapshot.data_quality.transport_tick_age_ms);
        if bridge_age_ms > state.policies.scalper.max_tick_age_ms {
            runtime.connection_status = "BRIDGE_DISCONNECTED";
            runtime.snapshot.data_quality.market_status = MarketStatus::BridgeDisconnected;
        } else if runtime.snapshot.data_quality.market_status == MarketStatus::MarketClosed {
            runtime.connection_status = "MARKET_CLOSED";
        } else if runtime.snapshot.data_quality.tick_age_ms > state.policies.scalper.max_tick_age_ms
        {
            runtime.connection_status = "MT5_STALE";
        }
    }
    runtime
}

async fn settlement_loop(store: Arc<Store>) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        match store.settle_expired_predictions() {
            Ok(settled) if settled > 0 => info!(settled, "prediction outcomes settled"),
            Ok(_) => {}
            Err(error) => warn!(%error, "prediction outcome settlement failed"),
        }
        match store.settle_proposal_instances() {
            Ok(settled) if settled > 0 => {
                info!(settled, "decision proposal instances settled")
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "decision proposal settlement failed"),
        }
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "failed to install Ctrl+C handler");
    }
}

fn demo_state() -> RuntimeState {
    let now = Utc::now();
    let bar_anchor = DateTime::<Utc>::from_timestamp(now.timestamp().div_euclid(300) * 300, 0)
        .expect("valid demo bar timestamp");
    let mut price: f64 = 3331.50;
    let mut bars = Vec::new();
    for index in (0..48).rev() {
        let wave = ((index as f64) / 3.5).sin() * 0.55;
        let drift = (48 - index) as f64 * 0.018;
        let open = price;
        let close = 3331.2 + drift + wave;
        let high = open.max(close) + 0.34 + (index % 3) as f64 * 0.04;
        let low = open.min(close) - 0.31 - (index % 2) as f64 * 0.05;
        bars.push(MarketBar {
            timestamp: bar_anchor - chrono::Duration::minutes(index * 5),
            open,
            high,
            low,
            close,
            tick_volume: 150.0 + (index % 9) as f64 * 11.0,
            bid_open: Some(open),
            bid_high: Some(high),
            bid_low: Some(low),
            bid_close: Some(close),
            ask_open: Some(open + 0.34),
            ask_high: Some(high + 0.34),
            ask_low: Some(low + 0.34),
            ask_close: Some(close + 0.34),
            executable_tick_count: 100,
            first_tick_msc: Some(
                (bar_anchor - chrono::Duration::minutes(index * 5)).timestamp() * 1000,
            ),
            last_tick_msc: Some(
                (bar_anchor - chrono::Duration::minutes(index * 5)).timestamp() * 1000 + 299_000,
            ),
            executable_tick_path: Vec::new(),
        });
        price = close;
    }

    let snapshot = MarketSnapshot {
        symbol: "GOLDm#".to_owned(),
        provider: "MetaTrader5 demo fixture".to_owned(),
        timestamp: now,
        timeframe: "M5".to_owned(),
        bid: 3332.74,
        ask: 3333.08,
        bars,
        current_bar: None,
        account: AccountSnapshot {
            login: 0,
            server: "Awaiting local terminal".to_owned(),
            balance: 1_000.0,
            equity: 1_000.0,
            free_margin: 1_000.0,
            leverage: 1_000,
            currency: "USD".to_owned(),
        },
        symbol_spec: SymbolSpec {
            description: "GOLD".to_owned(),
            contract_size: 1.0,
            volume_min: 0.1,
            volume_max: 100.0,
            volume_step: 0.1,
            tick_size: 0.01,
            tick_value: 0.01,
            stops_level_points: 0,
            digits: 2,
            margin_per_lot_buy: Some(3.34),
            margin_per_lot_sell: Some(3.34),
            chart_mode: ChartMode::Bid,
            quote_currency: "USD".to_owned(),
            pnl_currency: "USD".to_owned(),
            symbol_profit_currency: "USD".to_owned(),
            calculated_pnl_currency: "USD".to_owned(),
            profit_per_price_unit_per_lot_buy: Some(1.0),
            profit_per_price_unit_per_lot_sell: Some(1.0),
            pnl_calculation_source: "MT5_ORDER_CALC_PROFIT".to_owned(),
            conversion_rate: Some(1.0),
            conversion_timestamp: Some(now),
            trade_mode_enabled: true,
            trade_mode: TradeMode::Full,
        },
        data_quality: DataQuality {
            completeness: 1.0,
            tick_age_ms: 0,
            absolute_tick_age_ms: 0,
            transport_tick_age_ms: 0,
            market_status: MarketStatus::Open,
            missing_flags: Vec::new(),
            reason_codes: vec!["DEMO_DATA".to_owned()],
        },
    };

    let origin = snapshot
        .bars
        .iter()
        .max_by_key(|bar| bar.timestamp)
        .expect("demo snapshot has completed bars");
    let forecast = ForecastEnvelope {
        prediction_id: Uuid::new_v4(),
        model_id: "baseline-demo-v1".to_owned(),
        feature_version: "goldm-m5-v4".to_owned(),
        origin_bar_timestamp: origin.timestamp,
        origin_close: origin.close,
        origin_bar_index: origin.timestamp.timestamp().div_euclid(300),
        generated_at: now,
        direction_probability_up: 0.57,
        barrier_probability_long: 0.54,
        barrier_probability_short: 0.46,
        barrier_spec_id: BARRIER_SPEC_ID.to_owned(),
        barrier_horizon_bars: BARRIER_HORIZON_BARS,
        target_price_long: origin.close + 1.25,
        stop_price_long: origin.close - 1.0,
        target_price_short: origin.close - 1.25,
        stop_price_short: origin.close + 1.0,
        expected_mfe_long: 1.42,
        expected_mae_long: 0.91,
        expected_mfe_short: 1.31,
        expected_mae_short: 0.98,
        excursion_modelled: false,
        calibration: CalibrationStatus {
            target_coverage: 0.80,
            observed_coverage: 0.786,
            sample_size: 500,
        },
        drift_detected: false,
        points: vec![
            ForecastPoint {
                horizon_bars: 1,
                q10: -0.00022,
                q25: -0.00008,
                q50: 0.00004,
                q75: 0.00014,
                q90: 0.00025,
            },
            ForecastPoint {
                horizon_bars: 3,
                q10: -0.00048,
                q25: -0.00018,
                q50: 0.00010,
                q75: 0.00038,
                q90: 0.00070,
            },
            ForecastPoint {
                horizon_bars: 6,
                q10: -0.00077,
                q25: -0.00031,
                q50: 0.00017,
                q75: 0.00062,
                q90: 0.00108,
            },
            ForecastPoint {
                horizon_bars: 12,
                q10: -0.00125,
                q25: -0.00052,
                q50: 0.00026,
                q75: 0.00102,
                q90: 0.00177,
            },
        ],
    };
    let proposals = vec![
        decide(&snapshot, &forecast, &DecisionPolicy::scalper()),
        decide(&snapshot, &forecast, &DecisionPolicy::sniper()),
    ];

    RuntimeState {
        mode: "DEMO_SHADOW",
        connection_status: "WAITING_FOR_MT5",
        updated_at: now,
        forecast_status: ForecastStatus::Demo,
        snapshot,
        forecast,
        proposals,
        model_health: ModelHealth::warming_up(100),
        safety: SafetyStatus {
            auto_trading_enabled: false,
            human_confirmation_required: true,
            feed_is_demo: true,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_outcome_uses_exact_long_contract() {
        let bars = vec![(101.2, 99.5, 101.0)];

        assert_eq!(
            barrier_outcome(DecisionAction::Long, 101.0, 99.0, &bars),
            Some(BarrierOutcome::TpFirst)
        );
    }

    #[test]
    fn barrier_outcome_detects_short_stop_first() {
        let bars = vec![(101.2, 99.5, 100.8)];

        assert_eq!(
            barrier_outcome(DecisionAction::Short, 99.0, 101.0, &bars),
            Some(BarrierOutcome::SlFirst)
        );
    }

    #[test]
    fn barrier_outcome_keeps_same_bar_ambiguity_explicit() {
        let bars = vec![(101.2, 98.8, 100.2)];

        assert_eq!(
            barrier_outcome(DecisionAction::Long, 101.0, 99.0, &bars),
            Some(BarrierOutcome::AmbiguousSameBar)
        );
    }

    #[test]
    fn barrier_outcome_keeps_no_hit_before_expiry() {
        let bars = vec![(100.8, 99.4, 100.2)];

        assert_eq!(
            barrier_outcome(DecisionAction::Long, 101.0, 99.0, &bars),
            Some(BarrierOutcome::NoHitBeforeExpiry)
        );
    }

    #[test]
    fn tick_sequence_ignores_pre_quote_touch_inside_current_bar() {
        let result = tick_sequence_barrier_outcome(
            DecisionAction::Long,
            101.0,
            99.0,
            &[
                (1_000, 98.8, 99.0),
                (2_000, 100.0, 100.2),
                (3_000, 101.2, 101.4),
            ],
            1_500,
            4_000,
        )
        .expect("post-quote ticks");

        assert_eq!(result.outcome, BarrierOutcome::TpFirst);
        assert_eq!(result.first_touch_time_msc, Some(3_000));
        assert_eq!(result.first_touch_price, Some(101.2));
    }

    #[test]
    fn forecast_lifecycle_distinguishes_demo_waiting_mismatch_and_expiry() {
        let runtime = demo_state();
        assert_eq!(
            forecast_status(
                &runtime.snapshot,
                &runtime.forecast,
                true,
                runtime.updated_at
            ),
            ForecastStatus::Demo
        );
        assert_eq!(
            forecast_status(
                &runtime.snapshot,
                &runtime.forecast,
                false,
                runtime.updated_at
            ),
            ForecastStatus::WaitingForFirstForecast
        );

        let mut forecast = runtime.forecast.clone();
        forecast.model_id = "candidate-v1".to_owned();
        assert_eq!(
            forecast_status(&runtime.snapshot, &forecast, false, runtime.updated_at),
            ForecastStatus::Current
        );
        assert_eq!(
            forecast_status(
                &runtime.snapshot,
                &forecast,
                false,
                forecast.origin_bar_timestamp + chrono::Duration::minutes(11)
            ),
            ForecastStatus::Expired
        );
        forecast.origin_bar_timestamp -= chrono::Duration::minutes(5);
        assert_eq!(
            forecast_status(&runtime.snapshot, &forecast, false, runtime.updated_at),
            ForecastStatus::OriginMismatch
        );
    }

    #[test]
    fn model_health_gate_abstains_without_mutating_model() {
        let runtime = demo_state();
        let mut proposals = runtime.proposals;
        proposals[0].action = DecisionAction::Long;
        proposals[0].target_price = Some(3333.0);
        proposals[0].invalidation_price = Some(3328.0);
        let health = ModelHealth {
            status: ModelHealthStatus::Suspended,
            sample_size: 200,
            minimum_sample_size: 100,
            interval_coverage: Some(0.62),
            direction_brier: Some(0.31),
            direction_baseline_brier: Some(0.25),
            barrier_brier: Some(0.28),
            barrier_baseline_brier: Some(0.24),
            barrier_ece: Some(0.14),
            mae_q90_coverage: Some(0.76),
            reason_codes: vec!["LIVE_INTERVAL_COVERAGE_OUTSIDE_GATE".to_owned()],
            checks: Vec::new(),
            consecutive_failures: 2,
            consecutive_successes: 0,
        };

        apply_model_health_gate(&mut proposals, &health, true);

        assert_eq!(proposals[0].action, DecisionAction::Wait);
        assert_eq!(proposals[0].target_price, None);
        assert_eq!(proposals[0].invalidation_price, None);
        assert!(
            proposals[0]
                .reason_codes
                .contains(&"MODEL_LIVE_HEALTH_SUSPENDED".to_owned())
        );
    }

    #[test]
    fn calibration_helpers_match_known_probabilities() {
        let probabilities = [0.8, 0.2];
        let outcomes = [1.0, 0.0];
        assert!((brier_score(&probabilities, &outcomes).unwrap() - 0.04).abs() < 1e-12);
        assert!(
            (expected_calibration_error(&probabilities, &outcomes).unwrap() - 0.2).abs() < 1e-12
        );
    }

    #[test]
    fn sqlite_round_trip_persists_executable_origin_sides() {
        let connection = Connection::open_in_memory().expect("in-memory database");
        connection.execute_batch(MIGRATION).expect("migration");
        let store = Store {
            connection: Mutex::new(connection),
        };
        let runtime = demo_state();
        store
            .save_snapshot(&runtime.snapshot)
            .expect("snapshot persistence");
        assert!(
            store
                .save_prediction(&runtime.snapshot, &runtime.forecast, &runtime.proposals,)
                .expect("prediction persistence")
        );
        let origin = runtime
            .snapshot
            .bars
            .iter()
            .find(|bar| bar.timestamp == runtime.forecast.origin_bar_timestamp)
            .expect("origin bar");
        let connection = store.connection.lock().expect("database mutex");
        let persisted: (f64, f64) = connection
            .query_row(
                "SELECT origin_bid, origin_ask FROM predictions
                 WHERE prediction_id=?1",
                [runtime.forecast.prediction_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("persisted executable origin");

        assert_eq!(persisted.0, origin.bid_close.unwrap());
        assert_eq!(persisted.1, origin.ask_close.unwrap());
    }

    #[test]
    fn proposal_instances_are_idempotent_until_material_state_changes() {
        let connection = Connection::open_in_memory().expect("in-memory database");
        connection.execute_batch(MIGRATION).expect("migration");
        let store = Store {
            connection: Mutex::new(connection),
        };
        let runtime = demo_state();
        store
            .save_prediction(&runtime.snapshot, &runtime.forecast, &runtime.proposals)
            .expect("prediction persistence");
        let mut proposal = runtime.proposals[0].clone();
        proposal.action = DecisionAction::Long;
        proposal.target_price = Some(runtime.forecast.target_price_long);
        proposal.invalidation_price = Some(runtime.forecast.stop_price_long);
        proposal.reason_codes.clear();
        proposal.model_health_status = "HEALTHY".to_owned();
        let first = store
            .save_proposal_instances(&[proposal.clone()])
            .expect("first proposal instance");
        let repeated = store
            .save_proposal_instances(&[proposal.clone()])
            .expect("idempotent proposal instance");
        assert_eq!(first[0].proposal_id, repeated[0].proposal_id);
        assert!(first[0].evidence_eligible);
        proposal.reference_entry_price = proposal.reference_entry_price.map(|price| price + 0.001);
        let sub_tick = store
            .save_proposal_instances(&[proposal.clone()])
            .expect("sub-tick proposal noise");
        assert_eq!(first[0].proposal_id, sub_tick[0].proposal_id);
        proposal.reference_entry_price = proposal.reference_entry_price.map(|price| price + 0.01);
        let material_tick = store
            .save_proposal_instances(&[proposal.clone()])
            .expect("one-tick proposal change");
        assert_ne!(first[0].proposal_id, material_tick[0].proposal_id);
        assert!(!material_tick[0].evidence_eligible);
        let feedback = HumanFeedback {
            proposal_id: material_tick[0].proposal_id.expect("proposal id"),
            prediction_id: runtime.forecast.prediction_id,
            profile: material_tick[0].profile,
            proposal_action: material_tick[0].action,
            model_id: runtime.forecast.model_id.clone(),
            forecast_side: DecisionAction::Long,
            selected_reason: None,
            verdict: xpde_domain::FeedbackVerdict::Accepted,
            reason_codes: Vec::new(),
            note: None,
            created_at: Utc::now(),
        };
        assert!(
            store
                .feedback_matches_proposal(&feedback)
                .expect("feedback proposal match")
        );
        store
            .save_feedback(&feedback)
            .expect("feedback persistence");
        let evidence_sources = {
            let connection = store.connection.lock().expect("database mutex");
            let mut statement = connection
                .prepare(
                    "SELECT evidence_source FROM decision_proposal_evidence
                     ORDER BY evidence_source",
                )
                .expect("evidence query");
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .expect("evidence rows")
                .collect::<Result<Vec<_>, _>>()
                .expect("evidence values")
        };
        assert_eq!(
            evidence_sources,
            vec!["FIRST_ACTIONABLE".to_owned(), "HUMAN_ACCEPTED".to_owned()]
        );

        proposal.action = DecisionAction::Wait;
        proposal.target_price = None;
        proposal.invalidation_price = None;
        proposal.reason_codes = vec!["ENTRY_WINDOW_EXPIRED".to_owned()];
        let changed = store
            .save_proposal_instances(&[proposal])
            .expect("changed proposal instance");
        assert_ne!(first[0].proposal_id, changed[0].proposal_id);
        assert!(!changed[0].evidence_eligible);
    }

    #[tokio::test]
    async fn public_state_and_websocket_payload_path_are_database_read_only() {
        let connection = Connection::open_in_memory().expect("in-memory database");
        connection.execute_batch(MIGRATION).expect("migration");
        let store = Arc::new(Store {
            connection: Mutex::new(connection),
        });
        let mut runtime = demo_state();
        runtime.snapshot.bars[0].executable_tick_path = vec![(
            runtime.snapshot.bars[0]
                .first_tick_msc
                .expect("demo first tick"),
            runtime.snapshot.bars[0].bid_open.expect("demo bid"),
            runtime.snapshot.bars[0].ask_open.expect("demo ask"),
        )];
        let state = AppState {
            store: store.clone(),
            runtime: Arc::new(RwLock::new(runtime)),
            started_at: Utc::now(),
            policies: PolicySet {
                scalper: DecisionPolicy::scalper(),
                sniper: DecisionPolicy::sniper(),
                model_health: ModelHealthPolicy {
                    minimum_settled_predictions: 100,
                    minimum_interval_coverage: 0.70,
                    maximum_interval_coverage: 0.90,
                    maximum_direction_brier: 0.26,
                    maximum_direction_brier_ratio_to_baseline: 1.0,
                    maximum_barrier_brier_ratio_to_baseline: 1.0,
                    maximum_barrier_ece: 0.12,
                    minimum_mae_q90_coverage: 0.80,
                    maximum_mae_q90_coverage: 0.98,
                    degrade_after_failed_windows: 2,
                    suspend_after_severe_windows: 2,
                    recover_after_healthy_windows: 3,
                    warming_up_forces_wait: true,
                },
            },
        };
        let before = store
            .connection
            .lock()
            .expect("database mutex")
            .total_changes();

        let published = public_runtime_state(&state).await;

        let after = store
            .connection
            .lock()
            .expect("database mutex")
            .total_changes();
        assert_eq!(before, after);
        assert!(published.snapshot.bars[0].executable_tick_path.is_empty());
        assert_eq!(
            state.runtime.read().await.snapshot.bars[0]
                .executable_tick_path
                .len(),
            1
        );
    }

    #[test]
    fn partial_executable_bar_cannot_overwrite_more_complete_bar() {
        let connection = Connection::open_in_memory().expect("in-memory database");
        connection.execute_batch(MIGRATION).expect("migration");
        let store = Store {
            connection: Mutex::new(connection),
        };
        let runtime = demo_state();
        store
            .save_snapshot(&runtime.snapshot)
            .expect("full snapshot persistence");
        let latest = runtime
            .snapshot
            .bars
            .iter()
            .max_by_key(|bar| bar.timestamp)
            .expect("latest bar");
        let mut partial_snapshot = runtime.snapshot.clone();
        let partial = partial_snapshot
            .bars
            .iter_mut()
            .find(|bar| bar.timestamp == latest.timestamp)
            .expect("partial target");
        partial.bid_high = Some(latest.bid_high.unwrap() + 10.0);
        partial.ask_high = Some(latest.ask_high.unwrap() + 10.0);
        partial.executable_tick_count = 10;
        partial.last_tick_msc = partial.first_tick_msc.map(|value| value + 1_000);
        store
            .save_snapshot(&partial_snapshot)
            .expect("partial snapshot persistence");
        let connection = store.connection.lock().expect("database mutex");
        let persisted: (f64, i64) = connection
            .query_row(
                "SELECT bid_high, executable_tick_count FROM market_bars
                 WHERE symbol=?1 AND timeframe=?2 AND timestamp=?3",
                params![
                    runtime.snapshot.symbol,
                    runtime.snapshot.timeframe,
                    latest.timestamp.to_rfc3339(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("persisted bar");
        assert_eq!(persisted.0, latest.bid_high.unwrap());
        assert_eq!(persisted.1, latest.executable_tick_count as i64);
    }

    #[test]
    fn model_health_hysteresis_advances_only_on_new_evidence() {
        let connection = Connection::open_in_memory().expect("in-memory database");
        connection.execute_batch(MIGRATION).expect("migration");
        let store = Store {
            connection: Mutex::new(connection),
        };
        let policy = ModelHealthPolicy {
            minimum_settled_predictions: 3,
            minimum_interval_coverage: 0.50,
            maximum_interval_coverage: 1.0,
            maximum_direction_brier: 0.30,
            maximum_direction_brier_ratio_to_baseline: 1.0,
            maximum_barrier_brier_ratio_to_baseline: 1.0,
            maximum_barrier_ece: 0.20,
            minimum_mae_q90_coverage: 0.80,
            maximum_mae_q90_coverage: 1.0,
            degrade_after_failed_windows: 2,
            suspend_after_severe_windows: 2,
            recover_after_healthy_windows: 2,
            warming_up_forces_wait: true,
        };
        let insert_evidence = |index: i64| {
            let connection = store.connection.lock().expect("database mutex");
            let positive = index % 2 == 0;
            let probability = if positive { 0.9 } else { 0.1 };
            let outcome = if positive { "TP_FIRST" } else { "SL_FIRST" };
            let timestamp = DateTime::<Utc>::from_timestamp(1_700_000_000 + index * 300, 0)
                .expect("test timestamp")
                .to_rfc3339();
            let prediction_id = format!("health-{index}");
            connection
                .execute(
                    "INSERT INTO predictions
                     (prediction_id, model_id, feature_version, barrier_spec_id,
                      direction_probability_up, barrier_probability_long,
                      barrier_probability_short, symbol, timeframe,
                      origin_bar_timestamp, origin_close, origin_bid, origin_ask,
                      origin_bar_index, generated_at, expires_at, forecast_json,
                      proposal_json, created_at)
                     VALUES (?1, 'health-model', 'goldm-m5-v4', ?2, ?3, ?3, ?3,
                             'GOLDm#', 'M5', ?4, 100.0, 100.0, 100.2, ?5, ?4,
                             ?4, '{}', '[]', ?4)",
                    params![
                        prediction_id,
                        BARRIER_SPEC_ID,
                        probability,
                        timestamp,
                        (1_700_000_000 + index * 300).div_euclid(300),
                    ],
                )
                .expect("prediction evidence");
            connection
                .execute(
                    "INSERT INTO prediction_horizon_outcomes
                     (prediction_id, horizon_bars, origin_bar_timestamp,
                      outcome_bar_timestamp, actual_return, actual_high, actual_low,
                      interval_hit, direction_hit, barrier_outcome,
                      barrier_long_outcome, barrier_short_outcome,
                      error_metrics_json, settled_at)
                     VALUES (?1, 3, ?2, ?2, ?3, 101.0, 99.0, 1, 1, ?4, ?4, ?4,
                             '{\"mae_error_usd\":0.1}', ?2)",
                    params![
                        prediction_id,
                        timestamp,
                        if positive { 0.01 } else { -0.01 },
                        outcome,
                    ],
                )
                .expect("outcome evidence");
        };
        for index in 0..3 {
            insert_evidence(index);
        }
        let warming = store
            .model_health("health-model", &policy)
            .expect("first health window");
        assert_eq!(warming.status, ModelHealthStatus::WarmingUp);
        assert_eq!(warming.consecutive_successes, 1);

        insert_evidence(3);
        let healthy = store
            .model_health("health-model", &policy)
            .expect("second health window");
        assert_eq!(healthy.status, ModelHealthStatus::Healthy);
        assert_eq!(healthy.consecutive_successes, 2);
        let repeated = store
            .model_health("health-model", &policy)
            .expect("same health window");
        assert_eq!(repeated.consecutive_successes, 2);
    }
}
