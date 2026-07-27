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
    AccountSnapshot, CalibrationStatus, DataQuality, DecisionAction, DecisionPolicy,
    DecisionProposal, ForecastEnvelope, ForecastPoint, HumanFeedback, MarketBar, MarketSnapshot,
    SymbolSpec, decide,
};

const MIGRATION: &str = include_str!("../../../migrations/001_init.sql");

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    runtime: Arc<RwLock<RuntimeState>>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeState {
    mode: &'static str,
    connection_status: &'static str,
    updated_at: DateTime<Utc>,
    snapshot: MarketSnapshot,
    forecast: ForecastEnvelope,
    proposals: Vec<DecisionProposal>,
    safety: SafetyStatus,
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
    artifact_path: String,
    metrics: serde_json::Value,
}

fn barrier_outcome(
    start_price: f64,
    expected_mfe_usd: f64,
    proposals: &[DecisionProposal],
    bars: &[(f64, f64, f64)],
) -> Option<i64> {
    if !expected_mfe_usd.is_finite() || expected_mfe_usd <= 0.0 {
        return None;
    }
    let proposal = proposals.iter().find(|proposal| {
        matches!(
            proposal.action,
            DecisionAction::Long | DecisionAction::Short
        ) && proposal.invalidation_price.is_some()
    })?;
    let invalidation = proposal.invalidation_price?;
    let target = match proposal.action {
        DecisionAction::Long => start_price + expected_mfe_usd,
        DecisionAction::Short => start_price - expected_mfe_usd,
        _ => return None,
    };
    for &(high, low, _) in bars {
        let (tp_hit, sl_hit) = match proposal.action {
            DecisionAction::Long => (high >= target, low <= invalidation),
            DecisionAction::Short => (low <= target, high >= invalidation),
            _ => return None,
        };
        match (tp_hit, sl_hit) {
            (true, false) => return Some(1),
            (false, true) => return Some(0),
            (true, true) => return None,
            (false, false) => {}
        }
    }
    None
}

#[derive(Debug, Serialize)]
struct ModelRecord {
    model_id: String,
    model_type: String,
    status: String,
    feature_version: String,
    artifact_path: String,
    metrics: serde_json::Value,
    created_at: String,
    promoted_at: Option<String>,
}

struct Store {
    connection: Mutex<Connection>,
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

    fn save_snapshot(&self, snapshot: &MarketSnapshot) -> Result<(), rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO account_profiles
             (login, server, currency, leverage, balance, equity, free_margin, captured_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
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
              tick_size, tick_value, stops_level_points, digits, captured_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
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
               captured_at=excluded.captured_at",
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
                snapshot.timestamp.to_rfc3339(),
            ],
        )?;
        {
            let mut statement = transaction.prepare(
                "INSERT OR REPLACE INTO market_bars
                 (symbol, timeframe, timestamp, open, high, low, close, tick_volume)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            for bar in &snapshot.bars {
                statement.execute(params![
                    snapshot.symbol,
                    snapshot.timeframe,
                    bar.timestamp.to_rfc3339(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.tick_volume,
                ])?;
            }
        }
        transaction.execute(
            "INSERT INTO audit_events(event_type, entity_id, payload_json, created_at)
             VALUES ('MARKET_SNAPSHOT_ACCEPTED', ?1, ?2, ?3)",
            params![
                snapshot.symbol,
                serde_json::to_string(snapshot).unwrap_or_else(|_| "{}".to_owned()),
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
        }
        let inserted = {
            let mut statement = transaction.prepare(
                "INSERT OR REPLACE INTO market_bars
                 (symbol, timeframe, timestamp, open, high, low, close, tick_volume)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
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
                ])?;
            }
            inserted
        };
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

    fn save_prediction(
        &self,
        snapshot: &MarketSnapshot,
        forecast: &ForecastEnvelope,
        proposals: &[DecisionProposal],
    ) -> Result<(), rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        let expiry = proposals
            .first()
            .map(|proposal| proposal.expires_at)
            .unwrap_or(forecast.generated_at);
        connection.execute(
            "INSERT OR REPLACE INTO predictions
             (prediction_id, model_id, symbol, timeframe, generated_at, expires_at,
              forecast_json, proposal_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                forecast.prediction_id.to_string(),
                forecast.model_id,
                snapshot.symbol,
                snapshot.timeframe,
                forecast.generated_at.to_rfc3339(),
                expiry.to_rfc3339(),
                serde_json::to_string(forecast).unwrap_or_else(|_| "{}".to_owned()),
                serde_json::to_string(proposals).unwrap_or_else(|_| "[]".to_owned()),
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn save_feedback(&self, feedback: &HumanFeedback) -> Result<(), rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        connection.execute(
            "INSERT INTO human_feedback
             (prediction_id, verdict, reason_codes_json, note, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                feedback.prediction_id.to_string(),
                format!("{:?}", feedback.verdict).to_uppercase(),
                serde_json::to_string(&feedback.reason_codes).unwrap_or_else(|_| "[]".to_owned()),
                feedback.note,
                feedback.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn register_model(&self, model: &ModelRegistration) -> Result<(), rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        connection.execute(
            "INSERT INTO model_registry
             (model_id, model_type, status, feature_version, artifact_path, metrics_json,
              created_at, promoted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)
             ON CONFLICT(model_id) DO UPDATE SET
               model_type=excluded.model_type,
               status=excluded.status,
               feature_version=excluded.feature_version,
               artifact_path=excluded.artifact_path,
               metrics_json=excluded.metrics_json",
            params![
                model.model_id,
                model.model_type,
                model.status,
                model.feature_version,
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
            "SELECT model_id, model_type, status, feature_version, artifact_path,
                    metrics_json, created_at, promoted_at
             FROM model_registry ORDER BY created_at DESC",
        )?;
        statement
            .query_map([], |row| {
                let metrics_json: String = row.get(5)?;
                Ok(ModelRecord {
                    model_id: row.get(0)?,
                    model_type: row.get(1)?,
                    status: row.get(2)?,
                    feature_version: row.get(3)?,
                    artifact_path: row.get(4)?,
                    metrics: serde_json::from_str(&metrics_json).unwrap_or(serde_json::Value::Null),
                    created_at: row.get(6)?,
                    promoted_at: row.get(7)?,
                })
            })?
            .collect()
    }

    fn settle_expired_predictions(&self) -> Result<usize, rusqlite::Error> {
        let mut connection = self.connection.lock().expect("database mutex poisoned");
        let now = Utc::now().to_rfc3339();
        let pending = {
            let mut statement = connection.prepare(
                "SELECT p.prediction_id, p.symbol, p.timeframe, p.generated_at,
                        p.expires_at, p.forecast_json, p.proposal_json
                 FROM predictions p
                 LEFT JOIN prediction_outcomes o ON o.prediction_id=p.prediction_id
                 WHERE o.prediction_id IS NULL AND p.expires_at <= ?1
                 ORDER BY p.generated_at",
            )?;
            statement
                .query_map([&now], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
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
            generated_at,
            expires_at,
            forecast_json,
            proposal_json,
        ) in pending
        {
            let start = transaction
                .query_row(
                    "SELECT close FROM market_bars
                     WHERE symbol=?1 AND timeframe=?2 AND timestamp<=?3
                     ORDER BY timestamp DESC LIMIT 1",
                    params![symbol, timeframe, generated_at],
                    |row| row.get::<_, f64>(0),
                )
                .ok();
            let Some(start_price) = start else {
                continue;
            };
            let outcome_bars = {
                let mut statement = transaction.prepare(
                    "SELECT high, low, close FROM market_bars
                     WHERE symbol=?1 AND timeframe=?2 AND timestamp>?3 AND timestamp<=?4
                     ORDER BY timestamp",
                )?;
                statement
                    .query_map(
                        params![symbol, timeframe, generated_at, expires_at],
                        |row| {
                            Ok((
                                row.get::<_, f64>(0)?,
                                row.get::<_, f64>(1)?,
                                row.get::<_, f64>(2)?,
                            ))
                        },
                    )?
                    .collect::<Result<Vec<_>, _>>()?
            };
            if outcome_bars.is_empty() || start_price <= 0.0 {
                continue;
            }
            let Ok(forecast) = serde_json::from_str::<ForecastEnvelope>(&forecast_json) else {
                continue;
            };
            let Some(target) = forecast
                .points
                .iter()
                .find(|point| point.horizon_bars == 3)
                .or_else(|| forecast.points.first())
            else {
                continue;
            };
            let final_price = outcome_bars.last().map(|bar| bar.2).unwrap_or(start_price);
            let actual_return = (final_price / start_price).ln();
            let actual_high = outcome_bars
                .iter()
                .map(|bar| bar.0)
                .fold(f64::NEG_INFINITY, f64::max);
            let actual_low = outcome_bars
                .iter()
                .map(|bar| bar.1)
                .fold(f64::INFINITY, f64::min);
            let interval_hit =
                i64::from(actual_return >= target.q10 && actual_return <= target.q90);
            let direction_hit = i64::from((target.q50 >= 0.0) == (actual_return >= 0.0));
            let proposals =
                serde_json::from_str::<Vec<DecisionProposal>>(&proposal_json).unwrap_or_default();
            let tp_before_sl = barrier_outcome(
                start_price,
                forecast.expected_mfe_usd,
                &proposals,
                &outcome_bars,
            );
            let metrics = serde_json::json!({
                "median_error": (actual_return - target.q50).abs(),
                "interval_miss": interval_hit == 0,
                "direction_error": direction_hit == 0,
                "mfe_error_usd": forecast.expected_mfe_usd - (actual_high - start_price).max(start_price - actual_low),
                "mae_error_usd": forecast.expected_mae_usd - (start_price - actual_low).max(actual_high - start_price),
            });
            transaction.execute(
                "INSERT OR IGNORE INTO prediction_outcomes
                 (prediction_id, actual_return, actual_high, actual_low, interval_hit,
                  direction_hit, tp_before_sl, error_metrics_json, settled_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    prediction_id,
                    actual_return,
                    actual_high,
                    actual_low,
                    interval_hit,
                    direction_hit,
                    tp_before_sl,
                    metrics.to_string(),
                    now,
                ],
            )?;
            settled += 1;
        }
        transaction.commit()?;
        Ok(settled)
    }

    fn evaluation_summary(&self) -> Result<serde_json::Value, rusqlite::Error> {
        let connection = self.connection.lock().expect("database mutex poisoned");
        let overall = connection.query_row(
            "SELECT COUNT(*),
                    COALESCE(AVG(o.interval_hit), 0.0),
                    COALESCE(AVG(o.direction_hit), 0.0),
                    COUNT(o.tp_before_sl),
                    AVG(CASE WHEN o.tp_before_sl IS NOT NULL THEN o.tp_before_sl END)
             FROM prediction_outcomes o
             JOIN predictions p ON p.prediction_id=o.prediction_id
             WHERE p.model_id != 'baseline-demo-v1'",
            [],
            |row| {
                Ok(serde_json::json!({
                    "settled_predictions": row.get::<_, i64>(0)?,
                    "interval_coverage": row.get::<_, f64>(1)?,
                    "direction_accuracy": row.get::<_, f64>(2)?,
                    "tp_before_sl_samples": row.get::<_, i64>(3)?,
                    "tp_before_sl_rate": row.get::<_, Option<f64>>(4)?,
                }))
            },
        )?;
        let by_model = {
            let mut statement = connection.prepare(
                "SELECT p.model_id, COUNT(*), AVG(o.interval_hit), AVG(o.direction_hit),
                        COUNT(o.tp_before_sl),
                        AVG(CASE WHEN o.tp_before_sl IS NOT NULL THEN o.tp_before_sl END)
                 FROM prediction_outcomes o
                 JOIN predictions p ON p.prediction_id=o.prediction_id
                 GROUP BY p.model_id ORDER BY MAX(o.settled_at) DESC",
            )?;
            statement
                .query_map([], |row| {
                    Ok(serde_json::json!({
                        "model_id": row.get::<_, String>(0)?,
                        "settled_predictions": row.get::<_, i64>(1)?,
                        "interval_coverage": row.get::<_, f64>(2)?,
                        "direction_accuracy": row.get::<_, f64>(3)?,
                        "tp_before_sl_samples": row.get::<_, i64>(4)?,
                        "tp_before_sl_rate": row.get::<_, Option<f64>>(5)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(serde_json::json!({
            "overall": overall,
            "by_model": by_model,
            "target_coverage": 0.80,
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
    let runtime = demo_state();
    let state = AppState {
        store,
        runtime: Arc::new(RwLock::new(runtime)),
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
    if !snapshot.data_quality.is_valid(10_000) {
        return Err(ApiError::bad_request(
            "snapshot rejected because data is stale or incomplete",
        ));
    }
    state
        .store
        .save_snapshot(&snapshot)
        .map_err(|error| ApiError::internal(error.to_string()))?;

    let mut runtime = state.runtime.write().await;
    runtime.snapshot = snapshot;
    runtime.connection_status = "MT5_CONNECTED";
    runtime.mode = "LIVE_SHADOW";
    runtime.updated_at = Utc::now();
    runtime.safety.feed_is_demo = false;
    let scalper = decide(
        &runtime.snapshot,
        &runtime.forecast,
        &DecisionPolicy::scalper(),
    );
    let sniper = decide(
        &runtime.snapshot,
        &runtime.forecast,
        &DecisionPolicy::sniper(),
    );
    runtime.proposals = vec![scalper, sniper];
    Ok(StatusCode::ACCEPTED)
}

async fn post_forecast(
    State(state): State<AppState>,
    Json(forecast): Json<ForecastEnvelope>,
) -> Result<StatusCode, ApiError> {
    forecast
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let mut runtime = state.runtime.write().await;
    let proposals = vec![
        decide(&runtime.snapshot, &forecast, &DecisionPolicy::scalper()),
        decide(&runtime.snapshot, &forecast, &DecisionPolicy::sniper()),
    ];
    state
        .store
        .save_prediction(&runtime.snapshot, &forecast, &proposals)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    runtime.forecast = forecast;
    runtime.proposals = proposals;
    runtime.updated_at = Utc::now();
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
    state
        .store
        .evaluation_summary()
        .map(Json)
        .map_err(|error| ApiError::internal(error.to_string()))
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
    if !runtime.safety.feed_is_demo {
        let bridge_age_ms = Utc::now()
            .signed_duration_since(runtime.updated_at)
            .num_milliseconds()
            .max(0) as u64;
        runtime.snapshot.data_quality.tick_age_ms =
            runtime.snapshot.data_quality.tick_age_ms.max(bridge_age_ms);
        if runtime.snapshot.data_quality.tick_age_ms > DecisionPolicy::scalper().max_tick_age_ms {
            runtime.connection_status = "MT5_STALE";
            runtime.proposals = vec![
                decide(
                    &runtime.snapshot,
                    &runtime.forecast,
                    &DecisionPolicy::scalper(),
                ),
                decide(
                    &runtime.snapshot,
                    &runtime.forecast,
                    &DecisionPolicy::sniper(),
                ),
            ];
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
        },
        data_quality: DataQuality {
            completeness: 1.0,
            tick_age_ms: 0,
            missing_flags: Vec::new(),
            reason_codes: vec!["DEMO_DATA".to_owned()],
        },
    };

    let forecast = ForecastEnvelope {
        prediction_id: Uuid::new_v4(),
        model_id: "baseline-demo-v1".to_owned(),
        feature_version: "goldm-m5-v1".to_owned(),
        generated_at: now,
        direction_probability_up: 0.57,
        barrier_probability: 0.54,
        expected_mfe_usd: 1.42,
        expected_mae_usd: 0.91,
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
        snapshot,
        forecast,
        proposals,
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
    use xpde_domain::TradingProfile;

    fn proposal(action: DecisionAction, invalidation_price: Option<f64>) -> DecisionProposal {
        let generated_at = Utc::now();
        DecisionProposal {
            prediction_id: Uuid::new_v4(),
            profile: TradingProfile::Scalper,
            action,
            generated_at,
            expires_at: generated_at + chrono::Duration::minutes(15),
            expected_edge_after_cost_usd: 1.0,
            reference_lot: 0.1,
            invalidation_price,
            reason_codes: Vec::new(),
            risk_warnings: Vec::new(),
        }
    }

    #[test]
    fn barrier_outcome_uses_first_actionable_proposal() {
        let proposals = vec![
            proposal(DecisionAction::Wait, None),
            proposal(DecisionAction::Long, Some(99.0)),
        ];
        let bars = vec![(101.2, 99.5, 101.0)];

        assert_eq!(barrier_outcome(100.0, 1.0, &proposals, &bars), Some(1));
    }

    #[test]
    fn barrier_outcome_detects_short_stop_first() {
        let proposals = vec![proposal(DecisionAction::Short, Some(101.0))];
        let bars = vec![(101.2, 99.5, 100.8)];

        assert_eq!(barrier_outcome(100.0, 1.0, &proposals, &bars), Some(0));
    }

    #[test]
    fn barrier_outcome_keeps_same_bar_ambiguity_unresolved() {
        let proposals = vec![proposal(DecisionAction::Long, Some(99.0))];
        let bars = vec![(101.2, 98.8, 100.2)];

        assert_eq!(barrier_outcome(100.0, 1.0, &proposals, &bars), None);
    }
}
