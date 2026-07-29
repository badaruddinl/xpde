PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS account_profiles (
    id INTEGER PRIMARY KEY,
    login INTEGER NOT NULL,
    server TEXT NOT NULL,
    currency TEXT NOT NULL,
    leverage INTEGER NOT NULL,
    balance REAL NOT NULL,
    equity REAL NOT NULL,
    free_margin REAL NOT NULL,
    captured_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS symbol_specs (
    symbol TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    contract_size REAL NOT NULL,
    volume_min REAL NOT NULL,
    volume_max REAL NOT NULL,
    volume_step REAL NOT NULL,
    tick_size REAL NOT NULL,
    tick_value REAL NOT NULL,
    stops_level_points INTEGER NOT NULL,
    digits INTEGER NOT NULL,
    captured_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_bars (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    tick_volume REAL NOT NULL,
    PRIMARY KEY(symbol, timeframe, timestamp)
);

CREATE TABLE IF NOT EXISTS feature_snapshots (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    source_timestamp TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    staleness_ms INTEGER NOT NULL,
    missing_flags_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_registry (
    model_id TEXT PRIMARY KEY,
    model_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('candidate', 'challenger', 'champion', 'retired')),
    feature_version TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    promoted_at TEXT
);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL,
    barrier_spec_id TEXT,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    origin_bar_timestamp TEXT,
    origin_close REAL,
    origin_bar_index INTEGER,
    generated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    decision_valid_until TEXT,
    outcome_matures_at TEXT,
    forecast_json TEXT NOT NULL,
    proposal_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prediction_horizon_outcomes (
    prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
    horizon_bars INTEGER NOT NULL,
    origin_bar_timestamp TEXT NOT NULL,
    outcome_bar_timestamp TEXT NOT NULL,
    actual_return REAL NOT NULL,
    actual_high REAL NOT NULL,
    actual_low REAL NOT NULL,
    interval_hit INTEGER NOT NULL,
    direction_hit INTEGER NOT NULL,
    barrier_outcome TEXT CHECK(
        barrier_outcome IS NULL OR barrier_outcome IN (
            'TP_FIRST',
            'SL_FIRST',
            'NO_HIT_BEFORE_EXPIRY',
            'AMBIGUOUS_SAME_BAR'
        )
    ),
    barrier_long_outcome TEXT CHECK(
        barrier_long_outcome IS NULL OR barrier_long_outcome IN (
            'TP_FIRST',
            'SL_FIRST',
            'NO_HIT_BEFORE_EXPIRY',
            'AMBIGUOUS_SAME_BAR'
        )
    ),
    barrier_short_outcome TEXT CHECK(
        barrier_short_outcome IS NULL OR barrier_short_outcome IN (
            'TP_FIRST',
            'SL_FIRST',
            'NO_HIT_BEFORE_EXPIRY',
            'AMBIGUOUS_SAME_BAR'
        )
    ),
    error_metrics_json TEXT NOT NULL,
    settled_at TEXT NOT NULL,
    PRIMARY KEY(prediction_id, horizon_bars)
);

CREATE TABLE IF NOT EXISTS prediction_proposal_outcomes (
    prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
    profile TEXT NOT NULL CHECK(profile IN ('SCALPER', 'SNIPER')),
    horizon_bars INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('LONG', 'SHORT')),
    target_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    barrier_outcome TEXT NOT NULL CHECK(
        barrier_outcome IN (
            'TP_FIRST',
            'SL_FIRST',
            'NO_HIT_BEFORE_EXPIRY',
            'AMBIGUOUS_SAME_BAR'
        )
    ),
    settled_at TEXT NOT NULL,
    PRIMARY KEY(prediction_id, profile, horizon_bars)
);

CREATE TABLE IF NOT EXISTS prediction_outcomes (
    prediction_id TEXT PRIMARY KEY REFERENCES predictions(prediction_id),
    actual_return REAL NOT NULL,
    actual_high REAL NOT NULL,
    actual_low REAL NOT NULL,
    interval_hit INTEGER NOT NULL,
    direction_hit INTEGER NOT NULL,
    tp_before_sl INTEGER,
    error_metrics_json TEXT NOT NULL,
    settled_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS human_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_id TEXT NOT NULL,
    profile TEXT,
    proposal_action TEXT,
    model_id TEXT,
    forecast_side TEXT,
    selected_reason TEXT,
    verdict TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drift_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id TEXT NOT NULL,
    detector TEXT NOT NULL,
    severity TEXT NOT NULL,
    details_json TEXT NOT NULL,
    detected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    details_json TEXT NOT NULL,
    detected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    entity_id TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_bars_time
    ON market_bars(symbol, timeframe, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_time
    ON predictions(generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_prediction_horizon_outcomes_time
    ON prediction_horizon_outcomes(settled_at DESC, horizon_bars);
CREATE INDEX IF NOT EXISTS idx_prediction_proposal_outcomes_time
    ON prediction_proposal_outcomes(settled_at DESC, profile, horizon_bars);
CREATE INDEX IF NOT EXISTS idx_feedback_prediction
    ON human_feedback(prediction_id, created_at DESC);
