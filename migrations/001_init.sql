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
    chart_mode TEXT NOT NULL DEFAULT 'UNKNOWN',
    quote_currency TEXT NOT NULL DEFAULT '',
    pnl_currency TEXT NOT NULL DEFAULT '',
    symbol_profit_currency TEXT NOT NULL DEFAULT '',
    calculated_pnl_currency TEXT NOT NULL DEFAULT '',
    profit_per_price_unit_per_lot_buy REAL,
    profit_per_price_unit_per_lot_sell REAL,
    pnl_calculation_source TEXT NOT NULL DEFAULT '',
    conversion_rate REAL,
    conversion_timestamp TEXT,
    trade_mode_enabled INTEGER NOT NULL DEFAULT 0,
    trade_mode TEXT NOT NULL DEFAULT 'UNKNOWN',
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
    bid_open REAL,
    bid_high REAL,
    bid_low REAL,
    bid_close REAL,
    ask_open REAL,
    ask_high REAL,
    ask_low REAL,
    ask_close REAL,
    executable_tick_count INTEGER NOT NULL DEFAULT 0,
    first_tick_msc INTEGER,
    last_tick_msc INTEGER,
    PRIMARY KEY(symbol, timeframe, timestamp)
);

CREATE TABLE IF NOT EXISTS market_tick_paths (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    tick_path_json TEXT NOT NULL,
    path_point_count INTEGER NOT NULL,
    first_tick_msc INTEGER NOT NULL,
    last_tick_msc INTEGER NOT NULL,
    path_valid INTEGER NOT NULL,
    source TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(symbol, timeframe, timestamp)
);

CREATE TABLE IF NOT EXISTS market_backfill_staging (
    import_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    tick_volume REAL NOT NULL,
    bid_open REAL NOT NULL,
    bid_high REAL NOT NULL,
    bid_low REAL NOT NULL,
    bid_close REAL NOT NULL,
    ask_open REAL NOT NULL,
    ask_high REAL NOT NULL,
    ask_low REAL NOT NULL,
    ask_close REAL NOT NULL,
    executable_tick_count INTEGER NOT NULL CHECK(executable_tick_count > 0),
    first_tick_msc INTEGER NOT NULL,
    last_tick_msc INTEGER NOT NULL CHECK(last_tick_msc >= first_tick_msc),
    tick_path_json TEXT NOT NULL,
    path_point_count INTEGER NOT NULL CHECK(path_point_count > 0),
    provider TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(import_id, symbol, timeframe, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_market_backfill_staging_created
    ON market_backfill_staging(created_at);

CREATE TABLE IF NOT EXISTS market_backfill_import_chunks (
    import_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    chunk_index INTEGER NOT NULL CHECK(chunk_index >= 0),
    total_chunks INTEGER NOT NULL CHECK(
        total_chunks > 0 AND chunk_index < total_chunks
    ),
    created_at TEXT NOT NULL,
    PRIMARY KEY(import_id, symbol, timeframe, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_market_backfill_import_chunks_created
    ON market_backfill_import_chunks(created_at);

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

CREATE TABLE IF NOT EXISTS decision_proposal_instances (
    proposal_id TEXT PRIMARY KEY,
    prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
    profile TEXT NOT NULL CHECK(profile IN ('SCALPER', 'SNIPER')),
    evaluated_at TEXT NOT NULL,
    quote_timestamp TEXT NOT NULL,
    proposal_fingerprint TEXT NOT NULL,
    reference_entry_price REAL,
    action TEXT NOT NULL CHECK(action IN ('LONG', 'SHORT', 'WAIT', 'NO_PREDICTION')),
    target_price REAL,
    stop_price REAL,
    remaining_reward_account REAL NOT NULL,
    remaining_risk_account REAL NOT NULL,
    account_currency TEXT NOT NULL,
    cost_model_id TEXT NOT NULL,
    entry_spread REAL NOT NULL,
    expected_exit_spread REAL NOT NULL,
    reason_codes_json TEXT NOT NULL,
    model_health_status TEXT NOT NULL,
    proposal_json TEXT NOT NULL,
    evidence_eligible INTEGER NOT NULL DEFAULT 0,
    evidence_source TEXT NOT NULL DEFAULT 'DIAGNOSTIC',
    settlement_status TEXT NOT NULL DEFAULT 'PENDING',
    settlement_reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decision_proposal_evidence (
    proposal_id TEXT NOT NULL REFERENCES decision_proposal_instances(proposal_id),
    evidence_source TEXT NOT NULL CHECK(evidence_source IN (
        'FIRST_ACTIONABLE', 'HUMAN_ACCEPTED', 'HUMAN_REJECTED', 'DIAGNOSTIC'
    )),
    created_at TEXT NOT NULL,
    PRIMARY KEY(proposal_id, evidence_source)
);

CREATE INDEX IF NOT EXISTS ix_proposal_instances_prediction_profile_time
ON decision_proposal_instances(prediction_id, profile, evaluated_at);

CREATE TABLE IF NOT EXISTS decision_proposal_outcomes (
    proposal_id TEXT PRIMARY KEY REFERENCES decision_proposal_instances(proposal_id),
    prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
    profile TEXT NOT NULL,
    horizon_bars INTEGER NOT NULL,
    action TEXT NOT NULL,
    target_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    barrier_outcome TEXT NOT NULL CHECK(
        barrier_outcome IN (
            'TP_FIRST',
            'SL_FIRST',
            'NO_HIT_BEFORE_EXPIRY',
            'AMBIGUOUS_SAME_BAR',
            'AMBIGUOUS_SAME_TIMESTAMP'
        )
    ),
    first_touch_time_msc INTEGER,
    first_touch_price REAL,
    settlement_source TEXT NOT NULL DEFAULT 'TICK_SEQUENCE',
    settled_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_registry (
    model_id TEXT PRIMARY KEY,
    model_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('candidate', 'challenger', 'champion', 'retired')),
    feature_version TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 0,
    eligibility_gate_version INTEGER NOT NULL DEFAULT 0,
    training_mode TEXT NOT NULL DEFAULT '',
    eligible_for_shadow INTEGER NOT NULL DEFAULT 0,
    barrier_spec_id TEXT NOT NULL DEFAULT '',
    executable_side_contract_id TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    promoted_at TEXT
);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL,
    feature_version TEXT,
    barrier_spec_id TEXT,
    direction_probability_up REAL,
    barrier_probability_long REAL,
    barrier_probability_short REAL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    origin_bar_timestamp TEXT,
    origin_close REAL,
    origin_bid REAL,
    origin_ask REAL,
    origin_bar_index INTEGER,
    generated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    decision_valid_until TEXT,
    outcome_matures_at TEXT,
    forecast_json TEXT NOT NULL,
    proposal_json TEXT NOT NULL,
    is_duplicate INTEGER NOT NULL DEFAULT 0,
    settlement_status TEXT NOT NULL DEFAULT 'PENDING',
    settlement_reason TEXT,
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
            'AMBIGUOUS_SAME_BAR',
            'AMBIGUOUS_SAME_TIMESTAMP'
        )
    ),
    barrier_long_outcome TEXT CHECK(
        barrier_long_outcome IS NULL OR barrier_long_outcome IN (
            'TP_FIRST',
            'SL_FIRST',
            'NO_HIT_BEFORE_EXPIRY',
            'AMBIGUOUS_SAME_BAR',
            'AMBIGUOUS_SAME_TIMESTAMP'
        )
    ),
    barrier_short_outcome TEXT CHECK(
        barrier_short_outcome IS NULL OR barrier_short_outcome IN (
            'TP_FIRST',
            'SL_FIRST',
            'NO_HIT_BEFORE_EXPIRY',
            'AMBIGUOUS_SAME_BAR',
            'AMBIGUOUS_SAME_TIMESTAMP'
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
            'AMBIGUOUS_SAME_BAR',
            'AMBIGUOUS_SAME_TIMESTAMP'
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
    proposal_id TEXT REFERENCES decision_proposal_instances(proposal_id),
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

CREATE TABLE IF NOT EXISTS model_health_state (
    model_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    consecutive_severe_failures INTEGER NOT NULL DEFAULT 0,
    consecutive_successes INTEGER NOT NULL DEFAULT 0,
    evidence_fingerprint TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_health_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id TEXT NOT NULL,
    previous_status TEXT NOT NULL,
    current_status TEXT NOT NULL,
    evidence_fingerprint TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_predictions_origin_model
    ON predictions(origin_bar_timestamp, model_id);
CREATE INDEX IF NOT EXISTS idx_prediction_horizon_outcomes_time
    ON prediction_horizon_outcomes(settled_at DESC, horizon_bars);
CREATE INDEX IF NOT EXISTS idx_prediction_horizon_outcomes_prediction
    ON prediction_horizon_outcomes(prediction_id, horizon_bars);
CREATE INDEX IF NOT EXISTS idx_prediction_proposal_outcomes_time
    ON prediction_proposal_outcomes(settled_at DESC, profile, horizon_bars);
CREATE INDEX IF NOT EXISTS idx_feedback_prediction
    ON human_feedback(prediction_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_proposal_instances_prediction
    ON decision_proposal_instances(prediction_id, profile, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_proposal_instances_evidence
    ON decision_proposal_instances(evidence_eligible, evaluated_at);
CREATE INDEX IF NOT EXISTS idx_decision_proposal_outcomes_time
    ON decision_proposal_outcomes(settled_at DESC, profile, horizon_bars);
