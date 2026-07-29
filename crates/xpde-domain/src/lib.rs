use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const SUPPORTED_SYMBOL: &str = "GOLDm#";
pub const SUPPORTED_TIMEFRAME: &str = "M5";
pub const BARRIER_SPEC_ID: &str = "atr-1.25tp-1.00sl-h3-v1";
pub const BARRIER_HORIZON_BARS: u32 = 3;
pub const M5_BAR_MINUTES: i64 = 5;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketBar {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub tick_volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountSnapshot {
    pub login: i64,
    pub server: String,
    pub balance: f64,
    pub equity: f64,
    pub free_margin: f64,
    pub leverage: u32,
    pub currency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SymbolSpec {
    pub description: String,
    pub contract_size: f64,
    pub volume_min: f64,
    pub volume_max: f64,
    pub volume_step: f64,
    pub tick_size: f64,
    pub tick_value: f64,
    pub stops_level_points: u32,
    pub digits: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataQuality {
    pub completeness: f64,
    pub tick_age_ms: u64,
    pub missing_flags: Vec<String>,
    pub reason_codes: Vec<String>,
}

impl DataQuality {
    pub fn is_valid(&self, max_tick_age_ms: u64) -> bool {
        self.completeness >= 0.995
            && self.tick_age_ms <= max_tick_age_ms
            && self.missing_flags.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketSnapshot {
    pub symbol: String,
    pub provider: String,
    pub timestamp: DateTime<Utc>,
    pub timeframe: String,
    pub bid: f64,
    pub ask: f64,
    pub bars: Vec<MarketBar>,
    #[serde(default)]
    pub current_bar: Option<MarketBar>,
    pub account: AccountSnapshot,
    pub symbol_spec: SymbolSpec,
    pub data_quality: DataQuality,
}

impl MarketSnapshot {
    pub fn spread_usd(&self) -> f64 {
        self.ask - self.bid
    }

    pub fn mid_price(&self) -> f64 {
        (self.ask + self.bid) / 2.0
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        if self.symbol != SUPPORTED_SYMBOL {
            return Err(ContractError::UnsupportedSymbol(self.symbol.clone()));
        }
        if self.timeframe != SUPPORTED_TIMEFRAME {
            return Err(ContractError::UnsupportedTimeframe(self.timeframe.clone()));
        }
        if !(self.bid.is_finite() && self.ask.is_finite() && self.ask > self.bid) {
            return Err(ContractError::InvalidQuote);
        }
        if self.account.leverage == 0 {
            return Err(ContractError::InvalidLeverage);
        }
        if self.symbol_spec.contract_size <= 0.0
            || self.symbol_spec.volume_min <= 0.0
            || self.symbol_spec.volume_step <= 0.0
        {
            return Err(ContractError::InvalidSymbolSpec);
        }
        if self.bars.len() < 24 {
            return Err(ContractError::InsufficientBars(self.bars.len()));
        }
        let maximum_timestamp = self.timestamp + chrono::Duration::minutes(5);
        if self
            .bars
            .iter()
            .any(|bar| bar.timestamp > maximum_timestamp)
            || self
                .current_bar
                .as_ref()
                .is_some_and(|bar| bar.timestamp > maximum_timestamp)
        {
            return Err(ContractError::FutureMarketData);
        }
        if self
            .bars
            .iter()
            .any(|bar| bar.timestamp.timestamp() % 300 != 0)
            || self
                .current_bar
                .as_ref()
                .is_some_and(|bar| bar.timestamp.timestamp() % 300 != 0)
        {
            return Err(ContractError::MisalignedMarketBar);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForecastPoint {
    pub horizon_bars: u32,
    pub q10: f64,
    pub q25: f64,
    pub q50: f64,
    pub q75: f64,
    pub q90: f64,
}

impl ForecastPoint {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.q10 <= self.q25
            && self.q25 <= self.q50
            && self.q50 <= self.q75
            && self.q75 <= self.q90
        {
            Ok(())
        } else {
            Err(ContractError::InvalidQuantiles(self.horizon_bars))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CalibrationStatus {
    pub target_coverage: f64,
    pub observed_coverage: f64,
    pub sample_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForecastEnvelope {
    pub prediction_id: Uuid,
    pub model_id: String,
    pub feature_version: String,
    pub origin_bar_timestamp: DateTime<Utc>,
    pub origin_close: f64,
    pub origin_bar_index: i64,
    pub generated_at: DateTime<Utc>,
    pub direction_probability_up: f64,
    pub barrier_probability_long: f64,
    pub barrier_probability_short: f64,
    pub barrier_spec_id: String,
    pub barrier_horizon_bars: u32,
    pub target_price_long: f64,
    pub stop_price_long: f64,
    pub target_price_short: f64,
    pub stop_price_short: f64,
    pub expected_mfe_long: f64,
    pub expected_mae_long: f64,
    pub expected_mfe_short: f64,
    pub expected_mae_short: f64,
    pub excursion_modelled: bool,
    pub calibration: CalibrationStatus,
    pub drift_detected: bool,
    pub points: Vec<ForecastPoint>,
}

impl ForecastEnvelope {
    pub fn validate(&self) -> Result<(), ContractError> {
        if !(0.0..=1.0).contains(&self.direction_probability_up)
            || !(0.0..=1.0).contains(&self.barrier_probability_long)
            || !(0.0..=1.0).contains(&self.barrier_probability_short)
        {
            return Err(ContractError::InvalidProbability);
        }
        if self.origin_close <= 0.0
            || !self.origin_close.is_finite()
            || self.origin_bar_timestamp.timestamp() % 300 != 0
            || self.origin_bar_index != self.origin_bar_timestamp.timestamp().div_euclid(300)
        {
            return Err(ContractError::InvalidPredictionOrigin);
        }
        if [
            self.target_price_long,
            self.stop_price_long,
            self.target_price_short,
            self.stop_price_short,
            self.expected_mfe_long,
            self.expected_mae_long,
            self.expected_mfe_short,
            self.expected_mae_short,
        ]
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(ContractError::InvalidExcursion);
        }
        if self.barrier_spec_id != BARRIER_SPEC_ID
            || self.barrier_horizon_bars != BARRIER_HORIZON_BARS
            || self.stop_price_long >= self.origin_close
            || self.target_price_long <= self.origin_close
            || self.target_price_short >= self.origin_close
            || self.stop_price_short <= self.origin_close
        {
            return Err(ContractError::InvalidBarrierContract);
        }
        if self.points.is_empty() {
            return Err(ContractError::MissingForecastPoints);
        }
        for point in &self.points {
            point.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradingProfile {
    Scalper,
    Sniper,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DecisionAction {
    Long,
    Short,
    Wait,
    NoPrediction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecisionProposal {
    pub prediction_id: Uuid,
    pub profile: TradingProfile,
    pub action: DecisionAction,
    pub generated_at: DateTime<Utc>,
    pub decision_valid_until: DateTime<Utc>,
    pub outcome_matures_at: DateTime<Utc>,
    pub expected_edge_after_cost_usd: f64,
    pub reference_lot: f64,
    pub invalidation_price: Option<f64>,
    pub target_price: Option<f64>,
    pub reason_codes: Vec<String>,
    pub risk_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecisionPolicy {
    pub profile: TradingProfile,
    pub max_tick_age_ms: u64,
    pub max_spread_usd: f64,
    pub commission_usd_per_lot: f64,
    pub slippage_buffer_usd: f64,
    pub min_direction_probability: f64,
    pub min_barrier_probability: f64,
    pub min_coverage: f64,
    pub max_coverage: f64,
}

impl DecisionPolicy {
    pub fn scalper() -> Self {
        Self {
            profile: TradingProfile::Scalper,
            max_tick_age_ms: 10_000,
            max_spread_usd: 0.80,
            commission_usd_per_lot: 0.0,
            slippage_buffer_usd: 0.08,
            min_direction_probability: 0.56,
            min_barrier_probability: 0.52,
            min_coverage: 0.76,
            max_coverage: 0.84,
        }
    }

    pub fn sniper() -> Self {
        Self {
            profile: TradingProfile::Sniper,
            max_tick_age_ms: 5_000,
            max_spread_usd: 0.55,
            commission_usd_per_lot: 0.0,
            slippage_buffer_usd: 0.08,
            min_direction_probability: 0.64,
            min_barrier_probability: 0.60,
            min_coverage: 0.77,
            max_coverage: 0.83,
        }
    }
}

pub fn decide(
    snapshot: &MarketSnapshot,
    forecast: &ForecastEnvelope,
    policy: &DecisionPolicy,
) -> DecisionProposal {
    decide_at(snapshot, forecast, policy, snapshot.timestamp)
}

pub fn decide_at(
    snapshot: &MarketSnapshot,
    forecast: &ForecastEnvelope,
    policy: &DecisionPolicy,
    decision_time: DateTime<Utc>,
) -> DecisionProposal {
    let generated_at = forecast.generated_at;
    let fallback_valid_until = generated_at + chrono::Duration::minutes(M5_BAR_MINUTES);
    let fallback_matures_at = generated_at
        + chrono::Duration::minutes((BARRIER_HORIZON_BARS as i64 + 1) * M5_BAR_MINUTES);
    let no_prediction = |reason: &str, decision_valid_until, outcome_matures_at| DecisionProposal {
        prediction_id: forecast.prediction_id,
        profile: policy.profile,
        action: DecisionAction::NoPrediction,
        generated_at,
        decision_valid_until,
        outcome_matures_at,
        expected_edge_after_cost_usd: 0.0,
        reference_lot: snapshot.symbol_spec.volume_min,
        invalidation_price: None,
        target_price: None,
        reason_codes: vec![reason.to_owned()],
        risk_warnings: Vec::new(),
    };

    if snapshot.validate().is_err() {
        return no_prediction(
            "CONTRACT_INVALID",
            fallback_valid_until,
            fallback_matures_at,
        );
    }
    if forecast.validate().is_err() {
        return no_prediction(
            "FORECAST_INVALID",
            fallback_valid_until,
            fallback_matures_at,
        );
    }
    let latest_completed = snapshot
        .bars
        .iter()
        .max_by_key(|bar| bar.timestamp)
        .expect("snapshot bars were validated");
    let decision_valid_until =
        forecast.origin_bar_timestamp + chrono::Duration::minutes(2 * M5_BAR_MINUTES);
    let outcome_matures_at = forecast.origin_bar_timestamp
        + chrono::Duration::minutes((forecast.barrier_horizon_bars as i64 + 1) * M5_BAR_MINUTES);
    if forecast.origin_bar_timestamp != latest_completed.timestamp
        || (forecast.origin_close - latest_completed.close).abs() > 1e-8
    {
        return no_prediction(
            "FORECAST_ORIGIN_MISMATCH",
            decision_valid_until,
            outcome_matures_at,
        );
    }
    if decision_time > decision_valid_until {
        return no_prediction("FORECAST_EXPIRED", decision_valid_until, outcome_matures_at);
    }
    let horizon = forecast
        .points
        .iter()
        .find(|point| point.horizon_bars == BARRIER_HORIZON_BARS)
        .or_else(|| forecast.points.first())
        .expect("forecast points were validated");
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();

    if !snapshot.data_quality.is_valid(policy.max_tick_age_ms) {
        return no_prediction(
            "DATA_INVALID_OR_STALE",
            decision_valid_until,
            outcome_matures_at,
        );
    }
    if snapshot.spread_usd() > policy.max_spread_usd {
        return no_prediction(
            "SPREAD_ABOVE_LIMIT",
            decision_valid_until,
            outcome_matures_at,
        );
    }
    if forecast.drift_detected {
        return no_prediction("DRIFT_DETECTED", decision_valid_until, outcome_matures_at);
    }

    if forecast.calibration.observed_coverage < policy.min_coverage
        || forecast.calibration.observed_coverage > policy.max_coverage
    {
        reasons.push("CALIBRATION_OUTSIDE_GATE".to_owned());
    }

    let q50_side = if horizon.q50 > 0.0 {
        Some(DecisionAction::Long)
    } else if horizon.q50 < 0.0 {
        Some(DecisionAction::Short)
    } else {
        None
    };
    let classifier_side = if forecast.direction_probability_up >= 0.5 {
        DecisionAction::Long
    } else {
        DecisionAction::Short
    };
    let barrier_side = if forecast.barrier_probability_long > forecast.barrier_probability_short {
        Some(DecisionAction::Long)
    } else if forecast.barrier_probability_short > forecast.barrier_probability_long {
        Some(DecisionAction::Short)
    } else {
        None
    };
    let side_is_coherent =
        q50_side.is_some() && q50_side == Some(classifier_side) && q50_side == barrier_side;
    if !side_is_coherent {
        reasons.push("FORECAST_SIDE_CONFLICT".to_owned());
    }

    let selected_side = q50_side.unwrap_or(classifier_side);
    let (direction_probability, barrier_probability, target_price, stop_price) = match selected_side
    {
        DecisionAction::Long => (
            forecast.direction_probability_up,
            forecast.barrier_probability_long,
            forecast.target_price_long,
            forecast.stop_price_long,
        ),
        DecisionAction::Short => (
            1.0 - forecast.direction_probability_up,
            forecast.barrier_probability_short,
            forecast.target_price_short,
            forecast.stop_price_short,
        ),
        _ => unreachable!("selected side is always directional"),
    };
    let reference_lot = snapshot.symbol_spec.volume_min;
    let expected_move_usd =
        (forecast.origin_close * horizon.q50.exp() - forecast.origin_close).abs();
    let all_in_cost_usd = snapshot.spread_usd()
        + policy.slippage_buffer_usd
        + policy.commission_usd_per_lot / snapshot.symbol_spec.contract_size;
    let edge_per_lot = (expected_move_usd - all_in_cost_usd) * snapshot.symbol_spec.contract_size;
    let expected_edge = edge_per_lot * reference_lot;

    let direction_ok = direction_probability >= policy.min_direction_probability;
    let barrier_ok = barrier_probability >= policy.min_barrier_probability;

    if expected_edge <= 0.0 {
        reasons.push("EDGE_TOO_SMALL_AFTER_COST".to_owned());
    }
    if !direction_ok {
        reasons.push("DIRECTION_PROBABILITY_TOO_LOW".to_owned());
    }
    if !barrier_ok {
        reasons.push("BARRIER_PROBABILITY_TOO_LOW".to_owned());
    }
    if !forecast.excursion_modelled {
        reasons.push("EXCURSION_MODEL_UNAVAILABLE".to_owned());
    }

    let action = if reasons.is_empty() {
        selected_side
    } else {
        DecisionAction::Wait
    };

    if snapshot.account.leverage >= 1000 {
        warnings.push("HIGH_LEVERAGE_ACCOUNT".to_owned());
    }
    warnings.push("MANUAL_CONFIRMATION_REQUIRED".to_owned());

    let (target, invalidation) = match action {
        DecisionAction::Long | DecisionAction::Short => (Some(target_price), Some(stop_price)),
        _ => (None, None),
    };

    DecisionProposal {
        prediction_id: forecast.prediction_id,
        profile: policy.profile,
        action,
        generated_at,
        decision_valid_until,
        outcome_matures_at,
        expected_edge_after_cost_usd: expected_edge,
        reference_lot,
        invalidation_price: invalidation,
        target_price: target,
        reason_codes: reasons,
        risk_warnings: warnings,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HumanFeedback {
    pub prediction_id: Uuid,
    pub profile: TradingProfile,
    pub proposal_action: DecisionAction,
    pub model_id: String,
    pub forecast_side: DecisionAction,
    pub selected_reason: Option<String>,
    pub verdict: FeedbackVerdict,
    pub reason_codes: Vec<String>,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeedbackVerdict {
    Accepted,
    Rejected,
    Uncertain,
}

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("unsupported symbol: {0}")]
    UnsupportedSymbol(String),
    #[error("unsupported timeframe: {0}")]
    UnsupportedTimeframe(String),
    #[error("invalid bid/ask quote")]
    InvalidQuote,
    #[error("invalid account leverage")]
    InvalidLeverage,
    #[error("invalid symbol specification")]
    InvalidSymbolSpec,
    #[error("at least 24 bars are required, got {0}")]
    InsufficientBars(usize),
    #[error("market data timestamp is ahead of the provider snapshot")]
    FutureMarketData,
    #[error("M5 market bar timestamp is not aligned to a five-minute boundary")]
    MisalignedMarketBar,
    #[error("invalid quantile ordering for horizon {0}")]
    InvalidQuantiles(u32),
    #[error("probability must be between zero and one")]
    InvalidProbability,
    #[error("prediction origin must be an exact completed M5 candle")]
    InvalidPredictionOrigin,
    #[error("forecast excursions must be finite non-negative values")]
    InvalidExcursion,
    #[error("forecast barrier contract does not match the trained objective")]
    InvalidBarrierContract,
    #[error("forecast points are required")]
    MissingForecastPoints,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    fn sample_snapshot() -> MarketSnapshot {
        let current = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let now = current - chrono::Duration::minutes(i64::from(current.minute() % 5));
        MarketSnapshot {
            symbol: SUPPORTED_SYMBOL.to_owned(),
            provider: "MetaTrader5".to_owned(),
            timestamp: now,
            timeframe: SUPPORTED_TIMEFRAME.to_owned(),
            bid: 3330.00,
            ask: 3330.24,
            bars: (0..24)
                .map(|offset| MarketBar {
                    timestamp: now - chrono::Duration::minutes(offset * 5),
                    open: 3330.0,
                    high: 3331.0,
                    low: 3329.0,
                    close: 3330.5,
                    tick_volume: 100.0,
                })
                .collect(),
            current_bar: None,
            account: AccountSnapshot {
                login: 1,
                server: "Demo".to_owned(),
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
                tick_age_ms: 100,
                missing_flags: Vec::new(),
                reason_codes: Vec::new(),
            },
        }
    }

    fn sample_forecast() -> ForecastEnvelope {
        ForecastEnvelope {
            prediction_id: Uuid::new_v4(),
            model_id: "baseline-v1".to_owned(),
            feature_version: "m5-v1".to_owned(),
            origin_bar_timestamp: sample_snapshot().bars[0].timestamp,
            origin_close: 3330.5,
            origin_bar_index: sample_snapshot().bars[0]
                .timestamp
                .timestamp()
                .div_euclid(300),
            generated_at: Utc::now(),
            direction_probability_up: 0.67,
            barrier_probability_long: 0.63,
            barrier_probability_short: 0.37,
            barrier_spec_id: BARRIER_SPEC_ID.to_owned(),
            barrier_horizon_bars: BARRIER_HORIZON_BARS,
            target_price_long: 3333.0,
            stop_price_long: 3328.5,
            target_price_short: 3328.0,
            stop_price_short: 3332.5,
            expected_mfe_long: 2.4,
            expected_mae_long: 1.1,
            expected_mfe_short: 1.8,
            expected_mae_short: 1.4,
            excursion_modelled: true,
            calibration: CalibrationStatus {
                target_coverage: 0.8,
                observed_coverage: 0.79,
                sample_size: 500,
            },
            drift_detected: false,
            points: vec![ForecastPoint {
                horizon_bars: 3,
                q10: -0.0003,
                q25: 0.0001,
                q50: 0.0006,
                q75: 0.0010,
                q90: 0.0015,
            }],
        }
    }

    #[test]
    fn proposes_long_only_after_cost_and_quality_gates() {
        let decision = decide(
            &sample_snapshot(),
            &sample_forecast(),
            &DecisionPolicy::scalper(),
        );
        assert_eq!(decision.action, DecisionAction::Long);
        assert!(decision.expected_edge_after_cost_usd > 0.0);
        assert!(
            decision
                .risk_warnings
                .contains(&"MANUAL_CONFIRMATION_REQUIRED".to_owned())
        );
    }

    #[test]
    fn stale_tick_forces_no_prediction() {
        let mut snapshot = sample_snapshot();
        snapshot.data_quality.tick_age_ms = 60_000;
        let decision = decide(&snapshot, &sample_forecast(), &DecisionPolicy::scalper());
        assert_eq!(decision.action, DecisionAction::NoPrediction);
        assert_eq!(decision.reason_codes, vec!["DATA_INVALID_OR_STALE"]);
    }

    #[test]
    fn forecast_for_previous_completed_bar_forces_no_prediction() {
        let snapshot = sample_snapshot();
        let mut forecast = sample_forecast();
        forecast.origin_bar_timestamp -= chrono::Duration::minutes(M5_BAR_MINUTES);
        forecast.origin_bar_index = forecast.origin_bar_timestamp.timestamp().div_euclid(300);
        let decision = decide(&snapshot, &forecast, &DecisionPolicy::scalper());
        assert_eq!(decision.action, DecisionAction::NoPrediction);
        assert_eq!(decision.reason_codes, vec!["FORECAST_ORIGIN_MISMATCH"]);
    }

    #[test]
    fn actionable_proposal_uses_trained_barrier_and_separate_clocks() {
        let forecast = sample_forecast();
        let decision = decide(&sample_snapshot(), &forecast, &DecisionPolicy::scalper());
        assert_eq!(decision.target_price, Some(forecast.target_price_long));
        assert_eq!(decision.invalidation_price, Some(forecast.stop_price_long));
        assert_eq!(
            decision.decision_valid_until,
            forecast.origin_bar_timestamp + chrono::Duration::minutes(10)
        );
        assert_eq!(
            decision.outcome_matures_at,
            forecast.origin_bar_timestamp + chrono::Duration::minutes(20)
        );
    }

    #[test]
    fn future_bar_is_rejected() {
        let mut snapshot = sample_snapshot();
        snapshot.bars[0].timestamp = snapshot.timestamp + chrono::Duration::minutes(10);
        assert!(matches!(
            snapshot.validate(),
            Err(ContractError::FutureMarketData)
        ));
    }

    #[test]
    fn misaligned_bar_is_rejected() {
        let mut snapshot = sample_snapshot();
        snapshot.timestamp = snapshot
            .timestamp
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        for (index, bar) in snapshot.bars.iter_mut().enumerate() {
            bar.timestamp = snapshot.timestamp - chrono::Duration::minutes((index * 5) as i64);
        }
        snapshot.bars[0].timestamp += chrono::Duration::seconds(1);
        assert!(matches!(
            snapshot.validate(),
            Err(ContractError::MisalignedMarketBar)
        ));
    }

    #[test]
    fn side_conflict_forces_wait() {
        let mut forecast = sample_forecast();
        forecast.barrier_probability_long = 0.40;
        forecast.barrier_probability_short = 0.68;
        let decision = decide(&sample_snapshot(), &forecast, &DecisionPolicy::scalper());
        assert_eq!(decision.action, DecisionAction::Wait);
        assert!(
            decision
                .reason_codes
                .contains(&"FORECAST_SIDE_CONFLICT".to_owned())
        );
    }

    #[test]
    fn python_v3_forecast_fixture_deserializes_and_matches_golden_decision() {
        let forecast: ForecastEnvelope =
            serde_json::from_str(include_str!("../../../tests/fixtures/forecast-v3.json"))
                .expect("Python golden forecast must deserialize");
        forecast.validate().expect("golden forecast must validate");
        let mut snapshot = sample_snapshot();
        snapshot.timestamp = forecast.origin_bar_timestamp + chrono::Duration::minutes(6);
        for (index, bar) in snapshot.bars.iter_mut().enumerate() {
            bar.timestamp =
                forecast.origin_bar_timestamp - chrono::Duration::minutes(index as i64 * 5);
            bar.close = forecast.origin_close;
            bar.open = forecast.origin_close;
            bar.high = forecast.origin_close + 1.0;
            bar.low = forecast.origin_close - 1.0;
        }
        let proposal = decide(&snapshot, &forecast, &DecisionPolicy::scalper());
        assert_eq!(proposal.action, DecisionAction::Long);
        assert_eq!(proposal.target_price, Some(forecast.target_price_long));
        assert_eq!(proposal.invalidation_price, Some(forecast.stop_price_long));
    }
}
