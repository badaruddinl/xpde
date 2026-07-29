use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const SUPPORTED_SYMBOL: &str = "GOLDm#";
pub const SUPPORTED_TIMEFRAME: &str = "M5";
pub const FEATURE_VERSION_ID: &str = "goldm-m5-v5";
pub const LABEL_CONTRACT_ID: &str = "exact-contiguous-m5-horizons-v1";
pub const BARRIER_SPEC_ID: &str = "atr-1.25tp-1.00sl-h3-executable-tick-aligned-v6";
pub const EXECUTABLE_SIDE_CONTRACT_ID: &str =
    "bid-entry-exit-long-ask-exit-short-complete-tick-sequence-v5";
pub const MINIMUM_EXECUTABLE_TICK_COVERAGE: f64 = 0.95;
pub const EXECUTABLE_FEATURE_WINDOW_BARS: usize = 24;
pub const BARRIER_HORIZON_BARS: u32 = 3;
pub const FORECAST_HORIZONS: [u32; 4] = [1, 3, 6, 12];
pub const MAX_FORECAST_HORIZON_BARS: u32 = 12;
pub const M5_BAR_MINUTES: i64 = 5;

pub fn is_price_tick_aligned(price: f64, tick_size: f64) -> bool {
    if !price.is_finite() || !tick_size.is_finite() || price <= 0.0 || tick_size <= 0.0 {
        return false;
    }
    let ticks = price / tick_size;
    (ticks - ticks.round()).abs() <= 1e-7
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketBar {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub tick_volume: f64,
    #[serde(default)]
    pub bid_open: Option<f64>,
    #[serde(default)]
    pub bid_high: Option<f64>,
    #[serde(default)]
    pub bid_low: Option<f64>,
    #[serde(default)]
    pub bid_close: Option<f64>,
    #[serde(default)]
    pub ask_open: Option<f64>,
    #[serde(default)]
    pub ask_high: Option<f64>,
    #[serde(default)]
    pub ask_low: Option<f64>,
    #[serde(default)]
    pub ask_close: Option<f64>,
    #[serde(default)]
    pub executable_tick_count: u64,
    #[serde(default)]
    pub first_tick_msc: Option<i64>,
    #[serde(default)]
    pub last_tick_msc: Option<i64>,
    #[serde(default)]
    pub executable_tick_path: Vec<(i64, f64, f64)>,
}

impl MarketBar {
    pub fn has_executable_sides(&self) -> bool {
        let values = [
            self.bid_open,
            self.bid_high,
            self.bid_low,
            self.bid_close,
            self.ask_open,
            self.ask_high,
            self.ask_low,
            self.ask_close,
        ];
        if values.iter().any(Option::is_none) {
            return false;
        }
        let bid_open = self.bid_open.unwrap_or_default();
        let bid_high = self.bid_high.unwrap_or_default();
        let bid_low = self.bid_low.unwrap_or_default();
        let bid_close = self.bid_close.unwrap_or_default();
        let ask_open = self.ask_open.unwrap_or_default();
        let ask_high = self.ask_high.unwrap_or_default();
        let ask_low = self.ask_low.unwrap_or_default();
        let ask_close = self.ask_close.unwrap_or_default();
        values.iter().flatten().all(|value| value.is_finite())
            && bid_low > 0.0
            && ask_low > bid_low
            && bid_high >= bid_open.max(bid_close)
            && bid_low <= bid_open.min(bid_close)
            && ask_high >= ask_open.max(ask_close)
            && ask_low <= ask_open.min(ask_close)
            && ask_open > bid_open
            && ask_high > bid_high
            && ask_low > bid_low
            && ask_close > bid_close
            && self.executable_tick_count > 0
            && self.first_tick_msc.is_some()
            && self.last_tick_msc >= self.first_tick_msc
            && self.first_tick_msc.is_some_and(|value| {
                value >= self.timestamp.timestamp_millis()
                    && value < self.timestamp.timestamp_millis() + 300_000
            })
            && self.last_tick_msc.is_some_and(|value| {
                value >= self.timestamp.timestamp_millis()
                    && value < self.timestamp.timestamp_millis() + 300_000
            })
    }

    pub fn has_valid_tick_path(&self) -> bool {
        if self.executable_tick_path.is_empty() || !self.has_executable_sides() {
            return false;
        }
        let Some((first, last)) = self.first_tick_msc.zip(self.last_tick_msc) else {
            return false;
        };
        if self.executable_tick_path.first().map(|tick| tick.0) != Some(first)
            || self.executable_tick_path.last().map(|tick| tick.0) != Some(last)
            || self
                .executable_tick_path
                .windows(2)
                .any(|pair| pair[0].0 > pair[1].0)
            || self.executable_tick_path.iter().any(|(_, bid, ask)| {
                !bid.is_finite() || !ask.is_finite() || *bid <= 0.0 || *ask <= *bid
            })
        {
            return false;
        }
        let bid_open = self.executable_tick_path[0].1;
        let ask_open = self.executable_tick_path[0].2;
        let bid_close = self.executable_tick_path.last().expect("non-empty path").1;
        let ask_close = self.executable_tick_path.last().expect("non-empty path").2;
        let bid_high = self
            .executable_tick_path
            .iter()
            .map(|tick| tick.1)
            .fold(f64::NEG_INFINITY, f64::max);
        let bid_low = self
            .executable_tick_path
            .iter()
            .map(|tick| tick.1)
            .fold(f64::INFINITY, f64::min);
        let ask_high = self
            .executable_tick_path
            .iter()
            .map(|tick| tick.2)
            .fold(f64::NEG_INFINITY, f64::max);
        let ask_low = self
            .executable_tick_path
            .iter()
            .map(|tick| tick.2)
            .fold(f64::INFINITY, f64::min);
        let expected = [
            self.bid_open,
            self.bid_high,
            self.bid_low,
            self.bid_close,
            self.ask_open,
            self.ask_high,
            self.ask_low,
            self.ask_close,
        ];
        let actual = [
            bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, ask_low, ask_close,
        ];
        expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| expected.is_some_and(|value| (value - actual).abs() <= 1e-8))
    }

    pub fn has_complete_tick_coverage(&self, minimum_ratio: f64) -> bool {
        minimum_ratio.is_finite()
            && (0.0..=1.0).contains(&minimum_ratio)
            && self.tick_volume.is_finite()
            && self.tick_volume > 0.0
            && self.executable_tick_count as f64 / self.tick_volume >= minimum_ratio
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChartMode {
    Bid,
    Last,
    #[default]
    Unknown,
}

impl ChartMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bid => "BID",
            Self::Last => "LAST",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeMode {
    Full,
    LongOnly,
    ShortOnly,
    CloseOnly,
    Disabled,
    #[default]
    Unknown,
}

impl TradeMode {
    pub const fn allows(self, action: DecisionAction) -> bool {
        matches!(
            (self, action),
            (Self::Full, DecisionAction::Long | DecisionAction::Short)
                | (Self::LongOnly, DecisionAction::Long)
                | (Self::ShortOnly, DecisionAction::Short)
        )
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "FULL",
            Self::LongOnly => "LONG_ONLY",
            Self::ShortOnly => "SHORT_ONLY",
            Self::CloseOnly => "CLOSE_ONLY",
            Self::Disabled => "DISABLED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketStatus {
    Open,
    MarketClosed,
    FeedStale,
    BridgeDisconnected,
    #[default]
    Unknown,
}

impl MarketStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::MarketClosed => "MARKET_CLOSED",
            Self::FeedStale => "FEED_STALE",
            Self::BridgeDisconnected => "BRIDGE_DISCONNECTED",
            Self::Unknown => "UNKNOWN",
        }
    }
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
    #[serde(default)]
    pub margin_per_lot_buy: Option<f64>,
    #[serde(default)]
    pub margin_per_lot_sell: Option<f64>,
    #[serde(default)]
    pub chart_mode: ChartMode,
    #[serde(default)]
    pub quote_currency: String,
    #[serde(default)]
    pub symbol_profit_currency: String,
    #[serde(default)]
    pub calculated_pnl_currency: String,
    #[serde(default)]
    pub profit_per_price_unit_per_lot_buy: Option<f64>,
    #[serde(default)]
    pub profit_per_price_unit_per_lot_sell: Option<f64>,
    #[serde(default)]
    pub pnl_calculation_source: String,
    #[serde(default)]
    pub conversion_rate: Option<f64>,
    #[serde(default)]
    pub conversion_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub trade_mode_enabled: bool,
    #[serde(default)]
    pub trade_mode: TradeMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataQuality {
    pub completeness: f64,
    pub tick_age_ms: u64,
    #[serde(default)]
    pub absolute_tick_age_ms: u64,
    #[serde(default)]
    pub transport_tick_age_ms: u64,
    #[serde(default)]
    pub market_status: MarketStatus,
    #[serde(default)]
    pub market_session_open_until: Option<DateTime<Utc>>,
    pub missing_flags: Vec<String>,
    pub reason_codes: Vec<String>,
}

impl DataQuality {
    pub fn is_valid(&self, max_tick_age_ms: u64) -> bool {
        self.completeness >= 0.995
            && self.tick_age_ms <= max_tick_age_ms
            && self.absolute_tick_age_ms <= max_tick_age_ms
            && self.transport_tick_age_ms <= max_tick_age_ms
            && self.market_status == MarketStatus::Open
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

    pub fn has_complete_executable_feature_window(&self) -> bool {
        if self.bars.len() < EXECUTABLE_FEATURE_WINDOW_BARS {
            return false;
        }
        let mut ordered = self.bars.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|bar| bar.timestamp);
        let window = &ordered[ordered.len() - EXECUTABLE_FEATURE_WINDOW_BARS..];
        window
            .windows(2)
            .all(|pair| pair[0].timestamp < pair[1].timestamp)
            && window.iter().all(|bar| {
                bar.has_executable_sides()
                    && bar.has_complete_tick_coverage(MINIMUM_EXECUTABLE_TICK_COVERAGE)
            })
    }

    pub fn rolling_exit_spread(&self, maximum_bars: usize, quantile: f64) -> Option<(f64, usize)> {
        let mut recent = self
            .bars
            .iter()
            .chain(self.current_bar.iter())
            .filter_map(|bar| Some((bar.timestamp, bar.ask_close? - bar.bid_close?)))
            .filter(|(_, spread)| spread.is_finite() && *spread > 0.0)
            .collect::<Vec<_>>();
        recent.sort_by_key(|(timestamp, _)| std::cmp::Reverse(*timestamp));
        let mut spreads = recent
            .into_iter()
            .take(maximum_bars)
            .map(|(_, spread)| spread)
            .collect::<Vec<_>>();
        if spreads.is_empty() {
            return None;
        }
        spreads.sort_by(f64::total_cmp);
        let sample_size = spreads.len();
        let index =
            ((sample_size.saturating_sub(1)) as f64 * quantile.clamp(0.0, 1.0)).ceil() as usize;
        Some((spreads[index.min(sample_size - 1)], sample_size))
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
        if self.data_quality.market_status == MarketStatus::Open
            && self.data_quality.market_session_open_until.is_none()
        {
            return Err(ContractError::MissingMarketSessionBoundary);
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
        if self.bars.iter().chain(self.current_bar.iter()).any(|bar| {
            let has_any_executable_value = bar.bid_open.is_some()
                || bar.bid_high.is_some()
                || bar.bid_low.is_some()
                || bar.bid_close.is_some()
                || bar.ask_open.is_some()
                || bar.ask_high.is_some()
                || bar.ask_low.is_some()
                || bar.ask_close.is_some();
            has_any_executable_value && !bar.has_executable_sides()
        }) {
            return Err(ContractError::InvalidExecutableBar);
        }
        if self
            .bars
            .iter()
            .chain(self.current_bar.iter())
            .any(|bar| !bar.executable_tick_path.is_empty() && !bar.has_valid_tick_path())
        {
            return Err(ContractError::InvalidExecutableBar);
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
    pub label_contract_id: String,
    pub probability_reference: String,
    pub entry_conditioned_probability: bool,
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
        if self.feature_version != FEATURE_VERSION_ID
            || self.label_contract_id != LABEL_CONTRACT_ID
            || self.probability_reference != "FORECAST_ORIGIN"
            || self.entry_conditioned_probability
            || self.barrier_spec_id != BARRIER_SPEC_ID
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
        let horizons = self
            .points
            .iter()
            .map(|point| point.horizon_bars)
            .collect::<Vec<_>>();
        if horizons != FORECAST_HORIZONS {
            return Err(ContractError::InvalidForecastHorizons);
        }
        for point in &self.points {
            point.validate()?;
        }
        Ok(())
    }
}

pub fn full_forecast_envelope_matures_at(forecast: &ForecastEnvelope) -> DateTime<Utc> {
    forecast.origin_bar_timestamp
        + chrono::Duration::minutes((i64::from(MAX_FORECAST_HORIZON_BARS) + 1) * M5_BAR_MINUTES)
}

pub fn market_session_covers_full_forecast_envelope(
    snapshot: &MarketSnapshot,
    forecast: &ForecastEnvelope,
) -> bool {
    snapshot
        .data_quality
        .market_session_open_until
        .is_some_and(|session_end| session_end >= full_forecast_envelope_matures_at(forecast))
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

impl DecisionAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
            Self::Wait => "WAIT",
            Self::NoPrediction => "NO_PREDICTION",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecisionProposal {
    #[serde(default)]
    pub proposal_id: Option<Uuid>,
    pub prediction_id: Uuid,
    pub profile: TradingProfile,
    #[serde(default)]
    pub broker_policy_id: String,
    #[serde(default)]
    pub cost_model_id: String,
    #[serde(default)]
    pub account_currency: String,
    #[serde(default)]
    pub quote_currency: String,
    #[serde(default)]
    pub symbol_profit_currency: String,
    #[serde(default)]
    pub calculated_pnl_currency: String,
    #[serde(default)]
    pub pnl_calculation_source: String,
    #[serde(default)]
    pub conversion_rate: Option<f64>,
    #[serde(default)]
    pub conversion_timestamp: Option<DateTime<Utc>>,
    pub action: DecisionAction,
    pub generated_at: DateTime<Utc>,
    #[serde(default)]
    pub evaluated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub quote_timestamp: Option<DateTime<Utc>>,
    pub decision_valid_until: DateTime<Utc>,
    pub outcome_matures_at: DateTime<Utc>,
    #[serde(
        default,
        alias = "expected_edge_after_cost_usd",
        alias = "median_move_after_cost_usd"
    )]
    pub median_move_after_cost_account: f64,
    pub reference_lot: f64,
    #[serde(default)]
    pub reference_entry_price: Option<f64>,
    #[serde(default, alias = "remaining_reward_usd")]
    pub remaining_reward_account: f64,
    #[serde(default, alias = "remaining_risk_usd")]
    pub remaining_risk_account: f64,
    #[serde(default)]
    pub reward_risk_ratio: f64,
    #[serde(default)]
    pub entry_deviation_from_origin: f64,
    #[serde(default)]
    pub entry_spread: f64,
    #[serde(default)]
    pub expected_exit_spread: f64,
    #[serde(default)]
    pub exit_spread_sample_size: usize,
    #[serde(default)]
    pub exit_spread_quantile: f64,
    #[serde(default)]
    pub exit_spread_window: usize,
    #[serde(default)]
    pub slippage_assumption: f64,
    #[serde(default)]
    pub commission: f64,
    #[serde(default)]
    pub decision_age_seconds: u64,
    #[serde(default)]
    pub remaining_horizon_seconds: u64,
    #[serde(default)]
    pub maximum_decision_age_seconds: u64,
    #[serde(default)]
    pub forecast_generation_delay_ms: u64,
    #[serde(default)]
    pub maximum_forecast_generation_delay_ms: u64,
    #[serde(default)]
    pub model_health_status: String,
    #[serde(default)]
    pub evidence_eligible: bool,
    #[serde(default)]
    pub evidence_source: String,
    pub invalidation_price: Option<f64>,
    pub target_price: Option<f64>,
    pub reason_codes: Vec<String>,
    pub risk_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecisionPolicy {
    pub profile: TradingProfile,
    pub broker_policy_id: String,
    pub cost_model_id: String,
    pub max_tick_age_ms: u64,
    pub max_spread_usd: f64,
    pub commission_account_currency_per_lot: f64,
    pub expected_exit_spread_usd: f64,
    pub slippage_buffer_usd: f64,
    pub min_direction_probability: f64,
    pub min_barrier_probability: f64,
    pub min_coverage: f64,
    pub max_coverage: f64,
    pub max_entry_deviation_atr: f64,
    pub min_reward_risk_ratio: f64,
    pub max_spread_atr_ratio: f64,
    pub exit_spread_min_samples: usize,
    pub exit_spread_quantile: f64,
    pub exit_spread_window: usize,
    pub maximum_decision_age_seconds: u64,
    pub maximum_forecast_generation_delay_ms: u64,
}

impl DecisionPolicy {
    pub fn scalper() -> Self {
        Self {
            profile: TradingProfile::Scalper,
            broker_policy_id: "goldm-demo-v1".to_owned(),
            cost_model_id: "executable-side-v1".to_owned(),
            max_tick_age_ms: 10_000,
            max_spread_usd: 0.80,
            commission_account_currency_per_lot: 0.0,
            expected_exit_spread_usd: 0.34,
            slippage_buffer_usd: 0.08,
            min_direction_probability: 0.56,
            min_barrier_probability: 0.52,
            min_coverage: 0.76,
            max_coverage: 0.84,
            max_entry_deviation_atr: 0.50,
            min_reward_risk_ratio: 1.0,
            max_spread_atr_ratio: 0.50,
            exit_spread_min_samples: 12,
            exit_spread_quantile: 0.90,
            exit_spread_window: 24,
            maximum_decision_age_seconds: 60,
            maximum_forecast_generation_delay_ms: 10_000,
        }
    }

    pub fn sniper() -> Self {
        Self {
            profile: TradingProfile::Sniper,
            broker_policy_id: "goldm-demo-v1".to_owned(),
            cost_model_id: "executable-side-v1".to_owned(),
            max_tick_age_ms: 5_000,
            max_spread_usd: 0.55,
            commission_account_currency_per_lot: 0.0,
            expected_exit_spread_usd: 0.34,
            slippage_buffer_usd: 0.08,
            min_direction_probability: 0.64,
            min_barrier_probability: 0.60,
            min_coverage: 0.77,
            max_coverage: 0.83,
            max_entry_deviation_atr: 0.35,
            min_reward_risk_ratio: 1.25,
            max_spread_atr_ratio: 0.35,
            exit_spread_min_samples: 12,
            exit_spread_quantile: 0.90,
            exit_spread_window: 24,
            maximum_decision_age_seconds: 120,
            maximum_forecast_generation_delay_ms: 10_000,
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
    let expected_generation_time =
        forecast.origin_bar_timestamp + chrono::Duration::minutes(M5_BAR_MINUTES);
    let forecast_generation_delay_ms = generated_at
        .signed_duration_since(expected_generation_time)
        .num_milliseconds()
        .max(0) as u64;
    let fallback_valid_until = generated_at + chrono::Duration::minutes(M5_BAR_MINUTES);
    let fallback_matures_at = generated_at
        + chrono::Duration::minutes((BARRIER_HORIZON_BARS as i64 + 1) * M5_BAR_MINUTES);
    let no_prediction = |reason: &str, decision_valid_until, outcome_matures_at| DecisionProposal {
        proposal_id: None,
        prediction_id: forecast.prediction_id,
        profile: policy.profile,
        broker_policy_id: policy.broker_policy_id.clone(),
        cost_model_id: policy.cost_model_id.clone(),
        account_currency: snapshot.account.currency.clone(),
        quote_currency: snapshot.symbol_spec.quote_currency.clone(),
        symbol_profit_currency: snapshot.symbol_spec.symbol_profit_currency.clone(),
        calculated_pnl_currency: snapshot.symbol_spec.calculated_pnl_currency.clone(),
        pnl_calculation_source: snapshot.symbol_spec.pnl_calculation_source.clone(),
        conversion_rate: snapshot.symbol_spec.conversion_rate,
        conversion_timestamp: snapshot.symbol_spec.conversion_timestamp,
        action: DecisionAction::NoPrediction,
        generated_at,
        evaluated_at: Some(decision_time),
        quote_timestamp: Some(snapshot.timestamp),
        decision_valid_until,
        outcome_matures_at,
        median_move_after_cost_account: 0.0,
        reference_lot: snapshot.symbol_spec.volume_min,
        reference_entry_price: None,
        remaining_reward_account: 0.0,
        remaining_risk_account: 0.0,
        reward_risk_ratio: 0.0,
        entry_deviation_from_origin: 0.0,
        entry_spread: snapshot.spread_usd(),
        expected_exit_spread: policy.expected_exit_spread_usd,
        exit_spread_sample_size: 0,
        exit_spread_quantile: policy.exit_spread_quantile,
        exit_spread_window: policy.exit_spread_window,
        slippage_assumption: policy.slippage_buffer_usd,
        commission: policy.commission_account_currency_per_lot * snapshot.symbol_spec.volume_min,
        decision_age_seconds: decision_time
            .signed_duration_since(generated_at)
            .num_seconds()
            .max(0) as u64,
        remaining_horizon_seconds: outcome_matures_at
            .signed_duration_since(decision_time)
            .num_seconds()
            .max(0) as u64,
        maximum_decision_age_seconds: policy.maximum_decision_age_seconds,
        forecast_generation_delay_ms,
        maximum_forecast_generation_delay_ms: policy.maximum_forecast_generation_delay_ms,
        model_health_status: String::new(),
        evidence_eligible: false,
        evidence_source: "DIAGNOSTIC".to_owned(),
        invalidation_price: None,
        target_price: None,
        reason_codes: vec![reason.to_owned()],
        risk_warnings: Vec::new(),
    };

    if let Err(error) = snapshot.validate() {
        let reason = match error {
            ContractError::MissingMarketSessionBoundary => "MARKET_SESSION_BOUNDARY_UNAVAILABLE",
            _ => "CONTRACT_INVALID",
        };
        return no_prediction(reason, fallback_valid_until, fallback_matures_at);
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
    let decision_age_seconds = decision_time
        .signed_duration_since(forecast.generated_at)
        .num_seconds()
        .max(0) as u64;
    let remaining_horizon_seconds = outcome_matures_at
        .signed_duration_since(decision_time)
        .num_seconds()
        .max(0) as u64;
    let horizon = forecast
        .points
        .iter()
        .find(|point| point.horizon_bars == BARRIER_HORIZON_BARS)
        .or_else(|| forecast.points.first())
        .expect("forecast points were validated");
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();

    if !snapshot.data_quality.is_valid(policy.max_tick_age_ms) {
        let reason = match snapshot.data_quality.market_status {
            MarketStatus::MarketClosed => "MARKET_CLOSED",
            MarketStatus::FeedStale => "FEED_STALE",
            MarketStatus::BridgeDisconnected => "BRIDGE_DISCONNECTED",
            _ => "DATA_INVALID_OR_STALE",
        };
        return no_prediction(reason, decision_valid_until, outcome_matures_at);
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
    if forecast_generation_delay_ms > policy.maximum_forecast_generation_delay_ms {
        return no_prediction(
            "FORECAST_GENERATION_LATE",
            decision_valid_until,
            outcome_matures_at,
        );
    }
    match snapshot.data_quality.market_session_open_until {
        None => {
            return no_prediction(
                "MARKET_SESSION_BOUNDARY_UNAVAILABLE",
                decision_valid_until,
                outcome_matures_at,
            );
        }
        Some(session_end) if session_end < full_forecast_envelope_matures_at(forecast) => {
            return no_prediction(
                "HORIZON_CROSSES_MARKET_CLOSE",
                decision_valid_until,
                outcome_matures_at,
            );
        }
        Some(_) => {}
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
    if !snapshot.symbol_spec.trade_mode.allows(selected_side) {
        let reason = match snapshot.symbol_spec.trade_mode {
            TradeMode::LongOnly => "BROKER_LONG_ONLY",
            TradeMode::ShortOnly => "BROKER_SHORT_ONLY",
            TradeMode::CloseOnly => "BROKER_CLOSE_ONLY",
            TradeMode::Disabled => "BROKER_TRADE_MODE_DISABLED",
            TradeMode::Full | TradeMode::Unknown => "BROKER_TRADE_MODE_UNKNOWN",
        };
        return no_prediction(reason, decision_valid_until, outcome_matures_at);
    }
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
    let reference_entry_price = match selected_side {
        DecisionAction::Long => snapshot.ask,
        DecisionAction::Short => snapshot.bid,
        _ => unreachable!("selected side is always directional"),
    };
    let median_price = forecast.origin_close * horizon.q50.exp();
    let exit_spread_estimate =
        snapshot.rolling_exit_spread(policy.exit_spread_window, policy.exit_spread_quantile);
    let (expected_exit_spread, exit_spread_sample_size) =
        exit_spread_estimate.unwrap_or((policy.expected_exit_spread_usd, 0));
    let expected_exit_spread = expected_exit_spread.max(0.0);
    let median_exit_price = match selected_side {
        DecisionAction::Long => median_price,
        DecisionAction::Short => median_price + expected_exit_spread,
        _ => unreachable!("selected side is always directional"),
    };
    let median_move_price = match selected_side {
        DecisionAction::Long => median_exit_price - reference_entry_price,
        DecisionAction::Short => reference_entry_price - median_exit_price,
        _ => unreachable!("selected side is always directional"),
    };
    let authoritative_factor = match selected_side {
        DecisionAction::Long => snapshot.symbol_spec.profit_per_price_unit_per_lot_buy,
        DecisionAction::Short => snapshot.symbol_spec.profit_per_price_unit_per_lot_sell,
        _ => None,
    }
    .filter(|value| value.is_finite() && *value > 0.0);
    let same_currency = snapshot
        .account
        .currency
        .eq_ignore_ascii_case(&snapshot.symbol_spec.symbol_profit_currency);
    let (pnl_factor, pnl_calculation_source) = if let Some(factor) = authoritative_factor {
        (
            Some(factor),
            snapshot.symbol_spec.pnl_calculation_source.clone(),
        )
    } else if same_currency {
        (
            Some(snapshot.symbol_spec.contract_size),
            "CONTRACT_SIZE_SAME_CURRENCY".to_owned(),
        )
    } else {
        (None, "CURRENCY_CONVERSION_UNAVAILABLE".to_owned())
    };
    let pnl_factor_value = pnl_factor.unwrap_or(0.0);
    let slippage_cost = policy.slippage_buffer_usd * pnl_factor_value * reference_lot;
    let commission_cost = policy.commission_account_currency_per_lot * reference_lot;
    let median_move_after_cost =
        median_move_price * pnl_factor_value * reference_lot - slippage_cost - commission_cost;
    let non_spread_cost_price = policy.slippage_buffer_usd
        + policy.commission_account_currency_per_lot / pnl_factor_value.max(f64::EPSILON);
    let (remaining_reward_price, remaining_risk_price, entry_inside_barrier) = match selected_side {
        DecisionAction::Long => (
            target_price - reference_entry_price,
            reference_entry_price - stop_price,
            reference_entry_price < target_price && snapshot.bid > stop_price,
        ),
        DecisionAction::Short => (
            reference_entry_price - target_price,
            stop_price - reference_entry_price,
            reference_entry_price > target_price && snapshot.ask < stop_price,
        ),
        _ => unreachable!("selected side is always directional"),
    };
    let remaining_reward_account =
        remaining_reward_price.max(0.0) * pnl_factor_value * reference_lot;
    let remaining_risk_account = remaining_risk_price.max(0.0) * pnl_factor_value * reference_lot;
    let reward_risk_ratio = if remaining_risk_account > 0.0 {
        remaining_reward_account / remaining_risk_account
    } else {
        0.0
    };
    let entry_deviation_from_origin = (reference_entry_price - forecast.origin_close).abs();
    let inferred_atr = match selected_side {
        DecisionAction::Long => (forecast.target_price_long - forecast.origin_close) / 1.25,
        DecisionAction::Short => (forecast.origin_close - forecast.target_price_short) / 1.25,
        _ => unreachable!("selected side is always directional"),
    }
    .max(snapshot.symbol_spec.tick_size);
    let executable_barrier_state =
        snapshot
            .current_bar
            .as_ref()
            .and_then(|bar| match selected_side {
                DecisionAction::Long => Some((
                    bar.bid_high?,
                    bar.bid_low?,
                    bar.bid_high? >= target_price || bar.bid_low? <= stop_price,
                )),
                DecisionAction::Short => Some((
                    bar.ask_high?,
                    bar.ask_low?,
                    bar.ask_low? <= target_price || bar.ask_high? >= stop_price,
                )),
                _ => None,
            });
    let barrier_already_touched = executable_barrier_state
        .map(|(_, _, touched)| touched)
        .unwrap_or(false);

    let direction_ok = direction_probability >= policy.min_direction_probability;
    let barrier_ok = barrier_probability >= policy.min_barrier_probability;

    if barrier_already_touched {
        reasons.push("BARRIER_ALREADY_TOUCHED".to_owned());
    }
    if snapshot.symbol_spec.chart_mode != ChartMode::Bid {
        reasons.push("UNSUPPORTED_CHART_MODE".to_owned());
    }
    if executable_barrier_state.is_none() {
        reasons.push("EXECUTABLE_SIDE_BAR_MISSING".to_owned());
    }
    if !entry_inside_barrier {
        reasons.push("ENTRY_PRICE_OUTSIDE_BARRIER".to_owned());
    }
    if entry_deviation_from_origin / inferred_atr > policy.max_entry_deviation_atr {
        reasons.push("ENTRY_DEVIATION_TOO_LARGE".to_owned());
    }
    if snapshot.spread_usd() / inferred_atr > policy.max_spread_atr_ratio {
        reasons.push("SPREAD_ABOVE_ATR_LIMIT".to_owned());
    }
    if median_move_after_cost <= 0.0
        || remaining_reward_price <= non_spread_cost_price
        || reward_risk_ratio < policy.min_reward_risk_ratio
    {
        reasons.push("REMAINING_EDGE_TOO_SMALL".to_owned());
    }
    if pnl_factor.is_none() {
        reasons.push("CURRENCY_CONVERSION_UNAVAILABLE".to_owned());
    }
    if exit_spread_sample_size < policy.exit_spread_min_samples {
        reasons.push("EXIT_SPREAD_ESTIMATE_UNAVAILABLE".to_owned());
    }
    if decision_age_seconds > policy.maximum_decision_age_seconds {
        reasons.push("ENTRY_WINDOW_EXPIRED".to_owned());
    }
    let minimum_stop_distance =
        snapshot.symbol_spec.stops_level_points as f64 * snapshot.symbol_spec.tick_size;
    if minimum_stop_distance > 0.0
        && ((target_price - reference_entry_price).abs() < minimum_stop_distance
            || (stop_price - reference_entry_price).abs() < minimum_stop_distance)
    {
        reasons.push("BROKER_STOPS_LEVEL_VIOLATION".to_owned());
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
        proposal_id: None,
        prediction_id: forecast.prediction_id,
        profile: policy.profile,
        broker_policy_id: policy.broker_policy_id.clone(),
        cost_model_id: policy.cost_model_id.clone(),
        account_currency: snapshot.account.currency.clone(),
        quote_currency: snapshot.symbol_spec.quote_currency.clone(),
        symbol_profit_currency: snapshot.symbol_spec.symbol_profit_currency.clone(),
        calculated_pnl_currency: snapshot.symbol_spec.calculated_pnl_currency.clone(),
        pnl_calculation_source,
        conversion_rate: snapshot.symbol_spec.conversion_rate,
        conversion_timestamp: snapshot.symbol_spec.conversion_timestamp,
        action,
        generated_at,
        evaluated_at: Some(decision_time),
        quote_timestamp: Some(snapshot.timestamp),
        decision_valid_until,
        outcome_matures_at,
        median_move_after_cost_account: median_move_after_cost,
        reference_lot,
        reference_entry_price: Some(reference_entry_price),
        remaining_reward_account,
        remaining_risk_account,
        reward_risk_ratio,
        entry_deviation_from_origin,
        entry_spread: snapshot.spread_usd(),
        expected_exit_spread,
        exit_spread_sample_size,
        exit_spread_quantile: policy.exit_spread_quantile,
        exit_spread_window: policy.exit_spread_window,
        slippage_assumption: policy.slippage_buffer_usd,
        commission: commission_cost,
        decision_age_seconds,
        remaining_horizon_seconds,
        maximum_decision_age_seconds: policy.maximum_decision_age_seconds,
        forecast_generation_delay_ms,
        maximum_forecast_generation_delay_ms: policy.maximum_forecast_generation_delay_ms,
        model_health_status: String::new(),
        evidence_eligible: false,
        evidence_source: "DIAGNOSTIC".to_owned(),
        invalidation_price: invalidation,
        target_price: target,
        reason_codes: reasons,
        risk_warnings: warnings,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HumanFeedback {
    pub proposal_id: Uuid,
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
    #[error("open market snapshot requires an authoritative session boundary")]
    MissingMarketSessionBoundary,
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
    #[error("market bar contains an invalid executable Bid/Ask contract")]
    InvalidExecutableBar,
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
    #[error("forecast must contain exactly H1, H3, H6 and H12")]
    InvalidForecastHorizons,
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
                    bid_open: Some(3330.0),
                    bid_high: Some(3331.0),
                    bid_low: Some(3329.0),
                    bid_close: Some(3330.5),
                    ask_open: Some(3330.24),
                    ask_high: Some(3331.24),
                    ask_low: Some(3329.24),
                    ask_close: Some(3330.74),
                    executable_tick_count: 100,
                    first_tick_msc: Some(
                        (now - chrono::Duration::minutes(offset * 5)).timestamp() * 1000,
                    ),
                    last_tick_msc: Some(
                        (now - chrono::Duration::minutes(offset * 5)).timestamp() * 1000 + 299_000,
                    ),
                    executable_tick_path: Vec::new(),
                })
                .collect(),
            current_bar: Some(MarketBar {
                timestamp: now,
                open: 3330.5,
                high: 3330.8,
                low: 3330.2,
                close: 3330.6,
                tick_volume: 10.0,
                bid_open: Some(3330.5),
                bid_high: Some(3330.8),
                bid_low: Some(3330.2),
                bid_close: Some(3330.6),
                ask_open: Some(3330.74),
                ask_high: Some(3331.04),
                ask_low: Some(3330.44),
                ask_close: Some(3330.84),
                executable_tick_count: 10,
                first_tick_msc: Some(now.timestamp() * 1000),
                last_tick_msc: Some(now.timestamp() * 1000 + 1_000),
                executable_tick_path: Vec::new(),
            }),
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
                margin_per_lot_buy: Some(3.34),
                margin_per_lot_sell: Some(3.34),
                chart_mode: ChartMode::Bid,
                quote_currency: "USD".to_owned(),
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
                tick_age_ms: 100,
                absolute_tick_age_ms: 100,
                transport_tick_age_ms: 100,
                market_status: MarketStatus::Open,
                market_session_open_until: Some(Utc::now() + chrono::Duration::hours(8)),
                missing_flags: Vec::new(),
                reason_codes: Vec::new(),
            },
        }
    }

    fn sample_forecast() -> ForecastEnvelope {
        ForecastEnvelope {
            prediction_id: Uuid::new_v4(),
            model_id: "baseline-v1".to_owned(),
            feature_version: FEATURE_VERSION_ID.to_owned(),
            label_contract_id: LABEL_CONTRACT_ID.to_owned(),
            probability_reference: "FORECAST_ORIGIN".to_owned(),
            entry_conditioned_probability: false,
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
            points: FORECAST_HORIZONS
                .into_iter()
                .map(|horizon_bars| ForecastPoint {
                    horizon_bars,
                    q10: -0.0003,
                    q25: 0.0001,
                    q50: 0.0006,
                    q75: 0.0010,
                    q90: 0.0015,
                })
                .collect(),
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
        assert!(decision.median_move_after_cost_account > 0.0);
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
    fn broker_directional_trade_mode_blocks_wrong_side_and_close_only() {
        let mut snapshot = sample_snapshot();
        snapshot.symbol_spec.trade_mode = TradeMode::ShortOnly;
        let wrong_side = decide(&snapshot, &sample_forecast(), &DecisionPolicy::scalper());
        assert_eq!(wrong_side.action, DecisionAction::NoPrediction);
        assert_eq!(wrong_side.reason_codes, vec!["BROKER_SHORT_ONLY"]);

        snapshot.symbol_spec.trade_mode = TradeMode::CloseOnly;
        let close_only = decide(&snapshot, &sample_forecast(), &DecisionPolicy::scalper());
        assert_eq!(close_only.action, DecisionAction::NoPrediction);
        assert_eq!(close_only.reason_codes, vec!["BROKER_CLOSE_ONLY"]);
    }

    #[test]
    fn no_prediction_uses_database_contract_spelling() {
        assert_eq!(DecisionAction::NoPrediction.as_str(), "NO_PREDICTION");
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
        assert_eq!(decision.reference_entry_price, Some(sample_snapshot().ask));
        assert!(decision.remaining_reward_account > 0.0);
        assert!(decision.remaining_risk_account > 0.0);
        assert!(decision.reward_risk_ratio >= 1.0);
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
    fn side_aware_costs_do_not_double_charge_long_spread() {
        let snapshot = sample_snapshot();
        let forecast = sample_forecast();
        let policy = DecisionPolicy::scalper();
        let proposal = decide(&snapshot, &forecast, &policy);
        let h3 = forecast
            .points
            .iter()
            .find(|point| point.horizon_bars == BARRIER_HORIZON_BARS)
            .expect("H3 forecast");
        let median_bid_exit = forecast.origin_close * h3.q50.exp();
        let expected = (median_bid_exit - snapshot.ask)
            * snapshot.symbol_spec.contract_size
            * snapshot.symbol_spec.volume_min
            - policy.slippage_buffer_usd
                * snapshot.symbol_spec.contract_size
                * snapshot.symbol_spec.volume_min;
        assert!((proposal.median_move_after_cost_account - expected).abs() < 1e-10);
    }

    #[test]
    fn short_exit_uses_rolling_executable_ask_spread() {
        let snapshot = sample_snapshot();
        let mut forecast = sample_forecast();
        forecast.direction_probability_up = 0.33;
        forecast.barrier_probability_long = 0.37;
        forecast.barrier_probability_short = 0.63;
        forecast.stop_price_short = 3331.5;
        let h3 = forecast
            .points
            .iter_mut()
            .find(|point| point.horizon_bars == BARRIER_HORIZON_BARS)
            .expect("H3 forecast");
        h3.q10 = -0.0015;
        h3.q25 = -0.0010;
        h3.q50 = -0.0006;
        h3.q75 = -0.0001;
        h3.q90 = 0.0003;
        let mut policy = DecisionPolicy::scalper();
        policy.expected_exit_spread_usd = 0.80;

        let proposal = decide(&snapshot, &forecast, &policy);

        assert_eq!(proposal.action, DecisionAction::Short);
        assert!((proposal.expected_exit_spread - 0.24).abs() < 1e-10);
        let future_bid = forecast.origin_close
            * forecast
                .points
                .iter()
                .find(|point| point.horizon_bars == BARRIER_HORIZON_BARS)
                .expect("H3 forecast")
                .q50
                .exp();
        let expected_move = (snapshot.bid - (future_bid + 0.24))
            * snapshot.symbol_spec.contract_size
            * snapshot.symbol_spec.volume_min
            - policy.slippage_buffer_usd
                * snapshot.symbol_spec.contract_size
                * snapshot.symbol_spec.volume_min;
        assert!((proposal.median_move_after_cost_account - expected_move).abs() < 1e-10);
    }

    #[test]
    fn touched_barrier_forces_wait_before_entry() {
        let forecast = sample_forecast();
        let mut snapshot = sample_snapshot();
        snapshot.current_bar = Some(MarketBar {
            timestamp: snapshot.timestamp,
            open: forecast.origin_close,
            high: forecast.target_price_long,
            low: forecast.origin_close,
            close: forecast.origin_close,
            tick_volume: 10.0,
            bid_open: Some(forecast.origin_close),
            bid_high: Some(forecast.target_price_long),
            bid_low: Some(forecast.origin_close),
            bid_close: Some(forecast.origin_close),
            ask_open: Some(forecast.origin_close + 0.24),
            ask_high: Some(forecast.target_price_long + 0.24),
            ask_low: Some(forecast.origin_close + 0.24),
            ask_close: Some(forecast.origin_close + 0.24),
            executable_tick_count: 10,
            first_tick_msc: Some(snapshot.timestamp.timestamp() * 1000),
            last_tick_msc: Some(snapshot.timestamp.timestamp() * 1000 + 1_000),
            executable_tick_path: Vec::new(),
        });
        let decision = decide(&snapshot, &forecast, &DecisionPolicy::scalper());
        assert_eq!(decision.action, DecisionAction::Wait);
        assert!(
            decision
                .reason_codes
                .contains(&"BARRIER_ALREADY_TOUCHED".to_owned())
        );
    }

    #[test]
    fn non_account_currency_requires_authoritative_profit_conversion() {
        let forecast = sample_forecast();
        let mut snapshot = sample_snapshot();
        snapshot.account.currency = "IDR".to_owned();
        snapshot.symbol_spec.calculated_pnl_currency = "IDR".to_owned();
        snapshot.symbol_spec.symbol_profit_currency = "USD".to_owned();
        snapshot.symbol_spec.profit_per_price_unit_per_lot_buy = None;
        snapshot.symbol_spec.profit_per_price_unit_per_lot_sell = None;
        snapshot.symbol_spec.pnl_calculation_source = "UNAVAILABLE".to_owned();

        let decision = decide(&snapshot, &forecast, &DecisionPolicy::scalper());

        assert_eq!(decision.action, DecisionAction::Wait);
        assert!(
            decision
                .reason_codes
                .contains(&"CURRENCY_CONVERSION_UNAVAILABLE".to_owned())
        );
    }

    #[test]
    fn late_forecast_generation_is_never_actionable() {
        let mut forecast = sample_forecast();
        let snapshot = sample_snapshot();
        forecast.generated_at = forecast.origin_bar_timestamp
            + chrono::Duration::minutes(5)
            + chrono::Duration::seconds(11);
        let decision = decide_at(
            &snapshot,
            &forecast,
            &DecisionPolicy::scalper(),
            forecast.generated_at,
        );

        assert_eq!(decision.action, DecisionAction::NoPrediction);
        assert_eq!(decision.forecast_generation_delay_ms, 11_000);
        assert!(
            decision
                .reason_codes
                .contains(&"FORECAST_GENERATION_LATE".to_owned())
        );
    }

    #[test]
    fn horizon_crossing_session_end_is_never_actionable() {
        let forecast = sample_forecast();
        let mut snapshot = sample_snapshot();
        snapshot.data_quality.market_session_open_until =
            Some(forecast.origin_bar_timestamp + chrono::Duration::minutes(20));

        let decision = decide(&snapshot, &forecast, &DecisionPolicy::scalper());

        assert_eq!(decision.action, DecisionAction::NoPrediction);
        assert!(
            decision
                .reason_codes
                .contains(&"HORIZON_CROSSES_MARKET_CLOSE".to_owned())
        );
    }

    #[test]
    fn full_h12_session_boundary_keeps_the_envelope_actionable() {
        let forecast = sample_forecast();
        let mut snapshot = sample_snapshot();
        snapshot.data_quality.market_session_open_until =
            Some(forecast.origin_bar_timestamp + chrono::Duration::minutes(65));

        let decision = decide(&snapshot, &forecast, &DecisionPolicy::scalper());

        assert_eq!(decision.action, DecisionAction::Long);
        assert!(market_session_covers_full_forecast_envelope(
            &snapshot, &forecast
        ));
    }

    #[test]
    fn forecast_contract_requires_the_complete_horizon_envelope() {
        let mut forecast = sample_forecast();
        forecast
            .points
            .retain(|point| point.horizon_bars != MAX_FORECAST_HORIZON_BARS);

        assert!(matches!(
            forecast.validate(),
            Err(ContractError::InvalidForecastHorizons)
        ));
        let mut out_of_order = sample_forecast();
        out_of_order.points.swap(0, 1);
        assert!(matches!(
            out_of_order.validate(),
            Err(ContractError::InvalidForecastHorizons)
        ));
    }

    #[test]
    fn missing_session_boundary_is_never_actionable() {
        let forecast = sample_forecast();
        let mut snapshot = sample_snapshot();
        snapshot.data_quality.market_session_open_until = None;

        assert!(matches!(
            snapshot.validate(),
            Err(ContractError::MissingMarketSessionBoundary)
        ));
        let decision = decide(&snapshot, &forecast, &DecisionPolicy::scalper());

        assert_eq!(decision.action, DecisionAction::NoPrediction);
        assert!(
            decision
                .reason_codes
                .contains(&"MARKET_SESSION_BOUNDARY_UNAVAILABLE".to_owned())
        );
    }

    #[test]
    fn historical_tick_coverage_requires_the_configured_ratio() {
        let mut bar = sample_snapshot().bars[0].clone();
        bar.tick_volume = 100.0;
        bar.executable_tick_count = 94;
        assert!(!bar.has_complete_tick_coverage(0.95));
        bar.executable_tick_count = 95;
        assert!(bar.has_complete_tick_coverage(0.95));
        assert!(!bar.has_complete_tick_coverage(f64::NAN));
    }

    #[test]
    fn executable_feature_window_requires_24_unique_covered_bid_ask_bars() {
        let mut snapshot = sample_snapshot();
        assert!(snapshot.has_complete_executable_feature_window());

        snapshot.bars[0].executable_tick_count = 94;
        assert!(!snapshot.has_complete_executable_feature_window());
        snapshot.bars[0].executable_tick_count = 100;

        snapshot.bars[0].ask_close = None;
        assert!(!snapshot.has_complete_executable_feature_window());
        snapshot.bars[0].ask_close = Some(3330.74);

        snapshot.bars[0].timestamp = snapshot.bars[1].timestamp;
        assert!(!snapshot.has_complete_executable_feature_window());
    }

    #[test]
    fn broker_tick_alignment_rejects_fractional_ticks() {
        assert!(is_price_tick_aligned(3330.50, 0.01));
        assert!(is_price_tick_aligned(3330.505, 0.001));
        assert!(!is_price_tick_aligned(3330.505, 0.01));
        assert!(!is_price_tick_aligned(3330.50, 0.0));
    }

    #[test]
    fn forecast_contract_rejects_entry_conditioned_probability_claim() {
        let mut forecast = sample_forecast();
        forecast.entry_conditioned_probability = true;
        assert!(matches!(
            forecast.validate(),
            Err(ContractError::InvalidBarrierContract)
        ));
        forecast.entry_conditioned_probability = false;
        forecast.probability_reference = "CURRENT_ENTRY".to_owned();
        assert!(matches!(
            forecast.validate(),
            Err(ContractError::InvalidBarrierContract)
        ));
    }

    #[test]
    fn expired_entry_window_forces_wait_before_forecast_expiry() {
        let mut forecast = sample_forecast();
        let snapshot = sample_snapshot();
        forecast.generated_at = snapshot.timestamp - chrono::Duration::seconds(61);
        let decision = decide_at(
            &snapshot,
            &forecast,
            &DecisionPolicy::scalper(),
            snapshot.timestamp,
        );

        assert_eq!(decision.action, DecisionAction::Wait);
        assert_eq!(decision.decision_age_seconds, 61);
        assert!(
            decision
                .reason_codes
                .contains(&"ENTRY_WINDOW_EXPIRED".to_owned())
        );
    }

    #[test]
    fn consumed_remaining_reward_forces_wait() {
        let forecast = sample_forecast();
        let mut snapshot = sample_snapshot();
        snapshot.ask = forecast.target_price_long - 0.01;
        snapshot.bid = snapshot.ask - 0.24;
        let decision = decide(&snapshot, &forecast, &DecisionPolicy::scalper());
        assert_eq!(decision.action, DecisionAction::Wait);
        assert!(
            decision
                .reason_codes
                .contains(&"REMAINING_EDGE_TOO_SMALL".to_owned())
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
            bar.bid_open = Some(bar.open);
            bar.bid_high = Some(bar.high);
            bar.bid_low = Some(bar.low);
            bar.bid_close = Some(bar.close);
            bar.ask_open = Some(bar.open + 0.24);
            bar.ask_high = Some(bar.high + 0.24);
            bar.ask_low = Some(bar.low + 0.24);
            bar.ask_close = Some(bar.close + 0.24);
            bar.first_tick_msc = Some(bar.timestamp.timestamp_millis());
            bar.last_tick_msc = Some(bar.timestamp.timestamp_millis() + 299_000);
        }
        if let Some(current) = snapshot.current_bar.as_mut() {
            current.timestamp = forecast.origin_bar_timestamp + chrono::Duration::minutes(5);
            current.open = forecast.origin_close;
            current.high = forecast.origin_close + 0.25;
            current.low = forecast.origin_close - 0.25;
            current.close = forecast.origin_close;
            current.bid_open = Some(current.open);
            current.bid_high = Some(current.high);
            current.bid_low = Some(current.low);
            current.bid_close = Some(current.close);
            current.ask_open = Some(current.open + 0.24);
            current.ask_high = Some(current.high + 0.24);
            current.ask_low = Some(current.low + 0.24);
            current.ask_close = Some(current.close + 0.24);
            current.first_tick_msc = Some(current.timestamp.timestamp_millis());
            current.last_tick_msc = Some(current.timestamp.timestamp_millis() + 1_000);
        }
        let proposal = decide(&snapshot, &forecast, &DecisionPolicy::scalper());
        assert_eq!(proposal.action, DecisionAction::Long);
        assert_eq!(proposal.target_price, Some(forecast.target_price_long));
        assert_eq!(proposal.invalidation_price, Some(forecast.stop_price_long));
    }
}
