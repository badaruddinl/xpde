"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type Profile = "SCALPER" | "SNIPER";
type DecisionAction = "LONG" | "SHORT" | "WAIT" | "NO_PREDICTION";
type ForecastStatus =
  | "DEMO"
  | "WAITING_FOR_FIRST_FORECAST"
  | "CURRENT"
  | "ORIGIN_MISMATCH"
  | "EXPIRED";

interface MarketBar {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  tick_volume: number;
  bid_open?: number | null;
  bid_high?: number | null;
  bid_low?: number | null;
  bid_close?: number | null;
  ask_open?: number | null;
  ask_high?: number | null;
  ask_low?: number | null;
  ask_close?: number | null;
  executable_tick_count?: number;
}

interface ForecastPoint {
  horizon_bars: number;
  q10: number;
  q25: number;
  q50: number;
  q75: number;
  q90: number;
}

interface Proposal {
  prediction_id: string;
  profile: Profile;
  broker_policy_id: string;
  cost_model_id: string;
  account_currency: string;
  quote_currency: string;
  pnl_currency: string;
  action: DecisionAction;
  generated_at: string;
  decision_valid_until: string;
  outcome_matures_at: string;
  median_move_after_cost_usd: number;
  reference_lot: number;
  reference_entry_price: number | null;
  remaining_reward_usd: number;
  remaining_risk_usd: number;
  reward_risk_ratio: number;
  entry_deviation_from_origin: number;
  entry_spread: number;
  expected_exit_spread: number;
  slippage_assumption: number;
  commission: number;
  invalidation_price: number | null;
  target_price: number | null;
  reason_codes: string[];
  risk_warnings: string[];
}

interface DashboardState {
  mode: string;
  connection_status: string;
  updated_at: string;
  forecast_status: ForecastStatus;
  snapshot: {
    symbol: string;
    provider: string;
    timestamp: string;
    timeframe: string;
    bid: number;
    ask: number;
    bars: MarketBar[];
    current_bar?: MarketBar | null;
    account: {
      balance: number;
      equity: number;
      free_margin: number;
      leverage: number;
      currency: string;
    };
    symbol_spec: {
      contract_size: number;
      volume_min: number;
      volume_max: number;
      volume_step: number;
      tick_size: number;
      tick_value: number;
      margin_per_lot_buy?: number | null;
      margin_per_lot_sell?: number | null;
      chart_mode: "BID" | "LAST" | "UNKNOWN";
      quote_currency: string;
      pnl_currency: string;
    };
    data_quality: {
      completeness: number;
      tick_age_ms: number;
      absolute_tick_age_ms: number;
      transport_tick_age_ms: number;
      market_status:
        | "OPEN"
        | "MARKET_CLOSED"
        | "FEED_STALE"
        | "BRIDGE_DISCONNECTED"
        | "UNKNOWN";
      missing_flags: string[];
      reason_codes: string[];
    };
  };
  forecast: {
    prediction_id: string;
    model_id: string;
    feature_version: string;
    origin_bar_timestamp: string;
    origin_close: number;
    origin_bar_index: number;
    generated_at: string;
    direction_probability_up: number;
    barrier_probability_long: number;
    barrier_probability_short: number;
    barrier_spec_id: string;
    barrier_horizon_bars: number;
    target_price_long: number;
    stop_price_long: number;
    target_price_short: number;
    stop_price_short: number;
    expected_mfe_long: number;
    expected_mae_long: number;
    expected_mfe_short: number;
    expected_mae_short: number;
    excursion_modelled: boolean;
    calibration: {
      target_coverage: number;
      observed_coverage: number;
      sample_size: number;
    };
    drift_detected: boolean;
    points: ForecastPoint[];
  };
  proposals: Proposal[];
  model_health: {
    status: "WARMING_UP" | "HEALTHY" | "DEGRADED" | "SUSPENDED";
    sample_size: number;
    minimum_sample_size: number;
    interval_coverage: number | null;
    direction_brier: number | null;
    direction_baseline_brier: number | null;
    barrier_brier: number | null;
    barrier_baseline_brier: number | null;
    barrier_ece: number | null;
    mae_q90_coverage: number | null;
    reason_codes: string[];
  };
  safety: {
    auto_trading_enabled: boolean;
    human_confirmation_required: boolean;
    feed_is_demo: boolean;
  };
}

interface EvaluationMetrics {
  settled_predictions: number;
  interval_coverage: number;
  direction_accuracy: number;
  tp_before_sl_samples: number;
  tp_before_sl_rate: number | null;
  no_hit_samples: number;
  ambiguous_samples: number;
  tp_first_within_horizon_samples: number;
  tp_first_within_horizon_rate: number | null;
  tp_vs_sl_conditional_rate: number | null;
  direction_brier: number | null;
}

interface BarrierOutcomeMetrics {
  tp_before_sl_samples: number;
  tp_before_sl_rate: number | null;
  no_hit_samples: number;
  ambiguous_samples: number;
  tp_first_within_horizon_samples: number;
  tp_first_within_horizon_rate: number | null;
  tp_vs_sl_conditional_rate: number | null;
  brier_score?: number | null;
}

interface ProposalOutcomeMetrics extends BarrierOutcomeMetrics {
  profile: Profile;
  settled_proposals: number;
}

interface EvaluationSummary {
  scope: "LIVE_SHADOW_H3";
  overall: EvaluationMetrics;
  current_model: EvaluationMetrics & { model_id: string };
  current_session: EvaluationMetrics & {
    model_id: string;
    started_at: string;
  };
  by_model: Array<EvaluationMetrics & { model_id: string }>;
  forecast_barrier_by_side: Array<
    BarrierOutcomeMetrics & {
      side: "LONG" | "SHORT";
      settled_predictions: number;
    }
  >;
  proposal_outcomes_by_profile: ProposalOutcomeMetrics[];
  barrier_calibration_bins: Array<{
    side: "LONG" | "SHORT";
    bin_index: number;
    sample_size: number;
    mean_probability: number;
    observed_tp_rate: number;
    brier_score: number;
  }>;
  barrier_expected_calibration_error: number | null;
  target_coverage: number;
  current_model_window: number;
  updated_at: string;
}

const API_BASE = "http://127.0.0.1:8787";
const DEMO_REFERENCE_MS = Date.parse("2026-01-01T00:00:00.000Z");
const MIN_LIVE_EVIDENCE = 100;

function buildDemoState(): DashboardState {
  // This fixture is rendered on both the server and the browser during
  // hydration. Keep every value deterministic; live state replaces it after mount.
  const baseTime = DEMO_REFERENCE_MS - 47 * 5 * 60_000;
  let price = 3331.45;
  const bars = Array.from({ length: 48 }, (_, index) => {
    const open = price;
    const close = 3331.3 + index * 0.018 + Math.sin(index / 3.6) * 0.56;
    price = close;
    const high = Math.max(open, close) + 0.3 + (index % 3) * 0.05;
    const low = Math.min(open, close) - 0.28 - (index % 2) * 0.04;
    const spread = 0.34;
    return {
      timestamp: new Date(baseTime + index * 5 * 60_000).toISOString(),
      open,
      high,
      low,
      close,
      tick_volume: 155 + (index % 8) * 12,
      bid_open: open,
      bid_high: high,
      bid_low: low,
      bid_close: close,
      ask_open: open + spread,
      ask_high: high + spread,
      ask_low: low + spread,
      ask_close: close + spread,
      executable_tick_count: 155 + (index % 8) * 12,
    };
  });
  const predictionId = "demo-shadow-prediction";
  const now = new Date(DEMO_REFERENCE_MS).toISOString();
  const decisionValidUntil = new Date(DEMO_REFERENCE_MS + 10 * 60_000).toISOString();
  const outcomeMaturesAt = new Date(DEMO_REFERENCE_MS + 20 * 60_000).toISOString();
  return {
    mode: "DEMO_SHADOW",
    connection_status: "WAITING_FOR_MT5",
    updated_at: now,
    forecast_status: "DEMO",
    snapshot: {
      symbol: "GOLDm#",
      provider: "MetaTrader5 demo fixture",
      timestamp: now,
      timeframe: "M5",
      bid: 3332.74,
      ask: 3333.08,
      bars,
      current_bar: null,
      account: {
        balance: 1000,
        equity: 1000,
        free_margin: 1000,
        leverage: 1000,
        currency: "USD",
      },
      symbol_spec: {
        contract_size: 1,
        volume_min: 0.1,
        volume_max: 100,
        volume_step: 0.1,
        tick_size: 0.01,
        tick_value: 0.01,
        margin_per_lot_buy: 3.34,
        margin_per_lot_sell: 3.34,
        chart_mode: "BID",
        quote_currency: "USD",
        pnl_currency: "USD",
      },
      data_quality: {
        completeness: 1,
        tick_age_ms: 0,
        absolute_tick_age_ms: 0,
        transport_tick_age_ms: 0,
        market_status: "OPEN",
        missing_flags: [],
        reason_codes: ["DEMO_DATA"],
      },
    },
    forecast: {
      prediction_id: predictionId,
      model_id: "baseline-demo-v1",
      feature_version: "goldm-m5-v3",
      origin_bar_timestamp: bars[bars.length - 1].timestamp,
      origin_close: bars[bars.length - 1].close,
      origin_bar_index: Math.floor(
        Date.parse(bars[bars.length - 1].timestamp) / (5 * 60_000),
      ),
      generated_at: now,
      direction_probability_up: 0.57,
      barrier_probability_long: 0.54,
      barrier_probability_short: 0.46,
      barrier_spec_id: "atr-1.25tp-1.00sl-h3-executable-v2",
      barrier_horizon_bars: 3,
      target_price_long: bars[bars.length - 1].close + 1.25,
      stop_price_long: bars[bars.length - 1].close - 1,
      target_price_short: bars[bars.length - 1].close - 1.25,
      stop_price_short: bars[bars.length - 1].close + 1,
      expected_mfe_long: 1.42,
      expected_mae_long: 0.91,
      expected_mfe_short: 1.31,
      expected_mae_short: 0.98,
      excursion_modelled: false,
      calibration: {
        target_coverage: 0.8,
        observed_coverage: 0.786,
        sample_size: 500,
      },
      drift_detected: false,
      points: [
        { horizon_bars: 1, q10: -0.00022, q25: -0.00008, q50: 0.00004, q75: 0.00014, q90: 0.00025 },
        { horizon_bars: 3, q10: -0.00048, q25: -0.00018, q50: 0.0001, q75: 0.00038, q90: 0.0007 },
        { horizon_bars: 6, q10: -0.00077, q25: -0.00031, q50: 0.00017, q75: 0.00062, q90: 0.00108 },
        { horizon_bars: 12, q10: -0.00125, q25: -0.00052, q50: 0.00026, q75: 0.00102, q90: 0.00177 },
      ],
    },
    proposals: [
      {
        prediction_id: predictionId,
        profile: "SCALPER",
        broker_policy_id: "goldm-demo-v1",
        cost_model_id: "executable-side-rolling-spread-v1",
        account_currency: "USD",
        quote_currency: "USD",
        pnl_currency: "USD",
        action: "WAIT",
        generated_at: now,
        decision_valid_until: decisionValidUntil,
        outcome_matures_at: outcomeMaturesAt,
        median_move_after_cost_usd: -0.01,
        reference_lot: 0.1,
        reference_entry_price: 3333.08,
        remaining_reward_usd: 0.06,
        remaining_risk_usd: 0.17,
        reward_risk_ratio: 0.35,
        entry_deviation_from_origin: 0.17,
        entry_spread: 0.34,
        expected_exit_spread: 0.34,
        slippage_assumption: 0.03,
        commission: 0,
        invalidation_price: null,
        target_price: null,
        reason_codes: ["REMAINING_EDGE_TOO_SMALL"],
        risk_warnings: ["HIGH_LEVERAGE_ACCOUNT", "MANUAL_CONFIRMATION_REQUIRED"],
      },
      {
        prediction_id: predictionId,
        profile: "SNIPER",
        broker_policy_id: "goldm-demo-v1",
        cost_model_id: "executable-side-rolling-spread-v1",
        account_currency: "USD",
        quote_currency: "USD",
        pnl_currency: "USD",
        action: "WAIT",
        generated_at: now,
        decision_valid_until: decisionValidUntil,
        outcome_matures_at: outcomeMaturesAt,
        median_move_after_cost_usd: -0.01,
        reference_lot: 0.1,
        reference_entry_price: 3333.08,
        remaining_reward_usd: 0.06,
        remaining_risk_usd: 0.17,
        reward_risk_ratio: 0.35,
        entry_deviation_from_origin: 0.17,
        entry_spread: 0.34,
        expected_exit_spread: 0.34,
        slippage_assumption: 0.03,
        commission: 0,
        invalidation_price: null,
        target_price: null,
        reason_codes: [
          "REMAINING_EDGE_TOO_SMALL",
          "DIRECTION_PROBABILITY_TOO_LOW",
          "BARRIER_PROBABILITY_TOO_LOW",
        ],
        risk_warnings: ["HIGH_LEVERAGE_ACCOUNT", "MANUAL_CONFIRMATION_REQUIRED"],
      },
    ],
    model_health: {
      status: "WARMING_UP",
      sample_size: 0,
      minimum_sample_size: MIN_LIVE_EVIDENCE,
      interval_coverage: null,
      direction_brier: null,
      direction_baseline_brier: null,
      barrier_brier: null,
      barrier_baseline_brier: null,
      barrier_ece: null,
      mae_q90_coverage: null,
      reason_codes: ["MODEL_LIVE_HEALTH_WARMING_UP"],
    },
    safety: {
      auto_trading_enabled: false,
      human_confirmation_required: true,
      feed_is_demo: true,
    },
  };
}

function money(value: number, digits = 2, currency = "USD") {
  return new Intl.NumberFormat("en-ID", {
    style: "currency",
    currency,
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value);
}

function percent(value: number, digits = 1) {
  return `${(value * 100).toFixed(digits)}%`;
}

function time(value: string | null | undefined) {
  const parsed = new Date(value ?? "");
  if (Number.isNaN(parsed.getTime())) return "—";
  return new Intl.DateTimeFormat("id-ID", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "UTC",
  }).format(parsed);
}

function reasonLabel(reason: string) {
  const labels: Record<string, string> = {
    EDGE_TOO_SMALL_AFTER_COST: "Edge terlalu kecil setelah biaya",
    REMAINING_EDGE_TOO_SMALL: "Sisa reward dari harga entry tidak memadai",
    BARRIER_ALREADY_TOUCHED: "Target atau stop sudah tersentuh sejak forecast dibuat",
    ENTRY_PRICE_OUTSIDE_BARRIER: "Harga entry saat ini sudah di luar barrier",
    ENTRY_DEVIATION_TOO_LARGE: "Harga entry terlalu jauh dari origin forecast",
    DIRECTION_PROBABILITY_TOO_LOW: "Probabilitas arah belum melewati gate",
    BARRIER_PROBABILITY_TOO_LOW: "Peluang target belum memadai",
    CALIBRATION_OUTSIDE_GATE: "Coverage di luar toleransi",
    DATA_INVALID_OR_STALE: "Feed tidak valid atau stale",
    SPREAD_ABOVE_LIMIT: "Spread melewati batas profil",
    SPREAD_ABOVE_ATR_LIMIT: "Spread terlalu besar relatif terhadap ATR",
    DRIFT_DETECTED: "Drift model terdeteksi",
    FORECAST_SIDE_CONFLICT: "Arah quantile, classifier, dan barrier tidak selaras",
    FORECAST_ORIGIN_MISMATCH: "Forecast belum tersedia untuk candle M5 terbaru",
    FORECAST_EXPIRED: "Masa berlaku keputusan forecast sudah lewat",
    FORECAST_INVALID: "Kontrak forecast atau barrier tidak valid",
    EXCURSION_MODEL_UNAVAILABLE: "Model MFE/MAE dinamis belum tersedia",
    MARKET_CLOSED: "Pasar sedang tutup",
    FEED_STALE: "Tick broker sudah kedaluwarsa",
    BRIDGE_DISCONNECTED: "Bridge MT5 terputus",
    UNSUPPORTED_CHART_MODE: "Chart broker bukan Bid; kontrak executable tidak didukung",
    EXECUTABLE_SIDE_BAR_MISSING: "Candle Bid/Ask executable belum lengkap",
    MODEL_LIVE_HEALTH_WARMING_UP: "Evidence live belum mencapai sampel minimum",
    MODEL_LIVE_HEALTH_DEGRADED: "Kesehatan model live menurun; proposal dihentikan",
    MODEL_LIVE_HEALTH_SUSPENDED: "Model disuspensi oleh gate evidence live",
  };
  return labels[reason] ?? reason.replaceAll("_", " ").toLowerCase();
}

export default function Home() {
  const [state, setState] = useState<DashboardState>(() => buildDemoState());
  const [profile, setProfile] = useState<Profile>("SCALPER");
  const [coreApiConnected, setCoreApiConnected] = useState(false);
  const [transport, setTransport] = useState<"connected" | "reconnecting">("reconnecting");
  const [feedbackStatus, setFeedbackStatus] = useState("");
  const [riskPercent, setRiskPercent] = useState(1);
  const [evaluation, setEvaluation] = useState<EvaluationSummary | null>(null);
  const [retrying, setRetrying] = useState(false);
  const [retryStatus, setRetryStatus] = useState("");

  const loadState = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE}/api/v1/state`, { cache: "no-store" });
      if (!response.ok) throw new Error("API unavailable");
      setState((await response.json()) as DashboardState);
      setCoreApiConnected(true);
    } catch {
      setCoreApiConnected(false);
    }
  }, []);

  const loadEvaluation = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE}/api/v1/evaluation/summary`, {
        cache: "no-store",
      });
      if (!response.ok) throw new Error("Evaluation API unavailable");
      setEvaluation((await response.json()) as EvaluationSummary);
    } catch {
      setEvaluation(null);
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(loadState, 0);
    const timer =
      transport === "reconnecting" ? window.setInterval(loadState, 3000) : null;
    return () => {
      window.clearTimeout(initial);
      if (timer !== null) window.clearInterval(timer);
    };
  }, [loadState, transport]);

  useEffect(() => {
    const initial = window.setTimeout(loadEvaluation, 0);
    const timer = window.setInterval(loadEvaluation, 15_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [loadEvaluation]);

  useEffect(() => {
    let socket: WebSocket | null = null;
    let retryTimer: number | null = null;
    let stopped = false;
    let attempt = 0;

    const connect = () => {
      if (stopped) return;
      socket = new WebSocket("ws://127.0.0.1:8787/ws");
      socket.addEventListener("open", () => {
        attempt = 0;
        setCoreApiConnected(true);
        setTransport("connected");
      });
      socket.addEventListener("message", (event) => {
        try {
          setState(JSON.parse(event.data) as DashboardState);
          setCoreApiConnected(true);
          setTransport("connected");
        } catch {
          setTransport("reconnecting");
        }
      });
      socket.addEventListener("close", () => {
        if (stopped) return;
        setTransport("reconnecting");
        const delay = Math.min(30_000, 1000 * 2 ** attempt);
        attempt += 1;
        retryTimer = window.setTimeout(connect, delay);
      });
      socket.addEventListener("error", () => socket?.close());
    };

    connect();
    return () => {
      stopped = true;
      if (retryTimer !== null) window.clearTimeout(retryTimer);
      socket?.close();
    };
  }, []);

  const proposal = state.proposals.find((item) => item.profile === profile) ?? state.proposals[0];
  const activeEvaluation = evaluation?.current_model ?? null;
  const sessionEvaluation = evaluation?.current_session ?? null;
  const activeProposalEvaluation =
    evaluation?.proposal_outcomes_by_profile.find(
      (item) => item.profile === profile,
    ) ?? null;
  const liveCoverage =
    activeEvaluation &&
    activeEvaluation.settled_predictions >= MIN_LIVE_EVIDENCE
      ? activeEvaluation.interval_coverage
      : null;
  const sessionDirectionAccuracy =
    sessionEvaluation &&
    sessionEvaluation.settled_predictions >= MIN_LIVE_EVIDENCE
      ? sessionEvaluation.direction_accuracy
      : null;
  const realizedTpRate =
    activeProposalEvaluation &&
    activeProposalEvaluation.tp_first_within_horizon_samples >= MIN_LIVE_EVIDENCE
      ? activeProposalEvaluation.tp_first_within_horizon_rate
      : null;
  const horizonThree =
    state.forecast.points.find((point) => point.horizon_bars === 3) ??
    state.forecast.points[0];
  const forecastSide =
    proposal.action === "LONG" || proposal.action === "SHORT"
      ? proposal.action
      : horizonThree.q50 >= 0
        ? "LONG"
        : "SHORT";
  const barrierTarget =
    forecastSide === "LONG"
      ? state.forecast.target_price_long
      : state.forecast.target_price_short;
  const barrierStop =
    forecastSide === "LONG"
      ? state.forecast.stop_price_long
      : state.forecast.stop_price_short;
  const barrierProbability =
    forecastSide === "LONG"
      ? state.forecast.barrier_probability_long
      : state.forecast.barrier_probability_short;
  const expectedMfe =
    forecastSide === "LONG"
      ? state.forecast.expected_mfe_long
      : state.forecast.expected_mfe_short;
  const expectedMae =
    forecastSide === "LONG"
      ? state.forecast.expected_mae_long
      : state.forecast.expected_mae_short;
  const probabilityUp = state.forecast.direction_probability_up;
  const probabilityUpPercent = Math.round(probabilityUp * 1000) / 10;
  const probabilityDownPercent = Math.round((100 - probabilityUpPercent) * 10) / 10;
  const directionDifferencePoints = Math.abs(
    probabilityUpPercent - probabilityDownPercent,
  );
  const directionSummary =
    directionDifferencePoints < 0.1
      ? "Seimbang"
      : `Selisih ${directionDifferencePoints.toFixed(1)} poin`;
  const observedCoverage = state.forecast.calibration.observed_coverage;
  const coverageHealthy =
    profile === "SCALPER"
      ? observedCoverage >= 0.76 && observedCoverage <= 0.84
      : observedCoverage >= 0.77 && observedCoverage <= 0.83;
  const spread = state.snapshot.ask - state.snapshot.bid;
  const forecastIsStale =
    state.forecast_status !== "CURRENT" && state.forecast_status !== "DEMO";
  const maxForecastHorizon = Math.max(
    ...state.forecast.points.map((point) => point.horizon_bars),
    1,
  );
  const completedBars = state.snapshot.bars.slice(
    state.snapshot.current_bar ? -31 : -32,
  );
  const lastBars = state.snapshot.current_bar
    ? [...completedBars, state.snapshot.current_bar]
    : completedBars;
  const forecastPrices = state.forecast.points.flatMap((point) => [
    state.forecast.origin_close * Math.exp(point.q10),
    state.forecast.origin_close * Math.exp(point.q25),
    state.forecast.origin_close * Math.exp(point.q50),
    state.forecast.origin_close * Math.exp(point.q75),
    state.forecast.origin_close * Math.exp(point.q90),
  ]);
  const prices = [
    ...lastBars.flatMap((bar) => [bar.high, bar.low]),
    ...forecastPrices,
    barrierTarget,
    barrierStop,
  ];
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const range = Math.max(maxPrice - minPrice, 0.01);
  const lastPrice = (state.snapshot.bid + state.snapshot.ask) / 2;

  const riskPreview = useMemo(() => {
    if (proposal.action !== "LONG" && proposal.action !== "SHORT") {
      return { lot: null, reason: "Lot tidak dihitung karena proposal tidak actionable." };
    }
    if (!state.forecast.excursion_modelled || state.forecast_status !== "CURRENT") {
      return { lot: null, reason: "Forecast current dan model excursion diperlukan." };
    }
    if (
      proposal.reference_entry_price === null ||
      proposal.invalidation_price === null ||
      proposal.remaining_reward_usd <= 0 ||
      proposal.reward_risk_ratio <= 0
    ) {
      return { lot: null, reason: "Entry atau barrier proposal tidak valid." };
    }
    const riskUsd = state.snapshot.account.equity * (riskPercent / 100);
    const stopDistance = Math.abs(
      proposal.reference_entry_price - proposal.invalidation_price,
    );
    const tickSize = state.snapshot.symbol_spec.tick_size;
    const tickValue = state.snapshot.symbol_spec.tick_value;
    const lossPerLot = (stopDistance / tickSize) * tickValue;
    if (!Number.isFinite(lossPerLot) || lossPerLot <= 0) {
      return { lot: null, reason: "Tick value atau jarak stop tidak valid." };
    }
    const raw = riskUsd / lossPerLot;
    const minimumLot = state.snapshot.symbol_spec.volume_min;
    if (raw < minimumLot) {
      return { lot: null, reason: "MIN_LOT_EXCEEDS_RISK" };
    }
    const step = state.snapshot.symbol_spec.volume_step;
    const rounded = Math.floor(raw / step) * step;
    const lot = Math.min(
      state.snapshot.symbol_spec.volume_max,
      rounded,
    );
    const providerMarginPerLot =
      proposal.action === "LONG"
        ? state.snapshot.symbol_spec.margin_per_lot_buy
        : state.snapshot.symbol_spec.margin_per_lot_sell;
    const estimatedMargin =
      typeof providerMarginPerLot === "number" &&
      Number.isFinite(providerMarginPerLot) &&
      providerMarginPerLot > 0
        ? providerMarginPerLot * lot
        : (lastPrice * state.snapshot.symbol_spec.contract_size * lot) /
          state.snapshot.account.leverage;
    if (estimatedMargin > state.snapshot.account.free_margin) {
      return { lot: null, reason: "FREE_MARGIN_INSUFFICIENT" };
    }
    return { lot, reason: "" };
  }, [lastPrice, proposal, riskPercent, state]);

  async function submitFeedback(verdict: "ACCEPTED" | "REJECTED" | "UNCERTAIN") {
    if (state.safety.feed_is_demo || state.forecast_status !== "CURRENT") {
      setFeedbackStatus("Feedback hanya aktif untuk forecast live yang current.");
      return;
    }
    setFeedbackStatus("Menyimpan…");
    try {
      const response = await fetch(`${API_BASE}/api/v1/feedback`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          prediction_id: state.forecast.prediction_id,
          profile,
          proposal_action: proposal.action,
          model_id: state.forecast.model_id,
          forecast_side: forecastSide,
          selected_reason: proposal.reason_codes[0] ?? null,
          verdict,
          reason_codes: verdict === "REJECTED" ? ["MANUAL_REVIEW_REJECTED"] : [],
          note: null,
          created_at: new Date().toISOString(),
        }),
      });
      if (!response.ok) throw new Error("feedback rejected");
      setFeedbackStatus("Feedback tersimpan pada audit trail.");
    } catch {
      setFeedbackStatus("API lokal belum aktif; feedback belum disimpan.");
    }
  }

  async function retryRealtime() {
    setRetrying(true);
    setRetryStatus("Me-restart bridge MT5…");
    try {
      const response = await fetch("/api/local/retry-mt5-bridge", {
        method: "POST",
        headers: { "X-XPDE-Action": "retry-mt5-bridge" },
      });
      const result = (await response.json()) as {
        status?: "started" | "restarted" | "failed";
        error?: string;
      };
      if (!response.ok || result.status === "failed") {
        throw new Error(result.error ?? "Retry bridge ditolak");
      }

      setRetryStatus("Bridge aktif; menunggu tick MT5…");
      for (let attempt = 0; attempt < 8; attempt += 1) {
        await new Promise((resolve) => window.setTimeout(resolve, 750));
        const stateResponse = await fetch(`${API_BASE}/api/v1/state`, {
          cache: "no-store",
        });
        if (!stateResponse.ok) continue;
        const nextState = (await stateResponse.json()) as DashboardState;
        setState(nextState);
        setCoreApiConnected(true);
        const stateAgeMs = Date.now() - Date.parse(nextState.updated_at);
        if (
          nextState.connection_status === "MT5_CONNECTED" &&
          stateAgeMs < 15_000
        ) {
          setRetryStatus("Realtime tersambung kembali.");
          return;
        }
      }
      setRetryStatus("Bridge aktif; masih menunggu tick MT5.");
    } catch (error) {
      setRetryStatus(
        error instanceof Error ? `Retry gagal: ${error.message}` : "Retry gagal.",
      );
    } finally {
      setRetrying(false);
    }
  }

  const renderDecisionCard = () => (
    <section className={`panel decision-card action-${proposal.action.toLowerCase()}`}>
      <div className="decision-head"><span>Decision proposal</span><i>{profile} · {proposal.broker_policy_id}</i></div>
      <strong className="decision-action">{proposal.action.replace("_", " ")}</strong>
      <p>{proposal.reason_codes.length ? reasonLabel(proposal.reason_codes[0]) : "Semua gate profil terpenuhi."}</p>
      <div className="decision-numbers">
        <div><span>Median move setelah biaya · {proposal.reference_lot.toFixed(1)} lot</span><strong>{money(proposal.median_move_after_cost_usd, 2, proposal.pnl_currency || state.snapshot.account.currency)}</strong></div>
        <div>
          <span>Entry / reward / risk</span>
          <strong>
            {proposal.reference_entry_price === null
              ? "—"
              : `${proposal.reference_entry_price.toFixed(2)} · ${money(proposal.remaining_reward_usd, 2, proposal.pnl_currency || state.snapshot.account.currency)} / ${money(proposal.remaining_risk_usd, 2, proposal.pnl_currency || state.snapshot.account.currency)}`}
          </strong>
        </div>
        <div><span>Reward / risk tersisa</span><strong>{proposal.reward_risk_ratio.toFixed(2)}×</strong></div>
        <div>
          <span>Model MFE / MAE · {forecastSide}</span>
          <strong>
            {state.forecast.excursion_modelled
              ? `${expectedMfe.toFixed(2)} / ${expectedMae.toFixed(2)}`
              : "Belum tersedia"}
          </strong>
        </div>
        <div>
          <span>Barrier TP / SL · {forecastSide}</span>
          <strong>{barrierTarget.toFixed(2)} / {barrierStop.toFixed(2)}</strong>
        </div>
        <div>
          <span>Cost model · {proposal.cost_model_id}</span>
          <strong>
            spread {proposal.entry_spread.toFixed(2)} → {proposal.expected_exit_spread.toFixed(2)}
            {" · "}slip {proposal.slippage_assumption.toFixed(2)}
          </strong>
        </div>
      </div>
      <ul className="reason-list">
        {proposal.reason_codes.map((reason) => <li key={reason}>{reasonLabel(reason)}</li>)}
      </ul>
    </section>
  );

  return (
    <main className="terminal-shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">XP</span>
          <div>
            <strong>XPDE</strong>
            <span>GOLDm# probabilistic terminal</span>
          </div>
        </div>
        <div className="topbar-center">
          <span className="symbol">{state.snapshot.symbol}</span>
          <span className="timeframe">{state.snapshot.timeframe}</span>
          <span className="quote">
            {state.snapshot.bid.toFixed(2)}
            <i />
            {state.snapshot.ask.toFixed(2)}
          </span>
        </div>
        <div className="connection">
          <span className={`pulse ${coreApiConnected ? "connected" : "fallback"}`} />
          <div>
            <strong>{coreApiConnected ? "Core API tersambung" : "Core API tidak tersambung"}</strong>
            <span>
              WebSocket {transport === "connected" ? "live" : "reconnecting"}
              {" · "}{state.snapshot.data_quality.market_status.replaceAll("_", " ")}
            </span>
          </div>
          <button
            className="retry-button"
            disabled={retrying}
            onClick={retryRealtime}
            type="button"
          >
            <i aria-hidden="true">↻</i>
            {retrying ? "Retrying…" : "Retry realtime"}
          </button>
        </div>
      </header>

      <section className="safety-strip">
        <span className="safe-badge">SHADOW MODE</span>
        <span className={`data-badge ${state.safety.feed_is_demo ? "demo" : "live"}`}>
          {state.safety.feed_is_demo ? "DEMO DATA" : "MT5 LIVE"}
        </span>
        <span className={`data-badge ${state.snapshot.data_quality.market_status === "OPEN" ? "live" : "demo"}`}>
          MARKET {state.snapshot.data_quality.market_status.replaceAll("_", " ")}
        </span>
        <span className={`data-badge ${state.forecast_status === "CURRENT" ? "live" : "demo"}`}>
          FORECAST {state.forecast_status.replaceAll("_", " ")}
        </span>
        <p>Auto-trading nonaktif. Semua proposal membutuhkan verifikasi dan keputusan manusia.</p>
        {retryStatus ? <span className="retry-status" aria-live="polite">{retryStatus}</span> : null}
        <span>UTC {time(state.updated_at)}</span>
      </section>

      <div className="workspace">
        <div className="mobile-decision">{renderDecisionCard()}</div>
        <section className="primary-column">
          <div className={`panel chart-panel ${forecastIsStale ? "forecast-stale" : ""}`}>
            <div className="panel-heading">
              <div>
                <span className="eyebrow">Market + forecast envelope</span>
                <h1>Ketidakpastian terlihat, keputusan tetap milik Anda.</h1>
              </div>
              <div className="profile-switch" aria-label="Pilih profil trading">
                {(["SCALPER", "SNIPER"] as Profile[]).map((item) => (
                  <button
                    className={profile === item ? "active" : ""}
                    key={item}
                    onClick={() => setProfile(item)}
                    type="button"
                  >
                    {item}
                  </button>
                ))}
              </div>
            </div>

            <div className="chart-meta">
              <div><span>Last live</span><strong>{lastPrice.toFixed(2)}</strong></div>
              <div><span>Spread aktual</span><strong>{spread.toFixed(2)} {state.snapshot.symbol_spec.quote_currency}</strong></div>
              <div><span>Keputusan valid</span><strong>{time(proposal.decision_valid_until)} UTC</strong></div>
              <div><span>Outcome matang</span><strong>{time(proposal.outcome_matures_at)} UTC</strong></div>
              <div><span>Model</span><strong>{state.forecast.model_id}</strong></div>
            </div>

            <div className="market-chart" aria-label="Grafik candle historis dan prediction band">
              {forecastIsStale ? (
                <div className="stale-forecast-overlay" role="status">
                  <strong>FORECAST HISTORIS / STALE</strong>
                  <span>Tidak boleh digunakan untuk keputusan entry.</span>
                </div>
              ) : null}
              <div className="price-axis">
                <span>{maxPrice.toFixed(2)}</span>
                <span>{((maxPrice + minPrice) / 2).toFixed(2)}</span>
                <span>{minPrice.toFixed(2)}</span>
              </div>
              <div className="grid-lines" aria-hidden="true"><i /><i /><i /><i /></div>
              <div className="candles">
                {lastBars.map((bar, index) => {
                  const top = ((maxPrice - bar.high) / range) * 100;
                  const bottom = ((bar.low - minPrice) / range) * 100;
                  const bodyTop = ((maxPrice - Math.max(bar.open, bar.close)) / range) * 100;
                  const bodyHeight = (Math.abs(bar.close - bar.open) / range) * 100;
                  return (
                    <div
                      className={`candle ${bar.close >= bar.open ? "up" : "down"} ${
                        state.snapshot.current_bar && index === lastBars.length - 1
                          ? "live"
                          : ""
                      }`}
                      key={`${bar.timestamp}-${index}`}
                    >
                      <i style={{ top: `${top}%`, bottom: `${bottom}%` }} />
                      <b style={{ top: `${bodyTop}%`, height: `${Math.max(bodyHeight, 1.5)}%` }} />
                    </div>
                  );
                })}
              </div>
              <div className="forecast-zone">
                <span className="forecast-label">FORECAST</span>
                <div
                  className="barrier-reference target"
                  style={{ top: `${((maxPrice - barrierTarget) / range) * 100}%` }}
                >
                  <span>TP {barrierTarget.toFixed(2)}</span>
                </div>
                <div
                  className="barrier-reference stop"
                  style={{ top: `${((maxPrice - barrierStop) / range) * 100}%` }}
                >
                  <span>SL {barrierStop.toFixed(2)}</span>
                </div>
                {state.forecast.points.map((point) => {
                  const upper = state.forecast.origin_close * Math.exp(point.q90);
                  const innerUpper = state.forecast.origin_close * Math.exp(point.q75);
                  const median = state.forecast.origin_close * Math.exp(point.q50);
                  const innerLower = state.forecast.origin_close * Math.exp(point.q25);
                  const lower = state.forecast.origin_close * Math.exp(point.q10);
                  const top = ((maxPrice - upper) / range) * 100;
                  const bandHeight = ((upper - lower) / range) * 100;
                  const innerTop = ((maxPrice - innerUpper) / range) * 100;
                  const innerHeight = ((innerUpper - innerLower) / range) * 100;
                  const medianTop = ((maxPrice - median) / range) * 100;
                  return (
                    <div
                      className="forecast-step"
                      key={point.horizon_bars}
                      style={{
                        left: `${(point.horizon_bars / maxForecastHorizon) * 88}%`,
                        width: "12%",
                      }}
                    >
                      <i className="band-80" style={{ top: `${top}%`, height: `${Math.max(1, bandHeight)}%` }} />
                      <i className="band-50" style={{ top: `${innerTop}%`, height: `${Math.max(1, innerHeight)}%` }} />
                      <b className="median" style={{ top: `${medianTop}%` }} />
                      <span>+{point.horizon_bars}</span>
                    </div>
                  );
                })}
              </div>
            </div>
            <div className="chart-legend">
              <span><i className="legend-up" /> Bull candle</span>
              <span><i className="legend-down" /> Bear candle</span>
              <span><i className="legend-band" /> 80% interval</span>
              <span><i className="legend-inner-band" /> 50% interval</span>
              <span><i className="legend-median" /> Median forecast</span>
              <span><i className="legend-live" /> Live M5</span>
            </div>
          </div>

          <div className="metric-grid">
            <article className="panel metric direction-metric">
              <span>Peluang arah dalam 3 bar / 15 menit</span>
              <div className="direction-values">
                <div className="direction-stat up" aria-label={`Probabilitas naik ${probabilityUpPercent.toFixed(1)}%`}>
                  <i aria-hidden="true">↑</i>
                  <strong>{forecastIsStale ? "—" : `${probabilityUpPercent.toFixed(1)}%`}</strong>
                </div>
                <div className="direction-stat down" aria-label={`Probabilitas turun ${probabilityDownPercent.toFixed(1)}%`}>
                  <i aria-hidden="true">↓</i>
                  <strong>{forecastIsStale ? "—" : `${probabilityDownPercent.toFixed(1)}%`}</strong>
                </div>
              </div>
              <div
                className="direction-meter"
                aria-label={`${probabilityUpPercent.toFixed(1)}% naik, ${probabilityDownPercent.toFixed(1)}% turun`}
                role="img"
              >
                <i className="up" style={{ width: forecastIsStale ? "0%" : `${probabilityUpPercent}%` }} />
                <i className="down" style={{ width: forecastIsStale ? "0%" : `${probabilityDownPercent}%` }} />
              </div>
              <small className="direction-summary">{forecastIsStale ? "Forecast stale — bukan sinyal" : directionSummary}</small>
            </article>
            <article className="panel metric">
              <span>TP before invalidation · {forecastSide} · 3 bar</span>
              <strong>{forecastIsStale ? "—" : percent(barrierProbability)}</strong>
              <div className="meter amber"><i style={{ width: forecastIsStale ? "0%" : percent(barrierProbability) }} /></div>
              <small>{forecastIsStale ? "Forecast stale — bukan sinyal" : "Gate Sniper 60% · sisi harus konsisten"}</small>
            </article>
            <article className="panel metric">
              <span>Offline holdout coverage</span>
              <strong>{forecastIsStale ? "—" : percent(state.forecast.calibration.observed_coverage)}</strong>
              <div className="meter coverage"><i style={{ width: forecastIsStale ? "0%" : percent(state.forecast.calibration.observed_coverage) }} /></div>
              <small>
                {forecastIsStale ? "Forecast stale — artifact tidak aktif" : `Artifact evaluation · target 80% · n=${state.forecast.calibration.sample_size}`}
              </small>
            </article>
            <article className="panel metric">
              <span>Live coverage · 200 prediksi terakhir</span>
              <strong>{liveCoverage === null ? "—" : percent(liveCoverage)}</strong>
              <div className="meter coverage">
                <i style={{ width: liveCoverage === null ? "0%" : percent(liveCoverage) }} />
              </div>
              <small>
                {activeEvaluation
                  ? liveCoverage === null
                    ? `Mengumpulkan evidence · ${activeEvaluation.settled_predictions}/${MIN_LIVE_EVIDENCE}`
                    : `Live H3 settled n=${activeEvaluation.settled_predictions}`
                  : "Menunggu evaluation API"}
              </small>
            </article>
            <article className="panel metric">
              <span>Current runtime session · direction</span>
              <strong>
                {sessionDirectionAccuracy === null
                  ? "—"
                  : percent(sessionDirectionAccuracy)}
              </strong>
              <div className="meter coverage">
                <i
                  style={{
                    width:
                      sessionDirectionAccuracy === null
                        ? "0%"
                        : percent(sessionDirectionAccuracy),
                  }}
                />
              </div>
              <small>
                {sessionEvaluation
                  ? sessionDirectionAccuracy === null
                    ? `Mengumpulkan evidence · ${sessionEvaluation.settled_predictions}/${MIN_LIVE_EVIDENCE}`
                    : `Prediction sejak core start · n=${sessionEvaluation.settled_predictions}`
                  : "Menunggu evaluation API"}
              </small>
            </article>
            <article className="panel metric">
              <span>Proposal TP dalam horizon · {profile} · 200 prediksi</span>
              <strong>{realizedTpRate === null ? "—" : percent(realizedTpRate)}</strong>
              <div className="meter realized">
                <i style={{ width: realizedTpRate === null ? "0%" : percent(realizedTpRate) }} />
              </div>
              <small>
                {activeProposalEvaluation
                  ? realizedTpRate === null
                    ? `Mengumpulkan outcome valid · ${activeProposalEvaluation.tp_first_within_horizon_samples}/${MIN_LIVE_EVIDENCE}`
                    : `TP/horizon n=${activeProposalEvaluation.tp_first_within_horizon_samples} · conditional TP/SL=${activeProposalEvaluation.tp_vs_sl_conditional_rate === null ? "—" : percent(activeProposalEvaluation.tp_vs_sl_conditional_rate)} · ambigu=${activeProposalEvaluation.ambiguous_samples}`
                  : "Menunggu evaluation API"}
              </small>
            </article>
          </div>
        </section>

        <aside className="side-column">
          <div className="desktop-decision">{renderDecisionCard()}</div>

          <section className="panel account-panel">
            <div className="section-title">
              <div><span className="eyebrow">Account-aware policy</span><h2>Risk preview</h2></div>
              <span className="leverage">1:{state.snapshot.account.leverage}</span>
            </div>
            <div className="account-values">
              <div><span>Balance</span><strong>{money(state.snapshot.account.balance, 2, state.snapshot.account.currency)}</strong></div>
              <div><span>Equity</span><strong>{money(state.snapshot.account.equity, 2, state.snapshot.account.currency)}</strong></div>
              <div><span>Free margin</span><strong>{money(state.snapshot.account.free_margin, 2, state.snapshot.account.currency)}</strong></div>
            </div>
            <label className="risk-input">
              <span>Risk budget <strong>{riskPercent.toFixed(1)}%</strong></span>
              <input aria-label="Risk budget percent" disabled={!state.forecast.excursion_modelled || state.forecast_status !== "CURRENT" || (proposal.action !== "LONG" && proposal.action !== "SHORT")} max="3" min="0.1" onChange={(event) => setRiskPercent(Number(event.target.value))} step="0.1" type="range" value={riskPercent} />
            </label>
            <div className="lot-preview">
              <span>Lot indikatif</span><strong>{riskPreview.lot === null ? "—" : riskPreview.lot.toFixed(1)}</strong>
              <small>
                {riskPreview.lot === null
                  ? riskPreview.reason
                  : "Berdasarkan barrier stop, tick value, dan margin; verifikasi manual tetap wajib."}
              </small>
            </div>
          </section>

          <section className="panel quality-panel">
            <div className="section-title">
              <div><span className="eyebrow">System integrity</span><h2>Quality gates</h2></div>
              <span className="quality-score">{percent(state.snapshot.data_quality.completeness)}</span>
            </div>
            <div className="gate-row"><span><i className="ok" /> Feed completeness</span><strong>{percent(state.snapshot.data_quality.completeness)}</strong></div>
            <div className="gate-row"><span><i className={coverageHealthy ? "ok" : "warn"} /> Calibration</span><strong>{coverageHealthy ? "In range" : "Out of range"}</strong></div>
            <div className="gate-row"><span><i className={state.forecast.drift_detected ? "bad" : "ok"} /> Drift detector</span><strong>{state.forecast.drift_detected ? "Detected" : "Clear"}</strong></div>
            <div className="gate-row"><span><i className="warn" /> Live direction Brier</span><strong>{activeEvaluation?.direction_brier?.toFixed(4) ?? "—"}</strong></div>
            <div className="gate-row"><span><i className="warn" /> Barrier calibration ECE</span><strong>{evaluation?.barrier_expected_calibration_error === null || evaluation?.barrier_expected_calibration_error === undefined ? "—" : percent(evaluation.barrier_expected_calibration_error)}</strong></div>
            <div className="gate-row">
              <span>
                <i className={state.model_health.status === "HEALTHY" ? "ok" : state.model_health.status === "WARMING_UP" ? "warn" : "bad"} />
                Live model health
              </span>
              <strong>{state.model_health.status.replaceAll("_", " ")}</strong>
            </div>
            <div className="gate-row"><span><i className={state.snapshot.symbol_spec.chart_mode === "BID" ? "ok" : "bad"} /> Executable bars</span><strong>{state.snapshot.symbol_spec.chart_mode} · Bid/Ask</strong></div>
            <div className="gate-row"><span><i className={state.snapshot.data_quality.absolute_tick_age_ms <= 10_000 ? "ok" : "bad"} /> Absolute tick age</span><strong>{state.snapshot.data_quality.absolute_tick_age_ms} ms</strong></div>
            <div className="gate-row"><span><i className="warn" /> Data source</span><strong>{state.safety.feed_is_demo ? "Demo" : "MT5 live"}</strong></div>
          </section>

          <section className="panel feedback-panel">
            <span className="eyebrow">Human verification</span>
            <h2>Apakah proposal ini layak?</h2>
            <div className="feedback-actions">
              <button disabled={state.safety.feed_is_demo || state.forecast_status !== "CURRENT"} type="button" onClick={() => submitFeedback("ACCEPTED")}>Accept</button>
              <button disabled={state.safety.feed_is_demo || state.forecast_status !== "CURRENT"} type="button" onClick={() => submitFeedback("UNCERTAIN")}>Unsure</button>
              <button disabled={state.safety.feed_is_demo || state.forecast_status !== "CURRENT"} type="button" onClick={() => submitFeedback("REJECTED")}>Reject</button>
            </div>
            <small>{feedbackStatus || "Feedback tidak mengubah label harga objektif."}</small>
          </section>
        </aside>
      </div>

      <footer>
        <span>{state.forecast.feature_version} · Prediction {state.forecast.prediction_id.slice(0, 8)}</span>
        <strong>Decision support only · no order execution</strong>
      </footer>
    </main>
  );
}
