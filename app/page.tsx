"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type Profile = "SCALPER" | "SNIPER";
type DecisionAction = "LONG" | "SHORT" | "WAIT" | "NO_PREDICTION";

interface MarketBar {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  tick_volume: number;
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
  action: DecisionAction;
  generated_at: string;
  expires_at: string;
  expected_edge_after_cost_usd: number;
  reference_lot: number;
  invalidation_price: number | null;
  reason_codes: string[];
  risk_warnings: string[];
}

interface DashboardState {
  mode: string;
  connection_status: string;
  updated_at: string;
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
    };
    data_quality: {
      completeness: number;
      tick_age_ms: number;
      missing_flags: string[];
      reason_codes: string[];
    };
  };
  forecast: {
    prediction_id: string;
    model_id: string;
    feature_version: string;
    generated_at: string;
    direction_probability_up: number;
    barrier_probability: number;
    expected_mfe_usd: number;
    expected_mae_usd: number;
    calibration: {
      target_coverage: number;
      observed_coverage: number;
      sample_size: number;
    };
    drift_detected: boolean;
    points: ForecastPoint[];
  };
  proposals: Proposal[];
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
}

interface EvaluationSummary {
  overall: EvaluationMetrics;
  by_model: Array<EvaluationMetrics & { model_id: string }>;
  target_coverage: number;
  updated_at: string;
}

const API_BASE = "http://127.0.0.1:8787";
const DEMO_REFERENCE_MS = Date.parse("2026-01-01T00:00:00.000Z");

function buildDemoState(): DashboardState {
  // This fixture is rendered on both the server and the browser during
  // hydration. Keep every value deterministic; live state replaces it after mount.
  const baseTime = DEMO_REFERENCE_MS - 47 * 5 * 60_000;
  let price = 3331.45;
  const bars = Array.from({ length: 48 }, (_, index) => {
    const open = price;
    const close = 3331.3 + index * 0.018 + Math.sin(index / 3.6) * 0.56;
    price = close;
    return {
      timestamp: new Date(baseTime + index * 5 * 60_000).toISOString(),
      open,
      high: Math.max(open, close) + 0.3 + (index % 3) * 0.05,
      low: Math.min(open, close) - 0.28 - (index % 2) * 0.04,
      close,
      tick_volume: 155 + (index % 8) * 12,
    };
  });
  const predictionId = "demo-shadow-prediction";
  const now = new Date(DEMO_REFERENCE_MS).toISOString();
  const expiresAt = new Date(DEMO_REFERENCE_MS + 15 * 60_000).toISOString();
  return {
    mode: "DEMO_SHADOW",
    connection_status: "WAITING_FOR_MT5",
    updated_at: now,
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
      },
      data_quality: {
        completeness: 1,
        tick_age_ms: 0,
        missing_flags: [],
        reason_codes: ["DEMO_DATA"],
      },
    },
    forecast: {
      prediction_id: predictionId,
      model_id: "baseline-demo-v1",
      feature_version: "goldm-m5-v1",
      generated_at: now,
      direction_probability_up: 0.57,
      barrier_probability: 0.54,
      expected_mfe_usd: 1.42,
      expected_mae_usd: 0.91,
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
        action: "WAIT",
        generated_at: now,
        expires_at: expiresAt,
        expected_edge_after_cost_usd: -0.01,
        reference_lot: 0.1,
        invalidation_price: null,
        reason_codes: ["EDGE_TOO_SMALL_AFTER_COST"],
        risk_warnings: ["HIGH_LEVERAGE_ACCOUNT", "MANUAL_CONFIRMATION_REQUIRED"],
      },
      {
        prediction_id: predictionId,
        profile: "SNIPER",
        action: "WAIT",
        generated_at: now,
        expires_at: expiresAt,
        expected_edge_after_cost_usd: -0.01,
        reference_lot: 0.1,
        invalidation_price: null,
        reason_codes: [
          "EDGE_TOO_SMALL_AFTER_COST",
          "DIRECTION_PROBABILITY_TOO_LOW",
          "BARRIER_PROBABILITY_TOO_LOW",
        ],
        risk_warnings: ["HIGH_LEVERAGE_ACCOUNT", "MANUAL_CONFIRMATION_REQUIRED"],
      },
    ],
    safety: {
      auto_trading_enabled: false,
      human_confirmation_required: true,
      feed_is_demo: true,
    },
  };
}

function money(value: number, digits = 2) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value);
}

function percent(value: number, digits = 1) {
  return `${(value * 100).toFixed(digits)}%`;
}

function time(value: string) {
  return new Intl.DateTimeFormat("id-ID", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "UTC",
  }).format(new Date(value));
}

function reasonLabel(reason: string) {
  const labels: Record<string, string> = {
    EDGE_TOO_SMALL_AFTER_COST: "Edge terlalu kecil setelah biaya",
    DIRECTION_PROBABILITY_TOO_LOW: "Probabilitas arah belum melewati gate",
    BARRIER_PROBABILITY_TOO_LOW: "Peluang target belum memadai",
    CALIBRATION_OUTSIDE_GATE: "Coverage di luar toleransi",
    DATA_INVALID_OR_STALE: "Feed tidak valid atau stale",
    SPREAD_ABOVE_LIMIT: "Spread melewati batas profil",
    DRIFT_DETECTED: "Drift model terdeteksi",
  };
  return labels[reason] ?? reason.replaceAll("_", " ").toLowerCase();
}

export default function Home() {
  const [state, setState] = useState<DashboardState>(() => buildDemoState());
  const [profile, setProfile] = useState<Profile>("SCALPER");
  const [transport, setTransport] = useState<"connected" | "fallback">("fallback");
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
      setTransport("connected");
    } catch {
      setTransport("fallback");
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
    const timer = window.setInterval(loadState, 3000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [loadState]);

  useEffect(() => {
    const initial = window.setTimeout(loadEvaluation, 0);
    const timer = window.setInterval(loadEvaluation, 15_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [loadEvaluation]);

  useEffect(() => {
    const socket = new WebSocket("ws://127.0.0.1:8787/ws");
    socket.addEventListener("message", (event) => {
      try {
        setState(JSON.parse(event.data) as DashboardState);
        setTransport("connected");
      } catch {
        setTransport("fallback");
      }
    });
    socket.addEventListener("close", () => setTransport("fallback"));
    socket.addEventListener("error", () => setTransport("fallback"));
    return () => socket.close();
  }, []);

  const proposal = state.proposals.find((item) => item.profile === profile) ?? state.proposals[0];
  const activeEvaluation =
    evaluation?.by_model.find((item) => item.model_id === state.forecast.model_id) ??
    evaluation?.overall;
  const realizedTpRate = activeEvaluation?.tp_before_sl_rate ?? null;
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
  const completedBars = state.snapshot.bars.slice(
    state.snapshot.current_bar ? -31 : -32,
  );
  const lastBars = state.snapshot.current_bar
    ? [...completedBars, state.snapshot.current_bar]
    : completedBars;
  const prices = lastBars.flatMap((bar) => [bar.high, bar.low]);
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const range = Math.max(maxPrice - minPrice, 0.01);
  const lastPrice = (state.snapshot.bid + state.snapshot.ask) / 2;

  const suggestedLot = useMemo(() => {
    const riskUsd = state.snapshot.account.equity * (riskPercent / 100);
    const stopDistance = Math.max(state.forecast.expected_mae_usd, spread * 1.5);
    const raw = riskUsd / (stopDistance * state.snapshot.symbol_spec.contract_size);
    const step = state.snapshot.symbol_spec.volume_step;
    const rounded = Math.floor(raw / step) * step;
    return Math.min(
      state.snapshot.symbol_spec.volume_max,
      Math.max(state.snapshot.symbol_spec.volume_min, rounded),
    );
  }, [riskPercent, spread, state]);

  async function submitFeedback(verdict: "ACCEPTED" | "REJECTED" | "UNCERTAIN") {
    setFeedbackStatus("Menyimpan…");
    try {
      const response = await fetch(`${API_BASE}/api/v1/feedback`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          prediction_id: state.forecast.prediction_id,
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
        const stateAgeMs = Date.now() - Date.parse(nextState.updated_at);
        if (
          nextState.connection_status === "MT5_CONNECTED" &&
          stateAgeMs < 15_000
        ) {
          setTransport("connected");
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
          <span className={`pulse ${transport}`} />
          <div>
            <strong>{transport === "connected" ? "Core tersambung" : "Demo lokal"}</strong>
            <span>{state.connection_status.replaceAll("_", " ")}</span>
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
        <p>Auto-trading nonaktif. Semua proposal membutuhkan verifikasi dan keputusan manusia.</p>
        {retryStatus ? <span className="retry-status" aria-live="polite">{retryStatus}</span> : null}
        <span>UTC {time(state.updated_at)}</span>
      </section>

      <div className="workspace">
        <section className="primary-column">
          <div className="panel chart-panel">
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
              <div><span>Spread aktual</span><strong>{spread.toFixed(2)} USD</strong></div>
              <div><span>Expiry</span><strong>{time(proposal.expires_at)} UTC</strong></div>
              <div><span>Model</span><strong>{state.forecast.model_id}</strong></div>
            </div>

            <div className="market-chart" aria-label="Grafik candle historis dan prediction band">
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
                {state.forecast.points.map((point, index) => {
                  const upper = lastPrice * (1 + point.q90);
                  const lower = lastPrice * (1 + point.q10);
                  const median = lastPrice * (1 + point.q50);
                  const top = ((maxPrice - upper) / range) * 100;
                  const bandHeight = ((upper - lower) / range) * 100;
                  const medianTop = ((maxPrice - median) / range) * 100;
                  return (
                    <div className="forecast-step" key={point.horizon_bars} style={{ left: `${index * 24}%`, width: "25%" }}>
                      <i className="band-80" style={{ top: `${Math.max(2, top)}%`, height: `${Math.min(94, Math.max(5, bandHeight))}%` }} />
                      <b className="median" style={{ top: `${Math.min(96, Math.max(2, medianTop))}%` }} />
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
              <span><i className="legend-median" /> Median forecast</span>
              <span><i className="legend-live" /> Live M5</span>
            </div>
          </div>

          <div className="metric-grid">
            <article className="panel metric direction-metric">
              <span>Probabilitas arah</span>
              <div className="direction-values">
                <div className="direction-stat up" aria-label={`Probabilitas naik ${probabilityUpPercent.toFixed(1)}%`}>
                  <i aria-hidden="true">↑</i>
                  <strong>{probabilityUpPercent.toFixed(1)}%</strong>
                </div>
                <div className="direction-stat down" aria-label={`Probabilitas turun ${probabilityDownPercent.toFixed(1)}%`}>
                  <i aria-hidden="true">↓</i>
                  <strong>{probabilityDownPercent.toFixed(1)}%</strong>
                </div>
              </div>
              <div
                className="direction-meter"
                aria-label={`${probabilityUpPercent.toFixed(1)}% naik, ${probabilityDownPercent.toFixed(1)}% turun`}
                role="img"
              >
                <i className="up" style={{ width: `${probabilityUpPercent}%` }} />
                <i className="down" style={{ width: `${probabilityDownPercent}%` }} />
              </div>
              <small className="direction-summary">{directionSummary}</small>
            </article>
            <article className="panel metric">
              <span>TP before invalidation</span>
              <strong>{percent(state.forecast.barrier_probability)}</strong>
              <div className="meter amber"><i style={{ width: percent(state.forecast.barrier_probability) }} /></div>
              <small>Belum melewati gate Sniper 60%</small>
            </article>
            <article className="panel metric">
              <span>Observed coverage</span>
              <strong>{percent(state.forecast.calibration.observed_coverage)}</strong>
              <div className="meter coverage"><i style={{ width: percent(state.forecast.calibration.observed_coverage) }} /></div>
              <small>Target 80% · n={state.forecast.calibration.sample_size}</small>
            </article>
            <article className="panel metric">
              <span>Realized TP before SL</span>
              <strong>{realizedTpRate === null ? "—" : percent(realizedTpRate)}</strong>
              <div className="meter realized">
                <i style={{ width: realizedTpRate === null ? "0%" : percent(realizedTpRate) }} />
              </div>
              <small>
                {activeEvaluation
                  ? `Outcome valid n=${activeEvaluation.tp_before_sl_samples} · settled=${activeEvaluation.settled_predictions}`
                  : "Menunggu evaluation API"}
              </small>
            </article>
          </div>
        </section>

        <aside className="side-column">
          <section className={`panel decision-card action-${proposal.action.toLowerCase()}`}>
            <div className="decision-head"><span>Decision proposal</span><i>{profile}</i></div>
            <strong className="decision-action">{proposal.action.replace("_", " ")}</strong>
            <p>{proposal.reason_codes.length ? reasonLabel(proposal.reason_codes[0]) : "Semua gate profil terpenuhi."}</p>
            <div className="decision-numbers">
              <div><span>Net edge · {proposal.reference_lot.toFixed(1)} lot</span><strong>{money(proposal.expected_edge_after_cost_usd)}</strong></div>
              <div><span>Expected MFE / MAE</span><strong>{state.forecast.expected_mfe_usd.toFixed(2)} / {state.forecast.expected_mae_usd.toFixed(2)}</strong></div>
            </div>
            <ul className="reason-list">
              {proposal.reason_codes.map((reason) => <li key={reason}>{reasonLabel(reason)}</li>)}
            </ul>
          </section>

          <section className="panel account-panel">
            <div className="section-title">
              <div><span className="eyebrow">Account-aware policy</span><h2>Risk preview</h2></div>
              <span className="leverage">1:{state.snapshot.account.leverage}</span>
            </div>
            <div className="account-values">
              <div><span>Balance</span><strong>{money(state.snapshot.account.balance)}</strong></div>
              <div><span>Equity</span><strong>{money(state.snapshot.account.equity)}</strong></div>
              <div><span>Free margin</span><strong>{money(state.snapshot.account.free_margin)}</strong></div>
            </div>
            <label className="risk-input">
              <span>Risk budget <strong>{riskPercent.toFixed(1)}%</strong></span>
              <input aria-label="Risk budget percent" max="3" min="0.1" onChange={(event) => setRiskPercent(Number(event.target.value))} step="0.1" type="range" value={riskPercent} />
            </label>
            <div className="lot-preview">
              <span>Lot indikatif</span><strong>{suggestedLot.toFixed(1)}</strong>
              <small>Berdasarkan MAE forecast; verifikasi manual tetap wajib.</small>
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
            <div className="gate-row"><span><i className="warn" /> Data source</span><strong>{state.safety.feed_is_demo ? "Demo" : "MT5 live"}</strong></div>
          </section>

          <section className="panel feedback-panel">
            <span className="eyebrow">Human verification</span>
            <h2>Apakah proposal ini layak?</h2>
            <div className="feedback-actions">
              <button type="button" onClick={() => submitFeedback("ACCEPTED")}>Accept</button>
              <button type="button" onClick={() => submitFeedback("UNCERTAIN")}>Unsure</button>
              <button type="button" onClick={() => submitFeedback("REJECTED")}>Reject</button>
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
