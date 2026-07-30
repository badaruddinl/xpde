"""TechnicalAnalysisPacket builder."""

from __future__ import annotations

import math
from typing import Any

from ..contracts import (
    DECISION_AUTHORITY,
    FORECAST_AUTHORITY,
    INTERPRETATION_CONSTRAINTS,
    PACKET_VERSION,
    SEMANTIC_CONTRACT,
    TA_CONTRACT_ID,
)
from ..errors import ContractError
from .alignment import align_forecast
from .indicators import (
    classify_momentum,
    classify_trend,
    indicator_snapshot,
)
from .levels import nearest_levels
from .resample import aggregation_diagnostics, resample_completed_m5
from .structure import market_structure, swing_points
from .types import Bar, normalize_m5_bars

_PROFILE_TICK_AGE_LIMIT = {"SCALPER": 10_000, "SNIPER": 5_000}


def _finite_float(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ContractError(f"{field} must be numeric") from error
    if not math.isfinite(result):
        raise ContractError(f"{field} must be finite")
    return result


def market_data_current(state: dict[str, Any], profile: str) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    snapshot = state.get("snapshot", {})
    quality = snapshot.get("data_quality", {})
    if state.get("forecast_status") != "CURRENT":
        reasons.append(f"FORECAST_{state.get('forecast_status', 'UNKNOWN')}")
    if state.get("connection_status") != "MT5_CONNECTED":
        reasons.append(str(state.get("connection_status", "CONNECTION_UNKNOWN")))
    if quality.get("market_status") != "OPEN":
        reasons.append(f"MARKET_{quality.get('market_status', 'UNKNOWN')}")
    missing = quality.get("missing_flags", [])
    if not isinstance(missing, list):
        missing = ["INVALID_MISSING_FLAGS"]
    reasons.extend(str(value) for value in missing)
    maximum = _PROFILE_TICK_AGE_LIMIT[profile]
    for field in ("tick_age_ms", "absolute_tick_age_ms", "transport_tick_age_ms"):
        value = quality.get(field)
        if not isinstance(value, (int, float)) or value > maximum:
            reasons.append(f"{field.upper()}_STALE")
    return not reasons, sorted(set(reasons))


def normalize_forecast(forecast: dict[str, Any]) -> dict[str, Any]:
    origin = _finite_float(forecast.get("origin_close"), "origin_close")
    probability_up = _finite_float(
        forecast.get("direction_probability_up"), "direction_probability_up"
    )
    if not 0.0 <= probability_up <= 1.0:
        raise ContractError("direction_probability_up must be in [0, 1]")
    points = []
    h3_q50: float | None = None
    for point in forecast.get("points", []):
        horizon = int(point["horizon_bars"])
        quantiles_log_return = {
            key: _finite_float(point.get(key), f"{key} H{horizon}")
            for key in ("q10", "q25", "q50", "q75", "q90")
        }
        values = list(quantiles_log_return.values())
        if values != sorted(values):
            raise ContractError(f"forecast quantiles cross at H{horizon}")
        quantiles_bid_price = {
            key: origin * math.exp(value) for key, value in quantiles_log_return.items()
        }
        if horizon == 3:
            h3_q50 = quantiles_log_return["q50"]
        points.append(
            {
                "horizon_bars": horizon,
                "horizon_minutes": horizon * 5,
                "quantiles_log_return": quantiles_log_return,
                "quantiles_bid_price": quantiles_bid_price,
            }
        )
    if h3_q50 is None:
        raise ContractError("forecast has no H3 point")
    h3_side = "BULLISH" if h3_q50 > 0.0 else "BEARISH" if h3_q50 < 0.0 else "FLAT"
    return {
        "prediction_id": forecast.get("prediction_id"),
        "model_id": forecast.get("model_id"),
        "feature_version": forecast.get("feature_version"),
        "label_contract_id": forecast.get("label_contract_id"),
        "origin_bar_timestamp": forecast.get("origin_bar_timestamp"),
        "origin_close_bid": origin,
        "generated_at": forecast.get("generated_at"),
        "probability_reference": forecast.get(
            "probability_reference", "FORECAST_ORIGIN"
        ),
        "entry_conditioned_probability": bool(
            forecast.get("entry_conditioned_probability", False)
        ),
        "direction": {
            "probability_up": probability_up,
            "probability_non_up": 1.0 - probability_up,
            "non_up_definition": "negative or flat; not strict P(DOWN)",
            "h3_q50_side": h3_side,
        },
        "barrier": {
            "horizon_bars": forecast.get("barrier_horizon_bars"),
            "probability_long_origin": forecast.get("barrier_probability_long"),
            "probability_short_origin": forecast.get("barrier_probability_short"),
            "target_price_long_bid_exit": forecast.get("target_price_long"),
            "stop_price_long_bid_exit": forecast.get("stop_price_long"),
            "target_price_short_ask_exit": forecast.get("target_price_short"),
            "stop_price_short_ask_exit": forecast.get("stop_price_short"),
            "barrier_spec_id": forecast.get("barrier_spec_id"),
        },
        "origin_horizon_excursion": {
            "mfe_long_price_distance": forecast.get("expected_mfe_long"),
            "mae_long_price_distance": forecast.get("expected_mae_long"),
            "mfe_short_price_distance": forecast.get("expected_mfe_short"),
            "mae_short_price_distance": forecast.get("expected_mae_short"),
            "modelled": forecast.get("excursion_modelled"),
        },
        "quantile_points": points,
        "calibration": forecast.get("calibration"),
        "drift_detected": forecast.get("drift_detected"),
    }


def build_technical_view(
    native_bars: list[Bar],
    timeframe: str,
    *,
    swing_limit: int = 12,
    maximum_bars: int | None = None,
) -> dict[str, Any]:
    bars = resample_completed_m5(native_bars, timeframe)
    diagnostics = aggregation_diagnostics(native_bars, bars, timeframe)
    if maximum_bars is not None:
        bars = bars[-maximum_bars:]
    if not bars:
        return {
            "timeframe": timeframe,
            "bar_count": 0,
            "trend": {"state": "INSUFFICIENT_DATA"},
            "momentum": {"state": "INSUFFICIENT_DATA"},
            "structure": {"state": "INSUFFICIENT_DATA"},
            "levels": {
                "nearest_support": None,
                "nearest_resistance": None,
            },
            "aggregation": diagnostics,
            "limitations": ["No complete bars are available."],
        }
    indicators = indicator_snapshot(bars)
    points = swing_points(bars)
    structure = market_structure(points)
    trend_state = classify_trend(bars[-1].close, indicators)
    momentum_state = classify_momentum(indicators)
    atr = indicators.get("atr14_wilder")
    levels = nearest_levels(
        close=bars[-1].close,
        atr=float(atr) if isinstance(atr, (int, float)) else None,
        points=points,
        latest_bar_index=len(bars) - 1,
    )
    limitations = []
    if trend_state == "INSUFFICIENT_DATA":
        limitations.append("EMA20/EMA50 trend requires at least 50 completed bars.")
    if momentum_state == "INSUFFICIENT_DATA":
        limitations.append("RSI14/ROC3 momentum lacks completed-bar history.")
    if structure["state"] == "INSUFFICIENT_DATA":
        limitations.append("Two confirmed swing highs and lows are not available.")
    return {
        "timeframe": timeframe,
        "bar_count": len(bars),
        "latest_completed_timestamp": bars[-1].as_dict()["timestamp"],
        "latest_close_bid": bars[-1].close,
        "price_source": bars[-1].price_source,
        "indicators": indicators,
        "trend": {
            "state": trend_state,
            "rule": (
                "BULLISH: close > EMA20 > EMA50 and EMA20 slope(5) > 0; "
                "BEARISH: inverse; otherwise MIXED"
            ),
        },
        "momentum": {
            "state": momentum_state,
            "rule": (
                "BULLISH: RSI14 > 55 and ROC3 > 0; "
                "BEARISH: RSI14 < 45 and ROC3 < 0; otherwise NEUTRAL"
            ),
        },
        "volatility": {
            "atr14": indicators.get("atr14_wilder"),
            "atr_percentile_100": indicators.get("atr_percentile_100"),
            "percentile_sample_size": indicators.get("atr_percentile_sample_size"),
        },
        "structure": structure,
        "levels": levels,
        "confirmed_swing_points": points[-swing_limit:],
        "aggregation": diagnostics,
        "limitations": limitations,
    }


def _observation(
    identifier: str,
    category: str,
    fact: str,
    value: Any,
    source: str,
) -> dict[str, Any]:
    return {
        "id": identifier,
        "category": category,
        "fact": fact,
        "value": value,
        "source": source,
    }


def _build_observations(
    forecast: dict[str, Any],
    views: dict[str, dict[str, Any]],
    proposal: dict[str, Any],
    state: dict[str, Any],
) -> list[dict[str, Any]]:
    observations = [
        _observation(
            "obs-h3-q50",
            "FORECAST",
            "H3 q50 direction at forecast origin.",
            forecast["direction"]["h3_q50_side"],
            "XPDE_CORE",
        ),
        _observation(
            "obs-direction-probability",
            "FORECAST",
            "Origin direction probability is UP versus NON-UP.",
            {
                "up": forecast["direction"]["probability_up"],
                "non_up": forecast["direction"]["probability_non_up"],
            },
            "XPDE_CORE",
        ),
        _observation(
            "obs-core-decision",
            "DECISION",
            "Exact current XPDE core proposal.",
            {
                "action": proposal.get("action"),
                "reason_codes": proposal.get("reason_codes", []),
            },
            "XPDE_CORE",
        ),
        _observation(
            "obs-model-health",
            "EVIDENCE",
            "Current XPDE live model-health stage.",
            state.get("model_health", {}).get("status"),
            "XPDE_CORE",
        ),
        _observation(
            "obs-market-bars",
            "MARKET_DATA",
            "Completed indicator inputs were read from XPDE SQLite.",
            {
                timeframe: {
                    "bar_count": view.get("bar_count"),
                    "latest_completed_timestamp": view.get(
                        "latest_completed_timestamp"
                    ),
                }
                for timeframe, view in views.items()
            },
            "XPDE_SQLITE",
        ),
    ]
    for timeframe, view in views.items():
        observations.extend(
            [
                _observation(
                    f"obs-trend-{timeframe.lower()}",
                    "TREND",
                    f"{timeframe} completed-bar trend rule result.",
                    view.get("trend", {}).get("state"),
                    "MCP_DERIVED",
                ),
                _observation(
                    f"obs-momentum-{timeframe.lower()}",
                    "MOMENTUM",
                    f"{timeframe} completed-bar momentum rule result.",
                    view.get("momentum", {}).get("state"),
                    "MCP_DERIVED",
                ),
                _observation(
                    f"obs-structure-{timeframe.lower()}",
                    "STRUCTURE",
                    f"{timeframe} confirmed-swing structure.",
                    view.get("structure", {}).get("state"),
                    "MCP_DERIVED",
                ),
            ]
        )
    m5_levels = views.get("M5", {}).get("levels", {})
    observations.extend(
        [
            _observation(
                "obs-support-m5",
                "LEVEL",
                "Nearest M5 support from a confirmed swing pivot.",
                m5_levels.get("nearest_support"),
                "MCP_DERIVED",
            ),
            _observation(
                "obs-resistance-m5",
                "LEVEL",
                "Nearest M5 resistance from a confirmed swing pivot.",
                m5_levels.get("nearest_resistance"),
                "MCP_DERIVED",
            ),
        ]
    )
    return observations


def _compact_evidence(
    evaluation: dict[str, Any],
    model_record: dict[str, Any] | None,
    manifest_evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "offline": {
            "registry": model_record,
            "manifest": (
                manifest_evidence.get("manifest")
                if isinstance(manifest_evidence, dict)
                else None
            ),
            "manifest_status": (
                "AVAILABLE" if manifest_evidence is not None else "UNAVAILABLE"
            ),
        },
        "live": {
            "current_model": evaluation.get("current_model"),
            "current_session": evaluation.get("current_session"),
            "model_health": evaluation.get("model_health"),
            "flat_return_h3": evaluation.get("flat_return_h3"),
            "direction_expected_calibration_error": evaluation.get(
                "direction_expected_calibration_error"
            ),
            "barrier_expected_calibration_error": evaluation.get(
                "barrier_expected_calibration_error"
            ),
            "settlement_completeness": evaluation.get("settlement_completeness"),
            "proposal_outcomes_by_profile": evaluation.get(
                "proposal_outcomes_by_profile"
            ),
        },
    }


def build_technical_analysis_packet(
    *,
    state: dict[str, Any],
    evaluation: dict[str, Any],
    models: list[dict[str, Any]],
    market_bar_values: list[dict[str, Any]],
    profile: str,
    depth: str,
    manifest_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = profile.upper()
    depth = depth.upper()
    if profile not in _PROFILE_TICK_AGE_LIMIT:
        raise ContractError("profile must be SCALPER or SNIPER")
    if depth not in {"SUMMARY", "STANDARD", "DETAILED"}:
        raise ContractError("depth must be SUMMARY, STANDARD, or DETAILED")
    forecast_value = state.get("forecast")
    if not isinstance(forecast_value, dict):
        raise ContractError("XPDE state has no forecast object")
    forecast = normalize_forecast(forecast_value)
    proposals = state.get("proposals", [])
    proposal = next(
        (
            value
            for value in proposals
            if isinstance(value, dict) and value.get("profile") == profile
        ),
        None,
    )
    if proposal is None:
        raise ContractError(f"XPDE state has no {profile} core proposal")
    bars = normalize_m5_bars(market_bar_values)
    swing_limit = {"SUMMARY": 6, "STANDARD": 12, "DETAILED": 20}[depth]
    views = {
        timeframe: build_technical_view(
            bars,
            timeframe,
            swing_limit=swing_limit,
        )
        for timeframe in ("M5", "M15", "H1")
    }
    market_current, current_reasons = market_data_current(state, profile)
    action = proposal.get("action")
    core_actionable = market_current and action in {"LONG", "SHORT"}
    model_id = forecast.get("model_id")
    model_record = next(
        (value for value in models if value.get("model_id") == model_id),
        None,
    )
    alignment = align_forecast(
        forecast_side=forecast["direction"]["h3_q50_side"],
        views=views,
    )
    if not market_current:
        alignment["limitations"].append(
            "Current market/forecast context is not current; alignment is diagnostic only."
        )
    snapshot = state.get("snapshot", {})
    bid = _finite_float(snapshot.get("bid"), "snapshot.bid")
    ask = _finite_float(snapshot.get("ask"), "snapshot.ask")
    current_bar = snapshot.get("current_bar")
    packet = {
        "contract": {
            "packet_version": PACKET_VERSION,
            "ta_contract_id": TA_CONTRACT_ID,
            "forecast_authority": FORECAST_AUTHORITY,
            "decision_authority": DECISION_AUTHORITY,
            "native_timeframe": "M5",
            "derived_timeframes": ["M15", "H1"],
            "indicator_input": "COMPLETED_BARS_ONLY",
        },
        "source_status": {
            "forecast_usable_as_guide": market_current,
            "market_data_current": market_current,
            "market_data_reason_codes": current_reasons,
            "core_actionable": core_actionable,
            "evidence_stage": state.get("model_health", {}).get("status", "UNKNOWN"),
            "forecast_status": state.get("forecast_status"),
            "connection_status": state.get("connection_status"),
            "guide_statement": (
                "Forecast tersedia dan dapat dipertimbangkan sebagai panduan."
                if market_current
                else "Forecast tidak current dan hanya boleh dibaca sebagai diagnostik."
            ),
        },
        "market": {
            "symbol": snapshot.get("symbol"),
            "native_timeframe": snapshot.get("timeframe"),
            "quote_timestamp": snapshot.get("timestamp"),
            "bid": bid,
            "ask": ask,
            "spread": ask - bid,
            "data_quality": snapshot.get("data_quality"),
            "symbol_spec": snapshot.get("symbol_spec"),
            "current_m5_observation": {
                "bar": current_bar,
                "included_in_indicators": False,
                "reason": "Current candle is observation-only until completed.",
            },
        },
        "forecast": forecast,
        "technical_views": views,
        "levels": {timeframe: view.get("levels") for timeframe, view in views.items()},
        "alignment": alignment,
        "core_decision": {
            "action": action,
            "must_preserve": True,
            "profile": profile,
            "proposal_id": proposal.get("proposal_id"),
            "reason_codes": proposal.get("reason_codes", []),
            "remaining_reward_account": proposal.get("remaining_reward_account"),
            "remaining_risk_account": proposal.get("remaining_risk_account"),
            "reward_risk_ratio": proposal.get("reward_risk_ratio"),
            "proposal": proposal,
        },
        "evidence": _compact_evidence(
            evaluation,
            model_record,
            manifest_evidence,
        ),
        "observations": _build_observations(
            forecast,
            views,
            proposal,
            state,
        ),
        "semantic_contract": SEMANTIC_CONTRACT,
        "interpretation_constraints": INTERPRETATION_CONSTRAINTS,
    }
    return packet
