"""Versioned semantic and technical-analysis contracts."""

from __future__ import annotations

from typing import Final

PACKET_VERSION: Final = "1.0"
TA_CONTRACT_ID: Final = "xpde-ta-goldm-m5-v2"
FORECAST_AUTHORITY: Final = "XPDE_CORE"
DECISION_AUTHORITY: Final = "XPDE_CORE"
SUPPORTED_SYMBOL: Final = "GOLDm#"
NATIVE_TIMEFRAME: Final = "M5"
FORECAST_HORIZONS: Final = (1, 3, 6, 12)
MAXIMUM_TOOL_LIMIT: Final = 200
MAXIMUM_MARKET_BARS: Final = 6_000
MAXIMUM_MANIFEST_BYTES: Final = 2_000_000

SEMANTIC_CONTRACT: Final = {
    "forecast_is_future_truth": False,
    "direction_probability_up": "P(UP at forecast origin)",
    "direction_probability_non_up": ("1 - P(UP); negative or flat; not strict P(DOWN)"),
    "barrier_probability_reference": "FORECAST_ORIGIN",
    "entry_conditioned_probability": False,
    "warming_up": (
        "Forecast remains usable as a guide while live evidence is immature."
    ),
    "model_health_guide_semantics": {
        "HEALTHY": "GUIDE",
        "WARMING_UP": "GUIDE_WITH_IMMATURE_EVIDENCE",
        "DEGRADED": "GUIDE_WITH_STRONG_CAUTION",
        "SUSPENDED": "DIAGNOSTIC_ONLY",
        "UNKNOWN": "DIAGNOSTIC_ONLY",
    },
    "wait_is_valid_output": True,
    "no_prediction_is_valid_output": True,
    "core_decision_must_be_preserved": True,
    "mcp_can_create_alternative_trade_signal": False,
}

INTERPRETATION_CONSTRAINTS: Final = {
    "forecast_is_not_guarantee": True,
    "preserve_core_decision": True,
    "do_not_create_buy_sell_alternative": True,
    "probabilities_are_origin_based": True,
    "non_up_is_not_strict_down": True,
    "technical_analysis_is_deterministic_context_not_second_model": True,
    "human_verification_required": True,
}
