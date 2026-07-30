"""Forecast–technical alignment without decision authority."""

from __future__ import annotations

from typing import Any


def _technical_side(value: str) -> str | None:
    if value == "BULLISH":
        return "BULLISH"
    if value == "BEARISH":
        return "BEARISH"
    return None


def align_forecast(
    *,
    forecast_side: str,
    views: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    confluences: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    limitations: list[str] = []
    confluence_timeframes: set[str] = set()
    conflict_timeframes: set[str] = set()

    if forecast_side not in {"BULLISH", "BEARISH"}:
        return {
            "state": "NEUTRAL",
            "confluences": [],
            "conflicts": [],
            "limitations": ["H3 q50 is flat; directional alignment is undefined."],
            "counts": {
                "confluences": 0,
                "conflicts": 0,
                "confluence_timeframes": [],
                "conflict_timeframes": [],
            },
            "rule": "Deterministic context only; never replaces XPDE core decision.",
        }

    for timeframe, view in views.items():
        for key, label in (
            ("trend", "trend"),
            ("momentum", "momentum"),
            ("structure", "market structure"),
        ):
            section = view.get(key, {})
            value = section.get("state")
            side = _technical_side(str(value))
            basis_id = f"obs-{key}-{timeframe.lower()}"
            if side == forecast_side:
                confluence_timeframes.add(timeframe)
                confluences.append(
                    {
                        "statement": (
                            f"Forecast {forecast_side.lower()} aligns with "
                            f"{timeframe} {label}."
                        ),
                        "basis_ids": ["obs-h3-q50", basis_id],
                        "strength": "MEDIUM",
                        "timeframe": timeframe,
                    }
                )
            elif side is not None:
                conflict_timeframes.add(timeframe)
                conflicts.append(
                    {
                        "statement": (
                            f"Forecast {forecast_side.lower()} conflicts with "
                            f"{timeframe} {label}."
                        ),
                        "basis_ids": ["obs-h3-q50", basis_id],
                        "strength": "MEDIUM",
                        "timeframe": timeframe,
                    }
                )
            elif value == "INSUFFICIENT_DATA":
                limitations.append(f"{timeframe} {label} has insufficient data.")

    m5_levels = views.get("M5", {}).get("levels", {})
    if forecast_side == "BULLISH":
        nearby = m5_levels.get("nearest_resistance")
        label = "resistance"
        basis_id = "obs-resistance-m5"
    else:
        nearby = m5_levels.get("nearest_support")
        label = "support"
        basis_id = "obs-support-m5"
    if isinstance(nearby, dict):
        distance_atr = nearby.get("distance_atr")
        if isinstance(distance_atr, (int, float)) and distance_atr < 1.0:
            conflict_timeframes.add("M5")
            conflicts.append(
                {
                    "statement": (
                        f"Nearest M5 {label} is less than one ATR from price."
                    ),
                    "basis_ids": ["obs-h3-q50", basis_id],
                    "strength": "MEDIUM",
                    "timeframe": "M5",
                }
            )

    support_count = len(confluences)
    conflict_count = len(conflicts)
    directional_facts = support_count + conflict_count
    if directional_facts == 0:
        state = "INSUFFICIENT_DATA" if limitations else "NEUTRAL"
    elif conflict_count > support_count:
        state = "CONFLICTED"
    elif len(confluence_timeframes) >= 2 and conflict_count == 0:
        state = "ALIGNED"
    elif support_count > 0:
        state = "PARTIALLY_ALIGNED"
    else:
        state = "NEUTRAL"
    return {
        "state": state,
        "confluences": confluences,
        "conflicts": conflicts,
        "limitations": sorted(set(limitations)),
        "counts": {
            "confluences": support_count,
            "conflicts": conflict_count,
            "confluence_timeframes": sorted(confluence_timeframes),
            "conflict_timeframes": sorted(conflict_timeframes),
        },
        "rule": "Deterministic context only; never replaces XPDE core decision.",
    }
