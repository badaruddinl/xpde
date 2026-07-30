"""Versioned swing and HH/HL/LH/LL market-structure rules."""

from __future__ import annotations

from typing import Any

from .types import Bar

SWING_LEFT_BARS = 2
SWING_RIGHT_BARS = 2


def swing_points(
    bars: list[Bar],
    *,
    left: int = SWING_LEFT_BARS,
    right: int = SWING_RIGHT_BARS,
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    if left < 1 or right < 1:
        return points
    for index in range(left, len(bars) - right):
        bar = bars[index]
        neighbors = bars[index - left : index] + bars[index + 1 : index + right + 1]
        if all(bar.high > other.high for other in neighbors):
            points.append(
                {
                    "kind": "SWING_HIGH",
                    "timestamp": bar.timestamp.isoformat().replace("+00:00", "Z"),
                    "price": bar.high,
                    "bar_index": index,
                }
            )
        if all(bar.low < other.low for other in neighbors):
            points.append(
                {
                    "kind": "SWING_LOW",
                    "timestamp": bar.timestamp.isoformat().replace("+00:00", "Z"),
                    "price": bar.low,
                    "bar_index": index,
                }
            )
    return points


def market_structure(points: list[dict[str, Any]]) -> dict[str, Any]:
    highs = [point for point in points if point["kind"] == "SWING_HIGH"]
    lows = [point for point in points if point["kind"] == "SWING_LOW"]
    high_pattern = "INSUFFICIENT_DATA"
    low_pattern = "INSUFFICIENT_DATA"
    if len(highs) >= 2:
        high_pattern = (
            "HH"
            if highs[-1]["price"] > highs[-2]["price"]
            else "LH"
            if highs[-1]["price"] < highs[-2]["price"]
            else "EH"
        )
    if len(lows) >= 2:
        low_pattern = (
            "HL"
            if lows[-1]["price"] > lows[-2]["price"]
            else "LL"
            if lows[-1]["price"] < lows[-2]["price"]
            else "EL"
        )
    if high_pattern == "HH" and low_pattern == "HL":
        state = "BULLISH"
    elif high_pattern == "LH" and low_pattern == "LL":
        state = "BEARISH"
    elif "INSUFFICIENT_DATA" in {high_pattern, low_pattern}:
        state = "INSUFFICIENT_DATA"
    else:
        state = "RANGE_OR_MIXED"
    return {
        "state": state,
        "latest_high_pattern": high_pattern,
        "latest_low_pattern": low_pattern,
        "confirmed_swing_highs": len(highs),
        "confirmed_swing_lows": len(lows),
        "rule": {
            "left_bars": SWING_LEFT_BARS,
            "right_bars": SWING_RIGHT_BARS,
            "bullish": "HH + HL",
            "bearish": "LH + LL",
        },
    }
