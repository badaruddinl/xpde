"""Nearest confirmed pivot support and resistance."""

from __future__ import annotations

from typing import Any


def nearest_levels(
    *,
    close: float,
    atr: float | None,
    points: list[dict[str, Any]],
    latest_bar_index: int,
    lookback_bars: int = 120,
) -> dict[str, Any]:
    recent = [
        point
        for point in points
        if point["bar_index"] >= max(latest_bar_index - lookback_bars, 0)
    ]
    supports = [
        point
        for point in recent
        if point["kind"] == "SWING_LOW" and point["price"] < close
    ]
    resistances = [
        point
        for point in recent
        if point["kind"] == "SWING_HIGH" and point["price"] > close
    ]
    support = max(supports, key=lambda point: point["price"], default=None)
    resistance = min(resistances, key=lambda point: point["price"], default=None)

    def normalize(point: dict[str, Any] | None) -> dict[str, Any] | None:
        if point is None:
            return None
        distance = abs(close - float(point["price"]))
        return {
            "price": point["price"],
            "timestamp": point["timestamp"],
            "distance_price": distance,
            "distance_atr": (distance / atr if atr is not None and atr > 0 else None),
            "source": "CONFIRMED_SWING_PIVOT",
        }

    return {
        "nearest_support": normalize(support),
        "nearest_resistance": normalize(resistance),
        "lookback_bars": lookback_bars,
    }
