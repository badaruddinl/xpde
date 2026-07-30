"""Small deterministic indicator set; no indicator emits a trade signal."""

from __future__ import annotations

import itertools
import math
from collections.abc import Iterable

from .types import Bar


def ema(values: Iterable[float], period: int) -> list[float | None]:
    sequence = [float(value) for value in values]
    output: list[float | None] = [None] * len(sequence)
    if period <= 0 or len(sequence) < period:
        return output
    seed = sum(sequence[:period]) / period
    output[period - 1] = seed
    alpha = 2.0 / (period + 1.0)
    current = seed
    for index in range(period, len(sequence)):
        current = alpha * sequence[index] + (1.0 - alpha) * current
        output[index] = current
    return output


def rsi_wilder(values: Iterable[float], period: int = 14) -> list[float | None]:
    sequence = [float(value) for value in values]
    output: list[float | None] = [None] * len(sequence)
    if period <= 0 or len(sequence) <= period:
        return output
    changes = [
        sequence[index] - sequence[index - 1] for index in range(1, len(sequence))
    ]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period

    def value() -> float:
        if average_gain == 0.0 and average_loss == 0.0:
            return 50.0
        if average_loss == 0.0:
            return 100.0
        relative_strength = average_gain / average_loss
        return 100.0 - (100.0 / (1.0 + relative_strength))

    output[period] = value()
    for index in range(period + 1, len(sequence)):
        change_index = index - 1
        average_gain = (average_gain * (period - 1) + gains[change_index]) / period
        average_loss = (average_loss * (period - 1) + losses[change_index]) / period
        output[index] = value()
    return output


def true_ranges(bars: list[Bar]) -> list[float | None]:
    if not bars:
        return []
    output: list[float | None] = [None]
    for previous, current in itertools.pairwise(bars):
        output.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    return output


def atr_wilder(bars: list[Bar], period: int = 14) -> list[float | None]:
    output: list[float | None] = [None] * len(bars)
    ranges = true_ranges(bars)
    valid = [value for value in ranges[1 : period + 1] if value is not None]
    if period <= 0 or len(valid) < period:
        return output
    current = sum(valid) / period
    output[period] = current
    for index in range(period + 1, len(bars)):
        value = ranges[index]
        assert value is not None
        current = (current * (period - 1) + value) / period
        output[index] = current
    return output


def rate_of_change(values: list[float], periods: int) -> float | None:
    if periods <= 0 or len(values) <= periods or values[-periods - 1] == 0:
        return None
    return values[-1] / values[-periods - 1] - 1.0


def percentile_rank(values: list[float], current: float) -> float | None:
    finite = [value for value in values if math.isfinite(value)]
    if not finite:
        return None
    return sum(value <= current for value in finite) / len(finite)


def indicator_snapshot(bars: list[Bar]) -> dict[str, float | int | None]:
    closes = [bar.close for bar in bars]
    ema20_values = ema(closes, 20)
    ema50_values = ema(closes, 50)
    rsi_values = rsi_wilder(closes, 14)
    atr_values = atr_wilder(bars, 14)
    ema20 = ema20_values[-1] if ema20_values else None
    ema50 = ema50_values[-1] if ema50_values else None
    rsi14 = rsi_values[-1] if rsi_values else None
    atr14 = atr_values[-1] if atr_values else None
    ema20_five_bars_ago = ema20_values[-6] if len(ema20_values) >= 6 else None
    slope = (
        ema20 - ema20_five_bars_ago
        if ema20 is not None and ema20_five_bars_ago is not None
        else None
    )
    atr_history = [value for value in atr_values[-100:] if value is not None]
    return {
        "ema20": ema20,
        "ema50": ema50,
        "ema20_slope_5_bars": slope,
        "rsi14_wilder": rsi14,
        "roc3": rate_of_change(closes, 3),
        "roc6": rate_of_change(closes, 6),
        "atr14_wilder": atr14,
        "atr_percentile_100": (
            percentile_rank(atr_history, atr14) if atr14 is not None else None
        ),
        "atr_percentile_sample_size": len(atr_history),
    }


def classify_trend(
    close: float,
    indicators: dict[str, float | int | None],
) -> str:
    ema20 = indicators["ema20"]
    ema50 = indicators["ema50"]
    slope = indicators["ema20_slope_5_bars"]
    if not all(isinstance(value, (int, float)) for value in (ema20, ema50, slope)):
        return "INSUFFICIENT_DATA"
    if close > float(ema20) > float(ema50) and float(slope) > 0:
        return "BULLISH"
    if close < float(ema20) < float(ema50) and float(slope) < 0:
        return "BEARISH"
    return "MIXED"


def classify_momentum(
    indicators: dict[str, float | int | None],
) -> str:
    rsi = indicators["rsi14_wilder"]
    roc3 = indicators["roc3"]
    if not isinstance(rsi, (int, float)) or not isinstance(roc3, (int, float)):
        return "INSUFFICIENT_DATA"
    if rsi > 55.0 and roc3 > 0.0:
        return "BULLISH"
    if rsi < 45.0 and roc3 < 0.0:
        return "BEARISH"
    return "NEUTRAL"
