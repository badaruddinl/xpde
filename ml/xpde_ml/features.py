from __future__ import annotations

import math
from statistics import fmean, pstdev

from .contracts import Bar


def log_returns(bars: list[Bar]) -> list[float]:
    return [
        math.log(current.close / previous.close)
        for previous, current in zip(bars, bars[1:], strict=False)
        if previous.close > 0 and current.close > 0
    ]


def true_ranges(bars: list[Bar]) -> list[float]:
    ranges: list[float] = []
    for previous, current in zip(bars, bars[1:], strict=False):
        ranges.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    return ranges


def snapshot_features(bars: list[Bar], spread_usd: float) -> dict[str, float]:
    returns = log_returns(bars)
    ranges = true_ranges(bars)
    recent_returns = returns[-24:]
    recent_ranges = ranges[-24:]
    last = bars[-1]
    return {
        "return_1": returns[-1],
        "return_3": sum(returns[-3:]),
        "return_6": sum(returns[-6:]),
        "return_12": sum(returns[-12:]),
        "momentum_24": sum(recent_returns),
        "volatility_12": pstdev(returns[-12:]),
        "volatility_24": pstdev(recent_returns),
        "mean_true_range_24": fmean(recent_ranges),
        "candle_body": (last.close - last.open) / last.open,
        "upper_wick": (last.high - max(last.open, last.close)) / last.open,
        "lower_wick": (min(last.open, last.close) - last.low) / last.open,
        "spread_usd": spread_usd,
        "hour_utc": float(last.timestamp.hour),
        "weekday_utc": float(last.timestamp.weekday()),
    }
