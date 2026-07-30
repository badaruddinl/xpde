from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Any
import uuid

PREDICTION_ID_NAMESPACE = uuid.UUID("dfe13b8f-5417-4c25-9af5-80a03f41ce5f")
EXECUTABLE_FEATURE_WINDOW_BARS = 24
MINIMUM_EXECUTABLE_TICK_COVERAGE = 0.95
FLAT_RETURN_LOG_EPSILON = 1e-12


def deterministic_prediction_id(
    *,
    model_id: str,
    symbol: str,
    timeframe: str,
    origin_bar_timestamp: str,
    feature_version: str,
    barrier_spec_id: str,
    label_contract_id: str,
) -> str:
    key = "|".join(
        (
            model_id,
            symbol,
            timeframe,
            origin_bar_timestamp,
            feature_version,
            barrier_spec_id,
            label_contract_id,
        )
    )
    return str(uuid.uuid5(PREDICTION_ID_NAMESPACE, key))


def has_complete_tick_coverage(
    bar: dict[str, Any],
    minimum_ratio: float = MINIMUM_EXECUTABLE_TICK_COVERAGE,
) -> bool:
    try:
        tick_volume = float(bar.get("tick_volume", 0.0))
        executable_count = int(bar.get("executable_tick_count", 0))
    except (TypeError, ValueError):
        return False
    return (
        0.0 <= minimum_ratio <= 1.0
        and math.isfinite(tick_volume)
        and tick_volume > 0.0
        and executable_count > 0
        and executable_count / tick_volume >= minimum_ratio
    )


def has_complete_executable_feature_window(
    bars: list[dict[str, Any]],
    *,
    required_bars: int = EXECUTABLE_FEATURE_WINDOW_BARS,
    minimum_tick_coverage: float = MINIMUM_EXECUTABLE_TICK_COVERAGE,
) -> bool:
    """Validate the exact Bid/Ask history used by rolling spread features."""
    if required_bars <= 0 or len(bars) < required_bars:
        return False
    try:
        timestamped = [
            (
                datetime.fromisoformat(
                    str(bar["timestamp"]).replace("Z", "+00:00")
                ),
                bar,
            )
            for bar in bars
        ]
        ordered = [
            bar for _, bar in sorted(timestamped, key=lambda item: item[0])
        ]
    except (KeyError, TypeError, ValueError):
        return False
    window = ordered[-required_bars:]
    timestamps = [
        datetime.fromisoformat(str(bar["timestamp"]).replace("Z", "+00:00"))
        for bar in window
    ]
    if len(set(timestamps)) != len(timestamps):
        return False
    for bar in window:
        try:
            bid_close = float(bar["bid_close"])
            ask_close = float(bar["ask_close"])
        except (KeyError, TypeError, ValueError):
            return False
        if (
            not math.isfinite(bid_close)
            or not math.isfinite(ask_close)
            or bid_close <= 0.0
            or ask_close <= bid_close
            or not has_complete_tick_coverage(bar, minimum_tick_coverage)
        ):
            return False
    return True


SUPPORTED_SYMBOL = "GOLDm#"
SUPPORTED_TIMEFRAME = "M5"
HORIZONS = (1, 3, 6, 12)
QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)


class ContractError(ValueError):
    """Raised when a cross-runtime data contract is invalid."""


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: float

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Bar":
        return cls(
            timestamp=datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00")),
            open=float(value["open"]),
            high=float(value["high"]),
            low=float(value["low"]),
            close=float(value["close"]),
            tick_volume=float(value.get("tick_volume", 0.0)),
        )


def validate_snapshot(snapshot: dict[str, Any]) -> list[Bar]:
    if snapshot.get("symbol") != SUPPORTED_SYMBOL:
        raise ContractError(f"expected symbol {SUPPORTED_SYMBOL!r}")
    if snapshot.get("timeframe") != SUPPORTED_TIMEFRAME:
        raise ContractError(f"expected timeframe {SUPPORTED_TIMEFRAME!r}")
    bid = float(snapshot["bid"])
    ask = float(snapshot["ask"])
    if ask <= bid:
        raise ContractError("ask must be greater than bid")
    bars = [Bar.from_dict(value) for value in snapshot.get("bars", [])]
    if len(bars) < 48:
        raise ContractError("at least 48 completed M5 bars are required")
    if any(bar.high < max(bar.open, bar.close) for bar in bars):
        raise ContractError("bar high is inconsistent")
    if any(bar.low > min(bar.open, bar.close) for bar in bars):
        raise ContractError("bar low is inconsistent")
    return sorted(bars, key=lambda bar: bar.timestamp)


def validate_quantiles(values: list[float]) -> None:
    if len(values) != len(QUANTILES):
        raise ContractError("five quantiles are required")
    if values != sorted(values):
        raise ContractError("quantiles must be monotonically increasing")
