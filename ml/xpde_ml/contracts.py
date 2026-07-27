from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

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
