"""Internal immutable bar type."""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..errors import ContractError


def parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise ContractError(f"invalid market timestamp: {value!r}") from error
    if parsed.tzinfo is None:
        raise ContractError("market timestamps must include timezone")
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: float
    price_source: str = "BID_OHLC"

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> Bar:
        try:
            bar = cls(
                timestamp=parse_timestamp(str(value["timestamp"])),
                open=float(value["open"]),
                high=float(value["high"]),
                low=float(value["low"]),
                close=float(value["close"]),
                tick_volume=float(value.get("tick_volume", 0.0)),
                price_source=str(value.get("price_source", "BID_OHLC")),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ContractError("market bar has invalid fields") from error
        bar.validate()
        return bar

    def validate(self) -> None:
        values = (self.open, self.high, self.low, self.close, self.tick_volume)
        if any(not math.isfinite(value) for value in values):
            raise ContractError("market bar contains non-finite values")
        if (
            self.open <= 0
            or self.high <= 0
            or self.low <= 0
            or self.close <= 0
            or self.tick_volume < 0
            or self.high < max(self.open, self.close)
            or self.low > min(self.open, self.close)
            or self.high < self.low
        ):
            raise ContractError("market bar violates OHLC invariants")

    def as_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "tick_volume": self.tick_volume,
            "price_source": self.price_source,
        }


def normalize_m5_bars(values: list[dict[str, Any]]) -> list[Bar]:
    bars = sorted(
        (Bar.from_mapping(value) for value in values), key=lambda bar: bar.timestamp
    )
    if any(int(bar.timestamp.timestamp()) % 300 != 0 for bar in bars):
        raise ContractError("native market bars must align to exact M5 boundaries")
    if any(
        left.timestamp >= right.timestamp for left, right in itertools.pairwise(bars)
    ):
        raise ContractError("market bars must have unique increasing timestamps")
    return bars
