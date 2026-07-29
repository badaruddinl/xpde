from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime


def environment_integer(name: str, default: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    return default if not raw else int(raw)


@dataclass(frozen=True)
class BrokerClock:
    """Convert MT5 epoch timestamps to UTC using only an explicit provider override."""

    offset_hours: int = 0

    def to_utc(self, broker_epoch_seconds: float) -> datetime:
        return datetime.fromtimestamp(
            broker_epoch_seconds - self.offset_hours * 3600,
            UTC,
        )

    def iso_utc(self, broker_epoch_seconds: float) -> str:
        return self.to_utc(broker_epoch_seconds).isoformat().replace("+00:00", "Z")
