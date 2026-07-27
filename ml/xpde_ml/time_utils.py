from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def infer_broker_offset_hours(
    broker_epoch_seconds: float,
    *,
    now_utc: datetime | None = None,
) -> int:
    """Infer the whole-hour MT5 server offset encoded in broker timestamps."""
    reference = now_utc or datetime.now(UTC)
    raw_delta = broker_epoch_seconds - reference.timestamp()
    offset = round(raw_delta / 3600)
    return max(-14, min(14, offset))


@dataclass(frozen=True)
class BrokerClock:
    offset_hours: int

    @classmethod
    def from_tick(
        cls,
        tick_epoch_seconds: float,
        *,
        now_utc: datetime | None = None,
    ) -> "BrokerClock":
        return cls(infer_broker_offset_hours(tick_epoch_seconds, now_utc=now_utc))

    def to_utc(self, broker_epoch_seconds: float) -> datetime:
        return datetime.fromtimestamp(
            broker_epoch_seconds - self.offset_hours * 3600,
            UTC,
        )

    def iso_utc(self, broker_epoch_seconds: float) -> str:
        return self.to_utc(broker_epoch_seconds).isoformat().replace("+00:00", "Z")
