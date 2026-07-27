from __future__ import annotations

from datetime import UTC, datetime

from xpde_ml.time_utils import BrokerClock, infer_broker_offset_hours


def test_infers_and_removes_whole_hour_broker_offset() -> None:
    now = datetime(2026, 7, 27, 14, 0, tzinfo=UTC)
    broker_epoch = now.timestamp() + 3 * 3600
    assert infer_broker_offset_hours(broker_epoch, now_utc=now) == 3
    clock = BrokerClock.from_tick(broker_epoch, now_utc=now)
    assert clock.iso_utc(broker_epoch) == "2026-07-27T14:00:00Z"
