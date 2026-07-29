from __future__ import annotations

from datetime import UTC, datetime

from xpde_ml.time_utils import BrokerClock, environment_integer


def test_mt5_epoch_is_utc_by_default() -> None:
    timestamp = datetime(2026, 7, 27, 14, 0, tzinfo=UTC).timestamp()
    assert BrokerClock().iso_utc(timestamp) == "2026-07-27T14:00:00Z"


def test_explicit_provider_override_is_supported_without_auto_inference() -> None:
    encoded_provider_timestamp = datetime(
        2026, 7, 27, 17, 0, tzinfo=UTC
    ).timestamp()
    assert (
        BrokerClock(offset_hours=3).iso_utc(encoded_provider_timestamp)
        == "2026-07-27T14:00:00Z"
    )


def test_blank_environment_integer_uses_default(monkeypatch) -> None:
    monkeypatch.setenv("XPDE_TEST_OFFSET", "")
    assert environment_integer("XPDE_TEST_OFFSET", 3) == 3
