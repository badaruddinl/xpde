from __future__ import annotations

from datetime import UTC, datetime

from xpde_ml.executable_bars import aggregate_executable_ticks
from xpde_ml.mt5_bridge import infer_market_status
from xpde_ml.time_utils import BrokerClock


def test_tick_aggregation_builds_distinct_bid_and_ask_ohlc() -> None:
    base = int(datetime(2026, 7, 29, 10, 0, tzinfo=UTC).timestamp() * 1000)
    bars, maximum = aggregate_executable_ticks(
        [
            {"time_msc": base + 2_000, "bid": 4000.2, "ask": 4000.5},
            {"time_msc": base + 1_000, "bid": 4000.0, "ask": 4000.3},
            {"time_msc": base + 3_000, "bid": 3999.8, "ask": 4000.1},
        ],
        clock=BrokerClock(),
    )

    bar = bars["2026-07-29T10:00:00Z"]
    assert bar["bid_open"] == 4000.0
    assert bar["bid_high"] == 4000.2
    assert bar["bid_low"] == 3999.8
    assert bar["bid_close"] == 3999.8
    assert bar["ask_open"] == 4000.3
    assert bar["ask_high"] == 4000.5
    assert bar["ask_low"] == 4000.1
    assert bar["ask_close"] == 4000.1
    assert bar["executable_tick_count"] == 3
    assert maximum == base + 3_000
    assert BrokerClock(offset_hours=3).iso_utc(base / 1000 + 3 * 3600) in bars


def test_market_health_distinguishes_closed_stale_and_disconnected() -> None:
    weekday = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    weekend = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)

    assert (
        infer_market_status(
            now_utc=weekday,
            absolute_tick_age_ms=100,
            terminal_connected=False,
        )
        == "BRIDGE_DISCONNECTED"
    )
    assert (
        infer_market_status(
            now_utc=weekend,
            absolute_tick_age_ms=100_000,
            terminal_connected=True,
        )
        == "MARKET_CLOSED"
    )
    assert (
        infer_market_status(
            now_utc=weekday,
            absolute_tick_age_ms=100_000,
            terminal_connected=True,
        )
        == "FEED_STALE"
    )
    assert (
        infer_market_status(
            now_utc=weekday,
            absolute_tick_age_ms=100,
            terminal_connected=True,
        )
        == "OPEN"
    )
