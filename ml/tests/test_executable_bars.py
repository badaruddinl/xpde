from __future__ import annotations

from datetime import UTC, datetime, time

from xpde_ml.executable_bars import aggregate_executable_ticks
from xpde_ml.mt5_bridge import MarketCalendar, infer_market_status
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
    assert bar["first_tick_msc"] == base + 1_000
    assert bar["last_tick_msc"] == base + 3_000
    assert len(bar["_tick_path"]) == 3
    assert maximum == base + 3_000
    assert BrokerClock(offset_hours=3).iso_utc(base / 1000 + 3 * 3600) in bars
    shifted, _ = aggregate_executable_ticks(
        [{"time_msc": base + 3 * 3_600_000 + 1_000, "bid": 4000.0, "ask": 4000.3}],
        clock=BrokerClock(offset_hours=3),
    )
    assert shifted["2026-07-29T10:00:00Z"]["first_tick_msc"] == base + 1_000


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


def test_market_calendar_and_broker_trade_mode_are_authoritative() -> None:
    sunday = datetime(2026, 8, 2, 23, 30, tzinfo=UTC)
    calendar = MarketCalendar(
        sessions={6: ((time(23, 5), time(23, 59)),)},
        closed_dates=frozenset(),
    )
    assert (
        infer_market_status(
            now_utc=sunday,
            absolute_tick_age_ms=100,
            terminal_connected=True,
            calendar=calendar,
        )
        == "OPEN"
    )
    assert (
        infer_market_status(
            now_utc=sunday,
            absolute_tick_age_ms=100,
            terminal_connected=True,
            calendar=calendar,
            trade_mode_enabled=False,
        )
        == "MARKET_CLOSED"
    )
