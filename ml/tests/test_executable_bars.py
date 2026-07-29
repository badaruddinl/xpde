from __future__ import annotations

from datetime import UTC, datetime, time, timedelta

import pytest

from xpde_ml.executable_bars import (
    ExecutableBarTracker,
    aggregate_executable_ticks,
    collect_executable_bars,
)
from xpde_ml.mt5_bridge import (
    MarketCalendar,
    infer_market_status,
    load_market_calendar,
    market_session_open_until,
)
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


def test_tracker_overlap_keeps_distinct_quotes_with_the_same_millisecond() -> None:
    base = int(datetime(2026, 7, 29, 10, 0, tzinfo=UTC).timestamp() * 1000)

    class Mt5:
        COPY_TICKS_ALL = 0
        calls = 0

        def copy_ticks_range(self, *_args):
            self.calls += 1
            if self.calls == 1:
                return [
                    {"time_msc": base + 1_000, "bid": 4000.0, "ask": 4000.2},
                ]
            return [
                {"time_msc": base + 1_000, "bid": 4000.0, "ask": 4000.2},
                {"time_msc": base + 1_000, "bid": 4000.4, "ask": 4000.6},
            ]

        @staticmethod
        def last_error():
            return 0, "ok"

    tracker = ExecutableBarTracker(retention_bars=1)
    mt5 = Mt5()
    tracker.refresh(
        mt5,
        symbol="GOLDm#",
        clock=BrokerClock(),
        now_epoch=(base + 2_000) / 1000,
    )
    tracker.refresh(
        mt5,
        symbol="GOLDm#",
        clock=BrokerClock(),
        now_epoch=(base + 3_000) / 1000,
    )

    path = tracker.bars["2026-07-29T10:00:00Z"]["_tick_path"]
    assert path == [
        [base + 1_000, 4000.0, 4000.2],
        [base + 1_000, 4000.4, 4000.6],
    ]
    assert tracker.bars["2026-07-29T10:00:00Z"]["executable_tick_count"] == 2


def test_tracker_recounts_raw_ticks_instead_of_freezing_count_on_overlap() -> None:
    base = int(datetime(2026, 7, 29, 10, 0, tzinfo=UTC).timestamp() * 1000)

    class Mt5:
        COPY_TICKS_ALL = 0
        calls = 0

        def copy_ticks_range(self, *_args):
            self.calls += 1
            if self.calls == 1:
                return [
                    {"time_msc": base + 1_000, "bid": 4000.0, "ask": 4000.2},
                ]
            return [
                {"time_msc": base + 1_000, "bid": 4000.0, "ask": 4000.2},
                {"time_msc": base + 2_000, "bid": 4000.0, "ask": 4000.2},
                {"time_msc": base + 3_000, "bid": 4000.1, "ask": 4000.3},
            ]

        @staticmethod
        def last_error():
            return 0, "ok"

    tracker = ExecutableBarTracker(retention_bars=1)
    mt5 = Mt5()
    tracker.refresh(
        mt5,
        symbol="GOLDm#",
        clock=BrokerClock(),
        now_epoch=(base + 2_000) / 1000,
    )
    tracker.refresh(
        mt5,
        symbol="GOLDm#",
        clock=BrokerClock(),
        now_epoch=(base + 4_000) / 1000,
    )

    bar = tracker.bars["2026-07-29T10:00:00Z"]
    assert bar["executable_tick_count"] == 3
    assert bar["_tick_path"] == [
        [base + 1_000, 4000.0, 4000.2],
        [base + 2_000, 4000.0, 4000.2],
        [base + 3_000, 4000.1, 4000.3],
    ]


def test_tracker_recounts_previous_bucket_after_m5_rollover() -> None:
    base = int(datetime(2026, 7, 29, 10, 0, tzinfo=UTC).timestamp() * 1000)

    class Mt5:
        COPY_TICKS_ALL = 0
        calls = 0

        def copy_ticks_range(self, *_args):
            self.calls += 1
            if self.calls == 1:
                return [
                    {"time_msc": base + 299_000, "bid": 4000.0, "ask": 4000.2},
                ]
            return [
                {"time_msc": base + 299_000, "bid": 4000.0, "ask": 4000.2},
                {"time_msc": base + 299_500, "bid": 4000.1, "ask": 4000.3},
                {"time_msc": base + 301_000, "bid": 4000.2, "ask": 4000.4},
            ]

        @staticmethod
        def last_error():
            return 0, "ok"

    tracker = ExecutableBarTracker(retention_bars=2)
    mt5 = Mt5()
    tracker.refresh(
        mt5,
        symbol="GOLDm#",
        clock=BrokerClock(),
        now_epoch=(base + 299_100) / 1000,
    )
    tracker.refresh(
        mt5,
        symbol="GOLDm#",
        clock=BrokerClock(),
        now_epoch=(base + 301_500) / 1000,
    )

    assert tracker.bars["2026-07-29T10:00:00Z"]["executable_tick_count"] == 2
    assert tracker.bars["2026-07-29T10:05:00Z"]["executable_tick_count"] == 1


def test_tracker_rehydrates_retention_window_when_cache_is_empty() -> None:
    now = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)

    class Mt5:
        COPY_TICKS_ALL = 0
        requested_start = None

        def copy_ticks_range(self, _symbol, start, _end, _mode):
            self.requested_start = start
            return []

        @staticmethod
        def last_error():
            return 0, "ok"

    tracker = ExecutableBarTracker(retention_bars=24)
    tracker.last_tick_msc = int((now - timedelta(hours=12)).timestamp() * 1000)
    mt5 = Mt5()
    tracker.refresh(
        mt5,
        symbol="GOLDm#",
        clock=BrokerClock(),
        now_epoch=now.timestamp(),
    )

    assert mt5.requested_start == now - timedelta(hours=2)


def test_historical_chunk_overlap_never_regresses_the_tick_cursor() -> None:
    base = int(datetime(2026, 7, 29, 10, 0, tzinfo=UTC).timestamp() * 1000)

    class Mt5:
        COPY_TICKS_ALL = 0
        calls = 0

        def copy_ticks_range(self, *_args):
            self.calls += 1
            if self.calls == 1:
                return [{"time_msc": base + 1_000, "bid": 4000.0, "ask": 4000.2}]
            return []

        @staticmethod
        def last_error():
            return 0, "ok"

    bars, maximum = collect_executable_bars(
        Mt5(),
        symbol="GOLDm#",
        start_epoch=base / 1000,
        end_epoch=base / 1000 + 3_700,
        clock=BrokerClock(),
        chunk_hours=1,
    )

    assert maximum == base + 1_000
    assert list(bars) == ["2026-07-29T10:00:00Z"]


def test_session_end_is_reported_for_the_active_window() -> None:
    calendar = MarketCalendar(
        sessions={2: ((time(1, 0), time(23, 0)),)},
        closed_dates=frozenset(),
    )
    now = datetime(2026, 7, 29, 22, 55, tzinfo=UTC)

    assert market_session_open_until(
        now_utc=now,
        calendar=calendar,
        market_utc_offset_hours=0,
    ) == datetime(
        2026, 7, 29, 23, 0, tzinfo=UTC
    )


def test_market_calendar_fails_closed_for_missing_or_invalid_config(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="unavailable"):
        load_market_calendar(str(tmp_path / "missing.toml"))

    invalid = tmp_path / "invalid.toml"
    invalid.write_text(
        '[market_session]\ntimezone = "fixed_broker_utc_offset"\n'
        'closed_dates = ["29-07-2026"]\n',
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="invalid market closed date"):
        load_market_calendar(str(invalid))
