from __future__ import annotations

import io
import urllib.error
import urllib.request
from datetime import UTC, datetime

import pytest

from xpde_ml.mt5_bridge import (
    PayloadRejected,
    completed_bar_distance,
    fetch_catchup_bars,
    forecast_generation_delay_ms,
    next_retry_delay,
    post_payload,
)
from xpde_ml.time_utils import BrokerClock


def test_http_error_includes_api_response_body(monkeypatch: pytest.MonkeyPatch) -> None:
    def reject(*_args, **_kwargs):
        raise urllib.error.HTTPError(
            "http://127.0.0.1:8787/api/v1/market/snapshot",
            400,
            "Bad Request",
            {},
            io.BytesIO(b'{"error":"snapshot rejected because data is stale"}'),
        )

    monkeypatch.setattr(urllib.request, "urlopen", reject)

    with pytest.raises(PayloadRejected) as raised:
        post_payload(
            "http://127.0.0.1:8787/api/v1/market/snapshot",
            {"symbol": "GOLDm#"},
        )

    assert raised.value.status == 400
    assert "snapshot rejected because data is stale" in raised.value.body
    assert "HTTP 400" in str(raised.value)


def test_forecast_retry_accepts_already_accepted_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    post_payload(
        "http://127.0.0.1:8787/api/v1/forecast",
        {"prediction_id": "stable"},
        expected_status=(200, 202),
    )


def test_retry_delay_doubles_and_respects_cap() -> None:
    assert next_retry_delay(1.0, 10.0) == 2.0
    assert next_retry_delay(8.0, 10.0) == 10.0


def test_completed_bar_distance_detects_downtime_without_string_format_bias() -> None:
    assert (
        completed_bar_distance(
            "2026-07-28T07:00:00+00:00",
            "2026-07-28T19:00:00Z",
        )
        == 144
    )
    assert (
        completed_bar_distance(
            "2026-07-28T19:00:00+00:00",
            "2026-07-28T19:05:00Z",
        )
        == 1
    )


def test_catchup_keeps_ordered_tick_paths_for_every_recovered_bar() -> None:
    base = datetime(2026, 7, 28, 7, 5, tzinfo=UTC)

    class Mt5:
        TIMEFRAME_M5 = 5
        COPY_TICKS_ALL = 0

        @staticmethod
        def copy_rates_range(*_args):
            return [
                {
                    "time": base.timestamp(),
                    "open": 4000.0,
                    "high": 4001.0,
                    "low": 3999.0,
                    "close": 4000.5,
                    "tick_volume": 2,
                }
            ]

        @staticmethod
        def copy_ticks_range(*_args):
            start = int(base.timestamp() * 1000)
            return [
                {"time_msc": start, "bid": 4000.0, "ask": 4000.2},
                {"time_msc": start + 299_000, "bid": 4000.5, "ask": 4000.7},
            ]

        @staticmethod
        def last_error():
            return 0, "ok"

    bars = fetch_catchup_bars(
        Mt5(),
        after_timestamp="2026-07-28T07:00:00+00:00",
        through_timestamp="2026-07-28T07:05:00+00:00",
        clock=BrokerClock(),
    )

    assert len(bars) == 1
    assert bars[0]["executable_tick_path"] == [
        [int(base.timestamp() * 1000), 4000.0, 4000.2],
        [int(base.timestamp() * 1000) + 299_000, 4000.5, 4000.7],
    ]
    assert bars[0]["first_tick_msc"] < bars[0]["last_tick_msc"]


def test_generation_delay_is_measured_from_expected_m5_boundary() -> None:
    assert (
        forecast_generation_delay_ms(
            "2026-07-28T10:00:00+00:00",
            now_utc=datetime(2026, 7, 28, 10, 5, 9, tzinfo=UTC),
        )
        == 9_000
    )
