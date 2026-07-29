from __future__ import annotations

import io
import urllib.error
import urllib.request

import pytest

from xpde_ml.mt5_bridge import (
    PayloadRejected,
    completed_bar_distance,
    next_retry_delay,
    post_payload,
)


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
