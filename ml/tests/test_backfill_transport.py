from __future__ import annotations

import gzip
import json
import urllib.request

import pytest

from xpde_ml.backfill_transport import iter_backfill_payloads, post_backfill_payloads


def bar(index: int, points: int = 2, padding: int = 0) -> dict:
    start = 1_700_000_000_000 + index * 300_000
    return {
        "timestamp": f"2026-07-29T10:{index * 5:02d}:00+00:00",
        "open": 4000.0,
        "high": 4001.0,
        "low": 3999.0,
        "close": 4000.5,
        "tick_volume": float(points),
        "bid_open": 4000.0,
        "bid_high": 4001.0,
        "bid_low": 3999.0,
        "bid_close": 4000.5,
        "ask_open": 4000.2,
        "ask_high": 4001.2,
        "ask_low": 3999.2,
        "ask_close": 4000.7,
        "executable_tick_count": points,
        "first_tick_msc": start,
        "last_tick_msc": start + points - 1,
        "executable_tick_path": [
            [start + point, 4000.0 + point / 100, 4000.2 + point / 100]
            for point in range(points)
        ],
        "padding": "x" * padding,
    }


def payloads(bars: list[dict], *, reset: bool = True, **limits):
    return list(
        iter_backfill_payloads(
            symbol="GOLDm#",
            timeframe="M5",
            provider="MT5",
            broker_offset_hours=0,
            bars=bars,
            reset=reset,
            **limits,
        )
    )


def test_reset_chunks_have_one_stable_import_and_only_promote_at_the_end() -> None:
    chunks = payloads(
        [bar(0), bar(1), bar(2)],
        max_bars_per_request=1,
        max_payload_bytes=20_000,
        max_tick_points_per_request=20,
    )

    assert len(chunks) == 3
    import_ids = {payload["import_id"] for payload, _ in chunks}
    assert len(import_ids) == 1
    assert None not in import_ids
    assert [payload["reset"] for payload, _ in chunks] == [True, False, False]
    assert [payload["final_chunk"] for payload, _ in chunks] == [
        False,
        False,
        True,
    ]
    assert [payload["chunk_index"] for payload, _ in chunks] == [0, 1, 2]
    assert [payload["total_chunks"] for payload, _ in chunks] == [3, 3, 3]


def test_append_chunks_are_independently_idempotent_without_import_id() -> None:
    chunks = payloads(
        [bar(0), bar(1)],
        reset=False,
        max_bars_per_request=1,
        max_payload_bytes=20_000,
        max_tick_points_per_request=20,
    )

    assert all(payload["import_id"] is None for payload, _ in chunks)
    assert all(payload["chunk_index"] is None for payload, _ in chunks)
    assert all(payload["total_chunks"] is None for payload, _ in chunks)
    assert all(payload["reset"] is False for payload, _ in chunks)
    assert chunks[-1][0]["final_chunk"] is True


def test_chunking_respects_encoded_bytes_and_tick_point_limits() -> None:
    chunks = payloads(
        [bar(0, points=3, padding=300), bar(1, points=3, padding=300)],
        max_bars_per_request=10,
        max_payload_bytes=1_400,
        max_tick_points_per_request=4,
    )

    assert len(chunks) == 2
    for payload, encoded in chunks:
        assert len(encoded) <= 1_400
        assert (
            sum(len(item["executable_tick_path"]) for item in payload["bars"]) <= 4
        )
        assert json.loads(encoded) == payload


def test_one_oversized_bar_is_rejected_instead_of_being_truncated() -> None:
    with pytest.raises(ValueError, match="maximum payload size"):
        payloads(
            [bar(0, padding=10_000)],
            max_bars_per_request=10,
            max_payload_bytes=1_000,
            max_tick_points_per_request=20,
        )


def test_bar_without_tick_path_is_rejected() -> None:
    value = bar(0)
    value["executable_tick_path"] = []
    with pytest.raises(ValueError, match="has no executable tick path"):
        payloads([value])


def test_bars_must_be_strictly_ordered_without_duplicates() -> None:
    with pytest.raises(ValueError, match="strictly ordered"):
        payloads([bar(1), bar(0)])
    with pytest.raises(ValueError, match="strictly ordered"):
        payloads([bar(0), bar(0)])


def test_bar_timestamp_requires_an_explicit_offset() -> None:
    value = bar(0)
    value["timestamp"] = "2026-07-29T10:00:00"
    with pytest.raises(ValueError, match="UTC offset"):
        payloads([value])


def test_post_uses_gzip_without_changing_the_json_contract(monkeypatch) -> None:
    observed: list[tuple[dict, str | None]] = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def read():
            return b'{"inserted":1}'

    def open_request(request, **_kwargs):
        observed.append(
            (
                json.loads(gzip.decompress(request.data)),
                request.get_header("Content-encoding"),
            )
        )
        return Response()

    monkeypatch.setattr(urllib.request, "urlopen", open_request)

    assert (
        post_backfill_payloads(
            "http://127.0.0.1/backfill",
            symbol="GOLDm#",
            timeframe="M5",
            provider="MT5",
            broker_offset_hours=0,
            bars=[bar(0)],
            reset=False,
        )
        == 1
    )
    assert observed[0][0]["bars"][0]["timestamp"] == bar(0)["timestamp"]
    assert observed[0][1] == "gzip"
