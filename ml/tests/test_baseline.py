from __future__ import annotations

from datetime import UTC, datetime, timedelta

from xpde_ml.baseline import forecast_from_snapshot, forward_returns, percentile
from xpde_ml.contracts import Bar, deterministic_prediction_id


def sample_snapshot() -> dict:
    now = datetime.now(UTC)
    bars = []
    price = 3300.0
    for index in range(96):
        close = price + ((index % 7) - 3) * 0.11 + 0.04
        bars.append(
            {
                "timestamp": (now + timedelta(minutes=index * 5)).isoformat(),
                "open": price,
                "high": max(price, close) + 0.2,
                "low": min(price, close) - 0.2,
                "close": close,
                "tick_volume": 100 + index,
            }
        )
        price = close
    return {
        "symbol": "GOLDm#",
        "timeframe": "M5",
        "bid": price - 0.12,
        "ask": price + 0.12,
        "symbol_spec": {"tick_size": 0.01, "digits": 2},
        "bars": bars,
    }


def test_percentile_interpolates() -> None:
    assert percentile([0.0, 10.0], 0.25) == 2.5


def test_baseline_emits_monotonic_direct_horizons() -> None:
    forecast = forecast_from_snapshot(sample_snapshot())
    assert [point["horizon_bars"] for point in forecast["points"]] == [1, 3, 6, 12]
    for point in forecast["points"]:
        quantiles = [point[name] for name in ("q10", "q25", "q50", "q75", "q90")]
        assert quantiles == sorted(quantiles)
    assert 0.0 <= forecast["direction_probability_up"] <= 1.0


def test_prediction_id_is_deterministic_for_same_contract_origin() -> None:
    snapshot = sample_snapshot()
    first = forecast_from_snapshot(snapshot)
    second = forecast_from_snapshot(snapshot)
    assert first["prediction_id"] == second["prediction_id"]


def test_prediction_id_changes_when_label_contract_changes() -> None:
    common = {
        "model_id": "candidate",
        "symbol": "GOLDm#",
        "timeframe": "M5",
        "origin_bar_timestamp": "2026-01-01T00:00:00Z",
        "feature_version": "goldm-m5-v5",
        "barrier_spec_id": "barrier-v6",
    }
    first = deterministic_prediction_id(
        **common,
        label_contract_id="exact-contiguous-m5-horizons-v1",
    )
    second = deterministic_prediction_id(
        **common,
        label_contract_id="different-label-contract",
    )
    assert first != second


def test_transparent_baseline_forward_returns_do_not_cross_gaps() -> None:
    start = datetime(2026, 1, 2, 22, 50, tzinfo=UTC)
    timestamps = [
        start,
        start + timedelta(minutes=5),
        start + timedelta(days=2),
        start + timedelta(days=2, minutes=5),
    ]
    bars = [
        Bar(timestamp, 100.0, 101.0, 99.0, 100.0 + index, 100.0)
        for index, timestamp in enumerate(timestamps)
    ]
    assert len(forward_returns(bars, 1)) == 2
    assert forward_returns(bars, 3) == []
