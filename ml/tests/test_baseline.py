from __future__ import annotations

from datetime import UTC, datetime, timedelta

from xpde_ml.baseline import forecast_from_snapshot, percentile


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
