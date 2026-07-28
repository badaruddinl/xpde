from __future__ import annotations

import argparse
import json
import math
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean
from typing import Any

from .contracts import HORIZONS, QUANTILES, Bar, validate_quantiles, validate_snapshot
from .dataset import BARRIER_HORIZON, BARRIER_SPEC_ID, barrier_prices
from .features import log_returns, true_ranges


def percentile(values: list[float], probability: float) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile from an empty sample")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def forward_returns(bars: list[Bar], horizon: int) -> list[float]:
    return [
        math.log(bars[index + horizon].close / bars[index].close)
        for index in range(len(bars) - horizon)
        if bars[index].close > 0 and bars[index + horizon].close > 0
    ]


def forecast_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    bars = validate_snapshot(snapshot)
    returns = log_returns(bars)
    recent_bias = fmean(returns[-12:])
    points = []
    direction_samples: list[float] = []
    observed_coverage = 0.0
    calibration_size = 0

    for horizon in HORIZONS:
        samples = forward_returns(bars, horizon)
        if len(samples) < 24:
            raise ValueError(f"not enough forward-return samples for horizon {horizon}")
        split = max(18, int(len(samples) * 0.70))
        fit_samples = samples[:split]
        calibration_samples = samples[split:]
        # A conservative direct-horizon baseline: empirical distribution shifted
        # only slightly toward the most recent momentum.
        shift = recent_bias * horizon * 0.25
        values = [percentile(fit_samples, quantile) + shift for quantile in QUANTILES]
        validate_quantiles(values)
        points.append(
            {
                "horizon_bars": horizon,
                "q10": values[0],
                "q25": values[1],
                "q50": values[2],
                "q75": values[3],
                "q90": values[4],
            }
        )
        if horizon == 3:
            direction_samples = calibration_samples
            calibration_size = len(calibration_samples)
            observed_coverage = (
                sum(values[0] <= value <= values[4] for value in calibration_samples)
                / calibration_size
            )

    positive = sum(value > 0 for value in direction_samples)
    probability_up = (positive + 1) / (len(direction_samples) + 2)
    ranges = true_ranges(bars)[-48:]
    expected_range = fmean(ranges)
    atr_24 = fmean(true_ranges(bars)[-24:])
    barriers = barrier_prices(bars[-1].close, atr_24)
    previous_returns = returns[: max(1, len(returns) - 48)][-96:]
    recent_returns = returns[-48:]
    previous_mean_abs = fmean(abs(value) for value in previous_returns)
    recent_mean_abs = fmean(abs(value) for value in recent_returns)
    volatility_ratio = recent_mean_abs / max(previous_mean_abs, 1e-9)
    drift_detected = volatility_ratio > 2.5 or volatility_ratio < 0.4
    generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    origin = bars[-1]
    origin_bar_index = int(origin.timestamp.timestamp() // 300)

    return {
        "prediction_id": str(uuid.uuid4()),
        "model_id": "empirical-direct-baseline-v1",
        "feature_version": "goldm-m5-v1",
        "origin_bar_timestamp": origin.timestamp.isoformat().replace("+00:00", "Z"),
        "origin_close": origin.close,
        "origin_bar_index": origin_bar_index,
        "generated_at": generated_at,
        "direction_probability_up": probability_up,
        # The transparent baseline has no trained barrier classifier. Keeping
        # this neutral forces the policy to abstain until a validated model is promoted.
        "barrier_probability_long": 0.50,
        "barrier_probability_short": 0.50,
        "barrier_spec_id": BARRIER_SPEC_ID,
        "barrier_horizon_bars": BARRIER_HORIZON,
        **barriers,
        "expected_mfe_long": expected_range * 1.15,
        "expected_mae_long": expected_range * 0.85,
        "expected_mfe_short": expected_range * 1.15,
        "expected_mae_short": expected_range * 0.85,
        "excursion_modelled": False,
        "calibration": {
            "target_coverage": 0.80,
            "observed_coverage": observed_coverage,
            "sample_size": calibration_size,
        },
        "drift_detected": drift_detected,
        "points": points,
    }


def post_json(url: str, payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status not in (200, 201, 202, 204):
            raise RuntimeError(f"server returned HTTP {response.status}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a transparent XPDE baseline forecast")
    parser.add_argument("snapshot", type=Path, help="MarketSnapshot JSON file")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--api-url", default="http://127.0.0.1:8787/api/v1/forecast")
    parser.add_argument("--post", action="store_true")
    args = parser.parse_args()

    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    forecast = forecast_from_snapshot(snapshot)
    rendered = json.dumps(forecast, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    if args.post:
        post_json(args.api_url, forecast)


if __name__ == "__main__":
    main()
