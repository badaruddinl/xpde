from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
pytest.importorskip("catboost")

from xpde_ml.dataset import BARRIER_SPEC_ID, build_training_frame
from xpde_ml.model_inference import CandidateModel
from xpde_ml.train_catboost import train


pytestmark = pytest.mark.skipif(
    os.getenv("XPDE_RUN_TRAINING_E2E") != "1",
    reason="set XPDE_RUN_TRAINING_E2E=1 to run the synthetic training pipeline",
)


def test_synthetic_csv_trains_loads_and_forecasts_schema_v3(tmp_path) -> None:
    rng = np.random.default_rng(42)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    price = 3000.0
    for index in range(1400):
        change = float(rng.normal(0.0, 0.72))
        open_price = price
        close = max(100.0, open_price + change)
        upper = float(rng.uniform(0.12, 0.95))
        lower = float(rng.uniform(0.12, 0.95))
        rows.append(
            {
                "timestamp": start + timedelta(minutes=5 * index),
                "open": open_price,
                "high": max(open_price, close) + upper,
                "low": min(open_price, close) - lower,
                "close": close,
                "tick_volume": float(100 + index % 40),
                "spread_usd": 0.24,
            }
        )
        price = close
    frame = pd.DataFrame(rows)
    labelled = build_training_frame(frame)
    for side in ("long", "short"):
        classes = set(
            labelled[f"barrier_{side}_class"].dropna().astype(int).tolist()
        )
        assert classes == {0, 1, 2}

    bars_csv = tmp_path / "synthetic.csv"
    frame.to_csv(bars_csv, index=False)
    dataset_hash = hashlib.sha256(bars_csv.read_bytes()).hexdigest()
    bars_csv.with_suffix(".manifest.json").write_text(
        json.dumps(
            {
                "sha256": dataset_hash,
                "symbol": "GOLDm#",
                "timeframe": "M5",
                "row_count": len(frame),
                "contains_incomplete_bar": False,
                "git_dirty": False,
                "git_commit": "synthetic-e2e",
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "artifact"
    manifest = train(
        SimpleNamespace(
            bars_csv=bars_csv,
            output=output,
            iterations=20,
            folds=2,
            register_url="",
            training_mode="smoke",
            minimum_candidate_rows=20_000,
        )
    )
    assert manifest["schema_version"] == 3
    assert manifest["eligible_for_shadow"] is False
    assert manifest["barrier_spec"]["id"] == BARRIER_SPEC_ID

    latest = frame.tail(500)
    snapshot = {
        "symbol": "GOLDm#",
        "timeframe": "M5",
        "bid": float(latest.iloc[-1]["close"]) - 0.12,
        "ask": float(latest.iloc[-1]["close"]) + 0.12,
        "bars": [
            {
                "timestamp": row.timestamp.isoformat().replace("+00:00", "Z"),
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "tick_volume": row.tick_volume,
            }
            for row in latest.itertuples(index=False)
        ],
    }
    forecast = CandidateModel(output).forecast(snapshot)
    assert forecast["model_id"] == manifest["model_id"]
    assert forecast["barrier_spec_id"] == BARRIER_SPEC_ID
    assert forecast["barrier_horizon_bars"] == 3
    assert forecast["stop_price_long"] < forecast["origin_close"]
    assert forecast["target_price_long"] > forecast["origin_close"]
    assert len(forecast["points"]) == 4
