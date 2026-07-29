from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
pytest.importorskip("catboost")

from xpde_ml.dataset import BARRIER_SPEC_ID, LABEL_CONTRACT_ID, build_training_frame
from xpde_ml.model_inference import CandidateModel
from xpde_ml.train_catboost import train

IMPORT_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "import-colab-artifact.py"
)
IMPORT_SPEC = importlib.util.spec_from_file_location(
    "xpde_real_import_colab_artifact",
    IMPORT_SCRIPT,
)
assert IMPORT_SPEC and IMPORT_SPEC.loader
importer = importlib.util.module_from_spec(IMPORT_SPEC)
IMPORT_SPEC.loader.exec_module(importer)


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
        high = max(open_price, close) + upper
        low = min(open_price, close) - lower
        rows.append(
            {
                "timestamp": start + timedelta(minutes=5 * index),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "tick_volume": float(100 + index % 40),
                "spread_usd": 0.24,
                "bid_open": open_price,
                "bid_high": high,
                "bid_low": low,
                "bid_close": close,
                "ask_open": open_price + 0.24,
                "ask_high": high + 0.24,
                "ask_low": low + 0.24,
                "ask_close": close + 0.24,
                "executable_tick_count": 100 + index % 40,
                "chart_mode": "BID",
                "tick_size": 0.01,
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
                "schema_version": 3,
                "label_contract_id": LABEL_CONTRACT_ID,
                "sha256": dataset_hash,
                "symbol": "GOLDm#",
                "timeframe": "M5",
                "row_count": len(frame),
                "contains_incomplete_bar": False,
                "git_dirty": False,
                "git_commit": "synthetic-e2e",
                "chart_mode": "BID",
                "executable_side_source": "HISTORICAL_BID_ASK_TICKS",
                "tick_collection_mode": "COPY_TICKS_ALL",
                "executable_integrity": {
                    "parity_mismatch_rate": 0.0,
                    "bars_without_full_tick_history": 0,
                    "minimum_tick_coverage_per_bar": 1.0,
                    "tick_path_valid_rate": 1.0,
                    "tick_size": 0.01,
                },
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
        "symbol_spec": {"tick_size": 0.01, "digits": 2},
        "bars": [
            {
                "timestamp": row.timestamp.isoformat().replace("+00:00", "Z"),
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "tick_volume": row.tick_volume,
                "bid_close": row.bid_close,
                "ask_close": row.ask_close,
                "executable_tick_count": int(row.tick_volume),
            }
            for row in latest.itertuples(index=False)
        ],
    }
    candidate = CandidateModel(
        output,
        allow_ineligible_for_testing=True,
    )
    forecast = candidate.forecast(snapshot)
    assert forecast["model_id"] == manifest["model_id"]
    assert forecast["barrier_spec_id"] == BARRIER_SPEC_ID
    assert forecast["barrier_horizon_bars"] == 3
    assert forecast["stop_price_long"] < forecast["origin_close"]
    assert forecast["target_price_long"] > forecast["origin_close"]
    assert len(forecast["points"]) == 4
    reordered_snapshot = {**snapshot, "bars": list(reversed(snapshot["bars"]))}
    reordered_forecast = candidate.forecast(reordered_snapshot)
    assert reordered_forecast["origin_close"] == forecast["origin_close"]
    assert reordered_forecast["points"] == forecast["points"]
    incomplete_snapshot = copy.deepcopy(snapshot)
    incomplete_snapshot["bars"][-24].pop("ask_close")
    with pytest.raises(ValueError, match="executable feature window requires 24"):
        candidate.forecast(incomplete_snapshot)

    # Exercise the production importer with real CatBoost model files. The
    # synthetic training run uses smoke eligibility by design, so promote its
    # otherwise valid artifact only inside this isolated integration fixture.
    manifest["training_mode"] = "candidate"
    manifest["eligible_for_shadow"] = True
    manifest["eligibility_gates"] = {
        name: True for name in manifest["eligibility_gates"]
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    checksums = []
    for filename in sorted(importer.REQUIRED_ARTIFACT_FILES - {"checksums.sha256"}):
        digest = hashlib.sha256((output / filename).read_bytes()).hexdigest()
        checksums.append(f"{digest}  {filename}")
    (output / "checksums.sha256").write_text(
        "\n".join(checksums) + "\n",
        encoding="utf-8",
    )

    imported = importer.import_candidate(output, tmp_path / "verified-artifacts")

    assert imported.name == manifest["model_id"]
    assert (
        tmp_path / "verified-artifacts" / "latest" / "manifest.json"
    ).is_file()
