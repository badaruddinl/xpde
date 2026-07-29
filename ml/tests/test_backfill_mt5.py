from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from xpde_ml.backfill_mt5 import (
    validate_export_bars,
    write_csv,
    write_dataset_manifest,
)


def sample_bars() -> list[dict]:
    start = datetime(2026, 7, 28, 10, 0, tzinfo=UTC)
    bars = []
    for index in range(3):
        bar = {
            "timestamp": (start + timedelta(minutes=5 * index)).isoformat(),
            "open": 4000.0 + index,
            "high": 4001.2 + index,
            "low": 3999.5 + index,
            "close": 4001.0 + index,
            "tick_volume": 100.0 + index,
            "spread_usd": 0.24,
            "tick_size": 0.01,
        }
        for field in ("open", "high", "low", "close"):
            bar[f"bid_{field}"] = bar[field]
            bar[f"ask_{field}"] = bar[field] + 0.24
        bar["executable_tick_count"] = 100 + index
        bar["first_tick_msc"] = int(
            (start + timedelta(minutes=5 * index)).timestamp() * 1000
        )
        bar["last_tick_msc"] = bar["first_tick_msc"] + 299_000
        bar["executable_tick_path"] = [
            [bar["first_tick_msc"], bar["bid_open"], bar["ask_open"]],
            [bar["first_tick_msc"] + 100_000, bar["bid_high"], bar["ask_high"]],
            [bar["first_tick_msc"] + 200_000, bar["bid_low"], bar["ask_low"]],
            [bar["last_tick_msc"], bar["bid_close"], bar["ask_close"]],
        ]
        bar["executable_tick_path_json"] = json.dumps(
            bar["executable_tick_path"], separators=(",", ":")
        )
        bars.append(bar)
    return bars


def test_dataset_manifest_matches_export_hash(tmp_path) -> None:
    bars = sample_bars()
    validation = validate_export_bars(
        bars,
        now_utc=datetime(2026, 7, 28, 11, 0, tzinfo=UTC),
    )
    dataset = tmp_path / "goldm_m5.csv"
    write_csv(dataset, bars)
    manifest_path = write_dataset_manifest(
        dataset,
        bars,
        utc_offset_override_hours=3,
        validation=validation,
        chart_mode="BID",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["row_count"] == 3
    assert manifest["utc_offset_override_hours"] == 3
    assert manifest["contains_incomplete_bar"] is False
    assert manifest["chart_mode"] == "BID"
    assert manifest["executable_side_source"] == "HISTORICAL_BID_ASK_TICKS"
    assert manifest["tick_collection_mode"] == "COPY_TICKS_ALL"
    assert manifest["executable_integrity"]["parity_mismatch_rate"] == 0.0
    assert manifest["executable_integrity"]["tick_path_valid_rate"] == 1.0
    assert isinstance(manifest["git_dirty"], bool)
    assert manifest["sha256"] == hashlib.sha256(dataset.read_bytes()).hexdigest()
    with dataset.open(newline="", encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 3


def test_dataset_validation_rejects_duplicate_timestamp() -> None:
    bars = sample_bars()
    bars[1]["timestamp"] = bars[0]["timestamp"]
    with pytest.raises(ValueError, match="duplicate"):
        validate_export_bars(
            bars,
            now_utc=datetime(2026, 7, 28, 11, 0, tzinfo=UTC),
        )


def test_dataset_validation_reports_market_or_provider_gap() -> None:
    bars = sample_bars()
    bars[2]["timestamp"] = (
        datetime.fromisoformat(bars[1]["timestamp"]) + timedelta(minutes=15)
    ).isoformat()
    bars[2]["first_tick_msc"] = int(
        datetime.fromisoformat(bars[2]["timestamp"]).timestamp() * 1000
    )
    bars[2]["last_tick_msc"] = bars[2]["first_tick_msc"] + 299_000
    for index, tick in enumerate(bars[2]["executable_tick_path"]):
        tick[0] = (
            bars[2]["last_tick_msc"]
            if index == len(bars[2]["executable_tick_path"]) - 1
            else bars[2]["first_tick_msc"] + index * 100_000
        )
    bars[2]["executable_tick_path_json"] = json.dumps(
        bars[2]["executable_tick_path"], separators=(",", ":")
    )
    report = validate_export_bars(
        bars,
        now_utc=datetime(2026, 7, 28, 11, 0, tzinfo=UTC),
    )
    assert report["gap_count"] == 1
    assert report["max_gap_minutes"] == 15
    assert report["maximum_parity_error"] == 0.0
    assert report["bars_without_full_tick_history"] == 0
