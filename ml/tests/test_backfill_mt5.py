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
    return [
        {
            "timestamp": (start + timedelta(minutes=5 * index)).isoformat(),
            "open": 4000.0 + index,
            "high": 4001.2 + index,
            "low": 3999.5 + index,
            "close": 4001.0 + index,
            "tick_volume": 100.0 + index,
            "spread_usd": 0.24,
        }
        for index in range(3)
    ]


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
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["row_count"] == 3
    assert manifest["utc_offset_override_hours"] == 3
    assert manifest["contains_incomplete_bar"] is False
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
    report = validate_export_bars(
        bars,
        now_utc=datetime(2026, 7, 28, 11, 0, tzinfo=UTC),
    )
    assert report == {"gap_count": 1, "max_gap_minutes": 15}
