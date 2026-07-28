from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .mt5_bridge import SYMBOL, TIMEFRAME, initialize_mt5, load_local_env
from .time_utils import BrokerClock


def validate_export_bars(
    bars: list[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
) -> dict[str, int]:
    if not bars:
        raise ValueError("dataset export contains no completed bars")
    timestamps = [
        datetime.fromisoformat(str(bar["timestamp"]).replace("Z", "+00:00"))
        for bar in bars
    ]
    if timestamps != sorted(timestamps):
        raise ValueError("dataset timestamps are not sorted")
    if len(set(timestamps)) != len(timestamps):
        raise ValueError("dataset contains duplicate timestamps")
    if any(int(timestamp.timestamp()) % 300 != 0 for timestamp in timestamps):
        raise ValueError("dataset contains a non-aligned M5 timestamp")
    for bar in bars:
        if (
            float(bar["high"]) < max(float(bar["open"]), float(bar["close"]))
            or float(bar["low"]) > min(float(bar["open"]), float(bar["close"]))
        ):
            raise ValueError("dataset contains an invalid OHLC candle")

    reference = now_utc or datetime.now(UTC)
    latest_completed_start = int(reference.timestamp() // 300) * 300 - 300
    if timestamps[-1].timestamp() > latest_completed_start:
        raise ValueError("dataset contains a current or incomplete M5 candle")
    gaps = [
        int((later - earlier).total_seconds() // 60)
        for earlier, later in zip(timestamps, timestamps[1:], strict=False)
        if (later - earlier).total_seconds() > 300
    ]
    return {
        "gap_count": len(gaps),
        "max_gap_minutes": max(gaps, default=0),
    }


def _git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def _git_dirty() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode != 0 or bool(result.stdout.strip())


def write_dataset_manifest(
    dataset_path: Path,
    bars: list[dict[str, Any]],
    *,
    utc_offset_override_hours: int,
    validation: dict[str, int],
) -> Path:
    digest = hashlib.sha256(dataset_path.read_bytes()).hexdigest()
    manifest_path = dataset_path.with_suffix(".manifest.json")
    manifest = {
        "schema_version": 1,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "provider": "MetaTrader5",
        "first_timestamp": bars[0]["timestamp"],
        "last_timestamp": bars[-1]["timestamp"],
        "row_count": len(bars),
        "sha256": digest,
        "exported_at": datetime.now(UTC).isoformat(),
        "git_commit": _git_commit(),
        "git_dirty": _git_dirty(),
        "utc_offset_override_hours": utc_offset_override_hours,
        "gap_count": validation["gap_count"],
        "max_gap_minutes": validation["max_gap_minutes"],
        "contains_incomplete_bar": False,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def fetch_bars(mt5: Any, count: int) -> tuple[list[dict[str, Any]], int]:
    if not mt5.symbol_select(SYMBOL, True):
        raise RuntimeError(f"MetaTrader5 symbol {SYMBOL!r} is unavailable")
    tick = mt5.symbol_info_tick(SYMBOL)
    symbol = mt5.symbol_info(SYMBOL)
    rates = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 1, count)
    if tick is None or symbol is None or rates is None:
        code, message = mt5.last_error()
        raise RuntimeError(f"MetaTrader5 backfill failed ({code}): {message}")
    clock = BrokerClock(int(os.getenv("MT5_UTC_OFFSET_OVERRIDE_HOURS", "0")))
    point = float(symbol.point)
    bars = [
        {
            "timestamp": clock.iso_utc(float(rate["time"])),
            "open": float(rate["open"]),
            "high": float(rate["high"]),
            "low": float(rate["low"]),
            "close": float(rate["close"]),
            "tick_volume": float(rate["tick_volume"]),
            "spread_usd": float(rate["spread"]) * point,
        }
        for rate in rates
    ]
    bars.sort(key=lambda bar: bar["timestamp"])
    return bars, clock.offset_hours


def write_csv(path: Path, bars: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(bars[0]))
        writer.writeheader()
        writer.writerows(bars)


def post_chunks(
    api_url: str,
    bars: list[dict[str, Any]],
    broker_offset_hours: int,
    chunk_size: int,
    *,
    reset: bool,
) -> int:
    accepted = 0
    for start in range(0, len(bars), chunk_size):
        chunk = bars[start : start + chunk_size]
        payload = {
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "provider": "MetaTrader5",
            "broker_offset_hours": broker_offset_hours,
            "reset": reset and start == 0,
            "bars": [
                {key: value for key, value in bar.items() if key != "spread_usd"}
                for bar in chunk
            ],
        }
        request = urllib.request.Request(
            api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            result = json.load(response)
            accepted += int(result["inserted"])
    return accepted


def main() -> None:
    load_local_env()
    parser = argparse.ArgumentParser(description="Backfill completed GOLDm# M5 bars from MT5")
    parser.add_argument("--bars", type=int, default=50_000)
    parser.add_argument("--output", type=Path, default=Path("data/goldm_m5.csv"))
    parser.add_argument(
        "--api-url",
        default="http://127.0.0.1:8787/api/v1/market/backfill",
    )
    parser.add_argument("--no-post", action="store_true")
    parser.add_argument(
        "--append",
        action="store_true",
        help="do not replace the existing GOLDm# M5 history before importing",
    )
    parser.add_argument("--chunk-size", type=int, default=2_000)
    args = parser.parse_args()

    try:
        import MetaTrader5 as mt5
    except ImportError as error:
        raise SystemExit("Install MT5 extras: py -m pip install -e .[mt5]") from error

    initialize_mt5(mt5)
    try:
        bars, offset = fetch_bars(mt5, args.bars)
    finally:
        mt5.shutdown()
    if not bars:
        raise SystemExit("MT5 returned no completed bars")
    validation = validate_export_bars(bars)
    write_csv(args.output, bars)
    manifest_path = write_dataset_manifest(
        args.output,
        bars,
        utc_offset_override_hours=offset,
        validation=validation,
    )
    inserted = (
        0
        if args.no_post
        else post_chunks(
            args.api_url,
            bars,
            offset,
            args.chunk_size,
            reset=not args.append,
        )
    )
    print(
        json.dumps(
            {
                "bars": len(bars),
                "inserted": inserted,
                "broker_offset_hours": offset,
                "first_timestamp": bars[0]["timestamp"],
                "last_timestamp": bars[-1]["timestamp"],
                "output": str(args.output),
                "manifest": str(manifest_path),
                **validation,
            }
        )
    )


if __name__ == "__main__":
    main()
