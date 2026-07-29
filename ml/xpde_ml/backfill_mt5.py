from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .mt5_bridge import SYMBOL, TIMEFRAME, initialize_mt5, load_local_env
from .backfill_transport import post_backfill_payloads
from .dataset_files import dataset_manifest_path
from .dataset import LABEL_CONTRACT_ID
from .executable_bars import (
    collect_executable_bars,
    overlay_executable_bars,
    symbol_chart_mode,
)
from .time_utils import BrokerClock, environment_integer


def validate_export_bars(
    bars: list[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
) -> dict[str, int | float]:
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
    parity_errors: dict[str, list[float]] = {
        field: [] for field in ("open", "high", "low", "close")
    }
    tick_coverages: list[float] = []
    parity_mismatches = 0
    valid_tick_paths = 0
    tick_sizes: set[float] = set()
    for bar in bars:
        tick_size = float(bar.get("tick_size", 0.0))
        if tick_size <= 0.0:
            raise ValueError("dataset broker tick size must be positive")
        tick_sizes.add(tick_size)
        if (
            float(bar["high"]) < max(float(bar["open"]), float(bar["close"]))
            or float(bar["low"]) > min(float(bar["open"]), float(bar["close"]))
        ):
            raise ValueError("dataset contains an invalid OHLC candle")
        for side in ("bid", "ask"):
            side_open = float(bar[f"{side}_open"])
            side_high = float(bar[f"{side}_high"])
            side_low = float(bar[f"{side}_low"])
            side_close = float(bar[f"{side}_close"])
            if (
                side_open <= 0.0
                or side_close <= 0.0
                or side_high < max(side_open, side_close)
                or side_low > min(side_open, side_close)
            ):
                raise ValueError(
                    f"dataset contains an invalid executable {side.upper()} candle"
                )
        if any(
            float(bar[f"ask_{field}"]) <= float(bar[f"bid_{field}"])
            for field in ("open", "high", "low", "close")
        ):
            raise ValueError("dataset executable Ask OHLC is not above Bid")
        if int(bar.get("executable_tick_count", 0)) <= 0:
            raise ValueError("dataset executable-side candle has no source ticks")
        if int(bar.get("first_tick_msc", 0)) <= 0 or int(
            bar.get("last_tick_msc", 0)
        ) < int(bar.get("first_tick_msc", 0)):
            raise ValueError("dataset executable-side candle has invalid tick boundaries")
        bar_start_msc = int(
            datetime.fromisoformat(
                str(bar["timestamp"]).replace("Z", "+00:00")
            ).timestamp()
            * 1000
        )
        if not (
            bar_start_msc <= int(bar["first_tick_msc"]) < bar_start_msc + 300_000
            and bar_start_msc <= int(bar["last_tick_msc"]) < bar_start_msc + 300_000
        ):
            raise ValueError("dataset tick boundaries are outside their normalized M5 bar")
        raw_path = bar.get("executable_tick_path")
        if raw_path is None:
            try:
                raw_path = json.loads(str(bar.get("executable_tick_path_json", "[]")))
            except json.JSONDecodeError as error:
                raise ValueError("dataset tick path is not valid JSON") from error
        if not isinstance(raw_path, list) or not raw_path:
            raise ValueError("dataset executable tick path is missing")
        try:
            path = [
                (int(item[0]), float(item[1]), float(item[2]))
                for item in raw_path
            ]
        except (IndexError, TypeError, ValueError) as error:
            raise ValueError("dataset tick path item is invalid") from error
        if (
            path[0][0] != int(bar["first_tick_msc"])
            or path[-1][0] != int(bar["last_tick_msc"])
            or any(left[0] > right[0] for left, right in zip(path, path[1:]))
            or any(bid <= 0.0 or ask <= bid for _, bid, ask in path)
        ):
            raise ValueError("dataset tick path boundaries or ordering are invalid")
        reconstructed = {
            "bid_open": path[0][1],
            "bid_high": max(item[1] for item in path),
            "bid_low": min(item[1] for item in path),
            "bid_close": path[-1][1],
            "ask_open": path[0][2],
            "ask_high": max(item[2] for item in path),
            "ask_low": min(item[2] for item in path),
            "ask_close": path[-1][2],
        }
        if any(
            abs(float(bar[field]) - value) > 1e-12
            for field, value in reconstructed.items()
        ):
            raise ValueError("dataset tick path does not reconstruct executable OHLC")
        valid_tick_paths += 1
        tolerance = tick_size
        row_mismatch = False
        for field in parity_errors:
            error = abs(float(bar[field]) - float(bar[f"bid_{field}"]))
            parity_errors[field].append(error)
            row_mismatch = row_mismatch or error > tolerance
        parity_mismatches += int(row_mismatch)
        tick_volume = max(float(bar.get("tick_volume", 0.0)), 1.0)
        tick_coverages.append(
            min(1.0, int(bar["executable_tick_count"]) / tick_volume)
        )
        exact_spread = float(bar["ask_close"]) - float(bar["bid_close"])
        if abs(float(bar.get("spread_usd", exact_spread)) - exact_spread) > 1e-12:
            raise ValueError("dataset spread feature is not the exact executable close spread")

    if len(tick_sizes) != 1:
        raise ValueError("dataset contains more than one broker tick size")
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
        "bid_chart_open_error_max": max(parity_errors["open"], default=0.0),
        "bid_chart_high_error_max": max(parity_errors["high"], default=0.0),
        "bid_chart_low_error_max": max(parity_errors["low"], default=0.0),
        "bid_chart_close_error_max": max(parity_errors["close"], default=0.0),
        "maximum_parity_error": max(
            (value for values in parity_errors.values() for value in values),
            default=0.0,
        ),
        "parity_mismatch_rate": parity_mismatches / len(bars),
        "minimum_tick_coverage_per_bar": min(tick_coverages, default=0.0),
        "mean_tick_coverage_per_bar": sum(tick_coverages) / len(tick_coverages),
        "bars_without_full_tick_history": sum(
            coverage < 0.95 for coverage in tick_coverages
        ),
        "tick_path_valid_rate": valid_tick_paths / len(bars),
        "tick_size": next(iter(tick_sizes)),
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
    validation: dict[str, int | float],
    chart_mode: str,
) -> Path:
    digest = hashlib.sha256(dataset_path.read_bytes()).hexdigest()
    manifest_path = dataset_manifest_path(dataset_path)
    manifest = {
        "schema_version": 3,
        "label_contract_id": LABEL_CONTRACT_ID,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "provider": "MetaTrader5",
        "chart_mode": chart_mode,
        "executable_side_source": "HISTORICAL_BID_ASK_TICKS",
        "tick_collection_mode": "COPY_TICKS_ALL",
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
        "executable_integrity": {
            key: value
            for key, value in validation.items()
            if key not in {"gap_count", "max_gap_minutes"}
        },
        "contains_incomplete_bar": False,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def fetch_bars(mt5: Any, count: int) -> tuple[list[dict[str, Any]], int, str]:
    if not mt5.symbol_select(SYMBOL, True):
        raise RuntimeError(f"MetaTrader5 symbol {SYMBOL!r} is unavailable")
    tick = mt5.symbol_info_tick(SYMBOL)
    symbol = mt5.symbol_info(SYMBOL)
    rates = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 1, count)
    if tick is None or symbol is None or rates is None:
        code, message = mt5.last_error()
        raise RuntimeError(f"MetaTrader5 backfill failed ({code}): {message}")
    clock = BrokerClock(environment_integer("MT5_UTC_OFFSET_OVERRIDE_HOURS"))
    point = float(symbol.point)
    chart_mode = symbol_chart_mode(mt5, symbol)
    bars = [
        {
            "timestamp": clock.iso_utc(float(rate["time"])),
            "open": float(rate["open"]),
            "high": float(rate["high"]),
            "low": float(rate["low"]),
            "close": float(rate["close"]),
            "tick_volume": float(rate["tick_volume"]),
            "spread_usd": float(rate["spread"]) * point,
            "tick_size": float(symbol.trade_tick_size),
            "chart_mode": chart_mode,
        }
        for rate in rates
    ]
    bars.sort(key=lambda bar: bar["timestamp"])
    if not bars:
        return bars, clock.offset_hours, chart_mode
    raw_start = min(float(rate["time"]) for rate in rates)
    raw_end = max(float(rate["time"]) for rate in rates) + 300.0
    executable, _ = collect_executable_bars(
        mt5,
        symbol=SYMBOL,
        start_epoch=raw_start,
        end_epoch=raw_end,
        clock=clock,
    )
    overlaid = overlay_executable_bars(
        bars,
        executable,
        include_tick_path=True,
    )
    for bar in overlaid:
        if "ask_close" in bar and "bid_close" in bar:
            bar["spread_usd"] = float(bar["ask_close"]) - float(bar["bid_close"])
    return (overlaid, clock.offset_hours, chart_mode)


def write_csv(path: Path, bars: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [field for field in bars[0] if field != "executable_tick_path"]
    handle = (
        gzip.open(path, "wt", newline="", encoding="utf-8")
        if path.suffix.lower() == ".gz"
        else path.open("w", newline="", encoding="utf-8")
    )
    with handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
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
    return post_backfill_payloads(
        api_url,
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        provider="MetaTrader5",
        broker_offset_hours=broker_offset_hours,
        bars=bars,
        reset=reset,
        max_bars_per_request=chunk_size,
    )


def main() -> None:
    load_local_env()
    parser = argparse.ArgumentParser(description="Backfill completed GOLDm# M5 bars from MT5")
    parser.add_argument("--bars", type=int, default=50_000)
    parser.add_argument("--output", type=Path, default=Path("data/goldm_m5.csv.gz"))
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
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=250,
        help="maximum bars per request; byte and tick-point limits are also enforced",
    )
    args = parser.parse_args()

    try:
        import MetaTrader5 as mt5
    except ImportError as error:
        raise SystemExit("Install MT5 extras: py -m pip install -e .[mt5]") from error

    initialize_mt5(mt5)
    try:
        bars, offset, chart_mode = fetch_bars(mt5, args.bars)
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
        chart_mode=chart_mode,
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
