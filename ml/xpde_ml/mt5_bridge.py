from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .baseline import forecast_from_snapshot
from .time_utils import BrokerClock

SYMBOL = "GOLDm#"
TIMEFRAME = "M5"


class PayloadRejected(RuntimeError):
    def __init__(self, url: str, status: int, body: str) -> None:
        self.url = url
        self.status = status
        self.body = body
        detail = body.strip() or "empty response body"
        super().__init__(f"{url} rejected payload with HTTP {status}: {detail}")


def load_local_env(path: Path = Path(".env")) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip("\"'")


def next_retry_delay(current: float, maximum: float) -> float:
    return min(maximum, max(current * 2, 0.5))


def initialize_mt5(mt5: Any) -> None:
    kwargs: dict[str, Any] = {}
    terminal_path = os.getenv("MT5_PATH")
    login = os.getenv("MT5_LOGIN")
    password = os.getenv("MT5_PASSWORD")
    server = os.getenv("MT5_SERVER")
    if terminal_path:
        kwargs["path"] = terminal_path
    if login:
        kwargs["login"] = int(login)
    if password:
        kwargs["password"] = password
    if server:
        kwargs["server"] = server
    if not mt5.initialize(**kwargs):
        code, message = mt5.last_error()
        raise RuntimeError(f"MetaTrader5 initialization failed ({code}): {message}")


def build_snapshot(
    mt5: Any,
    bar_count: int = 500,
    broker_clock: BrokerClock | None = None,
) -> dict[str, Any]:
    if not mt5.symbol_select(SYMBOL, True):
        raise RuntimeError(f"MetaTrader5 symbol {SYMBOL!r} is unavailable")

    account = mt5.account_info()
    symbol = mt5.symbol_info(SYMBOL)
    tick = mt5.symbol_info_tick(SYMBOL)
    rates = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 1, bar_count)
    current_rates = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 0, 1)
    if account is None or symbol is None or tick is None or rates is None:
        code, message = mt5.last_error()
        raise RuntimeError(f"MetaTrader5 snapshot failed ({code}): {message}")

    tick_time_ms = int(getattr(tick, "time_msc", int(tick.time * 1000)))
    clock = broker_clock or BrokerClock()
    missing_flags: list[str] = []
    if tick.bid <= 0:
        missing_flags.append("BID_MISSING")
    if tick.ask <= tick.bid:
        missing_flags.append("ASK_INVALID")
    if len(rates) < 48:
        missing_flags.append("BARS_INSUFFICIENT")

    bars = [
        {
            "timestamp": clock.iso_utc(float(rate["time"])),
            "open": float(rate["open"]),
            "high": float(rate["high"]),
            "low": float(rate["low"]),
            "close": float(rate["close"]),
            "tick_volume": float(rate["tick_volume"]),
        }
        for rate in rates
    ]
    current_bar = None
    if current_rates is not None and len(current_rates) == 1:
        rate = current_rates[0]
        current_bar = {
            "timestamp": clock.iso_utc(float(rate["time"])),
            "open": float(rate["open"]),
            "high": float(rate["high"]),
            "low": float(rate["low"]),
            "close": float(rate["close"]),
            "tick_volume": float(rate["tick_volume"]),
        }

    return {
        "symbol": SYMBOL,
        "provider": "MetaTrader5",
        "timestamp": clock.iso_utc(tick_time_ms / 1000),
        "timeframe": TIMEFRAME,
        "bid": float(tick.bid),
        "ask": float(tick.ask),
        "bars": bars,
        "current_bar": current_bar,
        "account": {
            "login": int(account.login),
            "server": str(account.server),
            "balance": float(account.balance),
            "equity": float(account.equity),
            "free_margin": float(account.margin_free),
            "leverage": int(account.leverage),
            "currency": str(account.currency),
        },
        "symbol_spec": {
            "description": str(symbol.description),
            "contract_size": float(symbol.trade_contract_size),
            "volume_min": float(symbol.volume_min),
            "volume_max": float(symbol.volume_max),
            "volume_step": float(symbol.volume_step),
            "tick_size": float(symbol.trade_tick_size),
            "tick_value": float(symbol.trade_tick_value),
            "stops_level_points": int(symbol.trade_stops_level),
            "digits": int(symbol.digits),
        },
        "data_quality": {
            "completeness": 1.0 if not missing_flags else 0.0,
            # MT5 Python epoch timestamps are UTC. The polling loop replaces
            # this with a monotonic age based on actual tick changes.
            "tick_age_ms": 0,
            "missing_flags": missing_flags,
            "reason_codes": (
                [f"UTC_PROVIDER_OVERRIDE_HOURS_{clock.offset_hours}"]
                if clock.offset_hours
                else ["UTC_PROVIDER_TIME"]
            ),
        },
    }


def post_payload(api_url: str, payload: dict[str, Any], expected_status: int = 202) -> None:
    request = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status != expected_status:
                body = response.read().decode("utf-8", errors="replace")
                raise PayloadRejected(api_url, response.status, body)
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise PayloadRejected(api_url, error.code, body) from error


def get_payload(api_url: str) -> dict[str, Any]:
    request = urllib.request.Request(api_url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status != 200:
                body = response.read().decode("utf-8", errors="replace")
                raise PayloadRejected(api_url, response.status, body)
            return json.load(response)
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise PayloadRejected(api_url, error.code, body) from error


def fetch_catchup_bars(
    mt5: Any,
    *,
    after_timestamp: str,
    through_timestamp: str,
    clock: BrokerClock,
) -> list[dict[str, Any]]:
    after = datetime.fromisoformat(after_timestamp.replace("Z", "+00:00"))
    through = datetime.fromisoformat(through_timestamp.replace("Z", "+00:00"))
    start = after + timedelta(minutes=5, hours=clock.offset_hours)
    end = through + timedelta(hours=clock.offset_hours)
    if start > end:
        return []
    rates = mt5.copy_rates_range(SYMBOL, mt5.TIMEFRAME_M5, start, end)
    if rates is None:
        code, message = mt5.last_error()
        raise RuntimeError(f"MetaTrader5 catch-up failed ({code}): {message}")
    bars = [
        {
            "timestamp": clock.iso_utc(float(rate["time"])),
            "open": float(rate["open"]),
            "high": float(rate["high"]),
            "low": float(rate["low"]),
            "close": float(rate["close"]),
            "tick_volume": float(rate["tick_volume"]),
        }
        for rate in rates
    ]
    return sorted(
        {
            bar["timestamp"]: bar
            for bar in bars
            if after
            < datetime.fromisoformat(bar["timestamp"].replace("Z", "+00:00"))
            <= through
        }.values(),
        key=lambda bar: bar["timestamp"],
    )


def post_catchup(
    api_url: str,
    bars: list[dict[str, Any]],
    *,
    clock: BrokerClock,
) -> None:
    for start in range(0, len(bars), 5_000):
        post_payload(
            api_url,
            {
                "symbol": SYMBOL,
                "timeframe": TIMEFRAME,
                "provider": "MetaTrader5",
                "broker_offset_hours": clock.offset_hours,
                "reset": False,
                "bars": bars[start : start + 5_000],
            },
            expected_status=201,
        )


def completed_bar_distance(earlier: str, later: str) -> int:
    earlier_time = datetime.fromisoformat(earlier.replace("Z", "+00:00"))
    later_time = datetime.fromisoformat(later.replace("Z", "+00:00"))
    return max(0, int((later_time - earlier_time).total_seconds() // 300))


def main() -> None:
    load_local_env()
    parser = argparse.ArgumentParser(description="Read-only MetaTrader5 bridge for GOLDm#")
    parser.add_argument("--api-url", default="http://127.0.0.1:8787/api/v1/market/snapshot")
    parser.add_argument(
        "--forecast-url",
        default="http://127.0.0.1:8787/api/v1/forecast",
    )
    parser.add_argument(
        "--cursor-url",
        default="http://127.0.0.1:8787/api/v1/market/cursor",
    )
    parser.add_argument(
        "--backfill-url",
        default="http://127.0.0.1:8787/api/v1/market/backfill",
    )
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--max-retry-delay", type=float, default=10.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--snapshot-only", action="store_true")
    parser.add_argument("--print", action="store_true", dest="print_snapshot")
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path(os.environ["XPDE_MODEL_DIR"]) if os.getenv("XPDE_MODEL_DIR") else None,
        help="validated CatBoost artifact directory; otherwise use the baseline",
    )
    args = parser.parse_args()

    try:
        import MetaTrader5 as mt5
    except ImportError as error:
        raise SystemExit(
            "MetaTrader5 is not installed. Run: py -m pip install -e .[mt5]"
        ) from error

    initialize_mt5(mt5)
    try:
        initial_tick = mt5.symbol_info_tick(SYMBOL)
        if initial_tick is None:
            raise RuntimeError("MetaTrader5 did not return an initial tick")
        broker_clock = BrokerClock(
            int(os.getenv("MT5_UTC_OFFSET_OVERRIDE_HOURS", "0"))
        )
        candidate = None
        if args.model_dir:
            from .model_inference import CandidateModel

            candidate = CandidateModel(args.model_dir)
            if not candidate.manifest.get("eligible_for_shadow", False):
                raise RuntimeError(
                    f"candidate {candidate.model_id} did not pass the shadow eligibility gate"
                )
            print(f"XPDE candidate model loaded: {candidate.model_id}", flush=True)
        retry_delay = max(args.interval, 0.5)
        while True:
            try:
                startup_snapshot = build_snapshot(mt5, broker_clock=broker_clock)
                latest_completed_bar = startup_snapshot["bars"][-1]["timestamp"]
                cursor = get_payload(args.cursor_url).get(
                    "last_completed_bar_timestamp"
                )
                startup_had_gap = False
                if cursor and completed_bar_distance(
                    str(cursor), latest_completed_bar
                ) > 0:
                    catchup_bars = fetch_catchup_bars(
                        mt5,
                        after_timestamp=str(cursor),
                        through_timestamp=latest_completed_bar,
                        clock=broker_clock,
                    )
                    post_catchup(args.backfill_url, catchup_bars, clock=broker_clock)
                    startup_had_gap = True
                    print(
                        f"XPDE catch-up appended {len(catchup_bars)} completed M5 bars",
                        flush=True,
                    )
                post_payload(args.api_url, startup_snapshot)
                if not args.snapshot_only and not startup_had_gap:
                    startup_forecast = (
                        candidate.forecast(startup_snapshot)
                        if candidate is not None
                        else forecast_from_snapshot(startup_snapshot)
                    )
                    post_payload(args.forecast_url, startup_forecast)
                    print(
                        f"XPDE startup forecast posted for {latest_completed_bar}",
                        flush=True,
                    )
                break
            except (
                PayloadRejected,
                urllib.error.URLError,
                TimeoutError,
                OSError,
                RuntimeError,
            ) as error:
                print(
                    f"XPDE startup retrying in {retry_delay:.1f}s after: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                if args.once:
                    raise
                time.sleep(retry_delay)
                retry_delay = next_retry_delay(
                    retry_delay, args.max_retry_delay
                )

        # A restart with a history gap must never manufacture retroactive live
        # predictions. A gap-free restart may forecast the latest completed bar.
        last_forecast_bar: str | None = latest_completed_bar
        last_tick_signature: tuple[str, float, float] | None = (
            startup_snapshot["timestamp"],
            startup_snapshot["bid"],
            startup_snapshot["ask"],
        )
        last_tick_change = time.monotonic()
        retry_delay = max(args.interval, 0.5)
        while True:
            try:
                snapshot = build_snapshot(mt5, broker_clock=broker_clock)
                tick_signature = (
                    snapshot["timestamp"],
                    snapshot["bid"],
                    snapshot["ask"],
                )
                if tick_signature != last_tick_signature:
                    last_tick_signature = tick_signature
                    last_tick_change = time.monotonic()
                snapshot["data_quality"]["tick_age_ms"] = int(
                    (time.monotonic() - last_tick_change) * 1000
                )
                if args.print_snapshot:
                    print(json.dumps(snapshot, indent=2))
                latest_bar = snapshot["bars"][-1]["timestamp"]
                skipped_retroactive_forecast = False
                if (
                    last_forecast_bar is not None
                    and completed_bar_distance(last_forecast_bar, latest_bar) > 1
                ):
                    catchup_bars = fetch_catchup_bars(
                        mt5,
                        after_timestamp=last_forecast_bar,
                        through_timestamp=latest_bar,
                        clock=broker_clock,
                    )
                    post_catchup(
                        args.backfill_url,
                        catchup_bars,
                        clock=broker_clock,
                    )
                    last_forecast_bar = latest_bar
                    skipped_retroactive_forecast = True
                    print(
                        "XPDE recovered a downtime gap; historical bars were "
                        f"backfilled ({len(catchup_bars)}) without retroactive "
                        "live predictions",
                        flush=True,
                    )
                post_payload(args.api_url, snapshot)
                if (
                    not args.snapshot_only
                    and not skipped_retroactive_forecast
                    and latest_bar != last_forecast_bar
                ):
                    forecast = (
                        candidate.forecast(snapshot)
                        if candidate is not None
                        else forecast_from_snapshot(snapshot)
                    )
                    post_payload(args.forecast_url, forecast)
                    last_forecast_bar = latest_bar
            except (
                PayloadRejected,
                urllib.error.URLError,
                TimeoutError,
                OSError,
                RuntimeError,
            ) as error:
                print(
                    f"XPDE bridge retrying in {retry_delay:.1f}s after: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                if args.once:
                    raise
                time.sleep(retry_delay)
                retry_delay = next_retry_delay(retry_delay, args.max_retry_delay)
                continue

            retry_delay = max(args.interval, 0.5)
            if args.once:
                break
            time.sleep(max(args.interval, 0.25))
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()
