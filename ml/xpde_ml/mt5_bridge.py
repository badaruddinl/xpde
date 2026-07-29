from __future__ import annotations

import argparse
from dataclasses import dataclass
from functools import lru_cache
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any
import tomllib

from .backfill_transport import post_backfill_payloads
from .baseline import forecast_from_snapshot
from .contracts import (
    HORIZONS,
    has_complete_executable_feature_window,
    has_complete_tick_coverage,
)
from .executable_bars import (
    CHART_MODE_BID,
    ExecutableBarTracker,
    collect_executable_bars,
    overlay_executable_bars,
    symbol_chart_mode,
)
from .time_utils import BrokerClock, environment_integer

SYMBOL = "GOLDm#"
TIMEFRAME = "M5"
MAX_TICK_AGE_MS = 10_000
MAX_FORECAST_GENERATION_DELAY_MS = 10_000


@dataclass(frozen=True)
class MarketCalendar:
    sessions: dict[int, tuple[tuple[datetime_time, datetime_time], ...]]
    closed_dates: frozenset[str]


def _parse_market_time(value: str) -> datetime_time:
    hour, minute = (int(part) for part in value.split(":", 1))
    return datetime_time(hour, minute)


@lru_cache(maxsize=4)
def load_market_calendar(config_path: str) -> MarketCalendar:
    names = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    default_sessions = {
        day: ((datetime_time(1, 0), datetime_time(23, 59, 59)),)
        for day in range(5)
    }
    try:
        config = tomllib.loads(Path(config_path).read_text(encoding="utf-8"))
    except OSError as error:
        raise RuntimeError(f"market-session config is unavailable: {config_path}") from error
    except tomllib.TOMLDecodeError as error:
        raise RuntimeError(f"market-session config is invalid: {config_path}") from error
    raw = config.get("market_session", {})
    if raw.get("timezone") != "fixed_broker_utc_offset":
        raise RuntimeError(
            "market_session.timezone must be 'fixed_broker_utc_offset'"
        )
    sessions: dict[int, tuple[tuple[datetime_time, datetime_time], ...]] = {}
    for name, day in names.items():
        configured = raw.get(name)
        if configured is None:
            if day in default_sessions:
                sessions[day] = default_sessions[day]
            continue
        sessions[day] = tuple(
            (_parse_market_time(start), _parse_market_time(end))
            for start, end in configured
        )
    configured_closed = {str(value) for value in raw.get("closed_dates", [])}
    environment_closed = {
        value.strip()
        for value in os.getenv("XPDE_MARKET_CLOSED_DATES", "").split(",")
        if value.strip()
    }
    closed_dates = configured_closed | environment_closed
    for value in closed_dates:
        try:
            date.fromisoformat(value)
        except ValueError as error:
            raise RuntimeError(f"invalid market closed date: {value}") from error
    return MarketCalendar(
        sessions=sessions,
        closed_dates=frozenset(closed_dates),
    )


def infer_market_status(
    *,
    now_utc: datetime,
    absolute_tick_age_ms: int,
    terminal_connected: bool,
    market_utc_offset_hours: int = 0,
    calendar: MarketCalendar | None = None,
    trade_mode_enabled: bool = True,
) -> str:
    if not terminal_connected:
        return "BRIDGE_DISCONNECTED"
    if not trade_mode_enabled:
        return "MARKET_CLOSED"
    calendar = calendar or MarketCalendar(
        sessions={
            day: ((datetime_time(1, 0), datetime_time(23, 59, 59)),)
            for day in range(5)
        },
        closed_dates=frozenset(),
    )
    if market_session_open_until(
        now_utc=now_utc,
        market_utc_offset_hours=market_utc_offset_hours,
        calendar=calendar,
    ) is None:
        return "MARKET_CLOSED"
    if absolute_tick_age_ms > MAX_TICK_AGE_MS:
        return "FEED_STALE"
    return "OPEN"


def market_session_open_until(
    *,
    now_utc: datetime,
    market_utc_offset_hours: int,
    calendar: MarketCalendar,
) -> datetime | None:
    """Return the UTC end of the broker session containing ``now_utc``."""

    market_time = now_utc + timedelta(hours=market_utc_offset_hours)
    if market_time.date().isoformat() in calendar.closed_dates:
        return None
    candidates: list[tuple[datetime, datetime]] = []
    for day_delta in (-1, 0):
        local_day = market_time.date() + timedelta(days=day_delta)
        if local_day.isoformat() in calendar.closed_dates:
            continue
        for start, end in calendar.sessions.get(local_day.weekday(), ()):
            local_start = datetime.combine(local_day, start, tzinfo=UTC)
            local_end_day = local_day + timedelta(days=1) if end < start else local_day
            local_end = datetime.combine(local_end_day, end, tzinfo=UTC)
            candidates.append((local_start, local_end))
    local_now = market_time
    for local_start, local_end in candidates:
        if local_start <= local_now <= local_end:
            return local_end - timedelta(hours=market_utc_offset_hours)
    return None


class PayloadRejected(RuntimeError):
    def __init__(self, url: str, status: int, body: str) -> None:
        self.url = url
        self.status = status
        self.body = body
        detail = body.strip() or "empty response body"
        super().__init__(f"{url} rejected payload with HTTP {status}: {detail}")


RECOVERABLE_BRIDGE_ERRORS = (
    PayloadRejected,
    urllib.error.URLError,
    TimeoutError,
    OSError,
    RuntimeError,
)


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


def forecast_with_candidate(
    snapshot: dict[str, Any],
    candidate: Any | None,
) -> dict[str, Any]:
    if candidate is None:
        return forecast_from_snapshot(snapshot)
    try:
        return candidate.forecast(snapshot)
    except ValueError as error:
        raise RuntimeError(
            f"candidate inference rejected the current feature frame: {error}"
        ) from error


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
    *,
    executable_bars: dict[str, dict[str, Any]] | None = None,
    transport_tick_age_ms: int = 0,
    now_utc: datetime | None = None,
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

    margin_buy = None
    margin_sell = None
    order_calc_margin = getattr(mt5, "order_calc_margin", None)
    if callable(order_calc_margin):
        try:
            margin_buy = order_calc_margin(
                mt5.ORDER_TYPE_BUY, SYMBOL, 1.0, float(tick.ask)
            )
            margin_sell = order_calc_margin(
                mt5.ORDER_TYPE_SELL, SYMBOL, 1.0, float(tick.bid)
            )
        except Exception:
            # Some brokers do not expose margin calculation until the market is open.
            margin_buy = None
            margin_sell = None

    raw_trade_mode = int(getattr(symbol, "trade_mode", -1))
    trade_modes = {
        int(getattr(mt5, "SYMBOL_TRADE_MODE_FULL", 4)): "FULL",
        int(getattr(mt5, "SYMBOL_TRADE_MODE_LONGONLY", 1)): "LONG_ONLY",
        int(getattr(mt5, "SYMBOL_TRADE_MODE_SHORTONLY", 2)): "SHORT_ONLY",
        int(getattr(mt5, "SYMBOL_TRADE_MODE_CLOSEONLY", 3)): "CLOSE_ONLY",
        int(getattr(mt5, "SYMBOL_TRADE_MODE_DISABLED", 0)): "DISABLED",
    }
    trade_mode = trade_modes.get(raw_trade_mode, "UNKNOWN")
    trade_mode_enabled = trade_mode != "DISABLED"
    profit_buy = None
    profit_sell = None
    pnl_source = "UNAVAILABLE"
    order_calc_profit = getattr(mt5, "order_calc_profit", None)
    profit_distance = max(
        float(getattr(symbol, "trade_tick_size", 0.0)),
        float(getattr(symbol, "point", 0.0)),
    )
    if callable(order_calc_profit) and profit_distance > 0.0:
        try:
            buy_value = order_calc_profit(
                mt5.ORDER_TYPE_BUY,
                SYMBOL,
                1.0,
                float(tick.ask),
                float(tick.ask) + profit_distance,
            )
            sell_value = order_calc_profit(
                mt5.ORDER_TYPE_SELL,
                SYMBOL,
                1.0,
                float(tick.bid),
                float(tick.bid) - profit_distance,
            )
            if buy_value is not None and sell_value is not None:
                profit_buy = abs(float(buy_value)) / profit_distance
                profit_sell = abs(float(sell_value)) / profit_distance
                if profit_buy > 0.0 and profit_sell > 0.0:
                    pnl_source = "MT5_ORDER_CALC_PROFIT"
        except Exception:
            profit_buy = None
            profit_sell = None

    tick_time_ms = int(getattr(tick, "time_msc", int(tick.time * 1000)))
    clock = broker_clock or BrokerClock()
    reference_now = now_utc or datetime.now(UTC)
    tick_timestamp = clock.to_utc(tick_time_ms / 1000)
    signed_tick_age_ms = int(
        (reference_now - tick_timestamp).total_seconds() * 1000
    )
    absolute_tick_age_ms = max(0, signed_tick_age_ms)
    terminal = mt5.terminal_info()
    terminal_connected = bool(terminal and getattr(terminal, "connected", False))
    market_utc_offset_hours = environment_integer(
        "MT5_MARKET_UTC_OFFSET_HOURS",
        clock.offset_hours,
    )
    if not -23 <= market_utc_offset_hours <= 23:
        raise RuntimeError("MT5_MARKET_UTC_OFFSET_HOURS must be between -23 and +23")
    market_calendar = load_market_calendar(
        os.getenv("XPDE_CONFIG_PATH", "config/default.toml")
    )
    market_status = infer_market_status(
        now_utc=reference_now,
        absolute_tick_age_ms=absolute_tick_age_ms,
        terminal_connected=terminal_connected,
        market_utc_offset_hours=market_utc_offset_hours,
        calendar=market_calendar,
        trade_mode_enabled=trade_mode_enabled,
    )
    session_open_until = market_session_open_until(
        now_utc=reference_now,
        market_utc_offset_hours=market_utc_offset_hours,
        calendar=market_calendar,
    )
    chart_mode = symbol_chart_mode(mt5, symbol)
    missing_flags: list[str] = []
    if tick.bid <= 0:
        missing_flags.append("BID_MISSING")
    if tick.ask <= tick.bid:
        missing_flags.append("ASK_INVALID")
    if len(rates) < 48:
        missing_flags.append("BARS_INSUFFICIENT")
    if chart_mode != CHART_MODE_BID:
        missing_flags.append("CHART_MODE_UNSUPPORTED_FOR_MODEL")
    if signed_tick_age_ms < -MAX_TICK_AGE_MS:
        missing_flags.append("TICK_TIMESTAMP_IN_FUTURE")
    if not trade_mode_enabled:
        missing_flags.append("BROKER_TRADE_MODE_DISABLED")

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
    bars = overlay_executable_bars(bars, executable_bars or {})
    if bars:
        latest_executable = (executable_bars or {}).get(str(bars[-1]["timestamp"]))
        if latest_executable is not None:
            bars[-1]["executable_tick_path"] = latest_executable.get(
                "_tick_path", []
            )
    executable_fields = tuple(
        f"{side}_{field}"
        for side in ("bid", "ask")
        for field in ("open", "high", "low", "close")
    )
    if bars and (
        not all(field in bars[-1] for field in executable_fields)
        or int(bars[-1].get("executable_tick_count", 0)) <= 0
        or not bars[-1].get("executable_tick_path")
    ):
        missing_flags.append("EXECUTABLE_TICK_PATH_MISSING")
    if bars and not has_complete_tick_coverage(bars[-1]):
        missing_flags.append("EXECUTABLE_TICK_COVERAGE_INCOMPLETE")
    if not has_complete_executable_feature_window(bars):
        missing_flags.append("FEATURE_WINDOW_EXECUTABLE_HISTORY_INCOMPLETE")
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
        current_bar = overlay_executable_bars(
            [current_bar], executable_bars or {}, include_tick_path=True
        )[0]
    if current_bar is None or (
        not all(field in current_bar for field in executable_fields)
        or int(current_bar.get("executable_tick_count", 0)) <= 0
        or not current_bar.get("executable_tick_path")
    ):
        missing_flags.append("CURRENT_EXECUTABLE_TICK_PATH_MISSING")
    if current_bar is not None and not has_complete_tick_coverage(current_bar):
        missing_flags.append("CURRENT_EXECUTABLE_TICK_COVERAGE_INCOMPLETE")

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
            "chart_mode": chart_mode,
            "quote_currency": str(getattr(symbol, "currency_profit", "") or "USD"),
            "symbol_profit_currency": str(
                getattr(symbol, "currency_profit", "") or "USD"
            ),
            "calculated_pnl_currency": str(account.currency),
            "profit_per_price_unit_per_lot_buy": profit_buy,
            "profit_per_price_unit_per_lot_sell": profit_sell,
            "pnl_calculation_source": pnl_source,
            "conversion_rate": (
                profit_buy / float(symbol.trade_contract_size)
                if profit_buy is not None and float(symbol.trade_contract_size) > 0
                else None
            ),
            "conversion_timestamp": tick_timestamp.isoformat(),
            "trade_mode_enabled": trade_mode_enabled,
            "trade_mode": trade_mode,
            "margin_per_lot_buy": (
                float(margin_buy)
                if margin_buy is not None and float(margin_buy) > 0
                else None
            ),
            "margin_per_lot_sell": (
                float(margin_sell)
                if margin_sell is not None and float(margin_sell) > 0
                else None
            ),
        },
        "data_quality": {
            "completeness": 1.0 if not missing_flags else 0.0,
            "tick_age_ms": max(absolute_tick_age_ms, transport_tick_age_ms),
            "absolute_tick_age_ms": absolute_tick_age_ms,
            "transport_tick_age_ms": max(0, transport_tick_age_ms),
            "market_status": market_status,
            "market_session_open_until": (
                session_open_until.isoformat() if session_open_until else None
            ),
            "missing_flags": missing_flags,
            "reason_codes": (
                [f"UTC_PROVIDER_OVERRIDE_HOURS_{clock.offset_hours}"]
                if clock.offset_hours
                else ["UTC_PROVIDER_TIME"]
            ),
        },
    }


def post_payload(
    api_url: str,
    payload: dict[str, Any],
    expected_status: int | tuple[int, ...] = 202,
) -> None:
    request = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            accepted_statuses = (
                (expected_status,) if isinstance(expected_status, int) else expected_status
            )
            if response.status not in accepted_statuses:
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
    if len(rates):
        executable, _ = collect_executable_bars(
            mt5,
            symbol=SYMBOL,
            start_epoch=min(float(rate["time"]) for rate in rates),
            end_epoch=max(float(rate["time"]) for rate in rates) + 300.0,
            clock=clock,
        )
        bars = overlay_executable_bars(
            bars,
            executable,
            include_tick_path=True,
        )
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
    post_backfill_payloads(
        api_url,
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        provider="MetaTrader5",
        broker_offset_hours=clock.offset_hours,
        bars=bars,
        reset=False,
    )


def completed_bar_distance(earlier: str, later: str) -> int:
    earlier_time = datetime.fromisoformat(earlier.replace("Z", "+00:00"))
    later_time = datetime.fromisoformat(later.replace("Z", "+00:00"))
    return max(0, int((later_time - earlier_time).total_seconds() // 300))


def forecast_generation_delay_ms(
    origin_bar_timestamp: str,
    *,
    now_utc: datetime | None = None,
) -> int:
    origin = datetime.fromisoformat(origin_bar_timestamp.replace("Z", "+00:00"))
    expected = origin + timedelta(minutes=5)
    reference = now_utc or datetime.now(UTC)
    return max(0, int((reference - expected).total_seconds() * 1000))


def forecast_envelope_matures_at(origin_bar_timestamp: str) -> datetime:
    origin = datetime.fromisoformat(origin_bar_timestamp.replace("Z", "+00:00"))
    return origin + timedelta(minutes=(max(HORIZONS) + 1) * 5)


def market_session_covers_forecast_envelope(
    snapshot: dict[str, Any],
    origin_bar_timestamp: str,
) -> bool:
    raw_session_end = snapshot.get("data_quality", {}).get(
        "market_session_open_until"
    )
    if not raw_session_end:
        return False
    session_end = datetime.fromisoformat(str(raw_session_end).replace("Z", "+00:00"))
    return session_end >= forecast_envelope_matures_at(origin_bar_timestamp)


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
    parser.add_argument(
        "--model-register-url",
        default="http://127.0.0.1:8787/api/v1/models/register",
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
            environment_integer("MT5_UTC_OFFSET_OVERRIDE_HOURS")
        )
        # Keep more than the 24-bar spread feature window so startup and
        # maintenance gaps cannot leave the latest inference row underfilled.
        executable_tracker = ExecutableBarTracker(retention_bars=64)
        candidate = None
        if args.model_dir:
            from .model_inference import (
                CandidateModel,
                candidate_registration_payload,
            )

            candidate = CandidateModel(args.model_dir)
            if not candidate.manifest.get("eligible_for_shadow", False):
                raise RuntimeError(
                    f"candidate {candidate.model_id} did not pass the shadow eligibility gate"
                )
            print(f"XPDE candidate model loaded: {candidate.model_id}", flush=True)
            post_payload(
                args.model_register_url,
                candidate_registration_payload(candidate.manifest, args.model_dir),
                expected_status=(200, 201, 202),
            )
        retry_delay = max(args.interval, 0.5)
        pending_startup_forecast: dict[str, Any] | None = None
        pending_startup_bar: str | None = None
        while True:
            try:
                executable_tracker.refresh(
                    mt5,
                    symbol=SYMBOL,
                    clock=broker_clock,
                    now_epoch=datetime.now(UTC).timestamp(),
                )
                startup_snapshot = build_snapshot(
                    mt5,
                    broker_clock=broker_clock,
                    executable_bars=executable_tracker.bars,
                )
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
                    executable_tracker.ingest_completed_bars(catchup_bars)
                    post_catchup(args.backfill_url, catchup_bars, clock=broker_clock)
                    startup_had_gap = True
                    print(
                        f"XPDE catch-up appended {len(catchup_bars)} completed M5 bars",
                        flush=True,
                    )
                post_payload(args.api_url, startup_snapshot)
                if (
                    not args.snapshot_only
                    and not startup_had_gap
                    and startup_snapshot["data_quality"]["market_status"] == "OPEN"
                    and not startup_snapshot["data_quality"]["missing_flags"]
                    and forecast_generation_delay_ms(latest_completed_bar)
                    <= MAX_FORECAST_GENERATION_DELAY_MS
                    and market_session_covers_forecast_envelope(
                        startup_snapshot, latest_completed_bar
                    )
                ):
                    if (
                        pending_startup_forecast is None
                        or pending_startup_bar != latest_completed_bar
                    ):
                        pending_startup_forecast = forecast_with_candidate(
                            startup_snapshot,
                            candidate,
                        )
                        pending_startup_bar = latest_completed_bar
                    post_payload(
                        args.forecast_url,
                        pending_startup_forecast,
                        expected_status=(200, 202),
                    )
                    print(
                        f"XPDE startup forecast posted for {latest_completed_bar}",
                        flush=True,
                    )
                elif (
                    not args.snapshot_only
                    and not startup_had_gap
                    and forecast_generation_delay_ms(latest_completed_bar)
                    > MAX_FORECAST_GENERATION_DELAY_MS
                ):
                    print(
                        "XPDE skipped a late startup forecast and will wait for "
                        "the next completed M5 candle",
                        flush=True,
                    )
                break
            except RECOVERABLE_BRIDGE_ERRORS as error:
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
        pending_forecast: dict[str, Any] | None = None
        pending_forecast_bar: str | None = None
        retry_delay = max(args.interval, 0.5)
        while True:
            try:
                now_epoch = datetime.now(UTC).timestamp()
                executable_tracker.refresh(
                    mt5,
                    symbol=SYMBOL,
                    clock=broker_clock,
                    now_epoch=now_epoch,
                )
                transport_tick_age_ms = int(
                    (time.monotonic() - last_tick_change) * 1000
                )
                snapshot = build_snapshot(
                    mt5,
                    broker_clock=broker_clock,
                    executable_bars=executable_tracker.bars,
                    transport_tick_age_ms=transport_tick_age_ms,
                )
                tick_signature = (
                    snapshot["timestamp"],
                    snapshot["bid"],
                    snapshot["ask"],
                )
                if tick_signature != last_tick_signature:
                    last_tick_signature = tick_signature
                    last_tick_change = time.monotonic()
                snapshot["data_quality"]["transport_tick_age_ms"] = int(
                    (time.monotonic() - last_tick_change) * 1000
                )
                snapshot["data_quality"]["tick_age_ms"] = max(
                    int(snapshot["data_quality"]["absolute_tick_age_ms"]),
                    int(snapshot["data_quality"]["transport_tick_age_ms"]),
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
                    executable_tracker.ingest_completed_bars(catchup_bars)
                    post_catchup(
                        args.backfill_url,
                        catchup_bars,
                        clock=broker_clock,
                    )
                    last_forecast_bar = latest_bar
                    pending_forecast = None
                    pending_forecast_bar = None
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
                    and snapshot["data_quality"]["market_status"] == "OPEN"
                    and not snapshot["data_quality"]["missing_flags"]
                    and latest_bar != last_forecast_bar
                ):
                    if (
                        forecast_generation_delay_ms(latest_bar)
                        > MAX_FORECAST_GENERATION_DELAY_MS
                    ):
                        last_forecast_bar = latest_bar
                        pending_forecast = None
                        pending_forecast_bar = None
                        print(
                            "XPDE skipped a late live forecast and will wait for "
                            "the next completed M5 candle",
                            flush=True,
                        )
                        continue
                    if not market_session_covers_forecast_envelope(
                        snapshot, latest_bar
                    ):
                        last_forecast_bar = latest_bar
                        pending_forecast = None
                        pending_forecast_bar = None
                        print(
                            "XPDE skipped a forecast whose H1/H3/H6/H12 envelope "
                            "crosses the market session boundary",
                            flush=True,
                        )
                        continue
                    if pending_forecast is None or pending_forecast_bar != latest_bar:
                        pending_forecast = forecast_with_candidate(snapshot, candidate)
                        pending_forecast_bar = latest_bar
                    post_payload(
                        args.forecast_url,
                        pending_forecast,
                        expected_status=(200, 202),
                    )
                    last_forecast_bar = latest_bar
                    pending_forecast = None
                    pending_forecast_bar = None
            except RECOVERABLE_BRIDGE_ERRORS as error:
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
