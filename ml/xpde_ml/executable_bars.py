from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from .time_utils import BrokerClock

CHART_MODE_BID = "BID"
CHART_MODE_LAST = "LAST"
CHART_MODE_UNKNOWN = "UNKNOWN"


def symbol_chart_mode(mt5: Any, symbol: Any) -> str:
    value = int(getattr(symbol, "chart_mode", -1))
    if value == int(getattr(mt5, "SYMBOL_CHART_MODE_BID", 0)):
        return CHART_MODE_BID
    if value == int(getattr(mt5, "SYMBOL_CHART_MODE_LAST", 1)):
        return CHART_MODE_LAST
    return CHART_MODE_UNKNOWN


def _tick_value(tick: Any, field: str, default: float = 0.0) -> float:
    try:
        return float(tick[field])
    except (IndexError, KeyError, TypeError, ValueError):
        return float(getattr(tick, field, default))


def aggregate_executable_ticks(
    ticks: Iterable[Any],
    *,
    clock: BrokerClock,
    after_time_msc: int | None = None,
) -> tuple[dict[str, dict[str, Any]], int | None]:
    bars: dict[str, dict[str, Any]] = {}
    maximum_time_msc = after_time_msc
    ordered = sorted(
        ticks,
        key=lambda tick: int(
            _tick_value(
                tick,
                "time_msc",
                _tick_value(tick, "time", 0.0) * 1000.0,
            )
        ),
    )
    for tick in ordered:
        time_msc = int(
            _tick_value(
                tick,
                "time_msc",
                _tick_value(tick, "time", 0.0) * 1000.0,
            )
        )
        if after_time_msc is not None and time_msc <= after_time_msc:
            continue
        bid = _tick_value(tick, "bid")
        ask = _tick_value(tick, "ask")
        if time_msc <= 0 or bid <= 0.0 or ask <= bid:
            continue
        bar_epoch = (time_msc // 1000) // 300 * 300
        timestamp = clock.iso_utc(float(bar_epoch))
        bar = bars.get(timestamp)
        if bar is None:
            bars[timestamp] = {
                "timestamp": timestamp,
                "bid_open": bid,
                "bid_high": bid,
                "bid_low": bid,
                "bid_close": bid,
                "ask_open": ask,
                "ask_high": ask,
                "ask_low": ask,
                "ask_close": ask,
                "executable_tick_count": 1,
            }
        else:
            bar["bid_high"] = max(float(bar["bid_high"]), bid)
            bar["bid_low"] = min(float(bar["bid_low"]), bid)
            bar["bid_close"] = bid
            bar["ask_high"] = max(float(bar["ask_high"]), ask)
            bar["ask_low"] = min(float(bar["ask_low"]), ask)
            bar["ask_close"] = ask
            bar["executable_tick_count"] = int(bar["executable_tick_count"]) + 1
        maximum_time_msc = max(maximum_time_msc or 0, time_msc)
    return bars, maximum_time_msc


def merge_executable_bars(
    destination: dict[str, dict[str, Any]],
    additions: dict[str, dict[str, Any]],
) -> None:
    for timestamp, addition in additions.items():
        current = destination.get(timestamp)
        if current is None:
            destination[timestamp] = dict(addition)
            continue
        current["bid_high"] = max(
            float(current["bid_high"]), float(addition["bid_high"])
        )
        current["bid_low"] = min(
            float(current["bid_low"]), float(addition["bid_low"])
        )
        current["bid_close"] = addition["bid_close"]
        current["ask_high"] = max(
            float(current["ask_high"]), float(addition["ask_high"])
        )
        current["ask_low"] = min(
            float(current["ask_low"]), float(addition["ask_low"])
        )
        current["ask_close"] = addition["ask_close"]
        current["executable_tick_count"] = int(
            current.get("executable_tick_count", 0)
        ) + int(addition.get("executable_tick_count", 0))


def collect_executable_bars(
    mt5: Any,
    *,
    symbol: str,
    start_epoch: float,
    end_epoch: float,
    clock: BrokerClock,
    chunk_hours: int = 24,
    after_time_msc: int | None = None,
) -> tuple[dict[str, dict[str, Any]], int | None]:
    if end_epoch < start_epoch:
        return {}, after_time_msc
    result: dict[str, dict[str, Any]] = {}
    maximum_time_msc = after_time_msc
    cursor = datetime.fromtimestamp(start_epoch, tz=UTC)
    end = datetime.fromtimestamp(end_epoch, tz=UTC)
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(hours=chunk_hours))
        ticks = mt5.copy_ticks_range(
            symbol,
            cursor,
            chunk_end,
            getattr(mt5, "COPY_TICKS_INFO", getattr(mt5, "COPY_TICKS_ALL", 0)),
        )
        if ticks is None:
            code, message = mt5.last_error()
            raise RuntimeError(
                f"MetaTrader5 executable tick backfill failed ({code}): {message}"
            )
        additions, maximum_time_msc = aggregate_executable_ticks(
            ticks,
            clock=clock,
            after_time_msc=maximum_time_msc,
        )
        merge_executable_bars(result, additions)
        if chunk_end >= end:
            break
        cursor = chunk_end + timedelta(milliseconds=1)
    return result, maximum_time_msc


def overlay_executable_bars(
    chart_bars: list[dict[str, Any]],
    executable_bars: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    result = []
    for chart_bar in chart_bars:
        merged = dict(chart_bar)
        executable = executable_bars.get(str(chart_bar["timestamp"]))
        if executable is not None:
            merged.update(
                {
                    key: value
                    for key, value in executable.items()
                    if key != "timestamp"
                }
            )
        result.append(merged)
    return result


class ExecutableBarTracker:
    def __init__(self, *, retention_bars: int = 24) -> None:
        self.retention_bars = retention_bars
        self.bars: dict[str, dict[str, Any]] = {}
        self.last_tick_msc: int | None = None

    def refresh(
        self,
        mt5: Any,
        *,
        symbol: str,
        clock: BrokerClock,
        now_epoch: float,
    ) -> None:
        provider_now_epoch = now_epoch + clock.offset_hours * 3600
        start_epoch = (
            self.last_tick_msc / 1000.0
            if self.last_tick_msc is not None
            else provider_now_epoch - self.retention_bars * 300
        )
        additions, maximum = collect_executable_bars(
            mt5,
            symbol=symbol,
            start_epoch=start_epoch,
            end_epoch=provider_now_epoch,
            clock=clock,
            chunk_hours=2,
            after_time_msc=self.last_tick_msc,
        )
        merge_executable_bars(self.bars, additions)
        self.last_tick_msc = maximum
        minimum_epoch = now_epoch - self.retention_bars * 300
        self.bars = {
            timestamp: bar
            for timestamp, bar in self.bars.items()
            if datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
            >= minimum_epoch
        }
