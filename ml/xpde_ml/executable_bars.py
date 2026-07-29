from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
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
        normalized_time_msc = int(
            clock.to_utc(time_msc / 1000.0).timestamp() * 1000
        )
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
                "first_tick_msc": normalized_time_msc,
                "last_tick_msc": normalized_time_msc,
                "_tick_path": [[normalized_time_msc, bid, ask]],
            }
        else:
            bar["bid_high"] = max(float(bar["bid_high"]), bid)
            bar["bid_low"] = min(float(bar["bid_low"]), bid)
            bar["bid_close"] = bid
            bar["ask_high"] = max(float(bar["ask_high"]), ask)
            bar["ask_low"] = min(float(bar["ask_low"]), ask)
            bar["ask_close"] = ask
            bar["executable_tick_count"] = int(bar["executable_tick_count"]) + 1
            bar["last_tick_msc"] = normalized_time_msc
            path = bar["_tick_path"]
            if path[-1][1] != bid or path[-1][2] != ask:
                path.append([normalized_time_msc, bid, ask])
            elif (
                len(path) == 1
                or path[-2][1] != bid
                or path[-2][2] != ask
            ):
                path.append([normalized_time_msc, bid, ask])
            else:
                path[-1][0] = normalized_time_msc
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
        current_first = int(current.get("first_tick_msc", 0))
        addition_first = int(addition.get("first_tick_msc", 0))
        current_last = int(current.get("last_tick_msc", 0))
        addition_last = int(addition.get("last_tick_msc", 0))
        if addition_first and (not current_first or addition_first < current_first):
            current["bid_open"] = addition["bid_open"]
            current["ask_open"] = addition["ask_open"]
            current["first_tick_msc"] = addition_first
        current["bid_high"] = max(
            float(current["bid_high"]), float(addition["bid_high"])
        )
        current["bid_low"] = min(
            float(current["bid_low"]), float(addition["bid_low"])
        )
        if addition_last >= current_last:
            current["bid_close"] = addition["bid_close"]
            current["ask_close"] = addition["ask_close"]
            current["last_tick_msc"] = addition_last
        current["ask_high"] = max(
            float(current["ask_high"]), float(addition["ask_high"])
        )
        current["ask_low"] = min(
            float(current["ask_low"]), float(addition["ask_low"])
        )
        current_count = int(current.get("executable_tick_count", 0))
        addition_count = int(addition.get("executable_tick_count", 0))
        disjoint = bool(current_last and addition_first > current_last)
        combined_path = [
            *current.get("_tick_path", []),
            *addition.get("_tick_path", []),
        ]
        combined_path.sort(key=lambda item: int(item[0]))
        compact_path: list[list[float | int]] = []
        seen_exact: set[tuple[int, float, float]] = set()
        for item in combined_path:
            signature = (int(item[0]), float(item[1]), float(item[2]))
            if signature in seen_exact:
                continue
            seen_exact.add(signature)
            if not compact_path:
                compact_path.append(item)
            elif compact_path[-1][1] != item[1] or compact_path[-1][2] != item[2]:
                compact_path.append(item)
            elif (
                len(compact_path) == 1
                or compact_path[-2][1] != item[1]
                or compact_path[-2][2] != item[2]
            ):
                compact_path.append(item)
            else:
                compact_path[-1] = item
        current["_tick_path"] = compact_path
        current["executable_tick_count"] = (
            current_count + addition_count
            if disjoint
            else max(current_count, addition_count, len(compact_path))
        )


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
            getattr(mt5, "COPY_TICKS_ALL", 0),
        )
        if ticks is None:
            code, message = mt5.last_error()
            raise RuntimeError(
                f"MetaTrader5 executable tick backfill failed ({code}): {message}"
            )
        previous_maximum = maximum_time_msc
        additions, observed_maximum = aggregate_executable_ticks(
            ticks,
            clock=clock,
            after_time_msc=(
                maximum_time_msc - 1 if maximum_time_msc is not None else None
            ),
        )
        maxima = [
            value
            for value in (previous_maximum, observed_maximum)
            if value is not None
        ]
        maximum_time_msc = max(maxima, default=None)
        merge_executable_bars(result, additions)
        if chunk_end >= end:
            break
        # Overlap one millisecond because provider range-end inclusivity is not
        # assumed. Exact duplicates are removed during merge, while distinct
        # same-millisecond quotes remain available for ambiguity handling.
        cursor = chunk_end - timedelta(milliseconds=1)
    return result, maximum_time_msc


def overlay_executable_bars(
    chart_bars: list[dict[str, Any]],
    executable_bars: dict[str, dict[str, Any]],
    *,
    include_tick_path: bool = False,
) -> list[dict[str, Any]]:
    result = []
    for chart_bar in chart_bars:
        merged = dict(chart_bar)
        executable = executable_bars.get(str(chart_bar["timestamp"]))
        if executable is not None:
            merged.update({
                key: value
                for key, value in executable.items()
                if key not in {"timestamp", "_tick_path"}
            })
            if include_tick_path:
                merged["executable_tick_path"] = executable.get("_tick_path", [])
                merged["executable_tick_path_json"] = json.dumps(
                    executable.get("_tick_path", []),
                    separators=(",", ":"),
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
        current_bucket_epoch = int(provider_now_epoch // 300) * 300
        # Recount the current and immediately previous M5 bucket from their
        # authoritative raw MT5 ticks. This avoids estimating raw tick growth
        # from a compact price-change path when the provider query overlaps the
        # last millisecond. The previous bucket is recounted as well so a tick
        # arriving between the last pre-boundary poll and the first new-bucket
        # poll cannot be lost.
        start_epoch = (
            provider_now_epoch - self.retention_bars * 300
            if self.last_tick_msc is None or not self.bars
            else current_bucket_epoch - 300
        )
        additions, maximum = collect_executable_bars(
            mt5,
            symbol=symbol,
            start_epoch=start_epoch,
            end_epoch=provider_now_epoch,
            clock=clock,
            chunk_hours=2,
            after_time_msc=None,
        )
        for timestamp, bar in additions.items():
            self.bars[timestamp] = bar
        maxima = [
            value for value in (self.last_tick_msc, maximum) if value is not None
        ]
        self.last_tick_msc = max(maxima, default=None)
        minimum_epoch = now_epoch - self.retention_bars * 300
        self.bars = {
            timestamp: bar
            for timestamp, bar in self.bars.items()
            if datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
            >= minimum_epoch
        }
