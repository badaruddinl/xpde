from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import math
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
        if retention_bars <= 0:
            raise ValueError("retention_bars must be positive")
        self.retention_bars = retention_bars
        self.bars: dict[str, dict[str, Any]] = {}
        self.last_tick_msc: int | None = None
        self.last_full_rehydration_bucket_epoch: int | None = None

    @staticmethod
    def _timestamp_epoch(timestamp: str) -> float:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()

    def _completed_history_start_epoch(
        self,
        mt5: Any,
        *,
        symbol: str,
    ) -> float:
        rates = mt5.copy_rates_from_pos(
            symbol,
            getattr(mt5, "TIMEFRAME_M5", 5),
            1,
            self.retention_bars,
        )
        if rates is None:
            code, message = mt5.last_error()
            raise RuntimeError(
                "MetaTrader5 completed-bar history for executable rehydration "
                f"failed ({code}): {message}"
            )
        epochs = [
            float(rate["time"])
            for rate in rates
            if float(rate["time"]) > 0.0
        ]
        if not epochs:
            raise RuntimeError(
                "MetaTrader5 returned no completed trading bars for executable "
                "feature-history rehydration"
            )
        return min(epochs)

    def _trim_to_trading_bar_retention(
        self,
        *,
        current_bucket_utc_epoch: float,
    ) -> None:
        completed = sorted(
            (
                (timestamp, bar)
                for timestamp, bar in self.bars.items()
                if self._timestamp_epoch(timestamp) < current_bucket_utc_epoch
            ),
            key=lambda item: item[0],
        )[-self.retention_bars :]
        current = [
            (timestamp, bar)
            for timestamp, bar in self.bars.items()
            if math.isclose(
                self._timestamp_epoch(timestamp),
                current_bucket_utc_epoch,
                rel_tol=0.0,
                abs_tol=0.001,
            )
        ]
        self.bars = dict([*completed, *current])

    @staticmethod
    def _from_overlaid_bar(source: dict[str, Any]) -> dict[str, Any]:
        try:
            timestamp = str(source["timestamp"])
            bucket_start_msc = int(
                datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("catch-up bar timestamp is invalid") from error
        executable_fields = tuple(
            f"{side}_{field}"
            for side in ("bid", "ask")
            for field in ("open", "high", "low", "close")
        )
        path = source.get("executable_tick_path")
        if (
            not isinstance(path, list)
            or not path
            or any(field not in source for field in executable_fields)
        ):
            raise ValueError("catch-up bar has no complete executable tick path")
        normalized_path: list[list[float | int]] = []
        previous_time = -1
        for item in path:
            try:
                time_msc = int(item[0])
                bid = float(item[1])
                ask = float(item[2])
            except (IndexError, TypeError, ValueError) as error:
                raise ValueError("catch-up bar contains an invalid tick") from error
            if (
                time_msc < previous_time
                or not math.isfinite(bid)
                or not math.isfinite(ask)
                or bid <= 0.0
                or ask <= bid
            ):
                raise ValueError("catch-up tick path is invalid or unordered")
            normalized_path.append([time_msc, bid, ask])
            previous_time = time_msc
        first_tick_msc = int(source.get("first_tick_msc", 0))
        last_tick_msc = int(source.get("last_tick_msc", 0))
        if (
            normalized_path[0][0] != first_tick_msc
            or normalized_path[-1][0] != last_tick_msc
            or first_tick_msc < bucket_start_msc
            or last_tick_msc >= bucket_start_msc + 300_000
        ):
            raise ValueError(
                "catch-up tick path boundaries are inconsistent with its M5 bucket"
            )
        expected = {
            "bid_open": normalized_path[0][1],
            "bid_high": max(item[1] for item in normalized_path),
            "bid_low": min(item[1] for item in normalized_path),
            "bid_close": normalized_path[-1][1],
            "ask_open": normalized_path[0][2],
            "ask_high": max(item[2] for item in normalized_path),
            "ask_low": min(item[2] for item in normalized_path),
            "ask_close": normalized_path[-1][2],
        }
        if any(
            not math.isclose(
                float(source[field]),
                float(value),
                rel_tol=0.0,
                abs_tol=1e-9,
            )
            for field, value in expected.items()
        ):
            raise ValueError("catch-up tick path does not reconstruct executable OHLC")
        executable_tick_count = int(source.get("executable_tick_count", 0))
        if executable_tick_count < len(normalized_path):
            raise ValueError("catch-up raw tick count is smaller than its compact path")
        return {
            "timestamp": timestamp,
            **{field: float(source[field]) for field in executable_fields},
            "executable_tick_count": executable_tick_count,
            "first_tick_msc": first_tick_msc,
            "last_tick_msc": last_tick_msc,
            "_tick_path": normalized_path,
        }

    @staticmethod
    def _covers_at_least_as_much(
        candidate: dict[str, Any],
        reference: dict[str, Any],
    ) -> bool:
        return (
            int(candidate.get("first_tick_msc", 0))
            <= int(reference.get("first_tick_msc", 0))
            and int(candidate.get("last_tick_msc", 0))
            >= int(reference.get("last_tick_msc", 0))
            and int(candidate.get("executable_tick_count", 0))
            >= int(reference.get("executable_tick_count", 0))
        )

    def ingest_completed_bars(self, bars: Iterable[dict[str, Any]]) -> None:
        """Hydrate inference memory from authoritative catch-up results."""
        for source in bars:
            candidate = self._from_overlaid_bar(source)
            timestamp = str(candidate["timestamp"])
            current = self.bars.get(timestamp)
            if current is not None and not self._covers_at_least_as_much(
                candidate,
                current,
            ):
                continue
            self.bars[timestamp] = candidate
            self.last_tick_msc = max(
                self.last_tick_msc or 0,
                int(candidate["last_tick_msc"]),
            )
        if len(self.bars) > self.retention_bars + 1:
            retained = sorted(self.bars.items(), key=lambda item: item[0])[
                -(self.retention_bars + 1) :
            ]
            self.bars = dict(retained)

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
        current_bucket_utc_epoch = clock.to_utc(current_bucket_epoch).timestamp()
        latest_cached_epoch = max(
            (self._timestamp_epoch(timestamp) for timestamp in self.bars),
            default=None,
        )
        completed_cached_count = sum(
            self._timestamp_epoch(timestamp) < current_bucket_utc_epoch
            for timestamp in self.bars
        )
        cache_gap_bars = (
            int((current_bucket_utc_epoch - latest_cached_epoch) // 300)
            if latest_cached_epoch is not None
            else None
        )
        needs_full_rehydration = (
            self.last_tick_msc is None
            or not self.bars
            or cache_gap_bars is None
            or cache_gap_bars > 1
            or completed_cached_count < self.retention_bars
        )
        full_rehydration = (
            needs_full_rehydration
            and self.last_full_rehydration_bucket_epoch != current_bucket_epoch
        )
        # Recount the current and immediately previous M5 bucket from their
        # authoritative raw MT5 ticks. This avoids estimating raw tick growth
        # from a compact price-change path when the provider query overlaps the
        # last millisecond. The previous bucket is recounted as well so a tick
        # arriving between the last pre-boundary poll and the first new-bucket
        # poll cannot be lost.
        start_epoch = (
            self._completed_history_start_epoch(mt5, symbol=symbol)
            if full_rehydration
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
        if full_rehydration and additions:
            rehydrated = dict(self.bars)
            for timestamp, addition in additions.items():
                current = rehydrated.get(timestamp)
                if current is None or self._covers_at_least_as_much(
                    addition,
                    current,
                ):
                    rehydrated[timestamp] = addition
            self.bars = rehydrated
            self.last_tick_msc = maximum
            self.last_full_rehydration_bucket_epoch = current_bucket_epoch
        else:
            for timestamp, bar in additions.items():
                self.bars[timestamp] = bar
        maxima = [
            value for value in (self.last_tick_msc, maximum) if value is not None
        ]
        self.last_tick_msc = max(maxima, default=None)
        self._trim_to_trading_bar_retention(
            current_bucket_utc_epoch=current_bucket_utc_epoch
        )
