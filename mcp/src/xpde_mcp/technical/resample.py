"""Strict completed-bar aggregation without artificial higher-timeframe candles."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta

from ..errors import ContractError
from .types import Bar

_TIMEFRAME_MINUTES = {"M5": 5, "M15": 15, "H1": 60}


def timeframe_minutes(timeframe: str) -> int:
    try:
        return _TIMEFRAME_MINUTES[timeframe]
    except KeyError as error:
        raise ContractError("timeframe must be M5, M15, or H1") from error


def _bucket_start(timestamp: datetime, minutes: int) -> datetime:
    epoch_seconds = int(timestamp.timestamp())
    bucket_seconds = minutes * 60
    return datetime.fromtimestamp(
        epoch_seconds - epoch_seconds % bucket_seconds,
        tz=UTC,
    )


def resample_completed_m5(bars: list[Bar], timeframe: str) -> list[Bar]:
    """Aggregate only buckets containing every exact M5 component."""

    minutes = timeframe_minutes(timeframe)
    if timeframe == "M5":
        return list(bars)
    expected_count = minutes // 5
    grouped: dict[datetime, list[Bar]] = defaultdict(list)
    for bar in bars:
        grouped[_bucket_start(bar.timestamp, minutes)].append(bar)

    output: list[Bar] = []
    for start in sorted(grouped):
        members = sorted(grouped[start], key=lambda item: item.timestamp)
        expected = [
            start + timedelta(minutes=5 * index) for index in range(expected_count)
        ]
        if [member.timestamp for member in members] != expected:
            continue
        sources = {member.price_source for member in members}
        output.append(
            Bar(
                timestamp=start,
                open=members[0].open,
                high=max(member.high for member in members),
                low=min(member.low for member in members),
                close=members[-1].close,
                tick_volume=sum(member.tick_volume for member in members),
                price_source=(
                    sources.pop() if len(sources) == 1 else "MIXED_CHART_AND_BID_OHLC"
                ),
            )
        )
    return output


def aggregation_diagnostics(
    native_bars: list[Bar],
    aggregated_bars: list[Bar],
    timeframe: str,
) -> dict[str, int | str]:
    minutes = timeframe_minutes(timeframe)
    if timeframe == "M5":
        return {
            "timeframe": timeframe,
            "native_bars_seen": len(native_bars),
            "complete_bars": len(aggregated_bars),
            "rejected_incomplete_buckets": 0,
        }
    bucket_count = len({_bucket_start(bar.timestamp, minutes) for bar in native_bars})
    return {
        "timeframe": timeframe,
        "native_bars_seen": len(native_bars),
        "complete_bars": len(aggregated_bars),
        "rejected_incomplete_buckets": max(bucket_count - len(aggregated_bars), 0),
    }
