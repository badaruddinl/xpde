from __future__ import annotations

import gzip
import json
import urllib.request
from collections.abc import Iterator
from datetime import datetime
from typing import Any
from uuid import uuid4

DEFAULT_MAX_PAYLOAD_BYTES = 1_500_000
DEFAULT_MAX_BARS_PER_REQUEST = 250
DEFAULT_MAX_TICK_POINTS_PER_REQUEST = 20_000


def _wire_bar(bar: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in bar.items()
        if key
        not in {
            "spread_usd",
            "tick_size",
            "chart_mode",
            "executable_tick_path_json",
        }
    }


def _bar_timestamp(bar: dict[str, Any]) -> datetime:
    raw = bar.get("timestamp")
    if not isinstance(raw, str):
        raise ValueError("backfill bar timestamp must be an ISO-8601 string")
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"invalid backfill bar timestamp: {raw}") from error
    if value.tzinfo is None:
        raise ValueError("backfill bar timestamp must include a UTC offset")
    return value


def _payload(
    *,
    symbol: str,
    timeframe: str,
    provider: str,
    broker_offset_hours: int,
    bars: list[dict[str, Any]],
    reset: bool,
    import_id: str | None,
    final_chunk: bool,
    chunk_index: int | None = None,
    total_chunks: int | None = None,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "provider": provider,
        "broker_offset_hours": broker_offset_hours,
        "reset": reset,
        "import_id": import_id,
        "final_chunk": final_chunk,
        "chunk_index": chunk_index,
        "total_chunks": total_chunks,
        "bars": [_wire_bar(bar) for bar in bars],
    }


def iter_backfill_payloads(
    *,
    symbol: str,
    timeframe: str,
    provider: str,
    broker_offset_hours: int,
    bars: list[dict[str, Any]],
    reset: bool,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    max_bars_per_request: int = DEFAULT_MAX_BARS_PER_REQUEST,
    max_tick_points_per_request: int = DEFAULT_MAX_TICK_POINTS_PER_REQUEST,
) -> Iterator[tuple[dict[str, Any], bytes]]:
    """Build bounded requests without truncating an executable tick path.

    A reset import receives a stable import id and is only promoted by the
    server when the final chunk arrives. Append/catch-up requests write
    directly because each chunk is independently idempotent.
    """

    if not bars:
        return
    if max_payload_bytes <= 0 or max_bars_per_request <= 0:
        raise ValueError("backfill transport limits must be positive")
    if max_tick_points_per_request <= 0:
        raise ValueError("backfill tick-point limit must be positive")

    import_id = str(uuid4()) if reset else None
    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_points = 0
    timestamps = [_bar_timestamp(bar) for bar in bars]
    if any(left >= right for left, right in zip(timestamps, timestamps[1:])):
        raise ValueError(
            "backfill bars must be strictly ordered without duplicate timestamps"
        )

    for bar in bars:
        points = len(bar.get("executable_tick_path") or [])
        if points <= 0:
            raise ValueError(
                f"backfill bar {bar.get('timestamp')} has no executable tick path"
            )
        if points > max_tick_points_per_request:
            raise ValueError(
                f"backfill bar {bar.get('timestamp')} exceeds the per-request "
                "tick-point limit"
            )

        candidate = [*current, bar]
        candidate_payload = _payload(
            symbol=symbol,
            timeframe=timeframe,
            provider=provider,
            broker_offset_hours=broker_offset_hours,
            bars=candidate,
            reset=reset and not chunks,
            import_id=import_id,
            final_chunk=False,
            chunk_index=None,
            total_chunks=None,
        )
        encoded = json.dumps(
            candidate_payload, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        exceeds = (
            len(candidate) > max_bars_per_request
            or current_points + points > max_tick_points_per_request
            # Reserve room for the final numeric chunk_index/total_chunks
            # metadata, which is unknown until all chunks have been formed.
            or len(encoded) + 64 > max_payload_bytes
        )
        if exceeds and current:
            chunks.append(current)
            current = [bar]
            current_points = points
            single_payload = _payload(
                symbol=symbol,
                timeframe=timeframe,
                provider=provider,
                broker_offset_hours=broker_offset_hours,
                bars=current,
                reset=False,
                import_id=import_id,
                final_chunk=False,
                chunk_index=None,
                total_chunks=None,
            )
            single_encoded = json.dumps(
                single_payload, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
            if len(single_encoded) + 64 > max_payload_bytes:
                raise ValueError(
                    f"backfill bar {bar.get('timestamp')} exceeds the maximum "
                    "payload size"
                )
        elif exceeds:
            raise ValueError(
                f"backfill bar {bar.get('timestamp')} exceeds the maximum payload size"
            )
        else:
            current = candidate
            current_points += points

    if current:
        chunks.append(current)

    for index, chunk in enumerate(chunks):
        payload = _payload(
            symbol=symbol,
            timeframe=timeframe,
            provider=provider,
            broker_offset_hours=broker_offset_hours,
            bars=chunk,
            reset=reset and index == 0,
            import_id=import_id,
            final_chunk=index == len(chunks) - 1,
            chunk_index=index if import_id else None,
            total_chunks=len(chunks) if import_id else None,
        )
        encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode(
            "utf-8"
        )
        if len(encoded) > max_payload_bytes:
            raise ValueError("adaptive backfill chunk exceeded its byte limit")
        yield payload, encoded


def post_backfill_payloads(
    api_url: str,
    *,
    symbol: str,
    timeframe: str,
    provider: str,
    broker_offset_hours: int,
    bars: list[dict[str, Any]],
    reset: bool,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    max_bars_per_request: int = DEFAULT_MAX_BARS_PER_REQUEST,
    max_tick_points_per_request: int = DEFAULT_MAX_TICK_POINTS_PER_REQUEST,
    timeout_seconds: float = 30.0,
) -> int:
    accepted = 0
    for _, encoded in iter_backfill_payloads(
        symbol=symbol,
        timeframe=timeframe,
        provider=provider,
        broker_offset_hours=broker_offset_hours,
        bars=bars,
        reset=reset,
        max_payload_bytes=max_payload_bytes,
        max_bars_per_request=max_bars_per_request,
        max_tick_points_per_request=max_tick_points_per_request,
    ):
        compressed = gzip.compress(encoded, compresslevel=6, mtime=0)
        request = urllib.request.Request(
            api_url,
            data=compressed,
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
                "X-XPDE-Uncompressed-Bytes": str(len(encoded)),
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            result = json.load(response)
            accepted += int(result["inserted"])
    return accepted
