"""Offline analysis/replay settlement.

The Rust server is the canonical production settlement worker. Do not run this
module alongside it as a second live writer.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import groupby
from pathlib import Path

from .dataset import BARRIER_SPEC_ID

BARRIER_OUTCOMES = {
    "TP_FIRST",
    "SL_FIRST",
    "NO_HIT_BEFORE_EXPIRY",
    "AMBIGUOUS_SAME_BAR",
    "AMBIGUOUS_SAME_TIMESTAMP",
}


def classifier_direction_hit(
    direction_probability_up: float,
    actual_return: float,
) -> int:
    """Score the calibrated direction classifier against the training truth."""
    return int(
        (float(direction_probability_up) >= 0.5)
        == (float(actual_return) > 0.0)
    )


@dataclass(frozen=True)
class TickPathWindow:
    ticks: list[tuple[int, float, float]]
    expected_buckets: list[datetime]
    present_buckets: list[datetime]
    missing_buckets: list[datetime]
    missing_market_buckets: list[datetime]
    complete: bool

    @property
    def incomplete_reason(self) -> str:
        return (
            "SESSION_INTERRUPTED"
            if self.missing_market_buckets
            else "TICK_PATH_INCOMPLETE"
        )


def _is_exact_m5_horizon(origin: datetime, timestamps: list[str]) -> bool:
    if not timestamps:
        return False
    origin_utc = origin.astimezone(UTC)
    for index, timestamp in enumerate(timestamps, start=1):
        try:
            observed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return False
        if observed.tzinfo is None:
            return False
        if observed.astimezone(UTC) != origin_utc + timedelta(minutes=index * 5):
            return False
    return True


def barrier_outcome(
    action: str,
    target: float,
    stop: float,
    bars: list[sqlite3.Row | dict],
) -> str | None:
    if action not in {"LONG", "SHORT"}:
        return None
    if not math.isfinite(stop) or not math.isfinite(target):
        return None
    for bar in bars:
        high = float(bar["high"])
        low = float(bar["low"])
        if action == "LONG":
            tp_hit, sl_hit = high >= target, low <= stop
        else:
            tp_hit, sl_hit = low <= target, high >= stop
        if tp_hit and sl_hit:
            return "AMBIGUOUS_SAME_BAR"
        if tp_hit:
            return "TP_FIRST"
        if sl_hit:
            return "SL_FIRST"
    return "NO_HIT_BEFORE_EXPIRY"


def tick_sequence_barrier_outcome(
    action: str,
    target: float,
    stop: float,
    ticks: list[tuple[int, float, float]],
    *,
    start_exclusive_msc: int,
    end_exclusive_msc: int,
) -> tuple[str, int | None, float | None] | None:
    if action not in {"LONG", "SHORT"} or end_exclusive_msc <= start_exclusive_msc:
        return None
    boundary_prices = [
        bid if action == "LONG" else ask
        for time_msc, bid, ask in ticks
        if time_msc == start_exclusive_msc and time_msc < end_exclusive_msc
    ]
    if any(
        (action == "LONG" and (price >= target or price <= stop))
        or (action == "SHORT" and (price <= target or price >= stop))
        for price in boundary_prices
    ):
        return ("AMBIGUOUS_SAME_TIMESTAMP", start_exclusive_msc, None)
    filtered = [
        tick
        for tick in ticks
        if start_exclusive_msc < tick[0] < end_exclusive_msc
    ]
    for time_msc, grouped in groupby(filtered, key=lambda tick: tick[0]):
        prices = [tick[1] if action == "LONG" else tick[2] for tick in grouped]
        tp_prices = [
            price
            for price in prices
            if (action == "LONG" and price >= target)
            or (action == "SHORT" and price <= target)
        ]
        sl_prices = [
            price
            for price in prices
            if (action == "LONG" and price <= stop)
            or (action == "SHORT" and price >= stop)
        ]
        if tp_prices and sl_prices:
            return ("AMBIGUOUS_SAME_TIMESTAMP", time_msc, None)
        if tp_prices:
            return ("TP_FIRST", time_msc, tp_prices[0])
        if sl_prices:
            return ("SL_FIRST", time_msc, sl_prices[0])
    return ("NO_HIT_BEFORE_EXPIRY", None, None) if filtered else None


def _load_tick_path(
    connection: sqlite3.Connection,
    *,
    symbol: str,
    timeframe: str,
    first_bucket: datetime,
    end_exclusive: datetime,
) -> TickPathWindow:
    expected_buckets: list[datetime] = []
    cursor = first_bucket
    while cursor < end_exclusive:
        expected_buckets.append(cursor)
        cursor += timedelta(minutes=5)
    encoded = connection.execute(
        """
        SELECT timestamp, tick_path_json, path_point_count,
               first_tick_msc, last_tick_msc, source
        FROM market_tick_paths
        WHERE symbol=? AND timeframe=? AND timestamp>=? AND timestamp<?
          AND path_valid=1
        ORDER BY timestamp
        """,
        (symbol, timeframe, first_bucket.isoformat(), end_exclusive.isoformat()),
    ).fetchall()
    expected = {bucket.isoformat() for bucket in expected_buckets}
    valid_paths: dict[str, list[tuple[int, float, float]]] = {}
    for row in encoded:
        timestamp = str(row["timestamp"])
        if timestamp not in expected:
            continue
        try:
            parsed = [
                (int(item[0]), float(item[1]), float(item[2]))
                for item in json.loads(str(row["tick_path_json"]))
            ]
            bucket_start = int(
                datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except (IndexError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            not parsed
            or len(parsed) != int(row["path_point_count"])
            or str(row["source"]) == "LIVE_CURRENT"
            or parsed[0][0] != int(row["first_tick_msc"])
            or parsed[-1][0] != int(row["last_tick_msc"])
            or any(left[0] > right[0] for left, right in zip(parsed, parsed[1:]))
            or any(
                not (bucket_start <= time_msc < bucket_start + 300_000)
                or not math.isfinite(bid)
                or not math.isfinite(ask)
                or bid <= 0.0
                or ask <= bid
                for time_msc, bid, ask in parsed
            )
        ):
            continue
        valid_paths[timestamp] = parsed

    present_buckets = [
        bucket for bucket in expected_buckets if bucket.isoformat() in valid_paths
    ]
    missing_buckets = [
        bucket for bucket in expected_buckets if bucket.isoformat() not in valid_paths
    ]
    market_rows = connection.execute(
        """
        SELECT timestamp
        FROM market_bars
        WHERE symbol=? AND timeframe=? AND timestamp>=? AND timestamp<?
        """,
        (symbol, timeframe, first_bucket.isoformat(), end_exclusive.isoformat()),
    ).fetchall()
    market_buckets = {str(row["timestamp"]) for row in market_rows}
    missing_market_buckets = [
        bucket
        for bucket in expected_buckets
        if bucket.isoformat() not in market_buckets
    ]
    ticks: list[tuple[int, float, float]] = []
    for bucket in expected_buckets:
        ticks.extend(valid_paths.get(bucket.isoformat(), []))
    complete = (
        bool(expected_buckets)
        and not missing_buckets
        and not missing_market_buckets
        and bool(ticks)
        and not any(left[0] > right[0] for left, right in zip(ticks, ticks[1:]))
    )
    return TickPathWindow(
        ticks=ticks,
        expected_buckets=expected_buckets,
        present_buckets=present_buckets,
        missing_buckets=missing_buckets,
        missing_market_buckets=missing_market_buckets,
        complete=complete,
    )


def _ensure_schema(connection: sqlite3.Connection) -> None:
    prediction_columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(predictions)").fetchall()
    }
    for column, definition in (
        ("origin_bar_timestamp", "TEXT"),
        ("origin_close", "REAL"),
        ("origin_bar_index", "INTEGER"),
        ("origin_bid", "REAL"),
        ("origin_ask", "REAL"),
        ("barrier_spec_id", "TEXT"),
        ("feature_version", "TEXT"),
        ("direction_probability_up", "REAL"),
        ("barrier_probability_long", "REAL"),
        ("barrier_probability_short", "REAL"),
        ("is_duplicate", "INTEGER NOT NULL DEFAULT 0"),
        ("settlement_status", "TEXT NOT NULL DEFAULT 'PENDING'"),
        ("settlement_reason", "TEXT"),
    ):
        if column not in prediction_columns:
            connection.execute(
                f"ALTER TABLE predictions ADD COLUMN {column} {definition}"
            )
    outcome_columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(prediction_horizon_outcomes)"
        ).fetchall()
    }
    for column in ("barrier_long_outcome", "barrier_short_outcome"):
        if outcome_columns and column not in outcome_columns:
            connection.execute(
                f"ALTER TABLE prediction_horizon_outcomes ADD COLUMN {column} TEXT"
            )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_tick_paths (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            tick_path_json TEXT NOT NULL,
            path_point_count INTEGER NOT NULL,
            first_tick_msc INTEGER NOT NULL,
            last_tick_msc INTEGER NOT NULL,
            path_valid INTEGER NOT NULL,
            source TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(symbol, timeframe, timestamp)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_horizon_outcomes (
            prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
            horizon_bars INTEGER NOT NULL,
            origin_bar_timestamp TEXT NOT NULL,
            outcome_bar_timestamp TEXT NOT NULL,
            actual_return REAL NOT NULL,
            actual_high REAL NOT NULL,
            actual_low REAL NOT NULL,
            interval_hit INTEGER NOT NULL,
            direction_hit INTEGER NOT NULL,
            barrier_outcome TEXT,
            barrier_long_outcome TEXT,
            barrier_short_outcome TEXT,
            error_metrics_json TEXT NOT NULL,
            settled_at TEXT NOT NULL,
            PRIMARY KEY(prediction_id, horizon_bars)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_proposal_instances (
            proposal_id TEXT PRIMARY KEY,
            prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
            profile TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            quote_timestamp TEXT NOT NULL,
            action TEXT NOT NULL,
            target_price REAL,
            stop_price REAL,
            evidence_eligible INTEGER NOT NULL DEFAULT 0,
            proposal_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_proposal_outcomes (
            proposal_id TEXT PRIMARY KEY REFERENCES decision_proposal_instances(proposal_id),
            prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
            profile TEXT NOT NULL,
            horizon_bars INTEGER NOT NULL,
            action TEXT NOT NULL,
            target_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            barrier_outcome TEXT NOT NULL,
            first_touch_time_msc INTEGER,
            first_touch_price REAL,
            settlement_source TEXT NOT NULL DEFAULT 'TICK_SEQUENCE',
            settled_at TEXT NOT NULL,
            UNIQUE(proposal_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_proposal_evidence (
            proposal_id TEXT NOT NULL,
            evidence_source TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(proposal_id, evidence_source)
        )
        """
    )
    proposal_outcome_columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(decision_proposal_outcomes)"
        ).fetchall()
    }
    for column, definition in (
        ("first_touch_time_msc", "INTEGER"),
        ("first_touch_price", "REAL"),
        ("settlement_source", "TEXT NOT NULL DEFAULT 'TICK_SEQUENCE'"),
    ):
        if column not in proposal_outcome_columns:
            connection.execute(
                f"ALTER TABLE decision_proposal_outcomes ADD COLUMN {column} {definition}"
            )
    proposal_instance_columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(decision_proposal_instances)"
        ).fetchall()
    }
    for column, definition in (
        ("settlement_status", "TEXT NOT NULL DEFAULT 'PENDING'"),
        ("settlement_reason", "TEXT"),
    ):
        if column not in proposal_instance_columns:
            connection.execute(
                f"ALTER TABLE decision_proposal_instances ADD COLUMN {column} {definition}"
            )
    _ensure_same_timestamp_contract(connection)


def _ensure_same_timestamp_contract(connection: sqlite3.Connection) -> None:
    schemas = {
        str(row["name"]): str(row["sql"])
        for row in connection.execute(
            """
            SELECT name, sql FROM sqlite_master
            WHERE type='table' AND name IN (
              'prediction_horizon_outcomes',
              'decision_proposal_outcomes'
            )
            """
        ).fetchall()
    }
    if len(schemas) == 2 and all(
        "AMBIGUOUS_SAME_TIMESTAMP" in schema for schema in schemas.values()
    ):
        return
    connection.commit()
    connection.executescript(
        """
        PRAGMA foreign_keys=OFF;
        CREATE TABLE prediction_horizon_outcomes_v5 (
            prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
            horizon_bars INTEGER NOT NULL,
            origin_bar_timestamp TEXT NOT NULL,
            outcome_bar_timestamp TEXT NOT NULL,
            actual_return REAL NOT NULL,
            actual_high REAL NOT NULL,
            actual_low REAL NOT NULL,
            interval_hit INTEGER NOT NULL,
            direction_hit INTEGER NOT NULL,
            barrier_outcome TEXT CHECK(
              barrier_outcome IS NULL OR barrier_outcome IN (
                'TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY',
                'AMBIGUOUS_SAME_BAR','AMBIGUOUS_SAME_TIMESTAMP'
              )
            ),
            barrier_long_outcome TEXT CHECK(
              barrier_long_outcome IS NULL OR barrier_long_outcome IN (
                'TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY',
                'AMBIGUOUS_SAME_BAR','AMBIGUOUS_SAME_TIMESTAMP'
              )
            ),
            barrier_short_outcome TEXT CHECK(
              barrier_short_outcome IS NULL OR barrier_short_outcome IN (
                'TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY',
                'AMBIGUOUS_SAME_BAR','AMBIGUOUS_SAME_TIMESTAMP'
              )
            ),
            error_metrics_json TEXT NOT NULL,
            settled_at TEXT NOT NULL,
            PRIMARY KEY(prediction_id, horizon_bars)
        );
        INSERT INTO prediction_horizon_outcomes_v5
        SELECT prediction_id, horizon_bars, origin_bar_timestamp,
               outcome_bar_timestamp, actual_return, actual_high, actual_low,
               interval_hit, direction_hit, barrier_outcome,
               barrier_long_outcome, barrier_short_outcome,
               error_metrics_json, settled_at
        FROM prediction_horizon_outcomes;
        DROP TABLE prediction_horizon_outcomes;
        ALTER TABLE prediction_horizon_outcomes_v5
          RENAME TO prediction_horizon_outcomes;

        CREATE TABLE decision_proposal_outcomes_v5 (
            proposal_id TEXT PRIMARY KEY REFERENCES decision_proposal_instances(proposal_id),
            prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
            profile TEXT NOT NULL,
            horizon_bars INTEGER NOT NULL,
            action TEXT NOT NULL,
            target_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            barrier_outcome TEXT NOT NULL CHECK(
              barrier_outcome IN (
                'TP_FIRST','SL_FIRST','NO_HIT_BEFORE_EXPIRY',
                'AMBIGUOUS_SAME_BAR','AMBIGUOUS_SAME_TIMESTAMP'
              )
            ),
            first_touch_time_msc INTEGER,
            first_touch_price REAL,
            settlement_source TEXT NOT NULL DEFAULT 'TICK_SEQUENCE',
            settled_at TEXT NOT NULL
        );
        INSERT INTO decision_proposal_outcomes_v5
        SELECT proposal_id, prediction_id, profile, horizon_bars, action,
               target_price, stop_price, barrier_outcome,
               first_touch_time_msc, first_touch_price, settlement_source,
               settled_at
        FROM decision_proposal_outcomes;
        DROP TABLE decision_proposal_outcomes;
        ALTER TABLE decision_proposal_outcomes_v5
          RENAME TO decision_proposal_outcomes;

        CREATE INDEX IF NOT EXISTS idx_prediction_horizon_outcomes_time
          ON prediction_horizon_outcomes(settled_at DESC, horizon_bars);
        CREATE INDEX IF NOT EXISTS idx_decision_proposal_outcomes_time
          ON decision_proposal_outcomes(settled_at DESC, profile, horizon_bars);
        PRAGMA foreign_keys=ON;
        """
    )


def settle_with_report(database_path: Path) -> dict[str, int]:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    _ensure_schema(connection)
    predictions = connection.execute(
        """
        SELECT p.*
        FROM predictions p
        WHERE p.origin_bar_timestamp IS NOT NULL
          AND p.origin_close IS NOT NULL
          AND p.origin_bid IS NOT NULL
          AND p.origin_ask IS NOT NULL
          AND p.barrier_spec_id=?
          AND COALESCE(p.is_duplicate, 0)=0
          AND COALESCE(p.settlement_status, 'PENDING') IN (
            'PENDING',
            'PRICE_OUTCOMES_SETTLED',
            'BARRIER_PENDING',
            'TICK_PATH_INCOMPLETE',
            'SESSION_INTERRUPTED'
          )
        ORDER BY p.origin_bar_timestamp
        """,
        (BARRIER_SPEC_ID,),
    ).fetchall()
    settled_horizons = 0
    settled_predictions: set[str] = set()
    now = datetime.now(UTC).isoformat()

    for prediction in predictions:
        forecast = json.loads(prediction["forecast_json"])
        origin_timestamp = str(prediction["origin_bar_timestamp"])
        origin_close = float(prediction["origin_close"])
        if (
            forecast.get("origin_bar_timestamp") != origin_timestamp
            or abs(float(forecast.get("origin_close", 0.0)) - origin_close) > 1e-8
        ):
            continue
        points = sorted(
            forecast["points"],
            key=lambda point: int(point["horizon_bars"]),
        )
        if not points:
            continue
        direction_probability_up = prediction["direction_probability_up"]
        if direction_probability_up is None:
            direction_probability_up = forecast.get("direction_probability_up")
        if direction_probability_up is None:
            connection.execute(
                """
                UPDATE predictions
                SET settlement_status='LEGACY_UNSETTLEABLE',
                    settlement_reason='DIRECTION_PROBABILITY_UNAVAILABLE'
                WHERE prediction_id=?
                """,
                (prediction["prediction_id"],),
            )
            continue
        settlement_reason: str | None = None
        try:
            origin_time = datetime.fromisoformat(
                origin_timestamp.replace("Z", "+00:00")
            )
        except ValueError:
            continue
        if origin_time.tzinfo is None:
            continue
        origin_time = origin_time.astimezone(UTC)
        maximum_horizon = max(int(point["horizon_bars"]) for point in points)
        outcome_bars = connection.execute(
            """
            SELECT timestamp, bid_high, bid_low, bid_close,
                   ask_high, ask_low, ask_close
            FROM market_bars
            WHERE symbol=? AND timeframe=? AND timestamp>?
              AND bid_high IS NOT NULL AND bid_low IS NOT NULL
              AND bid_close IS NOT NULL AND ask_high IS NOT NULL
              AND ask_low IS NOT NULL AND ask_close IS NOT NULL
            ORDER BY timestamp LIMIT ?
            """,
            (
                prediction["symbol"],
                prediction["timeframe"],
                origin_timestamp,
                maximum_horizon,
            ),
        ).fetchall()
        for point in points:
            horizon = int(point["horizon_bars"])
            if horizon <= 0 or len(outcome_bars) < horizon:
                continue
            bars = outcome_bars[:horizon]
            if not _is_exact_m5_horizon(
                origin_time,
                [str(bar["timestamp"]) for bar in bars],
            ):
                settlement_reason = "SESSION_INTERRUPTED"
                continue
            final_price = float(bars[-1]["bid_close"])
            actual_return = math.log(final_price / origin_close)
            actual_high = max(float(bar["bid_high"]) for bar in bars)
            actual_low = min(float(bar["bid_low"]) for bar in bars)
            actual_ask_high = max(float(bar["ask_high"]) for bar in bars)
            actual_ask_low = min(float(bar["ask_low"]) for bar in bars)
            interval_hit = int(point["q10"] <= actual_return <= point["q90"])
            direction_hit = classifier_direction_hit(
                float(direction_probability_up),
                actual_return,
            )
            if point["q50"] >= 0:
                expected_mfe = float(forecast["expected_mfe_long"])
                expected_mae = float(forecast["expected_mae_long"])
                origin_ask = float(prediction["origin_ask"])
                actual_mfe = max(0.0, actual_high - origin_ask)
                actual_mae = max(0.0, origin_ask - actual_low)
            else:
                expected_mfe = float(forecast["expected_mfe_short"])
                expected_mae = float(forecast["expected_mae_short"])
                origin_bid = float(prediction["origin_bid"])
                actual_mfe = max(0.0, origin_bid - actual_ask_low)
                actual_mae = max(0.0, actual_ask_high - origin_bid)
            barrier_long = None
            barrier_short = None
            barrier = None
            if horizon == 3:
                first_bucket = origin_time + timedelta(minutes=5)
                end_exclusive = origin_time + timedelta(minutes=20)
                tick_window = _load_tick_path(
                    connection,
                    symbol=str(prediction["symbol"]),
                    timeframe=str(prediction["timeframe"]),
                    first_bucket=first_bucket,
                    end_exclusive=end_exclusive,
                )
                if tick_window.complete:
                    long_result = tick_sequence_barrier_outcome(
                        "LONG",
                        float(forecast["target_price_long"]),
                        float(forecast["stop_price_long"]),
                        tick_window.ticks,
                        start_exclusive_msc=int(first_bucket.timestamp() * 1000) - 1,
                        end_exclusive_msc=int(end_exclusive.timestamp() * 1000),
                    )
                    short_result = tick_sequence_barrier_outcome(
                        "SHORT",
                        float(forecast["target_price_short"]),
                        float(forecast["stop_price_short"]),
                        tick_window.ticks,
                        start_exclusive_msc=int(first_bucket.timestamp() * 1000) - 1,
                        end_exclusive_msc=int(end_exclusive.timestamp() * 1000),
                    )
                    barrier_long = long_result[0] if long_result else None
                    barrier_short = short_result[0] if short_result else None
                else:
                    settlement_reason = tick_window.incomplete_reason
                barrier = barrier_long if point["q50"] >= 0 else barrier_short
            metrics = {
                "median_error": abs(actual_return - point["q50"]),
                "interval_miss": not bool(interval_hit),
                "direction_error": not bool(direction_hit),
                "mfe_error_price_distance": (
                    expected_mfe - actual_mfe if horizon == 3 else None
                ),
                "mae_error_price_distance": (
                    expected_mae - actual_mae if horizon == 3 else None
                ),
            }
            inserted = connection.execute(
                """
                INSERT INTO prediction_horizon_outcomes
                (prediction_id, horizon_bars, origin_bar_timestamp,
                 outcome_bar_timestamp, actual_return, actual_high, actual_low,
                 interval_hit, direction_hit, barrier_outcome,
                 barrier_long_outcome, barrier_short_outcome,
                 error_metrics_json, settled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(prediction_id, horizon_bars) DO UPDATE SET
                  barrier_outcome=COALESCE(
                    prediction_horizon_outcomes.barrier_outcome,
                    excluded.barrier_outcome
                  ),
                  barrier_long_outcome=COALESCE(
                    prediction_horizon_outcomes.barrier_long_outcome,
                    excluded.barrier_long_outcome
                  ),
                  barrier_short_outcome=COALESCE(
                    prediction_horizon_outcomes.barrier_short_outcome,
                    excluded.barrier_short_outcome
                  ),
                  settled_at=CASE
                    WHEN excluded.barrier_long_outcome IS NOT NULL
                     AND excluded.barrier_short_outcome IS NOT NULL
                    THEN excluded.settled_at
                    ELSE prediction_horizon_outcomes.settled_at
                  END
                WHERE (
                    prediction_horizon_outcomes.barrier_long_outcome IS NULL
                    AND excluded.barrier_long_outcome IS NOT NULL
                  )
                   OR (
                    prediction_horizon_outcomes.barrier_short_outcome IS NULL
                    AND excluded.barrier_short_outcome IS NOT NULL
                  )
                """,
                (
                    prediction["prediction_id"],
                    horizon,
                    origin_timestamp,
                    bars[-1]["timestamp"],
                    actual_return,
                    actual_high,
                    actual_low,
                    interval_hit,
                    direction_hit,
                    barrier,
                    barrier_long,
                    barrier_short,
                    json.dumps(metrics),
                    now,
                ),
            ).rowcount
            if inserted:
                settled_horizons += 1
                settled_predictions.add(str(prediction["prediction_id"]))
        completion = connection.execute(
            """
            SELECT COUNT(*) AS completed_horizons,
                   MAX(CASE
                         WHEN horizon_bars=3
                          AND barrier_long_outcome IS NOT NULL
                          AND barrier_short_outcome IS NOT NULL
                         THEN 1 ELSE 0
                       END) AS barriers_complete
            FROM prediction_horizon_outcomes
            WHERE prediction_id=? AND horizon_bars IN (1,3,6,12)
            """,
            (prediction["prediction_id"],),
        ).fetchone()
        completed_horizons = int(completion["completed_horizons"] or 0)
        barriers_complete = bool(completion["barriers_complete"])
        if completed_horizons == 4 and barriers_complete:
            settlement_status, settlement_reason = "SETTLED", None
        elif completed_horizons == 4:
            settlement_status = settlement_reason or "BARRIER_PENDING"
        elif settlement_reason:
            settlement_status = settlement_reason
        else:
            settlement_status, settlement_reason = "PENDING", None
        connection.execute(
            """
            UPDATE predictions
            SET settlement_status=?, settlement_reason=?
            WHERE prediction_id=?
            """,
            (
                settlement_status,
                settlement_reason,
                prediction["prediction_id"],
            ),
        )

    settled_proposals = 0
    proposal_instances = connection.execute(
        """
        SELECT dpi.*, p.symbol, p.timeframe
        FROM decision_proposal_instances dpi
        JOIN predictions p ON p.prediction_id=dpi.prediction_id
        WHERE dpi.evidence_eligible=1
          AND dpi.action IN ('LONG','SHORT')
          AND dpi.target_price IS NOT NULL
          AND dpi.stop_price IS NOT NULL
          AND p.barrier_spec_id=?
          AND NOT EXISTS (
            SELECT 1 FROM decision_proposal_outcomes dpo
            WHERE dpo.proposal_id=dpi.proposal_id
          )
        """,
        (BARRIER_SPEC_ID,),
    ).fetchall()
    for proposal in proposal_instances:
        quote = datetime.fromisoformat(
            str(proposal["quote_timestamp"]).replace("Z", "+00:00")
        )
        bucket_epoch = int(quote.timestamp()) // 300 * 300
        first_bucket = datetime.fromtimestamp(bucket_epoch, tz=UTC)
        try:
            proposal_json = json.loads(str(proposal["proposal_json"]))
            outcome_matures_at = datetime.fromisoformat(
                str(proposal_json["outcome_matures_at"]).replace("Z", "+00:00")
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if datetime.now(UTC) < outcome_matures_at:
            continue
        tick_window = _load_tick_path(
            connection,
            symbol=str(proposal["symbol"]),
            timeframe=str(proposal["timeframe"]),
            first_bucket=first_bucket,
            end_exclusive=outcome_matures_at,
        )
        if not tick_window.complete:
            connection.execute(
                """
                UPDATE decision_proposal_instances
                SET settlement_status=?, settlement_reason=?
                WHERE proposal_id=?
                """,
                (
                    tick_window.incomplete_reason,
                    tick_window.incomplete_reason,
                    proposal["proposal_id"],
                ),
            )
            continue
        outcome = tick_sequence_barrier_outcome(
            str(proposal["action"]),
            float(proposal["target_price"]),
            float(proposal["stop_price"]),
            tick_window.ticks,
            start_exclusive_msc=int(quote.timestamp() * 1000),
            end_exclusive_msc=int(outcome_matures_at.timestamp() * 1000),
        )
        if outcome is None:
            continue
        settled_proposals += connection.execute(
            """
            INSERT OR IGNORE INTO decision_proposal_outcomes
            (proposal_id, prediction_id, profile, horizon_bars, action,
             target_price, stop_price, barrier_outcome, first_touch_time_msc,
             first_touch_price, settlement_source, settled_at)
            VALUES (?, ?, ?, 3, ?, ?, ?, ?, ?, ?, 'TICK_SEQUENCE', ?)
            """,
            (
                proposal["proposal_id"],
                proposal["prediction_id"],
                proposal["profile"],
                proposal["action"],
                proposal["target_price"],
                proposal["stop_price"],
                outcome[0],
                outcome[1],
                outcome[2],
                now,
            ),
        ).rowcount
        connection.execute(
            """
            UPDATE decision_proposal_instances
            SET settlement_status='SETTLED', settlement_reason=NULL
            WHERE proposal_id=?
            """,
            (proposal["proposal_id"],),
        )

    connection.commit()
    connection.close()
    return {
        "settled": settled_horizons,
        "settled_horizons": settled_horizons,
        "settled_predictions": len(settled_predictions),
        "settled_proposals": settled_proposals,
    }


def settle(database_path: Path) -> int:
    return settle_with_report(database_path)["settled"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Settle XPDE predictions by exact completed-bar horizon"
    )
    parser.add_argument("--database", type=Path, default=Path("data/xpde.sqlite"))
    args = parser.parse_args()
    print(json.dumps(settle_with_report(args.database)))


if __name__ == "__main__":
    main()
