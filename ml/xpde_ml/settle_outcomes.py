"""Offline analysis/replay settlement.

The Rust server is the canonical production settlement worker. Do not run this
module alongside it as a second live writer.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from .dataset import BARRIER_SPEC_ID

BARRIER_OUTCOMES = {
    "TP_FIRST",
    "SL_FIRST",
    "NO_HIT_BEFORE_EXPIRY",
    "AMBIGUOUS_SAME_BAR",
}


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
            settled_at TEXT NOT NULL,
            UNIQUE(proposal_id)
        )
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
            final_price = float(bars[-1]["bid_close"])
            actual_return = math.log(final_price / origin_close)
            actual_high = max(float(bar["bid_high"]) for bar in bars)
            actual_low = min(float(bar["bid_low"]) for bar in bars)
            actual_ask_high = max(float(bar["ask_high"]) for bar in bars)
            actual_ask_low = min(float(bar["ask_low"]) for bar in bars)
            interval_hit = int(point["q10"] <= actual_return <= point["q90"])
            direction_hit = int((point["q50"] >= 0) == (actual_return >= 0))
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
                barrier_long = barrier_outcome(
                    "LONG",
                    float(forecast["target_price_long"]),
                    float(forecast["stop_price_long"]),
                    [
                        {"high": bar["bid_high"], "low": bar["bid_low"]}
                        for bar in bars
                    ],
                )
                barrier_short = barrier_outcome(
                    "SHORT",
                    float(forecast["target_price_short"]),
                    float(forecast["stop_price_short"]),
                    [
                        {"high": bar["ask_high"], "low": bar["ask_low"]}
                        for bar in bars
                    ],
                )
                barrier = barrier_long if point["q50"] >= 0 else barrier_short
            metrics = {
                "median_error": abs(actual_return - point["q50"]),
                "interval_miss": not bool(interval_hit),
                "direction_error": not bool(direction_hit),
                "mfe_error_usd": expected_mfe - actual_mfe if horizon == 3 else None,
                "mae_error_usd": expected_mae - actual_mae if horizon == 3 else None,
            }
            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO prediction_horizon_outcomes
                (prediction_id, horizon_bars, origin_bar_timestamp,
                 outcome_bar_timestamp, actual_return, actual_high, actual_low,
                 interval_hit, direction_hit, barrier_outcome,
                 barrier_long_outcome, barrier_short_outcome,
                 error_metrics_json, settled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        bucket = datetime.fromtimestamp(bucket_epoch, tz=UTC).isoformat()
        side_prefix = "bid" if proposal["action"] == "LONG" else "ask"
        bars = connection.execute(
            f"""
            SELECT {side_prefix}_high AS high, {side_prefix}_low AS low
            FROM market_bars
            WHERE symbol=? AND timeframe=? AND timestamp>?
              AND {side_prefix}_high IS NOT NULL AND {side_prefix}_low IS NOT NULL
            ORDER BY timestamp LIMIT 3
            """,
            (proposal["symbol"], proposal["timeframe"], bucket),
        ).fetchall()
        if len(bars) < 3:
            continue
        outcome = barrier_outcome(
            str(proposal["action"]),
            float(proposal["target_price"]),
            float(proposal["stop_price"]),
            bars,
        )
        if outcome is None:
            continue
        settled_proposals += connection.execute(
            """
            INSERT OR IGNORE INTO decision_proposal_outcomes
            (proposal_id, prediction_id, profile, horizon_bars, action,
             target_price, stop_price, barrier_outcome, settled_at)
            VALUES (?, ?, ?, 3, ?, ?, ?, ?, ?)
            """,
            (
                proposal["proposal_id"],
                proposal["prediction_id"],
                proposal["profile"],
                proposal["action"],
                proposal["target_price"],
                proposal["stop_price"],
                outcome,
                now,
            ),
        ).rowcount

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
