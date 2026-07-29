from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

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
        CREATE TABLE IF NOT EXISTS prediction_proposal_outcomes (
            prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
            profile TEXT NOT NULL,
            horizon_bars INTEGER NOT NULL,
            action TEXT NOT NULL,
            target_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            barrier_outcome TEXT NOT NULL,
            settled_at TEXT NOT NULL,
            PRIMARY KEY(prediction_id, profile, horizon_bars)
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
        ORDER BY p.origin_bar_timestamp
        """
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
            SELECT timestamp, high, low, close FROM market_bars
            WHERE symbol=? AND timeframe=? AND timestamp>?
            ORDER BY timestamp LIMIT ?
            """,
            (
                prediction["symbol"],
                prediction["timeframe"],
                origin_timestamp,
                maximum_horizon,
            ),
        ).fetchall()
        proposals = json.loads(prediction["proposal_json"])

        for point in points:
            horizon = int(point["horizon_bars"])
            if horizon <= 0 or len(outcome_bars) < horizon:
                continue
            bars = outcome_bars[:horizon]
            final_price = float(bars[-1]["close"])
            actual_return = math.log(final_price / origin_close)
            actual_high = max(float(bar["high"]) for bar in bars)
            actual_low = min(float(bar["low"]) for bar in bars)
            interval_hit = int(point["q10"] <= actual_return <= point["q90"])
            direction_hit = int((point["q50"] >= 0) == (actual_return >= 0))
            if point["q50"] >= 0:
                expected_mfe = float(forecast["expected_mfe_long"])
                expected_mae = float(forecast["expected_mae_long"])
                actual_mfe = max(0.0, actual_high - origin_close)
                actual_mae = max(0.0, origin_close - actual_low)
            else:
                expected_mfe = float(forecast["expected_mfe_short"])
                expected_mae = float(forecast["expected_mae_short"])
                actual_mfe = max(0.0, origin_close - actual_low)
                actual_mae = max(0.0, actual_high - origin_close)
            barrier_long = None
            barrier_short = None
            barrier = None
            if horizon == 3:
                barrier_long = barrier_outcome(
                    "LONG",
                    float(forecast["target_price_long"]),
                    float(forecast["stop_price_long"]),
                    bars,
                )
                barrier_short = barrier_outcome(
                    "SHORT",
                    float(forecast["target_price_short"]),
                    float(forecast["stop_price_short"]),
                    bars,
                )
                barrier = barrier_long if point["q50"] >= 0 else barrier_short
                for proposal in proposals:
                    action = str(proposal.get("action", ""))
                    target_price = proposal.get("target_price")
                    stop_price = proposal.get("invalidation_price")
                    if (
                        action not in {"LONG", "SHORT"}
                        or target_price is None
                        or stop_price is None
                    ):
                        continue
                    proposal_outcome = barrier_outcome(
                        action,
                        float(target_price),
                        float(stop_price),
                        bars,
                    )
                    if proposal_outcome is None:
                        continue
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO prediction_proposal_outcomes
                        (prediction_id, profile, horizon_bars, action,
                         target_price, stop_price, barrier_outcome, settled_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            prediction["prediction_id"],
                            proposal["profile"],
                            horizon,
                            action,
                            float(target_price),
                            float(stop_price),
                            proposal_outcome,
                            now,
                        ),
                    )
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

    connection.commit()
    connection.close()
    return {
        "settled": settled_horizons,
        "settled_horizons": settled_horizons,
        "settled_predictions": len(settled_predictions),
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
