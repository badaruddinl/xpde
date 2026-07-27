from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


def settle(database_path: Path) -> int:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    now = datetime.now(UTC).isoformat()
    predictions = connection.execute(
        """
        SELECT p.*
        FROM predictions p
        LEFT JOIN prediction_outcomes o ON o.prediction_id = p.prediction_id
        WHERE o.prediction_id IS NULL AND p.expires_at <= ?
        ORDER BY p.generated_at
        """,
        (now,),
    ).fetchall()
    settled = 0

    for prediction in predictions:
        start = connection.execute(
            """
            SELECT close FROM market_bars
            WHERE symbol=? AND timeframe=? AND timestamp<=?
            ORDER BY timestamp DESC LIMIT 1
            """,
            (
                prediction["symbol"],
                prediction["timeframe"],
                prediction["generated_at"],
            ),
        ).fetchone()
        outcome_bars = connection.execute(
            """
            SELECT high, low, close FROM market_bars
            WHERE symbol=? AND timeframe=? AND timestamp>? AND timestamp<=?
            ORDER BY timestamp
            """,
            (
                prediction["symbol"],
                prediction["timeframe"],
                prediction["generated_at"],
                prediction["expires_at"],
            ),
        ).fetchall()
        if start is None or not outcome_bars:
            continue

        forecast = json.loads(prediction["forecast_json"])
        points = sorted(forecast["points"], key=lambda point: point["horizon_bars"])
        target = points[0]
        start_price = float(start["close"])
        final_price = float(outcome_bars[-1]["close"])
        actual_return = math.log(final_price / start_price)
        actual_high = max(float(bar["high"]) for bar in outcome_bars)
        actual_low = min(float(bar["low"]) for bar in outcome_bars)
        interval_hit = int(target["q10"] <= actual_return <= target["q90"])
        direction_hit = int((target["q50"] >= 0) == (actual_return >= 0))
        metrics = {
            "median_error": abs(actual_return - target["q50"]),
            "interval_miss": not bool(interval_hit),
            "direction_error": not bool(direction_hit),
        }
        connection.execute(
            """
            INSERT INTO prediction_outcomes
            (prediction_id, actual_return, actual_high, actual_low, interval_hit,
             direction_hit, tp_before_sl, error_metrics_json, settled_at)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                prediction["prediction_id"],
                actual_return,
                actual_high,
                actual_low,
                interval_hit,
                direction_hit,
                json.dumps(metrics),
                now,
            ),
        )
        settled += 1

    connection.commit()
    connection.close()
    return settled


def main() -> None:
    parser = argparse.ArgumentParser(description="Settle expired XPDE predictions")
    parser.add_argument("--database", type=Path, default=Path("data/xpde.sqlite"))
    args = parser.parse_args()
    print(json.dumps({"settled": settle(args.database)}))


if __name__ == "__main__":
    main()
