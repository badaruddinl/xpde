from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


def barrier_outcome(
    start_price: float,
    expected_mfe_usd: float,
    proposals: list[dict],
    bars: list[sqlite3.Row],
) -> int | None:
    if not math.isfinite(expected_mfe_usd) or expected_mfe_usd <= 0:
        return None
    proposal = next(
        (
            item
            for item in proposals
            if item.get("action") in {"LONG", "SHORT"}
            and item.get("invalidation_price") is not None
        ),
        None,
    )
    if proposal is None:
        return None

    action = proposal["action"]
    invalidation = float(proposal["invalidation_price"])
    target = (
        start_price + expected_mfe_usd
        if action == "LONG"
        else start_price - expected_mfe_usd
    )
    for bar in bars:
        high = float(bar["high"])
        low = float(bar["low"])
        if action == "LONG":
            tp_hit, sl_hit = high >= target, low <= invalidation
        else:
            tp_hit, sl_hit = low <= target, high >= invalidation
        if tp_hit and not sl_hit:
            return 1
        if sl_hit and not tp_hit:
            return 0
        if tp_hit and sl_hit:
            return None
    return None


def repair_missing_barrier_outcomes(
    connection: sqlite3.Connection,
    now: str,
) -> int:
    predictions = connection.execute(
        """
        SELECT p.*
        FROM predictions p
        JOIN prediction_outcomes o ON o.prediction_id = p.prediction_id
        WHERE o.tp_before_sl IS NULL AND p.expires_at <= ?
        ORDER BY p.generated_at
        """,
        (now,),
    ).fetchall()
    repaired = 0
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
        proposals = json.loads(prediction["proposal_json"])
        outcome = barrier_outcome(
            float(start["close"]),
            float(forecast["expected_mfe_usd"]),
            proposals,
            outcome_bars,
        )
        if outcome is None:
            continue
        repaired += connection.execute(
            """
            UPDATE prediction_outcomes
            SET tp_before_sl=?
            WHERE prediction_id=? AND tp_before_sl IS NULL
            """,
            (outcome, prediction["prediction_id"]),
        ).rowcount
    return repaired


def settle_with_report(database_path: Path) -> dict[str, int]:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    now = datetime.now(UTC).isoformat()
    repaired = repair_missing_barrier_outcomes(connection, now)
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
        target = next(
            (point for point in points if point["horizon_bars"] == 3),
            points[0],
        )
        proposals = json.loads(prediction["proposal_json"])
        start_price = float(start["close"])
        final_price = float(outcome_bars[-1]["close"])
        actual_return = math.log(final_price / start_price)
        actual_high = max(float(bar["high"]) for bar in outcome_bars)
        actual_low = min(float(bar["low"]) for bar in outcome_bars)
        interval_hit = int(target["q10"] <= actual_return <= target["q90"])
        direction_hit = int((target["q50"] >= 0) == (actual_return >= 0))
        tp_before_sl = barrier_outcome(
            start_price,
            float(forecast["expected_mfe_usd"]),
            proposals,
            outcome_bars,
        )
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prediction["prediction_id"],
                actual_return,
                actual_high,
                actual_low,
                interval_hit,
                direction_hit,
                tp_before_sl,
                json.dumps(metrics),
                now,
            ),
        )
        settled += 1

    connection.commit()
    connection.close()
    return {"settled": settled, "barriers_repaired": repaired}


def settle(database_path: Path) -> int:
    return settle_with_report(database_path)["settled"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Settle expired XPDE predictions")
    parser.add_argument("--database", type=Path, default=Path("data/xpde.sqlite"))
    args = parser.parse_args()
    print(json.dumps(settle_with_report(args.database)))


if __name__ == "__main__":
    main()
