from __future__ import annotations

import json
import math
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from xpde_ml.settle_outcomes import barrier_outcome, settle_with_report


def proposal(action: str, invalidation_price: float | None) -> dict:
    return {
        "action": action,
        "invalidation_price": invalidation_price,
    }


def test_barrier_outcome_uses_first_actionable_proposal() -> None:
    proposals = [
        proposal("WAIT", None),
        proposal("LONG", 99.0),
    ]
    bars = [{"high": 101.2, "low": 99.5, "close": 101.0}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) == "TP_FIRST"


def test_barrier_outcome_detects_short_stop_first() -> None:
    proposals = [proposal("SHORT", 101.0)]
    bars = [{"high": 101.2, "low": 99.5, "close": 100.8}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) == "SL_FIRST"


def test_barrier_outcome_keeps_same_bar_ambiguity_explicit() -> None:
    proposals = [proposal("LONG", 99.0)]
    bars = [{"high": 101.2, "low": 98.8, "close": 100.2}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) == "AMBIGUOUS_SAME_BAR"


def test_barrier_outcome_requires_directional_proposal() -> None:
    proposals = [proposal("NO_PREDICTION", None)]
    bars = [{"high": 102.0, "low": 98.0, "close": 101.0}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) is None


def test_barrier_outcome_keeps_no_hit_before_expiry() -> None:
    proposals = [proposal("LONG", 99.0)]
    bars = [{"high": 100.8, "low": 99.4, "close": 100.2}]

    assert (
        barrier_outcome(100.0, 1.0, proposals, bars)
        == "NO_HIT_BEFORE_EXPIRY"
    )


def test_settlement_uses_exact_origin_and_completed_bar_count(tmp_path) -> None:
    database = tmp_path / "xpde.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE predictions (
            prediction_id TEXT PRIMARY KEY,
            model_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            origin_bar_timestamp TEXT,
            origin_close REAL,
            origin_bar_index INTEGER,
            generated_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            forecast_json TEXT NOT NULL,
            proposal_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE market_bars (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            tick_volume REAL NOT NULL,
            PRIMARY KEY(symbol, timeframe, timestamp)
        );
        """
    )
    origin = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
    origin_text = origin.isoformat()
    forecast = {
        "origin_bar_timestamp": origin_text,
        "origin_close": 100.0,
        "expected_mfe_long": 1.0,
        "expected_mae_long": 1.0,
        "expected_mfe_short": 1.0,
        "expected_mae_short": 1.0,
        "points": [
            {
                "horizon_bars": horizon,
                "q10": -0.10,
                "q50": 0.01,
                "q90": 0.10,
            }
            for horizon in (1, 3, 6, 12)
        ],
    }
    proposals = [proposal("LONG", 99.0)]
    connection.execute(
        """
        INSERT INTO predictions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "prediction-1",
            "model-1",
            "GOLDm#",
            "M5",
            origin_text,
            100.0,
            int(origin.timestamp() // 300),
            (origin + timedelta(minutes=4, seconds=59)).isoformat(),
            (origin + timedelta(minutes=15)).isoformat(),
            json.dumps(forecast),
            json.dumps(proposals),
            origin.isoformat(),
        ),
    )
    for index, close in enumerate((100.2, 101.0, 103.0), 1):
        timestamp = (origin + timedelta(minutes=5 * index)).isoformat()
        connection.execute(
            """
            INSERT INTO market_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "GOLDm#",
                "M5",
                timestamp,
                100.0,
                close + 0.2,
                99.5,
                close,
                100.0,
            ),
        )
    connection.commit()
    connection.close()

    report = settle_with_report(database)
    assert report["settled_horizons"] == 2

    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT * FROM prediction_horizon_outcomes ORDER BY horizon_bars
        """
    ).fetchall()
    assert [row["horizon_bars"] for row in rows] == [1, 3]
    assert rows[1]["outcome_bar_timestamp"] == (
        origin + timedelta(minutes=15)
    ).isoformat()
    assert float(rows[1]["actual_return"]) == pytest.approx(math.log(103.0 / 100.0))
    assert rows[1]["barrier_outcome"] == "TP_FIRST"
    connection.close()
