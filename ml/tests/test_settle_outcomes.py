from __future__ import annotations

import json
import math
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from xpde_ml.settle_outcomes import (
    barrier_outcome,
    settle_with_report,
    tick_sequence_barrier_outcome,
)
from xpde_ml.dataset import BARRIER_SPEC_ID


def proposal(
    action: str,
    target_price: float | None,
    invalidation_price: float | None,
) -> dict:
    return {
        "profile": "SCALPER",
        "action": action,
        "target_price": target_price,
        "invalidation_price": invalidation_price,
    }


def test_barrier_outcome_uses_first_actionable_proposal() -> None:
    proposals = [
        proposal("WAIT", None, None),
        proposal("LONG", 101.0, 99.0),
    ]
    bars = [{"high": 101.2, "low": 99.5, "close": 101.0}]

    assert barrier_outcome("LONG", 101.0, 99.0, bars) == "TP_FIRST"


def test_barrier_outcome_detects_short_stop_first() -> None:
    proposals = [proposal("SHORT", 99.0, 101.0)]
    bars = [{"high": 101.2, "low": 99.5, "close": 100.8}]

    assert barrier_outcome("SHORT", 99.0, 101.0, bars) == "SL_FIRST"


def test_barrier_outcome_keeps_same_bar_ambiguity_explicit() -> None:
    proposals = [proposal("LONG", 101.0, 99.0)]
    bars = [{"high": 101.2, "low": 98.8, "close": 100.2}]

    assert (
        barrier_outcome("LONG", 101.0, 99.0, bars)
        == "AMBIGUOUS_SAME_BAR"
    )


def test_barrier_outcome_requires_directional_proposal() -> None:
    proposals = [proposal("NO_PREDICTION", None, None)]
    bars = [{"high": 102.0, "low": 98.0, "close": 101.0}]

    assert barrier_outcome("WAIT", 101.0, 99.0, bars) is None


def test_barrier_outcome_keeps_no_hit_before_expiry() -> None:
    proposals = [proposal("LONG", 101.0, 99.0)]
    bars = [{"high": 100.8, "low": 99.4, "close": 100.2}]

    assert (
        barrier_outcome("LONG", 101.0, 99.0, bars)
        == "NO_HIT_BEFORE_EXPIRY"
    )


def test_tick_sequence_starts_after_exact_quote_inside_current_candle() -> None:
    result = tick_sequence_barrier_outcome(
        "LONG",
        101.0,
        99.0,
        [
            (1_000, 98.8, 99.0),
            (2_000, 100.0, 100.2),
            (3_000, 101.2, 101.4),
        ],
        start_exclusive_msc=1_500,
        end_exclusive_msc=4_000,
    )

    assert result == ("TP_FIRST", 3_000, 101.2)


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
            origin_bid REAL,
            origin_ask REAL,
            barrier_spec_id TEXT,
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
            bid_high REAL,
            bid_low REAL,
            bid_close REAL,
            ask_high REAL,
            ask_low REAL,
            ask_close REAL,
            PRIMARY KEY(symbol, timeframe, timestamp)
        );
        CREATE TABLE market_tick_paths (
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
            PRIMARY KEY(symbol, timeframe,timestamp)
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
        "target_price_long": 101.0,
        "stop_price_long": 99.0,
        "target_price_short": 99.0,
        "stop_price_short": 101.0,
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
    proposals = [proposal("LONG", 101.0, 99.0)]
    connection.execute(
        """
        INSERT INTO predictions
        (prediction_id, model_id, symbol, timeframe, origin_bar_timestamp,
         origin_close, origin_bid, origin_ask, barrier_spec_id, origin_bar_index,
         generated_at, expires_at, forecast_json, proposal_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "prediction-1",
            "model-1",
            "GOLDm#",
            "M5",
            origin_text,
            100.0,
            100.0,
            100.2,
            BARRIER_SPEC_ID,
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
            INSERT INTO market_bars
            (symbol, timeframe, timestamp, open, high, low, close, tick_volume,
             bid_high, bid_low, bid_close, ask_high, ask_low, ask_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                close + 0.2,
                99.5,
                close,
                close + 0.4,
                99.7,
                close + 0.2,
            ),
        )
        start_msc = int((origin + timedelta(minutes=5 * index)).timestamp() * 1000)
        path = [
            [start_msc, 100.0, 100.2],
            [start_msc + 60_000, close + 0.2, close + 0.4],
            [start_msc + 120_000, 99.5, 99.7],
            [start_msc + 299_000, close, close + 0.2],
        ]
        connection.execute(
            """
            INSERT INTO market_tick_paths
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'TEST', ?)
            """,
            (
                "GOLDm#",
                "M5",
                timestamp,
                json.dumps(path),
                len(path),
                start_msc,
                start_msc + 299_000,
                datetime.now(UTC).isoformat(),
            ),
        )
    connection.execute(
        """
        CREATE TABLE decision_proposal_instances (
            proposal_id TEXT PRIMARY KEY,
            prediction_id TEXT NOT NULL,
            profile TEXT NOT NULL,
            evaluated_at TEXT NOT NULL,
            quote_timestamp TEXT NOT NULL,
            action TEXT NOT NULL,
            target_price REAL,
            stop_price REAL,
            evidence_eligible INTEGER NOT NULL,
            proposal_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        INSERT INTO decision_proposal_instances
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "proposal-1",
            "prediction-1",
            "SCALPER",
            (origin + timedelta(minutes=1)).isoformat(),
            (origin + timedelta(minutes=1)).isoformat(),
            "LONG",
            101.0,
            99.0,
            1,
            json.dumps(
                {
                    **proposals[0],
                    "outcome_matures_at": (origin + timedelta(minutes=20)).isoformat(),
                }
            ),
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
    assert rows[1]["barrier_long_outcome"] == "TP_FIRST"
    assert rows[1]["barrier_short_outcome"] == "SL_FIRST"
    h1_metrics = json.loads(rows[0]["error_metrics_json"])
    h3_metrics = json.loads(rows[1]["error_metrics_json"])
    assert h1_metrics["mfe_error_usd"] is None
    assert h1_metrics["mae_error_usd"] is None
    assert h3_metrics["mfe_error_usd"] is not None
    assert h3_metrics["mae_error_usd"] is not None
    proposal_rows = connection.execute(
        "SELECT profile, action, barrier_outcome FROM decision_proposal_outcomes"
    ).fetchall()
    assert [tuple(row) for row in proposal_rows] == [
        ("SCALPER", "LONG", "TP_FIRST")
    ]
    connection.close()
