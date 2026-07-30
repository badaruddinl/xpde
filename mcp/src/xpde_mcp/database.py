"""SQLite read-only evidence and market-data adapter."""

from __future__ import annotations

import json
import math
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from .contracts import MAXIMUM_MARKET_BARS, MAXIMUM_TOOL_LIMIT
from .errors import ContractError, NotFoundError, SourceUnavailableError

_SETTLEMENT_STATUSES = {
    "PENDING",
    "PRICE_OUTCOMES_SETTLED",
    "BARRIER_PENDING",
    "SETTLED",
    "TICK_PATH_INCOMPLETE",
    "SESSION_INTERRUPTED",
    "LEGACY_UNSETTLEABLE",
}


def _safe_json(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback
    return decoded


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(zip(row.keys(), row, strict=True))


def _column_names(
    connection: sqlite3.Connection,
    table: str,
) -> set[str]:
    if table not in {
        "predictions",
        "decision_proposal_instances",
    }:
        raise ContractError("unsupported schema introspection target")
    return {
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }


def _optional_column(
    columns: set[str],
    name: str,
    *,
    fallback_sql: str = "NULL",
) -> str:
    return name if name in columns else f"{fallback_sql} AS {name}"


def _validate_limit(limit: int, maximum: int = MAXIMUM_TOOL_LIMIT) -> int:
    if isinstance(limit, bool) or not 1 <= limit <= maximum:
        raise ContractError(f"limit must be between 1 and {maximum}")
    return limit


def _validate_prediction_id(prediction_id: str) -> str:
    try:
        return str(uuid.UUID(prediction_id))
    except (ValueError, AttributeError) as error:
        raise ContractError("prediction_id must be a UUID") from error


class ReadOnlyDatabase:
    """Opens a new `mode=ro` + `query_only` connection per operation."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path.resolve()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        if not self.database_path.is_file():
            raise SourceUnavailableError(
                f"XPDE database does not exist: {self.database_path}"
            )
        uri = f"{self.database_path.as_uri()}?mode=ro"
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=2.0)
        except sqlite3.Error as error:
            raise SourceUnavailableError(
                f"Cannot open XPDE database read-only: {error}"
            ) from error
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            yield connection
        finally:
            connection.close()

    def recent_predictions(
        self,
        *,
        model_id: str | None = None,
        settlement_status: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        limit = _validate_limit(limit)
        if settlement_status is not None:
            settlement_status = settlement_status.upper()
            if settlement_status not in _SETTLEMENT_STATUSES:
                raise ContractError(
                    f"unsupported settlement_status: {settlement_status}"
                )
        clauses = ["is_duplicate=0"]
        parameters: list[Any] = []
        if model_id:
            clauses.append("model_id=?")
            parameters.append(model_id)
        if settlement_status:
            clauses.append("settlement_status=?")
            parameters.append(settlement_status)
        parameters.append(limit)
        with self.connect() as connection:
            columns = _column_names(connection, "predictions")
            query = f"""
            SELECT prediction_id, model_id, feature_version,
                   {_optional_column(columns, "label_contract_id")},
                   barrier_spec_id, direction_probability_up,
                   barrier_probability_long, barrier_probability_short,
                   symbol, timeframe, origin_bar_timestamp, origin_close,
                   origin_bid, origin_ask, generated_at, expires_at,
                   decision_valid_until, outcome_matures_at,
                   settlement_status,
                   {_optional_column(columns, "settlement_reason")},
                   forecast_json,
                   proposal_json, created_at
            FROM predictions
            WHERE {" AND ".join(clauses)}
            ORDER BY origin_bar_timestamp DESC, generated_at DESC
            LIMIT ?
            """
            rows = connection.execute(query, parameters).fetchall()
        result = []
        for row in rows:
            item = _row_dict(row)
            item["forecast"] = _safe_json(item.pop("forecast_json"), {})
            item["initial_proposals"] = _safe_json(item.pop("proposal_json"), [])
            result.append(item)
        return result

    def prediction_evidence(self, prediction_id: str) -> dict[str, Any]:
        prediction_id = _validate_prediction_id(prediction_id)
        with self.connect() as connection:
            proposal_columns = _column_names(connection, "decision_proposal_instances")
            proposal_settlement_status = _optional_column(
                proposal_columns,
                "settlement_status",
                fallback_sql="'UNKNOWN'",
            )
            proposal_settlement_reason = _optional_column(
                proposal_columns,
                "settlement_reason",
            )
            prediction_row = connection.execute(
                """
                SELECT * FROM predictions
                WHERE prediction_id=? AND is_duplicate=0
                """,
                [prediction_id],
            ).fetchone()
            if prediction_row is None:
                raise NotFoundError(f"prediction not found: {prediction_id}")

            horizons = connection.execute(
                """
                SELECT horizon_bars, origin_bar_timestamp, outcome_bar_timestamp,
                       actual_return, actual_high, actual_low, interval_hit,
                       direction_hit, barrier_outcome, barrier_long_outcome,
                       barrier_short_outcome, error_metrics_json, settled_at
                FROM prediction_horizon_outcomes
                WHERE prediction_id=?
                ORDER BY horizon_bars
                """,
                [prediction_id],
            ).fetchall()
            proposals = connection.execute(
                f"""
                SELECT proposal_id, profile, evaluated_at, quote_timestamp,
                       reference_entry_price, action, target_price, stop_price,
                       remaining_reward_account, remaining_risk_account,
                       account_currency, cost_model_id, entry_spread,
                       expected_exit_spread, reason_codes_json,
                       model_health_status, evidence_eligible, evidence_source,
                       {proposal_settlement_status},
                       {proposal_settlement_reason},
                       proposal_json,
                       created_at
                FROM decision_proposal_instances
                WHERE prediction_id=?
                ORDER BY evaluated_at, profile
                """,
                [prediction_id],
            ).fetchall()
            outcomes = connection.execute(
                """
                SELECT proposal_id, profile, horizon_bars, action, target_price,
                       stop_price, barrier_outcome, first_touch_time_msc,
                       first_touch_price, settlement_source, settled_at
                FROM decision_proposal_outcomes
                WHERE prediction_id=?
                ORDER BY settled_at, profile
                """,
                [prediction_id],
            ).fetchall()
            memberships = connection.execute(
                """
                SELECT e.proposal_id, e.evidence_source, e.created_at
                FROM decision_proposal_evidence e
                JOIN decision_proposal_instances p ON p.proposal_id=e.proposal_id
                WHERE p.prediction_id=?
                ORDER BY e.created_at
                """,
                [prediction_id],
            ).fetchall()
            feedback = connection.execute(
                """
                SELECT id, proposal_id, profile, proposal_action, model_id,
                       forecast_side, selected_reason, verdict, reason_codes_json,
                       note, created_at
                FROM human_feedback
                WHERE prediction_id=?
                ORDER BY created_at
                """,
                [prediction_id],
            ).fetchall()

        prediction = _row_dict(prediction_row)
        prediction["forecast"] = _safe_json(prediction.pop("forecast_json"), {})
        prediction["initial_proposals"] = _safe_json(
            prediction.pop("proposal_json"), []
        )
        horizon_values = []
        for row in horizons:
            item = _row_dict(row)
            item["error_metrics"] = _safe_json(item.pop("error_metrics_json"), {})
            horizon_values.append(item)
        proposal_values = []
        for row in proposals:
            item = _row_dict(row)
            item["reason_codes"] = _safe_json(item.pop("reason_codes_json"), [])
            item["proposal"] = _safe_json(item.pop("proposal_json"), {})
            proposal_values.append(item)
        feedback_values = []
        for row in feedback:
            item = _row_dict(row)
            item["reason_codes"] = _safe_json(item.pop("reason_codes_json"), [])
            feedback_values.append(item)
        return {
            "prediction": prediction,
            "horizon_outcomes": horizon_values,
            "proposal_instances": proposal_values,
            "proposal_outcomes": [_row_dict(row) for row in outcomes],
            "evidence_memberships": [_row_dict(row) for row in memberships],
            "human_feedback": feedback_values,
        }

    def market_bars(
        self,
        *,
        symbol: str,
        timeframe: str = "M5",
        limit: int = 720,
        through_timestamp: str | None = None,
    ) -> list[dict[str, Any]]:
        limit = _validate_limit(limit, MAXIMUM_MARKET_BARS)
        if timeframe != "M5":
            raise ContractError("database source supports native M5 bars only")
        parameters: list[Any] = [symbol, timeframe]
        time_clause = ""
        if through_timestamp is not None:
            try:
                datetime.fromisoformat(through_timestamp.replace("Z", "+00:00"))
            except (ValueError, AttributeError) as error:
                raise ContractError("through_timestamp must be ISO-8601") from error
            time_clause = "AND timestamp<=?"
            parameters.append(through_timestamp)
        parameters.append(limit)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT timestamp,
                       COALESCE(bid_open, open) AS open,
                       COALESCE(bid_high, high) AS high,
                       COALESCE(bid_low, low) AS low,
                       COALESCE(bid_close, close) AS close,
                       tick_volume,
                       CASE WHEN bid_open IS NULL THEN 'CHART_OHLC'
                            ELSE 'BID_OHLC' END AS price_source
                FROM market_bars
                WHERE symbol=? AND timeframe=? {time_clause}
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                parameters,
            ).fetchall()
        result = [_row_dict(row) for row in reversed(rows)]
        for item in result:
            numeric = [item[key] for key in ("open", "high", "low", "close")]
            if any(
                not isinstance(value, (int, float)) or not math.isfinite(value)
                for value in numeric
            ):
                raise ContractError("market bar contains non-finite OHLC")
        return result
