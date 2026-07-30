from __future__ import annotations

import json
import math
import sqlite3
import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from xpde_mcp.api import XpdeApiClient
from xpde_mcp.config import Settings
from xpde_mcp.core import XpdeReadOnlyService

PREDICTION_ID = "11111111-1111-5111-8111-111111111111"
MODEL_ID = "catboost-test-v5"
START = datetime(2026, 7, 27, 0, 0, tzinfo=UTC)


def make_bar(index: int) -> dict[str, Any]:
    timestamp = START + timedelta(minutes=5 * index)
    base = 4000.0 + index * 0.03 + math.sin(index / 7.0) * 0.7
    close = base + math.sin(index / 3.0) * 0.1
    high = max(base, close) + 0.4
    low = min(base, close) - 0.4
    return {
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "open": base,
        "high": high,
        "low": low,
        "close": close,
        "tick_volume": 1000 + index,
        "bid_open": base,
        "bid_high": high,
        "bid_low": low,
        "bid_close": close,
        "ask_open": base + 0.3,
        "ask_high": high + 0.3,
        "ask_low": low + 0.3,
        "ask_close": close + 0.3,
        "executable_tick_count": 1000 + index,
        "first_tick_msc": int(timestamp.timestamp() * 1000) + 1,
        "last_tick_msc": int((timestamp + timedelta(minutes=4)).timestamp() * 1000),
        "executable_tick_path": [],
        "price_source": "BID_OHLC",
    }


def make_forecast(origin_bar: dict[str, Any]) -> dict[str, Any]:
    return {
        "prediction_id": PREDICTION_ID,
        "model_id": MODEL_ID,
        "feature_version": "goldm-m5-v5",
        "label_contract_id": "exact-contiguous-m5-horizons-v1",
        "probability_reference": "FORECAST_ORIGIN",
        "entry_conditioned_probability": False,
        "origin_bar_timestamp": origin_bar["timestamp"],
        "origin_close": origin_bar["close"],
        "origin_bar_index": int(
            datetime.fromisoformat(
                origin_bar["timestamp"].replace("Z", "+00:00")
            ).timestamp()
            // 300
        ),
        "generated_at": (
            datetime.fromisoformat(origin_bar["timestamp"].replace("Z", "+00:00"))
            + timedelta(minutes=5, seconds=1)
        )
        .isoformat()
        .replace("+00:00", "Z"),
        "direction_probability_up": 0.58,
        "barrier_probability_long": 0.62,
        "barrier_probability_short": 0.28,
        "barrier_spec_id": "atr-1.25tp-1.00sl-h3-executable-tick-aligned-v6",
        "barrier_horizon_bars": 3,
        "target_price_long": origin_bar["close"] + 2.0,
        "stop_price_long": origin_bar["close"] - 1.5,
        "target_price_short": origin_bar["close"] - 2.0,
        "stop_price_short": origin_bar["close"] + 1.5,
        "expected_mfe_long": 2.2,
        "expected_mae_long": 1.1,
        "expected_mfe_short": 2.0,
        "expected_mae_short": 1.2,
        "excursion_modelled": True,
        "calibration": {
            "target_coverage": 0.8,
            "observed_coverage": 0.79,
            "sample_size": 150,
        },
        "drift_detected": False,
        "points": [
            {
                "horizon_bars": horizon,
                "q10": -0.001 * horizon**0.5,
                "q25": -0.0005 * horizon**0.5,
                "q50": 0.0002 * horizon**0.5,
                "q75": 0.0007 * horizon**0.5,
                "q90": 0.0012 * horizon**0.5,
            }
            for horizon in (1, 3, 6, 12)
        ],
    }


def make_state(
    bars: list[dict[str, Any]],
    *,
    action: str = "WAIT",
    health: str = "WARMING_UP",
    forecast_status: str = "CURRENT",
) -> dict[str, Any]:
    origin = bars[-1]
    forecast = make_forecast(origin)
    current_timestamp = datetime.fromisoformat(
        origin["timestamp"].replace("Z", "+00:00")
    ) + timedelta(minutes=5, seconds=2)
    proposal = {
        "proposal_id": "22222222-2222-5222-8222-222222222222",
        "prediction_id": PREDICTION_ID,
        "profile": "SCALPER",
        "action": action,
        "evaluated_at": current_timestamp.isoformat().replace("+00:00", "Z"),
        "quote_timestamp": current_timestamp.isoformat().replace("+00:00", "Z"),
        "decision_valid_until": (current_timestamp + timedelta(minutes=1))
        .isoformat()
        .replace("+00:00", "Z"),
        "outcome_matures_at": (current_timestamp + timedelta(minutes=15))
        .isoformat()
        .replace("+00:00", "Z"),
        "reference_entry_price": origin["close"] + 0.3,
        "target_price": origin["close"] + 2.0 if action == "LONG" else None,
        "invalidation_price": origin["close"] - 1.5 if action == "LONG" else None,
        "remaining_reward_account": 1.7 if action == "LONG" else 0.0,
        "remaining_risk_account": 1.8 if action == "LONG" else 0.0,
        "reward_risk_ratio": 0.94 if action == "LONG" else 0.0,
        "reason_codes": ["MODEL_LIVE_HEALTH_WARMING_UP"] if action == "WAIT" else [],
        "model_health_status": health,
        "account_currency": "USD",
    }
    sniper = {**proposal, "profile": "SNIPER", "proposal_id": str(uuid.uuid4())}
    current_bar = make_bar(len(bars))
    return {
        "mode": "LIVE_SHADOW",
        "connection_status": "MT5_CONNECTED",
        "updated_at": current_timestamp.isoformat().replace("+00:00", "Z"),
        "last_market_snapshot_at": current_timestamp.isoformat().replace("+00:00", "Z"),
        "forecast_status": forecast_status,
        "snapshot": {
            "symbol": "GOLDm#",
            "provider": "MetaTrader5",
            "timestamp": current_timestamp.isoformat().replace("+00:00", "Z"),
            "timeframe": "M5",
            "bid": current_bar["close"],
            "ask": current_bar["close"] + 0.3,
            "bars": bars,
            "current_bar": current_bar,
            "account": {
                "login": 123456,
                "server": "Broker-Demo",
                "currency": "USD",
                "equity": 1000.0,
            },
            "symbol_spec": {
                "digits": 2,
                "tick_size": 0.01,
                "chart_mode": "BID",
            },
            "data_quality": {
                "completeness": 1.0,
                "tick_age_ms": 100,
                "absolute_tick_age_ms": 100,
                "transport_tick_age_ms": 100,
                "market_status": "OPEN",
                "missing_flags": [],
                "reason_codes": [],
            },
        },
        "forecast": forecast,
        "proposals": [proposal, sniper],
        "model_health": {
            "status": health,
            "sample_size": 0 if health == "WARMING_UP" else 200,
            "minimum_sample_size": 100,
            "reason_codes": ["MODEL_LIVE_HEALTH_WARMING_UP"]
            if health == "WARMING_UP"
            else [],
        },
        "safety": {
            "auto_trading_enabled": False,
            "human_confirmation_required": True,
            "feed_is_demo": False,
        },
    }


def create_database(
    path: Path, bars: list[dict[str, Any]], forecast: dict[str, Any]
) -> None:
    migration = (
        Path(__file__).resolve().parents[2] / "migrations" / "001_init.sql"
    ).read_text(encoding="utf-8")
    connection = sqlite3.connect(path)
    connection.executescript(migration)
    for bar in bars:
        connection.execute(
            """
            INSERT INTO market_bars(
              symbol,timeframe,timestamp,open,high,low,close,tick_volume,
              bid_open,bid_high,bid_low,bid_close,
              ask_open,ask_high,ask_low,ask_close,
              executable_tick_count,first_tick_msc,last_tick_msc
            ) VALUES ('GOLDm#','M5',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                bar["timestamp"],
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                bar["tick_volume"],
                bar["bid_open"],
                bar["bid_high"],
                bar["bid_low"],
                bar["bid_close"],
                bar["ask_open"],
                bar["ask_high"],
                bar["ask_low"],
                bar["ask_close"],
                bar["executable_tick_count"],
                bar["first_tick_msc"],
                bar["last_tick_msc"],
            ],
        )
    connection.execute(
        """
        INSERT INTO model_registry(
          model_id,model_type,status,feature_version,label_contract_id,
          schema_version,eligibility_gate_version,training_mode,
          eligible_for_shadow,barrier_spec_id,executable_side_contract_id,
          artifact_path,metrics_json,created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            MODEL_ID,
            "catboost_multi_quantile",
            "candidate",
            "goldm-m5-v5",
            "exact-contiguous-m5-horizons-v1",
            3,
            3,
            "candidate",
            1,
            "atr-1.25tp-1.00sl-h3-executable-tick-aligned-v6",
            "bid-entry-exit-long-ask-exit-short-complete-tick-sequence-v5",
            "REPLACED_BY_FIXTURE",
            json.dumps({"holdout": {"3": {"coverage_80": 0.8}}}),
            forecast["generated_at"],
        ],
    )
    connection.execute(
        """
        INSERT INTO predictions(
          prediction_id,model_id,feature_version,label_contract_id,barrier_spec_id,
          direction_probability_up,barrier_probability_long,
          barrier_probability_short,symbol,timeframe,origin_bar_timestamp,
          origin_close,origin_bid,origin_ask,origin_bar_index,generated_at,
          expires_at,decision_valid_until,outcome_matures_at,forecast_json,
          proposal_json,is_duplicate,settlement_status,created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            PREDICTION_ID,
            MODEL_ID,
            forecast["feature_version"],
            forecast["label_contract_id"],
            forecast["barrier_spec_id"],
            forecast["direction_probability_up"],
            forecast["barrier_probability_long"],
            forecast["barrier_probability_short"],
            "GOLDm#",
            "M5",
            forecast["origin_bar_timestamp"],
            forecast["origin_close"],
            forecast["origin_close"],
            forecast["origin_close"] + 0.3,
            forecast["origin_bar_index"],
            forecast["generated_at"],
            forecast["generated_at"],
            forecast["generated_at"],
            forecast["generated_at"],
            json.dumps(forecast),
            "[]",
            0,
            "SETTLED",
            forecast["generated_at"],
        ],
    )
    for horizon in (1, 3, 6, 12):
        connection.execute(
            """
            INSERT INTO prediction_horizon_outcomes(
              prediction_id,horizon_bars,origin_bar_timestamp,
              outcome_bar_timestamp,actual_return,actual_high,actual_low,
              interval_hit,direction_hit,barrier_outcome,
              barrier_long_outcome,barrier_short_outcome,error_metrics_json,
              settled_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                PREDICTION_ID,
                horizon,
                forecast["origin_bar_timestamp"],
                forecast["origin_bar_timestamp"],
                0.001,
                forecast["origin_close"] + 2,
                forecast["origin_close"] - 1,
                1,
                1,
                "TP_FIRST" if horizon == 3 else None,
                "TP_FIRST" if horizon == 3 else None,
                "SL_FIRST" if horizon == 3 else None,
                "{}",
                forecast["generated_at"],
            ],
        )
    proposal_id = "22222222-2222-5222-8222-222222222222"
    connection.execute(
        """
        INSERT INTO decision_proposal_instances(
          proposal_id,prediction_id,profile,evaluated_at,quote_timestamp,
          proposal_fingerprint,reference_entry_price,action,target_price,
          stop_price,remaining_reward_account,remaining_risk_account,
          account_currency,cost_model_id,entry_spread,expected_exit_spread,
          reason_codes_json,model_health_status,proposal_json,evidence_eligible,
          evidence_source,settlement_status,created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            proposal_id,
            PREDICTION_ID,
            "SCALPER",
            forecast["generated_at"],
            forecast["generated_at"],
            "fixture",
            forecast["origin_close"] + 0.3,
            "LONG",
            forecast["target_price_long"],
            forecast["stop_price_long"],
            2.0,
            1.5,
            "USD",
            "fixture-cost",
            0.3,
            0.35,
            "[]",
            "HEALTHY",
            "{}",
            1,
            "FIRST_ACTIONABLE",
            "SETTLED",
            forecast["generated_at"],
        ],
    )
    connection.execute(
        """
        INSERT INTO decision_proposal_evidence(
          proposal_id,evidence_source,created_at
        ) VALUES (?,?,?)
        """,
        [proposal_id, "FIRST_ACTIONABLE", forecast["generated_at"]],
    )
    connection.execute(
        """
        INSERT INTO decision_proposal_outcomes(
          proposal_id,prediction_id,profile,horizon_bars,action,target_price,
          stop_price,barrier_outcome,first_touch_time_msc,first_touch_price,
          settlement_source,settled_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            proposal_id,
            PREDICTION_ID,
            "SCALPER",
            3,
            "LONG",
            forecast["target_price_long"],
            forecast["stop_price_long"],
            "TP_FIRST",
            123,
            forecast["target_price_long"],
            "TICK_SEQUENCE",
            forecast["generated_at"],
        ],
    )
    connection.execute(
        """
        INSERT INTO human_feedback(
          proposal_id,prediction_id,profile,proposal_action,model_id,
          forecast_side,selected_reason,verdict,reason_codes_json,note,created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            proposal_id,
            PREDICTION_ID,
            "SCALPER",
            "LONG",
            MODEL_ID,
            "LONG",
            "trend conflict",
            "ACCEPTED",
            "[]",
            "fixture",
            forecast["generated_at"],
        ],
    )
    connection.commit()
    connection.close()


@pytest.fixture
def service_fixture(tmp_path: Path):
    bars = [make_bar(index) for index in range(720)]
    state = make_state(bars)
    artifact_root = tmp_path / "artifacts"
    run = artifact_root / "run"
    run.mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "model_id": MODEL_ID,
                "schema_version": 3,
                "feature_version": "goldm-m5-v5",
                "eligible_for_shadow": True,
            }
        ),
        encoding="utf-8",
    )
    database_path = tmp_path / "xpde.sqlite"
    create_database(database_path, bars, state["forecast"])
    connection = sqlite3.connect(database_path)
    connection.execute(
        "UPDATE model_registry SET artifact_path=? WHERE model_id=?",
        [str(run), MODEL_ID],
    )
    connection.commit()
    connection.close()
    evaluation = {
        "current_model": {
            "model_id": MODEL_ID,
            "settled_predictions": 50,
            "direction_brier": 0.24,
        },
        "current_session": {"settled_predictions": 5},
        "model_health": state["model_health"],
        "settlement_completeness": {"settlement_completeness_rate": 1.0},
    }
    models = [
        {
            "model_id": MODEL_ID,
            "model_type": "catboost_multi_quantile",
            "status": "candidate",
            "feature_version": "goldm-m5-v5",
            "artifact_path": str(run),
            "metrics": {"holdout": {"3": {"coverage_80": 0.8}}},
        }
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        payloads = {
            "/api/v1/state": state,
            "/api/v1/evaluation/summary": evaluation,
            "/api/v1/models": models,
        }
        if request.method != "GET" or request.url.path not in payloads:
            return httpx.Response(405)
        return httpx.Response(200, json=deepcopy(payloads[request.url.path]))

    settings = Settings.create(
        api_base="http://127.0.0.1:8787",
        database_path=database_path,
        artifact_root=artifact_root,
    )
    api = XpdeApiClient(
        settings.api_base,
        timeout_seconds=1.0,
        transport=httpx.MockTransport(handler),
    )
    service = XpdeReadOnlyService(settings, api_client=api)
    yield {
        "service": service,
        "state": state,
        "bars": bars,
        "database_path": database_path,
        "artifact_root": artifact_root,
        "models": models,
    }
    service.close()
