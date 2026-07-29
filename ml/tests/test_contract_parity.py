from __future__ import annotations

import json
from pathlib import Path

from xpde_ml.contracts import validate_quantiles
from xpde_ml.dataset import BARRIER_HORIZON, BARRIER_SPEC_ID
from xpde_ml.dataset import LABEL_CONTRACT_ID


def test_python_golden_forecast_matches_v3_contract() -> None:
    fixture = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "fixtures"
        / "forecast-v3.json"
    )
    forecast = json.loads(fixture.read_text(encoding="utf-8"))
    assert forecast["barrier_spec_id"] == BARRIER_SPEC_ID
    assert forecast["label_contract_id"] == LABEL_CONTRACT_ID
    assert forecast["probability_reference"] == "FORECAST_ORIGIN"
    assert forecast["entry_conditioned_probability"] is False
    assert forecast["barrier_horizon_bars"] == BARRIER_HORIZON
    assert forecast["stop_price_long"] < forecast["origin_close"]
    assert forecast["target_price_long"] > forecast["origin_close"]
    assert forecast["target_price_short"] < forecast["origin_close"]
    assert forecast["stop_price_short"] > forecast["origin_close"]
    point = forecast["points"][0]
    validate_quantiles(
        [point["q10"], point["q25"], point["q50"], point["q75"], point["q90"]]
    )
