from __future__ import annotations

import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path

import pytest
from conftest import PREDICTION_ID, make_bar
from jsonschema import Draft202012Validator
from xpde_mcp.server import create_server
from xpde_mcp.technical.alignment import align_forecast
from xpde_mcp.technical.indicators import atr_wilder, ema, rsi_wilder
from xpde_mcp.technical.packet import build_technical_view
from xpde_mcp.technical.resample import resample_completed_m5
from xpde_mcp.technical.types import normalize_m5_bars


def test_indicator_reference_values_are_deterministic() -> None:
    values = list(range(1, 21))
    ema_values = ema(values, 5)
    assert ema_values[4] == 3.0
    assert ema_values[-1] == pytest.approx(18.0, abs=1e-6)
    rsi_values = rsi_wilder(values, 14)
    assert rsi_values[-1] == 100.0
    bars = normalize_m5_bars([make_bar(index) for index in range(40)])
    atr_values = atr_wilder(bars, 14)
    assert atr_values[-1] is not None
    assert atr_values[-1] > 0


def test_resampling_rejects_incomplete_m15_and_h1_buckets() -> None:
    values = [make_bar(index) for index in range(24)]
    del values[1]
    bars = normalize_m5_bars(values)
    m15 = resample_completed_m5(bars, "M15")
    h1 = resample_completed_m5(bars, "H1")
    assert all(bar.timestamp.minute != 0 for bar in m15[:1])
    assert len(m15) == 7
    assert len(h1) == 1
    assert h1[0].timestamp.hour == 1


def test_technical_view_uses_completed_bar_rules_only() -> None:
    bars = normalize_m5_bars([make_bar(index) for index in range(720)])
    view = build_technical_view(bars, "H1")
    assert view["bar_count"] == 60
    assert view["trend"]["state"] in {"BULLISH", "BEARISH", "MIXED"}
    assert view["momentum"]["state"] in {"BULLISH", "BEARISH", "NEUTRAL"}
    assert view["volatility"]["atr14"] is not None
    assert view["structure"]["rule"]["left_bars"] == 2
    assert view["structure"]["rule"]["right_bars"] == 2


def test_requested_market_structure_depth_is_honored(service_fixture) -> None:
    result = service_fixture["service"].get_market_structure(
        timeframe="M15",
        bars=50,
    )
    assert result["view"]["bar_count"] == 50


def test_warming_up_forecast_is_usable_guide_but_wait_is_not_actionable(
    service_fixture,
) -> None:
    current = service_fixture["service"].get_current("SCALPER")
    assert current["source_status"] == {
        "market_data_current": True,
        "forecast_usable_as_guide": True,
        "core_actionable": False,
        "evidence_stage": "WARMING_UP",
        "reason_codes": [],
    }
    assert current["core_decision"]["action"] == "WAIT"
    assert current["core_decision"]["must_preserve"] is True
    assert "login" not in current["market"]["account"]
    assert "server" not in current["market"]["account"]


def test_packet_preserves_non_up_semantics_and_exact_core_decision(
    service_fixture,
) -> None:
    packet = service_fixture["service"].analyze_current(
        profile="SCALPER",
        depth="STANDARD",
    )
    direction = packet["forecast"]["direction"]
    assert direction["probability_up"] == 0.58
    assert direction["probability_non_up"] == pytest.approx(0.42)
    assert "not strict P(DOWN)" in direction["non_up_definition"]
    assert packet["core_decision"]["action"] == "WAIT"
    assert packet["core_decision"]["must_preserve"] is True
    assert packet["source_status"]["forecast_usable_as_guide"] is True
    assert packet["source_status"]["evidence_stage"] == "WARMING_UP"
    assert packet["market"]["current_m5_observation"]["included_in_indicators"] is False
    assert (
        packet["technical_views"]["M5"]["latest_completed_timestamp"]
        == service_fixture["bars"][-1]["timestamp"]
    )


def test_packet_matches_published_json_schema(service_fixture) -> None:
    packet = service_fixture["service"].analyze_current()
    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "xpde_mcp"
        / "schemas"
        / "technical-analysis-packet.schema.json"
    )
    schema = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(packet)


def test_every_alignment_basis_references_an_observation(service_fixture) -> None:
    packet = service_fixture["service"].analyze_current()
    identifiers = {item["id"] for item in packet["observations"]}
    for collection in ("confluences", "conflicts"):
        for item in packet["alignment"][collection]:
            assert set(item["basis_ids"]) <= identifiers


def test_stale_forecast_remains_diagnostic_not_usable(service_fixture) -> None:
    service = service_fixture["service"]
    original = service.api.get_state
    stale = deepcopy(service_fixture["state"])
    stale["forecast_status"] = "EXPIRED"
    service.api.get_state = lambda: deepcopy(stale)  # type: ignore[method-assign]
    packet = service.analyze_current()
    assert packet["source_status"]["forecast_usable_as_guide"] is False
    assert packet["source_status"]["core_actionable"] is False
    assert "FORECAST_EXPIRED" in packet["source_status"]["market_data_reason_codes"]
    assert any(
        "diagnostic only" in value for value in packet["alignment"]["limitations"]
    )
    service.api.get_state = original  # type: ignore[method-assign]


def test_alignment_never_emits_trade_action() -> None:
    view = {
        "trend": {"state": "BULLISH"},
        "momentum": {"state": "BULLISH"},
        "structure": {"state": "BULLISH"},
        "levels": {"nearest_resistance": None},
    }
    result = align_forecast(
        forecast_side="BULLISH",
        views={"M5": view, "M15": view, "H1": view},
    )
    assert result["state"] == "ALIGNED"
    assert "action" not in result
    assert "BUY" not in json.dumps(result)


def test_prediction_explanation_uses_origin_and_objective_evidence(
    service_fixture,
) -> None:
    result = service_fixture["service"].explain_prediction(PREDICTION_ID)
    assert result["contract"]["analysis_reference"] == "PREDICTION_ORIGIN"
    assert result["forecast"]["prediction_id"] == PREDICTION_ID
    assert len(result["evidence"]["horizon_outcomes"]) == 4
    assert result["evidence"]["proposal_outcomes"][0]["barrier_outcome"] == "TP_FIRST"


def test_quantile_prices_are_origin_times_exponential_returns(service_fixture) -> None:
    packet = service_fixture["service"].analyze_current()
    forecast = packet["forecast"]
    h3 = next(
        point for point in forecast["quantile_points"] if point["horizon_bars"] == 3
    )
    assert h3["quantiles_bid_price"]["q50"] == pytest.approx(
        forecast["origin_close_bid"] * math.exp(h3["quantiles_log_return"]["q50"])
    )


def test_mcp_surface_contains_read_only_tools_prompt_and_resources(
    service_fixture,
) -> None:
    async def inspect_surface():
        server = create_server(service_fixture["service"])
        tools = await server.list_tools()
        prompts = await server.list_prompts()
        resources = await server.list_resources()
        return tools, prompts, resources

    tools, prompts, resources = asyncio.run(inspect_surface())
    names = {tool.name for tool in tools}
    assert names == {
        "xpde_get_current",
        "xpde_get_analysis_bundle",
        "xpde_get_recent_predictions",
        "xpde_get_prediction_evidence",
        "xpde_get_evaluation_summary",
        "xpde_get_model_manifest",
        "xpde_analyze_current",
        "xpde_get_market_structure",
        "xpde_explain_prediction",
        "xpde_get_model_evidence",
    }
    assert all(
        token not in name
        for name in names
        for token in ("execute", "feedback", "promote", "train", "register")
    )
    assert [prompt.name for prompt in prompts] == ["analyze_current_xpde_prediction"]
    assert {str(resource.uri) for resource in resources} == {
        "xpde://contracts/technical-analysis-packet",
        "xpde://contracts/tools",
    }
