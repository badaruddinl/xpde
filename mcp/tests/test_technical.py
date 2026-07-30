from __future__ import annotations

import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path

import pytest
from conftest import PREDICTION_ID, make_bar
from jsonschema import Draft202012Validator
from xpde_mcp.contracts import SEMANTIC_CONTRACT, TA_CONTRACT_ID
from xpde_mcp.prompts import analyze_current_prompt
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
    assert result["contract"]["ta_contract_id"] == TA_CONTRACT_ID
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
        "guide_statement": (
            "Forecast tersedia sebagai panduan; live evidence masih WARMING_UP "
            "dan belum matang."
        ),
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
    assert packet["contract"]["ta_contract_id"] == TA_CONTRACT_ID
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
            assert item["timeframe"] in {"M5", "M15", "H1"}


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


@pytest.mark.parametrize(
    ("health", "expected_stage", "usable", "statement_fragment"),
    [
        ("HEALTHY", "HEALTHY", True, "dipertimbangkan sebagai panduan"),
        ("WARMING_UP", "WARMING_UP", True, "belum matang"),
        ("DEGRADED", "DEGRADED", True, "peringatan kuat"),
        ("SUSPENDED", "SUSPENDED", False, "bukan panduan"),
        ("UNRECOGNIZED", "UNKNOWN", False, "tidak diketahui"),
    ],
)
def test_health_guide_matrix_is_consistent_across_all_current_outputs(
    service_fixture,
    health: str,
    expected_stage: str,
    usable: bool,
    statement_fragment: str,
) -> None:
    state = service_fixture["state"]
    state["model_health"]["status"] = health
    current = service_fixture["service"].get_current("SCALPER")
    bundle_current = service_fixture["service"].get_analysis_bundle(profile="SCALPER")[
        "current"
    ]
    packet = service_fixture["service"].analyze_current(profile="SCALPER")
    outputs = [
        current["source_status"],
        bundle_current["source_status"],
        packet["source_status"],
    ]
    for source_status in outputs:
        assert source_status["evidence_stage"] == expected_stage
        assert source_status["forecast_usable_as_guide"] is usable
        assert statement_fragment in source_status["guide_statement"]


def test_suspended_model_cannot_be_marked_actionable_even_with_directional_core_action(
    service_fixture,
) -> None:
    state = service_fixture["state"]
    state["model_health"]["status"] = "SUSPENDED"
    for proposal in state["proposals"]:
        proposal["action"] = "LONG"
    current = service_fixture["service"].get_current("SCALPER")
    packet = service_fixture["service"].analyze_current(profile="SCALPER")
    assert current["core_decision"]["action"] == "LONG"
    assert packet["core_decision"]["action"] == "LONG"
    assert current["source_status"]["forecast_usable_as_guide"] is False
    assert current["source_status"]["core_actionable"] is False
    assert packet["source_status"]["core_actionable"] is False
    assert any("diagnostik" in value for value in packet["alignment"]["limitations"])


def test_missing_model_health_is_fail_closed_as_unknown_diagnostic(
    service_fixture,
) -> None:
    service_fixture["state"]["model_health"] = None
    current = service_fixture["service"].get_current("SCALPER")
    packet = service_fixture["service"].analyze_current(profile="SCALPER")
    for source_status in (current["source_status"], packet["source_status"]):
        assert source_status["evidence_stage"] == "UNKNOWN"
        assert source_status["forecast_usable_as_guide"] is False
        assert "tidak diketahui" in source_status["guide_statement"]


def test_machine_and_prompt_contracts_publish_health_guide_semantics() -> None:
    assert SEMANTIC_CONTRACT["model_health_guide_semantics"] == {
        "HEALTHY": "GUIDE",
        "WARMING_UP": "GUIDE_WITH_IMMATURE_EVIDENCE",
        "DEGRADED": "GUIDE_WITH_STRONG_CAUTION",
        "SUSPENDED": "DIAGNOSTIC_ONLY",
        "UNKNOWN": "DIAGNOSTIC_ONLY",
    }
    prompt = analyze_current_prompt("SCALPER")
    assert "DEGRADED berarti guide" in prompt
    assert "SUSPENDED berarti forecast diagnostic only" in prompt


def _alignment_view(
    *,
    trend: str = "MIXED",
    momentum: str = "NEUTRAL",
    structure: str = "RANGE_OR_MIXED",
) -> dict:
    return {
        "trend": {"state": trend},
        "momentum": {"state": momentum},
        "structure": {"state": structure},
        "levels": {
            "nearest_resistance": None,
            "nearest_support": None,
        },
    }


def test_alignment_never_emits_trade_action() -> None:
    view = _alignment_view(
        trend="BULLISH",
        momentum="BULLISH",
        structure="BULLISH",
    )
    result = align_forecast(
        forecast_side="BULLISH",
        views={"M5": view, "M15": view, "H1": view},
    )
    assert result["state"] == "ALIGNED"
    assert "action" not in result
    assert "BUY" not in json.dumps(result)


def test_three_supporting_facts_from_one_timeframe_are_only_partially_aligned() -> None:
    result = align_forecast(
        forecast_side="BULLISH",
        views={
            "M5": _alignment_view(
                trend="BULLISH",
                momentum="BULLISH",
                structure="BULLISH",
            ),
            "M15": _alignment_view(),
            "H1": _alignment_view(),
        },
    )
    assert result["state"] == "PARTIALLY_ALIGNED"
    assert result["counts"] == {
        "confluences": 3,
        "conflicts": 0,
        "confluence_timeframes": ["M5"],
        "conflict_timeframes": [],
    }


def test_support_from_two_distinct_timeframes_is_aligned_without_conflict() -> None:
    result = align_forecast(
        forecast_side="BULLISH",
        views={
            "M5": _alignment_view(trend="BULLISH"),
            "M15": _alignment_view(momentum="BULLISH"),
            "H1": _alignment_view(),
        },
    )
    assert result["state"] == "ALIGNED"
    assert result["counts"]["confluence_timeframes"] == ["M15", "M5"]
    assert result["counts"]["conflicts"] == 0


def test_cross_timeframe_support_with_any_directional_conflict_is_not_aligned() -> None:
    result = align_forecast(
        forecast_side="BULLISH",
        views={
            "M5": _alignment_view(trend="BULLISH"),
            "M15": _alignment_view(momentum="BULLISH"),
            "H1": _alignment_view(structure="BEARISH"),
        },
    )
    assert result["state"] == "PARTIALLY_ALIGNED"
    assert result["counts"]["confluence_timeframes"] == ["M15", "M5"]
    assert result["counts"]["conflict_timeframes"] == ["H1"]


def test_nearby_execution_level_prevents_otherwise_cross_timeframe_alignment() -> None:
    m5 = _alignment_view(trend="BULLISH")
    m5["levels"]["nearest_resistance"] = {"distance_atr": 0.75}
    result = align_forecast(
        forecast_side="BULLISH",
        views={
            "M5": m5,
            "M15": _alignment_view(momentum="BULLISH"),
            "H1": _alignment_view(),
        },
    )
    assert result["state"] == "PARTIALLY_ALIGNED"
    assert result["counts"]["confluence_timeframes"] == ["M15", "M5"]
    assert result["counts"]["conflict_timeframes"] == ["M5"]
    assert any(
        "less than one ATR" in conflict["statement"] for conflict in result["conflicts"]
    )


def test_dominant_cross_timeframe_conflicts_are_classified_as_conflicted() -> None:
    result = align_forecast(
        forecast_side="BEARISH",
        views={
            "M5": _alignment_view(trend="BEARISH"),
            "M15": _alignment_view(momentum="BULLISH"),
            "H1": _alignment_view(structure="BULLISH"),
        },
    )
    assert result["state"] == "CONFLICTED"
    assert result["counts"]["confluences"] == 1
    assert result["counts"]["conflicts"] == 2


def test_directional_forecast_with_only_insufficient_views_is_insufficient() -> None:
    result = align_forecast(
        forecast_side="BULLISH",
        views={
            timeframe: _alignment_view(
                trend="INSUFFICIENT_DATA",
                momentum="INSUFFICIENT_DATA",
                structure="INSUFFICIENT_DATA",
            )
            for timeframe in ("M5", "M15", "H1")
        },
    )
    assert result["state"] == "INSUFFICIENT_DATA"
    assert result["counts"]["confluences"] == 0
    assert result["counts"]["conflicts"] == 0
    assert len(result["limitations"]) == 9


def test_flat_forecast_has_explicit_empty_alignment_provenance() -> None:
    result = align_forecast(
        forecast_side="FLAT",
        views={timeframe: _alignment_view() for timeframe in ("M5", "M15", "H1")},
    )
    assert result["state"] == "NEUTRAL"
    assert result["counts"] == {
        "confluences": 0,
        "conflicts": 0,
        "confluence_timeframes": [],
        "conflict_timeframes": [],
    }


def test_prediction_explanation_uses_origin_and_objective_evidence(
    service_fixture,
) -> None:
    result = service_fixture["service"].explain_prediction(PREDICTION_ID)
    assert result["contract"]["ta_contract_id"] == TA_CONTRACT_ID
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
