"""STDIO-only FastMCP entry point."""

from __future__ import annotations

from importlib.resources import files
from typing import Literal

from mcp.server.fastmcp import FastMCP

from .config import Settings
from .core import XpdeReadOnlyService
from .prompts import analyze_current_prompt


def _schema_text(name: str) -> str:
    return files("xpde_mcp").joinpath("schemas", name).read_text(encoding="utf-8")


def create_server(service: XpdeReadOnlyService) -> FastMCP:
    server = FastMCP(
        name="XPDE Read-Only Technical Analysis",
        instructions=(
            "Read-only XPDE analysis. Preserve XPDE core decisions. "
            "Never create or execute replacement trade signals."
        ),
    )

    @server.tool()
    def xpde_get_current(
        profile: Literal["SCALPER", "SNIPER"] = "SCALPER",
    ) -> dict:
        """Get current market/forecast status and the exact core proposal."""

        return service.get_current(profile)

    @server.tool()
    def xpde_get_analysis_bundle(
        profile: Literal["SCALPER", "SNIPER"] = "SCALPER",
        recent_limit: int = 20,
    ) -> dict:
        """Get compact runtime, evidence, registry, manifest, and recent predictions."""

        return service.get_analysis_bundle(
            profile=profile,
            recent_limit=recent_limit,
        )

    @server.tool()
    def xpde_get_recent_predictions(
        model_id: str | None = None,
        settlement_status: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """Read canonical recent predictions with bounded optional filters."""

        return service.get_recent_predictions(
            model_id=model_id,
            settlement_status=settlement_status,
            limit=limit,
        )

    @server.tool()
    def xpde_get_prediction_evidence(prediction_id: str) -> dict:
        """Read one forecast, outcomes, proposals, first-touch evidence, and feedback."""

        return service.get_prediction_evidence(prediction_id)

    @server.tool()
    def xpde_get_evaluation_summary() -> dict:
        """Read offline/live evaluation and model-health evidence."""

        return service.get_evaluation_summary()

    @server.tool()
    def xpde_get_model_manifest(model_id: str | None = None) -> dict:
        """Read a registered manifest constrained to XPDE_ARTIFACT_ROOT."""

        return service.get_model_manifest(model_id)

    @server.tool()
    def xpde_analyze_current(
        profile: Literal["SCALPER", "SNIPER"] = "SCALPER",
        depth: Literal["SUMMARY", "STANDARD", "DETAILED"] = "STANDARD",
    ) -> dict:
        """Build the deterministic TechnicalAnalysisPacket for the current forecast."""

        return service.analyze_current(profile=profile, depth=depth)

    @server.tool()
    def xpde_get_market_structure(
        timeframe: Literal["M5", "M15", "H1"] = "M5",
        bars: int = 120,
    ) -> dict:
        """Read deterministic completed-bar structure and nearest pivot levels."""

        return service.get_market_structure(timeframe=timeframe, bars=bars)

    @server.tool()
    def xpde_explain_prediction(prediction_id: str) -> dict:
        """Explain one historical prediction using its origin technical context."""

        return service.explain_prediction(prediction_id)

    @server.tool()
    def xpde_get_model_evidence(model_id: str | None = None) -> dict:
        """Read normalized offline and live evidence for a model."""

        return service.get_model_evidence(model_id)

    @server.prompt(title="Analyze Current XPDE Prediction")
    def analyze_current_xpde_prediction(
        profile: Literal["SCALPER", "SNIPER"] = "SCALPER",
    ) -> str:
        return analyze_current_prompt(profile)

    @server.resource(
        "xpde://contracts/technical-analysis-packet",
        name="TechnicalAnalysisPacket JSON Schema",
        mime_type="application/schema+json",
    )
    def technical_analysis_packet_schema() -> str:
        return _schema_text("technical-analysis-packet.schema.json")

    @server.resource(
        "xpde://contracts/tools",
        name="XPDE MCP Tool Contracts",
        mime_type="application/json",
    )
    def tool_contracts() -> str:
        return _schema_text("tool-contracts.json")

    return server


_SERVICE = XpdeReadOnlyService(Settings.from_env())
mcp = create_server(_SERVICE)


def main() -> None:
    """Run only the STDIO transport; no HTTP listener is created."""

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
