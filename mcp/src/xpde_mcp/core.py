"""Application service composing fixed REST, read-only SQLite, and TA logic."""

from __future__ import annotations

from typing import Any

from .api import XpdeApiClient
from .config import Settings
from .contracts import INTERPRETATION_CONSTRAINTS, SEMANTIC_CONTRACT
from .database import ReadOnlyDatabase
from .errors import ContractError, XpdeMcpError
from .manifest import ManifestReader
from .technical.alignment import align_forecast
from .technical.packet import (
    build_technical_analysis_packet,
    build_technical_view,
    market_data_current,
    normalize_forecast,
)
from .technical.types import normalize_m5_bars


def _validate_profile(profile: str) -> str:
    value = profile.upper()
    if value not in {"SCALPER", "SNIPER"}:
        raise ContractError("profile must be SCALPER or SNIPER")
    return value


def _without_tick_path(bar: Any) -> Any:
    if not isinstance(bar, dict):
        return bar
    return {key: value for key, value in bar.items() if key != "executable_tick_path"}


def _compact_state(state: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(state.get("snapshot", {}))
    account = snapshot.get("account")
    if isinstance(account, dict):
        snapshot["account"] = {
            key: value
            for key, value in account.items()
            if key not in {"login", "server"}
        }
    bars = snapshot.pop("bars", [])
    snapshot["completed_bar_count"] = len(bars) if isinstance(bars, list) else 0
    snapshot["latest_completed_bar"] = (
        _without_tick_path(bars[-1]) if isinstance(bars, list) and bars else None
    )
    snapshot["current_bar"] = _without_tick_path(snapshot.get("current_bar"))
    return {
        "mode": state.get("mode"),
        "connection_status": state.get("connection_status"),
        "updated_at": state.get("updated_at"),
        "last_market_snapshot_at": state.get("last_market_snapshot_at"),
        "forecast_status": state.get("forecast_status"),
        "snapshot": snapshot,
        "forecast": state.get("forecast"),
        "proposals": state.get("proposals", []),
        "model_health": state.get("model_health"),
        "safety": state.get("safety"),
    }


def _latest_completed_timestamp(state: dict[str, Any]) -> str | None:
    bars = state.get("snapshot", {}).get("bars", [])
    timestamps = [
        value.get("timestamp")
        for value in bars
        if isinstance(value, dict) and isinstance(value.get("timestamp"), str)
    ]
    return max(timestamps) if timestamps else None


class XpdeReadOnlyService:
    """Read-only service exposed by MCP tools."""

    def __init__(
        self,
        settings: Settings,
        *,
        api_client: XpdeApiClient | None = None,
        database: ReadOnlyDatabase | None = None,
        manifest_reader: ManifestReader | None = None,
    ) -> None:
        self.settings = settings
        self.api = api_client or XpdeApiClient(
            settings.api_base,
            timeout_seconds=settings.request_timeout_seconds,
        )
        self.database = database or ReadOnlyDatabase(settings.database_path)
        self.manifests = manifest_reader or ManifestReader(settings.artifact_root)

    def close(self) -> None:
        self.api.close()

    def get_current(self, profile: str = "SCALPER") -> dict[str, Any]:
        profile = _validate_profile(profile)
        state = self.api.get_state()
        forecast_value = state.get("forecast")
        if not isinstance(forecast_value, dict):
            raise ContractError("XPDE state has no forecast")
        forecast = normalize_forecast(forecast_value)
        proposals = state.get("proposals", [])
        proposal = next(
            (
                value
                for value in proposals
                if isinstance(value, dict) and value.get("profile") == profile
            ),
            None,
        )
        if proposal is None:
            raise ContractError(f"XPDE state has no {profile} core proposal")
        current, reasons = market_data_current(state, profile)
        action = proposal.get("action")
        return {
            "source_status": {
                "market_data_current": current,
                "forecast_usable_as_guide": current,
                "core_actionable": current and action in {"LONG", "SHORT"},
                "evidence_stage": state.get("model_health", {}).get(
                    "status", "UNKNOWN"
                ),
                "reason_codes": reasons,
            },
            "market": _compact_state(state)["snapshot"],
            "forecast_status": state.get("forecast_status"),
            "forecast": forecast,
            "core_decision": {
                "action": action,
                "must_preserve": True,
                "proposal": proposal,
            },
            "model_health": state.get("model_health"),
            "safety": state.get("safety"),
            "semantic_contract": SEMANTIC_CONTRACT,
            "interpretation_constraints": INTERPRETATION_CONSTRAINTS,
        }

    def get_recent_predictions(
        self,
        *,
        model_id: str | None = None,
        settlement_status: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        return self.database.recent_predictions(
            model_id=model_id,
            settlement_status=settlement_status,
            limit=limit,
        )

    def get_prediction_evidence(self, prediction_id: str) -> dict[str, Any]:
        return self.database.prediction_evidence(prediction_id)

    def get_evaluation_summary(self) -> dict[str, Any]:
        return self.api.get_evaluation_summary()

    def get_model_manifest(self, model_id: str | None = None) -> dict[str, Any]:
        models = self.api.get_models()
        if model_id is None:
            state = self.api.get_state()
            model_id = state.get("forecast", {}).get("model_id")
        if not isinstance(model_id, str) or not model_id:
            raise ContractError("model_id is required when no current model exists")
        return self.manifests.read_registered(model_id=model_id, models=models)

    def get_analysis_bundle(
        self,
        *,
        profile: str = "SCALPER",
        recent_limit: int = 20,
    ) -> dict[str, Any]:
        profile = _validate_profile(profile)
        state = self.api.get_state()
        evaluation = self.api.get_evaluation_summary()
        models = self.api.get_models()
        model_id = state.get("forecast", {}).get("model_id")
        manifest: dict[str, Any] | None = None
        manifest_error: str | None = None
        if isinstance(model_id, str) and model_id:
            try:
                manifest = self.manifests.read_registered(
                    model_id=model_id,
                    models=models,
                )
            except XpdeMcpError as error:
                manifest_error = str(error)
        current = self.get_current_from_state(state, profile)
        return {
            "current": current,
            "evaluation_summary": evaluation,
            "model_registry": models,
            "current_model_manifest": manifest,
            "manifest_read_error": manifest_error,
            "recent_predictions": self.database.recent_predictions(
                model_id=model_id if isinstance(model_id, str) else None,
                limit=recent_limit,
            ),
            "semantic_contract": SEMANTIC_CONTRACT,
            "interpretation_constraints": INTERPRETATION_CONSTRAINTS,
        }

    def get_current_from_state(
        self,
        state: dict[str, Any],
        profile: str,
    ) -> dict[str, Any]:
        profile = _validate_profile(profile)
        forecast_value = state.get("forecast")
        if not isinstance(forecast_value, dict):
            raise ContractError("XPDE state has no forecast")
        proposals = state.get("proposals", [])
        proposal = next(
            (
                item
                for item in proposals
                if isinstance(item, dict) and item.get("profile") == profile
            ),
            None,
        )
        if proposal is None:
            raise ContractError(f"XPDE state has no {profile} core proposal")
        current, reasons = market_data_current(state, profile)
        action = proposal.get("action")
        return {
            "source_status": {
                "market_data_current": current,
                "forecast_usable_as_guide": current,
                "core_actionable": current and action in {"LONG", "SHORT"},
                "evidence_stage": state.get("model_health", {}).get(
                    "status", "UNKNOWN"
                ),
                "reason_codes": reasons,
            },
            "runtime": _compact_state(state),
            "forecast": normalize_forecast(forecast_value),
            "core_decision": {
                "action": action,
                "must_preserve": True,
                "proposal": proposal,
            },
        }

    def analyze_current(
        self,
        *,
        profile: str = "SCALPER",
        depth: str = "STANDARD",
    ) -> dict[str, Any]:
        profile = _validate_profile(profile)
        state = self.api.get_state()
        evaluation = self.api.get_evaluation_summary()
        models = self.api.get_models()
        snapshot = state.get("snapshot", {})
        symbol = snapshot.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ContractError("XPDE state has no market symbol")
        model_id = state.get("forecast", {}).get("model_id")
        manifest_evidence = None
        if isinstance(model_id, str):
            try:
                manifest_evidence = self.manifests.read_registered(
                    model_id=model_id,
                    models=models,
                )
            except XpdeMcpError:
                manifest_evidence = None
        bars = self.database.market_bars(
            symbol=symbol,
            limit=1_440,
            through_timestamp=_latest_completed_timestamp(state),
        )
        return build_technical_analysis_packet(
            state=state,
            evaluation=evaluation,
            models=models,
            market_bar_values=bars,
            profile=profile,
            depth=depth,
            manifest_evidence=manifest_evidence,
        )

    def get_market_structure(
        self,
        *,
        timeframe: str = "M5",
        bars: int = 120,
    ) -> dict[str, Any]:
        if timeframe not in {"M5", "M15", "H1"}:
            raise ContractError("timeframe must be M5, M15, or H1")
        if isinstance(bars, bool) or not 20 <= bars <= 500:
            raise ContractError("bars must be between 20 and 500")
        state = self.api.get_state()
        symbol = state.get("snapshot", {}).get("symbol")
        if not isinstance(symbol, str):
            raise ContractError("XPDE state has no market symbol")
        factor = {"M5": 1, "M15": 3, "H1": 12}[timeframe]
        native_limit = min(max(bars * factor + 120, 240), 6_000)
        values = self.database.market_bars(
            symbol=symbol,
            limit=native_limit,
            through_timestamp=_latest_completed_timestamp(state),
        )
        native = normalize_m5_bars(values)
        view = build_technical_view(
            native,
            timeframe,
            swing_limit=20,
            maximum_bars=bars,
        )
        return {
            "contract": {
                "ta_contract_id": "xpde-ta-goldm-m5-v1",
                "decision_authority": "XPDE_CORE",
            },
            "symbol": symbol,
            "requested_completed_bars": bars,
            "view": view,
            "interpretation_constraints": INTERPRETATION_CONSTRAINTS,
        }

    def explain_prediction(self, prediction_id: str) -> dict[str, Any]:
        evidence = self.database.prediction_evidence(prediction_id)
        prediction = evidence["prediction"]
        forecast_value = prediction.get("forecast")
        if not isinstance(forecast_value, dict):
            raise ContractError("stored prediction has no forecast object")
        forecast = normalize_forecast(forecast_value)
        symbol = prediction.get("symbol")
        origin = prediction.get("origin_bar_timestamp")
        if not isinstance(symbol, str) or not isinstance(origin, str):
            raise ContractError("stored prediction has no symbol/origin")
        values = self.database.market_bars(
            symbol=symbol,
            limit=1_440,
            through_timestamp=origin,
        )
        native = normalize_m5_bars(values)
        views = {
            timeframe: build_technical_view(native, timeframe, swing_limit=12)
            for timeframe in ("M5", "M15", "H1")
        }
        return {
            "contract": {
                "ta_contract_id": "xpde-ta-goldm-m5-v1",
                "analysis_reference": "PREDICTION_ORIGIN",
                "decision_authority": "XPDE_CORE",
            },
            "forecast": forecast,
            "technical_views_at_origin": views,
            "alignment_at_origin": align_forecast(
                forecast_side=forecast["direction"]["h3_q50_side"],
                views=views,
            ),
            "evidence": evidence,
            "interpretation_constraints": INTERPRETATION_CONSTRAINTS,
        }

    def get_model_evidence(self, model_id: str | None = None) -> dict[str, Any]:
        state = self.api.get_state()
        evaluation = self.api.get_evaluation_summary()
        models = self.api.get_models()
        selected = model_id or state.get("forecast", {}).get("model_id")
        manifest = None
        manifest_error = None
        if isinstance(selected, str) and selected:
            try:
                manifest = self.manifests.read_registered(
                    model_id=selected,
                    models=models,
                )
            except XpdeMcpError as error:
                manifest_error = str(error)
        registry = next(
            (item for item in models if item.get("model_id") == selected),
            None,
        )
        return {
            "model_id": selected,
            "offline": {
                "registry": registry,
                "manifest": manifest,
                "manifest_read_error": manifest_error,
            },
            "live": evaluation,
            "evidence_stage": state.get("model_health", {}).get("status", "UNKNOWN"),
            "semantic_contract": SEMANTIC_CONTRACT,
        }
