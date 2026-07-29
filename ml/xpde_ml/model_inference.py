from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import HORIZONS, deterministic_prediction_id, validate_snapshot
from .dataset import (
    BARRIER_HORIZON,
    BARRIER_SPEC_ID,
    EXECUTABLE_SIDE_CONTRACT_ID,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    barrier_prices,
    engineer_features,
    postprocess_quantiles,
)

REQUIRED_ARTIFACT_FILES = frozenset(
    {
        *(f"quantile_h{horizon}.cbm" for horizon in HORIZONS),
        "direction.cbm",
        "barrier_long_h3.cbm",
        "barrier_short_h3.cbm",
        "mfe_long_h3.cbm",
        "mae_long_h3.cbm",
        "mfe_short_h3.cbm",
        "mae_short_h3.cbm",
        "evaluation.json",
        "model_card.md",
        "manifest.json",
        "checksums.sha256",
    }
)

INFERENCE_DEPENDENCIES = ("catboost", "numpy", "pandas")


def preflight_inference_dependencies() -> None:
    missing = [
        dependency
        for dependency in INFERENCE_DEPENDENCIES
        if importlib.util.find_spec(dependency) is None
    ]
    if missing:
        raise RuntimeError(
            "Candidate valid, but local inference dependencies are missing: "
            f"{', '.join(missing)}. Run XPDE-Install.cmd again."
        )


def candidate_registration_payload(
    manifest: dict[str, Any],
    artifact_path: Path,
) -> dict[str, Any]:
    return {
        "model_id": manifest["model_id"],
        "model_type": "catboost_multi_quantile",
        "status": "candidate",
        "feature_version": manifest["feature_version"],
        "schema_version": manifest["schema_version"],
        "eligibility_gate_version": manifest["eligibility_gate_version"],
        "training_mode": manifest["training_mode"],
        "eligible_for_shadow": manifest["eligible_for_shadow"],
        "barrier_spec_id": manifest["barrier_spec"]["id"],
        "executable_side_contract_id": manifest["executable_side_contract"]["id"],
        "eligibility_gates": manifest["eligibility_gates"],
        "artifact_path": str(artifact_path.resolve()),
        "metrics": manifest["metrics"],
    }


def verify_artifact_checksums(artifact_dir: Path) -> None:
    checksum_path = artifact_dir / "checksums.sha256"
    if not checksum_path.is_file():
        raise ValueError("artifact checksums.sha256 is missing")
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        expected, separator, filename = line.partition("  ")
        if not separator or not filename or Path(filename).name != filename:
            raise ValueError("artifact checksum manifest contains an invalid entry")
        artifact = artifact_dir / filename
        if not artifact.is_file():
            raise ValueError(f"artifact file is missing: {filename}")
        actual = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f"artifact checksum mismatch: {filename}")


def _calibrate_probability(value: float, calibration: dict[str, Any]) -> float:
    import numpy as np

    method = calibration.get("method", "isotonic")
    if method == "isotonic":
        return float(np.interp(value, calibration["x"], calibration["y"]))
    if method == "platt":
        logit = (
            value * float(calibration["coefficient"])
            + float(calibration["intercept"])
        )
        return float(1.0 / (1.0 + np.exp(-logit)))
    if method == "constant":
        return float(calibration["value"])
    raise ValueError(f"unsupported probability calibrator: {method}")


class CandidateModel:
    def __init__(
        self,
        artifact_dir: Path,
        *,
        allow_ineligible_for_testing: bool = False,
    ):
        preflight_inference_dependencies()
        import catboost

        self.artifact_dir = artifact_dir
        verify_artifact_checksums(artifact_dir)
        self.manifest = json.loads(
            (artifact_dir / "manifest.json").read_text(encoding="utf-8")
        )
        if int(self.manifest.get("schema_version", 0)) != 3:
            raise ValueError("artifact schema is incompatible; schema v3 is required")
        if int(self.manifest.get("eligibility_gate_version", 0)) < 3:
            raise ValueError("artifact eligibility gate version is incompatible")
        if (
            not allow_ineligible_for_testing
            and self.manifest.get("training_mode") != "candidate"
        ):
            raise ValueError("only candidate-mode artifacts can run in shadow")
        if (
            not allow_ineligible_for_testing
            and self.manifest.get("eligible_for_shadow") is not True
        ):
            raise ValueError("artifact did not pass shadow eligibility")
        eligibility_gates = self.manifest.get("eligibility_gates")
        if not isinstance(eligibility_gates, dict) or not eligibility_gates:
            raise ValueError("artifact eligibility gates are missing")
        if not allow_ineligible_for_testing and not all(
            value is True for value in eligibility_gates.values()
        ):
            raise ValueError("artifact contains a failed eligibility gate")
        declared_files = self.manifest.get("artifact_files")
        if set(declared_files or ()) != REQUIRED_ARTIFACT_FILES:
            raise ValueError("artifact file declaration is incomplete or unexpected")
        actual_files = {path.name for path in artifact_dir.iterdir() if path.is_file()}
        if actual_files != REQUIRED_ARTIFACT_FILES:
            raise ValueError("artifact directory does not match the required file set")
        checked_files = {
            line.partition("  ")[2]
            for line in (artifact_dir / "checksums.sha256")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        }
        if checked_files != REQUIRED_ARTIFACT_FILES - {"checksums.sha256"}:
            raise ValueError("artifact checksum entries do not cover every required file")
        if self.manifest["feature_version"] != FEATURE_VERSION:
            raise ValueError("artifact feature version is incompatible")
        if tuple(self.manifest["feature_columns"]) != FEATURE_COLUMNS:
            raise ValueError("artifact feature columns are incompatible")
        barrier_spec = self.manifest.get("barrier_spec", {})
        if (
            barrier_spec.get("id") != BARRIER_SPEC_ID
            or int(barrier_spec.get("horizon_bars", 0)) != BARRIER_HORIZON
        ):
            raise ValueError("artifact barrier contract is incompatible")
        executable_side = self.manifest.get("executable_side_contract", {})
        if (
            executable_side.get("id") != EXECUTABLE_SIDE_CONTRACT_ID
            or executable_side.get("chart_mode") != "BID"
            or executable_side.get("long_exit_ohlc") != "BID"
            or executable_side.get("short_exit_ohlc") != "ASK"
            or executable_side.get("source") != "HISTORICAL_BID_ASK_TICKS"
        ):
            raise ValueError("artifact executable-side contract is incompatible")
        self.quantile_models = {}
        for horizon in HORIZONS:
            model = catboost.CatBoostRegressor()
            model.load_model(artifact_dir / f"quantile_h{horizon}.cbm")
            self.quantile_models[horizon] = model
        self.direction_model = catboost.CatBoostClassifier()
        self.direction_model.load_model(artifact_dir / "direction.cbm")
        self.barrier_models = {}
        for side in ("long", "short"):
            model = catboost.CatBoostClassifier()
            model.load_model(artifact_dir / f"barrier_{side}_h3.cbm")
            self.barrier_models[side] = model
        self.excursion_models = {}
        for side in ("long", "short"):
            for excursion in ("mfe", "mae"):
                model = catboost.CatBoostRegressor()
                model.load_model(artifact_dir / f"{excursion}_{side}_h3.cbm")
                self.excursion_models[(side, excursion)] = model

    @property
    def model_id(self) -> str:
        return str(self.manifest["model_id"])

    def forecast(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        import numpy as np
        import pandas as pd

        bars = validate_snapshot(snapshot)
        raw = pd.DataFrame(
            [
                {
                    "timestamp": bar.timestamp,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "tick_volume": bar.tick_volume,
                    "spread_usd": float(snapshot["ask"]) - float(snapshot["bid"]),
                }
                for bar in bars
            ]
        )
        features = engineer_features(raw)
        row = features[list(FEATURE_COLUMNS)].iloc[[-1]]
        if not np.isfinite(row.to_numpy()).all():
            raise ValueError("latest feature row contains missing or non-finite values")

        points = []
        for horizon in HORIZONS:
            values = np.asarray(self.quantile_models[horizon].predict(row)).reshape(-1)
            correction = float(
                self.manifest["conformal_correction"].get(str(horizon), 0.0)
            )
            values[0] -= correction
            values[4] += correction
            values = postprocess_quantiles(values)
            points.append(
                {
                    "horizon_bars": horizon,
                    "q10": float(values[0]),
                    "q25": float(values[1]),
                    "q50": float(values[2]),
                    "q75": float(values[3]),
                    "q90": float(values[4]),
                }
            )

        direction_raw = float(self.direction_model.predict_proba(row)[0, 1])
        probability_calibration = self.manifest["probability_calibration"]
        direction = _calibrate_probability(
            direction_raw,
            probability_calibration["direction"],
        )
        barrier_probabilities = {}
        excursions = {}
        for side in ("long", "short"):
            raw_probabilities = self.barrier_models[side].predict_proba(row)[0]
            classes = [
                int(value)
                for value in self.barrier_models[side].classes_
            ]
            tp_index = classes.index(0)
            barrier_probabilities[side] = _calibrate_probability(
                float(raw_probabilities[tp_index]),
                probability_calibration["barrier"][side],
            )
            excursions[side] = {
                excursion: max(
                    0.0,
                    float(
                        self.excursion_models[(side, excursion)]
                        .predict(row)[0]
                    ),
                )
                for excursion in ("mfe", "mae")
            }
        drift_count = 0
        for column in FEATURE_COLUMNS:
            stats = self.manifest["feature_stats"][column]
            std = max(abs(float(stats["std"])), 1e-12)
            if abs(float(row.iloc[0][column]) - float(stats["mean"])) / std > 5:
                drift_count += 1

        calibration = self.manifest["calibration_status"]
        origin = bars[-1]
        atr_24 = float(features.iloc[-1]["atr_24"])
        barriers = barrier_prices(origin.close, atr_24)
        return {
            "prediction_id": deterministic_prediction_id(
                model_id=self.model_id,
                symbol=snapshot["symbol"],
                timeframe=snapshot["timeframe"],
                origin_bar_timestamp=origin.timestamp.isoformat().replace("+00:00", "Z"),
                feature_version=FEATURE_VERSION,
                barrier_spec_id=BARRIER_SPEC_ID,
            ),
            "model_id": self.model_id,
            "feature_version": FEATURE_VERSION,
            "origin_bar_timestamp": origin.timestamp.isoformat().replace("+00:00", "Z"),
            "origin_close": origin.close,
            "origin_bar_index": int(origin.timestamp.timestamp() // 300),
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "direction_probability_up": min(1.0, max(0.0, direction)),
            "barrier_probability_long": min(
                1.0, max(0.0, barrier_probabilities["long"])
            ),
            "barrier_probability_short": min(
                1.0, max(0.0, barrier_probabilities["short"])
            ),
            "barrier_spec_id": BARRIER_SPEC_ID,
            "barrier_horizon_bars": BARRIER_HORIZON,
            **barriers,
            "expected_mfe_long": excursions["long"]["mfe"],
            "expected_mae_long": excursions["long"]["mae"],
            "expected_mfe_short": excursions["short"]["mfe"],
            "expected_mae_short": excursions["short"]["mae"],
            "excursion_modelled": True,
            "calibration": calibration,
            "drift_detected": drift_count > max(2, len(FEATURE_COLUMNS) // 5),
            "points": points,
        }
