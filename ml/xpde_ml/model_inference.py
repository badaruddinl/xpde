from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import HORIZONS, validate_snapshot
from .dataset import FEATURE_COLUMNS, FEATURE_VERSION, engineer_features


def _calibrate_probability(value: float, calibration: dict[str, list[float]]) -> float:
    import numpy as np

    return float(np.interp(value, calibration["x"], calibration["y"]))


class CandidateModel:
    def __init__(self, artifact_dir: Path):
        try:
            import catboost
        except ImportError as error:
            raise RuntimeError("CatBoost is required for candidate inference") from error

        self.artifact_dir = artifact_dir
        self.manifest = json.loads(
            (artifact_dir / "manifest.json").read_text(encoding="utf-8")
        )
        if self.manifest["feature_version"] != FEATURE_VERSION:
            raise ValueError("artifact feature version is incompatible")
        if tuple(self.manifest["feature_columns"]) != FEATURE_COLUMNS:
            raise ValueError("artifact feature columns are incompatible")
        self.quantile_models = {}
        for horizon in HORIZONS:
            model = catboost.CatBoostRegressor()
            model.load_model(artifact_dir / f"quantile_h{horizon}.cbm")
            self.quantile_models[horizon] = model
        self.direction_model = catboost.CatBoostClassifier()
        self.direction_model.load_model(artifact_dir / "direction.cbm")
        self.barrier_models = {}
        for side in ("up", "down"):
            model = catboost.CatBoostClassifier()
            model.load_model(artifact_dir / f"barrier_{side}.cbm")
            self.barrier_models[side] = model

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
            values = np.maximum.accumulate(values)
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
        side = "up" if direction >= 0.5 else "down"
        barrier_raw = float(self.barrier_models[side].predict_proba(row)[0, 1])
        barrier = _calibrate_probability(
            barrier_raw,
            probability_calibration["barrier"][side],
        )
        excursion = self.manifest["excursion_usd"][side]
        drift_count = 0
        for column in FEATURE_COLUMNS:
            stats = self.manifest["feature_stats"][column]
            std = max(abs(float(stats["std"])), 1e-12)
            if abs(float(row.iloc[0][column]) - float(stats["mean"])) / std > 5:
                drift_count += 1

        calibration = self.manifest["calibration_status"]
        return {
            "prediction_id": str(uuid.uuid4()),
            "model_id": self.model_id,
            "feature_version": FEATURE_VERSION,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "direction_probability_up": min(1.0, max(0.0, direction)),
            "barrier_probability": min(1.0, max(0.0, barrier)),
            "expected_mfe_usd": float(excursion["mfe"]),
            "expected_mae_usd": float(excursion["mae"]),
            "calibration": calibration,
            "drift_detected": drift_count > max(2, len(FEATURE_COLUMNS) // 5),
            "points": points,
        }
