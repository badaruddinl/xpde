from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import HORIZONS, validate_snapshot
from .dataset import FEATURE_COLUMNS, FEATURE_VERSION, engineer_features


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
        verify_artifact_checksums(artifact_dir)
        self.manifest = json.loads(
            (artifact_dir / "manifest.json").read_text(encoding="utf-8")
        )
        if int(self.manifest.get("schema_version", 0)) < 2:
            raise ValueError(
                "artifact schema is incompatible; train an XPDE schema v2 candidate"
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
        return {
            "prediction_id": str(uuid.uuid4()),
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
            "expected_mfe_long": excursions["long"]["mfe"],
            "expected_mae_long": excursions["long"]["mae"],
            "expected_mfe_short": excursions["short"]["mfe"],
            "expected_mae_short": excursions["short"]["mae"],
            "excursion_modelled": True,
            "calibration": calibration,
            "drift_detected": drift_count > max(2, len(FEATURE_COLUMNS) // 5),
            "points": points,
        }
