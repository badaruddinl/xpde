from __future__ import annotations

import hashlib
import importlib.util
import json
import math
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "import-colab-artifact.py"
SPEC = importlib.util.spec_from_file_location("xpde_import_colab_artifact", SCRIPT)
assert SPEC and SPEC.loader
importer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(importer)


def make_artifact(path: Path, *, eligible: bool = True) -> Path:
    path.mkdir()
    artifact_files = sorted(importer.REQUIRED_ARTIFACT_FILES)
    manifest = {
        "schema_version": 3,
        "model_id": "candidate-test-v3",
        "feature_version": importer.FEATURE_VERSION,
        "label_contract_id": importer.LABEL_CONTRACT_ID,
        "eligible_for_shadow": eligible,
        "eligibility_gate_version": 3,
        "training_mode": "candidate",
        "eligibility_gates": {
            "candidate_training_mode": eligible,
            "dataset_integrity": eligible,
        },
        "artifact_files": artifact_files,
        "barrier_spec": {
            "id": importer.BARRIER_SPEC_ID,
            "horizon_bars": 3,
            "price_alignment": "BROKER_TICK_SIZE_OUTWARD",
            "tick_size": 0.01,
        },
        "executable_side_contract": {
            "id": importer.EXECUTABLE_SIDE_CONTRACT_ID,
            "chart_mode": "BID",
            "long_exit_ohlc": "BID",
            "short_exit_ohlc": "ASK",
            "source": "HISTORICAL_BID_ASK_TICKS",
            "minimum_tick_coverage": 0.95,
        },
    }
    (path / "manifest.json").write_text(
        json.dumps(manifest) + "\n", encoding="utf-8"
    )
    for filename in importer.REQUIRED_ARTIFACT_FILES - {
        "manifest.json",
        "checksums.sha256",
    }:
        (path / filename).write_bytes(f"verified:{filename}".encode())
    checksums = []
    for filename in sorted(
        importer.REQUIRED_ARTIFACT_FILES - {"checksums.sha256"}
    ):
        digest = hashlib.sha256((path / filename).read_bytes()).hexdigest()
        checksums.append(f"{digest}  {filename}")
    (path / "checksums.sha256").write_text(
        "\n".join(checksums) + "\n", encoding="utf-8"
    )
    return path


class FakeCandidateModel:
    def __init__(self, path: Path):
        self.path = path

    def forecast(self, snapshot: dict) -> dict:
        origin = snapshot["bars"][-1]
        tick_size = float(snapshot["symbol_spec"]["tick_size"])

        def snap(value: float, *, upward: bool) -> float:
            ticks = value / tick_size
            snapped_ticks = math.ceil(ticks - 1e-10) if upward else math.floor(ticks + 1e-10)
            return snapped_ticks * tick_size

        return {
            "origin_close": origin["close"],
            "direction_probability_up": 0.55,
            "barrier_probability_long": 0.54,
            "barrier_probability_short": 0.46,
            "barrier_spec_id": importer.BARRIER_SPEC_ID,
            "barrier_horizon_bars": 3,
            "label_contract_id": importer.LABEL_CONTRACT_ID,
            "probability_reference": "FORECAST_ORIGIN",
            "entry_conditioned_probability": False,
            "target_price_long": snap(origin["close"] + 1.25, upward=True),
            "stop_price_long": snap(origin["close"] - 1.0, upward=False),
            "target_price_short": snap(origin["close"] - 1.25, upward=False),
            "stop_price_short": snap(origin["close"] + 1.0, upward=True),
            "points": [
                {
                    "horizon_bars": horizon,
                    "q10": -0.01,
                    "q25": -0.005,
                    "q50": 0.0,
                    "q75": 0.005,
                    "q90": 0.01,
                }
                for horizon in importer.HORIZONS
            ],
        }


class FakeIncompleteCandidateModel(FakeCandidateModel):
    def forecast(self, snapshot: dict) -> dict:
        forecast = super().forecast(snapshot)
        forecast["points"] = forecast["points"][:-1]
        return forecast


def test_imports_immutable_verified_candidate(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(importer, "CandidateModel", FakeCandidateModel)
    artifact = make_artifact(tmp_path / "source")
    target = importer.import_candidate(
        artifact,
        tmp_path / "artifacts",
        promote_latest=False,
    )
    assert target.name == "candidate-test-v3"
    assert (target / "direction.cbm").is_file()
    with pytest.raises(FileExistsError):
        importer.import_candidate(
            artifact,
            tmp_path / "artifacts",
            promote_latest=False,
        )


def test_ineligible_candidate_is_not_copied_to_latest(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(importer, "CandidateModel", FakeCandidateModel)
    artifact = make_artifact(tmp_path / "source", eligible=False)
    with pytest.raises(ValueError, match="eligibility"):
        importer.import_candidate(
            artifact,
            tmp_path / "artifacts",
            promote_latest=True,
        )
    assert not (tmp_path / "artifacts" / "runs").exists()


def test_incomplete_forecast_envelope_is_rejected_before_promotion(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(importer, "CandidateModel", FakeIncompleteCandidateModel)
    artifact = make_artifact(tmp_path / "source")

    with pytest.raises(ValueError, match="H1, H3, H6 and H12"):
        importer.import_candidate(
            artifact,
            tmp_path / "artifacts",
            promote_latest=True,
        )

    assert not (tmp_path / "artifacts" / "latest").exists()


def test_missing_required_model_is_rejected(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(importer, "CandidateModel", FakeCandidateModel)
    artifact = make_artifact(tmp_path / "source")
    (artifact / "direction.cbm").unlink()
    with pytest.raises(ValueError, match="missing"):
        importer.verify_candidate(artifact)
