from __future__ import annotations

import hashlib

import pytest

from xpde_ml.model_inference import (
    _calibrate_probability,
    candidate_registration_payload,
    verify_artifact_checksums,
)


def test_artifact_checksum_verification_detects_tampering(tmp_path) -> None:
    artifact = tmp_path / "manifest.json"
    artifact.write_text('{"schema_version": 2}\n', encoding="utf-8")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    (tmp_path / "checksums.sha256").write_text(
        f"{digest}  manifest.json\n",
        encoding="utf-8",
    )

    verify_artifact_checksums(tmp_path)
    artifact.write_text('{"schema_version": 3}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="checksum mismatch"):
        verify_artifact_checksums(tmp_path)


def test_artifact_checksum_rejects_path_traversal(tmp_path) -> None:
    (tmp_path / "checksums.sha256").write_text(
        f"{'0' * 64}  ../secret\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="invalid entry"):
        verify_artifact_checksums(tmp_path)


def test_probability_calibration_supports_v2_and_v3_payloads() -> None:
    assert _calibrate_probability(
        0.5,
        {"x": [0.0, 1.0], "y": [0.2, 0.8]},
    ) == pytest.approx(0.5)
    assert _calibrate_probability(
        0.5,
        {"method": "platt", "coefficient": 2.0, "intercept": -1.0},
    ) == pytest.approx(0.5)
    assert _calibrate_probability(
        0.1,
        {"method": "constant", "value": 0.37},
    ) == pytest.approx(0.37)


def test_registration_payload_carries_executable_contract(tmp_path) -> None:
    manifest = {
        "model_id": "candidate-v3",
        "feature_version": "goldm-m5-v3",
        "schema_version": 3,
        "eligibility_gate_version": 3,
        "training_mode": "candidate",
        "eligible_for_shadow": True,
        "barrier_spec": {"id": "atr-1.25tp-1.00sl-h3-executable-v2"},
        "executable_side_contract": {
            "id": "bid-entry-exit-long-ask-exit-short-v1"
        },
        "eligibility_gates": {"dataset_integrity": True},
        "metrics": {"holdout": "verified"},
    }

    payload = candidate_registration_payload(manifest, tmp_path)

    assert payload["schema_version"] == 3
    assert payload["eligibility_gate_version"] == 3
    assert (
        payload["executable_side_contract_id"]
        == "bid-entry-exit-long-ask-exit-short-v1"
    )
