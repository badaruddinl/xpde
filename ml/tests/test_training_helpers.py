from __future__ import annotations

import json
from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
pandas = pytest.importorskip("pandas")
sklearn_isotonic = pytest.importorskip("sklearn.isotonic")
sklearn_linear = pytest.importorskip("sklearn.linear_model")

from xpde_ml.train_catboost import (
    _apply_probability_calibrator,
    _block_bootstrap_improvement_lcb,
    _finite_sample_conformal_quantile,
    _flat_return_diagnostics,
    _select_probability_calibrator,
    _validate_barrier_class_coverage,
)
from xpde_ml import train_catboost


def test_small_calibration_split_uses_platt() -> None:
    raw = np.linspace(0.05, 0.95, 120)
    truth = (raw > 0.55).astype(int)
    payload = _select_probability_calibrator(
        raw,
        truth,
        IsotonicRegression=sklearn_isotonic.IsotonicRegression,
        LogisticRegression=sklearn_linear.LogisticRegression,
    )
    calibrated = _apply_probability_calibrator(raw, payload)
    assert payload["method"] == "platt"
    assert np.all((calibrated >= 0.0) & (calibrated <= 1.0))


def test_large_calibration_split_compares_supported_calibrators() -> None:
    raw = np.linspace(0.01, 0.99, 1000)
    truth = (raw + 0.08 * np.sin(raw * 20) > 0.5).astype(int)
    payload = _select_probability_calibrator(
        raw,
        truth,
        IsotonicRegression=sklearn_isotonic.IsotonicRegression,
        LogisticRegression=sklearn_linear.LogisticRegression,
    )
    assert payload["method"] in {"platt", "isotonic"}
    assert payload["selection_samples"] == 300


def test_conformal_quantile_uses_finite_sample_higher_rank() -> None:
    assert _finite_sample_conformal_quantile(np.arange(10), 0.80) == 9.0


def test_flat_return_diagnostics_publish_explicit_epsilon_and_denominator() -> None:
    metrics = _flat_return_diagnostics([0.0, 1e-13, -1e-13, 1e-6, -1e-6])
    assert metrics == {
        "flat_return_log_epsilon": 1e-12,
        "flat_return_samples_h3": 3,
        "flat_return_denominator_h3": 5,
        "flat_return_rate_h3": 0.6,
    }
    with pytest.raises(ValueError, match="epsilon"):
        _flat_return_diagnostics([0.0], epsilon=-1.0)


def test_block_bootstrap_requires_positive_improvement_confidence() -> None:
    positive = np.linspace(0.01, 0.03, 200)
    negative = -positive
    assert _block_bootstrap_improvement_lcb(positive, samples=100) > 0.0
    assert _block_bootstrap_improvement_lcb(negative, samples=100) < 0.0


def test_barrier_class_coverage_reports_missing_partition_class() -> None:
    frame = pandas.DataFrame({"label": [0] * 5 + [1] * 5 + [2]})
    with pytest.raises(ValueError, match="holdout class coverage"):
        _validate_barrier_class_coverage(
            frame,
            "label",
            side="long",
            partition="holdout",
            minimum_per_class=2,
        )


def test_manual_registration_includes_the_label_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict = {}

    class Response:
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def accept(request, *, timeout):
        assert timeout == 30
        captured.update(json.loads(request.data.decode("utf-8")))
        return Response()

    monkeypatch.setattr(train_catboost.urllib.request, "urlopen", accept)
    manifest = {
        "model_id": "candidate-test",
        "feature_version": "goldm-m5-v5",
        "label_contract_id": "exact-contiguous-m5-horizons-v1",
        "schema_version": 3,
        "eligibility_gate_version": 3,
        "training_mode": "candidate",
        "eligible_for_shadow": True,
        "barrier_spec": {"id": "barrier-v6"},
        "executable_side_contract": {"id": "executable-v5"},
        "eligibility_gates": {"gate": True},
        "metrics": {},
    }

    train_catboost._register("http://127.0.0.1/models", manifest, tmp_path)

    assert captured["label_contract_id"] == manifest["label_contract_id"]


def test_local_training_defers_registration_until_verified_import() -> None:
    script = (
        Path(__file__).resolve().parents[2] / "scripts" / "train-candidate.ps1"
    ).read_text(encoding="utf-8")

    assert "xpde_ml.train_catboost" in script
    assert "--no-register" in script
    assert script.index("--no-register") < script.index("import-colab-artifact.py")
