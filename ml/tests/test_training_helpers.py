from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")
pandas = pytest.importorskip("pandas")
sklearn_isotonic = pytest.importorskip("sklearn.isotonic")
sklearn_linear = pytest.importorskip("sklearn.linear_model")

from xpde_ml.train_catboost import (
    _apply_probability_calibrator,
    _block_bootstrap_improvement_lcb,
    _finite_sample_conformal_quantile,
    _select_probability_calibrator,
    _validate_barrier_class_coverage,
)


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
