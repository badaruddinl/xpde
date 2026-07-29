from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")
sklearn_isotonic = pytest.importorskip("sklearn.isotonic")
sklearn_linear = pytest.importorskip("sklearn.linear_model")

from xpde_ml.train_catboost import (
    _apply_probability_calibrator,
    _select_probability_calibrator,
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
