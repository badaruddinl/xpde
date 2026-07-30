from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import subprocess
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import FLAT_RETURN_LOG_EPSILON, HORIZONS, QUANTILES
from .dataset import (
    BARRIER_HORIZON,
    BARRIER_SL_ATR_MULTIPLIER,
    BARRIER_SPEC_ID,
    BARRIER_TP_ATR_MULTIPLIER,
    EXECUTABLE_SIDE_CONTRACT_ID,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    LABEL_CONTRACT_ID,
    METADATA,
    build_training_frame,
    postprocess_quantiles,
)
from .dataset_files import dataset_manifest_path


def _training_environment() -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    try:
        git_commit = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        git_dirty = bool(
            subprocess.check_output(
                ["git", "-C", str(repo_root), "status", "--porcelain"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        )
    except (OSError, subprocess.CalledProcessError):
        git_commit = None
        git_dirty = None
    packages = {}
    for distribution in ("catboost", "numpy", "pandas", "scikit-learn"):
        try:
            packages[distribution.replace("-", "_")] = importlib.metadata.version(
                distribution
            )
        except importlib.metadata.PackageNotFoundError:
            packages[distribution.replace("-", "_")] = None
    runtime = os.getenv("XPDE_TRAINING_RUNTIME")
    if not runtime:
        runtime = (
            "google-colab-cpu"
            if os.getenv("COLAB_RELEASE_TAG")
            else f"local-{platform.system().lower()}"
        )
    return {
        "training_git_commit": git_commit,
        "training_git_dirty": git_dirty,
        "python_version": platform.python_version(),
        **packages,
        "training_runtime": runtime,
    }


def _apply_probability_calibrator(values, payload):
    import numpy as np

    array = np.asarray(values, dtype=float)
    method = payload["method"]
    if method == "isotonic":
        return np.interp(array, payload["x"], payload["y"])
    if method == "platt":
        logits = array * float(payload["coefficient"]) + float(payload["intercept"])
        return 1.0 / (1.0 + np.exp(-logits))
    if method == "constant":
        return np.repeat(float(payload["value"]), len(array))
    raise ValueError(f"unsupported probability calibrator: {method}")


def _select_probability_calibrator(
    raw_probabilities,
    truth,
    *,
    IsotonicRegression,
    LogisticRegression,
):
    import numpy as np

    raw = np.asarray(raw_probabilities, dtype=float)
    labels = np.asarray(truth, dtype=int)
    if len(raw) != len(labels) or len(raw) < 20:
        raise ValueError("probability calibration requires at least 20 aligned samples")
    if len(np.unique(labels)) < 2:
        return {
            "method": "constant",
            "value": float(labels.mean()),
            "selection_brier": 0.0,
            "selection_samples": len(labels),
        }

    split = max(10, int(len(raw) * 0.70))
    split = min(split, len(raw) - 10)
    fit_raw, select_raw = raw[:split], raw[split:]
    fit_truth, select_truth = labels[:split], labels[split:]
    candidates: list[tuple[str, float]] = []

    platt = LogisticRegression(random_state=42)
    platt.fit(fit_raw.reshape(-1, 1), fit_truth)
    platt_probability = platt.predict_proba(select_raw.reshape(-1, 1))[:, 1]
    candidates.append(("platt", _brier(select_truth, platt_probability)))

    if len(fit_raw) >= 500:
        isotonic = IsotonicRegression(out_of_bounds="clip")
        isotonic.fit(fit_raw, fit_truth)
        candidates.append(
            ("isotonic", _brier(select_truth, isotonic.predict(select_raw)))
        )

    selected_method, selection_brier = min(candidates, key=lambda item: item[1])
    if selected_method == "isotonic":
        selected = IsotonicRegression(out_of_bounds="clip").fit(raw, labels)
        payload = {
            "method": "isotonic",
            "x": [float(value) for value in selected.X_thresholds_],
            "y": [float(value) for value in selected.y_thresholds_],
        }
    else:
        selected = LogisticRegression(random_state=42).fit(
            raw.reshape(-1, 1), labels
        )
        payload = {
            "method": "platt",
            "coefficient": float(selected.coef_[0, 0]),
            "intercept": float(selected.intercept_[0]),
        }
    payload["selection_brier"] = float(selection_brier)
    payload["selection_samples"] = len(select_truth)
    return payload


def _pinball(y_true, y_pred, alpha: float) -> float:
    import numpy as np

    errors = y_true - y_pred
    return float(np.mean(np.maximum(alpha * errors, (alpha - 1) * errors)))


def _pinball_values(y_true, y_pred, alpha: float):
    import numpy as np

    errors = y_true - y_pred
    return np.maximum(alpha * errors, (alpha - 1) * errors)


def _brier(y_true, probabilities) -> float:
    import numpy as np

    return float(np.mean((probabilities - y_true) ** 2))


def _flat_return_diagnostics(
    actual_returns,
    epsilon: float = FLAT_RETURN_LOG_EPSILON,
) -> dict[str, float | int]:
    if not math.isfinite(epsilon) or epsilon < 0.0:
        raise ValueError("flat-return epsilon must be finite and non-negative")
    values = [float(value) for value in actual_returns]
    if any(not math.isfinite(value) for value in values):
        raise ValueError("flat-return diagnostics require finite returns")
    flat_samples = sum(abs(value) <= epsilon for value in values)
    denominator = len(values)
    return {
        "flat_return_log_epsilon": epsilon,
        "flat_return_samples_h3": flat_samples,
        "flat_return_denominator_h3": denominator,
        "flat_return_rate_h3": (
            flat_samples / denominator if denominator > 0 else 0.0
        ),
    }


def _finite_sample_conformal_quantile(scores, coverage: float = 0.80) -> float:
    import numpy as np

    values = np.asarray(scores, dtype=float)
    if values.size == 0:
        raise ValueError("conformal calibration requires at least one score")
    quantile_level = min(1.0, np.ceil((values.size + 1) * coverage) / values.size)
    return float(np.quantile(values, quantile_level, method="higher"))


def _block_bootstrap_improvement_lcb(
    loss_improvements,
    *,
    confidence: float = 0.95,
    samples: int = 500,
) -> float:
    import numpy as np

    values = np.asarray(loss_improvements, dtype=float)
    if values.size < 20:
        return float("-inf")
    block_length = max(2, int(np.sqrt(values.size)))
    block_starts = np.arange(0, values.size - block_length + 1)
    rng = np.random.default_rng(42)
    means = []
    block_count = int(np.ceil(values.size / block_length))
    for _ in range(samples):
        starts = rng.choice(block_starts, size=block_count, replace=True)
        sample = np.concatenate(
            [values[start : start + block_length] for start in starts]
        )[: values.size]
        means.append(float(np.mean(sample)))
    return float(np.quantile(means, 1.0 - confidence, method="higher"))


def _validate_barrier_class_coverage(
    frame,
    label: str,
    *,
    side: str,
    partition: str,
    minimum_per_class: int,
) -> None:
    counts = frame[label].astype(int).value_counts().to_dict()
    missing = {
        class_id: int(counts.get(class_id, 0))
        for class_id in (0, 1, 2)
        if int(counts.get(class_id, 0)) < minimum_per_class
    }
    if missing:
        raise ValueError(
            f"{side} barrier {partition} class coverage is insufficient; "
            f"required={minimum_per_class} per class, observed={missing}"
        )


def _fit_with_temporal_early_stopping(model, x, y):
    split = int(len(x) * 0.90)
    if split < 100 or len(x) - split < 50:
        model.fit(x, y)
        return model
    model.fit(
        x.iloc[:split],
        y.iloc[:split],
        eval_set=(x.iloc[split:], y.iloc[split:]),
        use_best_model=True,
    )
    return model


def _fit_classifier(CatBoostClassifier, x, y, iterations: int):
    model = CatBoostClassifier(
        loss_function="Logloss",
        eval_metric="BrierScore",
        iterations=iterations,
        depth=6,
        learning_rate=0.04,
        random_seed=42,
        verbose=False,
        allow_writing_files=False,
        od_type="Iter",
        od_wait=max(10, min(50, iterations // 5)),
    )
    return _fit_with_temporal_early_stopping(model, x, y.astype(int))


def _fit_barrier_classifier(CatBoostClassifier, x, y, iterations: int):
    model = CatBoostClassifier(
        loss_function="MultiClass",
        eval_metric="MultiClass",
        iterations=iterations,
        depth=6,
        learning_rate=0.04,
        random_seed=42,
        verbose=False,
        allow_writing_files=False,
        od_type="Iter",
        od_wait=max(10, min(50, iterations // 5)),
    )
    return _fit_with_temporal_early_stopping(model, x, y.astype(int))


def _fit_excursion_model(
    CatBoostRegressor,
    x,
    y,
    *,
    alpha: float,
    iterations: int,
):
    model = CatBoostRegressor(
        loss_function=f"Quantile:alpha={alpha}",
        iterations=iterations,
        depth=6,
        learning_rate=0.04,
        random_seed=42,
        verbose=False,
        allow_writing_files=False,
        od_type="Iter",
        od_wait=max(10, min(50, iterations // 5)),
    )
    return _fit_with_temporal_early_stopping(model, x, y)


def _temporal_partitions(size: int, purge_gap: int):
    calibration_start = int(size * 0.70)
    holdout_start = int(size * 0.85)
    if calibration_start < 500 or holdout_start - calibration_start < 200:
        raise ValueError("at least 1,000 fully labelled bars are required")
    train = slice(0, calibration_start - purge_gap)
    calibration = slice(calibration_start, holdout_start - purge_gap)
    holdout = slice(holdout_start, size)
    return train, calibration, holdout


def _register(url: str, manifest: dict[str, Any], artifact_path: Path) -> None:
    payload = {
        "model_id": manifest["model_id"],
        "model_type": "catboost_multi_quantile",
        "status": "candidate",
        "feature_version": manifest["feature_version"],
        "label_contract_id": manifest["label_contract_id"],
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
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in (200, 201, 202):
            raise RuntimeError(f"model registration failed with HTTP {response.status}")


def train(args) -> dict[str, Any]:
    try:
        import numpy as np
        import pandas as pd
        from catboost import CatBoostClassifier, CatBoostRegressor
        from sklearn.isotonic import IsotonicRegression
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import TimeSeriesSplit
    except ImportError as error:
        raise SystemExit("Install training extras: py -m pip install -e .[train]") from error

    model_id = f"catboost-goldm-m5-{datetime.now(UTC):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
    if args.output is None:
        args.output = Path("artifacts/catboost/runs") / model_id
    source_sha256 = hashlib.sha256(args.bars_csv.read_bytes()).hexdigest()
    source_manifest_path = dataset_manifest_path(args.bars_csv)
    source_manifest = None
    if source_manifest_path.is_file():
        source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
        if source_manifest.get("sha256") != source_sha256:
            raise ValueError("dataset SHA-256 does not match its manifest")
    raw = pd.read_csv(args.bars_csv, parse_dates=["timestamp"]).sort_values("timestamp")
    tick_sizes = sorted(
        {
            float(value)
            for value in raw.get("tick_size", pd.Series(dtype=float)).dropna()
            if math.isfinite(float(value)) and float(value) > 0.0
        }
    )
    if source_manifest is not None:
        if source_manifest.get("symbol") != "GOLDm#":
            raise ValueError("dataset manifest symbol is incompatible")
        if source_manifest.get("timeframe") != "M5":
            raise ValueError("dataset manifest timeframe is incompatible")
        if int(source_manifest.get("row_count", -1)) != len(raw):
            raise ValueError("dataset row count does not match its manifest")
    executable_integrity = (
        source_manifest.get("executable_integrity", {})
        if source_manifest is not None
        else {}
    )
    dataset_integrity_ok = bool(
        source_manifest is not None
        and int(source_manifest.get("schema_version", 0)) >= 3
        and source_manifest.get("label_contract_id") == LABEL_CONTRACT_ID
        and source_manifest.get("contains_incomplete_bar") is False
        and source_manifest.get("git_dirty") is False
        and source_manifest.get("chart_mode") == "BID"
        and source_manifest.get("executable_side_source")
        == "HISTORICAL_BID_ASK_TICKS"
        and source_manifest.get("tick_collection_mode") == "COPY_TICKS_ALL"
        and float(executable_integrity.get("parity_mismatch_rate", 1.0)) == 0.0
        and int(executable_integrity.get("bars_without_full_tick_history", 1)) == 0
        and float(executable_integrity.get("minimum_tick_coverage_per_bar", 0.0))
        >= 0.95
        and float(executable_integrity.get("tick_path_valid_rate", 0.0)) == 1.0
        and len(tick_sizes) == 1
        and math.isclose(
            float(executable_integrity.get("tick_size", 0.0)),
            tick_sizes[0],
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    )
    training_mode = getattr(args, "training_mode", "candidate")
    if training_mode == "candidate" and not dataset_integrity_ok:
        raise ValueError(
            "candidate training requires a clean dataset manifest with BID chart "
            "mode, COPY_TICKS_ALL path integrity, chart/tick Bid parity, sufficient "
            "tick coverage, and exact HISTORICAL_BID_ASK_TICKS executable sides"
        )
    frame = build_training_frame(raw)
    label_integrity = {
        "contract_id": LABEL_CONTRACT_ID,
        "invalid_forward_labels": {
            str(horizon): int(frame[f"target_{horizon}"].isna().sum())
            for horizon in HORIZONS
        },
        "invalid_h3_barrier_labels": int(frame["barrier_long_class"].isna().sum()),
        "invalid_h3_excursion_labels": int(
            frame["mfe_long_price_distance"].isna().sum()
        ),
    }
    required = list(FEATURE_COLUMNS) + [
        *(f"target_{horizon}" for horizon in HORIZONS),
        "direction_3",
        "mfe_long_price_distance",
        "mae_long_price_distance",
        "mfe_short_price_distance",
        "mae_short_price_distance",
    ]
    dataset = frame.dropna(subset=required).reset_index(drop=True)
    minimum_candidate_rows = int(getattr(args, "minimum_candidate_rows", 20_000))
    candidate_sample_ok = len(dataset) >= minimum_candidate_rows
    if training_mode == "candidate" and not candidate_sample_ok:
        raise ValueError(
            f"candidate training requires at least {minimum_candidate_rows:,} "
            f"fully labelled bars; got {len(dataset):,}. Use --training-mode smoke "
            "only for pipeline verification."
        )
    train_slice, calibration_slice, holdout_slice = _temporal_partitions(
        len(dataset),
        METADATA.purge_gap,
    )
    x = dataset[list(FEATURE_COLUMNS)]
    x_train = x.iloc[train_slice]
    x_calibration = x.iloc[calibration_slice]
    x_holdout = x.iloc[holdout_slice]
    args.output.mkdir(parents=True, exist_ok=True)

    splitter = TimeSeriesSplit(n_splits=args.folds, gap=METADATA.purge_gap)
    fold_metrics: dict[str, list[dict[str, float]]] = {
        str(horizon): [] for horizon in HORIZONS
    }
    quantile_models: dict[int, Any] = {}
    conformal: dict[str, float] = {}
    holdout_metrics: dict[str, dict[str, float]] = {}
    baseline_metrics: dict[str, dict[str, float]] = {}
    h3_loss_improvements = None
    h3_baseline_interval_width = 0.0

    evaluation_frame = dataset.iloc[: calibration_slice.stop].reset_index(drop=True)
    evaluation_x = evaluation_frame[list(FEATURE_COLUMNS)]
    for horizon in HORIZONS:
        target = dataset[f"target_{horizon}"]
        fold_target = evaluation_frame[f"target_{horizon}"]
        for fold, (fit_indices, validation_indices) in enumerate(splitter.split(evaluation_x), 1):
            fold_model = CatBoostRegressor(
                loss_function="MultiQuantile:alpha=0.1,0.25,0.5,0.75,0.9",
                iterations=args.iterations,
                depth=6,
                learning_rate=0.04,
                random_seed=42 + fold,
                verbose=False,
                allow_writing_files=False,
                od_type="Iter",
                od_wait=max(10, min(50, args.iterations // 5)),
            )
            _fit_with_temporal_early_stopping(
                fold_model,
                evaluation_x.iloc[fit_indices],
                fold_target.iloc[fit_indices],
            )
            fold_prediction = np.asarray(
                fold_model.predict(evaluation_x.iloc[validation_indices])
            )
            fold_crossing_rate = float(
                np.mean(np.any(np.diff(fold_prediction, axis=1) < 0, axis=1))
            )
            fold_prediction = postprocess_quantiles(fold_prediction)
            fold_truth = fold_target.iloc[validation_indices].to_numpy()
            fold_baseline = float(np.quantile(fold_target.iloc[fit_indices], 0.5))
            fold_pinball = _pinball(fold_truth, fold_prediction[:, 2], 0.5)
            fold_metrics[str(horizon)].append(
                {
                    "fold": float(fold),
                    "pinball_q50": fold_pinball,
                    "baseline_pinball_q50": _pinball(
                        fold_truth,
                        np.repeat(fold_baseline, len(fold_truth)),
                        0.5,
                    ),
                    "quantile_crossing_rate": fold_crossing_rate,
                    "coverage_80": float(
                        np.mean(
                            (
                                fold_truth >= fold_prediction[:, 0]
                            )
                            & (fold_truth <= fold_prediction[:, 4])
                        )
                    ),
                }
            )

        model = CatBoostRegressor(
            loss_function="MultiQuantile:alpha=0.1,0.25,0.5,0.75,0.9",
            iterations=args.iterations,
            depth=6,
            learning_rate=0.04,
            random_seed=42,
            verbose=False,
            allow_writing_files=False,
            od_type="Iter",
            od_wait=max(10, min(50, args.iterations // 5)),
        )
        _fit_with_temporal_early_stopping(model, x_train, target.iloc[train_slice])
        calibration_prediction = postprocess_quantiles(
            np.asarray(model.predict(x_calibration))
        )
        calibration_truth = target.iloc[calibration_slice].to_numpy()
        nonconformity = np.maximum(
            calibration_prediction[:, 0] - calibration_truth,
            calibration_truth - calibration_prediction[:, 4],
        )
        correction = _finite_sample_conformal_quantile(
            np.maximum(nonconformity, 0.0),
            coverage=0.80,
        )
        conformal[str(horizon)] = correction

        holdout_prediction_raw = np.asarray(model.predict(x_holdout))
        crossing_rate = float(
            np.mean(np.any(np.diff(holdout_prediction_raw, axis=1) < 0, axis=1))
        )
        holdout_prediction = postprocess_quantiles(holdout_prediction_raw)
        holdout_prediction[:, 0] -= correction
        holdout_prediction[:, 4] += correction
        holdout_truth = target.iloc[holdout_slice].to_numpy()
        metrics = {
            f"pinball_q{int(alpha * 100)}": _pinball(
                holdout_truth,
                holdout_prediction[:, index],
                alpha,
            )
            for index, alpha in enumerate(QUANTILES)
        }
        metrics["coverage_80"] = float(
            np.mean(
                (holdout_truth >= holdout_prediction[:, 0])
                & (holdout_truth <= holdout_prediction[:, 4])
            )
        )
        metrics["interval_width"] = float(
            np.mean(holdout_prediction[:, 4] - holdout_prediction[:, 0])
        )
        metrics["quantile_crossing_rate_before_postprocess"] = crossing_rate
        holdout_metrics[str(horizon)] = metrics

        train_quantiles = np.quantile(target.iloc[train_slice], QUANTILES)
        baseline_metrics[str(horizon)] = {
            f"pinball_q{int(alpha * 100)}": _pinball(
                holdout_truth,
                np.repeat(train_quantiles[index], len(holdout_truth)),
                alpha,
            )
            for index, alpha in enumerate(QUANTILES)
        }
        baseline_metrics[str(horizon)]["interval_width"] = float(
            train_quantiles[4] - train_quantiles[0]
        )
        if horizon == BARRIER_HORIZON:
            per_quantile_improvements = []
            for index, alpha in enumerate(QUANTILES):
                baseline_prediction = np.repeat(
                    train_quantiles[index], len(holdout_truth)
                )
                per_quantile_improvements.append(
                    _pinball_values(holdout_truth, baseline_prediction, alpha)
                    - _pinball_values(
                        holdout_truth,
                        holdout_prediction[:, index],
                        alpha,
                    )
                )
            h3_loss_improvements = np.mean(
                np.vstack(per_quantile_improvements),
                axis=0,
            )
            h3_baseline_interval_width = float(train_quantiles[4] - train_quantiles[0])
        model.save_model(args.output / f"quantile_h{horizon}.cbm")
        quantile_models[horizon] = model

    direction_model = _fit_classifier(
        CatBoostClassifier,
        x_train,
        dataset["direction_3"].iloc[train_slice],
        args.iterations,
    )
    direction_calibration_raw = direction_model.predict_proba(x_calibration)[:, 1]
    direction_calibration = _select_probability_calibrator(
        direction_calibration_raw,
        dataset["direction_3"].iloc[calibration_slice],
        IsotonicRegression=IsotonicRegression,
        LogisticRegression=LogisticRegression,
    )
    direction_holdout_raw = direction_model.predict_proba(x_holdout)[:, 1]
    direction_holdout = _apply_probability_calibrator(
        direction_holdout_raw, direction_calibration
    )
    direction_truth = dataset["direction_3"].iloc[holdout_slice].to_numpy()
    direction_metrics = {
        "brier": _brier(direction_truth, direction_holdout),
        "accuracy": float(np.mean((direction_holdout >= 0.5) == direction_truth)),
        "baseline_brier": _brier(
            direction_truth,
            np.repeat(float(dataset["direction_3"].iloc[train_slice].mean()), len(direction_truth)),
        ),
        **_flat_return_diagnostics(
            dataset["target_3"].iloc[holdout_slice].to_numpy()
        ),
    }
    direction_model.save_model(args.output / "direction.cbm")

    barrier_models: dict[str, Any] = {}
    barrier_calibration: dict[str, dict[str, list[float]]] = {}
    barrier_metrics: dict[str, dict[str, float]] = {}
    for side in ("long", "short"):
        label = f"barrier_{side}_class"
        outcome_label = f"barrier_{side}_outcome"
        side_train = dataset.iloc[train_slice].dropna(subset=[label])
        side_calibration = dataset.iloc[calibration_slice].dropna(subset=[label])
        side_holdout = dataset.iloc[holdout_slice].dropna(subset=[label])
        if min(len(side_train), len(side_calibration), len(side_holdout)) < 50:
            raise ValueError(f"not enough resolved {side} barrier labels")
        minimum_class_samples = 50 if training_mode == "candidate" else 2
        for partition, frame in (
            ("train", side_train),
            ("calibration", side_calibration),
            ("holdout", side_holdout),
        ):
            _validate_barrier_class_coverage(
                frame,
                label,
                side=side,
                partition=partition,
                minimum_per_class=minimum_class_samples,
            )
        side_model = _fit_barrier_classifier(
            CatBoostClassifier,
            side_train[list(FEATURE_COLUMNS)],
            side_train[label],
            args.iterations,
        )
        classes = [int(value) for value in side_model.classes_]
        tp_index = classes.index(0)
        raw_calibration = side_model.predict_proba(
            side_calibration[list(FEATURE_COLUMNS)]
        )[:, tp_index]
        calibration_tp_truth = (side_calibration[label].to_numpy() == 0).astype(int)
        calibrator_payload = _select_probability_calibrator(
            raw_calibration,
            calibration_tp_truth,
            IsotonicRegression=IsotonicRegression,
            LogisticRegression=LogisticRegression,
        )
        raw_holdout = side_model.predict_proba(
            side_holdout[list(FEATURE_COLUMNS)]
        )[:, tp_index]
        calibrated_holdout = _apply_probability_calibrator(
            raw_holdout, calibrator_payload
        )
        holdout_tp_truth = (side_holdout[label].to_numpy() == 0).astype(int)
        train_tp_rate = float((side_train[label].to_numpy() == 0).mean())
        holdout_outcomes = dataset.iloc[holdout_slice][outcome_label]
        outcome_counts = holdout_outcomes.value_counts()
        labelled_count = max(1, int(holdout_outcomes.notna().sum()))
        barrier_metrics[side] = {
            "tp_first_brier": _brier(holdout_tp_truth, calibrated_holdout),
            "baseline_tp_first_brier": _brier(
                holdout_tp_truth,
                np.repeat(train_tp_rate, len(holdout_tp_truth)),
            ),
            "resolved_sample_size": float(len(side_holdout)),
            "labelled_sample_size": float(labelled_count),
            "tp_first": float(outcome_counts.get("TP_FIRST", 0)),
            "sl_first": float(outcome_counts.get("SL_FIRST", 0)),
            "no_hit_before_expiry": float(
                outcome_counts.get("NO_HIT_BEFORE_EXPIRY", 0)
            ),
            "ambiguous_same_bar": float(
        outcome_counts.get("AMBIGUOUS_SAME_BAR", 0)
        + outcome_counts.get("AMBIGUOUS_SAME_TIMESTAMP", 0)
            ),
            "no_hit_rate": float(
                outcome_counts.get("NO_HIT_BEFORE_EXPIRY", 0) / labelled_count
            ),
            "ambiguity_rate": float(
        (
            outcome_counts.get("AMBIGUOUS_SAME_BAR", 0)
            + outcome_counts.get("AMBIGUOUS_SAME_TIMESTAMP", 0)
        )
        / labelled_count
            ),
        }
        barrier_calibration[side] = calibrator_payload
        side_model.save_model(args.output / f"barrier_{side}_h3.cbm")
        barrier_models[side] = side_model

    excursion_metrics: dict[str, dict[str, float]] = {}
    for side in ("long", "short"):
        excursion_metrics[side] = {}
        for excursion, alpha in (("mfe", 0.50), ("mae", 0.90)):
            label = f"{excursion}_{side}_price_distance"
            excursion_model = _fit_excursion_model(
                CatBoostRegressor,
                x_train,
                dataset[label].iloc[train_slice],
                alpha=alpha,
                iterations=args.iterations,
            )
            holdout_prediction = np.maximum(
                0.0,
                np.asarray(excursion_model.predict(x_holdout)),
            )
            holdout_truth = dataset[label].iloc[holdout_slice].to_numpy()
            model_pinball = _pinball(holdout_truth, holdout_prediction, alpha)
            baseline_value = float(
                np.quantile(dataset[label].iloc[train_slice], alpha)
            )
            prefix = f"{excursion}_q{int(alpha * 100)}"
            excursion_metrics[side][f"{prefix}_pinball"] = model_pinball
            excursion_metrics[side][f"{prefix}_baseline_pinball"] = _pinball(
                holdout_truth,
                np.repeat(baseline_value, len(holdout_truth)),
                alpha,
            )
            if excursion == "mae":
                excursion_metrics[side]["mae_q90_coverage"] = float(
                    np.mean(holdout_truth <= holdout_prediction)
                )
            excursion_model.save_model(
                args.output / f"{excursion}_{side}_h3.cbm"
            )

    median_pinball_model = float(
        np.mean([holdout_metrics[str(h)]["pinball_q50"] for h in HORIZONS])
    )
    median_pinball_baseline = float(
        np.mean([baseline_metrics[str(h)]["pinball_q50"] for h in HORIZONS])
    )
    coverage_h3 = holdout_metrics["3"]["coverage_80"]
    mean_pinball_model = float(
        np.mean(
            [
                holdout_metrics[str(horizon)][f"pinball_q{int(alpha * 100)}"]
                for horizon in HORIZONS
                for alpha in QUANTILES
            ]
        )
    )
    mean_pinball_baseline = float(
        np.mean(
            [
                baseline_metrics[str(horizon)][f"pinball_q{int(alpha * 100)}"]
                for horizon in HORIZONS
                for alpha in QUANTILES
            ]
        )
    )
    pinball_improvement = 1.0 - mean_pinball_model / mean_pinball_baseline
    h3_model_pinball = float(
        np.mean(
            [
                holdout_metrics[str(BARRIER_HORIZON)][
                    f"pinball_q{int(alpha * 100)}"
                ]
                for alpha in QUANTILES
            ]
        )
    )
    h3_baseline_pinball = float(
        np.mean(
            [
                baseline_metrics[str(BARRIER_HORIZON)][
                    f"pinball_q{int(alpha * 100)}"
                ]
                for alpha in QUANTILES
            ]
        )
    )
    h3_pinball_improvement = 1.0 - h3_model_pinball / h3_baseline_pinball
    h3_q50_improvement = 1.0 - (
        holdout_metrics[str(BARRIER_HORIZON)]["pinball_q50"]
        / baseline_metrics[str(BARRIER_HORIZON)]["pinball_q50"]
    )
    h3_tail_model = float(
        np.mean(
            [
                holdout_metrics[str(BARRIER_HORIZON)]["pinball_q10"],
                holdout_metrics[str(BARRIER_HORIZON)]["pinball_q90"],
            ]
        )
    )
    h3_tail_baseline = float(
        np.mean(
            [
                baseline_metrics[str(BARRIER_HORIZON)]["pinball_q10"],
                baseline_metrics[str(BARRIER_HORIZON)]["pinball_q90"],
            ]
        )
    )
    h3_tail_improvement = 1.0 - h3_tail_model / h3_tail_baseline
    if h3_loss_improvements is None:
        raise RuntimeError("h3 holdout loss improvements were not collected")
    h3_bootstrap_lcb = _block_bootstrap_improvement_lcb(h3_loss_improvements)
    h3_interval_width = holdout_metrics[str(BARRIER_HORIZON)]["interval_width"]
    h3_interval_width_ok = (
        math.isfinite(h3_interval_width)
        and h3_interval_width > 0.0
        and h3_interval_width <= 3.0 * max(h3_baseline_interval_width, 1e-12)
    )
    h3_fold_stability_ok = all(
        fold["pinball_q50"] <= fold["baseline_pinball_q50"]
        for fold in fold_metrics[str(BARRIER_HORIZON)]
    )
    barrier_beats_baseline = all(
        metrics["tp_first_brier"] < metrics["baseline_tp_first_brier"]
        for metrics in barrier_metrics.values()
    )
    excursion_beats_baseline = all(
        metrics[f"{excursion}_q{int(alpha * 100)}_pinball"]
        < metrics[f"{excursion}_q{int(alpha * 100)}_baseline_pinball"]
        for metrics in excursion_metrics.values()
        for excursion, alpha in (("mfe", 0.50), ("mae", 0.90))
    )
    mae_coverage_ok = all(
        0.87 <= metrics["mae_q90_coverage"] <= 0.93
        for metrics in excursion_metrics.values()
    )
    fold_stability_ok = all(
        fold["pinball_q50"] <= 1.25 * fold["baseline_pinball_q50"]
        for metrics in fold_metrics.values()
        for fold in metrics
    )
    ambiguity_ok = all(
        metrics["ambiguity_rate"] <= 0.05 for metrics in barrier_metrics.values()
    )
    crossing_ok = all(
        metrics["quantile_crossing_rate_before_postprocess"] <= 0.01
        for metrics in holdout_metrics.values()
    )
    calibration_sample_ok = len(x_calibration) >= 500 and len(x_holdout) >= 500
    eligibility_gates = {
        "candidate_training_mode": training_mode == "candidate",
        "minimum_candidate_rows": candidate_sample_ok,
        "dataset_integrity": dataset_integrity_ok,
        "mean_pinball_improvement": pinball_improvement >= 0.01,
        "h3_mean_pinball_improvement": h3_pinball_improvement > 0.0,
        "h3_q50_improvement": h3_q50_improvement > 0.0,
        "h3_tail_improvement": h3_tail_improvement > 0.0,
        "h3_interval_width": h3_interval_width_ok,
        "h3_worst_fold": h3_fold_stability_ok,
        "h3_block_bootstrap_lcb": h3_bootstrap_lcb > 0.0,
        "h3_coverage": 0.74 <= coverage_h3 <= 0.86,
        "direction_brier": (
            direction_metrics["brier"] <= direction_metrics["baseline_brier"]
        ),
        "barrier_brier": barrier_beats_baseline,
        "excursion_pinball": excursion_beats_baseline,
        "mae_q90_coverage": mae_coverage_ok,
        "fold_stability": fold_stability_ok,
        "ambiguity_rate": ambiguity_ok,
        "quantile_crossing": crossing_ok,
        "minimum_calibration_samples": calibration_sample_ok,
    }
    eligible = all(eligibility_gates.values())
    train_frame = dataset.iloc[train_slice]
    manifest = {
        "schema_version": 3,
        "model_id": model_id,
        "status": "candidate",
        "eligible_for_shadow": eligible,
        "eligibility_gate_version": 3,
        "eligibility_gates": eligibility_gates,
        "training_mode": training_mode,
        "minimum_candidate_rows": minimum_candidate_rows,
        "feature_version": FEATURE_VERSION,
        "label_contract_id": LABEL_CONTRACT_ID,
        "feature_columns": list(FEATURE_COLUMNS),
        "horizons": list(HORIZONS),
        "barrier_horizon": BARRIER_HORIZON,
        "barrier_spec": {
            "id": BARRIER_SPEC_ID,
            "horizon_bars": BARRIER_HORIZON,
            "take_profit_atr_multiplier": BARRIER_TP_ATR_MULTIPLIER,
            "stop_loss_atr_multiplier": BARRIER_SL_ATR_MULTIPLIER,
            "price_alignment": "BROKER_TICK_SIZE_OUTWARD",
            "tick_size": tick_sizes[0] if len(tick_sizes) == 1 else None,
        },
        "executable_side_contract": {
            "id": EXECUTABLE_SIDE_CONTRACT_ID,
            "chart_mode": "BID",
            "long_exit_ohlc": "BID",
            "short_exit_ohlc": "ASK",
            "source": "HISTORICAL_BID_ASK_TICKS",
            "first_passage": "ORDERED_PRICE_CHANGE_TICKS",
            "spread_features": "EXACT_BID_ASK_CLOSE_WINDOW",
            "minimum_tick_coverage": 0.95,
        },
        "quantiles": list(QUANTILES),
        "purge_gap": METADATA.purge_gap,
        "created_at": datetime.now(UTC).isoformat(),
        "training_environment": _training_environment(),
        "source": str(args.bars_csv.resolve()),
        "source_dataset": {
            "sha256": source_sha256,
            "manifest": (
                str(source_manifest_path.resolve())
                if source_manifest is not None
                else None
            ),
            "manifest_verified": source_manifest is not None,
            "integrity_gate_passed": dataset_integrity_ok,
            "executable_integrity": executable_integrity,
            "label_integrity": label_integrity,
            "export_git_commit": (
                source_manifest.get("git_commit")
                if source_manifest is not None
                else None
            ),
        },
        "rows": {
            "raw": len(raw),
            "labelled": len(dataset),
            "train": len(x_train),
            "calibration": len(x_calibration),
            "holdout": len(x_holdout),
        },
        "time_range": {
            "first": dataset["timestamp"].iloc[0].isoformat(),
            "last": dataset["timestamp"].iloc[-1].isoformat(),
        },
        "conformal_correction": conformal,
        "probability_calibration": {
            "direction": direction_calibration,
            "barrier": barrier_calibration,
        },
        "feature_stats": {
            column: {
                "mean": float(train_frame[column].mean()),
                "std": float(train_frame[column].std()),
            }
            for column in FEATURE_COLUMNS
        },
        "calibration_status": {
            "target_coverage": 0.80,
            "observed_coverage": coverage_h3,
            "sample_size": len(x_holdout),
        },
        "metrics": {
            "walk_forward": fold_metrics,
            "holdout": holdout_metrics,
            "baseline": baseline_metrics,
            "direction": direction_metrics,
            "barrier": barrier_metrics,
            "excursion": excursion_metrics,
            "median_pinball_model": median_pinball_model,
            "median_pinball_baseline": median_pinball_baseline,
            "mean_pinball_model": mean_pinball_model,
            "mean_pinball_baseline": mean_pinball_baseline,
            "pinball_improvement": pinball_improvement,
            "h3": {
                "mean_pinball_model": h3_model_pinball,
                "mean_pinball_baseline": h3_baseline_pinball,
                "mean_pinball_improvement": h3_pinball_improvement,
                "q50_improvement": h3_q50_improvement,
                "tail_improvement": h3_tail_improvement,
                "interval_width": h3_interval_width,
                "baseline_interval_width": h3_baseline_interval_width,
                "block_bootstrap_improvement_lcb_95": h3_bootstrap_lcb,
            },
        },
    }
    artifact_files = [
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
    ]
    manifest["artifact_files"] = artifact_files
    (args.output / "evaluation.json").write_text(
        json.dumps(
            {
                "model_id": model_id,
                "eligible_for_shadow": eligible,
                "rows": manifest["rows"],
                "time_range": manifest["time_range"],
                "metrics": manifest["metrics"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (args.output / "model_card.md").write_text(
        "\n".join(
            [
                f"# XPDE candidate {model_id}",
                "",
                "- Symbol: `GOLDm#`",
                "- Timeframe: `M5`",
                "- Mode: candidate / shadow-only",
                f"- Feature version: `{FEATURE_VERSION}`",
                f"- Dataset SHA-256: `{source_sha256}`",
                f"- Eligible for shadow: `{str(eligible).lower()}`",
                (
                    "- Holdout flat return H3: "
                    f"`{direction_metrics['flat_return_rate_h3']:.6f}` "
                    f"(`{direction_metrics['flat_return_samples_h3']}`/"
                    f"`{direction_metrics['flat_return_denominator_h3']}`, "
                    f"`|log-return| <= {FLAT_RETURN_LOG_EPSILON:g}`)"
                ),
                "",
                "This artifact cannot execute orders and requires manual promotion.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    checksum_lines = []
    for filename in sorted(
        name for name in artifact_files if name != "checksums.sha256"
    ):
        artifact = args.output / filename
        checksum_lines.append(
            f"{hashlib.sha256(artifact.read_bytes()).hexdigest()}  {filename}"
        )
    (args.output / "checksums.sha256").write_text(
        "\n".join(checksum_lines) + "\n",
        encoding="utf-8",
    )
    if args.register_url:
        _register(args.register_url, manifest, args.output)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train calibrated direct-horizon CatBoost candidate models"
    )
    parser.add_argument("bars_csv", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help="immutable artifact directory; defaults to artifacts/catboost/runs/<model-id>",
    )
    parser.add_argument("--iterations", type=int, default=250)
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument(
        "--training-mode",
        choices=("candidate", "smoke"),
        default="candidate",
        help="candidate enforces production sample gates; smoke only verifies the pipeline",
    )
    parser.add_argument("--minimum-candidate-rows", type=int, default=20_000)
    parser.add_argument(
        "--register-url",
        default="http://127.0.0.1:8787/api/v1/models/register",
    )
    parser.add_argument("--no-register", action="store_const", const="", dest="register_url")
    args = parser.parse_args()
    manifest = train(args)
    print(
        json.dumps(
            {
                "model_id": manifest["model_id"],
                "eligible_for_shadow": manifest["eligible_for_shadow"],
                "rows": manifest["rows"],
                "output": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()
