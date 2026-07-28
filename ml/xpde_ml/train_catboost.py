from __future__ import annotations

import argparse
import json
import math
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import HORIZONS, QUANTILES
from .dataset import (
    BARRIER_HORIZON,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    METADATA,
    build_training_frame,
)


def _calibrator_payload(model) -> dict[str, list[float]]:
    return {
        "x": [float(value) for value in model.X_thresholds_],
        "y": [float(value) for value in model.y_thresholds_],
    }


def _pinball(y_true, y_pred, alpha: float) -> float:
    import numpy as np

    errors = y_true - y_pred
    return float(np.mean(np.maximum(alpha * errors, (alpha - 1) * errors)))


def _brier(y_true, probabilities) -> float:
    import numpy as np

    return float(np.mean((probabilities - y_true) ** 2))


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
    )
    model.fit(x, y.astype(int))
    return model


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
    )
    model.fit(x, y.astype(int))
    return model


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
    )
    model.fit(x, y)
    return model


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
        from sklearn.model_selection import TimeSeriesSplit
    except ImportError as error:
        raise SystemExit("Install training extras: py -m pip install -e .[train]") from error

    model_id = f"catboost-goldm-m5-{datetime.now(UTC):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
    if args.output is None:
        args.output = Path("artifacts/catboost/runs") / model_id
    raw = pd.read_csv(args.bars_csv, parse_dates=["timestamp"]).sort_values("timestamp")
    frame = build_training_frame(raw)
    required = list(FEATURE_COLUMNS) + [
        *(f"target_{horizon}" for horizon in HORIZONS),
        "direction_3",
        "mfe_long_usd",
        "mae_long_usd",
        "mfe_short_usd",
        "mae_short_usd",
    ]
    dataset = frame.dropna(subset=required).reset_index(drop=True)
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
            )
            fold_model.fit(evaluation_x.iloc[fit_indices], fold_target.iloc[fit_indices])
            fold_prediction = np.asarray(
                fold_model.predict(evaluation_x.iloc[validation_indices])
            )
            fold_metrics[str(horizon)].append(
                {
                    "fold": float(fold),
                    "pinball_q50": _pinball(
                        fold_target.iloc[validation_indices].to_numpy(),
                        fold_prediction[:, 2],
                        0.5,
                    ),
                    "coverage_80": float(
                        np.mean(
                            (
                                fold_target.iloc[validation_indices].to_numpy()
                                >= fold_prediction[:, 0]
                            )
                            & (
                                fold_target.iloc[validation_indices].to_numpy()
                                <= fold_prediction[:, 4]
                            )
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
        )
        model.fit(x_train, target.iloc[train_slice])
        calibration_prediction = np.asarray(model.predict(x_calibration))
        calibration_truth = target.iloc[calibration_slice].to_numpy()
        nonconformity = np.maximum(
            calibration_prediction[:, 0] - calibration_truth,
            calibration_truth - calibration_prediction[:, 4],
        )
        correction = float(np.quantile(np.maximum(nonconformity, 0.0), 0.80))
        conformal[str(horizon)] = correction

        holdout_prediction = np.asarray(model.predict(x_holdout))
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
        model.save_model(args.output / f"quantile_h{horizon}.cbm")
        quantile_models[horizon] = model

    direction_model = _fit_classifier(
        CatBoostClassifier,
        x_train,
        dataset["direction_3"].iloc[train_slice],
        args.iterations,
    )
    direction_calibration_raw = direction_model.predict_proba(x_calibration)[:, 1]
    direction_calibrator = IsotonicRegression(out_of_bounds="clip").fit(
        direction_calibration_raw,
        dataset["direction_3"].iloc[calibration_slice],
    )
    direction_holdout_raw = direction_model.predict_proba(x_holdout)[:, 1]
    direction_holdout = direction_calibrator.predict(direction_holdout_raw)
    direction_truth = dataset["direction_3"].iloc[holdout_slice].to_numpy()
    direction_metrics = {
        "brier": _brier(direction_truth, direction_holdout),
        "accuracy": float(np.mean((direction_holdout >= 0.5) == direction_truth)),
        "baseline_brier": _brier(
            direction_truth,
            np.repeat(float(dataset["direction_3"].iloc[train_slice].mean()), len(direction_truth)),
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
        calibrator = IsotonicRegression(out_of_bounds="clip").fit(
            raw_calibration,
            calibration_tp_truth,
        )
        raw_holdout = side_model.predict_proba(
            side_holdout[list(FEATURE_COLUMNS)]
        )[:, tp_index]
        calibrated_holdout = calibrator.predict(raw_holdout)
        holdout_tp_truth = (side_holdout[label].to_numpy() == 0).astype(int)
        holdout_outcomes = dataset.iloc[holdout_slice][outcome_label]
        outcome_counts = holdout_outcomes.value_counts()
        labelled_count = max(1, int(holdout_outcomes.notna().sum()))
        barrier_metrics[side] = {
            "tp_first_brier": _brier(holdout_tp_truth, calibrated_holdout),
            "resolved_sample_size": float(len(side_holdout)),
            "labelled_sample_size": float(labelled_count),
            "tp_first": float(outcome_counts.get("TP_FIRST", 0)),
            "sl_first": float(outcome_counts.get("SL_FIRST", 0)),
            "no_hit_before_expiry": float(
                outcome_counts.get("NO_HIT_BEFORE_EXPIRY", 0)
            ),
            "ambiguous_same_bar": float(
                outcome_counts.get("AMBIGUOUS_SAME_BAR", 0)
            ),
            "no_hit_rate": float(
                outcome_counts.get("NO_HIT_BEFORE_EXPIRY", 0) / labelled_count
            ),
            "ambiguity_rate": float(
                outcome_counts.get("AMBIGUOUS_SAME_BAR", 0) / labelled_count
            ),
        }
        barrier_calibration[side] = _calibrator_payload(calibrator)
        side_model.save_model(args.output / f"barrier_{side}_h3.cbm")
        barrier_models[side] = side_model

    excursion_metrics: dict[str, dict[str, float]] = {}
    for side in ("long", "short"):
        excursion_metrics[side] = {}
        for excursion, alpha in (("mfe", 0.50), ("mae", 0.90)):
            label = f"{excursion}_{side}_usd"
            excursion_model = _fit_excursion_model(
                CatBoostRegressor,
                x_train,
                dataset[label].iloc[train_slice],
                alpha=alpha,
                iterations=args.iterations,
            )
            holdout_prediction = excursion_model.predict(x_holdout)
            excursion_metrics[side][f"{excursion}_q{int(alpha * 100)}_pinball"] = (
                _pinball(
                    dataset[label].iloc[holdout_slice].to_numpy(),
                    holdout_prediction,
                    alpha,
                )
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
    eligible = (
        pinball_improvement >= 0.01
        and 0.74 <= coverage_h3 <= 0.86
        and direction_metrics["brier"] <= direction_metrics["baseline_brier"]
    )
    train_frame = dataset.iloc[train_slice]
    manifest = {
        "schema_version": 2,
        "model_id": model_id,
        "status": "candidate",
        "eligible_for_shadow": eligible,
        "feature_version": FEATURE_VERSION,
        "feature_columns": list(FEATURE_COLUMNS),
        "horizons": list(HORIZONS),
        "barrier_horizon": BARRIER_HORIZON,
        "quantiles": list(QUANTILES),
        "purge_gap": METADATA.purge_gap,
        "created_at": datetime.now(UTC).isoformat(),
        "source": str(args.bars_csv.resolve()),
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
            "direction": _calibrator_payload(direction_calibrator),
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
        },
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
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
