from __future__ import annotations

from dataclasses import dataclass

from .contracts import HORIZONS

FEATURE_VERSION = "goldm-m5-v2"
FEATURE_COLUMNS = (
    "return_1",
    "return_3",
    "return_6",
    "return_12",
    "return_24",
    "volatility_3",
    "volatility_6",
    "volatility_12",
    "volatility_24",
    "range_pct",
    "mean_true_range_24_pct",
    "candle_body",
    "upper_wick",
    "lower_wick",
    "volume_z24",
    "spread_pct",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
)


@dataclass(frozen=True)
class DatasetMetadata:
    feature_version: str
    feature_columns: tuple[str, ...]
    horizons: tuple[int, ...]
    purge_gap: int


METADATA = DatasetMetadata(
    feature_version=FEATURE_VERSION,
    feature_columns=FEATURE_COLUMNS,
    horizons=HORIZONS,
    purge_gap=max(HORIZONS),
)


def engineer_features(frame):
    """Create causal features shared by training and live inference."""
    import numpy as np
    import pandas as pd

    result = frame.copy().sort_values("timestamp").reset_index(drop=True)
    result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
    close = result["close"].astype(float)
    previous_close = close.shift(1)
    result["return_1"] = np.log(close / previous_close)
    for lag in (3, 6, 12, 24):
        result[f"return_{lag}"] = np.log(close / close.shift(lag))
        result[f"volatility_{lag}"] = result["return_1"].rolling(lag).std()

    true_range = pd.concat(
        [
            result["high"] - result["low"],
            (result["high"] - previous_close).abs(),
            (result["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    result["atr_24"] = true_range.rolling(24).mean()
    result["range_pct"] = (result["high"] - result["low"]) / close
    result["mean_true_range_24_pct"] = result["atr_24"] / close
    result["candle_body"] = (close - result["open"]) / close
    result["upper_wick"] = (result["high"] - result[["open", "close"]].max(axis=1)) / close
    result["lower_wick"] = (result[["open", "close"]].min(axis=1) - result["low"]) / close
    volume = result["tick_volume"].astype(float)
    volume_mean = volume.rolling(24).mean()
    volume_std = volume.rolling(24).std().replace(0.0, np.nan)
    result["volume_z24"] = (volume - volume_mean) / volume_std
    result["spread_usd"] = result.get("spread_usd", 0.0)
    result["spread_pct"] = result["spread_usd"].astype(float) / close
    hour = result["timestamp"].dt.hour + result["timestamp"].dt.minute / 60.0
    weekday = result["timestamp"].dt.weekday
    result["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    result["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    result["weekday_sin"] = np.sin(2 * np.pi * weekday / 7)
    result["weekday_cos"] = np.cos(2 * np.pi * weekday / 7)
    return result


def add_objective_labels(frame, *, barrier_horizon: int = 12):
    """Add future-return, direction, excursion and TP-before-SL labels."""
    import numpy as np

    result = frame.copy()
    close = result["close"].astype(float)
    for horizon in HORIZONS:
        result[f"target_{horizon}"] = np.log(close.shift(-horizon) / close)
    result["direction_3"] = (result["target_3"] > 0).astype(float)

    barrier_up: list[float] = []
    barrier_down: list[float] = []
    mfe_up: list[float] = []
    mae_up: list[float] = []
    mfe_down: list[float] = []
    mae_down: list[float] = []
    for index, row in result.iterrows():
        future = result.iloc[index + 1 : index + 1 + barrier_horizon]
        atr = float(row.get("atr_24", np.nan))
        start = float(row["close"])
        if len(future) < barrier_horizon or not np.isfinite(atr) or atr <= 0:
            barrier_up.append(np.nan)
            barrier_down.append(np.nan)
            mfe_up.append(np.nan)
            mae_up.append(np.nan)
            mfe_down.append(np.nan)
            mae_down.append(np.nan)
            continue

        upper = start + 1.25 * atr
        lower = start - atr
        up_label = np.nan
        down_label = np.nan
        for _, future_bar in future.iterrows():
            hits_upper = float(future_bar["high"]) >= upper
            hits_lower = float(future_bar["low"]) <= lower
            if hits_upper and hits_lower:
                break
            if hits_upper:
                up_label, down_label = 1.0, 0.0
                break
            if hits_lower:
                up_label, down_label = 0.0, 1.0
                break
        barrier_up.append(up_label)
        barrier_down.append(down_label)
        future_high = float(future["high"].max())
        future_low = float(future["low"].min())
        mfe_up.append(max(0.0, future_high - start))
        mae_up.append(max(0.0, start - future_low))
        mfe_down.append(max(0.0, start - future_low))
        mae_down.append(max(0.0, future_high - start))

    result["barrier_up"] = barrier_up
    result["barrier_down"] = barrier_down
    result["mfe_up_usd"] = mfe_up
    result["mae_up_usd"] = mae_up
    result["mfe_down_usd"] = mfe_down
    result["mae_down_usd"] = mae_down
    return result


def build_training_frame(frame):
    return add_objective_labels(engineer_features(frame))
