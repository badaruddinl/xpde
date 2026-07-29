from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from itertools import groupby
import json

from .contracts import HORIZONS

FEATURE_VERSION = "goldm-m5-v5"
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
    "spread_close_pct",
    "spread_median_24_pct",
    "spread_q75_24_pct",
    "spread_q90_24_pct",
    "spread_atr_ratio",
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

BARRIER_HORIZON = 3
BARRIER_SPEC_ID = "atr-1.25tp-1.00sl-h3-executable-tick-aligned-v6"
EXECUTABLE_SIDE_CONTRACT_ID = (
    "bid-entry-exit-long-ask-exit-short-complete-tick-sequence-v5"
)
LABEL_CONTRACT_ID = "exact-contiguous-m5-horizons-v1"
MINIMUM_EXECUTABLE_TICK_COVERAGE = 0.95
BARRIER_TP_ATR_MULTIPLIER = 1.25
BARRIER_SL_ATR_MULTIPLIER = 1.0
BARRIER_CLASS = {
    "TP_FIRST": 0,
    "SL_FIRST": 1,
    "NO_HIT_BEFORE_EXPIRY": 2,
}


def snap_price_to_tick(price: float, tick_size: float, *, upward: bool) -> float:
    """Snap a price outward to an executable broker tick without float drift."""
    if not all(map(np_is_finite, (price, tick_size))) or price <= 0 or tick_size <= 0:
        raise ValueError("price and tick size must be finite positive values")
    price_decimal = Decimal(str(price))
    tick_decimal = Decimal(str(tick_size))
    rounding = ROUND_CEILING if upward else ROUND_FLOOR
    ticks = (price_decimal / tick_decimal).to_integral_value(rounding=rounding)
    return float(ticks * tick_decimal)


def barrier_prices(
    origin_close: float, atr: float, tick_size: float
) -> dict[str, float]:
    """Return the exact prices represented by the barrier classifiers."""
    if (
        not all(map(np_is_finite, (origin_close, atr, tick_size)))
        or origin_close <= 0
        or atr <= 0
        or tick_size <= 0
    ):
        raise ValueError("barrier origin, ATR and tick size must be finite positive values")
    return {
        "target_price_long": snap_price_to_tick(
            origin_close + BARRIER_TP_ATR_MULTIPLIER * atr,
            tick_size,
            upward=True,
        ),
        "stop_price_long": snap_price_to_tick(
            origin_close - BARRIER_SL_ATR_MULTIPLIER * atr,
            tick_size,
            upward=False,
        ),
        "target_price_short": snap_price_to_tick(
            origin_close - BARRIER_TP_ATR_MULTIPLIER * atr,
            tick_size,
            upward=False,
        ),
        "stop_price_short": snap_price_to_tick(
            origin_close + BARRIER_SL_ATR_MULTIPLIER * atr,
            tick_size,
            upward=True,
        ),
    }


def np_is_finite(value: float) -> bool:
    # Kept dependency-free so the contract helper can be used by baseline inference.
    return value == value and value not in (float("inf"), float("-inf"))


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
    if {"ask_close", "bid_close"}.issubset(result.columns):
        result["spread_usd"] = (
            result["ask_close"].astype(float) - result["bid_close"].astype(float)
        )
    else:
        result["spread_usd"] = result.get("spread_usd", 0.0)
    spread = result["spread_usd"].astype(float)
    result["spread_close_pct"] = spread / close
    result["spread_median_24_pct"] = spread.rolling(24).median() / close
    result["spread_q75_24_pct"] = spread.rolling(24).quantile(0.75) / close
    result["spread_q90_24_pct"] = spread.rolling(24).quantile(0.90) / close
    result["spread_atr_ratio"] = spread / result["atr_24"].replace(0.0, np.nan)
    hour = result["timestamp"].dt.hour + result["timestamp"].dt.minute / 60.0
    weekday = result["timestamp"].dt.weekday
    result["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    result["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    result["weekday_sin"] = np.sin(2 * np.pi * weekday / 7)
    result["weekday_cos"] = np.cos(2 * np.pi * weekday / 7)
    return result


def postprocess_quantiles(values):
    """Apply the same monotonic quantile contract in evaluation and live inference."""
    import numpy as np

    array = np.asarray(values, dtype=float).copy()
    return np.maximum.accumulate(array, axis=-1)


def contiguous_forward_mask(timestamps, horizon: int, *, minutes: int = 5):
    """Require every forward step to be an exact consecutive M5 timestamp."""
    import pandas as pd

    if horizon <= 0:
        raise ValueError("horizon must be positive")
    normalized = pd.to_datetime(timestamps, utc=True)
    mask = normalized.notna()
    for step in range(1, horizon + 1):
        mask &= normalized.shift(-step).eq(
            normalized + pd.Timedelta(minutes=minutes * step)
        )
    return mask


def add_objective_labels(frame, *, barrier_horizon: int = BARRIER_HORIZON):
    """Add future-return, direction, excursion and TP-before-SL labels."""
    import numpy as np

    result = frame.copy()
    executable_columns = {
        f"{side}_{field}"
        for side in ("bid", "ask")
        for field in ("open", "high", "low", "close")
    }
    missing = sorted(executable_columns - set(result.columns))
    if missing:
        raise ValueError(
            "executable Bid/Ask OHLC is required for objective labels; missing "
            + ", ".join(missing)
        )
    if "chart_mode" not in result.columns or not (
        result["chart_mode"].astype(str).str.upper() == "BID"
    ).all():
        raise ValueError("candidate labels currently require MT5 BID chart mode")
    close = result["close"].astype(float)
    continuity_masks = {
        horizon: contiguous_forward_mask(result["timestamp"], horizon)
        for horizon in HORIZONS
    }
    continuity_masks.setdefault(
        barrier_horizon,
        contiguous_forward_mask(result["timestamp"], barrier_horizon),
    )
    for horizon in HORIZONS:
        raw_target = np.log(close.shift(-horizon) / close)
        result[f"target_{horizon}"] = raw_target.where(continuity_masks[horizon])
    result["direction_3"] = np.where(
        result["target_3"].notna(),
        (result["target_3"] > 0).astype(float),
        np.nan,
    )

    barrier_long_outcome: list[str | None] = []
    barrier_short_outcome: list[str | None] = []
    barrier_long_class: list[float] = []
    barrier_short_class: list[float] = []
    barrier_long_first_touch_time: list[float] = []
    barrier_short_first_touch_time: list[float] = []
    mfe_long: list[float] = []
    mae_long: list[float] = []
    mfe_short: list[float] = []
    mae_short: list[float] = []

    def classify_barrier(
        future,
        *,
        target: float,
        stop: float,
        side: str,
    ) -> tuple[str, int | None]:
        for _, future_bar in future.iterrows():
            raw_path = future_bar.get("executable_tick_path_json")
            if isinstance(raw_path, str) and raw_path.strip():
                try:
                    path = json.loads(raw_path)
                except json.JSONDecodeError:
                    path = []
                normalized = [
                    (int(tick[0]), float(tick[1]), float(tick[2]))
                    for tick in path
                    if isinstance(tick, list) and len(tick) >= 3
                ]
                for time_msc, grouped in groupby(
                    normalized, key=lambda tick: tick[0]
                ):
                    prices = [
                        tick[1] if side == "long" else tick[2]
                        for tick in grouped
                    ]
                    tp_hit = any(
                        price >= target if side == "long" else price <= target
                        for price in prices
                    )
                    sl_hit = any(
                        price <= stop if side == "long" else price >= stop
                        for price in prices
                    )
                    if tp_hit and sl_hit:
                        return "AMBIGUOUS_SAME_TIMESTAMP", time_msc
                    if tp_hit:
                        return "TP_FIRST", time_msc
                    if sl_hit:
                        return "SL_FIRST", time_msc
            if side == "long":
                high = float(future_bar["bid_high"])
                low = float(future_bar["bid_low"])
                tp_hit, sl_hit = high >= target, low <= stop
            else:
                high = float(future_bar["ask_high"])
                low = float(future_bar["ask_low"])
                tp_hit, sl_hit = low <= target, high >= stop
            if tp_hit and sl_hit:
                return "AMBIGUOUS_SAME_BAR", None
            if tp_hit:
                return "TP_FIRST", None
            if sl_hit:
                return "SL_FIRST", None
        return "NO_HIT_BEFORE_EXPIRY", None

    for index, row in result.iterrows():
        future = result.iloc[index + 1 : index + 1 + barrier_horizon]
        atr = float(row.get("atr_24", np.nan))
        start = float(row["close"])
        is_contiguous = (
            barrier_horizon in continuity_masks
            and bool(continuity_masks[barrier_horizon].iloc[index])
        )
        tick_size = float(row.get("tick_size", np.nan))
        if (
            len(future) < barrier_horizon
            or not is_contiguous
            or not np.isfinite(atr)
            or atr <= 0
            or not np.isfinite(tick_size)
            or tick_size <= 0
        ):
            barrier_long_outcome.append(None)
            barrier_short_outcome.append(None)
            barrier_long_class.append(np.nan)
            barrier_short_class.append(np.nan)
            barrier_long_first_touch_time.append(np.nan)
            barrier_short_first_touch_time.append(np.nan)
            mfe_long.append(np.nan)
            mae_long.append(np.nan)
            mfe_short.append(np.nan)
            mae_short.append(np.nan)
            continue

        prices = barrier_prices(start, atr, tick_size)
        long_outcome, long_touch_time = classify_barrier(
            future,
            target=prices["target_price_long"],
            stop=prices["stop_price_long"],
            side="long",
        )
        short_outcome, short_touch_time = classify_barrier(
            future,
            target=prices["target_price_short"],
            stop=prices["stop_price_short"],
            side="short",
        )
        barrier_long_outcome.append(long_outcome)
        barrier_short_outcome.append(short_outcome)
        barrier_long_class.append(BARRIER_CLASS.get(long_outcome, np.nan))
        barrier_short_class.append(BARRIER_CLASS.get(short_outcome, np.nan))
        barrier_long_first_touch_time.append(
            float(long_touch_time) if long_touch_time is not None else np.nan
        )
        barrier_short_first_touch_time.append(
            float(short_touch_time) if short_touch_time is not None else np.nan
        )
        future_bid_high = float(future["bid_high"].max())
        future_bid_low = float(future["bid_low"].min())
        future_ask_high = float(future["ask_high"].max())
        future_ask_low = float(future["ask_low"].min())
        long_entry_ask = float(row["ask_close"])
        short_entry_bid = float(row["bid_close"])
        mfe_long.append(max(0.0, future_bid_high - long_entry_ask))
        mae_long.append(max(0.0, long_entry_ask - future_bid_low))
        mfe_short.append(max(0.0, short_entry_bid - future_ask_low))
        mae_short.append(max(0.0, future_ask_high - short_entry_bid))

    result["barrier_long_outcome"] = barrier_long_outcome
    result["barrier_short_outcome"] = barrier_short_outcome
    result["barrier_long_class"] = barrier_long_class
    result["barrier_short_class"] = barrier_short_class
    result["barrier_long_first_touch_time_msc"] = barrier_long_first_touch_time
    result["barrier_short_first_touch_time_msc"] = barrier_short_first_touch_time
    result["mfe_long_price_distance"] = mfe_long
    result["mae_long_price_distance"] = mae_long
    result["mfe_short_price_distance"] = mfe_short
    result["mae_short_price_distance"] = mae_short
    return result


def build_training_frame(frame):
    return add_objective_labels(engineer_features(frame))
