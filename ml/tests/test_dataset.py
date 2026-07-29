from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

import pytest

pytest.importorskip("pandas")

from xpde_ml.dataset import (
    BARRIER_CLASS,
    BARRIER_HORIZON,
    FEATURE_COLUMNS,
    add_objective_labels,
    barrier_prices,
    build_training_frame,
    postprocess_quantiles,
)


def test_training_features_are_causal_and_labels_use_future_bars() -> None:
    import pandas as pd

    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    price = 3000.0
    for index in range(120):
        close = price + 0.2 + (index % 5) * 0.03
        rows.append(
            {
                "timestamp": start + timedelta(minutes=5 * index),
                "open": price,
                "high": close + 0.4,
                "low": price - 0.3,
                "close": close,
                "tick_volume": 100 + index,
                "spread_usd": 0.24,
                "bid_open": price,
                "bid_high": close + 0.4,
                "bid_low": price - 0.3,
                "bid_close": close,
                "ask_open": price + 0.24,
                "ask_high": close + 0.64,
                "ask_low": price - 0.06,
                "ask_close": close + 0.24,
                "chart_mode": "BID",
            }
        )
        price = close

    result = build_training_frame(pd.DataFrame(rows))
    assert set(FEATURE_COLUMNS).issubset(result.columns)
    assert result.loc[40, "target_3"] > 0
    assert result.tail(12)["target_12"].isna().all()
    assert BARRIER_HORIZON == 3
    assert {
        "barrier_long_outcome",
        "barrier_short_outcome",
        "mfe_long_usd",
        "mae_short_usd",
    }.issubset(result.columns)


def test_barrier_labels_keep_no_hit_and_report_same_bar_ambiguity() -> None:
    import pandas as pd

    rows = [
        {"close": 100.0, "high": 100.1, "low": 99.9, "atr_24": 1.0},
        {"close": 100.0, "high": 100.2, "low": 99.8, "atr_24": 1.0},
        {"close": 100.0, "high": 100.3, "low": 99.7, "atr_24": 1.0},
        {"close": 100.0, "high": 100.4, "low": 99.6, "atr_24": 1.0},
    ]
    for row in rows:
        for field in ("open", "high", "low", "close"):
            value = row.get(field, row["close"])
            row[f"bid_{field}"] = value
            row[f"ask_{field}"] = value + 0.24
        row["chart_mode"] = "BID"
    frame = pd.DataFrame(rows)
    no_hit = add_objective_labels(frame)
    assert no_hit.loc[0, "barrier_long_outcome"] == "NO_HIT_BEFORE_EXPIRY"
    assert (
        no_hit.loc[0, "barrier_long_class"]
        == BARRIER_CLASS["NO_HIT_BEFORE_EXPIRY"]
    )

    frame.loc[1, ["high", "low"]] = [101.5, 98.8]
    frame.loc[1, ["bid_high", "bid_low"]] = [101.5, 98.8]
    ambiguous = add_objective_labels(frame)
    assert ambiguous.loc[0, "barrier_long_outcome"] == "AMBIGUOUS_SAME_BAR"
    assert ambiguous.loc[0, "barrier_long_class"] != ambiguous.loc[0, "barrier_long_class"]


def test_barrier_contract_and_quantile_postprocessing_are_explicit() -> None:
    prices = barrier_prices(100.0, 2.0)
    assert prices == {
        "target_price_long": 102.5,
        "stop_price_long": 98.0,
        "target_price_short": 97.5,
        "stop_price_short": 102.0,
    }
    processed = postprocess_quantiles([[0.1, -0.2, 0.0, 0.3, 0.2]])
    assert processed.tolist() == [[0.1, 0.1, 0.1, 0.3, 0.3]]


def test_tick_sequence_resolves_same_bar_first_touch() -> None:
    import pandas as pd

    rows = [
        {"close": 100.0, "high": 100.1, "low": 99.9, "atr_24": 1.0},
        {"close": 100.0, "high": 101.5, "low": 98.8, "atr_24": 1.0},
        {"close": 100.0, "high": 100.2, "low": 99.8, "atr_24": 1.0},
        {"close": 100.0, "high": 100.2, "low": 99.8, "atr_24": 1.0},
    ]
    for index, row in enumerate(rows):
        for field in ("open", "high", "low", "close"):
            value = row.get(field, row["close"])
            row[f"bid_{field}"] = value
            row[f"ask_{field}"] = value + 0.24
        row["chart_mode"] = "BID"
        row["executable_tick_path_json"] = json.dumps(
            [
                [1_000 + index * 300_000, 100.0, 100.24],
                [2_000 + index * 300_000, 101.3, 101.54],
                [3_000 + index * 300_000, 98.8, 99.04],
            ]
        )
    labelled = add_objective_labels(pd.DataFrame(rows))
    assert labelled.loc[0, "barrier_long_outcome"] == "TP_FIRST"
    assert labelled.loc[0, "barrier_long_first_touch_time_msc"] == 302_000
