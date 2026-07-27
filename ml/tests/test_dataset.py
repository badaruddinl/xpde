from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pandas")

from xpde_ml.dataset import FEATURE_COLUMNS, build_training_frame


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
            }
        )
        price = close

    result = build_training_frame(pd.DataFrame(rows))
    assert set(FEATURE_COLUMNS).issubset(result.columns)
    assert result.loc[40, "target_3"] > 0
    assert result.tail(12)["target_12"].isna().all()
