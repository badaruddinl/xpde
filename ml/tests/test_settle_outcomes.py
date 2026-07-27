from __future__ import annotations

from xpde_ml.settle_outcomes import barrier_outcome


def proposal(action: str, invalidation_price: float | None) -> dict:
    return {
        "action": action,
        "invalidation_price": invalidation_price,
    }


def test_barrier_outcome_uses_first_actionable_proposal() -> None:
    proposals = [
        proposal("WAIT", None),
        proposal("LONG", 99.0),
    ]
    bars = [{"high": 101.2, "low": 99.5, "close": 101.0}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) == 1


def test_barrier_outcome_detects_short_stop_first() -> None:
    proposals = [proposal("SHORT", 101.0)]
    bars = [{"high": 101.2, "low": 99.5, "close": 100.8}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) == 0


def test_barrier_outcome_keeps_same_bar_ambiguity_unresolved() -> None:
    proposals = [proposal("LONG", 99.0)]
    bars = [{"high": 101.2, "low": 98.8, "close": 100.2}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) is None


def test_barrier_outcome_requires_directional_proposal() -> None:
    proposals = [proposal("NO_PREDICTION", None)]
    bars = [{"high": 102.0, "low": 98.0, "close": 101.0}]

    assert barrier_outcome(100.0, 1.0, proposals, bars) is None
