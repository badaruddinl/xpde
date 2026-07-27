# GOLDm# M5 candidate report — 2026-07-27

## Candidate

- Model ID: `catboost-goldm-m5-20260727152716-9e6316`
- Feature contract: `goldm-m5-v2`
- Status: `candidate` / live shadow
- Data: 50,000 completed M5 bars, normalized from broker UTC+3 to UTC
- Labelled rows: 49,964
- Train / calibration / final holdout: 34,962 / 7,483 / 7,495
- Purge gap: 12 bars

## Final holdout

| Metric | Candidate | Baseline |
| --- | ---: | ---: |
| Mean pinball loss, all horizons and quantiles | 0.00047450 | 0.00048477 |
| Relative pinball improvement | 2.12% | — |
| H3 80% interval coverage | 81.17% | — |
| Direction Brier score | 0.24964 | 0.25083 |
| Direction accuracy | 52.06% | — |

Holdout interval coverage by horizon:

| Horizon | Coverage | Mean interval width |
| ---: | ---: | ---: |
| 1 bar | 80.59% | 0.001875 |
| 3 bars | 81.17% | 0.003270 |
| 6 bars | 81.48% | 0.004680 |
| 12 bars | 80.59% | 0.006707 |

## Gate result

The candidate passed the local shadow eligibility gate:

- at least 1% mean pinball improvement over the empirical baseline;
- H3 interval coverage between 74% and 86%;
- calibrated direction Brier score no worse than the baseline.

This is not a champion promotion and is not evidence of profitability. Live
shadow outcomes must accumulate across enough sessions and regimes before a
human promotion decision. Automatic order execution remains unavailable.
