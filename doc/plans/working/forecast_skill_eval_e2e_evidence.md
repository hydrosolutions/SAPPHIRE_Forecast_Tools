# Forecast Skill Eval P7 Evidence

Date: 2026-06-18

## Gates

- Part A test gate: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_skill_eval`
  passed with 73 passed, 0 failed, 0 skipped.
- Local API readiness: `http://localhost:8000/health/ready` returned
  `{"status":"ready","service":"API Gateway","preprocessing":"ready"}`.
- `sapphire-api-client` version: 0.5.0.
- Raw live artifacts are under `apps/forecast_skill_eval/artifacts/` and are
  gitignored. This document is aggregate-only: no real station codes and no
  discharge or norm values are reported.

## Validation Fixes

- Added a genuine Part B guard in `orchestrator.run(...)`: per-horizon
  exceptions now record `stage=horizon, reason=horizon_error` and skip that
  horizon with a reason instead of aborting the whole run.
- Fixed a genuine live API-client contract bug in the long-term reader:
  `read_long_term_forecasts(...)` now uses `horizon_type=...`, matching
  `sapphire-api-client==0.5.0`.
- No files under `sapphire/services/` were changed.

## Live Run Scope

All live runs used a two-station subset selected from available local archive
coverage. Station IDs are intentionally omitted. No date cap was applied to the
final live runs; this avoids silently starving calculated norms. A first
date-bounded day smoke attempt produced 0 pairs because the selected slice had
no usable non-sentinel day forecasts, so the final day smoke used an
archive-derived two-station subset.

| horizon | run id | models | n_pairs |
| --- | --- | --- | ---: |
| day | p7-smoke-day-tft | TFT | 532 |
| pentad | p7-pentad | EM, TFT | 394 |
| decade | p7-decade | EM, TFT | 194 |
| month | p7-month | LR_Base, LR_SM | 3053 |
| quarter | p7-quarter | LR_Base, LR_SM | 274 |
| season | p7-season | LR_Base, LR_SM | 199 |

## Exclusions

| horizon | stage | reason | count |
| --- | --- | --- | ---: |
| day | observed | observed_missing | 6681 |
| day | pair | forecast_missing | 66 |
| day | pair | forecast_sentinel | 16011 |
| pentad | norm | norm_unavailable_lt_min_years | 141 |
| pentad | observed | observed_missing | 1241 |
| pentad | pair | forecast_sentinel | 1183 |
| pentad | pair | observed_unmatched | 61 |
| decade | norm | norm_unavailable_lt_min_years | 71 |
| decade | observed | observed_missing | 617 |
| decade | pair | forecast_sentinel | 593 |
| decade | pair | observed_unmatched | 33 |
| month | observed | observed_incomplete_month | 12 |
| month | pair | forecast_missing | 56 |
| month | pair | observed_unmatched | 149 |
| quarter | observed | observed_incomplete_quarter | 12 |
| quarter | pair | forecast_missing | 17 |
| quarter | pair | forecast_rolling_window | 420 |
| quarter | pair | observed_unmatched | 27 |
| season | observed | observed_incomplete_season | 10 |
| season | pair | observed_unmatched | 104 |

## Pooled Metrics

`beats_climatology_pod` compares each model POD with the matched climatology
baseline POD for the same sample.

| horizon | model | n_pairs | base_rate | POD | FAR | FN_count | beats_climatology_pod |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| day | TFT | 532 | 0.363 | 0.290 | 0.176 | 137 | yes |
| pentad | EM | 205 | 0.478 | 0.959 | 0.041 | 4 | yes |
| pentad | TFT | 189 | 0.360 | 0.926 | 0.100 | 5 | yes |
| decade | EM | 91 | 0.374 | 0.941 | 0.030 | 2 | yes |
| decade | TFT | 103 | 0.408 | 0.952 | 0.048 | 2 | yes |
| month | LR_Base | 1578 | 0.124 | 0.480 | 0.271 | 102 | yes |
| month | LR_SM | 1475 | 0.118 | 0.224 | 0.400 | 135 | yes |
| quarter | LR_Base | 137 | 0.095 | 0.154 | 0.500 | 11 | yes |
| quarter | LR_SM | 137 | 0.102 | 0.143 | 0.600 | 12 | yes |
| season | LR_Base | 100 | 0.150 | 0.133 | 0.333 | 13 | yes |
| season | LR_SM | 99 | 0.152 | 0.000 | 1.000 | 15 | no |

## Norm Provenance

| horizon | norm_provenance | n_pairs |
| --- | --- | ---: |
| day | calculated | 532 |
| pentad | calculated | 394 |
| decade | calculated | 194 |
| month | official | 3053 |
| quarter | aggregated_from_monthly | 274 |
| season | aggregated_from_monthly | 199 |

## Anomalies For Review

- No final live horizon had 0 pairs.
- Day exclusions are dominated by sentinel forecast rows.
- Pentad EM base rate is high at 0.478; decade TFT is slightly high at 0.408.
- Month, quarter, and season base rates are low relative to the rough 0.2-0.4
  expectation.
- Quarter has 420 rolling-window exclusions. Month and season did not show
  rolling-window exclusions in this two-station subset.
- Season LR_SM POD is 0.000 and does not beat the climatology POD baseline.
