# Forecast Skill Evaluation P0 Inventory

Generated on 2026-06-17 from the live local SAPPHIRE gateway. This was a read-only inventory: no application code was changed, no files under `sapphire/services/` were modified, and no station identifiers or raw discharge values are included below.

## 1. Environment

Required live gate passed.

| Item | Result |
|---|---|
| Gateway `/health/ready` | `{"status":"ready","service":"API Gateway","preprocessing":"ready"}` |
| API base URL | `http://localhost:8000` |
| `sapphire-api-client` version | `0.5.0` |
| Client pagination | Full skip/limit loops to completion; probe limit was 10,000 per page |

Note: `sapphire/.env` did not contain all Compose variables required by the full dashboard stack. The API/database services were brought up with runtime environment values and the dashboard was not required for this inventory.

## 2. Model x Horizon

Short-term `forecasts` contains these `(horizon_type, model_type)` pairs:

| Horizon | Models | Model count |
|---|---:|---:|
| day | TFT, TSMixer, TiDE | 3 |
| pentad | EM, NE, TFT, TSMixer, TiDE | 5 |
| decade | EM, NE, TFT, TSMixer, TiDE | 5 |

No short-term horizon is covered by only one or two models.

Long-term `long_forecasts` contains these `(horizon_type, model_type)` pairs:

| Horizon | Models | Model count |
|---|---:|---:|
| month | EM, GBT, LR_Base, LR_SM, LR_SM_DT, LR_SM_ROF, MC_ALD, Naive Mean, SM_GBT, SM_GBT_LR, SM_GBT_Norm, Skilled Mean | 12 |
| quarter | EM, GBT, LR_Base, LR_SM, LR_SM_DT, LR_SM_ROF, MC_ALD, Naive Mean, SM_GBT, SM_GBT_LR, SM_GBT_Norm, Skilled Mean | 12 |
| season | LR_Base, LR_SM | 2 |

Flag: `season` is covered by only two long-term models.

## 3. Period-Index Ranges

| Horizon | Runoffs `horizon_in_year` | Hydrographs `horizon_in_year` | Forecasts `horizon_in_year` | Long forecasts expose `horizon_in_year` |
|---|---:|---:|---:|---|
| day | 1..366, 366 distinct | 1..365, 365 distinct | 0..178, 84 distinct | no |
| pentad | 1..72, 72 distinct | 1..72, 72 distinct | 0..72, 68 distinct | no |
| decade | 1..36, 36 distinct | 1..36, 36 distinct | 0..36, 37 distinct | no |
| month | none | 1..12, 12 distinct | none | no |
| quarter | none | 1..4, 4 distinct | none | no |
| season | none | 1 only | none | no |

Confirmed: API horizon literal is `decade`; the API response also returns `decade`.

Important live-data caveats:

- Short-term forecasts include `horizon_in_year = 0` sentinel rows for day, pentad, and decade. Those rows do not join to hydrographs.
- `long_forecasts` has no `horizon_in_year`.
- `runoffs.decade.horizon_value` spans 1..36, while `hydrographs/forecasts.decade.horizon_value` use 1..3 plus forecast sentinel 0. Do not use `horizon_value` as the norm join key.

## 4. Verified Join-Key Mapping

Short-term day/pentad/decade:

| Horizon | Correct candidate key | Row-level result | `horizon_value` result | Plausibility ratio `norm / observed climatology` |
|---|---|---:|---:|---:|
| day | `forecasts.(code, horizon_in_year)` -> `hydrographs.(code, horizon_in_year)` | 70,185 rows matched one norm row; 1,096,227 had no match, dominated by period 0 rows | 0 rows matched one norm row | n=3,147 keys; p05=0.787, p50=0.938, p95=1.054 |
| pentad | same | 45,779 rows matched one norm row; 9 rows hit duplicate norm keys; 197,966 had no match, dominated by period 0 rows | positive-period rows map to multiple hydrograph rows by `horizon_value`, so not a 1:1 key | no usable ratio: hydrograph `norm` is unusable for all pentad rows |
| decade | same | 25,940 rows matched one norm row; 15 rows hit duplicate norm keys; 98,878 had no match, dominated by period 0 rows | positive-period rows map to multiple hydrograph rows by `horizon_value`, so not a 1:1 key | no usable ratio: hydrograph `norm` is unusable for all decade rows |

Conclusion for short-term joins: `horizon_in_year` is the only viable period column, and `horizon_value` is not the join key. The archive still fails a full 1:1 join gate because of period 0 forecast rows, small duplicate hydrograph keys for pentad/decade, and absent usable pentad/decade norms.

Long-term month/quarter/season:

| Horizon | Exact mapping that exists in current hydrographs | Row-level result | Plausibility ratio |
|---|---|---:|---:|
| month | Calendar-month rows: `valid_from = first day of month` and `valid_to = last day of same month`; map `valid_from.month` -> `hydrographs.horizon_in_year` | 526,404 exact calendar rows matched one norm row; 72,643 exact calendar rows had no norm row for the forecast code; 517,195 rolling-window rows cannot be exactly represented by the 12 monthly hydrograph keys | not computable: month runoffs truth is absent |
| quarter | Calendar-quarter rows: Jan-Mar -> 1, Apr-Jun -> 2, Jul-Sep -> 3, Oct-Dec -> 4 | 42,640 exact calendar rows matched one norm row; 14,711 exact calendar rows had no norm row for the forecast code; 16,095 rolling-quarter rows cannot be exactly represented by the 4 quarterly hydrograph keys | not computable: quarter runoffs truth is absent |
| season | Configured season Apr-01..Sep-30 -> `hydrographs.horizon_in_year = 1` | 12,988 rows matched one norm row; 1,635 had no norm row for the forecast code | not computable: season runoffs truth is absent |

Conclusion for long-term joins: `horizon_value` is lead-like and is not a reliable period key. The exact hydrograph mapping must be derived from `valid_from/valid_to`, but month and quarter contain rolling windows that the current calendar-only hydrograph period keys cannot exactly represent.

## 5. Point-Value Null Rates

Short-term deterministic point column: `forecasted_discharge`.

| Horizon | Rows | Non-null rate |
|---|---:|---:|
| day | 1,166,412 | 65.5615% |
| pentad | 243,754 | 99.7366% |
| decade | 124,833 | 99.2478% |

Long-term deterministic point chain: `q -> q50 -> q_loc`.

| Horizon | Rows | `q` non-null | `q50` non-null | `q_loc` non-null | Any chain value |
|---|---:|---:|---:|---:|---:|
| month | 1,116,242 | 96.2911% | 13.4280% | 9.8780% | 98.5162% |
| quarter | 73,446 | 95.8187% | 13.3608% | 0.0000% | 96.5594% |
| season | 14,623 | 98.1878% | 0.0000% | 0.0000% | 98.1878% |

Downstream assumption change: do not assume deterministic point values are always present, especially for day forecasts and long-term chain fallbacks.

## 6. Long-Term Leads

Long-term rows do carry multiple `horizon_value` leads per target when grouping by `(code, model_type, valid_from, valid_to)`. When the issue date is included in the grouping, each issue-target group has one `horizon_value`.

| Horizon | `horizon_value` range | Target groups with multiple leads | Max leads per target | Multiple leads within same issue-target |
|---|---:|---:|---:|---:|
| month | 0..12, 13 distinct | 155,436 | 4 | no |
| quarter | 1..4, 4 distinct | 7,917 | 2 | no |
| season | 0..3, 4 distinct | 2,943 | 4 | no |

P4 lead-time stratification is meaningful for long-term horizons, but should treat the issue date as part of the forecast instance.

## 7. Observed-Truth Coverage

| Horizon | Runoffs rows | Distinct years |
|---|---:|---:|
| day | 889,786 | 87 |
| pentad | 123,199 | 27 |
| decade | 61,588 | 27 |
| month | 0 | 0 |
| quarter | 0 | 0 |
| season | 0 | 0 |

Blocker: long-horizon observed truth is absent from `read_runoff()` for month, quarter, and season. Those horizons must be skip-and-logged downstream unless an alternate observed-truth source is explicitly introduced.

## 8. Norm Coverage

| Horizon | Hydrograph rows | Usable norm fraction | `hydrographs.count` distribution | Key uniqueness |
|---|---:|---:|---|---|
| day | 28,789 | 85.5778% | n=28,789; p05=11, p50=24, p95=75, max=87 | 28,789/28,789 keys are 1:1 |
| pentad | 5,766 | 0.0000% | all count values null | 5,754 keys are 1:1; 6 keys duplicate; max rows/key=2 |
| decade | 2,382 | 0.0000% | all count values null | 2,370 keys are 1:1; 6 keys duplicate; max rows/key=2 |
| month | 636 | 100.0000% | all count values null | 636/636 keys are 1:1 |
| quarter | 212 | 100.0000% | all count values null | 212/212 keys are 1:1 |
| season | 53 | 100.0000% | all count values null | 53/53 keys are 1:1 |

Confirmed: hydrograph API records contain no provenance/source field. Provenance must be assigned from configured horizon mapping, not read from the API.

## 9. Gate Decision

Verdict: the archive shape is not sufficient to proceed to a full P1 implementation across all requested horizons.

Proceedable only with hard guards:

- Day can be used only for rows with `horizon_in_year > 0`, a 1:1 hydrograph join, usable norm, observed truth, and non-null point value. Its norm/observed-climatology ratio clusters near 1 for matched keys.
- Pentad and decade must be blocked or remediated before norm-based skill work: both have observed truth, but hydrograph `norm` is unusable across the API response, forecast period 0 rows are common, and a small number of norm keys duplicate.
- Month, quarter, and season must be excluded from observed-truth-based skill evaluation until long-horizon runoffs/truth are available. Month and quarter also contain rolling valid windows that cannot be exactly joined to calendar-only hydrograph period keys.

Downstream assumptions to change:

- `forecasted_discharge` and the long-term `q/q50/q_loc` chain are not universally populated.
- Long-term lead-time stratification is meaningful, but target grouping must distinguish issue date.
- `horizon_value` is not a safe norm join key for short-term forecasts and is not a reliable long-term period key.
