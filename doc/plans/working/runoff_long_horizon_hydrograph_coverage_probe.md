# Runoff Long-Horizon Hydrograph - Coverage & Audit Probe

**Date produced**: 2026-06-02
**Plan reference**: doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md (commit 355e276)
**Decisions artifact**: doc/plans/working/runoff_long_horizon_hydrograph_decisions.md (commit 28ba979)
**Target year (Y)**: 2026 (Y-1 = 2025)

## 1. Stack health

Python urllib GET on http://localhost:8000/health -> HTTP 200.

## 2. Station set

- Source: API fallback query `GET /runoff/?horizon=day&start_date=2026-01-01&end_date=2026-01-02` with a high result limit; env-referenced local station-selection JSON was not present in this checkout.
- Count: 61
- Aliasing: stations are aliased as `<station-1>` ... `<station-61>` in this artifact; real codes never appear.

## 3. Coverage results (>=80% threshold per plan)

Threshold: non-null daily runoff covers >=80% of calendar days per (station, year) in {Y, Y-1}. Denominators: 365 for 2025, 365 for 2026 (366 if leap; 2024 was leap, 2025/2026 are not).

Schema note: the live endpoint returned daily runoff under `discharge`; no `value` key was present in sampled rows, so finite `discharge` values were counted as the runoff value field.

| Station | Year | Rows | Non-null | Total days | % | Pass? |
|---|---:|---:|---:|---:|---:|---|
| `<station-1>` | 2026 | 166 | 128 | 365 | 35.1% | no |
| `<station-1>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-2>` | 2026 | 153 | 122 | 365 | 33.4% | no |
| `<station-2>` | 2025 | 365 | 363 | 365 | 99.5% | yes |
| `<station-3>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-3>` | 2025 | 365 | 259 | 365 | 71.0% | no |
| `<station-4>` | 2026 | 153 | 151 | 365 | 41.4% | no |
| `<station-4>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-5>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-5>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-6>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-6>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-7>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-7>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-8>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-8>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-9>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-9>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-10>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-10>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-11>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-11>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-12>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-12>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-13>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-13>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-14>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-14>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-15>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-15>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-16>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-16>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-17>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-17>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-18>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-18>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-19>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-19>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-20>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-20>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-21>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-21>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-22>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-22>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-23>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-23>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-24>` | 2026 | 147 | 74 | 365 | 20.3% | no |
| `<station-24>` | 2025 | 365 | 141 | 365 | 38.6% | no |
| `<station-25>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-25>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-26>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-26>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-27>` | 2026 | 153 | 145 | 365 | 39.7% | no |
| `<station-27>` | 2025 | 365 | 350 | 365 | 95.9% | yes |
| `<station-28>` | 2026 | 105 | 78 | 365 | 21.4% | no |
| `<station-28>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-29>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-29>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-30>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-30>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-31>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-31>` | 2025 | 365 | 268 | 365 | 73.4% | no |
| `<station-32>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-32>` | 2025 | 365 | 259 | 365 | 71.0% | no |
| `<station-33>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-33>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-34>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-34>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-35>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-35>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-36>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-36>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-37>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-37>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-38>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-38>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-39>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-39>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-40>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-40>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-41>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-41>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-42>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-42>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-43>` | 2026 | 153 | 121 | 365 | 33.2% | no |
| `<station-43>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-44>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-44>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-45>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-45>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-46>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-46>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-47>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-47>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-48>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-48>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-49>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-49>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-50>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-50>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-51>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-51>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-52>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-52>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-53>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-53>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-54>` | 2026 | 83 | 56 | 365 | 15.3% | no |
| `<station-54>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-55>` | 2026 | 105 | 78 | 365 | 21.4% | no |
| `<station-55>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-56>` | 2026 | 106 | 79 | 365 | 21.6% | no |
| `<station-56>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-57>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-57>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-58>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-58>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-59>` | 2026 | 153 | 149 | 365 | 40.8% | no |
| `<station-59>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-60>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-60>` | 2025 | 365 | 365 | 365 | 100.0% | yes |
| `<station-61>` | 2026 | 153 | 153 | 365 | 41.9% | no |
| `<station-61>` | 2025 | 365 | 365 | 365 | 100.0% | yes |

**Coverage gate**: FAIL. Passing pairs: 57; failing pairs: 65. All 61 aliases fail for 2026; 2025 failures: `<station-3>`, `<station-24>`, `<station-31>`, `<station-32>`.

## 4. Audit results (grep over apps/)

Command run:

```
rg -nP '"(month|season)"|horizon_type\s*=\s*["\x27]?(month|season)|write_hydrograph\b' apps/
```

Raw matches: 494 across 63 files. Matches are grouped below by file/category to avoid exposing station codes from test fixtures while still triaging every raw match category.

| File | Line | Match | Triage |
|---|---|---|---|
| apps/preprocessing_runoff/sync_monthly_norms.py | multiple | 2 raw matches | Expected wrapper for old monthly runoff norm path, to be retired in Phase 4. |
| apps/iEasyHydroForecast/forecast_library.py | multiple | 9 raw matches | Expected delegate used by sync_monthly_norms.py; writes norm-only hydrograph rows with horizon_type month. |
| apps/preprocessing_runoff/src/src.py | multiple | 5 raw matches | False positive for this gate: month arithmetic plus daily/pentad/decad hydrograph writer, not month/season hydrograph rows. |
| apps/preprocessing_runoff/test/test_api_write.py | multiple | 16 raw matches | Tests/mocks for preprocessing hydrograph writes; no production month/season writer. |
| apps/forecast_dashboard/** | multiple | 77 raw matches across 13 files | Dashboard read, display, bulletin, and test paths; no app-side hydrograph writer. |
| apps/postprocessing_forecasts/** | multiple | 308 raw matches across 31 files | Long-forecast and skill-metric readers/writers/tests; writes long_forecasts or skill_metrics, not hydrographs. |
| apps/long_term_forecasting/** | multiple | 37 raw matches across 8 files | Long-term model/config/readme/test references; no hydrograph writer. |
| apps/iEasyHydroForecast/tests/** and apps/linear_regression/test/** | multiple | 25 raw matches | Tests/mocks only; no production writer. |
| apps/validate_pipeline/** and apps/pipeline/tests/** | multiple | 12 raw matches | Validation and schedule test references; no hydrograph writer. |
| apps/conceptual_model/**, apps/machine_learning/** | multiple | 3 raw matches | Date/month feature logic; no hydrograph writer. |

**Audit gate**: PASS. No untriaged matches and no independent third-party month/season hydrograph writer surfaced beyond the expected monthly-norm wrapper/delegate path.

## 5. DECISION

The dispatch decision is recorded as the final line of this artifact.

## 6. Notes for Phase 1

Phase 1 should not dispatch while the coverage gate is failing. The 2026 API rows appear partial-year relative to the 365-day denominator, and the 2025 gaps are limited to the aliases listed in the coverage-gate line above.

DISPATCH: BLOCKED — daily runoff coverage below 80% threshold
