# Runoff Long-Horizon Hydrograph - Coverage & Audit Probe

**Date produced**: 2026-06-02
**Plan reference**: doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md (commit 355e276)
**Decisions artifact**: doc/plans/working/runoff_long_horizon_hydrograph_decisions.md (commit 28ba979)
**Target year (Y)**: 2026 (Y-1 = 2025)

## 1. Stack health

Python urllib GET on http://localhost:8000/health -> HTTP 200.
Daily runoff endpoint probe returned HTTP 200 for the station-set seed query and for station-year detail queries.

## 2. Station set

- Source: API fallback query `GET /runoff/?horizon=day&start_date=2026-01-01&end_date=2026-01-02` with a high result limit; env file was inspected first and exposed HRU-like candidates but no single runoff station list.
- Count: 61
- Aliasing: stations are aliased as `<station-1>` ... `<station-61>` in this artifact; real codes never appear.

## 3. Coverage table (informational - not a dispatch gate)

The per-`(station, year)` table below records row counts and non-null counts for operator awareness only. The earlier >=80% year-level gate was retired on 2026-06-02 because real sparseness clusters in specific months; the data-quality gate moved into Phase 1 as a per-month threshold (D-Q6). Denominator kind is `full` for complete years (365/366) and `elapsed` for the in-progress year (`today.timetuple().tm_yday`, 153 on 2026-06-02).

Schema note: the live endpoint returned daily runoff under `discharge`; no `value` key was present in sampled rows, so finite `discharge` values were counted as the runoff value field.

| Station | Year | Rows | Non-null | Denominator (kind) | % (info) |
|---|---:|---:|---:|---|---:|
| `<station-1>` | 2026 | 166 | 128 | 153 (elapsed) | 83.7% |
| `<station-1>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-2>` | 2026 | 153 | 122 | 153 (elapsed) | 79.7% |
| `<station-2>` | 2025 | 365 | 363 | 365 (full) | 99.5% |
| `<station-3>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-3>` | 2025 | 365 | 259 | 365 (full) | 71.0% |
| `<station-4>` | 2026 | 153 | 151 | 153 (elapsed) | 98.7% |
| `<station-4>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-5>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-5>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-6>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-6>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-7>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-7>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-8>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-8>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-9>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-9>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-10>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-10>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-11>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-11>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-12>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-12>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-13>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-13>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-14>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-14>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-15>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-15>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-16>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-16>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-17>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-17>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-18>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-18>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-19>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-19>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-20>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-20>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-21>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-21>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-22>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-22>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-23>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-23>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-24>` | 2026 | 147 | 74 | 153 (elapsed) | 48.4% |
| `<station-24>` | 2025 | 365 | 141 | 365 (full) | 38.6% |
| `<station-25>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-25>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-26>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-26>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-27>` | 2026 | 153 | 145 | 153 (elapsed) | 94.8% |
| `<station-27>` | 2025 | 365 | 350 | 365 (full) | 95.9% |
| `<station-28>` | 2026 | 105 | 78 | 153 (elapsed) | 51.0% |
| `<station-28>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-29>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-29>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-30>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-30>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-31>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-31>` | 2025 | 365 | 268 | 365 (full) | 73.4% |
| `<station-32>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-32>` | 2025 | 365 | 259 | 365 (full) | 71.0% |
| `<station-33>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-33>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-34>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-34>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-35>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-35>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-36>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-36>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-37>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-37>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-38>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-38>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-39>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-39>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-40>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-40>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-41>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-41>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-42>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-42>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-43>` | 2026 | 153 | 121 | 153 (elapsed) | 79.1% |
| `<station-43>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-44>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-44>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-45>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-45>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-46>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-46>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-47>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-47>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-48>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-48>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-49>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-49>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-50>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-50>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-51>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-51>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-52>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-52>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-53>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-53>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-54>` | 2026 | 83 | 56 | 153 (elapsed) | 36.6% |
| `<station-54>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-55>` | 2026 | 105 | 78 | 153 (elapsed) | 51.0% |
| `<station-55>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-56>` | 2026 | 106 | 79 | 153 (elapsed) | 51.6% |
| `<station-56>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-57>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-57>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-58>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-58>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-59>` | 2026 | 153 | 149 | 153 (elapsed) | 97.4% |
| `<station-59>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-60>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-60>` | 2025 | 365 | 365 | 365 (full) | 100.0% |
| `<station-61>` | 2026 | 153 | 153 | 153 (elapsed) | 100.0% |
| `<station-61>` | 2025 | 365 | 365 | 365 (full) | 100.0% |

**Substrate gate**: PASS. All 122 recorded `(station, year)` pairs have non-null >= 1; the gate only requires at least one non-empty pair.

**Operator observations**: Chronic complete-year low coverage: `<station-24>`/2025; low 2026 row-count aliases: `<station-28>`, `<station-54>`, `<station-55>`, `<station-56>`.

## 4. Audit results (grep over apps/)

Command run:

```
rg -nP '"(month|season)"|horizon_type\s*=\s*["\x27]?(month|season)|write_hydrograph\b' apps/
```

Raw matches: 494 across 63 files. Matches are grouped below by file/category to avoid exposing station codes from test fixtures while still triaging every raw match category.

| File | Line | Match | Triage |
|---|---|---|---|
| apps/preprocessing_runoff/sync_monthly_norms.py | multiple | 2 raw matches | Expected old runoff path, to be retired in Phase 4. |
| apps/preprocessing_runoff/sync_long_horizon_hydrograph.py | absent | 0 raw matches | Expected absent before Phase 1; the new writer has not been built yet. |
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

Phase 1 may dispatch under the sanity-gate-only P0b criteria. The informational table still shows specific sparse aliases for Phase 1 D-Q6 monthly handling; no real station codes are included here.

DISPATCH: PROCEED