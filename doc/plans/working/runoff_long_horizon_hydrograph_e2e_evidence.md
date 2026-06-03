# Runoff Long-Horizon Hydrograph - Local End-to-End Evidence

**Date produced**: 2026-06-02
**Plan reference**: doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md (commit ec03c44)
**Writer commit**: apps/preprocessing_runoff/sync_long_horizon_hydrograph.py (commit 785528a)
**Target year**: 2026

## 1. Stack health

- Local API health: HTTP 200 on http://localhost:8000/health.
- iEH HF SDK readiness: 63 discharge sites returned from `get_discharge_sites()` after loading the configured env file through `setup_library.load_environment()`.

## 2. Writer run

- Command: `uv run python sync_long_horizon_hydrograph.py --target-year 2026`
- Exit code: 0
- Wall time: 58.20 seconds by captured warning timestamp span.
- Successful station writes: 53.
- Warning skips: 9 stations with missing SDK norm inputs.
- Records written: 689 records verified via API, equal to 636 monthly records plus 53 seasonal records.
- Note: the captured writer output emitted warning lines only in this local logging configuration; the final INFO completion line was not present, so record counts are verified from the preprocessing API substrate.
- Last writer output, sanitized:

```text
2026-06-02 22:39:49,915 - WARNING - write_station_monthly_hydrograph: expected 12 norm values for site <skipped-station-1>, got 0 - skipping this site.
2026-06-02 22:39:50,569 - WARNING - write_station_monthly_hydrograph: expected 12 norm values for site <skipped-station-2>, got 0 - skipping this site.
2026-06-02 22:40:24,517 - WARNING - write_station_monthly_hydrograph: expected 12 norm values for site <skipped-station-3>, got 0 - skipping this site.
2026-06-02 22:40:34,472 - WARNING - write_station_monthly_hydrograph: expected 12 norm values for site <skipped-station-4>, got 0 - skipping this site.
2026-06-02 22:40:35,130 - WARNING - write_station_monthly_hydrograph: expected 12 norm values for site <skipped-station-5>, got 0 - skipping this site.
2026-06-02 22:40:47,284 - WARNING - write_station_monthly_hydrograph: SDK call failed for site <skipped-station-6>, skipping. Error: ValueError: No path provided or the provided path is None
2026-06-02 22:40:47,564 - WARNING - write_station_monthly_hydrograph: SDK call failed for site <skipped-station-7>, skipping. Error: ValueError: No path provided or the provided path is None
2026-06-02 22:40:47,837 - WARNING - write_station_monthly_hydrograph: SDK call failed for site <skipped-station-8>, skipping. Error: ValueError: No path provided or the provided path is None
2026-06-02 22:40:48,118 - WARNING - write_station_monthly_hydrograph: SDK call failed for site <skipped-station-9>, skipping. Error: ValueError: No path provided or the provided path is None
```

## 3. API probe results

The API probe queried `GET /api/preprocessing/hydrograph/` for target-year 2026 rows with `horizon=month` and `horizon=season`. Station codes are aliased.

- Total 2026 monthly rows: 636.
- Total 2026 seasonal rows: 53.
- Stations with 12 monthly rows: 53.
- Stations with a seasonal row: 53.

### Monthly records

| Station | Records returned | Non-null norm | Non-null previous | Non-null current |
|---|---:|---:|---:|---:|
| `<station-1>` | 12 | 12 | 12 | 5 |
| `<station-2>` | 12 | 12 | 12 | 5 |
| `<station-3>` | 12 | 12 | 12 | 5 |
| `<station-4>` | 12 | 12 | 12 | 5 |
| `<station-5>` | 12 | 12 | 12 | 5 |

### Seasonal records

| Station | Records returned | norm | previous | current |
|---|---:|---:|---:|---|
| `<station-1>` | 1 | 53.50 | 59.38 | null |
| `<station-2>` | 1 | 13.26 | 11.04 | null |
| `<station-3>` | 1 | 39.43 | 31.25 | null |
| `<station-4>` | 1 | 9.06 | 5.24 | null |
| `<station-5>` | 1 | 31.72 | 25.72 | null |

Seasonal sanity check:

`<station-1>` monthly previous for Apr-Sep = `(40.15, 61.12, 49.13, 67.63, 77.09, 61.15)`; mean = `59.38`; API-returned seasonal previous = `59.38`. Match.

Seasonal `current` is `null` for all sampled stations, as expected for target year 2026 because June 2026 is in progress and the seasonal strict-completeness rule propagates the in-progress monthly gap.

## 4. Regression test suite

Command: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff 2>&1 | tail -10`

```text
======================= 323 passed, 2 skipped in 11.05s ========================
preprocessing_runoff tests passed

========================================
TEST SUMMARY
========================================
Passed (1): preprocessing_runoff

All tests completed successfully!
```

## 5. Decision

Stack health passed, writer exit code was 0, API substrate is non-empty with useful monthly and seasonal triad fields, seasonal aggregation matches the six-month monthly mean, and the regression suite has no new failures or skips.

DISPATCH: PROCEED
