# Gateway Snow Stat Population Plan

## Overview

The `Snow` API schema already carries `count`, `std`, and the ten dashboard-facing stat fields, but gateway write paths currently populate only `value`, `norm`, and `value1..value14`. The implementation shape is a write-side recalculation flow in `apps/preprocessing_gateway/`, modeled on `calculate_snow_norms_from_api` in `apps/preprocessing_gateway/dg_utils.py:379` and the existing yearly script in `apps/preprocessing_gateway/recalculate_snow_norms.py:39`. No `sapphire/services/`, dashboard, or `apps/iEasyHydroForecast/forecast_library.py` changes are part of this work. Scope is medium: add one aggregation helper, extend the existing norm recalculation path, verify locally, and document the operator cadence.

## Coordination

**Recalc cadence:** Run this with the yearly snow norm recalculation, likely end of August after snow reanalysis refresh, matching `recalculate_snow_norms.py:7` and `bin/yearly_snow_norm_recalculation.sh:8`. Wire it through the existing operator script rather than CI, because it needs a running API and operational environment.

**Branch ordering:** This work lands first on a new branch such as `develop_gateway_snow_stat_population` and merges to `maxat_sapphire_2`. Only then should `develop_dashboard_snow_display` rebase and re-run its Phase 0 gate.

**Coexistence with `recalculate_snow_norms.py`:** Extend the existing script so one yearly invocation computes and writes both `norm` and snow stat columns from the same API history read. The unified operator invocation remains `uv run recalculate_snow_norms.py`, with added flags only where needed for one-time historical backfill.

**Dashboard cross-plan contract:** The dashboard plan's `test_get_snow_single_empty_response_has_expected_contract` requires stat columns be NaN-present-not-dropped, which this plan honours by design: Phase 1 returns rows below `n_years_min` with `NaN` stats, not absent rows.

**Stat-column DB cleanup:** Stat-column DB cleanup is not in scope. If an operator wants a clean slate after a Phase 2 rollback, coordinate a one-shot SQL `UPDATE` with the preprocessing service maintainer (Maxat).

**CI and operator split:** Recalc and backfill run only via the operator wrapper, not from CI. Unit tests for Phases 1 and 2 are mock-only and run in CI under the existing `bash run_tests.sh preprocessing_gateway`.

**Wrapper naming:** Leave the `prepgw-snow-norm-recalc` container name as-is unless the operator asks for a rename; the historical name can cover the unified norm + stat recalculation.

## Decisions Committed

1. **`previous` and `current` at write time vs read time**

DEFAULT: Write `previous` and `current` in the recalc script.

RATIONALE: Hydrograph writes these fields during stat population before `_write_hydrograph_to_api`, preserving the API contract that comparison values live on each row. Snow has no forecast-run hook, so the closest equivalent is the yearly write-side recalc. Phase 2 computes these from the year-Y and year-(Y-1) snow rows it already fetches; Phase 1's helper is year-independent and intentionally does not return them. This keeps the dashboard reader simple and avoids a client-side fallback path.

2. **Partial-year handling**

DEFAULT: Compute percentile/stat columns only from complete historical years; for an incomplete target year, write only `current` from that year's raw value and `previous` from the prior year when available.

RATIONALE: Percentiles from partial years can distort the dashboard bands because winter/spring snow values and summer zeros are seasonally imbalanced. Keeping stats tied to complete years makes the bands climatological, while `current` still gives the dashboard the live-year line.

3. **Minimum-years threshold**

DEFAULT: `n_years_min = 5`; below threshold, return rows with `count` populated and stat columns as `NaN`, not absent rows.

RATIONALE: Hydrograph has no explicit threshold, and existing snow norms write whatever data exists. Snow station history can be short, though, and percentile bands from 1-2 years are visually overconfident. Five years is a conservative default that preserves row shape while preventing weak climatologies from masquerading as robust bands. Loosely consistent with WMO climatological-normals convention which prefers ≥10 years but tolerates shorter records when station history is limited; five is a pragmatic compromise for the snow network's variable history depth.

4. **Leap-year DOY alignment**

DEFAULT: Use the existing snow norm convention: `date.dt.dayofyear` grouping, including day 366 for leap years.

RATIONALE: `calculate_snow_norms_from_api` already groups by `dt.dayofyear` at `dg_utils.py:453`, and the recalc script builds target-year dates from the same DOY convention at `recalculate_snow_norms.py:122-166`. Matching snow norms is more important than importing hydrograph's pentad-specific leap handling from `forecast_library.py`.

5. **Initial backfill scope**

DEFAULT: Yes, run a one-time full-history backfill for all historical years and all discovered stations after implementation.

RATIONALE: The dashboard reads historical date windows, and Phase 0 already showed null fields in both 2024 and 2025-2026 windows. A current-year-only run would unblock only part of the display. Add an operator-side `bin/` script that loops years and calls the unified recalc with progress logging and resumability.

## Phase 0 - Design Decisions And Small Spike

**Goal:** Create the decision artifact and a read-only dev spike that verifies the API/data shape needed by later phases.

**Files:**
- `doc/plans/working/snow_stat_population_decisions.md`
- `apps/preprocessing_gateway/dev_code/probe_snow_stat_population_shape.py`
- If `apps/preprocessing_gateway/dev_code/` does not exist, the agent creates it, following the pattern in `apps/preprocessing_runoff/dev_code/`.

**Depends on:** None

**Agents:** 1 agent writes only the decision note and spike script.

**Acceptance criteria:**
- Decision artifact contains all five `DEFAULT:` and `RATIONALE:` blocks above.
- Spike confirms the helper input shape is long-format data with `code`, `date`, `value`, and `dayofyear`.
- Spike confirms one snow API row includes stat keys with `null` values rather than missing keys.
- Spike script reads the station code from `os.environ['PROBE_CODE']` only; redacts the code in any printed output. At script entry, assert `os.environ.get('PROBE_CODE', '').startswith('199') or os.environ.get('ALLOW_REAL_CODE')` so the spike refuses real codes by default unless the operator explicitly opts in via env var.

**Constraint sentence:** No production code changes. Do not run tests. Do not modify existing files outside the listed files.

**Expected before:**
- Design defaults live only in the planning prompt.
- API row shape is known from prior memo but not captured in a runnable dev probe.

**Expected after:**
- Implementation agents can cite one decision artifact.
- Spike can be run manually against a local stack before Phase 1.

## Phase 1 - New Aggregation Helper In `dg_utils.py`

**Goal:** Add `calculate_snow_stats_from_api(client, variables: list[str], n_years_min: int = 5) -> pd.DataFrame` beside `calculate_snow_norms_from_api`. Phase 1 helper is year-independent; `previous`/`current` are Phase 2's responsibility.

**Files:**
- `apps/preprocessing_gateway/dg_utils.py`
- `apps/preprocessing_gateway/test/test_snow_norms_from_api.py` - Extend the existing file by adding a new class `TestCalculateSnowStatsFromApi`. Do not rename the file. Do not modify the existing `TestCalculateSnowNormsFromApi` class.

**Depends on:** Phase 0

**Agents:** 1 implementation agent for helper plus focused tests.

**Acceptance criteria:**
- Helper paginates full snow history using the pattern at `dg_utils.py:402`.
- Output columns: `snow_type`, `code`, `dayofyear`, `count`, `mean`, `std`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`.
- Uses `dt.dayofyear` alignment and `n_years_min = 5`.
- Rows below threshold remain present with `count` and `NaN` stats.
- Empty input returns an empty typed DataFrame with the expected columns.
- Tests added:
  - `test_snow_stats_populates_columns_from_multiyear_history`
  - `test_snow_stats_threshold_keeps_rows_with_nan_stats`
  - `test_snow_stats_uses_existing_dayofyear_leap_alignment`
  - `test_snow_stats_empty_api_response_returns_typed_frame`

**Constraint sentence:** "Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**."

**Expected before:**
- `calculate_snow_norms_from_api` returns only `snow_type`, `code`, `dayofyear`, `norm`.
- No gateway helper computes percentiles.

**Expected after:**
- A separate stats helper returns dashboard stat bands without changing norm behavior.
- The stats helper remains year-independent and leaves `previous`/`current` to Phase 2.
- Existing norm tests still pass unchanged.

## Phase 2 - Extend Recalc Script To Write Stat Columns

**Goal:** Extend `recalculate_snow_norms.py` to compute stats in the same run, read snow rows for both year Y and year Y-1, compute `previous` / `current` per `(snow_type, code, date)`, preserve existing values/bands, and write stat fields through the existing snow upsert path. Use the existing `read_snow` call at `recalculate_snow_norms.py:143` as the integration point for the target-year read and add the prior-year read beside it. `previous` is looked up by **calendar date**: for target row `(snow_type, code, date=Y-MM-DD)`, `previous` = the `value` from row `(snow_type, code, date=(Y-1)-MM-DD)`. If the prior calendar date does not exist (e.g. `2024-02-29` -> `2023-02-29`), `previous = NaN`. This is distinct from DEFAULT 4's DOY rule, which governs Phase 1's climatology grouping only.

**Files:**
- `apps/preprocessing_gateway/recalculate_snow_norms.py`
- `apps/preprocessing_gateway/test/test_recalculate_snow_norms.py` - Extend the existing file by adding new test functions for the stat-record builder. Do not modify existing tests for the norm-only behaviour.

**Depends on:** Phase 1

**Agents:** 1 implementation agent.

**Acceptance criteria:**
- Script calls both `calculate_snow_norms_from_api` and `calculate_snow_stats_from_api`.
- Script reads year Y-1 snow rows in addition to year Y; missing prior-year rows produce `previous = NaN` without aborting.
- `previous` uses calendar-date alignment (year-1, same month, same day); rows where the prior calendar date does not exist have `previous = NaN` without aborting the run.
- Record builder adds `count`, `mean`, `std`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`, `current`.
- Preserve-then-overwrite invariant at `recalculate_snow_norms.py:174` remains intact for `value` and `value1..value14`.
- Existing `norm`-only behavior remains valid for current callers.
- Per-station write failures are logged and isolated.
- Tests use the existing `unittest.mock.patch` pattern in `test_recalculate_snow_norms.py`; no live API calls.
- Tests added:
  - `test_record_builder_includes_snow_stat_columns`
  - `test_record_builder_previous_uses_calendar_date_alignment`
  - `test_record_builder_writes_current_from_target_year_value`
  - `test_recalculate_stats_is_idempotent_on_rerun`
  - `test_preserves_existing_value_and_band_fields_when_writing_stats`
  - `test_station_write_error_does_not_abort_other_stations`

**Constraint sentence:** "Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**; additions to the record dict are allowed; do NOT modify the existing `norm`-only behaviour for callers that pass the existing flags."

**Expected before:**
- Script writes `norm` records only.
- Stat columns remain null after recalc.

**Expected after:**
- Same recalc invocation writes norms and stats.
- Re-running produces the same database state.

## Phase 3 - Local End-To-End Verification

**Goal:** Run the unified recalc against the local stack and capture evidence that the dashboard Phase 0 gate can proceed.

**Files:**
- `doc/plans/working/snow_stat_population_e2e_evidence.md`

**Depends on:** Phase 2

**Agents:** 1 verification agent.

**Acceptance criteria:**
- Evidence records exact recalc invocation.
- Evidence records non-null counts per snow type and per stat field.
- At least one station has non-null values for all ten dashboard stat fields.
- File includes `READY: YES` or `READY: NO` for unblocking the dashboard plan.
- Record wall-clock duration of the unified recalc so Phase 4 can cite it.
- If recalc fails or fields remain null, agent escalates instead of patching code.

**Constraint sentence:** No production code changes. Evidence only.

**Expected before:**
- Dashboard Phase 0 evidence says `DECISION: STOP`.
- Snow stat fields are null in checked windows.

**Expected after:**
- Local evidence shows whether the dashboard branch can resume cleanly.
- Any failure is routed back to Phase 2 follow-up work.

## Phase 4 - Documentation Update

**Goal:** Document the unified snow norm/stat recalculation cadence and operator invocation.

**Files:**
- `apps/preprocessing_gateway/README.md`
- `doc/deployment.md` if the agent finds a natural yearly-maintenance section
- `bin/yearly_snow_norm_recalculation.sh`

**Depends on:** Phase 3

**Agents:** 1 documentation agent.

**Acceptance criteria:**
- Docs explain when to run the recalc, required env/API inputs, expected duration/log location, and relationship to existing yearly snow norm cadence.
- README script table includes `recalculate_snow_norms.py`.
- Operator guidance mentions the dashboard dependency.
- Wrapper's docstring block (lines ~4-9) reflects the unified norm + stat scope.
- Crontab comment (line ~23) updated if its wording is norm-only.
- Keep `LOG_DIR` as `snow_norm_recalc` (historical name from the norm-only era) and add a one-line docstring note that the directory now covers the unified norm + stat recalculation.
- `prepgw-snow-norm-recalc` container name: leave as-is unless the operator asks for a rename; flag in Coordination.

**Constraint sentence:** Documentation-only changes. Do not modify code.

**Expected before:**
- README describes gateway scripts but does not document snow stat recalculation.
- Operator script comments mention norms only.

**Expected after:**
- Operators know the yearly invocation and why stats are included.
- Dashboard dependency is discoverable.

## Phase 5 - One-Time Full-History Backfill

**Goal:** Add an operator-runnable wrapper that backfills snow stat columns for all historical years and all discovered stations.

**Files:**
- `bin/backfill_snow_stats_history.sh`

**Depends on:** Phase 3

**Agents:** 1 implementation agent.

**Acceptance criteria:**
- Wrapper loops target years and invokes the unified recalc per year.
- On year-N completion, append the year to a stamp file at `${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill/backfill_progress.txt`. On resume, parse the stamp file and skip years already listed there.
- Wrapper sets `LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill"` at startup and runs `mkdir -p "${LOG_DIR}"` before writing the progress file. The directory is a sibling of the existing `snow_norm_recalc` log directory used by `bin/yearly_snow_norm_recalculation.sh:48`.
- Script does not include real station codes or sensitive data.
- Backfill produces non-null stat columns for all stations/years that satisfy `n_years_min`.

**Constraint sentence:** "Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**."

**Expected before:**
- Current-year recalc can populate one target year.
- Historical dashboard windows remain partially null unless manually looped.

**Expected after:**
- Operators have a repeatable one-time historical backfill entry point.
- Interrupted backfills can resume from logged progress.

## Dependency Graph

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```

## Test Summary

| Phase | Test files added/modified | Headline assertions |
|---|---|---|
| P0 | None | Dev spike only; no tests |
| P1 | `apps/preprocessing_gateway/test/test_snow_norms_from_api.py` (extended; do not rename) | Stats columns, threshold behavior, leap DOY convention, empty typed output |
| P2 | `apps/preprocessing_gateway/test/test_recalculate_snow_norms.py` (extended; do not rename) | Stat record fields, idempotency, preserve existing values/bands, station error isolation |
| P3 | None | Local evidence file records post-recalc non-null counts |
| P4 | None | Documentation-only |
| P5 | Optional shell smoke/manual check | Backfill logging and resumability |

## Risks & Rollback

P0 risk is leaking local station codes in spike output; rollback by deleting the artifact and regenerating with redaction.

P1 risk is changing norm behavior while adding stats; rollback by reverting only `dg_utils.py` helper additions and related tests.

P2 risk is clobbering existing `value`, `norm`, or band values. Code revert is safe; populated stat columns remain in the DB and are harmless because the dashboard treats null stats as no-data and any values written by the helper are valid statistics from real historical `value` rows. The data is not reverted by the code revert. If an operator later wants to clear the columns (e.g. to re-test the empty-stats path), that is a SQL operation against the preprocessing DB and a coordination ask with the service maintainer (Maxat) - it is **not** part of this plan.

P3 risk is local stack/data mismatch; do not patch in Phase 3, record `READY: NO`, and send the failure back to Phase 2.

P4 risk is stale operator guidance; rollback doc edits only.

P5 risk is long-running or interrupted backfill; use progress logs for resume, and stop the wrapper without database schema changes.

## Out Of Scope

No `sapphire/services/` changes.

No `apps/forecast_dashboard/` changes.

No `apps/iEasyHydroForecast/forecast_library.py` changes.

No client-side/dashboard computation fallback for `previous`, `current`, or percentiles.
