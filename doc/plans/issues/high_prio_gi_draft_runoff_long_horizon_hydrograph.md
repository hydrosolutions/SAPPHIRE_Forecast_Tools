# Runoff Long-Horizon Hydrograph Plan

## Overview

This plan fills the preprocessing write-side gap for long-horizon runoff
hydrographs by producing the monthly and seasonal `norm + previous + current`
triad. Monthly norms continue to come from the existing iEasyHydro HF SDK norm
call, while monthly `previous` and `current` are computed from daily SAPPHIRE
runoff records. Seasonal rows are derived from the newly complete monthly
hydrograph rows for April-September only. Quarter hydrograph writes,
service/API schema changes, and dashboard read-side wiring remain out of scope.

## Coordination

1. **Daily SAPPHIRE runoff coverage check:** Because the default monthly
   discharge source is the local SAPPHIRE daily runoff store, confirm before
   Phase 1 dispatch that daily `runoff` records exist for at least target year
   `Y` and prior year `Y-1` for the station set. This guards against the same
   "schema fields exist but are unpopulated" class of issue documented in memory
   `[[snow-stat-fields-write-gap]]`.

2. **iEH HF SDK monthly-discharge endpoint discovery:** Not a phase gate. The
   plan does not depend on an SDK monthly-discharge endpoint. The only iEH HF
   SDK long-horizon method already pinned by current code is
   `get_norm_for_site(code, "discharge", norm_period="m")`, called by
   `write_month_hydrograph_data` at
   `apps/iEasyHydroForecast/forecast_library.py:5411-5415`.

3. **Dashboard handoff:** Dashboard read-side wiring is deliberately out of
   scope. Phase 5 captures a forward-pointer for a downstream plan to update
   `_get_data_monthly` and `_get_data_season`, which currently return empty
   hydrograph overlay DataFrames per the memo.

4. **Code organization and retirement decision:** Add a new sibling script,
   `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`, and keep
   all new monthly/seasonal runoff hydrograph writes there. Retire the old
   norm-only runoff path in Phase 4 by removing
   `YearlyMonthlyNormsRecalculation` from
   `apps/pipeline/pipeline_docker.py:2040-2045`, removing the
   `"monthly_norms"` dispatcher entry at `apps/pipeline/pipeline_docker.py:2090`,
   and marking `sync_monthly_norms.py` deprecated or deleting it. Snow yearly
   recalculation at `apps/pipeline/pipeline_docker.py:2034` stays untouched.

5. **Operator wrapper decision:** Add a new sibling wrapper,
   `bin/yearly_runoff_hydrograph_aggregation.sh`, instead of extending
   `bin/yearly_snow_norm_recalculation.sh`. Snow and runoff use different
   source data, timing, logs, and failure modes.

## Decisions Committed (User)

- **D1 (aggregation formula)**: arithmetic mean of monthly values. Season norm =
  mean of 6 monthly norms; season `previous` = mean of 6 monthly `previous`;
  season `current` = mean of 6 monthly `current`.
- **D2 (monthly previous/current source)**: mean discharge per month, computed
  from discharge history. For a month-Y row, `previous` = mean discharge across
  all days in month-Y in year Y-1; `current` = mean discharge across all days in
  month-Y in year Y. For the in-progress current month (i.e.
  `target_year == today.year` and `target_month == today.month`), `current = None`
  regardless of days-so-far. Only completed months receive `current`. A month is
  complete when `(target_year, target_month) < (today.year, today.month)`. Locked
  in by `test_current_is_none_for_in_progress_month` in Phase 1.
- **D3 (season window)**: April-September (vegetation / high-flow), six months.
- **D4 (aggregation location)**: in `apps/preprocessing_runoff/`. The module
  already gathers data from iEH HF and writes the hydrograph table; the new
  aggregations join the same pipeline.

## Decisions Committed (Planner Defaults)

1. **Q-1 monthly mean discharge source: DEFAULT A, local SAPPHIRE daily runoff
   aggregation.** Rationale: the current long-horizon SDK usage only pins
   monthly norms through `get_norm_for_site(..., norm_period="m")`
   (`apps/iEasyHydroForecast/forecast_library.py:5379-5381`,
   `apps/iEasyHydroForecast/forecast_library.py:5411-5415`). No confirmed
   monthly-discharge SDK endpoint is present in the existing path. Aggregating
   daily SAPPHIRE runoff records keeps the implementation inside
   `apps/preprocessing_runoff/`, uses already-synced data, and avoids adding
   operator env vars.

2. **Q-2 cadence: DEFAULT A, yearly maintenance cadence.** Rationale: existing
   `sync_monthly_norms.py` is designed to run once a year
   (`apps/preprocessing_runoff/sync_monthly_norms.py:12`). Norms change rarely,
   and a yearly long-horizon writer avoids making every operational run perform
   broad daily-runoff reads. Operators may still rerun with an explicit target
   year for backfill or repair.

3. **Q-3 code organization: DEFAULT hybrid by responsibility.** Rationale:
   create `sync_long_horizon_hydrograph.py` in `apps/preprocessing_runoff/` for
   the complete monthly triad and seasonal aggregation. Retire the old
   `sync_monthly_norms.py` path in Phase 4 so there is only one live monthly
   runoff hydrograph writer. This keeps long-horizon runoff logic in one
   preprocessing-runoff module while avoiding changes to the iEasyHydroForecast helper, whose
   `_write_hydrograph_to_api` accepts only `pentad`, `decade`, and `month`
   (`apps/iEasyHydroForecast/forecast_library.py:3431-3444`).

4. **Q-4 missing previous/current behavior: DEFAULT C.** Rationale: write the
   station/month row when norm data exists, populate whichever of `previous` or
   `current` can be computed, and leave the missing field as `None`. This avoids
   silently dropping stations and matches the API writer pattern of serializing
   missing numeric fields as `None`
   (`apps/iEasyHydroForecast/forecast_library.py:3517-3526`).

5. **Q-5 operator wrapper: DEFAULT new sibling wrapper.** Rationale:
   `bin/yearly_runoff_hydrograph_aggregation.sh` should mirror the deployment
   style of `bin/yearly_snow_norm_recalculation.sh` without coupling runoff
   hydrograph aggregation to the snow recalculation job. The snow wrapper
   currently owns snow-specific log naming, container naming, and command text
   (`bin/yearly_snow_norm_recalculation.sh:47-54`,
   `bin/yearly_snow_norm_recalculation.sh:102-130`).

## Phase 0a -- Decisions Artifact

**Goal:** Commit the five planner defaults above to
`doc/plans/working/runoff_long_horizon_hydrograph_decisions.md`. No production
code.

**Files:**
- `doc/plans/working/runoff_long_horizon_hydrograph_decisions.md`

**Depends on:** None

**Agents:** 1 documentation agent.

**Expected behavior before:**
- Implementation agents must infer defaults from the plan and investigation
  memo.
- Q-1 through Q-5 are not captured in a small canonical artifact.

**Expected behavior after:**
- Implementation agents cite the decisions artifact as the single source of
  truth.
- The artifact states daily-runoff aggregation, yearly cadence, new
  long-horizon script, missing-field behavior, and new runoff wrapper.

**Acceptance criteria:**
- File exists and contains D1-D4 plus Q-1 through Q-5.
- No code files changed.
- No references to quarter writes, service schema changes, or dashboard
  implementation work.

**Constraint sentence:** No production code changes in this phase.

## Phase 0b -- Coverage & Audit Probe

**Goal:** Produce
`doc/plans/working/runoff_long_horizon_hydrograph_coverage_probe.md` recording
read-only evidence for two gates before implementation dispatch: daily SAPPHIRE
runoff coverage for target year `Y` and prior year `Y-1`, and a grep audit of
all app-side writers that produce hydrograph rows with `horizon_type` in
`(month, season)`. This is the explicit guard against the
`[[snow-stat-fields-write-gap]]` pitfall where schema fields exist but are not
populated.

**Probe target.** The coverage check queries the preprocessing API endpoint
`GET /runoff/?horizon=day&code=<station>&start_date=YYYY-01-01&end_date=YYYY-12-31`
once per station per year for each station in the planned set, for both `Y`
and `Y-1`. The probe records the returned row count and the count of records
with non-null `value` per station-year.

**Coverage threshold.** Non-null daily runoff must cover **≥80% of expected
days** for each `(station, year)` pair in `{Y, Y-1}`. The denominator depends
on whether the year is complete:

- **Complete year** (`year < today.year`): denominator = 365 (or 366 for a
  leap year). 2024 was leap; 2025 / 2026 are not.
- **In-progress year** (`year == today.year`): denominator = days elapsed
  so far in the year (i.e. `today.timetuple().tm_yday`, which gives 153 on
  2026-06-02). This avoids penalising a year for days that haven't happened
  yet — the probe initially BLOCKED in round 1 of P0b because of the
  fixed-365 denominator on the in-progress year (see commit `6d5c81a`
  evidence). Future years (`year > today.year`) are not part of the
  `{Y, Y-1}` set so this case does not arise.

Any `(station, year)` below the threshold (under whichever denominator
applies) causes `DISPATCH: BLOCKED` with the failing pairs enumerated in
the evidence file. This threshold is plan-pinned rather than agent-picked
so the dispatch gate is operationally enforceable.

**Files:**
- `doc/plans/working/runoff_long_horizon_hydrograph_coverage_probe.md`

**Depends on:** Phase 0a

**Agents:** 1 verification/documentation agent.

**Expected behavior before:**
- Daily runoff coverage for `Y` and `Y-1` is assumed rather than evidenced.
- Other possible monthly/season hydrograph writers have not been audited.

**Expected behavior after:**
- The probe records read-only live API evidence (per the probe target above)
  with the iEH HF tunnel up.
- The probe records a grep audit over `apps/` for month/season hydrograph
  writers, using the concrete command
  `rg -nP '"(month|season)"|horizon_type\s*=\s*["\']?(month|season)|write_hydrograph\b' apps/`.
  Every match must be manually triaged in the evidence file. Any narrowing of
  the pattern (e.g. excluding a file as a false positive) must be justified in
  the evidence file inline next to the excluded match. Expected result: only
  `sync_monthly_norms.py`, which is retired in Phase 4, plus the new writer
  planned here. If other writers surface, halt and escalate.
- The evidence file ends with exactly one dispatch line:
  `DISPATCH: PROCEED` or `DISPATCH: BLOCKED - <reason>`.

**Acceptance criteria:**
- Probe runs read-only against the live API
  (`GET /runoff/?horizon=day&code=<station>&start_date=YYYY-01-01&end_date=YYYY-12-31`)
  with the tunnel up.
- Evidence file records non-null daily count and percentage per
  `(station, year)` for `{Y, Y-1}`, with the denominator chosen per the
  threshold definition above (full year for complete years; days-elapsed
  for the in-progress year). Each pair is checked against the plan-pinned
  **≥80%** threshold and failing pairs are enumerated.
- Grep audit ran the plan-pinned command (`rg -nP '"(month|season)"...'`
  above) over `apps/` and triaged every match in the evidence file.
- Phase 1 must not dispatch unless the final dispatch line is
  `DISPATCH: PROCEED`.

**Constraint sentence:** No production code changes in this phase.

## Phase 1 -- Monthly Previous/Current Writer

**Goal:** Add a monthly long-horizon writer so every monthly hydrograph record
carries `norm + previous + current`. Norms come from the iEH HF SDK monthly norm
endpoint already used today; `previous` and `current` come from daily SAPPHIRE
runoff aggregation over target year `Y` and `Y-1`.

**Files:**
- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
- `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`

**Depends on:** Phase 0b

**Agents:** 1 implementation agent.

**Expected behavior before:**
- `sync_monthly_norms.py` delegates to `write_month_hydrograph_data`
  (`apps/preprocessing_runoff/sync_monthly_norms.py:216-223`).
- `write_month_hydrograph_data` writes 12 monthly norm-only rows
  (`apps/iEasyHydroForecast/forecast_library.py:5367-5373`,
  `apps/iEasyHydroForecast/forecast_library.py:5429-5438`).
- Monthly rows persist `previous=None` and `current=None` because the shared
  helper only derives those fields from 4-digit year columns
  (`apps/iEasyHydroForecast/forecast_library.py:3446-3456`,
  `apps/iEasyHydroForecast/forecast_library.py:3517-3526`).

**Expected behavior after:**
- The new long-horizon writer builds 12 records per station for
  `horizon_type="month"`.
- Each record has `norm` from `get_norm_for_site(code, "discharge",
  norm_period="m")`.
- Each record has `previous` as the mean daily discharge for the same month in
  `Y-1` when available.
- Each record has `current` as the mean daily discharge for the same month in
  `Y` when available.
- Missing one year leaves only that field as `None`; the row is not skipped if
  norm exists.

**Implementation notes:**
- Do not edit `apps/iEasyHydroForecast/forecast_library.py` in this phase. The
  new writer uses `SapphirePreprocessingClient.write_hydrograph` directly per
  D-Q3. If the direct client is somehow insufficient, escalate to the
  orchestrator rather than touching `forecast_library.py`. The retire decision
  means the old path is removed in Phase 4; no edit to
  `write_month_hydrograph_data` is required.
- Every numeric record field (`norm`, `previous`, `current`) must pass through an
  `isfinite` JSON-safety helper in `sync_long_horizon_hydrograph.py` before
  `client.write_hydrograph`. The helper maps `NaN`, `+inf`, `-inf`, and `None`
  to `None`, matching the snow `_json_safe` pattern from `2793b62`. Do not put
  this helper in `forecast_library.py` or the shared client. Rationale:
  `SapphirePreprocessingClient.write_hydrograph` is a thin pass-through, and
  existing `pd.notna` checks allow infinities that can make FastAPI reject the
  whole batch.
- Use `SAPPHIRE_API_URL` / `SAPPHIRE_API_ENABLED` behavior already established
  in preprocessing runoff; do not add operator env vars.
- Monthly dates remain `YYYY-MM-01`, `horizon_value=month`,
  `horizon_in_year=month`, and mid-month `day_of_year` matching the existing
  monthly norm helper (`apps/iEasyHydroForecast/forecast_library.py:5402-5438`).

**Acceptance criteria:**
- Mocked-API unit test covers a normal station with complete target and
  prior-year daily runoff; written monthly records contain expected mean
  `previous` and `current`.
- Mocked-API unit test covers one-year-missing behavior; the available field is
  written and the missing field is `None`.
- `test_current_is_none_for_in_progress_month` covers the D2 rule that the
  current month itself gets `current=None` regardless of days-so-far.
- `test_writes_none_when_daily_series_contains_nan` uses mocked daily runoff
  with `NaN` and asserts the written `previous` or `current` field is `None`,
  not `NaN`, with no exception and no 422. Repeat the same assertion for
  `+inf` and `-inf`.
- `test_idempotent_writes_with_identical_upstream`: with the daily runoff API
  mock and the iEH HF SDK norm mock returning identical data across two
  consecutive writer invocations, the second run produces identical
  `(norm, previous, current)` per row AND the second run triggers zero
  `Updated hydrograph` log lines from the service-side
  `crud.create_hydrograph` (i.e. `_has_changes` returns False on every row).
  This is a determinism invariant; the test must mock identical inputs. The
  service upsert key is `(horizon_type, code, date)`
  (`sapphire/services/preprocessing/app/crud.py:88-129`).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes after the phase.

**Constraint sentence:** "Do NOT change any existing function signatures, data
flow logic, or control flow beyond the scope listed in **Files** above. Your
changes must be purely additive or modify only the specific behaviour described
in **Goal**."

## Phase 2 -- Season Aggregation Writer

**Goal:** Add seasonal aggregation that reads monthly hydrograph records for
April-September and writes one `horizon_type="season"` row per `(code, year)`
with `norm`, `previous`, and `current` as arithmetic means of the six monthly
values.

**Files:**
- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
- `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`

**Depends on:** Phase 1

**Agents:** 1 implementation agent.

**Expected behavior before:**
- The preprocessing enum accepts `season`
  (`sapphire/services/preprocessing/app/models.py:6-13`), and the shared
  hydrograph model already has `norm`, `previous`, and `current`
  (`sapphire/services/preprocessing/app/models.py:41-78`).
- No seasonal hydrograph writer exists in `apps/preprocessing_runoff`.
- The iEasyHydroForecast helper rejects `season`
  (`apps/iEasyHydroForecast/forecast_library.py:3431-3444`).

**Expected behavior after:**
- The long-horizon writer reads or receives monthly triad rows for months 4-9.
- It writes one season row per station/year with `date=YYYY-04-01`,
  `horizon_value=1`, `horizon_in_year=1`, and `day_of_year` matching April 1
  unless implementation chooses another documented season marker.
- `horizon_in_year=1` for season records is a sentinel meaning "the one-and-only
  April-September season", NOT an ordinal position. Future code MUST NOT
  interpret it ordinally.
- `norm`, `previous`, and `current` are each computed independently from the six
  monthly values.
- If any of the six monthly values for a field is missing, that seasonal field
  is `None`; other complete fields can still be written.

**Acceptance criteria:**
- Mocked test gives Apr-Sep monthly triads and asserts seasonal `norm`,
  `previous`, and `current` equal the arithmetic means.
- Mocked test covers missing monthly values and asserts only the affected
  seasonal field is `None`.
- Mocked test covers `NaN`, `+inf`, and `-inf` in monthly inputs and asserts the
  affected seasonal numeric field is written as `None`, with no exception and no
  422.
- Mocked test asserts posted records use `horizon_type="season"` and stable
  `(horizon_type, code, date)` identity matching the model unique constraint
  (`sapphire/services/preprocessing/app/models.py:75-79`).
- Idempotency test simulates rerun/upsert behavior.
- No edits to `sapphire/services/`.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes after the phase.

**Constraint sentence:** "Do NOT change any existing function signatures, data
flow logic, or control flow beyond the scope listed in **Files** above. Your
changes must be purely additive or modify only the specific behaviour described
in **Goal**."

## Phase 3 -- Local End-to-End Verification

**Goal:** Run the long-horizon writer against the user's local stack with the
iEH HF SSH tunnel up and capture evidence that monthly and seasonal triad fields
are populated through `/hydrograph/`.

**Files:**
- `doc/plans/working/runoff_long_horizon_hydrograph_e2e_evidence.md`

**Depends on:** Phase 2

**Agents:** 1 verification agent.

**Expected behavior before:**
- Local `/hydrograph/?horizon=month` records may contain monthly norms but lack
  `previous` and `current`.
- Local `/hydrograph/?horizon=season` may be empty.

**Expected behavior after:**
- Local probes show non-null `previous` and `current` for month and season rows
  for at least the HS station codes populated by local backfill/sync.
- Evidence file records commands, timestamps, row counts, and representative
  null/non-null assertions without committing sensitive data. All station codes
  that appear in the evidence file MUST be replaced with the project sentinel
  `19999`. Cites `[[feedback-no-real-station-codes]]`.

**Acceptance criteria:**
- Evidence file confirms the local stack was reachable and the iEH HF tunnel was
  active.
- Evidence file shows monthly records with non-null `previous` and `current`.
- Evidence file shows seasonal records with non-null `previous` and `current`.
- If a station fails, the agent documents a scoped follow-up; do not relax
  assertions globally.
- No code changes in this phase.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes after verification unless
  the orchestrator explicitly limits Phase 3 to live-stack probing.

**Constraint sentence:** No production code changes in this phase.

## Phase 4 -- Operator Wrapper Update and Old Path Retirement

**Goal:** Add the yearly runoff hydrograph aggregation wrapper so operators can
run monthly and seasonal triad aggregation in the yearly maintenance window, and
retire the old norm-only monthly runoff path so only the new writer remains live.

**Files:**
- `bin/yearly_runoff_hydrograph_aggregation.sh`
- `apps/preprocessing_runoff/README.md`
- `apps/pipeline/pipeline_docker.py`
- `apps/preprocessing_runoff/sync_monthly_norms.py`

**Depends on:** Phase 3

**Agents:** 1 implementation agent.

**Expected behavior before:**
- `bin/yearly_snow_norm_recalculation.sh` exists for snow-specific annual
  recalculation (`bin/yearly_snow_norm_recalculation.sh:1-24`).
- There is no runoff-specific yearly wrapper for long-horizon hydrograph
  aggregation.
- `YearlyMonthlyNormsRecalculation` and the `"monthly_norms"` dispatcher key
  still expose the old norm-only runoff path
  (`apps/pipeline/pipeline_docker.py:2040-2090`).
- `apps/preprocessing_runoff/README.md` describes the main preprocessing script
  but not the long-horizon hydrograph aggregation entry point
  (`apps/preprocessing_runoff/README.md:17-26`).

**Expected behavior after:**
- A new runoff wrapper runs the long-horizon hydrograph script in the same
  deployment style as existing yearly wrappers.
- Help/banner text clearly states that it writes monthly and seasonal runoff
  hydrograph triads.
- README documents the script, cadence, prerequisites, and local SSH tunnel/API
  expectations.
- `YearlyMonthlyNormsRecalculation` and the `"monthly_norms"` dispatcher key are
  removed from `apps/pipeline/pipeline_docker.py`.
- `sync_monthly_norms.py` is deleted or clearly marked deprecated in its header.
- No snow wrapper behavior changes.

**Acceptance criteria:**
- `bash -n bin/yearly_runoff_hydrograph_aggregation.sh` passes.
- `shellcheck -x bin/yearly_runoff_hydrograph_aggregation.sh` passes.
- Help/banner text includes monthly and seasonal hydrograph triad wording.
- `grep -q 'yearly_runoff_hydrograph_aggregation' apps/preprocessing_runoff/README.md`
  exits 0.
- README mentions no new env vars beyond those already used by
  preprocessing/API setup.
- Grep assertions confirm `YearlyMonthlyNormsRecalculation` and
  `sync_monthly_norms.py` have no live references after retirement.
- Deletion-or-deprecation check confirms `sync_monthly_norms.py` is either gone
  or has a clear deprecated header pointing operators to
  `sync_long_horizon_hydrograph.py`.
- Snow yearly task is byte-identical: `YearlySnowNormRecalculation`, its
  `command=["uv", "run", "recalculate_snow_norms.py"]`, and snow wrapper
  behavior are not touched.
- `test_yearly_monthly_norms_task_retired` is added as a unit test or static
  check confirming `pipeline_docker.py` no longer instantiates
  `YearlyMonthlyNormsRecalculation` and the `"monthly_norms"` dispatcher key is
  gone.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes after the phase.

**Constraint sentence:** "Do NOT change any existing function signatures, data
flow logic, or control flow beyond the scope listed in **Files** above. Your
changes must be purely additive or modify only the specific behaviour described
in **Goal**."

## Phase 5 -- Dashboard Handoff Stub

**Goal:** Capture a forward-pointer for a downstream plan that wires monthly and
seasonal hydrograph triads into the dashboard reader.

**Files:**
- `doc/plans/working/runoff_long_horizon_hydrograph_dashboard_handoff.md`

**Depends on:** Phase 3

**Agents:** 1 documentation agent.

**Expected behavior before:**
- Dashboard read-side work is known but not captured as a next plan artifact.

**Expected behavior after:**
- A short handoff note names `_get_data_monthly` and `_get_data_season` as the
  downstream scope.
- It explicitly states that quarter remains out of scope for preprocessing
  writes.

**Acceptance criteria:**
- Handoff file exists.
- No production code changes in this phase.
- It does not instruct implementation agents to edit `apps/forecast_dashboard/`
  in this plan.

**Constraint sentence:** No production code changes in this phase.

## Dependency Graph

```json
{
  "phases": {
    "P0a": { "depends_on": [], "parallel_agents": 1 },
    "P0b": { "depends_on": ["P0a"], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0b"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```

P4 and P5 can run in parallel after P3 completes. Both depend only on P3 and
touch disjoint file sets: P4 edits `bin/yearly_runoff_hydrograph_aggregation.sh`,
the README, `apps/pipeline/pipeline_docker.py`, and the retired monthly-norms
script; P5 edits only the dashboard handoff doc.

## Test Summary

| Phase | Tests | Headline assertions |
|---|---|---|
| P0a | Documentation review | Decisions artifact contains D1-D4 and Q-1 through Q-5; no code changes |
| P0b | Read-only live API probe plus grep audit | Evidence file records daily runoff coverage threshold/results and all month/season hydrograph writers; final dispatch line is `DISPATCH: PROCEED` or `DISPATCH: BLOCKED - <reason>` |
| P1 | Mocked unit tests in `test_sync_long_horizon_hydrograph.py` | Monthly records include norm, previous, current; missing one year leaves only that field `None`; current month gets `current=None`; non-finite values write as `None`; identical upstream rerun is deterministic |
| P2 | Mocked unit tests in `test_sync_long_horizon_hydrograph.py` | Apr-Sep seasonal triad equals arithmetic mean of six monthly fields; missing or non-finite monthly field yields seasonal `None`; horizon is `season` |
| P3 | Local live-stack probe plus evidence artifact | `/hydrograph/` returns month and season records with non-null previous/current for locally populated stations |
| P4 | `bash -n`, `shellcheck -x`, full test suite | New wrapper is syntactically clean, shellcheck clean, and documented |
| P5 | Documentation review | Dashboard follow-up is captured without starting dashboard implementation |

## Risks & Rollback

| Phase | Risk | Rollback |
|---|---|---|
| P0a | Decisions artifact drifts from this plan | Replace artifact with the Q-1 through Q-5 defaults above |
| P0b | Coverage probe blocks dispatch or discovers another live writer | Do not dispatch Phase 1; document blocker and escalate with the probe evidence |
| P1 | Daily runoff coverage is incomplete, producing many `None` values | Revert Phase 1 files; run daily runoff backfill/maintenance; rerun Phase 1 after coverage evidence |
| P1 | New writer accidentally duplicates old norm-only path behavior | Remove only the new long-horizon script and tests; old path retirement remains isolated to Phase 4 |
| P2 | Seasonal partial-data handling is misinterpreted | Revert Phase 2 additions; clarify whether season fields require six complete months or may use skip-null means |
| P3 | Local stack/tunnel issues block verification | Keep code changes, document environment failure, and dispatch a scoped infra verification follow-up |
| P4 | Wrapper or retirement breaks existing snow maintenance | Revert the P4 changes; restore the byte-identical snow yearly task and re-apply only runoff retirement/wrapper edits |
| P5 | Handoff note accidentally expands this plan into dashboard work | Replace with a one-paragraph pointer only |

## Out-of-Scope

- **Quarter hydrograph triad**: no quarter records are stored or written. The
  preprocessing enum lacks `quarter`
  (`sapphire/services/preprocessing/app/models.py:6-13`), and the reservoir
  quarter card reads monthly data through upstream PR #341.
- **API/schema changes**: no edits under `sapphire/services/`. The shared
  `Hydrograph` table already has `norm`, `previous`, and `current`
  (`sapphire/services/preprocessing/app/models.py:70-73`), and the service
  already exposes shared `/hydrograph/` POST/GET endpoints
  (`sapphire/services/preprocessing/app/main.py:101-136`).
- **Dashboard read-side changes**: no edits under `apps/forecast_dashboard/`.
  Monthly and seasonal loader wiring is a downstream plan.
- **Quarter forecast display**: already handled upstream; do not add a parallel
  preprocessing path.
