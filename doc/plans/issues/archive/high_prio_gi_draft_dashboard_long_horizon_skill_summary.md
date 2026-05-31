# Dashboard long-horizon skill metrics missing from summary table

| Field | Value |
|---|---|
| Module | `forecast_dashboard` |
| Priority | High |
| Status | **Completed (2026-05-31)** |
| Branch | `fix_postprocessing_skill_metrics` |
| Labels | `bug`, `dashboard`, `forecast_dashboard`, `skill-metrics`, `tajik-deployment` |

## Completion Summary

Implemented on branch `fix_postprocessing_skill_metrics` alongside PP-036. Five commits across four phases:

| Commit | Phase | Content |
|---|---|---|
| `ba4dee8` | P1 | Tests-first: quarter/season period keys, stats renames, guarded merges, partial-coverage (LR_Base/LR_SM split), replacement of obsolete `test_forecast_stats_is_empty` locking tests |
| `0f96d1e` | P2 | `apps/forecast_dashboard/src/db.py` data-layer fix: `_horizon_in_year_col` cases for quarter/season, `quarter_in_year = ((valid_from.dt.month - 1) // 3 + 1)`, `season_in_year = 1` (constant by design), `get_forecast_stats(...)` fetch + merge in `_get_data_quarter` / `_get_data_season` |
| `dd2da96` | P3 | Integration assertion: season summary table renders populated `accuracy`, `delta`, `sdivsigma`, `mae` for LR_Base / LR_SM |
| `85d6aeb` | P4a | Tests for `_get_data_monthly()` enrichment of `long_forecasts_quarter` (the data path the reservoir quarterly card consumes on the month tab). Includes monthly `forecasts_all` byte-identity regression guard |
| `65e9662` | P4b | `_get_data_monthly()` quarter skill merge into `long_forecasts_quarter`, reusing the existing `can_merge` guard and merge kwargs (+19/-1 in `db.py`) |

**Final test run:** 288 passed, 0 failed, 0 unexpected skips.

**Red-phase verifications:**

- **2026-05-31 (P1-P3):** reverting `0f96d1e` produced 18 expected failures across P1 + P3 assertions; no Type-II errors; no non-fix-related failures.
- **2026-05-31 (P4):** reverting `65e9662` produced exactly 3 P4a failures (all `KeyError: 'delta'` on `long_forecasts_quarter`), all P1-P3 tests stayed green, no Type-II errors. The unrelated Playwright `errors` observed in some runs are macOS sandbox browser-launch issues tracked under [`low_prio_gi_draft_infra_playwright_sandbox_browser_launch.md`](low_prio_gi_draft_infra_playwright_sandbox_browser_launch.md).

**Deferred to deployment time:** operator runs the plan's operational check on a real stack — for season: navigate to the season tab for a station with seasonal LR_Base skill data and observe populated metric columns in the summary table; for reservoir quarter: navigate to the month tab for a reservoir station with quarter LR_Base skill data and observe populated metric columns on the quarterly card. Discover stations via `/api/postprocessing/skill-metric/?horizon={season|quarter}&model_type=LR_Base&limit=5`.

**Decisions worth preserving:**

- **Reservoir quarter on month tab was the actual Tajik visible-impact target for the quarter side.** The original plan's framing — "quarter is hidden in the UI, so the quarter data-layer fix is preemptive plumbing" — was wrong. Quarter is rendered on the month tab for reservoirs via `long_forecasts_quarter` (commit `1b6715c`). P4 was added 2026-05-31 to close this gap, caught after P1-P3 red-phase verification by user review.
- **Quarter-tab exposure remains deferred.** `dashboard/widgets.py:89-96` was not modified. The quarter tab is still hidden; the quarter data-layer fix in `_get_data_quarter` lands proactively for whenever the quarter tab is exposed in a future plan.
- **Dedicated skill chart / skill table for long horizons was deferred.** `create_skill_table()` and `plot_forecast_skill()` are still pentad/decade-only (`vizualization.py:4637-4647`); enabling them for long horizons requires formatter and period-key work beyond this fix.
- **Mixed-fixture station-code transition (`19999` for new tests; existing `99001` fixtures preserved)** is intentional and documented in the plan body. Consolidation is a separate cleanup if desired.

## Summary

Quarter and season skill metrics exist in the postprocessing DB, but the
dashboard data layer did not merge them into long-horizon forecast frames. The
visible wins are seasonal summary-table rows for `LR_Base` and `LR_SM`, plus
the reservoir quarterly card rendered on the month tab via
`long_forecasts_quarter`, showing `accuracy`, `delta`, `sdivsigma`, and `mae`
once the data layer supplies those columns.

## Scope

In scope:
- Render skill metrics in the summary table for `season` and `quarter` horizons
  by fixing the dashboard data layer.
- Add quarter data-layer support for both the hidden quarter tab path and the
  visible reservoir quarterly card on the month tab. The card consumes
  `long_forecasts_quarter`, which is populated by `_get_data_monthly()`.

Out of scope:
- Do not expose quarter in `apps/forecast_dashboard/dashboard/widgets.py:89-96`;
  quarter-tab exposure remains deferred, but the existing reservoir
  quarter-on-month-tab card is in scope for P4.
- Do not edit `sapphire/services/`.
- Do not edit `_read_long_forecasts_api()`, the skill-metric writer, or the
  recalculation pipeline.
- Do not add dependencies, compatibility shims, feature flags, or opt-out
  toggles.

## Investigation Anchors

- `_horizon_in_year_col()` currently maps only decade and month; quarter and
  season fall through to `pentad_in_year`
  (`apps/forecast_dashboard/src/db.py:32-37`).
- `_horizon_in_year_col()` has 13 current call sites in
  `apps/forecast_dashboard/src/db.py`:
  - `db.py:189`: declares the empty `get_hydrograph_pentad_all()` period-key
    column.
  - `db.py:197`: renames hydrograph API `horizon_in_year` to the local
    period-key column.
  - `db.py:327`: declares the empty `get_linreg_predictor()` period-key
    column.
  - `db.py:332`: renames LR predictor API `horizon_in_year` to the local
    period-key column.
  - `db.py:339`: sets `hin` for `get_forecasts_all()` forecast merge/schema
    handling.
  - `db.py:433`: declares empty `get_forecast_stats()` skill-metric schema.
  - `db.py:437`: renames skill-metric API `horizon_in_year` in
    `get_forecast_stats()`.
  - `db.py:442`: deduplicates `get_forecast_stats()` on
    `(code, period_key, model_short)`.
  - `db.py:471`: declares empty `get_forecast_stats_all()` schema when no
    pages are returned.
  - `db.py:478`: declares empty `get_forecast_stats_all()` schema when
    `model_type` is absent.
  - `db.py:482`: renames skill-metric API `horizon_in_year` in
    `get_forecast_stats_all()`.
  - `db.py:487`: deduplicates `get_forecast_stats_all()` on
    `(code, period_key, model_short)`.
  - `db.py:644`: sets `hin` for the generic short-horizon `get_data()` branch.
- No-regression argument: None of these call sites is reached today with
  `horizon="quarter"` or `horizon="season"`. The router in `get_data()` at
  `db.py:637-642` dispatches long horizons to `_get_data_monthly` /
  `_get_data_quarter` / `_get_data_season` before the generic branch at
  `db.py:644`. The helpers `get_forecasts_all`, `get_hydrograph_pentad_all`,
  `get_linreg_predictor`, and `get_forecast_stats_all` are not invoked with
  quarter or season by any current caller — verified by reading
  `plot_manager.py:198-203` and `plot_manager.py:424`. Line-number drift:
  the reviewer cited `db.py:639-642`; current code has the month dispatch at
  `db.py:637-638` and quarter/season dispatch at `db.py:639-642`.
- The month skill-metric rename site is already generic:
  `get_forecast_stats()` renames `horizon_in_year` to
  `_horizon_in_year_col(horizon)` at
  `apps/forecast_dashboard/src/db.py:436-438`, and
  `get_forecast_stats_all()` mirrors it at
  `apps/forecast_dashboard/src/db.py:481-482`. Once the helper knows quarter
  and season, both functions work for those horizons without public signature
  changes; their empty schemas and dedup keys also call the same helper at
  `apps/forecast_dashboard/src/db.py:432-442` and
  `apps/forecast_dashboard/src/db.py:468-488`.
- The monthly working template computes `month_in_year` from `valid_from` at
  `apps/forecast_dashboard/src/db.py:528-529` and merges stats into
  `forecasts_all` with a `can_merge` guard at
  `apps/forecast_dashboard/src/db.py:685-703`.
- Quarter and season long forecasts currently compute only `month_in_year`
  (`apps/forecast_dashboard/src/db.py:569-570`,
  `apps/forecast_dashboard/src/db.py:615-616`) and `_get_data_quarter()` /
  `_get_data_season()` return literal empty `forecast_stats`
  (`apps/forecast_dashboard/src/db.py:737-765`).
- Existing dashboard unit coverage belongs in
  `apps/forecast_dashboard/tests/test_db.py`: helper mapping at lines 47-58,
  month skill rename at lines 175-210, monthly merge at lines 216-338,
  quarter/season long-forecast coverage at lines 381-436, and current
  quarter/season `get_data()` tests at lines 442-591.
- The summary renderer already preserves or creates the four metric columns for
  long horizons at `apps/forecast_dashboard/src/vizualization.py:3082-3099`; no
  rendering code change is needed for the summary table.
- `_get_data_monthly()` loads monthly forecasts into `forecasts_all` and, for
  every station, loads quarterly forecasts into `long_forecasts_quarter` via
  `get_long_forecasts_quarter(station, horizon_value=1)` at
  `apps/forecast_dashboard/src/db.py:685-741`. The existing month merge only
  enriches `forecasts_all`; it does not enrich `long_forecasts_quarter`.
- The reservoir quarterly card on the month tab is rendered by
  `apps/forecast_dashboard/dashboard/plot_manager.py:322-389`. It reads
  `DataManager.long_forecasts_quarter`, gates display on `horizon == "month"`
  and Russian reservoir marker `вдхр`, filters by `station_labels`, then calls
  `create_forecast_summary_tabulator()` with the filtered quarterly frame.
  Therefore the data-layer fix can be unconditional; reservoir visibility is a
  rendering-layer concern.
- `DataManager.long_forecasts_quarter` is a thin accessor over the data key at
  `apps/forecast_dashboard/dashboard/data_manager.py:163-165`.
- Current `apps/forecast_dashboard/tests/test_db.py` covers month merges into
  `forecasts_all`, optional `long_forecasts_m0` merges, quarter/season period
  keys, and `_get_data_quarter()` / `_get_data_season()` skill merges. It does
  not assert that `_get_data_monthly()` enriches `long_forecasts_quarter`.

## Optional Dedicated Skill Table Decision

Defer. The guards in `apps/forecast_dashboard/dashboard/plot_manager.py:197-203`
and `apps/forecast_dashboard/dashboard/plot_manager.py:423-428` are small, but
the formatter is not long-horizon ready: `create_skill_table()` maps every
non-pentad horizon to `decad_in_year` / `decad_in_month`
(`apps/forecast_dashboard/src/vizualization.py:4637-4647`) and then calls
`add_month_pentad_per_month_to_df()` (`apps/forecast_dashboard/src/vizualization.py:4648-4652`).
That is not evidence that quarter `n_pairs`, `accuracy`, or season period keys
render correctly without new formatter logic. Leave the guards intact.

## Phase P1 - Tests First

**Goal**: Add failing tests that lock down quarter/season period keys, stats
renames, guarded merges, no-skill behavior, and existing month/pentad/decade
behavior before implementation.

**Files allowed**:
- `apps/forecast_dashboard/tests/test_db.py`

**Forbidden**:
- `sapphire/services/`
- Public API signature changes
- Any file outside the allow-list
- Any production-code change

**Depends on**: none

**Agents**: 1 Sonnet 4.6 general-purpose agent, isolation: `worktree`.

**Agent instructions**:
- Do NOT change any existing function signatures, data flow logic, or control flow.
- Add tests only. New quarter/season test fixtures use station code `19999`.
  Do not modify the existing `99001` fixtures elsewhere in `tests/test_db.py`
  — they cover unrelated cases and migrating them is out of scope for this
  fix. Test files may contain both codes during this transition; consolidate
  to `19999` in a separate cleanup if desired.
- Mock API responses; do not call the real API. Use fixed date strings in
  fixtures; do not introduce `date.today()`.
- Model the `/skill-metric/` payload with the live 18-column schema confirmed
  by the investigator. It must include API `horizon_in_year` and the
  dashboard-relevant fields `model_type`, `model_type_description`,
  `sdivsigma`, `nse`, `delta`, `accuracy`, `mae`, and `n_pairs`.
- Replace test_forecast_stats_is_empty at tests/test_db.py:476-487 (Quarter)
  and :552-563 (Season) with assertions that forecast_stats is populated and
  that forecasts_all carries delta, sdivsigma, mae, accuracy for the matching
  (code, period_key, model_short) row.
- Test where skill data exists for LR_Base but not LR_SM. Assert: the LR_SM
  forecast row is preserved with NaN values in delta, sdivsigma, mae,
  accuracy; the LR_Base forecast row carries populated metric values; the
  can_merge guard does not collapse the LR_SM row.
- Add named failing assertions for:
  - `_horizon_in_year_col("quarter") == "quarter_in_year"` and
    `_horizon_in_year_col("season") == "season_in_year"`.
  - `get_forecast_stats("quarter", "19999")` and
    `get_forecast_stats("season", "19999")` rename API `horizon_in_year` to
    the horizon-specific column and do not expose `pentad_in_year`.
  - `get_forecast_stats_all("quarter")` and `get_forecast_stats_all("season")`
    page and rename through the same helper.
  - `get_long_forecasts_quarter("19999")` includes `quarter_in_year`; derive
    April `valid_from` as quarter 2.
  - `get_long_forecasts_season("19999")` includes `season_in_year == 1`.
  - `get_data("quarter", "19999", ...)` and `get_data("season", "19999", ...)`
    merge metrics into `forecasts_all` for matching `LR_Base` and `LR_SM` rows.
  - No-skill responses for a station/model leave forecasts available, do not
    crash, and do not require metric columns to exist before the summary
    renderer adds NaNs.
  - Existing month tests still assert the same `month_in_year` behavior and
    metric merge; pentad and decade helper mapping remains unchanged. The
    existing assertions at `tests/test_db.py:50-58`
    (`_horizon_in_year_col("pentad") == "pentad_in_year"`,
    `("decade") == "decad_in_year"`) must continue to pass without
    modification — name them explicitly as must-not-weaken regression guards.

**Acceptance criteria**:
- Running the new/changed tests against current code fails because quarter and
  season map to `pentad_in_year`, quarter/season long forecasts lack their
  period keys, and `_get_data_quarter()` / `_get_data_season()` return empty
  `forecast_stats`.
- Existing month assertions are not weakened.
- No production files are modified.

## Phase P2 - Data Layer Implementation

**Goal**: Make quarter and season data paths supply skill metrics to
`forecasts_all` using the same guarded merge pattern as month.

**Files allowed**:
- `apps/forecast_dashboard/src/db.py`

**Forbidden**:
- `sapphire/services/`
- Public API signature changes
- Any file outside the allow-list
- `apps/forecast_dashboard/dashboard/widgets.py`
- `_read_long_forecasts_api()`, skill-metric writer code, and recalculation
  pipeline code

**Depends on**: P1

**Agents**: 1 Sonnet 4.6 general-purpose agent, isolation: `worktree`.

**Agent instructions**:
- Do NOT change any existing function signatures, data flow logic, or control flow.
- Modify only the specific behavior described here.
- Add explicit `_horizon_in_year_col()` cases for `quarter` and `season`.
- In `get_long_forecasts_quarter()`, add `quarter_in_year` from
  `valid_from.dt.month` using calendar quarters. Use the formula
  `((valid_from.dt.month - 1) // 3 + 1)` so April → 2, July → 3, etc.
  Preserve existing output fields unless tests require only additive changes.
- In `get_long_forecasts_season()`, add `season_in_year = 1`. The value is
  constant by design: season is a single-bucket horizon in the data model
  (one season per year-station-model combination), so the period key is
  always 1. This is not a bug or placeholder — do not parameterize.
  Preserve existing output fields unless tests require only additive changes.
- Ensure empty quarter/season long-forecast DataFrames declare the new period
  key columns.
- In `_get_data_quarter()` and `_get_data_season()`, fetch
  `get_forecast_stats("quarter", station)` / `get_forecast_stats("season", station)`,
  pass through `i18n_models`, and merge into `forecasts_all` on
  `["code", _horizon_in_year_col(horizon), "model_short"]` with the same
  `can_merge` guard used by month.
- Do not add `date.today()` or equivalent business-logic date defaults.

**Acceptance criteria**:
- P1 tests pass.
- `get_forecast_stats()` and `get_forecast_stats_all()` return
  `quarter_in_year` / `season_in_year` for those horizons because the existing
  `horizon_in_year` rename now resolves correctly.
- Quarter and season no-skill paths return forecast data without crashing.
- Month behavior remains unchanged; pentad/decade behavior remains unchanged.

## Phase P3 - Rendering Verification

**Goal**: Assert that the season summary table renders the merged skill columns
for `LR_Base` and `LR_SM`, with no summary-renderer code change expected.

**Files allowed**:
- `apps/forecast_dashboard/tests/test_db.py`
- `apps/forecast_dashboard/tests/test_integration.py`

**Forbidden**:
- `sapphire/services/`
- Public API signature changes
- Any file outside the allow-list
- Production rendering code

**Depends on**: P2

**Agents**: 1 Sonnet 4.6 general-purpose agent, isolation: `worktree`.

**Agent instructions**:
- Do NOT change any existing function signatures, data flow logic, or control flow.
- Prefer a deterministic mocked integration assertion that builds season data
  through `db.get_data("season", "19999", ...)` and passes the resulting
  `forecasts_all` into `vizualization.create_forecast_summary_table()`.
- Assert the rendered table has populated `Accuracy`, `δ`, `s/σ`, and `MAE`
  cells for `LR_Base` and `LR_SM`.
- If extending the existing Playwright suite, reuse the current
  `apps/forecast_dashboard/tests/test_integration.py` long-horizon flow; do
  not invent a new browser harness. Keep all new deterministic fixtures on
  station `19999`.
- Keep the dedicated skill chart/table assertions skipped for long horizons.

**Acceptance criteria**:
- The summary-table assertion proves that merged season skill metrics survive
  the data layer and renderer.
- The no-skill rendering path still produces a summary table with NaN/empty
  metric cells rather than a crash.
- Existing long-horizon Playwright behavior is not weakened.

## Phase P4a - Reservoir Quarter-On-Month Tests

**Goal**: Add failing data-layer tests proving `_get_data_monthly()` enriches
`long_forecasts_quarter` with quarter skill metrics for the reservoir quarterly
card path while preserving existing monthly behavior.

P4 was added 2026-05-31 after a user-flagged gap in scope: the quarterly card
on the month tab (commit 1b6715c, reservoir-only rendering) consumes
`long_forecasts_quarter` but the prior P2 fix only addressed
`_get_data_quarter`.

**Files allowed**:
- `apps/forecast_dashboard/tests/test_db.py`
- A new `apps/forecast_dashboard/tests/test_*.py` file only if
  `test_db.py` becomes unwieldy

**Forbidden**:
- `sapphire/services/`
- Public API signature changes
- Any file outside the allow-list
- Any production-code change
- Rendering-layer code

**Depends on**: P2. P3 is not a hard prerequisite, although it has already
landed in practice.

**Agents**: 1 Sonnet 4.6 general-purpose agent, isolation: `worktree`.

**Agent instructions**:
- Do NOT change any existing function signatures, data flow logic, or control flow.
- Add tests first. Use station code `19999` for all new fixtures and assertions.
  Leave existing `99001` fixtures elsewhere unchanged.
- Mock API responses; do not call the real API. Use fixed date strings in
  fixtures; do not introduce `date.today()`.
- Test `_get_data_monthly()` / `get_data("month", "19999", ...)`, not the
  rendering layer. Do not model reservoir flags; the data key is loaded for
  every station and reservoir filtering happens later in plot/bulletin code.
- Add a test where monthly skill rows and quarter skill rows are both present.
  Assert:
  - `forecasts_all` still carries the same monthly metric columns and values as
    before P4, proving the existing month merge is unchanged.
  - `long_forecasts_quarter` carries `delta`, `sdivsigma`, `mae`, and
    `accuracy` for matching quarter `LR_Base` rows.
  - The quarter merge key is `quarter_in_year`, and the month merge for
    `LR_Base` still uses `month_in_year`.
- Add a partial-coverage test where quarter skill rows exist for `LR_Base` but
  not for another quarter model emitted for the station. Assert the unmatched
  forecast row is preserved with NaN values in `delta`, `sdivsigma`, `mae`, and
  `accuracy`.
- Add a no-quarter-skill test. Follow existing month no-skill behavior: if the
  quarter stats frame is empty and the guarded merge does not run,
  `long_forecasts_quarter` must preserve forecast rows and need not contain
  metric columns.
- Add an empty-quarter-forecast test. Assert empty `long_forecasts_quarter`
  does not crash `_get_data_monthly()` and does not create degenerate merged
  rows.
- The tests must fail on current post-P3 code because
  `long_forecasts_quarter` lacks merged quarter metric columns while
  `forecasts_all` continues to pass the monthly assertions.

**Acceptance criteria**:
- P4a tests fail pre-P4b in named ways tied to missing quarter metrics on
  `long_forecasts_quarter`.
- Existing month, quarter-tab, and season tests are not weakened.
- No production files are modified.

## Phase P4b - Monthly Data-Layer Quarter Merge

**Goal**: Wire quarter skill metrics into `_get_data_monthly()` for the
`long_forecasts_quarter` frame using the P2 infrastructure.

**Files allowed**:
- `apps/forecast_dashboard/src/db.py`

**Forbidden**:
- `sapphire/services/`
- Public API signature changes
- Any file outside the allow-list
- Rendering-layer code
- `_get_data_quarter()` or `_get_data_season()` changes
- Helper extraction or broad refactors

**Depends on**: P4a and P2. P3 is not a hard prerequisite, although it has
already landed in practice.

**Agents**: 1 Sonnet 4.6 general-purpose agent, isolation: `worktree`.

**Agent instructions**:
- Do NOT change any existing function signatures, data flow logic, or control flow.
- Modify only `_get_data_monthly()`.
- After loading `long_forecasts_quarter`, fetch
  `get_forecast_stats("quarter", station)` and pass it through `i18n_models`.
- Merge quarter skill metrics into `long_forecasts_quarter` with the same
  `can_merge` guard pattern already used for monthly `forecasts_all` and
  optional `long_forecasts_m0`.
- Use `_horizon_in_year_col("quarter")` for the quarter period key and merge on
  `["code", quarter_key, "model_short"]`.
- Keep the existing monthly `forecast_stats` value and monthly merges
  unchanged. Do not reuse month stats for quarter forecasts.
- Do not add `date.today()` or equivalent business-logic date defaults.

**Acceptance criteria**:
- P4a tests pass post-P4b and fail pre-P4b.
- `_get_data_monthly()` returns `long_forecasts_quarter` with `delta`,
  `sdivsigma`, `mae`, and `accuracy` populated when quarter skill rows exist
  for the station/model.
- Station without quarter skill data preserves `long_forecasts_quarter`
  forecasts without crashing.
- Empty `long_forecasts_quarter` does not crash or produce degenerate rows.
- Existing P1+P2+P3 tests continue to pass without modification.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`
  passes with zero unexpected skips.
- Operational acceptance for deploy engineer: navigate to the month tab for a
  reservoir station on a running stack with quarter `LR_Base` skill data,
  confirm the quarterly card displays populated skill columns. Discover a
  suitable station via
  `/api/postprocessing/skill-metric/?horizon=quarter&model_type=LR_Base&limit=5`;
  do not hard-code or document a real station code.

## Whole-Fix Acceptance

- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes with
  zero failures and zero unexpected skips.
- Operational check: open the dashboard on a running local stack, pick any
  station on the running stack that has `LR_Base`/`LR_SM` seasonal forecasts
  (query `/api/postprocessing/skill-metric/?horizon=season&model_type=LR_Base&limit=5`
  to find one), navigate to the season tab for that station, and observe that
  `accuracy`, `delta`, `sdivsigma`, and `mae` columns are populated in the
  summary table for `LR_Base` and `LR_SM` rows. Do not commit the real code
  anywhere.
- Operational check: on the same running stack, query
  `/api/postprocessing/skill-metric/?horizon=quarter&model_type=LR_Base&limit=5`
  to discover a reservoir station with quarter skill data, navigate to its
  month tab, and observe that the quarterly card shows populated skill columns.
  Do not name or commit a real station code.

## Risks And Deferred Work

- Quarter remains hidden in the dashboard horizon selector
  (`apps/forecast_dashboard/dashboard/widgets.py:89-96`), so direct quarter-tab
  exposure remains deferred. However, quarter forecasts are already visible on
  the month tab for reservoir stations via `long_forecasts_quarter`; P4 covers
  that existing visible path.
- The dedicated skill chart and dedicated skill table remain intentionally
  deferred for long horizons. `plot_forecast_skill()` and `create_skill_table()`
  are pentad/decade-specific today, and the existing plot-manager guards should
  stay intact.
- Adding a second skill-metric API call inside `_get_data_monthly()` doubles
  skill-metric requests for the month tab. The volume is small (per-station,
  one extra call), so this is acceptable but should be watched if the month tab
  is later bulk-loaded.
- The quarterly card on the month tab is reservoir-only at the rendering layer;
  the data-layer quarter merge runs for every station. Extra metric columns on
  non-reservoir quarterly frames are inert.
- The tests must not rely on live Tajik/Kyrgyz station codes. Use `19999` for
  automated fixtures and reserve real station discovery for the operational
  check only.
- Shared-helper mutation risk: _horizon_in_year_col is shared by 13 call sites
  across src/db.py. Today none of them is invoked with quarter or season (see
  caller enumeration in Investigation Anchors). If a future caller passes
  either horizon expecting pentad_in_year (the prior default), behavior will
  silently change. Reviewers of any future code that adds a quarter/season
  call site must consult this helper.
- Pagination ceiling: get_forecast_stats (src/db.py:421-444) is single-page
  with limit=1000 and no pagination loop. Per-station quarter/season skill
  volumes are well below this today (≤ ~50 rows per station), so the fix is
  correct as designed. If per-station skill rows ever exceed 1000 (e.g., from
  finer horizon-in-year granularity or per-day instead of per-period rows),
  the dashboard will silently see a subset.

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4a": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4b": { "depends_on": ["P4a", "P2"], "parallel_agents": 1 }
  }
}
```
