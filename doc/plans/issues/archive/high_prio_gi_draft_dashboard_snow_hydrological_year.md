# High Priority - Dashboard Snow Hydrological-Year Display

## Overview

The dashboard already reads `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD` and passes it to snow plotting, but snow API fetches still use the fixed `PREVIOUS_YEAR-01-01` to `CURRENT_YEAR-12-31` range in `apps/forecast_dashboard/src/db.py:255`. This truncates September-start seasons after September 1. The fix is to share the display-window logic, use it for snow fetch bounds, and make snow legend labels season-aware only when display start is not `01-01`.

## Coordination

1. `bin/utils/common_functions.sh:280` and `:291` have the same cosmetic cleanup exit-1 bug when `$ieasyhydroforecast_ssh_tunnel_pid` is unbound under `set -u`. Out of scope; file separately.
2. Future true hydrological-year `previous` / `current` semantics would touch `apps/preprocessing_gateway/recalculate_snow_norms.py` and `apps/preprocessing_gateway/dg_utils.py`. This plan keeps calendar-date semantics.
3. Daily runoff/hydrograph plots remain calendar-year aligned. Future hydrological-year wording there is a separate product decision.
4. This supersedes `doc/plans/working/snow_field_population_check.md:88` `DATE_WINDOW_DECISION: KEEP`. The fetch range is now display-window-derived per D-Q2; the earlier evidence file remains historical.

## Decisions

**Decisions (User)**

- **D1 (semantics):** Keep `current_year` / `last_year` on calendar-date alignment. No changes to `recalculate_snow_norms.py` or `previous` / `current` field semantics.
- **D2 (fetch window):** `_get_snow_single()` derives `start_date` / `end_date` from `snow_display_window()` instead of the fixed `PREVIOUS_YEAR-01-01` / `CURRENT_YEAR-12-31` pattern.
- **D3 (climatology grouping):** Stay calendar-DOY. `calculate_snow_stats_from_api()` in `apps/preprocessing_gateway/dg_utils.py` is not changed.
- **D4 (labels):** Switch legend labels to season-aware wording when display start is not `01-01`; add i18n keys in both `en_CH` and `ru_KG`.

**Decisions Committed (Planner Defaults)**

- **D-Q1 DEFAULT:** Move snow window logic into new `apps/forecast_dashboard/src/snow_window.py` as public `snow_display_window(...)`.
  **RATIONALE:** Avoids `src/db.py` importing heavy visualization code and lets tests import the real helper.

- **D-Q2 DEFAULT:** Thread optional display-start values from `DashboardConfig` through `DataManager`, `db.get_data(...)`, horizon loaders, `get_snow_data(...)`, and `_get_snow_single(...)`.
  **RATIONALE:** `dashboard/config.py:45` remains the single env-parse source. `plot_manager.py:505-506` already threads config to `viz.plot_daily_snow_data()`; only data-fetch side needs threading. `plot_manager.py` is intentionally absent from Phase 1 Files.

- **D-Q3 DEFAULT:** Add optional `ref_date` / `snow_ref_date` parameters for deterministic tests.
  **RATIONALE:** Production callers pass `None`; `_get_snow_single` resolves to `date.today()`. `get_snow_data` snapshots the date once before HS / ROF / SWE sub-fetches. Future tickets may thread dashboard date-picker semantics if needed; out of scope here.

- **D-Q4 DEFAULT:** Use `Current season {YYYY}/{YY+1}` and `Previous season {YYYY}/{YY+1}`, e.g. `Current season 2025/26`.
  **RATIONALE:** Slash notation is compact and ASCII-only. Current season is determined from the latest non-null current snow curve date in the displayed window, falling back to `date_picker`; previous season is one season earlier. If neither exists, fall back to `display_begin` year.

- **D-Q5 DEFAULT:** If display start is `01-01`, preserve "Current year" / "Last year". Add public `is_hydrological_year_display(month, day) -> bool` in `apps/forecast_dashboard/src/snow_window.py`.
  **RATIONALE:** Co-locating the predicate with `snow_display_window(...)` avoids duplicate `(1, 1)` checks; public naming wins for clarity.

## Phase 0 - Design Decisions Artifact

**Goal:** Create `doc/plans/working/snow_hydrological_year_decisions.md` recording D1-D4 and D-Q1-D-Q5.

**Files:** `doc/plans/working/snow_hydrological_year_decisions.md`

**Depends on:** None

**Agents:** 1 documentation agent.

**Acceptance criteria:** Artifact contains all decisions and rationales; no production code changes.

**Constraint Sentence:** No code changes. Do not run tests. Write only the decision artifact.

**Expected Behaviour Before:** Decisions exist only in planner context.

**Expected Behaviour After:** Implementation agents have one decision artifact to cite.

## Phase 1 - Shared Window Helper And Fetch Widening

**Goal:** Relocate snow display-window calculation and make `_get_snow_single()` fetch the configured display window.

**Files:**
- `apps/forecast_dashboard/src/snow_window.py`
- `apps/forecast_dashboard/src/db.py`
- `apps/forecast_dashboard/src/vizualization.py`
- `apps/forecast_dashboard/dashboard/data_manager.py`
- `apps/forecast_dashboard/forecast_dashboard.py`
- `apps/forecast_dashboard/tests/test_snow_display_window.py`
- `apps/forecast_dashboard/tests/test_db.py`

**Depends on:** Phase 0

**Agents:** 1 worktree-isolated implementation agent.

**Acceptance criteria:**
- `vizualization.py` imports `snow_display_window(...)`; local helper is removed.
- `snow_window.py` owns `snow_display_window(...)` and `is_hydrological_year_display(...)`.
- `_get_snow_single(..., display_start_month=1, display_start_day=1, ref_date=None)` derives fetch params from `snow_display_window(...)`.
- Existing callers still work with calendar-year defaults.
- `test_get_snow_single_fetches_through_next_year_after_sept_1`: `2026-09-15`, `09-01` fetches `2026-09-01` to `2027-08-31`.
- `test_get_snow_single_fetches_only_current_hydroyear_in_spring`: `2026-03-15`, `09-01` fetches `2025-09-01` to `2026-08-31`, narrower than the old range, with the 14-column snow contract intact.
- Existing snow contract tests in `test_db.py:111`, `:143`, and `:189` still pass.
- Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`; zero failures and zero unexpected skips. Expected pass count: 279, except only `SAPPHIRE_API_AVAILABLE` dependency-gated skips are tolerated.
- Also run gated kghm verification:
  `SAPPHIRE_TEST_ENV=True SAPPHIRE_SNOW_STATS_AVAILABLE=true ieasyhydroforecast_env_file_path=$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm bash run_tests.sh forecast_dashboard`.
  Confirm `test_integration.py:1880` still passes; if any station becomes null-only, escalate to a scoped Phase 1.x fix.

**Constraint Sentence:** Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**. `_get_snow_single()`'s existing callers that don't supply the new display-start parameters must continue to work with calendar-year defaults.

**Expected Behaviour Before:** Snow fetches use fixed previous/current calendar years and can miss next-calendar-year season dates.

**Expected Behaviour After:** Snow fetch range matches active display window; defaults remain calendar-year compatible.

## Phase 2 - Season-Aware Snow Plot Labels

**Goal:** Switch `current_year` and `last_year` snow curve labels to season labels when display start is not `01-01`.

**Files:**
- `apps/forecast_dashboard/src/vizualization.py`
- `apps/forecast_dashboard/tests/test_snow_plot.py`

**Depends on:** Phase 1

**Agents:** 1 worktree-isolated implementation agent.

**Acceptance criteria:**
- `vizualization.py` imports `is_hydrological_year_display(...)`.
- Calendar start keeps existing labels.
- Hydrological start uses gettext keys `Current season {season}` and `Previous season {season}`.
- Predictor-mean suffix composition at `vizualization.py:2337` stays unchanged after the season-label gettext call.
- Add `test_snow_plot_labels_use_calendar_year_wording_when_start_is_jan_1`.
- Add `test_snow_plot_labels_use_season_wording_when_start_is_sept_1`, including the degenerate empty synthetic-frame fallback.
- Add `test_snow_plot_season_year_label_transitions_at_start_day`: with `09-01`, `ref_date=2025-08-31` yields labels containing `Previous season 2024/25` and `Current season 2024/25`; `ref_date=2025-09-01` yields `Previous season 2024/25` and `Current season 2025/26`.
- Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`; zero failures and zero unexpected skips. Expected pass count: 282, except only `SAPPHIRE_API_AVAILABLE` dependency-gated skips are tolerated.

**Constraint Sentence:** Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**. `_get_snow_single()`'s existing callers that don't supply the new display-start parameters must continue to work with calendar-year defaults.

**Expected Behaviour Before:** September-start plots still say "Current year" and "Last year".

**Expected Behaviour After:** September-start plots say season labels; January-start plots are unchanged.

## Phase 3 - i18n Catalogue Updates

**Goal:** Add season label gettext keys and compile locale binaries.

**Files:**
- `apps/config/locale/messages.pot`
- `apps/config/locale/en_CH/LC_MESSAGES/forecast_dashboard.po`
- `apps/config/locale/en_CH/LC_MESSAGES/forecast_dashboard.mo`
- `apps/config/locale/ru_KG/LC_MESSAGES/forecast_dashboard.po`
- `apps/config/locale/ru_KG/LC_MESSAGES/forecast_dashboard.mo`

**Depends on:** Phase 2

**Agents:** 1 catalogue agent.

**Acceptance criteria:**
- Add `Current season {season}` and `Previous season {season}` to template and both `.po` files.
- English msgstrs match msgids.
- Russian msgstrs are `Текущий сезон {season}` and `Предыдущий сезон {season}` unless operator requests different wording.
- `msgfmt` exits 0 for both locales.
- Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`.

**Constraint Sentence:** Documentation-only changes to catalogues. Do not modify code.

**Expected Behaviour Before:** New season keys fall back to untranslated msgids.

**Expected Behaviour After:** Season labels round-trip through gettext in English and Russian.

## Dependency Graph

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 }
  }
}
```

## Test Summary

| Phase | Tests added/modified | Headline assertions |
|---|---|---|
| P0 | None | Written decision artifact only |
| P1 | `test_snow_display_window.py`, `test_db.py` | Real helper imported; Sep-1 autumn and spring fetch bounds correct; snow contract unchanged |
| P2 | `test_snow_plot.py` | Jan-1 labels unchanged; Sep-1 labels season-aware; transition test covers Aug 31 / Sep 1 |
| P3 | gettext compile commands | New keys exist in `.pot` / `.po`; `.mo` files compile cleanly |

## Risks & Rollback

- **P0:** Artifact drift. Roll back only `snow_hydrological_year_decisions.md`.
- **P1:** Threading could miss a loader. Roll back `snow_window.py`, `db.py`, `vizualization.py`, `data_manager.py`, `forecast_dashboard.py`, plus `test_snow_display_window.py` and `test_db.py` together. Reverting without `vizualization.py` breaks the import at startup.
- **P2:** Label helper could pick the wrong season when data are sparse. Roll back `vizualization.py` and `test_snow_plot.py`.
- **P3:** Stale `.mo` binaries could mismatch `.po`. Re-run `msgfmt` or revert all locale files.

## Out-of-Scope

- No changes to `apps/preprocessing_gateway/`.
- No changes to `apps/preprocessing_gateway/dg_utils.py::calculate_snow_stats_from_api`.
- No hydrological-DOY grouping mode.
- No new operator env vars beyond `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD`.
- No edits under `sapphire/services/`.
- No redefinition of snow `previous` / `current`.
- No Phase 4 integration-test expansion.
