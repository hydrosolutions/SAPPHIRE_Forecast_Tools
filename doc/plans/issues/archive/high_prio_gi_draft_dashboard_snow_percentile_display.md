# High priority - Dashboard snow percentile display

## Overview

Enrich the forecast dashboard snow plots for SWE, HS, and RoF so they match the daily runoff Hydrograph card: current year plus norm/mean, last year, full min-max envelope, p5-p95 band, and p25-p75 band. The chosen strategy is Option A: consume the existing `/snow/` `SnowResponse` statistical fields rather than deriving a new service contract. The main risk is that the schema exposes `mean`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`, and `current`, but the investigator could not confirm these fields are populated operationally. Therefore Phase 0 is a hard gate before implementation.

## Coordination

1. **Population of snow stat fields**: confirm with the write-side owner (preprocessing-gateway maintainer — `<TBD: name, to be filled in before P0 dispatch>`; relevant code: `apps/preprocessing_gateway/dg_utils.py` snow writer at `:667`/`:708` and `apps/preprocessing_gateway/recalculate_snow_norms.py`) whether `q05/q25/q50/q75/q95/min/max/mean/previous/current` are populated for operational snow data. Schema support exists (`sapphire/services/preprocessing/app/models.py:144`), but the investigator found no confirmed population path beyond `value`, `norm`, and `value1..value14`.

2. **`norm` vs `mean` semantics**: decide which field is the canonical long-term mean for snow plots. The dashboard currently extracts and plots `norm` in `plot_daily_snow_data()` at `apps/forecast_dashboard/src/vizualization.py:2309` and `apps/forecast_dashboard/src/vizualization.py:2367`; hydrograph-style plotting uses `mean` at `apps/forecast_dashboard/src/vizualization.py:1836`. Phase 2 uses `mean` if non-null and falls back to `norm`, but the long-term semantic decision is still owed back as a follow-up.

3. **PR #341 ordering**: prefer landing PR #341 first if convenient, but treat it as a soft ordering preference only. PR #341 modifies the same functions but not the same lines as the snow callsites, so line-number drift is expected; verify no logic conflict during rebase. Line numbers are approximate; re-run `git diff` before rebase.

4. **Investigator memo location**: this plan was prepared from the investigator findings embedded in the working prompts. If a standalone investigator memo exists elsewhere, add its path here before dispatching implementation; otherwise Phase 0's written evidence carries extra weight.

5. **Write-side owner**: identify the preprocessing/gateway owner responsible for `/preprocessing/snow/` stat-field population before filing any "stop and escalate" ticket.

## Phase 0 - Verify Field Population

**Goal**

Confirm against a real `/snow/` response whether `{mean, min, max, q05, q25, q50, q75, q95, previous, current}` are non-null for at least one station and recent date range. This phase produces a written go/no-go note only.

**Files**

- `doc/plans/working/snow_field_population_check.md`

**Depends on**

- None

**Agents**

- 1 agent: Run the read-only API verification, summarize populated/missing fields, and write the go/no-go note.

**Verification command**

Use a non-sensitive station code supplied by the operator via environment variable:

```bash
SNOW_CHECK_API_BASE="${SNOW_CHECK_API_BASE:-http://localhost:8000/api}" \
SNOW_CHECK_CODE="${SNOW_CHECK_CODE:?set a non-sensitive station code}" \
python - <<'PY'
import os
import requests

api_base = os.environ["SNOW_CHECK_API_BASE"].rstrip("/")
code = os.environ["SNOW_CHECK_CODE"]
fields = ["mean", "min", "max", "q05", "q25", "q50", "q75", "q95", "previous", "current"]

for snow_type in ["HS", "ROF", "SWE"]:
    resp = requests.get(
        f"{api_base}/preprocessing/snow/",
        params={
            "snow_type": snow_type,
            "code": code,
            "start_date": "2025-01-01",
            "end_date": "2026-12-31",
            "limit": 10000,
        },
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json()
    print(f"\n{snow_type}: {len(rows)} rows")
    if len(rows) == 0:
        raise SystemExit(
            f"{snow_type}: zero rows for code={code}; retry with a known-active "
            "code/window before declaring stats unpopulated"
        )
    for field in fields:
        count = sum(row.get(field) is not None for row in rows)
        print(f"  {field}: {count} non-null")
PY
```

**Decision Tree**

- If all statistical fields are populated for HS, ROF, and SWE, proceed to Phase 1.
- If partially populated, add optional **Phase 0.5** before Phase 1: compute missing statistics client-side in `get_snow_data()` from per-DOY history fetched via a widened date range. This is Option B as a fallback layered on Option A.
- If zero rows are returned for any `(code, snow_type, window)`, retry with a known-active code/window before declaring fields unpopulated.
- If unpopulated, stop and escalate. Do not implement empty snow bands. Raise a coordination ticket for the preprocessing/gateway owner.

**Artifact Gate**

The final line of `doc/plans/working/snow_field_population_check.md` must be exactly one of:

- `DECISION: proceed_to_phase_1`
- `DECISION: run_phase_0_5_first`
- `DECISION: stop_and_escalate`

The orchestrator must read this marker before dispatching follow-up work. Dispatch Phase 1 immediately only for `proceed_to_phase_1`; dispatch Phase 0.5 first for `run_phase_0_5_first`; stop all implementation for `stop_and_escalate`.

**Acceptance Criteria**

- `doc/plans/working/snow_field_population_check.md` records API base used, snow types checked, date window, row counts, per-field non-null counts, whether `plot_daily_snow_data()` needs data past `CURRENT_YEAR-12-31`, and an explicit go/no-go decision.
- No application code is changed.
- The note ends with exactly one machine-readable marker: `DECISION: proceed_to_phase_1`, `DECISION: run_phase_0_5_first`, or `DECISION: stop_and_escalate`.

**Constraint Sentence**

Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**.

**Expected Behaviour Before**

- Snow plots use only `value` renamed to variable plus `norm`.
- It is unknown whether snow statistical API fields are operationally populated.

**Expected Behaviour After**

- The team has written evidence showing whether Option A is viable.
- Implementation proceeds only when populated data exists or a fallback has been explicitly approved.

## Phase 0.5 - Optional Client-Side Population Fallback

**Justification**

Only run this phase if Phase 0 finds partial population, for example `mean` present but quantiles missing. If Phase 0 finds the fields unpopulated, do not run this phase; stop and escalate.

**Goal**

Add a minimal fallback in dashboard data loading that computes missing snow stats per day-of-year from a wider history window while preserving populated API fields. This keeps Option A as the primary source and uses Option B only for missing columns.

This fallback is a temporary bridge only. The long-term fix is to extend the gateway-owned snow aggregation path, especially `apps/preprocessing_gateway/dg_utils.py:379` (`calculate_snow_norms_from_api`), to populate the API fields directly; create a follow-up issue placeholder named `TODO: gateway snow percentile population` when this phase is dispatched.

**Files**

- `apps/forecast_dashboard/src/db.py`
- `apps/forecast_dashboard/tests/test_db.py`

**Depends on**

- Phase 0 partial-population result

**Agents**

- 1 agent: Implement fallback statistics in `db.py` and focused unit coverage in `test_db.py`.

**Acceptance Criteria**

- `_get_snow_single()` still prefers non-null API fields.
- Missing `min`, `max`, `mean`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`, or `current` are filled only when enough historical `value` data exists.
- Unit tests cover "API fields win", "missing quantiles are computed", and "all missing with insufficient history stays NaN".
- No files under `sapphire/services/` are edited.

**Constraint Sentence**

Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**.

**Expected Behaviour Before**

- Missing stat columns from `/snow/` remain missing or null in dashboard snow data.
- Plot bands would be empty if implementation proceeded.

**Expected Behaviour After**

- Partial API responses can still produce snow bands where historical `value` data is sufficient.
- Fully unpopulated operational data still blocks via Phase 0 escalation.

## Phase 1 - Data Contract in `get_snow_data` / `_get_snow_single`

**Goal**

Update the dashboard snow data contract so `_get_snow_single()` preserves all snow statistical fields needed by the plot, renames them to hydrograph-compatible column names, and converts HS units consistently across all statistical columns.

**Files**

- `apps/forecast_dashboard/src/db.py`
- `apps/forecast_dashboard/tests/test_db.py`

**Depends on**

- Phase 0 go decision, or Phase 0.5 if partial-population fallback is triggered

**Agents**

- 1 agent: Modify snow data normalization in `db.py`; add mocked `/snow/` unit tests in `test_db.py`.

**Implementation Notes**

Current snow fetch lives at `apps/forecast_dashboard/src/db.py:241`. It currently requests `PREVIOUS_YEAR-01-01` to `CURRENT_YEAR-12-31` at `apps/forecast_dashboard/src/db.py:245` and drops `snow_type`, `value1..value14`, and `id` at `apps/forecast_dashboard/src/db.py:250`.

Preserve these output columns when present:

- `code`
- `date`
- `<variable>` from `value`
- `norm`
- `mean`
- `min`
- `max`
- `5%`
- `25%`
- `50%`
- `75%`
- `95%`
- `last_year`
- `current_year`
- any existing non-service metadata already used by callers, excluding the known drop list

Snow keeps `norm` in addition to `mean` because Coordination item 2 is unresolved; hydrograph does not keep `norm` in its daily contract.

Drop only:

- `snow_type`
- `value1` through `value14`
- `id`

Rename map:

- `value` -> `<variable>` (`HS`, `RoF`, or `SWE`)
- `previous` -> `last_year`
- `current` -> `current_year`
- `q05` -> `5%`
- `q25` -> `25%`
- `q50` -> `50%`
- `q75` -> `75%`
- `q95` -> `95%`

Hydrograph helper compatibility note: daily hydrograph uses `5%`, `25%`, `50%`, `75%`, `95%` from `apps/forecast_dashboard/src/db.py:166` and plot code renames these at `apps/forecast_dashboard/src/vizualization.py:1769`. Use the same percentile labels for snow.

Date-window decision: follow the explicit Phase 0 note. If `snow_field_population_check.md` says `plot_daily_snow_data()` needs data past `CURRENT_YEAR-12-31`, widen mechanically to `start_date=f"{PREVIOUS_YEAR}-01-01"` and `end_date=f"{CURRENT_YEAR + 1}-12-31"`. If the note says no widening is needed, keep the existing range and document that in the Phase 1 PR notes. The September-to-August display window in `plot_daily_snow_data()` can otherwise ask for dates not fetched by `db.py`.

HS conversion: Phase 2 currently converts only `HS` and `norm` at `apps/forecast_dashboard/src/vizualization.py:2295`. Move or extend conversion so every numeric snow stat column is converted consistently: `HS`, `norm`, `mean`, `min`, `max`, `5%`, `25%`, `50%`, `75%`, `95%`, `last_year`, `current_year`.

**Acceptance Criteria**

Add tests in `apps/forecast_dashboard/tests/test_db.py`:

- `test_get_snow_single_preserves_statistical_fields`: mocked `/snow/` response with all fields returns expected columns after rename.
- `test_get_snow_single_drops_only_service_and_elevation_band_fields`: output excludes `snow_type`, `id`, and `value1..value14`.
- `test_get_snow_single_renames_percentiles_to_hydrograph_names`: asserts `q05/q25/q50/q75/q95` become `5%/25%/50%/75%/95%`.
- `test_get_snow_data_hs_converts_all_stat_columns_to_cm`: HS values and every stat column are scaled by 100; SWE/RoF are unchanged.
- `test_get_snow_single_empty_response_has_expected_contract`: empty/missing response still has the full enumerated column set with the expected dtypes — `code`, `date` (datetime64), `<variable>` (`HS` / `RoF` / `SWE`), `norm`, `mean`, `min`, `max`, `5%`, `25%`, `50%`, `75%`, `95%`, `last_year`, `current_year`. Missing stat columns must be present-but-empty (NaN), not dropped, so downstream plot code can assume the contract.

**Constraint Sentence**

Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**.

**Expected Behaviour Before**

- `_get_snow_single()` only renames `value` to the variable and drops elevation-band columns.
- Snow data may not expose percentile/stat columns to the plot.

**Expected Behaviour After**

- `get_snow_data()` returns snow DataFrames with hydrograph-compatible percentile/stat columns.
- HS units are consistent for all plotted stat values.

## Phase 2 - Plot Rewrite for `plot_daily_snow_data`

**Goal**

Rewrite `plot_daily_snow_data()` so snow plots render hydrograph-style statistical layers while preserving current-year predictor-period labels and forecast curves.

**Files**

- `apps/forecast_dashboard/src/vizualization.py`
- `apps/forecast_dashboard/tests/test_snow_plot.py`

**Depends on**

- Phase 1

**Agents**

- 1 agent: Update only snow plotting logic and add HoloViews overlay tests.

**Implementation Notes**

Snow plotting starts at `apps/forecast_dashboard/src/vizualization.py:2250`. Reuse existing helpers:

1. **Full range (min-max)**: `plot_runoff_range_area()` from `apps/forecast_dashboard/src/vizualization.py:726`, label `_("Min-Max")` or existing `_("Full range legend entry")`.
2. **90-percentile band (5%-95%)**: `plot_runoff_range_area()`, label `_("5%-95% range")` or existing `_("90-percentile range legend entry")`.
3. **50-percentile band (25%-75%)**: `plot_runoff_range_area()`, label `_("25%-75% range")` or existing `_("50-percentile range legend entry")`.
4. **Mean / norm line**: use `plot_runoff_line()` from `apps/forecast_dashboard/src/vizualization.py:344`. Prefer `mean` if non-null; fall back to `norm` if `mean` is absent or all-null until Coordination item 2 is resolved.
5. **Last-year line**: `plot_runoff_line()` on `last_year`.
6. **Current-year line + predictor-period label**: `plot_runoff_line()` on `current_year` if present, otherwise current `<variable>` column; preserve label construction at `apps/forecast_dashboard/src/vizualization.py:2354`.
7. **Forecast span / curve**: preserve existing forecast curve logic at `apps/forecast_dashboard/src/vizualization.py:2389`. Preserve predictor/forecast-span behaviour if currently intended; if adding spans, follow the daily hydrograph pattern at `apps/forecast_dashboard/src/vizualization.py:1784`.

Add `create_cached_vlines(_, horizon, for_dates=True, y_text=1)` from `apps/forecast_dashboard/src/vizualization.py:1465`, matching daily hydrograph use at `apps/forecast_dashboard/src/vizualization.py:1801`.

Recommend, but do not require, extracting a small helper such as `build_statistical_overlay(data, date_col, labels, colors)` that can be called from both the daily hydrograph block at `apps/forecast_dashboard/src/vizualization.py:1824` and the snow plot. If the helper is extracted in this phase, only the snow plot calls it; the hydrograph callsite stays inline until a follow-up phase converts it. Helper extraction here must be purely additive — do not modify `apps/forecast_dashboard/src/vizualization.py:1824-1853`. If the implementation stays inline, add a one-line cross-reference comment in `plot_daily_snow_data()` pointing future maintainers to the hydrograph overlay block at `apps/forecast_dashboard/src/vizualization.py:1824`.

Recompute y-axis bounds by concatenating all plotted stat columns that exist and are visible (`min`, `max`, `5%`, `25%`, `75%`, `95%`, `mean`, `last_year`, `current_year`, `<variable>`, plus `norm` when used as fallback), preserving the existing `0.9`/`1.1` padding.

**Acceptance Criteria**

Add `apps/forecast_dashboard/tests/test_snow_plot.py` with assertions:

- `test_snow_plot_contains_min_max_area`: overlay contains an `hv.Area` labelled for min-max/full range.
- `test_snow_plot_contains_percentile_bands`: overlay contains `hv.Area` layers for `5%-95%` and `25%-75%`.
- `test_snow_plot_contains_mean_or_norm_line`: overlay contains a mean line when `mean` exists, otherwise a norm line.
- `test_snow_plot_contains_last_and_current_year_lines`: overlay contains curves labelled "Last year" and "Current year".
- `test_snow_plot_preserves_forecast_curve`: dates on/after `date_picker` still produce a forecast curve.
- `test_snow_plot_hs_uses_already_converted_cm_contract`: avoids double-scaling HS if Phase 1 moved conversion into `db.py`.
- `test_snow_plot_y_axis_includes_all_visible_layers`: y-axis bounds reflect the recomputed envelope across every visible stat column (`min`, `max`, `5%`, `25%`, `75%`, `95%`, `mean`, `last_year`, `current_year`, `<variable>`, and `norm` when used as fallback), with the existing `0.9`/`1.1` padding preserved.

**Constraint Sentence**

Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**.

**Expected Behaviour Before**

- Snow plots render norm, current-year curve, and forecast curve only.
- Existing y-axis bounds consider only current-year and norm values at `apps/forecast_dashboard/src/vizualization.py:2359`.

**Expected Behaviour After**

- Snow plots include min-max, 5%-95%, 25%-75%, mean/norm, last-year, current-year, forecast, and vline overlays.
- Y-axis bounds include all visible statistical layers.

## Phase 3 - Wiring & Layout

**Goal**

Confirm all four dashboard data-loading paths pass the enriched `snow_data` contract through unchanged, and make layout changes only if visual sizing or card labels break after Phase 2.

**Files**

- `apps/forecast_dashboard/src/layout.py` only if card labelling or sizing needs to change

**Depends on**

- Phase 2 and Phase 4

**Agents**

- 1 agent: Verify snow data callsites read-only and perform only a minimal layout adjustment if a concrete card label/size defect is found.

**Implementation Notes**

Snow data callsites are:

- `apps/forecast_dashboard/src/db.py:651`
- `apps/forecast_dashboard/src/db.py:710`
- `apps/forecast_dashboard/src/db.py:745`
- `apps/forecast_dashboard/src/db.py:761`

No layout edit is expected. If Phase 2 overlays fit existing cards, explicitly skip `apps/forecast_dashboard/src/layout.py`.

Do not modify `apps/forecast_dashboard/src/db.py` in this phase. If the callsites need adjustment, report back and dispatch a new Phase 1.x scoped to `db.py`.

Manual verification view: use the main pentad dashboard view, because it includes snow cards and predictor data. If pentad is unavailable, use the monthly view to verify that snow bands render without predictor spans.

**Acceptance Criteria**

- All four `snow_data` callsites still call `get_snow_data(station)` and receive the enriched contract; this is verified read-only.
- At least one snow card in the pentad dashboard renders with the new layers and Phase 4 gettext labels.
- `apps/forecast_dashboard/src/layout.py` is unchanged unless the agent documents a concrete label/size defect.

**Constraint Sentence**

Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**.

**Expected Behaviour Before**

- All horizons load snow data, but plots only consume a narrow column set.
- Layout is sized for the existing simpler snow plot.

**Expected Behaviour After**

- Enriched snow data reaches every snow-consuming dashboard view.
- Layout remains unchanged unless proven necessary.
- Manual render verification includes the Phase 4 gettext labels.

## Phase 4 - i18n Strings

**Goal**

Add gettext entries for new snow layer labels and compile catalogues cleanly.

**Files**

The forecast dashboard reads runtime locale files from the configured data folder, but the in-repo source templates are under `apps/config/locale/`, per `doc/configuration.md:491`.

- `apps/config/locale/messages.pot`
- `apps/config/locale/en_CH/LC_MESSAGES/forecast_dashboard.po`
- `apps/config/locale/en_CH/LC_MESSAGES/forecast_dashboard.mo`
- `apps/config/locale/ru_KG/LC_MESSAGES/forecast_dashboard.po`
- `apps/config/locale/ru_KG/LC_MESSAGES/forecast_dashboard.mo`

Do not edit `forecast_dashboard copy.po` files.

**Depends on**

- Phase 2

**Agents**

- 1 agent: Add gettext keys, update `.po` translations, and regenerate `.mo` files.

**New Gettext Keys**

Add exact keys only if Phase 2 introduces them rather than reusing existing legend keys:

- `Last year`
- `5%-95% range`
- `25%-75% range`
- `Min-Max`
- `Mean`
- `Norm`

If Phase 2 reuses existing keys, verify the existing keys are present instead:

- `Last year legend entry`
- `90-percentile range legend entry`
- `50-percentile range legend entry`
- `Full range legend entry`
- `Mean legend entry`
- `Norm legend entry`

**Acceptance Criteria**

- `msgfmt` succeeds for both `en_CH` and `ru_KG`.
- English fallback renders the new labels.
- Russian catalogue has non-empty `msgstr` values or explicit accepted fallback strings.
- No locale files outside the listed paths are changed.

**Constraint Sentence**

Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**.

**Expected Behaviour Before**

- New snow layer labels may render as raw English keys or be missing from catalogues.
- Runtime `.mo` files do not include the new keys.

**Expected Behaviour After**

- New snow labels round-trip through gettext.
- Runtime catalogues include compiled translations.

## Phase 5 - Integration Test Update

**Goal**

Update integration coverage so snow API fixtures and dashboard verification assert percentile/statistical columns, not only the raw `value` series.

**Files**

- `apps/forecast_dashboard/tests/test_integration.py`

**Depends on**

- Phase 3 and Phase 4

**Agents**

- 1 agent: Extend existing snow integration checks only.

**Implementation Notes**

Existing snow API fetch touchpoint is `apps/forecast_dashboard/tests/test_integration.py:339`, which requests `/preprocessing/snow/`. Existing canvas assertion checks only `value` at `apps/forecast_dashboard/tests/test_integration.py:1922`.

Extend the integration test to assert presence/non-nullness of percentile-related fields in fetched snow rows before the canvas check:

- `min`
- `max`
- `q05`
- `q25`
- `q50`
- `q75`
- `q95`
- `mean`
- `previous`
- `current`

After Phase 1 renaming, also assert plotted/dashboard data includes:

- `5%`
- `25%`
- `50%`
- `75%`
- `95%`
- `last_year`
- `current_year`

**Acceptance Criteria**

- Integration test fails clearly if `/snow/` responses contain only `value`/`norm`.
- Integration test requires an explicit per-environment setting such as `SAPPHIRE_SNOW_STATS_AVAILABLE=true` before it may expect populated stats. If the variable is unset, the test fails with a configuration message rather than silently skipping.
- The env-var gate lives in `apps/forecast_dashboard/tests/conftest.py` as a fixture shared by all snow stat assertions, not as per-test reads. Its docstring states that `SAPPHIRE_SNOW_STATS_AVAILABLE` is an operator assertion (set per-environment so CI/staging can opt-in once the population path is live), not a developer override; this keeps semantics unambiguous when the variable is propagated to new environments.
- Existing value-series canvas assertion remains intact.
- Tests do not hard-code sensitive station codes beyond pre-existing test usage.

**Constraint Sentence**

Do NOT change any existing function signatures, data flow logic, or control flow beyond the scope listed in **Files** above. Your changes must be purely additive or modify only the specific behaviour described in **Goal**.

**Expected Behaviour Before**

- Integration verifies snow `value` appears on canvas.
- Empty percentile fields could ship unnoticed.

**Expected Behaviour After**

- Integration verifies statistical snow fields are available for plotting.
- Canvas/value regression coverage remains.

## Dependency Graph

The graph below uses only the `CLAUDE.md` schema keys. Conditional dispatch is controlled by the Phase 0 artifact marker in `doc/plans/working/snow_field_population_check.md`: dispatch P0.5 only when the marker is `DECISION: run_phase_0_5_first`; dispatch P1 directly when the marker is `DECISION: proceed_to_phase_1`; stop all implementation when the marker is `DECISION: stop_and_escalate`. If P0.5 runs, the orchestrator must complete and verify it before dispatching P1 even though both are shown as depending on P0 for schema compatibility.

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P0_5": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2", "P4"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```

> Note: the graph encodes P1's hard dependency on P0 only. If P0's `DECISION:` artifact triggers P0.5, the orchestrator must complete and verify P0.5 before dispatching P1 — see the prose preceding the graph. The graph stays schema-minimal per `CLAUDE.md`; the conditional handoff is enforced by the orchestrator reading the `DECISION:` artifact, not by extension keys.

## Test Summary

| Phase | Test files added/modified | Headline assertions |
|---|---|---|
| P0 | `doc/plans/working/snow_field_population_check.md` | Written row counts, non-null counts, date-window decision, and final `DECISION:` marker |
| P0.5 | `apps/forecast_dashboard/tests/test_db.py` | Missing stat fields computed only when partially populated and enough history exists |
| P1 | `apps/forecast_dashboard/tests/test_db.py` | Snow stat fields preserved, renamed, elevation bands dropped, HS stat columns scaled |
| P2 | `apps/forecast_dashboard/tests/test_snow_plot.py` | Overlay contains min-max, percentile bands, mean/norm, last-year, current-year, forecast |
| P3 | Manual dashboard verification | Pentad snow card renders enriched layers and Phase 4 labels; layout unchanged unless necessary |
| P4 | gettext compile commands | `.po` keys present and `.mo` compilation succeeds |
| P5 | `apps/forecast_dashboard/tests/test_integration.py` | Snow API/dashboard checks include percentile/stat columns as well as `value` |

## Risks & Rollback

- **P0**: API base or station may be wrong. Roll back by deleting only `doc/plans/working/snow_field_population_check.md` and rerun with corrected environment.
- **P0.5**: Client-side stats could diverge from service semantics. Roll back by reverting `apps/forecast_dashboard/src/db.py` fallback and its tests.
- **P1**: Column rename mismatch could break `plot_daily_snow_data()`. Roll back `db.py` snow-contract changes and `test_db.py` additions.
- **P2**: HoloViews overlay ordering or labels could break tests or visual readability. Roll back `vizualization.py` snow plot changes and `test_snow_plot.py`.
- **P3**: Layout edits could affect unrelated cards. Prefer no layout edits; if changed, revert `apps/forecast_dashboard/src/layout.py` independently.
- **P4**: Catalogue compilation could introduce stale `.mo` files. Re-run `msgfmt` from the listed `.po` files or revert locale files.
- **P5**: Integration assertions could be too strict for environments without populated stats. Keep Phase 0 as the gate; require an explicit environment variable such as `SAPPHIRE_SNOW_STATS_AVAILABLE=true` for stat assertions, and fail with a configuration message when it is unset rather than silently skipping.

## Out-of-Scope Reaffirmed

- No edits under `sapphire/services/`.
- No changes to forecast algorithms, model registry, postprocessing, or skill-metric code.
- No elevation-band visualization from `value1..value14`.
- No silent degradation when snow statistical fields are unpopulated.
- No bundling Phase 0 verification with implementation.
- No parallel implementation phases touching the same `db.py` neighbourhood.
