# FD-029: Quarterly card shows the latest calendar quarter, fetched with a year-safe window

**Status**: Draft (2026-09-25, rev 2 after out-of-loop review)
**Module**: `apps/forecast_dashboard`
**Priority**: Medium. **Deploy before 2026-12-25**, when the first kghm Q1 is issued: on Dec 25–31 its
quarter rows dated 2027-01-01 fall outside the fetch window (Problem 1).
**Labels**: `forecast_dashboard`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**:
- LTF-014 (schedule), PP-064 (postprocessing, incl. its three date populations), FD-030 (the bulletin's
  quarterly section)
- FD-019 (the quarter `forecast_year` in bulletin metadata stays there; that path is unreachable from the UI)
- FD-020 Bug 2 (the unconditional flag parse in this function; not fixed here)

## Where quarter appears

- "Quarter" is not a UI horizon. `create_horizon_selector` offers pentad, decad, month and season
  (`dashboard/widgets.py:97-103`), so `_get_data_quarter` (`src/db.py:962-970, 1208-1253`) is unreachable.
- The live consumers of `get_long_forecasts_quarter` (`src/db.py:817-871`) are:
  - the **"Quarterly forecast" card on the month horizon**, for reservoir stations only
    (`'вдхр'` in `punkt_name_ru`; `dashboard/plot_manager.py:362-428`, `dashboard/widget_manager.py:230-233`);
  - the month bulletin's quarterly section (`dashboard/bulletin_manager.py:393-399, 761-765, 891-895`;
    FD-030).

## How quarter rows are dated in the DB

This is essential context; see PP-064 § Mechanism 5. A calendar Q2 for kghm can exist as three rows per
model:
- **(a) the native row**, dated 25 Mar;
- **(b) a flag-OFF postprocessing rewrite**, dated `valid_from` = 1 Apr (all models, LR included,
  `apps/postprocessing_forecasts/src/api_writer.py:1193-1214`);
- **(c) a persisted monthly-derived row** (flag ON: dated 1 Mar for hv1).

Ensemble rows are type (b) under flag OFF. Under flag ON they take the first LR `date`.

## Problems (trunk `82946683`)

1. **The fetch window is by issue `date` and frozen at import.**
   - Parameters: `start_date = {PREVIOUS_YEAR}-12-20`, `end_date = {CURRENT_YEAR}-12-31` (`src/db.py:824-825`),
     with `CURRENT_YEAR`/`PREVIOUS_YEAR` computed at import (`:29-30`).
   - On 2026-12-25 the kghm Q1 native rows (dated Dec 25) are fetched. Their type-(b) rows and ensemble
     rows (dated 2027-01-01) are not.
   - A process started in 2026 and still running in 2027 never fetches the 2027-03-25 Q2 issue.
2. **One row per `(code, model_short)` by latest `date`, whatever the window** (`src/db.py:861-870`).
   - `valid_from`/`valid_to` are never checked, so a rolling row (e.g. issued Jul 23 → Aug–Oct, labelled
     Q3 by `:858`) is shown.
   - Because the rewrite (b) is later than the native row (a), it wins. The caption then shows "1 April"
     instead of "25 March".
   - Dedup by issue date also means a later backfill of an **older** quarter hides that model's newer quarter.
3. **The table renderer filters to the maximum issue date.** `update_quarterly_summary_tabulator` passes
   `filtered["date"].max()` (`plot_manager.py:408-423`), and `create_forecast_summary_table`
   (`src/vizualization.py:3160`, called from `create_forecast_summary_tabulator`, `:3239-3243`) keeps only
   rows at that date. When models carry different dates for the same quarter (a/b/c mixed), some models
   disappear from the card.
   - Whether this shows on live data depends on the flag and the date populations. Check with a read-only
     query before relying on it (P1 step 0).
4. **The caption trusts site attributes and falls back to lead-1 arithmetic.** `_format_quarterly_forecast_info`
   (`plot_manager.py:65-102`) prefers `site.quarterly_valid_from/to`. Those are set by the bulletin paths
   (`src/site.py:369`) and can belong to a different quarter than the card shows. Otherwise `:80-91` assumes
   lead 1, which is wrong for tjhm (lead 0).
5. **`year` comes from the issue date** (`src/db.py:860`). A Dec 25 Q1 gets the issue year.

## Plan

### P1 — Year-safe fetch, calendar selection per target quarter, renderer and caption (one code agent)

**Step 0 (read-only, before coding).** Query the local DB (aggregate counts only). For each org's
calendar QUARTER rows, count how many `(code, target quarter)` groups have models with **different**
`date` values. Record the result in the PR. It decides whether Test 4 reproduces a live defect or is
defence in depth.

**Files (only these may be modified)**:
- `apps/forecast_dashboard/src/db.py`: **only** `get_long_forecasts_quarter`
- `apps/forecast_dashboard/src/vizualization.py`: `create_forecast_summary_table` /
  `create_forecast_summary_tabulator`. Additive keyword argument only (e.g. `filter_by_date: bool = True`),
  so that the quarterly card can bypass the max-date reduction. Default behaviour is unchanged.
- `apps/forecast_dashboard/dashboard/plot_manager.py`: `_format_quarterly_forecast_info` (additive
  keyword parameters for the selected window and issue date) and `update_quarterly_summary_tabulator`
- Tests: `apps/forecast_dashboard/tests/test_db.py` (append) and a new `tests/test_quarter_calendar_card.py`

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Signature
changes are limited to the additive keyword arguments named above. Keep:
- the pagination (`_read_data_paginated`)
- the `horizon_value` filter
- the flag-OFF skill fan-out golden (`tests/test_db.py:2017`)
- every other caller of the renderer
- `_get_data_quarter`, the month path, the bulletin code and `widgets.py`

**Behaviour after**
1. **Fetch window.** Computed at call time from `dt.date.today()`, not from the module constants:
   `start_date = {today.year-1}-12-01`, `end_date = {today.year+1}-03-31`. The module constants and other
   functions are untouched.
2. **Calendar only.** Drop rows whose window is not an exact calendar quarter, before any dedup. Log the
   count at INFO. `year = valid_from.year`.
3. **Dedup per target quarter.** Keep one row per `(code, model_short, year, quarter_in_year)`, plus
   `horizon_value` under the flag. Within that key, prefer:
   1. a row whose `date` is earlier than `valid_from` and equals the latest such date (native issue);
   2. otherwise the latest row.
4. **Card selection.** Show all models' rows for the station's **latest target quarter** (max `valid_from`),
   using the renderer without the max-date reduction.
5. **Caption.** Built on every refresh from the selected rows' `valid_from`/`valid_to` (never from site
   attributes, never lead arithmetic).
   - Issue date = the latest `date` < `valid_from` among the selected LR_Base/LR_SM rows.
   - If none exists (only rewrites/ensembles), show the period without an issue date (text "issue date
     not available").
   - Showing a synthetic `valid_from` date as an issue date is not allowed.
   - A persisted monthly-derived row (population c) also has `date` < `valid_from`. Excluding it needs the
     configured issue day, which the dashboard does not read today. See overview decision D5.

**Tests (station `19999`; each must fail on trunk unless noted)**
1. **Fetch window at Dec 25.** With `today` = 2026-12-25 (monkeypatched at call time), the request covers
   2027-01-01 dates.
   - The mocked API returns native LR rows dated 2026-12-25 and type-(b)/EM rows dated 2027-01-01, all Q1 2027.
   - Expect all models present.
2. **Rolling excluded.** Calendar Q2 issued 2026-03-25 plus rolling Jun–Aug issued 2026-05-25 for the same
   model → one row, Apr–Jun.
3. **Native vs rewrite.** Native LR row 2026-03-25 plus rewrite 2026-04-01, same window → the kept row
   has `date` 2026-03-25, and the caption shows "25th of March 2026".
4. **Renderer.** LR rows dated 2026-03-25 and an EM row dated 2026-04-01, all Apr–Jun, through the **real**
   `create_forecast_summary_tabulator` (not mocked) → the tabulator holds all three models.
   - Run it on trunk first. Record whether it fails there.
5. **Older quarter with a later issue date.** A Q3 row dated 2026-06-25 plus a backfilled Q2 row dated
   2026-07-02 for the same model, in shuffled order → the card shows Q3.
6. **Stale site attributes.** The site carries `quarterly_valid_from/to` for Q2 while the selected rows are
   Q3 → the caption says Jul–Sep.
7. **tjhm lead 0.** Issue 2027-01-01, Jan–Mar → the caption reads "Jan 2027 – Mar 2027".
8. **Unchanged:** the existing quarter tests in `tests/test_db.py` (`:903-1020`, `:1600-2060`, `:2981`),
   `tests/test_monthly_lead_golden.py:541,598,618`, and all other callers of the renderer.

**Acceptance**: tests 1–3 and 5–7 fail on trunk and pass after; test 4 as recorded.
`cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` gives zero failures; the only
skips are the operator-gated Playwright tests (`TEST_PENTAD`/`TEST_DECAD`/`TEST_LOCAL`). `git diff --stat`
is limited to the listed files.

## Out of scope

- The bulletin's quarterly section (FD-030).
- The quarter `forecast_year` in `get_bulletin_metadata` (FD-019; unreachable).
- The import-time years for other horizons (`src/db.py:29-30`; overview, deferred findings).
- Exposing "quarter" as a horizon.
