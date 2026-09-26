# FD-029: Quarterly card shows the latest calendar quarter, fetched with a year-safe window

**Status**: Draft (2026-09-26, rev 4 after the second review round)
**Module**: `apps/forecast_dashboard`
**Priority**: Medium. **Deploy before 2026-12-25**, when the first kghm Q1 is issued: on Dec 25–31 its
quarter rows dated 2027-01-01 fall outside the fetch window (Problem 1).
**Labels**: `forecast_dashboard`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only. Overview decision D5 (caption issue date) is **resolved here** by the schedule-based
native-row rule (Behaviour after, items 5 and 9).
**Related**:
- LTF-014 (schedule); PP-064 (its Contract defines the native-row rule and the three date populations);
  PP-065 (P1b implements the same rule in postprocessing; produces the seven derived models and the
  quarterly Naive Mean / Skilled Mean; **no quarterly EM** after it, owner round 2, 2026-09-26); FD-030 (the
  bulletin's quarterly section)
- FD-019 (the quarter `forecast_year` in bulletin metadata stays there; that path is unreachable from the UI)
- FD-020 Bug 2 (the unconditional flag parse in this function; not fixed here)

## Where quarter appears

- "Quarter" is not a UI horizon. `create_horizon_selector` offers pentad, decad, month and season
  (`dashboard/widgets.py:97-103`), so `_get_data_quarter` (`src/db.py:962-970, 1208-1253`) is unreachable.
- The live consumers of `get_long_forecasts_quarter` (`src/db.py:816-871`) are:
  - the **"Quarterly forecast" card on the month horizon**, for reservoir stations only
    (`'вдхр'` in `punkt_name_ru`; `dashboard/plot_manager.py:362-428`, `dashboard/widget_manager.py:230-233`).
    Its load path is `_get_data_monthly` (`src/db.py:1119`), which then left-merges the quarter skill rows
    (`:1120-1133`); that merge is what puts `delta` on the card rows;
  - the month bulletin's quarterly section, three blocks that each call the function directly
    (`dashboard/bulletin_manager.py:394-399` in `_populate_forecast_attributes`, `:760-765` in `_on_add`,
    `:890-895` in `_on_add_m0`; FD-030).

## How quarter rows are dated in the DB

See PP-064 § "Mechanism and problems", item 5. A calendar Q2 for kghm can exist as three LR rows per model:
- **(a) the native row**, dated 25 Mar;
- **(b) a flag-OFF postprocessing rewrite**, dated `valid_from` = 1 Apr
  (`apps/postprocessing_forecasts/src/api_writer.py:1199-1204`). PP-065 stops writing LR rows; existing
  (b) rows persist;
- **(c) a persisted monthly-derived row** (flag ON: dated 1 Mar for hv1).

For tjhm (issue day 1, lead 0) a (b) row has the native key and is the same DB row as (a); PP-064 Chunk C
(decision F) cleans those. The seven derived models and the Naive Mean / Skilled Mean rows (PP-065) are
dated `valid_from` under flag OFF and carry the row's issue `date` under flag ON (`api_writer.py:1199-1204`).
Old quarterly EM rows remain in the DB for past quarters (accepted, round-2 decision 2); no new ones are
written.

## Problems (trunk `82946683`)

1. **The fetch window is by issue `date` and frozen at import.**
   - Parameters: `start_date = {PREVIOUS_YEAR}-12-20`, `end_date = {CURRENT_YEAR}-12-31` (`src/db.py:824-825`),
     with `CURRENT_YEAR`/`PREVIOUS_YEAR` computed at import (`:29-30`).
   - On 2026-12-25 the kghm Q1 native rows (dated Dec 25) are fetched. Their type-(b) rows and the
     flag-OFF derived and ensemble rows (dated 2027-01-01) are not.
   - A process started in 2026 and still running in 2027 never fetches the 2027-03-25 Q2 issue.
2. **One row per `(code, model_short)` by latest `date`, whatever the window** (`src/db.py:861-870`).
   - `valid_to` is never parsed (only `valid_from`, `:856`), so a rolling row (e.g. issued Jul 23 →
     Aug–Oct, labelled Q3 by `:858`) is shown.
   - Because the rewrite (b) is later than the native row (a), it wins. The caption then shows "1 April"
     instead of "25 March".
   - Dedup by issue date also means a later backfill of an **older** quarter hides that model's newer quarter.
   - Nothing keeps a quarter hidden before its issue date: tjhm rows dated 2026-10-01 already in the DB
     are shown now.
3. **The table renderer reduces to the maximum issue date.** `update_quarterly_summary_tabulator` passes
   `filtered["date"].max()` (`plot_manager.py:408-423`); `create_forecast_summary_table` then keeps only
   rows at that date (`src/vizualization.py:3161-3166`; called from `create_forecast_summary_tabulator`,
   `:3244-3246`). When models carry different dates for the same quarter (a/b/c mixed), some models
   disappear from the card.
   - **Step 0 measured (local DB, aggregate counts only):** kyg hv1: 1802 of 5724 `(code, target quarter)`
     groups have models with different latest dates → **live defect on kghm**. taj: 0.
4. **The caption trusts site attributes and falls back to lead-1 arithmetic.** `_format_quarterly_forecast_info`
   (`plot_manager.py:65-102`) prefers `site.quarterly_valid_from/to`. Those are set by the bulletin paths
   (`src/site.py:369-376`) and can belong to a different quarter than the card shows. Otherwise `:80-91`
   assumes lead 1, which is wrong for tjhm (lead 0).
5. **`year` comes from the issue date** (`src/db.py:860`). A Dec 25 Q1 gets the issue year.
6. **Null quantiles blank the card range.** On the month horizon the renderer takes the bounds from
   `Q25`/`Q75` only (`src/vizualization.py:3190-3198`). Derived quarter rows and ensembles with a derived
   member have null `Q25`/`Q75` (overview decisions A, B), so their range is blank.

## Plan

### P1 — Year-safe fetch, eligibility cutoff, native LR selection, renderer and caption (one code agent)

**Files (only these may be modified)**:
- `apps/forecast_dashboard/src/db.py`: **only** `get_long_forecasts_quarter`, plus adding
  `operational_schedule_for_mode` to the existing `long_term_horizon_resolver` import (`:10-17`)
- `apps/forecast_dashboard/src/vizualization.py`: `create_forecast_summary_table` (additive keyword
  `filter_by_date: bool = True`) and `create_forecast_summary_tabulator` (the same keyword, threaded
  through to `create_forecast_summary_table`); and the all-NaN guard at the `idxmax` in
  `create_forecast_summary_tabulator` (`:3256`)
- `apps/forecast_dashboard/dashboard/plot_manager.py`: `_format_quarterly_forecast_info` (additive
  keyword parameters for the selected window and issue date) and `update_quarterly_summary_tabulator`
- Tests: `apps/forecast_dashboard/tests/test_db.py` (append only) and a new
  `tests/test_quarter_calendar_card.py`

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Signature
changes are limited to the additive keyword arguments named in this plan. Keep:
- the pagination (`_read_data_paginated`) and the `horizon_value` request filter;
- the flag-OFF skill fan-out golden (`tests/test_db.py:2017`);
- every other caller of the renderer (default `filter_by_date=True` is byte-identical to trunk);
- `_get_data_quarter`, `_get_data_monthly`, the bulletin code and `widgets.py`.

**Behaviour after**
1. **Fetch window at call time (coverage only).** `get_long_forecasts_quarter` gains an additive keyword
   `today: date | None = None`, resolved as `today or date.today()` inside the call (`db.py:3` imports
   `from datetime import date, datetime`; there is no `dt` alias). `start_date = {today.year-1}-12-01`,
   `end_date = {today.year+1}-03-31`. The module constants `CURRENT_YEAR`/`PREVIOUS_YEAR` and all other
   functions are untouched. What may be **shown** is decided by item 4, not by the window.
2. **Calendar quarters only.** Parse `valid_to`. A row is a calendar quarter iff `valid_from` is day 1 of
   Jan/Apr/Jul/Oct **and** `valid_to` = `valid_from` + 3 months − 1 day. Drop all other rows (incl. a
   null `valid_to`) before any dedup; log the dropped count once per call at INFO. Set
   `year = valid_from.year`.
3. **Schedule.** Read `operational_schedule_for_mode("quarter")`
   (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:112-142`) once per call for `lead_time` and
   `issue_day`. If it raises `LongTermHorizonResolverError` (which covers its subclass
   `UnsupportedLongTermModeError`, `:25-29`) or `FileNotFoundError`, log one WARNING and run **degraded**:
   no native rule (LR is treated like the other models in item 5) and eligibility falls back to
   `date <= today`. This mirrors `_safe_lead` (`db.py:1047-1056`). **Required:** the autouse fixture in
   `tests/test_db.py:19-42` writes `quarter.json` with the lead only, so raising here, or hiding LR rows
   when degraded, would break existing quarter tests.
4. **Eligibility cutoff.** A target quarter is returned only once its **configured issue date** has
   arrived: `issue_date(Q) = date(y, m, issue_day)`, where (y, m) = Q's first month shifted back by
   `lead_time` months (year-aware), and `issue_date(Q) <= today`. This holds even when stored rows are
   dated at the future quarter start (flag OFF). Examples: tjhm Q4 2026 → 2026-10-01, hidden on
   2026-09-26; kghm Q1 2027 → 2026-12-25, visible on 2026-12-25 although its flag-OFF Skilled Mean is
   dated 2027-01-01. Drop ineligible rows before the dedup.
5. **Dedup per target quarter.** One row per `(code, model_short, year, quarter_in_year)`, plus
   `horizon_value` under the flag. Several target quarters per model are returned.
   - **LR_Base / LR_SM: native only.** A row is **native** iff `date.day == issue_day` **and** the
     year-aware lead `(valid_from.year − date.year)·12 + (valid_from.month − date.month)` == `lead_time`
     (the PP-064 Contract rule; the formula of `apps/postprocessing_forecasts/src/data_reader.py:346-348`;
     identical to PP-065 P1b). Non-native LR rows ((b), (c)) are **never** returned; with no native row the
     quarter has no LR row.
   - **All other models** (the seven derived models, `Naive Mean`, `Skilled Mean`, old `EM`): the latest
     `date`; ties broken by the highest API `id` (keep `id` until after the dedup, then drop it as today).
6. **Card selection** (`update_quarterly_summary_tabulator`).
   - Select all models' rows for the station's **latest eligible target quarter** (max `valid_from`).
   - Pass `max(date)` **of the selected rows** as `date_picker`, so the renderer's
     `date <= date_picker + 1 day` filter (`vizualization.py:3147-3153`) keeps them all, and call the
     renderer with `filter_by_date=False`, which skips only the max-date reduction (`:3161-3166`).
   - The renderer also filters on `model_selection.options`, which come from the **monthly**
     `forecasts_all` (`dashboard/data_manager.py:269-281, 287-290`). A quarter model with no monthly rows
     is hidden. This dependency stays; the seven derived models, `Naive Mean` and `Skilled Mean` are monthly
     models too, so the card shows them when present.
   - **Fallback quarters have no LR row (accepted, round-2 decision 3).** PP-065's temporary LR fallback
     is not persisted, so until LTF-014 P0/P2 a quarter without a native LR row shows the seven models and
     the ensembles but no LR_Base/LR_SM. This goes into the hydromet notice.
7. **δ bounds for null quantiles (overview decision D).** In the card's copy of the selected rows, before
   the renderer: where `Q25` is null, set `Q25 = forecasted_discharge − delta`; where `Q75` is null, set
   `Q75 = forecasted_discharge + delta` (the arithmetic of `processing.calculate_forecast_range`,
   `src/processing.py:1264-1266`; not routed through the range selector). `delta` is the quarter skill
   `delta` merged in by `_get_data_monthly`. If `delta` is null or absent, the bounds stay empty. Rows
   with native `Q25`/`Q75` are unchanged. The month-horizon renderer branch is not edited.
   - **K = 10 (PP-065).** Skill rows with fewer than 10 pairs are suppressed, so those models have no
     `delta` and empty bounds (accepted by decision D). tjhm may show empty bounds until more history is
     scored; PP-065 P2 measures how often.
8. **All-NaN accuracy.** In `create_forecast_summary_tabulator`, when the accuracy column has no
   non-null value, use index 0 without calling `idxmax` (all-NaN `idxmax` warns in pandas 2.3.3, the
   locked version, and raises in pandas 3). Behaviour for any non-all-NaN column is unchanged.
9. **Caption.** Built on every refresh from the selected rows' `valid_from`/`valid_to`, never from site
   attributes, never from lead arithmetic.
   - Issue date = the `date` of the selected **native LR** row (one date per target quarter by
     construction). tjhm Q1 2027 then reads "1st of January 2027".
   - Without a native LR row (fallback quarter, degraded schedule), show the period and
     "issue date not available". A `valid_from`-dated row's `date` is never shown as an issue date.

**Behaviour after — bulletin input (accepted in the interim, overview decision E).** The three bulletin
blocks call the same function with `head(1)` on the latest `date`. After P1:
- rolling-window, non-native LR and not-yet-eligible rows are no longer returned;
- rows dated 2027-01-01 appear on Dec 25–31;
- several target quarters are returned per model, so `head(1)` may pick a backfilled older quarter.

The bulletin keeps publishing what is available until FD-030 replaces those blocks.

**Tests (station `19999`; each must fail on trunk)**. New tests that need the schedule write their own
`quarter.json` with `operational_month_lead_time` **and** `operational_issue_day` (kghm-shaped 1/25,
tjhm-shaped 0/1) and pass `today` explicitly. Ensemble fixtures use `Naive Mean` / `Skilled Mean`, never a
new `EM` row.
1. **Fetch window at Dec 25.** Call with `today=date(2026, 12, 25)` and capture the request params. Assert
   `start_date <= "2026-12-25"` and `end_date >= "2027-01-01"`. (`_make_mock_response` ignores params,
   `tests/test_db.py:537-543`, so assert on the params, not on the returned rows.)
2. **Rolling excluded.** Calendar Q2 issued 2026-03-25 plus rolling Jun–Aug issued 2026-05-25 for the same
   model → one row, Apr–Jun.
3. **Native vs rewrite.** Native LR row 2026-03-25 plus rewrite 2026-04-01, same window → the kept row
   has `date` 2026-03-25, and the caption shows "25th of March 2026".
4. **Renderer.** LR rows dated 2026-03-25 and a `Skilled Mean` row dated 2026-04-01, all Apr–Jun, through
   the **real** `create_forecast_summary_tabulator` with `model_selection.options` including LR_Base, LR_SM
   and `Skilled Mean` → the tabulator holds all three models. (Live defect for kghm, Problem 3.)
5. **Older quarter with a later issue date.** GBT: a Q3 row dated 2026-06-25 plus a backfilled Q2 row
   dated 2026-07-02, in shuffled order, `today=2026-07-10` → the card shows Q3.
6. **Stale site attributes.** The site carries `quarterly_valid_from/to` for Q2 while the selected rows are
   Q3 → the caption says Jul–Sep.
7. **tjhm lead 0.** Native LR row issued 2027-01-01, Jan–Mar 2027, plus a `Naive Mean` row of the same date,
   `today=2027-01-02` → the caption reads "Jan 2027 – Mar 2027" **and** "1st of January 2027".
8. **δ bounds.** A GBT row and a `Skilled Mean` row with null `Q25`/`Q75` and `delta` 5.0, forecast 100 →
   bounds 95/105 for both, card visible; a row with null `delta` → empty bounds, card still visible.
9. **All-NaN accuracy.** Selected rows whose accuracy is all NaN → no warning (run with
   `warnings.simplefilter("error")`), first row selected.
10. **Bulletin characterisation.** Through `bulletin_manager._populate_forecast_attributes` with the real
    `get_long_forecasts_quarter` bound to `today=2026-12-26` (e.g. monkeypatch `db.get_long_forecasts_quarter`
    with a `functools.partial` of the real function; mock `db._read_data_paginated`; month horizon,
    reservoir site, kghm schedule): a native Q1 2027 row dated 2026-12-25 plus a rolling Feb–Apr 2027 row
    dated 2026-12-26 → the site's `quarterly_valid_from` is 2027-01-01 (trunk picks the rolling row). This
    pins the interim input the bulletin receives; FD-030 replaces it.
    (`tests/test_bulletin_header_date.py:278-286` mocks the function and stays as is.)
11. **Degraded schedule.** Under a lead-only `quarter.json`, `today=2026-05-01`: an LR_Base row and a GBT
    row dated 2026-03-22, Apr–Jun, plus a GBT Q3 row dated 2026-06-22 → no exception; the LR_Base and the
    Q2 GBT rows are returned, the Q3 row (dated after `today`) is not; caption "issue date not available".
12. **Quarter with no native LR (fallback, accepted).** kghm schedule, flag OFF, `today=2027-01-05`: for
    Q1 2027 an LR_Base rewrite, GBT, `Naive Mean` and `Skilled Mean` rows, all dated 2027-01-01 → the card
    holds GBT, `Naive Mean` and `Skilled Mean` and **no** LR_Base; caption "Jan 2027 – Mar 2027" and
    "issue date not available".
13. **Eligibility, tjhm.** tjhm schedule, `today=2026-09-26`: a native-shaped LR Q4 row dated 2026-10-01
    and a native Q3 row dated 2026-07-01 → only Q3 is returned; the card shows Jul–Sep.
14. **Eligibility, kghm Dec 25.** kghm schedule, flag OFF, `today=2026-12-25`: a `Skilled Mean` Q1 2027
    row dated 2027-01-01 plus the native LR Q1 rows dated 2026-12-25 → Q1 2027 is returned and selected
    by the card, with the Skilled Mean row visible.

**Acceptance**
- Tests 1–14 fail on trunk and pass after.
- The full module suite passes with **no existing test edited**:
  `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` gives zero failures. The only
  allowed skips: `tests/test_docker.py:22` (no Docker daemon) and the Playwright tests, gated by
  `importorskip` (`tests/test_integration.py:7`, `tests/test_integration_bea.py:9`) plus the env
  `skipif` (`TEST_PENTAD`/`TEST_DECAD`/`TEST_LOCAL`).
- `git diff --stat` is limited to the listed files.

**Deploy.** Rebuild the dashboard image, redeploy it and restart the dashboard container on kghm and
tjhm **before 2026-12-25**. To check: the card exists only on the month horizon, for reservoir stations.

## Out of scope

- The bulletin's quarterly section (FD-030).
- The quarter `forecast_year` in `get_bulletin_metadata` (FD-019; unreachable).
- The import-time years for other horizons (`src/db.py:29-30`; overview, deferred findings).
- **`get_forecast_stats`' import-time skill window** (`src/db.py:679-684`: `{PREVIOUS_YEAR}-12-31` …
  `{CURRENT_YEAR}-12-31`). Quarter skill rows are dated in the recalc year, so a dashboard restarted in
  January, before that month's recalc, has no quarter skill and therefore no δ (empty bounds) until the
  recalc writes the new year's rows.
- The quarter skill merge in `_get_data_monthly` (`src/db.py:1112-1133`) is not lead-filtered. Under
  flag OFF, quarter skill is written at the sentinel hv 0 only
  (`apps/postprocessing_forecasts/src/api_writer.py:661-669`), so the merge is 1:1; a fan-out needs skill
  rows at more than one `horizon_value` (pre-existing).
- Exposing "quarter" as a horizon.
