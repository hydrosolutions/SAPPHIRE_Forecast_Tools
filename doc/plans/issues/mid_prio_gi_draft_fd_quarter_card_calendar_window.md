# FD-029: Quarterly card shows the latest calendar quarter, fetched with a year-safe window

**Status**: Draft (2026-09-26, rev 6 after the fourth review round)
**Module**: `apps/forecast_dashboard`
**Priority**: Medium. **Deploy before 2026-12-25**, when the first kghm calendar Q1 is issued: on Dec 25–31
its flag-OFF derived and ensemble rows, dated 2027-01-01, fall outside the fetch window (Problem 1).
**Labels**: `forecast_dashboard`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only. Overview decision D5 (caption issue date) is **resolved here** by the
schedule-computed `issue_date(Q)` (Behaviour after, items 4 and 9).
**Related**:
- LTF-014 (schedule; P0 deferred, configs stay at `forecast_months [3..9]`); PP-064 (its Contract defines
  the native-row rule and the three date populations); PP-065 (P1b implements the same rule in
  postprocessing; produces the seven derived models and the quarterly Naive Mean / Skilled Mean; **no
  quarterly EM** after it, owner round 2, 2026-09-26); FD-030 (the bulletin's quarterly section)
- FD-019 (the quarter `forecast_year` in bulletin metadata stays there; that path is unreachable from the UI)
- FD-020 Bug 2 (the unconditional flag parse in this function; not fixed here)

## Where quarter appears

- "Quarter" is not a UI horizon. `create_horizon_selector` offers pentad, decad, month and season
  (`dashboard/widgets.py:97-103`), so `_get_data_quarter` (`src/db.py:962-970, 1208-1253`) is unreachable.
- The live consumers of `get_long_forecasts_quarter` (`src/db.py:816-871`) are:
  - the **"Quarterly forecast" card on the month horizon**, for reservoir stations only
    (`'вдхр'` in `punkt_name_ru`; `dashboard/plot_manager.py:362-428`, `dashboard/widget_manager.py:230-233`).
    Its load path is `_get_data_monthly` (trunk `src/db.py:1020`; branch `fix_fd_quarter_card_calendar`
    `:1277`, shifted by the fetch-window and LR-drop-logging code inserted earlier in the file), which
    then left-merges the quarter skill rows (trunk `:1108-1131`; branch `:1365-1389`); that merge is
    what puts `delta` on the card rows;
  - the month bulletin's quarterly section, three blocks that each call the function directly
    (`dashboard/bulletin_manager.py:394-399` in `_populate_forecast_attributes`, `:760-765` in `_on_add`,
    `:890-895` in `_on_add_m0`; FD-030).

## How quarter rows are dated in the DB

See PP-064 § "Mechanism and problems", item 5. A calendar Q2 for kghm can exist as three LR rows per model:
- **(a) the native row**, dated 25 Mar;
- **(b) a flag-OFF postprocessing rewrite**, dated `valid_from` = 1 Apr — the `record_date` logic is at
  trunk `apps/postprocessing_forecasts/src/api_writer.py:1199-1204`; PP-064's branch
  (`fix_pp_quarter_calendar_window`) inserts its own calendar-window guard earlier in the same function,
  shifting this same logic to `:1264-1269` there — cited as `:1264-1269` (PP-064 branch) from here on,
  since that is the code this plan is deployed against. PP-065 stops writing LR rows; existing
  (b) rows persist;
- **(c) a persisted monthly-derived row** (flag ON: dated 1 Mar for hv1).

For tjhm (issue day 1, lead 0) a (b) row has the native key and is the same DB row as (a); PP-064 Chunk C
(decision F) cleans those. The seven derived models and the Naive Mean / Skilled Mean rows (PP-065) are
dated `valid_from` under flag OFF and carry the row's issue `date` under flag ON (`api_writer.py:1264-1269`,
PP-064 branch).
Old quarterly EM rows remain in the DB for past quarters (accepted, round-2 decision 2); no new ones are
written, and P1 no longer returns them (Behaviour after, item 5).

## Problems (trunk `82946683`)

1. **The fetch window is by issue `date` and frozen at import.**
   - Parameters: `start_date = {PREVIOUS_YEAR}-12-20`, `end_date = {CURRENT_YEAR}-12-31` (`src/db.py:824-825`),
     with `CURRENT_YEAR`/`PREVIOUS_YEAR` computed at import (`:29-30`).
   - On 2026-12-25 the flag-OFF derived and ensemble rows of kghm Q1 (dated 2027-01-01) are not fetched.
     While LTF-014 P0 is deferred, kghm Q1 has no native LR row (the configs do not issue in December), so
     this Q1 is fallback-only, and without P1 the card keeps showing the previous quarter on Dec 25–31.
     Only rows dated Dec 25 (flag ON) would be fetched.
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
  `create_forecast_summary_tabulator` (`:3267`)
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
1. **Fetch window at call time, schedule-derived and widened both directions (coverage only).**
   `get_long_forecasts_quarter` gains an additive keyword `today: date | None = None`, resolved as
   `today or date.today()` inside the call (`db.py:859`; `db.py:3` imports
   `from datetime import date, datetime`; there is no `dt` alias). The window's bounds are computed
   **after** item 3's schedule/degraded resolution (`db.py:872-898`), since they depend on it, not
   before — a fixed, edge-by-edge-patched window kept missing cases, so it is now sized generously in
   both directions instead (`db.py:900-971`):
   - `fetch_lead = max(schedule.lead_time, resolved_horizon_value or 0)` when the schedule resolved;
     `resolved_horizon_value if resolved_horizon_value is not None else 3` when degraded (`db.py:940-943`);
     then clamped `fetch_lead = max(fetch_lead, 0)` (`db.py:947`) — a misconfigured negative
     `operational_month_lead_time`, or a negative explicit `horizon_value` in degraded mode, must never
     narrow the window below lead 0's own reach.
   - `start_date` = the 1st of the month `(12 + fetch_lead)` months before the **start of today's own
     calendar quarter** (`db.py:948-953`) — always at or before the original fixed bound
     `{today.year-1}-12-01` (worst case: lead 0, today in Q4, lands on `{today.year-1}-10-01`), so that
     fixed bound is dropped entirely rather than kept as a no-op `min`.
   - `end_date` = `max({today.year+1}-03-31, the last day of the month (fetch_lead + 1) months after
     today's month)` (`db.py:955-960`) — the original fixed upper bound is **kept** as one side of this
     `max` (it still wins for `fetch_lead <= 2` through most of the year); the schedule-derived side
     additionally covers a lead>=4 config's flag-OFF row, dated at the *next* quarter's own `valid_from`.
   - **Rationale (verified, not asserted).** An oracle sweep over every day of 2025–2028, leads 0–4,
     issue days 1/25/31 and 0–5 consecutive missing quarters found **zero** mismatches through 3
     consecutive missing quarters; a partial older quarter is possible only with **four or more**
     consecutive missing quarters (accepted; `db.py:900-939`).
   The module constants `CURRENT_YEAR`/`PREVIOUS_YEAR` and all other functions are untouched. What may be
   **shown** is decided by item 4, not by the window.
2. **Calendar quarters only.** Parse `valid_to`. A row is a calendar quarter iff `valid_from` is day 1 of
   Jan/Apr/Jul/Oct **and** `valid_to` = `valid_from` + 3 months − 1 day. Drop all other rows (incl. a
   null `valid_to`) before any dedup; log the dropped count once per call at INFO. Set
   `year = valid_from.year`.
3. **Schedule, resolved once per call — before item 1's window is even sized.** Read
   `operational_schedule_for_mode("quarter")`
   (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:112-142`) once per call, immediately after
   resolving `today` and *before* item 1's fetch window is built (`db.py:872-898`; item 1's `fetch_lead`
   reads `schedule`/`degraded` from here), for `lead_time` and `issue_day`. Run **degraded** — no native
   preference and no LR strictness (every model, LR included, takes the latest `date` in item 5),
   `is_native` is False for every row, `quarter_issue_date` is `NaT`, and eligibility falls back to
   `date <= today` — under **either** of two conditions, each logging its own WARNING:
   - the call raises `LongTermHorizonResolverError` (covers its subclass
     `UnsupportedLongTermModeError`, `:25-29`) or `FileNotFoundError` (`db.py:872-881`); or
   - the schedule resolves but its `issue_day < 1` (`db.py:891-898`) — `_require_int_field`
     (`long_term_horizon_resolver.py`) only checks the field is an int, not a valid day-of-month, so an
     invalid config (e.g. `0` or negative) would otherwise reach the date construction further down and
     raise `ValueError`, aborting the monthly dashboard load / the reservoir bulletin instead of
     degrading.
   This mirrors `_safe_lead` (`db.py:1304-1312`). **Required:** the autouse fixture in
   `tests/test_db.py:19-42` writes `quarter.json` with the lead only, so raising here, or hiding LR rows
   when degraded, would break existing quarter tests.
   - **Accepted limitation: degraded mode hides lead>=1 quarters between their issue date and quarter
     start.** Degraded eligibility is `date <= today` (no schedule, so no `issue_date(Q)` from item 4), and
     under flag OFF the fresh derived/ensemble rows are dated `valid_from` — the quarter's own start, not
     the actual issue date, which for lead>=1 (kghm) falls `lead_time` months earlier. So on kghm, degraded
     mode hides a quarter's Naive Mean/Skilled Mean (and every other flag-OFF derived row) for the whole
     `lead_time`-month gap between when they are actually issued and `valid_from` itself — exactly the
     window item 4's schedule-computed eligibility cutoff would otherwise show them in. Only triggers when
     the schedule read fails or `issue_day` is misconfigured (not in steady state); does not affect tjhm
     (lead 0, `valid_from` == the issue date). **Recommend:** PP-064 Chunk C step 0's per-org
     `quarter.json`/`issue_day` read (`../high_prio_gi_draft_pp_quarter_calendar_window_validation.md`,
     § Chunk C) also be a precondition of this plan's own Deploy step, so a bad or missing quarter config
     on either org is caught before deploy rather than discovered as degraded mode in production.
4. **Eligibility cutoff.** A target quarter is returned only once its **configured issue date** has
   arrived: `issue_date(Q) = date(y, m, issue_day)`, where (y, m) = Q's first month shifted back by
   `lead_time` months (year-aware), and `issue_date(Q) <= today`. This holds even when stored rows are
   dated at the future quarter start (flag OFF). Examples: tjhm Q4 2026 → 2026-10-01, hidden on
   2026-09-26; kghm Q1 2027 → 2026-12-25, visible on 2026-12-25 although its flag-OFF Skilled Mean is
   dated 2027-01-01. Drop ineligible rows before the dedup. Carry the value as a new column
   `quarter_issue_date` on every returned row (one value per target quarter; NaT when degraded); the caption
   (item 9) reads it.
   - **Always `datetime64[ns]`, never a plain object/float column.** Two paths would otherwise return a
     dtype that breaks the docstring's promise and the caption's `.dt` access: the empty-API-response
     early return, where an empty, columns-only `DataFrame` defaults every column (including
     `quarter_issue_date`) to `object` dtype — re-cast explicitly with `pd.to_datetime` (`db.py:988`); and
     the final `_convert_na_to_nan`/sort step, whose `infer_objects()` cannot distinguish an all-`NaT`
     (degraded-mode) datetime column from an all-`NaN` float column and returns `float64` — re-cast again
     as a no-op when already `datetime64[ns]` (`db.py:1123-1129`).
5. **Dedup per target quarter.** One row per `(code, model_short, year, quarter_in_year)`, plus
   `horizon_value` under the flag. Several target quarters per model are returned.
   - **No quarter EM.** Before the dedup, drop rows whose upper-cased `model_short` is `EM` or
     `ENSEMBLE_MEAN` (the API spelling is `EM`, `sapphire/services/postprocessing/app/models.py:38`). This
     mirrors FD-030's "EM is never a candidate" and covers the card and the bulletin input. Old rows stay in
     the DB (round-2 decision 2) but are not shown.
   - **EM interim (until PP-065 P1b ships).** PP-064 A still writes fresh quarterly EM rows today:
     `ensemble_calculator.py` sets `model_short = "EM"` directly in the quarter aggregation path
     (`_create_aggregated_ensemble_forecasts:765`), and `api_writer.py` (its quarter-write loop,
     `:1157-1158`) resolves that through `MODEL_TYPE_MAP`'s identity `"EM": "EM"` entry (line ~27) — not
     the `"ENSEMBLE_MEAN": "EM"` entry (line 50), which is a separate mapping used only by the
     skill-metrics write path, not the quarter forecast write path. This plan's dedup above already drops
     every quarter EM row it reads, consistent with the owner decision of no quarterly EM, but PP-065 P1b
     is what stops the write. Until P1b ships, a quarter whose only rows are a fresh EM row plus a
     non-native LR row shows **nothing** on the card: the EM row is dropped here and the LR row is dropped
     by the native-only rule below.
   - A row is **native** iff `date.day == issue_day` **, clamped to the issue month's length** (item 4
     already computes `quarter_issue_date` with this clamp — `clamped_issue_day =
     np.minimum(int(schedule.issue_day), days_in_issue_month)`; mirrors the producer,
     `apps/long_term_forecasting/lt_utils.py:170-172 nearest_scheduled_issue_date`) **and** the year-aware
     lead `(valid_from.year − date.year)·12 + (valid_from.month − date.month)` == `lead_time`, i.e. `date`
     == `issue_date(Q)` of item 4 (the PP-064 Contract rule; the formula of
     `apps/postprocessing_forecasts/src/data_reader.py:346-348`; identical to PP-065 P1b).
   - **`is_native` column.** Add a boolean `is_native` (the predicate above) to every returned row; False
     for every row when degraded (item 3). The native preference and the LR strictness below, the card and
     FD-030 use this column wherever LR nativeness matters; nothing re-derives the rule. The renderer's
     `reindex(columns=expected_cols)` (`src/vizualization.py:3211`) drops `is_native` and
     `quarter_issue_date`, so the tabulator is unaffected.
   - **All models: prefer the native row.** `date` is part of the `long_forecasts` natural key
     (`sapphire/services/postprocessing/app/models.py:193-201`), so under flag ON legacy rows dated
     `valid_from` coexist with fresh rows dated at the issue date, and "latest `date`" alone would pick the
     legacy row. Rule: the native row if one exists; otherwise the latest `date`, ties broken by the
     highest API `id`.
   - **Known limitation (accepted): rollback from flag ON to OFF — applies to today's ensemble rows too,
     not only a hypothetical future case.** Rows written while `SAPPHIRE_SKILL_LEAD_AWARE` was ON are
     native-shaped (`date` = the schedule issue date, the Contract rule) whenever their `date` is
     non-null and `horizon_type == "quarter"` — `api_writer.py`'s `record_date` logic stamps
     `record_date = date` under that condition regardless of `model_short` (`:1264-1269` on PP-064's
     branch `fix_pp_quarter_calendar_window`, which shifts this from trunk's `:1199-1204`). This is not
     LR-only: postprocessing's own quarterly ensemble aggregation (`ensemble_calculator.py`) carries the
     `date` column through with `agg("first")` for EM (`_create_aggregated_ensemble_forecasts:758`),
     Skilled Mean (`_add_skilled_mean_aggregated_ens:865`) and Naive Mean
     (`_add_naive_mean_aggregated_ens:906`) — so an ensemble row built (today) from a native LR member
     inherits that member's native `date`, gets stamped as `record_date` the same way, and is then
     classified native by the `is_native` predicate here. Test 15 already fixes this in place (flag-ON
     fresh `Naive Mean`/`Skilled Mean` rows dated at the issue date, i.e. native-shaped) — this bullet
     states the general rule test 15 is an instance of, not a separate future concern.
     - **(c) persisted monthly-derived rows are the exception — on kghm only.** Under flag ON those are
       dated `valid_from − horizon_value months` ("How quarter rows are dated in the DB" above, e.g. 1
       Mar for a kghm hv1 Q2), so `date.day` is generally `1`, not the configured `issue_day` (25 for
       kghm) — `is_native` is already False for them via the predicate above, and they never win the
       dedup over a fresh flag-OFF row on that account. **This does not hold for tjhm** (lead 0, issue
       day 1): there, `valid_from − 0 months = valid_from`, whose day already **is** 1 — the exact
       configured `issue_day` — so a (c) row is native-shaped by the same coincidence already noted for
       (b) ("How quarter rows are dated in the DB" above: "a (b) row has the native key and is the same
       DB row as (a)"). **Owner decision 2026-09-26: accept and document this interim on tjhm** — until
       PP-065 P1b (the writer stops writing raw LR rows) and the decision-F cleanup land, tjhm's
       monthly-derived LR_Base/LR_SM quarter rows are indistinguishable from genuinely native ones by
       this predicate, so the card and the bulletin show them as native LR. kghm is unaffected. No code
       change; see the overview's decisions section.
     - **The rollback mechanism.** After a rollback to OFF, fresh rows (LR or ensemble) are re-dated to
       `valid_from` (`api_writer.py:1264-1269`, PP-064 branch) and are non-native for any mode whose lead
       is not 0 (e.g. kghm, lead 1). The dedup sorts `is_native` ahead of `date` (`src/db.py:1108-1121`:
       `sort_values(["is_native", "date"], ascending=[False, False])`, `drop_duplicates(..., keep="first")`),
       so an older flag-ON native row — LR **or ensemble** — keeps outranking a newer flag-OFF rewrite
       for the same `(code, model_short, year, quarter_in_year)` until the old native row is deleted or a
       fresh write lands at its exact key. Unlike the "Flag OFF" bullet below, the upsert does not clear
       this twin: `date` is part of the natural key, so a flag-OFF rewrite (dated `valid_from`) and the
       old flag-ON native row (dated the issue date) occupy different keys and both persist. Accepted as
       a documented rollback caveat, not a defect this plan fixes. **A flag rollback must also remove
       the ensemble twins**, not only LR's — added to PP-064 Chunk C (rollout).
   - **Move the `id` drop.** Today (trunk `src/db.py:852`) `id` is dropped **before** the dedup, together
     with `horizon_type` in one `drop_cols` list,
     so the tie-break has nothing to read. Drop `id` after the dedup instead; the `horizon_type` and flag-OFF
     `horizon_value` drops stay where they are. When the response has no `id` column, keep today's order
     (existing mocks do not all carry `id`).
   - **LR_Base / LR_SM: native only (stricter) — effective on kghm today; on tjhm only after PP-065 P1b +
     decision F.** Non-native LR rows ((b), (c)) are **never** returned; with no native row the quarter
     has no LR row. On **kghm** (lead 1) this filters real rewrites/persisted-derived rows out, since
     their `date.day` differs from the configured `issue_day`. On **tjhm** (lead 0, issue day 1) it has
     **no effect today**: (b) and (c) rows share the native key by construction (above), so this
     predicate cannot tell them apart from a genuine native issuance until PP-065 P1b stops writing raw
     LR rows and decision F cleans up the DB — until then, tjhm's card and bulletin show the same
     monthly-derived values labelled as native LR. Log the dropped count **at INFO, with the station
     `code`** (`db.py:1082-1100`) — not a WARNING: under flag OFF (kghm) a persisted LR rewrite dated at
     `valid_from` is non-native in **steady state**, so this fires on every reservoir-station load and
     every bulletin site, not as an anomaly; the code is included to match the neighbouring INFO/"no
     data" lines in this same function (dashboard logs are local and already log codes).
   - **Flag OFF.** Fresh non-LR rows (the seven derived models, `Naive Mean`, `Skilled Mean`) are dated
     `valid_from` (`api_writer.py:1264-1269`, PP-064 branch) and fall through to "latest `date`". For kghm
     the legacy hv1
     rows with the same key are overwritten by the upsert, so no stale twin remains; for tjhm `valid_from`
     is the issue date, so fresh rows are native.
6. **Card selection** (`update_quarterly_summary_tabulator`, `plot_manager.py:393-550`; the selection
   logic at `:437-481`).
   - Select the station's **latest eligible target quarter** (max `valid_from`) **over DISPLAYABLE rows
     only** (`:462-471`) — rows the renderer would actually keep — not over every row
     `get_long_forecasts_quarter` returned. Mirrors all **three** of the renderer's own filters
     (`create_forecast_summary_table`, `src/vizualization.py`), named explicitly: `model_short` in
     `model_checkbox.options` (via `model_selection.options.values()`); a non-null
     `forecasted_discharge` (the renderer's own null-discharge drop); and a non-null `date` (the
     renderer's `date <= date_picker + 1 day` comparison is always `False` against a `NaT` `date`, so it
     drops those rows too — a row that is in-options with non-null discharge but a `NaT` `date` would
     otherwise pass the first two filters and render a visible card with an empty table). Selecting over
     every row, including ones the renderer would drop anyway, could pick a quarter whose displayable
     rows are all filtered out downstream — an empty table with a caption for that quarter, where trunk
     fell back to an older displayable quarter.
   - Once the latest displayable quarter is identified (by its `valid_from`), select **all** of that
     quarter's rows from the original (not the displayable-filtered) set (`:476-477`) — a row the
     displayable filter excluded still belongs to the selected quarter; only the *quarter choice* is
     computed over displayable rows, not the final row set handed to the renderer.
   - **Hide the card entirely** (`card.visible = False`) when **no** row is displayable at all
     (`:472-474`) — not an empty table with a caption, which is what selecting over every row could
     produce.
   - Pass `max(date)` **of the selected rows** as `date_picker`, so the renderer's
     `date <= date_picker + 1 day` filter (`vizualization.py:3147-3153`) keeps them all, and call the
     renderer with `filter_by_date=False`, which skips only the max-date reduction (`:3166-3170`).
   - The renderer also filters on `model_selection.options`, which come from the **monthly**
     `forecasts_all` (`dashboard/data_manager.py:269-281, 287-290`). A quarter model with no monthly rows
     is hidden. This dependency stays; the seven derived models, `Naive Mean` and `Skilled Mean` are monthly
     models too, so the card shows them when present.
   - **Fallback quarters have no LR row (accepted, round-2 decision 3).** PP-065's temporary LR fallback
     is not persisted, so until LTF-014 P0/P2 a quarter without a native LR row shows the seven models and
     the ensembles but no LR_Base/LR_SM. This goes into the hydromet notice.
   - **tjhm interim: monthly-derived LR shown as native (owner decision 2026-09-26, accepted).** Where a
     (b)/(c) row *does* exist for tjhm, the "native only" rule above cannot hide it — it is shown on the
     card and in the bulletin as if it were a genuine native LR issuance, until PP-065 P1b and decision F
     land. kghm is unaffected. This also goes into the hydromet notice.
   - **An explicit `horizon_value` affects only the request filter and the fetch window, never
     eligibility or nativeness** (`db.py:833-841`, docstring): it widens/narrows the API `horizon_value`
     filter and sizes `fetch_lead` (item 1), but the eligibility cutoff (item 4) and the native predicate
     (this item) always follow the configured schedule's own `lead_time`, never this override. No
     production caller passes `horizon_value` explicitly today.
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
   - Issue date = the selected rows' `quarter_issue_date`, i.e. the schedule-computed `issue_date(Q)` of
     item 4 (one value per target quarter by construction). It does not depend on a native LR row, so a
     fallback quarter still gets its issue date: tjhm Q1 2027, fallback-derived while LTF-014 P0 is deferred,
     reads "1st of January 2027".
   - Only in degraded mode (item 3) show the period and "issue date not available". A row's `date` is never
     shown as an issue date.

**Behaviour after — bulletin input (accepted in the interim, overview decision E).** The three bulletin
blocks call the same function with `head(1)` on the latest `date`. After P1:
- rolling-window, non-native LR, quarter `EM` and not-yet-eligible rows are no longer returned;
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
4. **Renderer, through the card.** LR rows dated 2026-03-25 and a `Skilled Mean` row dated 2026-04-01, all
   Apr–Jun, run through `PlotManager.update_quarterly_summary_tabulator`, **not** the renderer directly, so
   the card's selection and `filter_by_date=False` are exercised. Build the `PlotManager` as in
   `tests/test_widgets.py:60-140` (`object.__new__(PlotManager)`, `_make_stub_pm` at `:75-101`) and set:
   `_` = identity; `_cfg.viz` = the real `src.vizualization` module (as `dashboard/config.py:196`);
   `summary_table_q_card`; `_dm.sites_list` with a reservoir site and `_dm.long_forecasts_quarter` with
   `station_labels`; `_wm` with horizon `"month"`, the station selector, `model_checkbox.options` including
   LR_Base, LR_SM and `Skilled Mean`, range selector/slider, date picker, `forecast_tabulator_q` from
   `widgets.create_forecast_tabulator()` and `forecast_info_q` → the tabulator holds all three models.
   (Live defect for kghm, Problem 3.)
5. **Older quarter with a later issue date.** GBT: a Q3 row dated 2026-06-25 plus a backfilled Q2 row
   dated 2026-07-02, in shuffled order, `today=2026-07-10` → the card shows Q3.
6. **Stale site attributes.** The site carries `quarterly_valid_from/to` for Q2 while the selected rows are
   Q3 → the caption says Jul–Sep.
7. **tjhm lead 0, fallback-derived Q1.** No LR row (tjhm Q1 2027 is fallback-derived while LTF-014 P0 is
   deferred); a GBT and a `Naive Mean` row dated 2027-01-01, Jan–Mar 2027, `today=2027-01-02` → the caption
   reads "Jan 2027 – Mar 2027" **and** "1st of January 2027", from the schedule. This fixture's "no LR
   row" premise is idealized: in practice, until PP-065 P1b + decision F land, postprocessing may still
   have written a (b)/(c) LR row for this tjhm quarter, which — per the tjhm interim accepted above —
   would be shown as native LR here rather than absent. This test only covers the genuinely-empty case.
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
    Q2 GBT rows are returned, the Q3 row (dated after `today`) is not; `is_native` is False on every row;
    caption "issue date not available".
12. **Quarter with no native LR (fallback, accepted).** kghm schedule, flag OFF, `today=2027-01-05`: for
    Q1 2027 an LR_Base rewrite, GBT, `Naive Mean` and `Skilled Mean` rows, all dated 2027-01-01 → the card
    holds GBT, `Naive Mean` and `Skilled Mean` and **no** LR_Base; `is_native` is False on every returned
    row; caption "Jan 2027 – Mar 2027" and "25th of December 2026" (schedule, not "issue date not
    available").
13. **Eligibility, tjhm.** tjhm schedule, `today=2026-09-26`: a `Naive Mean` Q4 row dated 2026-10-01 (Q4
    is fallback-derived while LTF-014 P0 is deferred) and a native LR Q3 row dated 2026-07-01 → only Q3 is
    returned, its `is_native` is True; the card shows Jul–Sep and "1st of July 2026".
14. **Eligibility, kghm Dec 25.** kghm schedule, flag OFF, `today=2026-12-25`: a `Skilled Mean` Q1 2027
    row dated 2027-01-01 plus the native LR Q1 rows dated 2026-12-25 → Q1 2027 is returned and selected
    by the card, with the Skilled Mean row visible.
15. **Fresh vs legacy rows, all models.** kghm schedule, `today=2027-01-05`, Q1 2027 rows for GBT,
    `Naive Mean` and `Skilled Mean`, fresh and legacy with **different values**:
    - flag ON: fresh rows dated 2026-12-25 (native) plus legacy rows dated 2027-01-01 (`valid_from`,
      later) → the fresh values are returned and shown;
    - flag OFF: fresh rows dated 2027-01-01 (`valid_from`) plus legacy rows dated 2026-12-01 (not native)
      → the fresh values are returned and shown.
16. **No quarter EM.** Q2 rows for GBT, `Naive Mean` and an old `EM` row, `EM` also in
    `model_selection.options` → `get_long_forecasts_quarter`'s result does not hold `EM` (only the `db.py`
    result is asserted here; the card is not separately tested in this case, though it consumes that
    same result, so `EM` cannot reach it either).
17. **`id` tie-break.** Two non-native GBT rows for the same quarter, same `date`, different values, the
    lower-`id` row first in the response → the higher-`id` value is returned, and `id` is not a result
    column.
18. **Fetch window, schedule-derived and widened (added after review rounds; `TestGetLongForecastsQuarterFetchWindow`
    in `tests/test_db.py`).** A tightly schedule-sized window kept missing cases; these lock the wider one:
    `test_c1_missed_lt_run_does_not_narrow_below_the_spec_window` (C1: a missed LT run for the previous
    calendar quarter must not empty the card — the `(12 + fetch_lead)`-months-back lower bound reaches
    it where a `(3 + lead)`-months-back bound alone would not);
    `test_c2_degraded_window_uses_resolved_horizon_value_as_lead` (C2: in degraded mode the window is
    sized off the *resolved* `horizon_value`, not a fixed guess);
    `test_w1_lead4_flag_off_row_dated_next_quarter_start_is_fetched` (W1: lead>=4 — a flag-OFF row dated
    at the *next* quarter's own `valid_from` needs the widened upper bound);
    `test_w1_lead0_early_january_still_reaches_older_eligible_quarter` (W1: a lead-0 config's issue day
    can still push a narrower lower bound past an eligible OLDER quarter in early January);
    `test_w2_explicit_horizon_value_widens_window_beyond_schedule_lead` (W2: an explicit `horizon_value`
    override wider than the schedule's own lead must widen the window, not stay narrowed to the
    schedule's lead); and `test_negative_configured_lead_does_not_narrow_below_lead_zero` (a
    misconfigured negative lead must clamp `fetch_lead` to 0, never narrowing the window below lead 0's
    reach).
19. **Calendar-window edge case** (`TestGetLongForecastsQuarterCalendarOnly::test_valid_to_mismatch_excluded`).
    A `valid_from` that is a clean quarter start (day 1, month 4) but a `valid_to` one month too long
    (Jul 31 instead of the Q2-correct Jun 30) is excluded — only the `valid_to` equality predicate catches
    this, not the day/month checks alone.
20. **EM exclusion, case-insensitive** (`TestGetLongForecastsQuarterNativeSelection::
    test_no_quarter_em_returned_case_insensitive`). `ensemble_mean`, `ENSEMBLE_MEAN`, `em` and `Em` are
    all excluded, not only an exact-case match against `EM`/`ENSEMBLE_MEAN`.
21. **Caption issue date is the schedule's, not the row's**
    (`TestCardSelectionThroughPlotManager::test_caption_issue_date_is_the_schedule_date_not_the_row_date`
    in `tests/test_quarter_calendar_card.py`). A fallback-derived row dated at its own `valid_from`
    (2027-01-01) but whose `quarter_issue_date` is schedule-computed (2026-12-25) must show "25th of
    December 2026" in the caption, never "1st of January 2027" — a mutation reading the row's own `date`
    instead of `quarter_issue_date` would pass every other caption test undetected.
22. **Displayable-rows card selection and hide-on-none (added after review rounds).**
    `TestCardSelectionThroughPlotManager::test_v1_selection_falls_back_when_newest_quarter_is_not_displayable`
    (the newest quarter's rows are all filtered by the renderer's own filters — the card falls back to an
    older, displayable quarter instead of showing an empty table) and `::test_y2_nat_dated_row_hides_the_card`
    (a single in-options, non-null-discharge row with `date = NaT` must still hide the card — it would
    otherwise pass the first two displayable filters and render an empty table), both in
    `tests/test_quarter_calendar_card.py`.
23. **Non-native LR drop logged at INFO with the station code, not WARNING.**
    `TestGetLongForecastsQuarterNativeSelection::test_v2_non_native_lr_drop_logs_one_aggregated_info_line`
    (`tests/test_db.py`) — asserts `levelname == "INFO"` and that the station code appears in the
    message; a WARNING would fail it. Renamed from an earlier `..._warning` version once the level
    changed.
24. **Mutation-gap tests added alongside the above** (`tests/test_db.py` unless noted): `is_native`
    compares the full `date`, not just the day-of-month
    (`test_is_native_checks_full_date_not_just_day`); the fetch window's lower bound anchors on the
    start of today's calendar quarter, not on today's own month
    (`test_window_anchors_on_quarter_start_not_todays_month`); the degraded-mode lead comes from the
    resolved `horizon_value`, not a hardcoded constant (`test_degraded_window_uses_resolved_lead_not_a_constant`);
    and the δ-fill only fills null `Q25`/`Q75`, never overwrites native quantiles
    (`test_delta_fill_preserves_native_bounds`, `tests/test_quarter_calendar_card.py`).

**Acceptance**
- Tests 1–24 fail on trunk and pass after (test 15 via its flag-ON case).
- The full module suite passes with **no existing test edited**:
  `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` gives zero failures. The only
  allowed skips: `tests/test_docker.py:22` (no Docker daemon) and the Playwright tests, gated by
  `importorskip` (`tests/test_integration.py:7`, `tests/test_integration_bea.py:9`) plus the env
  `skipif` (`TEST_PENTAD`/`TEST_DECAD`/`TEST_LOCAL`).
- `git diff --stat` is limited to the listed files.

**Deploy.** Precondition: PP-064 Chunk C step 0's per-org `quarter.json`/`issue_day` read (see item 3's
accepted limitation above) — confirm both orgs' schedule config resolves before deploying, so this card
does not silently run in degraded mode. Rebuild the dashboard image, redeploy it and restart the dashboard
container on kghm and tjhm **before 2026-12-25**. To check: the card exists only on the month horizon, for
reservoir stations.

## Out of scope

- The bulletin's quarterly section (FD-030).
- The quarter `forecast_year` in `get_bulletin_metadata` (FD-019; unreachable).
- The import-time years for other horizons (`src/db.py:29-30`; overview, deferred findings).
- **`get_forecast_stats`' import-time skill window** (`src/db.py:679-684`: `{PREVIOUS_YEAR}-12-31` …
  `{CURRENT_YEAR}-12-31`). Quarter skill rows are dated in the recalc year, so a dashboard restarted in
  January, before that month's recalc, has no quarter skill and therefore no δ (empty bounds) until the
  recalc writes the new year's rows.
- The quarter skill merge in `_get_data_monthly` (`src/db.py:1365-1389`) is not lead-filtered. Under
  flag OFF, quarter skill is written at the sentinel hv 0 only
  (`apps/postprocessing_forecasts/src/api_writer.py:661-669`), so the merge is 1:1; a fan-out needs skill
  rows at more than one `horizon_value` (pre-existing).
- Exposing "quarter" as a horizon.
