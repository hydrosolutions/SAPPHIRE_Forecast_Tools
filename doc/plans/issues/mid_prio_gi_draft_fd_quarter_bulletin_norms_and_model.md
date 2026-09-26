# FD-030: The month bulletin's quarterly section uses the monthly norm, and publishes an arbitrary model

**Status**: Draft (2026-09-26, rev 6 after the fourth review round). **Blocked on owner decisions
D6a–D6c below**; the agent brief in P1 applies once they are recorded in this file.
**Module**: `apps/forecast_dashboard`
**Priority**: Medium. The quarterly section of reservoir bulletins publishes wrong numbers today: % of norm,
norm volume and last-year value.
**Labels**: `forecast_dashboard`, `bulletin`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md) (decisions B as
revised in round 2 — quarterly ensembles are Naive Mean + Skilled Mean only, no quarterly EM — plus D and
E of 2026-09-26; overview D6 is split into D6a–D6c here)
**Related**: FD-029 (changes the shared input of the three bulletin blocks, see its "Behaviour after —
bulletin input"; its selected rows are this plan's input), PP-065 (produces the derived models and the
quarterly Naive Mean / Skilled Mean), PREPQ-008 (the quarter hydrograph write is rejected on deployments
whose API lacks `horizon_type=quarter`), PR-QHN-001
(`high_prio_gi_draft_preprocessing_runoff_quarterly_hydrograph_norms.md`)

**Interim (decision E).** No guard is added before PP-065 deploys. The bulletin publishes whatever is
available until this plan is implemented. The interim bulletin has **no δ bounds**: the three blocks take
`head(1)` of the latest row and read `Q25`/`Q75` only, so when that row is a derived model or an ensemble
with a derived member, the quarterly range prints blank (the section is still shown, Problem 3). δ bounds
reach the bulletin only with this plan.

## Problems (trunk `82946683`)

1. **Norm, % of norm and norm volume use the MONTHLY norm.**
   - `get_quarterly_forecast_attributes_for_site` sets `forecast_norm_q = self.hydrograph_norm`
     (`src/site.py:357-359`). That is the bulletin target **month's** norm, hydrated by
     `hydrate_month_hydrograph_stats` (`dashboard/utils.py:6-49`, called at `dashboard/bulletin_manager.py:392`).
   - The quarterly `Q_LAST_YEAR` tag reads `forecast_prevyear_q`, which is **shared with the monthly
     section** (`src/bulletins.py:1304` monthly, `:1339` quarterly; set from `month_last_year_q` at
     `src/site.py:269`).
   - Calendar-quarter norms exist as `horizon_type=quarter` hydrograph rows, 4 per station and snapshot
     year, dated on the quarter's first day of the snapshot's `target_year`, with `norm`, `previous` and
     `current` (`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py:137, 641-668`). Nothing reads them.
   - The generic fetch `get_hydrograph_pentad_all` accepts a horizon, but it fixes the query window and
     renames `previous`/`current` using import-time years (`src/db.py:373-396`). So it cannot answer
     "the norm and last-year value for Q1 2027" safely.
2. **One arbitrary model, and no binding to the bulletin's target period.** Each of the three blocks
   (`dashboard/bulletin_manager.py:394-399` in `_populate_forecast_attributes`, `:760-765` in `_on_add`,
   `:890-895` in `_on_add_m0`) takes the latest-dated row across all models with `head(1)`.
   - Under flag OFF, every derived and ensemble row shares `date = valid_from` (PP-064 population b), so
     the tie is broken by row order.
   - Nothing ties the chosen quarter to the bulletin's month/year. Reopening an old bulletin can pick up a
     newer quarter.
   - The VNORM/volume seconds come from the chosen row's window (`bulletin_manager.py:401-405`), so a
     rolling-window row can be paired with a calendar-quarter norm.
3. **Blank range for derived rows.** The section is range-only: `Q_MIN`/`Q_MAX`/`V_MIN`/`V_MAX`, norm,
   last year, % (`src/bulletins.py:1329-1346`); there is no point-value tag. The bounds come from
   `Q25`/`Q75` (`bulletin_manager.py:76-89`), and `_fmt_discharge` blanks NaN (`src/bulletins.py:937-942`).
   Derived quarter rows and ensembles with a derived member have null `Q25`/`Q75`, so the range prints
   blank while the section is still shown: the `has_quarterly` gate (`src/bulletins.py:1223-1227`) tests
   `is not None`, and NaN passes.
4. **Section gating and labels.**
   - The section is shown whenever any reservoir has a non-None quarterly bound (`:1223-1227`). Under the
     calendar schedule, bulletins in a quarter's 2nd and 3rd months will carry that quarter again.
   - Shared quarter labels come from the first site's bounds (`src/bulletins.py:965-988`). Nothing checks
     that all reservoirs in one bulletin refer to the same quarter.
   - No issue date is stored on the site (`src/site.py:369-376` stores only the window).
5. **`PERC_PREVYEAR` is always blank.** `perc_prevyear_q` is hard-coded None (`src/site.py:367`), as are the
   monthly and seasonal `perc_prevyear` (`src/site.py:266, 289, 313`). **Not changed by this plan:** the
   new quarterly last-year attribute is used for `Q_LAST_YEAR` only, and `PERC_PREVYEAR` stays blank in all
   sections. Computing it is a separate owner request.

## Owner decisions needed

(Named D6a–D6c to avoid a clash with the overview's D1–D3.)

- **D6a. Product.** Which product the section publishes: **Skilled Mean**, **Naive Mean**, or a named model.
  There is no quarterly Ensemble Mean after PP-065 (owner round 2); old quarter `EM` rows in the DB are
  never selected. Quarterly ensembles follow PP-065: Naive Mean = mean of all raw quarter models, no skill
  gate on membership, but it needs at least two members (`is_multi_model_composition`,
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:915`) and a non-empty quarter skill frame
  (the run-level skip, PP-064 B5; `postprocessing_operational_long_term.py:209`); Skilled Mean =
  long-term gate (NSE > 0) plus K = 10, 1/MAE-weighted, so it can legitimately be absent (e.g. tjhm with
  little scored history). Either ensemble can therefore be missing, so D6a must fix the fallback order
  when the chosen product has no row (e.g. Skilled Mean → Naive Mean → none), or "blank if missing". Any
  such order can end in **none**: that reservoir then has no chosen row and its quarterly bounds stay
  None, so the `has_quarterly` gate (`src/bulletins.py:1223-1227`) hides the section when no reservoir
  has a row.
- **D6b. Presentation.**
  - Eligible quarter for a bulletin with target month M/year Y: the quarter containing M, or the latest
    quarter issued on or before the bulletin date? What is the issuance cutoff for a reopened bulletin?
  - In a quarter's 2nd and 3rd months: show the current quarter again, or hide the section?
  - One shared quarter label, or a per-reservoir period?
- **D6c. Norm reference year.** For a Q1 issued on Dec 25–31, the next-year snapshot (dated Jan 1) may not
  exist yet. Either (a) require it (norm and last year blank until written), or (b) read the Q1 row of
  the current-year snapshot and take last year from its `current` field. (b) relies on the field
  semantics of `build_quarterly_records`; the implementing agent verifies them before relying on it.
  - PREPQ-008 status on kghm and tjhm is a precondition, not a decision.
- **Optional (ops).** Add a point-value tag and a model/issue-date tag to the quarterly section. Deployed
  templates live in the data repos, so this is a template change and an ops step. Not needed for
  correctness.

## Plan

### P1 — One selection helper, strict quarter hydration, δ bounds (one code agent; after FD-029 and D6a–D6c)

**Files (only these may be modified)**:
- `apps/forecast_dashboard/dashboard/bulletin_manager.py`: replace the three quarter blocks
  (`:394-411`, `:760-777`, `:890-907`) by calls to one new helper
- `apps/forecast_dashboard/dashboard/utils.py` **or** a new `dashboard/quarter_bulletin.py`: the selection,
  δ lookup and quarter hydration helpers
- `apps/forecast_dashboard/src/db.py`: a new year-aware quarter hydrograph fetch (additive function;
  `get_hydrograph_pentad_all` untouched)
- `apps/forecast_dashboard/src/site.py`: `get_quarterly_forecast_attributes_for_site` (norm source, new
  quarterly last-year attribute)
- `apps/forecast_dashboard/src/bulletins.py`: `:1339` (quarterly `Q_LAST_YEAR` tag only), `:1223-1227`
  (`has_quarterly` gate, only if D6b hides the section), `:965-988` (labels, only if D6b chooses a
  per-reservoir period)
- Tests: new `apps/forecast_dashboard/tests/test_bulletin_quarter_section.py`

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Keep:
- the monthly section (sec1) tags and the monthly `forecast_prevyear_q` byte-identical;
- `get_long_forecasts_quarter` as left by FD-029 (call it; do not re-implement its fetch, calendar filter,
  eligibility cutoff or native LR rule);
- `hydrate_month_hydrograph_stats` and the seasonal paths.

**Behaviour after**
1. **One helper** used by `_populate_forecast_attributes`, `_on_add` and `_on_add_m0`. It takes the site
   and the bulletin's target (month, year) and:
   - starts from FD-029's output of `get_long_forecasts_quarter`: calendar quarters only, LR_Base/LR_SM
     native only (non-native LR never returned), every other model the native row if present, else the
     latest `date` with the `id` tie-break (FD-029 item 5; this inherits its fresh-over-legacy rule), one
     row per model and target quarter, eligible quarters only, quarter `EM` rows already dropped. The
     helper does **not** re-apply the native rule, so a flag-OFF Skilled Mean or derived row dated
     `valid_from` stays selectable. Where LR nativeness matters it reads FD-029's `is_native` column
     (False for every row when FD-029 runs degraded); where an issue date is needed (e.g. the optional
     issue-date tag) it reads `quarter_issue_date`, never the row's `date`;
   - selects the D6b-eligible quarter, then the D6a product with its fallback order. `EM` is never a
     candidate (FD-029 already excludes it; the helper keeps the guard).
   - A fallback quarter has no LR row (FD-029 "Behaviour after", item 6; round-2 decision 3), so a named
     LR product is absent there and D6a's fallback order applies.
2. **δ bounds (overview decision D).** When the chosen row has null `Q25`/`Q75`, set the bounds to
   forecast ∓ δ, so `Q_MIN`/`Q_MAX` and `V_MIN`/`V_MAX` are filled. The bulletin path has no skill merge
   (the blocks call `get_long_forecasts_quarter` directly), so the helper reads
   `get_forecast_stats("quarter", code)` (`src/db.py:677`) and matches on `(code, quarter_in_year,
   model_short)` using raw model names (neither frame goes through `i18n_models` on this path).
   - **Lead filter: mirror the flag branch of `_get_data_monthly`** (`src/db.py:1064-1091`, branch on
     `skill_lead_aware_enabled()`), not its values: under flag OFF, monthly filters on
     `horizon_value == 1` (`src/db.py:1088-1091`); quarter must use hv 0.
     - flag ON: keep skill rows with `horizon_value == quarter_horizon_value()`;
     - flag OFF: keep the sentinel rows `horizon_value == 0`. Flag-OFF quarter skill is grouped without hv
       (`apps/postprocessing_forecasts/src/skill_metrics.py:2642-2647`) and written at hv 0
       (`apps/postprocessing_forecasts/src/api_writer.py:661-669`), so filtering to the config lead (1 on
       kghm) would find nothing;
     - if the `horizon_value` column is absent, keep all rows; if `quarter_horizon_value()` raises, log a
       WARNING and use no δ.
   - If δ is missing, the bounds stay empty. **K = 10 (PP-065):** skill rows with fewer than 10 pairs are
     suppressed, so those models have no δ. tjhm may print empty ranges until more history is scored;
     PP-065 P2 measures how often.
   - **Blank range, section shown (recommended).** Keep the `has_quarterly` gate (`src/bulletins.py:1223-1227`)
     unchanged: with a chosen row but no bounds and no δ the section is still rendered, with the range cells
     empty and the norm/last-year cells filled. Hiding it instead would be a D6b choice.
3. **Strict quarter hydration.** Clear the quarter fields first; fill norm and last year from the
   quarter hydrograph row for (station, quarter, D6c year); on a miss, an error or a null field leave them
   empty. Never substitute another year or the monthly norm.
4. **A distinct quarterly last-year attribute**, read by the quarterly `Q_LAST_YEAR` tag (`:1339`) only.
5. **Volume seconds** come from the chosen calendar row, so a legacy rolling row can never be paired
   with a calendar-quarter norm.

**Tests (station `19999`; each must fail on trunk)**. Ensemble fixtures use `Naive Mean` / `Skilled Mean`.
1. **δ range.** Chosen row with null `Q25`/`Q75`, forecast 100, quarter skill `delta` 5.0 → `Q_MIN` 95,
   `Q_MAX` 105, and `V_MIN`/`V_MAX` from the calendar quarter's seconds. Missing δ → blank range cells,
   and the section is still rendered.
2. **δ under flag OFF, kghm-shaped.** Flag OFF, `quarter.json` lead 1 / day 25, skill row at hv 0 with
   `delta` 5.0 → δ is found and `Q_MIN` is 95. Flag ON counterpart: skill rows at hv 0 (`delta` 9.0) and
   hv 1 (`delta` 5.0) → 5.0 is used.
3. **Flag-OFF kghm product selection.** Flag OFF, kghm schedule, Q1 2027: native LR rows dated 2026-12-25
   and `Skilled Mean` and GBT rows dated 2027-01-01 (`valid_from`) → with D6a = Skilled Mean the helper
   picks the Skilled Mean row; with D6a = GBT it picks the GBT row. Neither is dropped as non-native.
4. **Product fallback.** No `Skilled Mean` row, `Naive Mean` present, plus an old `EM` row for the same
   quarter → the D6a fallback order applies and `EM` is never chosen. Neither ensemble present (only an
   old `EM` row) → no chosen row, quarterly bounds None, section hidden for a single reservoir.
5. **All three entry points** (`_populate_forecast_attributes`, `_on_add`, `_on_add_m0`) go through the
   helper (spy on it; each yields the same site fields for the same input).
6. **Norm source.** Quarter norm 50, monthly norm 80, forecast 60 → `PERC_NORM` 120.0.
7. **Strict hydration.** Hydrate the **same site object** successfully, then with a missing quarter row →
   the quarter fields are empty afterwards.
8. **Render two reservoirs** and assert both sections' `Q_LAST_YEAR`, norm and volume cells together; the
   sec1 monthly tags are byte-identical to trunk.
9. **Reopen** an old bulletin after a newer quarterly issue → the old quarter is kept (per D6b).
10. **No rolling/calendar mix.** A legacy rolling-window row is never paired with a calendar-quarter norm
    (volume seconds from the chosen row, `bulletin_manager.py:401-405`).
11. **Fresh over legacy (inherited from FD-029 item 5).** Flag ON, kghm schedule, Q1 2027: a fresh
    `Skilled Mean` row dated 2026-12-25 and a legacy `Skilled Mean` row dated 2027-01-01 with a different
    value, D6a = Skilled Mean → the bulletin publishes the fresh value.

**Acceptance**
- Tests 1–11 fail on trunk and pass after.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes with no existing test
  edited; allowed skips as in FD-029.
- `git diff --stat` is limited to the listed files.
- If the optional tags are chosen: the template change is listed as an ops step in the PR.
