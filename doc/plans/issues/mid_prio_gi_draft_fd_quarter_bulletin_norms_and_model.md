# FD-030: The month bulletin's quarterly section uses the monthly norm, and publishes an arbitrary model

**Status**: Draft (2026-09-26, rev 3 after the owner decisions of 2026-09-26). **Blocked on owner
decisions D1–D3 below**; the agent brief in P1 applies once they are recorded in this file.
**Module**: `apps/forecast_dashboard`
**Priority**: Medium. The quarterly section of reservoir bulletins publishes wrong numbers today: % of norm,
norm volume and last-year value.
**Labels**: `forecast_dashboard`, `bulletin`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md) (decisions B, D, E
of 2026-09-26; overview D6 is split into D1–D3 here)
**Related**: FD-029 (changes the shared input of the three bulletin blocks, see its "Behaviour after —
bulletin input"), PREPQ-008 (the quarter hydrograph write is rejected on deployments whose API lacks
`horizon_type=quarter`), PR-QHN-001 (`high_prio_gi_draft_preprocessing_runoff_quarterly_hydrograph_norms.md`)

**Interim (decision E).** No guard is added before PP-065 deploys. The bulletin publishes whatever is
available until this plan is implemented. With δ bounds (decision D) the blank-range risk mostly goes away.

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
   - Under flag OFF, every model's row shares `date = valid_from` (PP-064 population b), so the tie is
     broken by row order.
   - Nothing ties the chosen quarter to the bulletin's month/year. Reopening an old bulletin can pick up a
     newer quarter.
   - The VNORM/volume seconds come from the chosen row's window (`bulletin_manager.py:401-405`), so a
     rolling-window row can be paired with a calendar-quarter norm.
3. **Blank range for derived rows.** The section is range-only: `Q_MIN`/`Q_MAX`/`V_MIN`/`V_MAX`, norm,
   last year, % (`src/bulletins.py:1329-1346`); there is no point-value tag. The bounds come from
   `Q25`/`Q75` (`bulletin_manager.py:76-89`), and `_fmt_discharge` blanks NaN (`src/bulletins.py:937-942`).
   Derived quarter rows and ensembles with a derived member have null `Q25`/`Q75`, so the range prints
   blank while the section is still shown (the gate tests `is not None`, and NaN passes).
4. **Section gating and labels.**
   - The section is shown whenever any reservoir has a non-None quarterly bound
     (`src/bulletins.py:1223-1227`). Under the calendar schedule, bulletins in a quarter's 2nd and 3rd
     months will carry that quarter again.
   - Shared quarter labels come from the first site's bounds (`src/bulletins.py:965-988`). Nothing checks
     that all reservoirs in one bulletin refer to the same quarter.
   - No issue date is stored on the site (`src/site.py:369-376` stores only the window).
5. **`PERC_PREVYEAR` is always blank.** `perc_prevyear_q` is hard-coded None (`src/site.py:367`), as are the
   monthly and seasonal `perc_prevyear` (`src/site.py:266, 289, 313`). **Not changed by this plan:** the
   new quarterly last-year attribute is used for `Q_LAST_YEAR` only, and `PERC_PREVYEAR` stays blank in all
   sections. Computing it is a separate owner request.

## Owner decisions needed

- **D1. Product.** Which product the section publishes: Ensemble Mean, Skilled Mean, Naive Mean, or a
  named model. The ensembles are defined per overview decision B (quarter ensembles follow the monthly
  rules: NM = mean of all raw quarter models; SM = long-term-gated, inverse-MAE weighted; EM = default
  gate, unweighted, needs more than one model, so it may be absent). D1 must also fix the fallback order
  when the chosen product has no row (e.g. EM → Skilled Mean → none), or "blank if missing".
- **D2. Presentation.**
  - Eligible quarter for a bulletin with target month M/year Y: the quarter containing M, or the latest
    quarter issued on or before the bulletin date? What is the issuance cutoff for a reopened bulletin?
  - In a quarter's 2nd and 3rd months: show the current quarter again, or hide the section?
  - One shared quarter label, or a per-reservoir period?
- **D3. Norm reference year.** For a Q1 issued on Dec 25–31, the next-year snapshot (dated Jan 1) may not
  exist yet. Either (a) require it (norm and last year blank until written), or (b) read the Q1 row of
  the current-year snapshot and take last year from its `current` field. (b) relies on the field
  semantics of `build_quarterly_records`; the implementing agent verifies them before relying on it.
  - PREPQ-008 status on kghm and tjhm is a precondition, not a decision.
- **Optional (ops).** Add a point-value tag and a model/issue-date tag to the quarterly section. Deployed
  templates live in the data repos, so this is a template change and an ops step. Not needed for
  correctness.

## Plan

### P1 — One selection helper, strict quarter hydration, δ bounds (one code agent; after FD-029 and D1–D3)

**Files (only these may be modified)**:
- `apps/forecast_dashboard/dashboard/bulletin_manager.py`: replace the three quarter blocks
  (`:394-411`, `:760-777`, `:890-907`) by calls to one new helper
- `apps/forecast_dashboard/dashboard/utils.py` **or** a new `dashboard/quarter_bulletin.py`: the selection
  and quarter hydration helpers
- `apps/forecast_dashboard/src/db.py`: a new year-aware quarter hydrograph fetch (additive function;
  `get_hydrograph_pentad_all` untouched)
- `apps/forecast_dashboard/src/site.py`: `get_quarterly_forecast_attributes_for_site` (norm source, new
  quarterly last-year attribute)
- `apps/forecast_dashboard/src/bulletins.py`: `:1339` (quarterly `Q_LAST_YEAR` tag only), `:1223-1227`
  (`has_quarterly` gate, only if D2 hides the section), `:965-988` (labels, only if D2 chooses a
  per-reservoir period)
- Tests: new `apps/forecast_dashboard/tests/test_bulletin_quarter_section.py`

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Keep:
- the monthly section (sec1) tags and the monthly `forecast_prevyear_q` byte-identical;
- `get_long_forecasts_quarter` as left by FD-029 (call it; do not re-implement its fetch);
- `hydrate_month_hydrograph_stats` and the seasonal paths.

**Behaviour after**
1. **One helper** used by `_populate_forecast_attributes`, `_on_add` and `_on_add_m0`. It takes the site
   and the bulletin's target (month, year) and:
   - keeps calendar-window rows only and applies the native-row rule of FD-029 / PP-064 B2
     (`date.day` == configured issue day and year-aware lead == configured lead,
     `operational_schedule_for_mode("quarter")`);
   - selects the D2-eligible quarter, then the D1 product with its fallback order.
2. **δ bounds (overview decision D).** When the chosen row has null `Q25`/`Q75`, set the bounds to
   forecast ∓ δ, so `Q_MIN`/`Q_MAX` and `V_MIN`/`V_MAX` are filled. δ is the quarter skill `delta` of the
   chosen `(code, quarter_in_year, model_short)` at the configured quarter lead. The bulletin path has no
   skill merge (the blocks call `get_long_forecasts_quarter` directly), so the helper reads it via
   `get_forecast_stats("quarter", code)` (`src/db.py:677`) and filters to `horizon_value` =
   `quarter_horizon_value()`. If δ is missing, the bounds stay empty.
3. **Strict quarter hydration.** Clear the quarter fields first; fill norm and last year from the
   quarter hydrograph row for (station, quarter, D3 year); on a miss, an error or a null field leave them
   empty. Never substitute another year or the monthly norm.
4. **A distinct quarterly last-year attribute**, read by the quarterly `Q_LAST_YEAR` tag (`:1339`) only.
5. **Volume seconds** come from the chosen calendar row, so a legacy rolling row can never be paired
   with a calendar-quarter norm.

**Tests (station `19999`; each must fail on trunk)**
1. **δ range.** Chosen row with null `Q25`/`Q75`, forecast 100, quarter skill `delta` 5.0 → `Q_MIN` 95,
   `Q_MAX` 105, and `V_MIN`/`V_MAX` from the calendar quarter's seconds. Missing δ → blank range.
2. **Native-rule selection.** Native LR rows, a flag-OFF rewrite dated `valid_from` and a rolling row →
   the helper picks per D1 among calendar, native-rule rows only.
3. **All three entry points** (`_populate_forecast_attributes`, `_on_add`, `_on_add_m0`) go through the
   helper (spy on it; each yields the same site fields for the same input).
4. **Norm source.** Quarter norm 50, monthly norm 80, forecast 60 → `PERC_NORM` 120.0.
5. **Strict hydration.** Hydrate the **same site object** successfully, then with a missing quarter row →
   the quarter fields are empty afterwards.
6. **Render two reservoirs** and assert both sections' `Q_LAST_YEAR`, norm and volume cells together; the
   sec1 monthly tags are byte-identical to trunk.
7. **Reopen** an old bulletin after a newer quarterly issue → the old quarter is kept (per D2).
8. **No rolling/calendar mix.** A legacy rolling-window row is never paired with a calendar-quarter norm
   (volume seconds from the chosen row, `bulletin_manager.py:401-405`).

**Acceptance**
- Tests 1–8 fail on trunk and pass after.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes with no existing test
  edited; allowed skips as in FD-029.
- `git diff --stat` is limited to the listed files.
- If the optional tags are chosen: the template change is listed as an ops step in the PR.
