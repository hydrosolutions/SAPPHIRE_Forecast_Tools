# FD-030: The month bulletin's quarterly section uses the monthly norm, and publishes an arbitrary model

**Status**: Draft (2026-09-25, rev 2). **Blocked on owner decisions D1–D3**; not implementation-ready.
**Module**: `apps/forecast_dashboard`
**Priority**: Medium. The quarterly section of reservoir bulletins publishes wrong numbers today: % of norm,
norm volume and last-year value.
**Labels**: `forecast_dashboard`, `bulletin`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md)
**Related**: FD-029 (its `get_long_forecasts_quarter` changes feed this section too), PREPQ-008 (the quarter
hydrograph write is rejected on deployments whose API lacks `horizon_type=quarter`), PR-QHN-001
(`high_prio_gi_draft_preprocessing_runoff_quarterly_hydrograph_norms.md`)

## Problems (trunk `82946683`)

1. **Norm, % of norm and norm volume use the MONTHLY norm.**
   - `get_quarterly_forecast_attributes_for_site` sets `forecast_norm_q = self.hydrograph_norm`
     (`src/site.py:357-363`). That is the bulletin target **month's** norm, hydrated by
     `hydrate_month_hydrograph_stats` (`dashboard/utils.py:6-49`, called at `dashboard/bulletin_manager.py:390`).
   - The quarterly `Q_LAST_YEAR` tag reads `forecast_prevyear_q`, which is **shared with the monthly
     section** (`src/bulletins.py:1304` monthly, `:1339` quarterly).
   - Calendar-quarter norms exist as `horizon_type=quarter` hydrograph rows, 4 per station, each dated
     with its reference year (`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py:137, 621-668`),
     but nothing reads them.
   - The generic fetch `get_hydrograph_pentad_all` accepts a horizon, but it fixes the query window and
     renames `previous`/`current` using import-time years (`src/db.py:373, 391`). So it cannot answer
     "the norm and last-year value for Q1 2027" safely.
2. **One arbitrary model, and no binding to the bulletin's target period.** Each of the three blocks
   (`bulletin_manager.py:393-399, 761-765, 891-895`) takes the latest-dated row across all models with
   `head(1)`.
   - Under flag OFF, every model's row shares `date = valid_from` (PP-064 population b), so the tie is
     broken by row order.
   - Nothing ties the chosen quarter to the bulletin's month/year. Reopening an old bulletin can therefore
     pick up a newer quarter.
3. **Section gating and labels.**
   - The section is shown whenever any quarter row exists (`plot_manager.py:391-403`, `src/bulletins.py:1223-1227`).
     Under the calendar schedule, bulletins in a quarter's 2nd and 3rd months will carry that quarter again.
   - Shared quarter labels come from the first site's bounds (`src/bulletins.py:965-988`). Nothing checks
     that all reservoirs in one bulletin refer to the same quarter.
   - No issue date is stored on the site (`src/site.py:369`).

## Owner decisions needed (overview D6)

- **D1. Selection.** Which quarter is eligible for a bulletin with target month M/year Y:
  - the quarter containing M, or the next quarter issued on or before the bulletin date?
  - What is the issuance cutoff for a reopened bulletin?
  - Which model is published (e.g. Ensemble Mean = mean(LR_Base, LR_SM) per M1, or the operator's
    monthly choice)?
  - What is the fallback if that model is missing?
- **D2. Presentation.** In a quarter's 2nd and 3rd months, show the current quarter (with its issue date)
  or hide the section? One shared quarter label, or a per-reservoir period?
- **D3. Norm reference.** For a Q1 issued in December, use the climatology snapshot already published, or
  require the next-year snapshot?
  - PREPQ-008 status on kghm and tjhm is a precondition, not a decision.

## Implementation outline (to be expanded after D1–D3; not yet an agent brief)

- **One shared helper** used by all three bulletin blocks: select the D1-eligible quarter and model.
- **A strict quarter hydration helper:**
  - clear the quarter fields first;
  - fill them from the quarter hydrograph row for (station, quarter, D3 reference year);
  - leave them empty on a miss, error or null field;
  - never substitute another year or the monthly norm.
- **A year-aware quarter hydrograph fetch in `src/db.py`.** Select the snapshot by row date; do not use
  the import-time column renaming.
- **A distinct quarterly last-year attribute and tag wiring.**
  - Files: `src/site.py`, `src/bulletins.py` (`:1339` quarterly tag only).
  - The monthly `forecast_prevyear_q` and its tag stay byte-identical.
- **If D2 shows an issue date:** a new site attribute and template tag. Deployed templates live in the data
  repos, so a template change is an ops step.

**Verification that proves the property** (not just site attributes):
- Render a two-reservoir bulletin and assert both sections' `Q_LAST_YEAR`, norm and volume cells together.
- Hydrate the **same site object** successfully, then with a missing row: the quarter fields must be empty
  afterwards.
- Reopen an old bulletin after a newer quarterly issue: the old quarter is kept.
- Quarter norm 50, monthly norm 80, forecast 60 → `PERC_NORM` 120.0.
