# LTF-017: GBT-family climatological Q25/Q75 take σ from the raw window's month, not the target month

**Status**: Draft (2026-09-26, rev 4 after the second review round). New in this round.
**Module**: `apps/long_term_forecasting`
**Priority**: High. **Live on trunk** for operational GBT-family monthly forecasts whose raw window starts in
the month before the target. It is independent of the quarter chain.
**Labels**: `long-term`, `bug`, `uncertainty`, `monthly`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md)
**Found**: 2026-09-26 in the second review round of the quarter plans. The per-mode impact below was
re-derived from the pinned `lt_forecasting` and the Dropbox mode configs.
**Related**:
- LTF-005 (`review_gi_draft_lt_gbt_quantile_bounds.md`): introduced these bounds in commit `6dbf2d0f`
  (2026-04-14).
- LTF-016: stale stored labels. That producer is already fixed; this one is not.
- PP-065: derived quarter rows null their quantiles regardless, so the quarter product does not use these
  bounds.

## Problem

- **Where the month comes from.** `run_forecast._add_climatological_quantile_bounds`
  (`apps/long_term_forecasting/run_forecast.py:86-208`):
  - takes the target month from the **raw** `valid_from` (`:124`);
  - merges the per-(code, calendar month) σ on that month (`:179-187`);
  - sets Q25/Q75 = forecast ∓ 0.674 σ (`:190-191`).
- **When it runs.** The call comes **before** post-processing (`:381-389` vs `:399-403`). It runs only for
  `sciregressor` models when `calendar_month_adjustment` is on (`:382-383`). That means GBT, SM_GBT,
  SM_GBT_Norm and SM_GBT_LR in every month mode of both orgs. Quarter and seasonal configs set the flag to
  False (checked 2026-09-26), so those modes are not affected.
- **The raw window.**
  - The pinned `lt_forecasting` (`pyproject.toml:68`, rev `d717d247`) sets
    `valid_from = today + 1 + (offset − prediction_horizon)` days (`SciRegressor.py:746-754`).
  - The row's `date` is `today` (`:772`).
  - With the deployed mode configs (`prediction_horizon` 30), the raw start falls in the **month before the
    target** for these issues (derived for 2026–2028):

| Org / mode | Issue day / lead / offset | Raw `valid_from` | Issue months that use the wrong month's σ |
|---|---|---|---|
| kghm month_0 | 10 / 0 / 20 | 1st of the issue month | none |
| kghm month_1 | 25 / 1 / 35 | issue + 6 days | Jan, Mar, May, Jul, Aug, Oct, Dec (31-day issue months) |
| kghm month_2 | 25 / 2 / 65 | issue + 36 days | every month except Jan and Feb |
| kghm month_3 | 25 / 3 / 95 | issue + 66 days | Mar–Nov; also Jan in a leap year, and Dec before a leap February |
| tjhm month_1 | 1 / 0 / 30 | 2nd of the issue month | none |
| tjhm month_2 | 1 / 1 / 60 | issue + 31 days | none |
| tjhm month_3 | 1 / 2 / 90 | issue + 61 days | Jul and Dec (Jul+Aug and Dec+Jan have 62 days) |

- **Leave-one-out uses the wrong year.** It excludes `today.year` (`run_forecast.py:153-155`), not the
  target year that post-processing uses (`post_process_lt_forecast.py:752-757`).
  - For a target in the next year (kghm Oct–Dec issues → Jan–Mar; tjhm month_3 Dec → Feb), the issue-year
    observations of the target month are dropped although they are available.
  - Operationally the target year has no observations yet, so there is no leakage. σ is merely computed
    from one year fewer.
- **Consequence.**
  - For the listed issues, the GBT-family Q25/Q75 carry the previous calendar month's σ.
  - Post-processing then ratio-scales Q25/Q75 as quantile columns (`post_process_lt_forecast.py:583-592`,
    `:651-681`). There is no Q50, so delta is 0 (`:635-648`).
  - The displayed monthly range width is therefore wrong. The central forecast is unaffected.
- **Scope.**
  - Operational and recovery runs are affected: both go through `run_single_model`.
  - The hindcast path adds no bounds (`calibrate_and_hindcast.py`).
  - GBT-family MONTH rows written since 2026-04-14 already carry these bounds; see P2.

## Plan

### P1 — derive the target month and year from the issue date and lead (one code agent)

**Fix.** In `_add_climatological_quantile_bounds`, compute the target the way `post_process_lt_forecast`
does, with issue = `today`:
- `target_month = (today.month + lead − 1) % 12 + 1`;
- `target_year = today.year + (target_month < today.month)`.

Merge σ on `target_month`, and exclude `target_year` (not `today.year`) in the leave-one-out.

**The one permitted signature change** is an additive, keyword-only parameter with a
behaviour-preserving default: `operational_month_lead_time: int | None = None`.
- With `None`, the function behaves exactly as today (raw `valid_from` month, `today.year`), so the
  existing tests and any other caller are unchanged.
- The call site at `run_forecast.py:384-389` passes `forecast_configs.get_operational_month_lead_time()`
  (`config_forecast.py:230-231`). Change nothing else in `run_single_model`.

**Rejected here:** moving the call after `post_process_lt_forecast`.
- It changes control flow.
- The bounds would no longer be ratio-scaled by post-processing, which changes the width of **every** row,
  not only the mis-assigned ones.
- That is a design change for the owner, not part of this fix.

**Files (only these may be modified)**:
- `apps/long_term_forecasting/run_forecast.py`: the `_add_climatological_quantile_bounds` signature
  (the one keyword above) and body, plus the call at `:384-389`.
- `apps/long_term_forecasting/tests/test_quantile_bounds.py`: new tests only.

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* The keyword
parameter above is the specific behaviour described. Do not edit existing tests.

**Tests** (station `19999`):
- **Fixture.** Daily discharge 2010-01-01 up to the day before `today`. Monthly means differ by month
  **and** by year, so σ differs clearly between adjacent months. Compute the expected σ in the test with
  `ddof=1` over the included years.
- **Tests 1–4 fail on trunk.**
  1. kghm month_1-like: `today` 2026-01-25, lead 1, raw window 2026-01-31..2026-03-02 → σ of **February**.
  2. kghm month_2-like: `today` 2026-03-25, lead 2, raw 2026-04-30..2026-05-30 → σ of **May**.
  3. tjhm month_3-like: `today` 2026-07-01, lead 2, raw 2026-08-31..2026-09-30 → σ of **September**.
  4. Year wrap and leave-one-out: `today` 2026-10-25, lead 3, raw 2026-12-30..2027-01-29 → σ of
     **January**, computed **including** January 2026. Make January 2026 an outlier so that the result
     differs from both trunk behaviours: December's σ, and σ without 2026.
- **Controls** (pass before and after):
  5. tjhm month_1-like: 2026-04-01, lead 0, raw 2026-04-02..2026-05-02 → April.
  6. kghm month_0-like: 2026-03-10, lead 0, raw 2026-03-01..2026-03-31 → March.
- **Test 7, the call site** (fails on trunk). Call `run_single_model` with these patches:
  - `run_forecast.check_valid_forecast_issue_date` → 2026-01-25;
  - `run_forecast.create_model_instance` → a fake whose `predict_operational` returns a raw GBT-shaped
    frame (`Q_GBT`, raw window as in test 1);
  - `run_forecast.post_process_lt_forecast` → identity;
  - `run_forecast.save_forecast` → captures the frame.

  Use a small fake config: model type `sciregressor`, `calendar_month_adjustment` True, lead 1, no model
  or data dependencies, and `all_paths` under `tmp_path`. The captured Q25/Q75 must use February's σ.
- The existing tests in `tests/test_quantile_bounds.py` pass unchanged; they use calendar-aligned windows
  and no lead.

**Acceptance**:
- Tests 1–4 and 7 fail on trunk: run them once before the fix and record the result in the PR. After the
  fix, all new tests pass.
- No existing test is edited.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` gives zero failures and zero
  unexpected skips.
- `git diff --stat` shows only the two files above.

### P2 — stored rows (owner decision, no code)

GBT-family MONTH rows written between 2026-04-14 and the P1 deploy keep their wrong-σ Q25/Q75. `lt_recovery`
cannot rewrite them: it covers only the current or previous month and refuses existing rows.
- **Default:** the fix is forward-only, and past ranges stay as stored.
- The owner may choose otherwise; record the decision in the overview.

## Out of scope

- Moving the call after post-processing, or changing how post-processing scales Q25/Q75 (design; owner).
- The central forecast value (unaffected).
- Stored window labels (LTF-016).
- Quarter uncertainty bounds: FD-029/FD-030 use the δ method (overview decision D).
