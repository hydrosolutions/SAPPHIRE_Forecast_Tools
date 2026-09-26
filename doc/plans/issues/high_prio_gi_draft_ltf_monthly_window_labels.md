# LTF-016: Monthly forecast windows are stored offset or in the wrong year

**Status**: Draft (2026-09-26). This is the follow-up to decision A of the calendar-quarter plan set.
**Module**: `apps/long_term_forecasting`. There are also data fixes in `long_forecasts`; those need the
postprocessing service owner.
**Priority**: High. Defect 2 is a live **monthly** skill bug for kghm, independent of the quarter work.
**Labels**: `long-term`, `bug`, `data-quality`, `monthly`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md)
**Found**: 2026-09-26 by the out-of-loop review of PP-065 (two reviewers independently). Local dev DB
only; aggregate counts only.
**Related**:
- PP-065 matches monthly rows by (issue date, `horizon_value`) precisely so that it tolerates both defects.
  Fixing them does not change PP-065's output.
- Memory/prior finding "taj raw valid_from" ("Finding B"): the prod taj MONTH rows were 100 % snapped on
  2026-07-10.

## Defect 1: tjhm hindcast MONTH rows carry raw offset windows

tjhm MONTH rows with flag 1 (hindcast) have `valid_from`/`valid_to` equal to the model's raw 30-day period.
For example, the 2015-01-01 issue has windows 01-02..02-01, 02-01..03-03 and 03-03..04-02. The windows are
not snapped to calendar months. Only the 2026 operational rows (flag 0/2) are snapped.

- **Suspected source:** the from-file importers copy `valid_from`/`valid_to` verbatim from
  `{model}_hindcast.csv` (`bin/utils/migration_py/long_forecast.py`, `_build_record`), and those CSVs were
  produced without `post_process_lt_forecast`.
- **Impact:**
  - consumers that key on the `valid_from` month (monthly skill pairing via `data_reader`, and
    `forecast_skill_eval`'s alignment check) can mis-attribute a month;
  - PP-065 is unaffected.
- **Scope check first:** count the unsnapped MONTH rows per org, flag and model **on each server**. The
  earlier prod check (2026-07-10) found taj prod 100 % snapped, so this may be local-only.

## Defect 2: kghm GBT, SM_GBT and SM_GBT_NORM label January targets with the issue year

For issues on Oct 25 (hv 3), Nov 25 (hv 2) and Dec 25 (hv 1), the January target row has `valid_from` in the
**issue** year: about 940 flag-1 rows per model per issue month. The same holds for flag-0 rows issued on
2025-12-25, so this is **operational, not only historical**. LR_Base/LR_SM and the other models are correct.

- **Impact:**
  - monthly skill for January scores these models against the wrong year's observations;
  - Naive Mean inherits the error (~232 rows per issue month);
  - the monthly EM and Skilled Mean inherit it where these models qualify;
  - the dashboard may show a January forecast under the wrong year.
- **Suspected source (to confirm):** the GBT-family output path sets the target year differently from the
  LR path. `post_process_lt_forecast.py:446-470` and `:748-752` compute `target_year` correctly for the
  calendar-month mapping. So check where the GBT family's `valid_from` is produced (the `lt_forecasting`
  SciRegressor output and its date handling), and whether it bypasses `post_process_lt_forecast`.

## Plan

**P0 — measure, read-only, per server.**
- Defect 1: counts by org × flag × model of MONTH rows whose `valid_from` is not on day 1.
- Defect 2: counts by org × model × issue month of MONTH rows where the target month from issue month + hv
  is January but `valid_from`'s year equals the issue year.
- Report aggregates only.

**P1 — root cause and producer fix.** One code agent.
- **Files:** to be determined in P0. They are expected in `apps/long_term_forecasting/` (the post-processing
  of GBT-family outputs), plus tests in `apps/long_term_forecasting/tests/`.
- **Agent instruction:** *"Do NOT change any existing function signatures, data flow logic, or control
  flow. Your changes must be purely additive or modify only the specific behavior described."*
- **Tests** (station `19999`):
  - A GBT-family monthly forecast issued 2026-12-25 at lead 1 → `valid_from` 2027-01-01.
  - Issued 2026-10-25 at lead 3 → 2027-01-01.
  - The LR path is unchanged.
- **Acceptance:** `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` passes with
  zero failures and zero unexpected skips.

**P2 — data fix.** Owner and service owner; a one-way step, so back up first.
- Re-label the affected rows, or re-generate them:
  - Defect 2: set `valid_from`/`valid_to` to the correct year. This changes the natural key, so check for
    collisions first.
  - Defect 1: snap the windows, or re-import hindcasts that went through post-processing.
- Then run the **monthly** skill recalc, and the quarterly one if PP-065 is live.

## Out of scope

- PP-065's derivation, which is already tolerant of both defects.
- Snapping quarter windows (PP-064).
