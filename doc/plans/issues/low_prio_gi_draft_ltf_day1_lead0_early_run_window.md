# LTF-015: An early manual run of an issue-day-1, lead-0 mode gets the previous month's window label

**Status**: Draft (2026-09-25, rev 2). **Deferred**; the fix contract needs an owner decision.
**Module**: `apps/long_term_forecasting`
**Priority**: Low. Affects **direct/manual operational runs** made 1–5 days before the 1st:
- cron runs on day 1, so it is not affected;
- guarded recovery is not affected either, because `lt_recovery` rejects future and non-exact dates
  (`lt_recovery.py:295, 338`).

It matters for tjhm quarter once LTF-014 lands, because Q1 is issued on Jan 1.
**Labels**: `long-term`, `bug`, `scheduling`
**Found**: 2026-09-25, read-only simulation while mapping LTF-014
**Related**: LTF-014, LTF-007

## Problem

- `lt_utils.check_valid_forecast_issue_date` (`lt_utils.py:175-231`) accepts a run up to 5 days early and
  keeps **today** as the issue date. Only a late run is snapped back.
- `run_forecast.py` passes that date to `predict_operational` (`:341, 365`), which needs a feature row
  dated exactly then (`lt_forecasting` `LINEAR_REGRESSION.py:468`). Observations are loaded up to the
  actual run date (`data_interface.py:399`).
- For `operational_issue_day = 1`, `operational_month_lead_time = 0` (tjhm quarter and tjhm `month_1`), a run
  on 2026-12-29 is labelled from issue month 12 by `adjust_forecast_dates_dynamic`
  (`post_process_lt_forecast.py:132-199`). The result is `valid_from 2026-12-01`, `valid_to 2027-02-28`:
  - it starts before the issue date;
  - it is not a calendar quarter (PP-064 excludes it);
  - it is not the Q1 intended.
- The model's actual target on that run is about today+1 … +90 days (≈ 30 Dec – 29 Mar), i.e. roughly Q1.
  **Only the label is wrong.**

## Fix options (owner decision; overview D7)

- **(A) Relabel from the scheduled issue date (recommended).**
  - Keep the issue date = the run date, so features and the `today <= now` invariant are unchanged.
  - Compute `valid_from`/`valid_to` from the **scheduled** issue date that the early run belongs to.
  - Only the window label changes.
- **(B) Refuse early runs** that would cross into the previous month for issue-day-1 modes.

Snapping the issue date **forward** is not an option: no feature row exists for a future date, and it
breaks `lt_utils.py:183-185`.

## Constraints and acceptance (after the decision)

**Files**:
- `apps/long_term_forecasting/post_process_lt_forecast.py` (A) or `lt_utils.py` (B)
- the caller in `run_forecast.py`, only to pass the scheduled date
- `tests/test_lt_utils.py`, `tests/test_post_process_lt_forecast.py`

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."*

**Tests**:
- **Freeze both clocks** (`get_today` and the real `now` used at `lt_utils.py:179`).
- **(A)** Issue day 1, lead 0, run 2026-12-29 → stored `date` 2026-12-29, window 2027-01-01..2027-03-31.
- **(B)** The same run is refused with a clear log line.
- **Unchanged:**
  - day-25 modes, where an early run on the 20th–24th keeps today's date and the same window;
  - the day-10 early-run contract (`tests/test_lt_utils.py:128`);
  - the ±5/±6-day boundaries and the warning line;
  - recovery's exact-date and no-future guards.

`cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` gives zero failures and zero
unexpected skips.
