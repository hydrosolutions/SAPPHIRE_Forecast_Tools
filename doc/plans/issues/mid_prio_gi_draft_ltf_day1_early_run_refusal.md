# LTF-015: Refuse an early long-term run that falls in a different calendar month than its scheduled issue date

**Status**: Draft (2026-09-26, rev 3). Fix contract decided by the owner on 2026-09-26 (overview decision H /
D7: refuse, no relabel). Ready to implement after LTF-014 P1.
**Module**: `apps/long_term_forecasting`
**Priority**: Medium. Live today for the tjhm month modes, independent of LTF-014; it reaches the tjhm
quarter once LTF-014 P0 adds the Jan 1 and Oct 1 issues. It affects **direct/manual runs** made 1–5 days
before the 1st (a direct `run_forecast.py`, or a manually triggered pipeline, which the scheduler admits
within ±10 days):
- cron runs on the issue day, so it is not affected;
- guarded recovery is not affected either, because `lt_recovery` rejects future and non-exact dates
  (`lt_recovery.py:295, 338`).

**Labels**: `long-term`, `bug`, `scheduling`
**Found**: 2026-09-25, read-only simulation while mapping LTF-014; value impact confirmed in the 2026-09-26
review round
**Depends on**: LTF-014 P1 (both edit `apps/long_term_forecasting/tests/test_lt_utils.py`; P1 adds a
`check_valid_forecast_issue_date` test block there)
**Related**: LTF-014, LTF-007

## Problem

- `lt_utils.check_valid_forecast_issue_date` (`lt_utils.py:177-228`) accepts a run up to 5 days early
  and keeps **today** as the issue date (`:202-209` window; only a late run is snapped back, `:212-217`).
- `run_forecast.py` passes that date to `predict_operational` (`:341, 365`), and everything downstream
  derives the target month from it.
- With the ±5-day window, an early run can only land in the previous calendar month when the issue day
  is 1–5. Deployed: every tjhm mode has issue day 1; kghm uses days 10 and 25 and is unaffected.
- **tjhm month modes** (`month_1..3`, issue day 1, leads 0–2, no `forecast_months` restriction, calendar
  adjustment on by default): the post-processing target month is issue month + lead
  (`post_process_lt_forecast.py:754`, `:451`, `:469`), and the ratio adjustment uses that month's
  climatology (`:745-786`). A run on 03-29 for the 1 April issue is therefore labelled **and scaled** to
  March (lead 0), April (lead 1) and May (lead 2) instead of April, May and June. The **values** are
  corrupted, not only the labels.
- **tjhm quarter** (issue day 1, lead 0, `calendar_month_adjustment` False) after LTF-014: a run on
  2026-12-29 is labelled from issue month 12 by `adjust_forecast_dates_dynamic`
  (`post_process_lt_forecast.py:132-201`) → `valid_from 2026-12-01`, `valid_to 2027-02-28`. That is not a
  calendar quarter (PP-064 excludes it) and not the Q1 intended.
- tjhm `seasonal_april` (issue day 1, fixed target Apr–Sep) keeps a correct label but issues from March
  data; the rule below refuses it too.

## Fix (owner decision H)

In `check_valid_forecast_issue_date`, after the existing ±5-day window check and before the late snap:
if the run is early (`day_offset < 0`) and `today` is in a different calendar month (year, month) than
`scheduled_issue_date`, log one clear line and return **None**.

- The line names the model, the run date and the scheduled issue date, says the run is refused because it
  falls in a different calendar month, and tells the operator to run on the scheduled date. Tests match on
  the phrase "different calendar month".
- Returning None reuses the existing path: the caller logs the skip and counts it as a failure
  (`run_forecast.py:345-347`); the model shows as FAILED in the run summary (`:527-529`) and dependent
  models are skipped (`:496-501`). The process exit status is not changed by this issue.
- No relabelling, no forward snap, no new parameters. Same-month early runs (e.g. kghm 20th–24th, or
  day-10 modes on the 5th–9th) keep today's date and the existing warning, exactly as now.
- The rule is general, not keyed on issue day 1; the window makes it inert for issue days above 5.

**Files (only these may be modified)**:
- `apps/long_term_forecasting/lt_utils.py` (`check_valid_forecast_issue_date` body only)
- `apps/long_term_forecasting/tests/test_lt_utils.py`
- `apps/long_term_forecasting/readme.md` (one added bullet under "When Forecasts Run", `:165-180`, for the
  new refusal)

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Do not edit
existing tests; use station code `19999` if a code is needed.

## Tests

Add to `TestCheckValidForecastIssueDate` (`tests/test_lt_utils.py:81`, reuse `_make_mock_config`).
**Freeze both clocks** in every new test: patch `lt_utils.get_today` and `pd.Timestamp.now`
(`patch.object(pd.Timestamp, "now", return_value=...)`), because `lt_utils.py:179-186` asserts
`today <= now` against the real clock.

1. tjhm-quarter-like (day 1, `[1,4,7,10]`): 2026-12-29 → None, log contains "different calendar month".
2. tjhm-month_1-like (day 1, all months): 2026-03-29 → None, same log.
3. kghm-quarter-like (day 25, `[3,6,9,12]`): 2026-12-22 → 2026-12-22 (accepted with today's date, unchanged).
4. Boundaries, day 1, all months:
   - 2026-12-27 (5 days early, previous month) → None with the new line;
   - 2026-12-26 (6 days early) → None via the existing "not scheduled" line, not the new one;
   - 2027-01-06 (5 days late) → 2027-01-01 (snap unchanged); 2027-01-07 (6 days late) → None.
5. Boundaries, day 10, all months: 2024-03-05 → 2024-03-05 with the existing "before the scheduled issue
   date" warning; 2024-03-04 → None.
6. Unchanged without edits: the day-10 early-run contract (`tests/test_lt_utils.py:128`) and the recovery
   exact-date and no-future tests in `tests/test_lt_recovery.py`.

## Acceptance

- The new tests pass; no existing test is edited.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` gives zero failures and zero
  unexpected skips.
- `git diff --stat` shows only the three files above.
