# LTF-015: Refuse an early long-term run that falls in a different calendar month than its scheduled issue date

**Status**: Draft (2026-09-26, rev 6 after the fourth review round). Fix contract decided by the owner on
2026-09-26 (overview decision H / D7: refuse, no relabel; round-2 decision 6: warn on same-month early
runs). Ready to implement; LTF-014 P1 is deferred with P0, so there is no hard dependency on it.
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
**Depends on**: nothing hard. LTF-014 P1 is deferred with P0 (owner, 2026-09-26); only shared-file
sequencing remains:
- **LTF-014 P1:** both edit `apps/long_term_forecasting/tests/test_lt_utils.py` (P1 adds a
  `check_valid_forecast_issue_date` test block there). LTF-015 runs before or after P1; whichever lands
  second rebases.
- **DOC-009 P2b:** both edit `apps/long_term_forecasting/readme.md`. LTF-015 edits `:165-180`; DOC-009 P2b
  row 11 (after LTF-014 P0, deferred) edits `:41`, `:89-103`, `:193`, `:206-207`. The ranges are
  disjoint, but row 11's rewrites shift line numbers: whichever lands second rebases and re-locates the
  section by its heading "When Forecasts Run".
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

## Fix (owner decision H and round-2 decision 6)

In `check_valid_forecast_issue_date`, after the existing ±5-day window check and before the late snap:
if the run is early (`day_offset < 0`) and `today` is in a different calendar month (year, month) than
`scheduled_issue_date`, log one clear line and return **None**. If the run is early and in the **same**
calendar month, log the new WARNING below when its scope applies, and continue as today.

- The line names the model, the run date and the scheduled issue date, says the run is refused because it
  falls in a different calendar month, and tells the operator to run on the scheduled date. Tests match on
  the phrase "different calendar month".
- Returning None reuses the existing path: the caller logs the skip and counts it as a failure
  (`run_forecast.py:345-347`); the model shows as FAILED in the run summary (`:527-529`) and dependent
  models are skipped (`:496-501`).
- **The refusal is visible only in the log.** This issue does not change the exit status or the schedule:
  - without `--recover` the process exits 0 (`run_forecast.py:618-625`: `run_forecast()` returns, no
    `sys.exit`);
  - the schedule query still reports the mode as active (`lt_schedule_query.py:100-127`: issue-day
    distance ≤ 10 and a model scheduled within 10 days), so the Luigi `LTScheduleQuery`
    (`apps/pipeline/pipeline_docker.py:2110`) and the `run_locally.sh` gate (`query_lt_schedule`,
    `apps/run_locally.sh:336-381`) start the mode as usual.

  The operator finds the refusal in the new log line and the FAILED summary, nowhere else.
- No relabelling, no forward snap, no new parameters. Same-month early runs (e.g. kghm 20th–24th, or
  day-10 modes on the 5th–9th) keep today's date and the existing warning (`lt_utils.py:219-227`).
- **Same-month early runs of quarter-feeding modes get one new WARNING line** (round-2 decision 6),
  logged in addition to the existing warning. It says that rows dated before the scheduled issue date
  produce **no quarterly product** downstream, because PP-065 derives quarters only from monthly rows
  dated on the configured issue day. Tests match on the phrase "no quarterly product". The run itself is
  still accepted.
  - **Scope: only modes that feed quarter derivation, at a quarter issue date.** Check, in this order:
    1. `forecast_configs.forecast_mode` (set by `load_forecast_config`, `config_forecast.py:62`) is
       `month_1`, `month_2` or `month_3`;
    2. with N the mode number and ℓ = `forecast_configs.get_operational_month_lead_time()`
       (`config_forecast.py:230-231`), the month (scheduled issue month + ℓ − (N − 1)), wrapped to 1–12, is
       a calendar-quarter start month (1, 4, 7 or 10). That month is month_1's target, i.e. the first
       month of the derived quarter.
  - This relies on month_1–month_3 having consecutive leads that start at the quarter lead, and on the
    same issue day as the quarter mode. Both hold in the Dropbox master mode configs (read
    2026-09-26): kghm month_1–3 leads 1–3 at day 25, quarter lead 1 at day 25; tjhm month_1–3 leads 0–2
    at day 1, quarter lead 0 at day 1.
  - It never fires for `month_0`, `quarter` or the season modes, nor for month_1–3 issues whose derived
    first month is not a quarter start (e.g. kghm Oct 25). In practice only kghm hits it: a tjhm (day 1)
    early run always lands in the previous month and is refused.
- The rule is general, not keyed on issue day 1; the window makes it inert for issue days above 5.

**Files (only these may be modified)**:
- `apps/long_term_forecasting/lt_utils.py` (`check_valid_forecast_issue_date` body only)
- `apps/long_term_forecasting/tests/test_lt_utils.py`
- `apps/long_term_forecasting/readme.md`, section "When Forecasts Run" (`:165-180`) only:
  - one added bullet for the new refusal;
  - a correction of `:178`. Its claim that a run ">5 days off … refuses to run and raises an error" is
    wrong: the check returns None (`lt_utils.py:202-209`), the model is FAILED in the summary, and the
    process exits 0.

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Do not edit
existing tests; use station code `19999` if a code is needed.

## Tests

Add to `TestCheckValidForecastIssueDate` (`tests/test_lt_utils.py:81`, reuse `_make_mock_config`).
**Freeze both clocks** in every new test: patch `lt_utils.get_today` and `pd.Timestamp.now`
(`patch.object(pd.Timestamp, "now", return_value=...)`), because `lt_utils.py:179-186` asserts
`today <= now` against the real clock. Tests that assert on log text call
`caplog.set_level(logging.INFO, logger="long_term_forecasting")` (the module logger,
`__init__.py:17`). The existing "not scheduled" line is INFO (`lt_utils.py:203-208`), and the root logger
may be capped above INFO.

Each new test sets `forecast_mode` and `get_operational_month_lead_time.return_value` on the mock that
`_make_mock_config` returns, without editing the helper. Existing tests leave `forecast_mode` a
`MagicMock`, which is not one of `month_1`–`month_3`, so they log no new line.

1. tjhm-quarter-like (`quarter`, lead 0, day 1, `[1,4,7,10]`): 2026-12-29 → None, log contains
   "different calendar month".
2. tjhm-month_1-like (`month_1`, lead 0, day 1, all months): 2026-03-29 → None, same log.
3. kghm quarter-feeding modes (day 25, all months), 2026-12-22 → 2026-12-22 (accepted with today's date),
   the existing "before the scheduled issue date" warning, and a WARNING containing "no quarterly
   product": `month_1` lead 1 and `month_3` lead 3 (parametrised).
4. Scope negatives, each accepted with the existing warning and **no** "no quarterly product" line:
   - `month_1` lead 1, day 25, 2026-10-22 (Oct 25 issue → November, not a quarter start);
   - `quarter` lead 1, day 25, `[3,6,9,12]`, 2026-12-22;
   - a season mode (mock `seasonal_march`, lead 1, day 25, `[12]`), 2026-12-22;
   - `month_0`: see test 6.
5. Boundaries, `month_1` lead 0, day 1, all months:
   - 2026-12-27 (5 days early, previous month) → None with the new line, and no "no quarterly product"
     line;
   - 2026-12-26 (6 days early) → None via the existing "not scheduled" line, not the new one;
   - 2027-01-06 (5 days late) → 2027-01-01 (snap unchanged); 2027-01-07 (6 days late) → None.
6. Boundaries, `month_0` lead 0, day 10, all months: 2024-03-05 → 2024-03-05 with the existing "before
   the scheduled issue date" warning and no "no quarterly product" line; 2024-03-04 → None.
7. Unchanged without edits: the day-10 early-run contract (`tests/test_lt_utils.py:128`) and the recovery
   exact-date and no-future tests in `tests/test_lt_recovery.py`.

## Acceptance

- The new tests pass; no existing test is edited.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` gives zero failures and zero
  unexpected skips.
- `git diff --stat` shows only the three files above.


> **Quarter mode (2026-09-26).** An early kghm quarter-mode run on the 20th–24th also yields no quarter product under PP-065's native-row rule. It is already covered by the **existing** early-run warning (`lt_utils.py:219-223`). So the new line stays scoped to `month_1`–`month_3`, as above, and the quarter-mode exclusion test stays.
