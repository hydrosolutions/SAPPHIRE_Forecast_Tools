# PP-066: `select_operational_issuances` robustness: mixed-tz dates and unclamped issue day

**Status**: Draft (2026-09-26)
**Module**: `apps/postprocessing_forecasts`
**Priority**: Low — both problems pre-existing on trunk; both **latent** today (Problem 1: API rows
carry date-only strings; Problem 2: configured issue days are 25 (kghm) and 1 (tjhm), neither near a
month-length boundary).
**Related**: PP-064 (`high_prio_gi_draft_pp_quarter_calendar_window_validation.md`) fixed the mixed-tz
failure mode at its own two call sites via `local_calendar_date`
(`apps/postprocessing_forecasts/src/aggregation.py`), and added the CLAMPED issue-day rule to its own
Contract and to FD-029's card, but explicitly left `select_operational_issuances` unmodified (Contract:
"It only has to receive calendar-only rows."). PP-065 P1b's own native-row helper applies the clamp on
its own, separate comparison — it does not touch this selector either. This issue is the follow-up that
owns both gaps in the one function PP-064 and PP-065 deliberately did not touch.

## Problem 1: mixed tz-aware/naive `date` column crashes

`select_operational_issuances` (`apps/postprocessing_forecasts/src/data_reader.py:225-398`, def at
`:225`) parses its issue-date column with a bare `pd.to_datetime(candidates[date_col])`
(`:335`) — no `format="mixed"`, no tz handling. A batch mixing tz-aware and tz-naive `date` strings
(e.g. `"2024-12-25"` next to `"2025-03-25T00:00:00+06:00"`) raises `ValueError` there, aborting the
whole call. Both quarterly readers call this function under `SAPPHIRE_SKILL_LEAD_AWARE=true`:
`read_quarterly_forecasts`'s flag-ON direct branch and `read_latest_quarterly_forecasts`'s flag-ON
direct branch (both in the same file). Every other caller that passes an **unparsed** `date` column
straight through is exposed to the same crash — confirmed for the monthly caller: `_normalize_monthly_forecasts`
(`:1479-1505`) parses `valid_from` but never touches `date`, so it too reaches
`select_operational_issuances` raw. The **seasonal** caller is different, not exposed to this crash: its
`date` column is already coerced (lossily, not raised on) upstream before it ever reaches this function
— see "Out of scope, deferred" below, which owns that separate failure mode.

Pre-existing on trunk, not introduced by PP-064. Latent: the postprocessing API returns `date` as a
date-only string today, so no live batch mixes formats yet — same latency class as the mixed-format
crash PP-064 found and fixed at its own two call sites.

## Problem 2: the day match is unclamped, unlike every other native-row check in the codebase

`select_operational_issuances` compares the raw, unclamped `date.day` to the configured `issue_day`
(`data_reader.py:349-353`: `issue_day = candidates[date_col].dt.day`, then `allowed_schedules =
{(s.lead_time, s.issue_day) for s in schedules.values()}`, matched via `(lead, day) in
allowed_schedules`). The producer clamps the issue day to the issue month's own length before ever
issuing a forecast (`apps/long_term_forecasting/lt_utils.py:170-172 nearest_scheduled_issue_date`:
`min(issue_day, calendar.monthrange(year, month)[1])`), and both PP-064's Contract (native quarter row
rule) and FD-029's card (`is_native` predicate) apply that same clamp when deciding whether a row is
native. This selector is the one place in the codebase that still compares unclamped — a genuinely
native row issued on a clamped day (e.g. `operational_issue_day = 31`, issued on the 30th of a 30-day
month) would never match here, and would silently fall out of the operational-issuance selection under
flag ON (monthly, seasonal, and quarterly readers alike, since they all share this one function).

**Latent today**: neither deployed org's configured `operational_issue_day` is anywhere near a
month-length boundary (kghm 25, tjhm 1), so no live schedule currently exercises a clamped day. This is
the same latency class as Problem 1, not a live incident.

## Evidence (Problem 1)

- Trunk `28ee535d`: `apps/postprocessing_forecasts/src/data_reader.py:335` —
  `candidates[date_col] = pd.to_datetime(candidates[date_col])`.
- Reproduced (pandas 2.3.3, the locked version): `pd.to_datetime(pd.Series(["2024-12-25",
  "2025-03-25T00:00:00+06:00"]))` raises `ValueError: unconverted data remains when parsing with
  format "%Y-%m-%d": "T00:00:00+06:00", at position 1.`
- Same line and function on the PP-064 branch (`fix_pp_quarter_calendar_window`, HEAD `ac2a5a51`,
  worktree `sapphire-pp064a`): unchanged, per PP-064's own Contract not to modify this function.
- `format="mixed"` alone does not fix it either — reproduced: it silences the `ValueError` but the
  resulting object-dtype Series then raises `AttributeError` on `.dt.year`/`.dt.day` (the same
  failure mode PP-064 hit at its own two call sites) **and separately** raises `TypeError: can't
  compare offset-naive and offset-aware datetimes` from `sort_values(by=date_col)` (`:390`) — the
  tie-break this function relies on (see Proposed fix).

## Proposed fix (Problem 1)

Once PP-064 Chunk A has merged, its `local_calendar_date` helper
(`apps/postprocessing_forecasts/src/aggregation.py:100-199`, added by that branch — not in
`data_reader.py`) is safe to reuse for the day/month/year arithmetic here (`derived_lead` at
`:346-348`, `issue_day` at `:349`) — those only ever read `.dt.year`/`.dt.month`/`.dt.day`, which are
unaffected by dropping time-of-day. `local_calendar_date` is a per-value `pd.Timestamp` parse
(tz-aware → local wall-clock date; out-of-range — before 1677-09-22, or not representable at ns
resolution — → `NaT`; never raises), vectorised for a `datetime64` column and de-duplicated only for
exact-`str` values elsewhere; both `date_col` and `valid_from_col` here are typically object/string
columns, so the per-value or string-dedup path applies, not the vectorised one.

**Do not simply reassign `candidates[date_col]` to the truncated value and sort by it, as PP-064's
own two call sites do.** This function's docstring and code make `date_col`'s relative ordering a
load-bearing part of the contract, not an implementation detail: "a duplicate same-day reissue
resolves deterministically: latest `date` wins" (`:272-274`), implemented as
`candidates.sort_values(by=date_col, kind="stable")` then `drop_duplicates(..., keep="last")`
(`:390-391`). Two same-target reissues on the same calendar day but different times (e.g. 18:00 and
06:00) are today ordered correctly by the full timestamp; truncating `date_col` to a bare calendar
date before that sort would make them compare equal and fall back to input order — the wrong row
could then win depending on API response order, a **silent, untested behaviour change**, not a
fix.

**Required:** keep a separate key for the sort/tie-break — the full parsed timestamp where
parseable — distinct from whatever `date_col` value is used for the day/month/year arithmetic;
do not collapse same-day-different-time rows onto one sort value. If preserving exact
sub-day ordering across mixed tz-aware/naive rows turns out to be impractical (e.g. no principled
way to compare an offset-aware and an offset-naive instant), that must be an **explicit, documented
contract change** with its own test proving the chosen tie-break — not an unstated side effect of
reusing `local_calendar_date`. Recommendation: keep the existing latest-date-wins ordering.

Leave the `valid_from_col` parse (`:336`) untouched unless a similar mixed-format report surfaces
for it; every existing write path already writes `valid_from` as a bare date. **For this Problem-1
fix specifically**, do not otherwise change `select_operational_issuances`'s selection logic,
signature, the derived-lead computation, or the issue-day match itself — only the date-parsing that
feeds them. (Problem 2, below, is the one deliberate exception: it changes the issue-day match, and
only that — see its own scope note.)

**Out of scope, deferred (do not fold in here):** the season branch of `_normalize_combined_forecasts`
(`:3902-3903`, `df["date"] = pd.to_datetime(df["date"], errors="coerce")`, no `format="mixed"`) and
`read_seasonal_forecasts` (calls `_normalize_combined_forecasts` at `:3331` before reaching
`select_operational_issuances`) have a **different** failure mode on the same kind of mixed batch:
reproduced — plain `pd.to_datetime(..., errors="coerce")` on a mixed tz-aware/naive batch does not
raise at all; it silently coerces the tz-aware row to `NaT` (data loss, not a crash), before
`select_operational_issuances` ever sees it. That is a separate defect (silent loss vs. this
issue's crash) on a separate code path; explicitly deferred, not covered by this issue's fix or
tests.

## Proposed fix (Problem 2)

**This is the one change in scope that touches the issue-day match itself** — Problem 1's fix above
leaves that match alone; this is the deliberate exception, and only the match, not anything else
Problem 1's "do not change" list covers.

Clamp the issue day to the issue month's own length before matching, mirroring the producer
(`lt_utils.py:170-172`) and PP-064/FD-029's own native-row predicates: for each row, compute
`clamped_issue_day = min(s.issue_day, days_in_month(derived issue year, derived issue month))` per
schedule `s`, and match against `(lead, day) in {(s.lead_time, min(s.issue_day, days_in_that_month))
for s in schedules.values()}` — the clamp target depends on the *row's own* derived issue year/month
(from `derived_lead`), not a fixed month, since the same configured `issue_day` clamps differently in a
28-day February versus a 30-day June. Do not change the function's signature, its selection grain, the
derived-lead computation, or the tie-break `select_operational_issuances` uses elsewhere (Problem 1's
fix) — clamp only the day value the match compares against. Keep the exact-match semantics for every
issue day that never needs clamping (the overwhelming majority — any day ≤ 28).

## Tests

- Direct rows for Q1 2025 issued `"2024-12-25"` and Q2 2025 issued
  `"2025-03-25T00:00:00+06:00"`, lead 1, issue day 25, `SAPPHIRE_SKILL_LEAD_AWARE=true` → no
  exception from `read_quarterly_forecasts(codes, 2025, 2025)`, and both quarters are returned with
  their operational issuance selected.
- Same rows, no exception from `read_latest_quarterly_forecasts(codes, forecast_date=date(2025, 6,
  1))`, flag ON.
- A direct unit test on `select_operational_issuances` with the same mixed-format `date` column,
  asserting the selected row's derived lead and issue day are unaffected by the tz offset.
- **Same-target, same-day reissue, both input orders.** Two rows for the same `(code, model,
  target_year[, target_period], lead)` unit, issued on the same calendar day at different times
  (e.g. `"2025-03-25T18:00:00"` value A and `"2025-03-25T06:00:00"` value B), run once with A first
  in the input and once with B first → both orderings select the same (genuinely later-issued)
  value, matching today's (pre-crash) tie-break contract. This is the regression test for the
  ordering risk above; if the fix explicitly changes the contract instead, this test documents and
  asserts the new, chosen behaviour rather than being silently invalidated.
- **Clamped issue day, flag ON (Problem 2).** Schedule configured with `operational_issue_day = 31`,
  lead 1, target month July (a target whose issue month — June, one lead-month back — has 30 days): a
  row dated `date = <year>-06-30` (the producer's own clamp for a 31-configured day in a 30-day June),
  `valid_from` = July 1 → selected as the operational issuance through `select_operational_issuances`,
  both directly and through both quarterly readers under flag ON. Fails before this fix (the row's raw
  `date.day` = 30 never equals the configured `issue_day` = 31, so `is_candidate` is False and the row
  is dropped as a "no operational candidate" unit).

## Acceptance

- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` passes, zero unexpected skips.
- No existing test edited; the new tests fail on trunk (and on the PP-064 branch, pre-fix) — Problem 1's
  with `ValueError`, Problem 2's with the row silently dropped (no exception, just missing output).
- `git diff --stat` limited to `apps/postprocessing_forecasts/src/data_reader.py` and its tests.
- Station code `19999` in any fixture; no real station codes.
