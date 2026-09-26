# PP-066: `select_operational_issuances` crashes on a mixed tz-aware/naive `date` column

**Status**: Draft (2026-09-26)
**Module**: `apps/postprocessing_forecasts`
**Priority**: Low — pre-existing on trunk; **latent** today (API rows carry date-only strings).
**Related**: PP-064 (`high_prio_gi_draft_pp_quarter_calendar_window_validation.md`) fixed the same
failure mode at its own two call sites via `_issue_date_local_calendar_date`, but explicitly left
`select_operational_issuances` unmodified (Contract: "It only has to receive calendar-only rows.").
This issue is the follow-up that touches the function PP-064 deliberately did not.

## Problem

`select_operational_issuances` (`apps/postprocessing_forecasts/src/data_reader.py:225-398`, def at
`:225`) parses its issue-date column with a bare `pd.to_datetime(candidates[date_col])`
(`:335`) — no `format="mixed"`, no tz handling. A batch mixing tz-aware and tz-naive `date` strings
(e.g. `"2024-12-25"` next to `"2025-03-25T00:00:00+06:00"`) raises `ValueError` there, aborting the
whole call. Both quarterly readers call this function under `SAPPHIRE_SKILL_LEAD_AWARE=true`:
`read_quarterly_forecasts`'s flag-ON direct branch and `read_latest_quarterly_forecasts`'s flag-ON
direct branch (both in the same file). Every other `select_operational_issuances` caller (monthly,
seasonal) is exposed to the same crash if its `date` column ever mixes formats.

Pre-existing on trunk, not introduced by PP-064. Latent: the postprocessing API returns `date` as a
date-only string today, so no live batch mixes formats yet — same latency class as the mixed-format
crash PP-064 found and fixed at its own two call sites.

## Evidence

- Trunk `28ee535d`: `apps/postprocessing_forecasts/src/data_reader.py:335` —
  `candidates[date_col] = pd.to_datetime(candidates[date_col])`.
- Reproduced (pandas 2.3.3, the locked version): `pd.to_datetime(pd.Series(["2024-12-25",
  "2025-03-25T00:00:00+06:00"]))` raises `ValueError: unconverted data remains when parsing with
  format "%Y-%m-%d": "T00:00:00+06:00", at position 1.`
- Same line and function on the PP-064 branch (`fix_pp_quarter_calendar_window`, HEAD `275826de`,
  worktree `sapphire-pp064a`): unchanged, per PP-064's own Contract not to modify this function.
- `format="mixed"` alone does not fix it either — reproduced: it silences the `ValueError` but the
  resulting object-dtype Series then raises `AttributeError` on `.dt.year`/`.dt.day` (the same
  failure mode PP-064 hit at its own two call sites) **and separately** raises `TypeError: can't
  compare offset-naive and offset-aware datetimes` from `sort_values(by=date_col)` (`:390`) — the
  tie-break this function relies on (see Proposed fix).

## Proposed fix

Once PP-064 Chunk A has merged, its `_issue_date_local_calendar_date` helper
(`apps/postprocessing_forecasts/src/data_reader.py`, added by that branch) is safe to reuse for the
day/month/year arithmetic here (`derived_lead` at `:346-348`, `issue_day` at `:349`) — those only ever read
`.dt.year`/`.dt.month`/`.dt.day`, which are unaffected by dropping time-of-day.

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
reusing PP-064's truncating helper. Recommendation: keep the existing latest-date-wins ordering.

Leave the `valid_from_col` parse (`:336`) untouched unless a similar mixed-format report surfaces
for it; every existing write path already writes `valid_from` as a bare date. Do not otherwise
change `select_operational_issuances`'s selection logic, signature, or the derived-lead/issue-day
arithmetic.

**Out of scope, deferred (do not fold in here):** the season branch of `_normalize_combined_forecasts`
(`:3770-3771`, `df["date"] = pd.to_datetime(df["date"], errors="coerce")`, no `format="mixed"`) and
`read_seasonal_forecasts` (calls `_normalize_combined_forecasts` at `:3263` before reaching
`select_operational_issuances`) have a **different** failure mode on the same kind of mixed batch:
reproduced — plain `pd.to_datetime(..., errors="coerce")` on a mixed tz-aware/naive batch does not
raise at all; it silently coerces the tz-aware row to `NaT` (data loss, not a crash), before
`select_operational_issuances` ever sees it. That is a separate defect (silent loss vs. this
issue's crash) on a separate code path; explicitly deferred, not covered by this issue's fix or
tests.

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

## Acceptance

- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` passes, zero unexpected skips.
- No existing test edited; the new tests fail on trunk (and on the PP-064 branch, pre-fix) with
  `ValueError`.
- `git diff --stat` limited to `apps/postprocessing_forecasts/src/data_reader.py` and its tests.
- Station code `19999` in any fixture; no real station codes.
