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

- Trunk `82946683`: `apps/postprocessing_forecasts/src/data_reader.py:335` —
  `candidates[date_col] = pd.to_datetime(candidates[date_col])`.
- Reproduced (pandas 2.3.3, the locked version): `pd.to_datetime(pd.Series(["2024-12-25",
  "2025-03-25T00:00:00+06:00"]))` raises `ValueError: unconverted data remains when parsing with
  format "%Y-%m-%d": "T00:00:00+06:00", at position 1.`
- Same line and function on the PP-064 branch (`fix_pp_quarter_calendar_window`, HEAD `275826de`,
  worktree `sapphire-pp064a`): unchanged, per PP-064's own Contract not to modify this function.

## Proposed fix

Once PP-064 Chunk A has merged, reuse its `_issue_date_local_calendar_date` helper
(`apps/postprocessing_forecasts/src/data_reader.py`, added by that branch) for the `date_col` parse
at `:335` — it keeps only the first 10 characters (the local calendar date) before parsing, so a
mixed tz-aware/tz-naive batch cannot raise. Leave the `valid_from_col` parse (`:336`) untouched
unless a similar mixed-format report surfaces for it; every existing write path already writes
`valid_from` as a bare date. Do not otherwise change `select_operational_issuances`'s selection
logic, signature, or the derived-lead/issue-day arithmetic that depends on `date_col`/`valid_from_col`
being naive datetimes.

## Tests

- Direct rows for Q1 2025 issued `"2024-12-25"` and Q2 2025 issued
  `"2025-03-25T00:00:00+06:00"`, lead 1, issue day 25, `SAPPHIRE_SKILL_LEAD_AWARE=true` → no
  exception from `read_quarterly_forecasts(codes, 2025, 2025)`, and both quarters are returned with
  their operational issuance selected.
- Same rows, no exception from `read_latest_quarterly_forecasts(codes, forecast_date=date(2025, 6,
  1))`, flag ON.
- A direct unit test on `select_operational_issuances` with the same mixed-format `date` column,
  asserting the selected row's derived lead and issue day are unaffected by the tz offset (i.e. the
  local-calendar-date truncation does not shift which schedule a row matches).

## Acceptance

- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` passes, zero unexpected skips.
- No existing test edited; the new tests fail on trunk (and on the PP-064 branch, pre-fix) with
  `ValueError`.
- `git diff --stat` limited to `apps/postprocessing_forecasts/src/data_reader.py` and its tests.
- Station code `19999` in any fixture; no real station codes.
