# PREPG-008: Snow climatology leap-year day-of-year alignment bug

**Status**: Draft (2026-06-25)
**Module**: `apps/preprocessing_gateway`
**Priority**: **Medium** (real snow data-quality bug; next leap year is 2028)
**Labels**: `preprocessing_gateway`, `snow-data`, `climatology`, `leap-year`, `dashboard`
**Related**: PREPG-001, PREPG-006, PREPG-007, FD-014

> Sanitized: use placeholder station code `19999` in tests and examples. Do not commit real
> station codes, real SWE values, or deployment secrets.

---

## Summary

Snow climatology is keyed by `dayofyear`. In leap years, dates after February shift by one day
relative to non-leap years. This conflates neighboring calendar dates and assigns the wrong norm and
percentile band to much of a leap year.

The bug is pre-existing, but PREPG-007's Jan-1 full-year band write makes it visible earlier and for a
whole leap year. It is split out because the fix must edit `recalculate_snow_norms.py`, which is
explicitly out of PREPG-007 scope.

---

## Problem

The snow norm/stat computation groups historical records by `df["date"].dt.dayofyear`. That makes
Feb 29 share the ordinal calendar with Mar 1 in non-leap years, and every date after February is
shifted by one in leap years relative to non-leap years.

Result: for roughly 300 days in and after leap years, the `norm`, `mean`, `min`, `max`, and
percentile bands assigned to a date can be a neighboring day's climatology rather than that calendar
date's climatology.

## Evidence

- `apps/preprocessing_gateway/dg_utils.py:453` sets `df["dayofyear"] = df["date"].dt.dayofyear` in
  `calculate_snow_norms_from_api()`.
- `apps/preprocessing_gateway/dg_utils.py:467` groups norms by `dayofyear`.
- `apps/preprocessing_gateway/dg_utils.py:593` sets `df["dayofyear"] = df["date"].dt.dayofyear` in
  `calculate_snow_stats_from_api()`.
- `apps/preprocessing_gateway/dg_utils.py:596` groups stats by `["snow_type", "code", "dayofyear"]`.
- `apps/preprocessing_gateway/recalculate_snow_norms.py:142` builds a fresh target-year
  `date_range`.
- `apps/preprocessing_gateway/recalculate_snow_norms.py:232`, `:234`, and `:272` remap each target
  date through `dt.dayofyear` lookups.

## Why Now

This bug already exists in the climatology implementation. PREPG-007 retargets `snow_norms` from
Aug 31 to Jan 1 and writes the full current calendar year immediately. In the next leap year, that
means the shifted climatology can be written early and persist across the whole dashboard season
instead of surfacing only late in August.

## Fix Direction

Choose one calendar-normalization strategy and apply it consistently to norms and stats:

- Key climatology by `(month, day)` and handle Feb 29 explicitly, for example by omitting it,
  interpolating it, or using a documented neighboring-day policy.
- Or normalize all years to a common 365-day climatology calendar before grouping, with an explicit
  Feb-29 policy.

Add leap/non-leap alignment tests that prove:

- Mar 1 climatology does not mix with Feb 29.
- Dates after February map to the same calendar day across leap and non-leap years.
- The target-year recalc writes the expected norm/stat values for a leap year and a non-leap year.

This requires editing `apps/preprocessing_gateway/recalculate_snow_norms.py` and the snow
climatology helpers/tests. Do not fold it into PREPG-007.

## Candidate Files

- `apps/preprocessing_gateway/dg_utils.py`
- `apps/preprocessing_gateway/recalculate_snow_norms.py`
- `apps/preprocessing_gateway/test/test_recalculate_snow_norms.py`
- `apps/preprocessing_gateway/test/test_api_integration.py` or a focused climatology test module

## Acceptance Criteria

- [ ] Snow norms and stats no longer group leap-year and non-leap-year calendar dates by raw
      `dayofyear` in a way that shifts dates after February.
- [ ] Feb 29 behavior is explicit, documented in code comments/docstrings, and covered by tests.
- [ ] Leap/non-leap tests prove Mar 1 and later dates receive the intended same-calendar-day
      climatology.
- [ ] Existing preservation behavior remains intact: recalc still preserves existing `value`,
      `current`, and elevation-band values.
- [ ] From `apps/`, `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` passes with zero
      unexpected skips. The only acceptable skip is the explicit `sapphire-api-client` dependency gate.

## Out of Scope

- PREPG-007 maintenance-window widening and cron retargeting.
- Dashboard rendering changes.
- `sapphire/services/**` changes.
