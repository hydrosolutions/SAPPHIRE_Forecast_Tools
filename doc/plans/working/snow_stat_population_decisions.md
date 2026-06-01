# Snow Stat Population — Committed Design Decisions

Source: `doc/plans/issues/high_prio_gi_draft_gateway_snow_stat_population.md`
§Decisions Committed (rounds 1-3 reviewed; approved 2026-06-01).

These five defaults are committed. Phase 1 and Phase 2 agents implement
to these decisions; do not re-litigate them.

## 1. `previous` and `current` at write time vs read time

DEFAULT: Write `previous` and `current` in the recalc script (Phase 2).

RATIONALE: Hydrograph writes these fields during stat population
before `_write_hydrograph_to_api`, preserving the API contract that
comparison values live on each row. Snow has no forecast-run hook, so
the closest equivalent is the yearly write-side recalc. This keeps the
dashboard reader simple and avoids a client-side fallback path. Phase 2
computes these from the year-Y and year-(Y-1) snow rows it already
fetches; Phase 1's helper is year-independent and intentionally does
not return them.

## 2. Partial-year handling

DEFAULT: Compute percentile/stat columns only from complete historical
years; for an incomplete target year, write only `current` from that
year's raw value and `previous` from the prior year when available.

RATIONALE: Percentiles from partial years can distort the dashboard
bands because winter/spring snow values and summer zeros are seasonally
imbalanced. Keeping stats tied to complete years makes the bands
climatological, while `current` still gives the dashboard the live-year
line.

## 3. Minimum-years threshold

DEFAULT: `n_years_min = 5`; below threshold, return rows with `count`
populated and stat columns as `NaN`, not absent rows.

RATIONALE: Hydrograph has no explicit threshold, and existing snow norms
write whatever data exists. Snow station history can be short, though,
and percentile bands from 1-2 years are visually overconfident. Five
years is a conservative default that preserves row shape while
preventing weak climatologies from masquerading as robust bands.
Loosely consistent with WMO climatological-normals convention which
prefers ≥10 years but tolerates shorter records when station history
is limited; five is a pragmatic compromise for the snow network's
variable history depth.

## 4. Leap-year DOY alignment

DEFAULT: Use the existing snow norm convention: `date.dt.dayofyear`
grouping, including day 366 for leap years.

RATIONALE: `calculate_snow_norms_from_api` already groups by
`dt.dayofyear` at `dg_utils.py:453`, and the recalc script builds
target-year dates from the same DOY convention at
`recalculate_snow_norms.py:122-166`. Matching snow norms is more
important than importing hydrograph's pentad-specific leap handling
from `forecast_library.py`. Note: this DOY rule governs Phase 1's
climatology grouping only. `previous` in Phase 2 uses calendar-date
alignment (year-1, same month, same day) — see the plan's Phase 2 Goal.

## 5. Initial backfill scope

DEFAULT: Yes, run a one-time full-history backfill for all historical
years and all discovered stations after implementation.

RATIONALE: The dashboard reads historical date windows, and Phase 0
already showed null fields in both 2024 and 2025-2026 windows. A
current-year-only run would unblock only part of the display. Add an
operator-side `bin/` script that loops years and calls the unified
recalc with progress logging and resumability (Phase 5).
