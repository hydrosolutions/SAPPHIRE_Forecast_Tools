# Snow plot's "previous season" legend label is off by a day, not a year

**Status**: Review (2026-09-02). Implemented — see § Implementation status below.
`SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` is green: 653 passed, 6 skipped
(gated `TEST_LOCAL`/`TEST_PENTAD`/`TEST_DECAD` only), zero unexpected skips.
**Priority:** mid — no data corruption, no crash, and the underlying plotted values (the
`last_year` climatology curve) are correct. But the legend text is wrong on every day of the
season except the one exact start-day, on any deployment that configures a hydrological-year
snow display (`ieasyhydroforecast_SNOW_DISPLAY_START_MMDD` != `01-01`, e.g. FD-014) — a
hydrologist reading the snow chart is told the "previous season" curve is the *same* season
as "current", which misrepresents a year-over-year comparison chart every time it is viewed.
**Module:** `apps/forecast_dashboard` (fair game).
**Found:** 2026-09-02, while mapping a snow-window fetch/render defect (the `snow_ref_date`
parameter change — see Notes) in `apps/forecast_dashboard/src/vizualization.py`. Pre-existing
on trunk; not introduced or touched by that change.
**Depends on:** none to file.

## What is implemented today

`_snow_season_label(reference, start_month, start_day)`
(`apps/forecast_dashboard/src/vizualization.py:2237-2243`) maps a date to its
hydrological-year label (`"YYYY/YY+1"`) by comparing `(reference.month, reference.day) >=
(start_month, start_day)`:

```python
def _snow_season_label(reference, start_month, start_day):
    """Return the YYYY/YY+1 label for the hydrological year containing reference."""
    if (reference.month, reference.day) >= (start_month, start_day):
        start_year = reference.year
    else:
        start_year = reference.year - 1
    end_year_two_digit = (start_year + 1) % 100
    return f"{start_year}/{end_year_two_digit:02d}"
```

Its two call sites, inside `plot_daily_snow_data`
(`apps/forecast_dashboard/src/vizualization.py:2342-2345`, reached only when
`is_hydrological_year_display(snow_display_start_month, snow_display_start_day)` is true —
`vizualization.py:2340`), compute:

```python
current_season = _snow_season_label(
    current_ref, snow_display_start_month, snow_display_start_day)
previous_season = _snow_season_label(
    current_ref - dt.timedelta(days=1),
    snow_display_start_month,
    snow_display_start_day)
```

`current_ref` comes from `_snow_current_season_reference` (`vizualization.py:2246-2254`) —
the latest non-null `current_year` date, else `date_picker`, else the display-window start.
The two labels feed `current_year_label = _("Current season {season}")` and `last_year_label
= _("Previous season {season}")` (`vizualization.py:2347-2348`), which become the legend
entries for the `current_year_line` and `last_year_line` curves
(`vizualization.py:2382-2386`).

## What is broken

Subtracting **one day** from `current_ref` only crosses the `(month, day) >= (start_month,
start_day)` boundary when `current_ref` falls exactly on the configured season start day. On
every other day of the year, `current_ref` and `current_ref - 1 day` compare the same way
against `(start_month, start_day)`, so `_snow_season_label` returns the **same** `YYYY/YY+1`
string for both `current_season` and `previous_season`. The "previous season" legend entry
then reads as the current season's years, not the season a year earlier — the intended
operation is subtracting one **year** (or one hydrological year), not one **day**.

Example: `start = 09-01`, `current_ref = 2026-11-15`. `current_season` = `"2026/27"`. `2026-11-15
- 1 day = 2026-11-14`, still `>= (9, 1)`, so `previous_season` is also `"2026/27"`. The chart
legend shows "Current season 2026/27" and "Previous season 2026/27" for two different curves
(this year's data vs. last year's climatology), even though the curves themselves are
correctly computed from different years.

## Reproduction conditions

- `is_hydrological_year_display(snow_display_start_month, snow_display_start_day)` must be
  true, i.e. the deployment's `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD` env var (read in
  `apps/forecast_dashboard/dashboard/config.py:45`, default `'01-01'`) is set to anything
  other than `01-01`.
- `current_ref` must **not** equal the configured start day exactly (true on every day of the
  season but one).
- Verified directly with the existing test fixtures: `start_month=9, start_day=1,
  date_picker="2025-12-15"` yields `current_season == previous_season == "2025/26"` (see
  below). `test_snow_plot_season_year_label_transitions_at_start_day`
  (`apps/forecast_dashboard/tests/test_snow_plot.py:262-278`) demonstrates the mechanism
  directly: with `ref_date="2025-09-01"` (exactly the start day) the labels correctly differ
  (`"2025/26"` vs `"2024/25"`); with `ref_date="2025-08-31"` (one day off the boundary) both
  labels are `"2024/25"` — the bug, already captured in that test's own case data.

## Existing tests that encode the current (buggy) behaviour

`apps/forecast_dashboard/tests/test_snow_plot.py`,
`test_snow_plot_labels_use_season_wording_when_start_is_sept_1`
(`test_snow_plot.py:235-259`), lines 246-247:

```python
assert any("Current season 2025/26" in label for label in labels)
assert any("Previous season 2025/26" in label for label in labels)
```

and the same pair of assertions repeated at lines 258-259 for the `no_current_value_plot`
variant. Both assert the **buggy** pairing — current and previous season sharing the same
`"2025/26"` label — for `date_picker="2025-12-15"`, `start_month=9, start_day=1`. A correct
fix must change `previous_season` to `"2024/25"` in both cases, so these assertions will fail
under a correct fix and must be updated as part of it — call this out explicitly in the PR so
it does not read as an accidental test regression.

`test_snow_plot_season_year_label_transitions_at_start_day`
(`test_snow_plot.py:262-278`) already asserts the **correct** value for the one day where the
current buggy code happens to get it right (the exact start day, `"2025-09-01"` case) and the
**buggy** value for the off-boundary day (`"2025-08-31"` case, both labels `"2024/25"`). A
correct fix changes the `"2025-08-31"` case's expected `previous_label` from `"Previous
season 2024/25"` to `"Previous season 2023/24"`; the `"2025-09-01"` case is already correct
and must not change.

## Fix direction

Compute `previous_season` from a reference one **hydrological year** earlier than
`current_ref` — e.g. `_snow_season_label(current_ref.replace(year=current_ref.year - 1),
...)`, guarding the Feb-29 case, or by subtracting one year via a small helper — not
`current_ref - dt.timedelta(days=1)`. The result should differ from `current_season` by
exactly one year pair on every `current_ref`, not just on the start day.

## Acceptance criteria

- For any `current_ref` and any `(start_month, start_day) != (1, 1)`, `previous_season`'s
  start year is exactly `current_season`'s start year minus one — verified by a
  property-style or parametrised test sweeping several `current_ref` values across a season,
  not just the one boundary day already covered.
- `test_snow_plot_labels_use_season_wording_when_start_is_sept_1` and
  `test_snow_plot_season_year_label_transitions_at_start_day` are updated (not deleted) to
  assert the corrected previous-season values, with a comment noting the values changed and
  why.
- No change to `current_season`'s derivation or to the plotted curve data — this is a legend
  string fix only, matching the "display only" scope of this defect (see Notes).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero unexpected
  skips.

## Implementation status (2026-09-02)

Fixed in `apps/forecast_dashboard/src/vizualization.py`. As foreshadowed in Fix direction, the
Feb-29 case ruled out `current_ref.replace(year=current_ref.year - 1)` (raises `ValueError`
when the reference year is a leap year and the previous year is not, e.g. 2028-02-29). Instead
of date arithmetic, `_snow_season_label` was factored into two small int/string helpers so the
previous season can be derived from the current season's **start year**, never from a date:

- `_season_start_year(reference, start_month, start_day)` — returns the integer start year of
  the hydrological year containing `reference` (the `(month, day) >= (start_month, start_day)`
  comparison, unchanged, extracted out of `_snow_season_label`).
- `_season_label_from_start_year(start_year)` — formats a start year as `"YYYY/YY+1"` (the
  formatting half, also extracted unchanged).
- `_snow_season_label(reference, start_month, start_day)` keeps its exact original signature and
  return value — now a thin wrapper calling the two helpers above. Its one other call site (the
  empty-window guard, `vizualization.py`) and any test that calls it directly are unaffected.

In `plot_daily_snow_data`, the `is_hydrological_year_display` branch now computes
`current_start_year = _season_start_year(current_ref, ...)`, `current_season =
_season_label_from_start_year(current_start_year)`, and `previous_season =
_season_label_from_start_year(current_start_year - 1)` — i.e. one **hydrological year** behind
`current_season` on every `current_ref`, not one **day**. `current_season`'s value, the title,
the forecast split, the window computation, and the empty-window guard are all unchanged.

Test assertions updated in `apps/forecast_dashboard/tests/test_snow_plot.py` (all update the
*expected value*, none change what is being tested):

- `test_snow_plot_labels_use_season_wording_when_start_is_sept_1` (both the main and
  `no_current_value_plot` cases): `"Previous season 2025/26"` → `"Previous season 2024/25"`
  (one season behind `"Current season 2025/26"`, not the same season).
- `test_snow_plot_season_year_label_transitions_at_start_day`: re-expressed from asserting
  "correct on the boundary day, buggy off it" to asserting the one-season-behind invariant on
  both reference dates it already covered — `"2025-08-31"`'s expected previous label changed
  from `"Previous season 2024/25"` to `"Previous season 2023/24"`; `"2025-09-01"`'s
  (`"Previous season 2024/25"`) was already correct and is unchanged.
- `test_snow_plot_season_and_title_follow_plotted_window_not_date_picker`: expected label
  changed from `"Previous season 2026/27"` to `"Previous season 2025/26"` (one season behind the
  unchanged `current_season` of `2026/27`); a comment was added recording that this test still
  discriminates the `window_ref` regression it targets (reverting that regression would change
  the label to `"2024/25"`, not `"2026/27"`).
- Added `test_snow_plot_season_label_leap_day_reference_does_not_raise`: reference
  `2028-02-29` (2028 is a leap year, 2027 is not) with a Jan-1 start, calling the helpers
  directly — proves the chosen (start-year-based) fix does not hit the `ValueError` a
  `.replace(year=...)` fix would.

Mutation-checked: reintroducing the one-day subtraction fails both updated
`test_snow_plot_labels_use_season_wording_when_start_is_sept_1` and
`test_snow_plot_season_year_label_transitions_at_start_day`; reintroducing the `date_picker`
fallback ahead of `window_ref` in `_snow_current_season_reference` fails
`test_snow_plot_season_and_title_follow_plotted_window_not_date_picker`. Both mutations were
reverted after confirming the failures.

## Notes

Found while mapping `plot_daily_snow_data`'s call graph for an unrelated defect: the function
now accepts a `snow_ref_date` parameter (currently uncommitted in the working tree, alongside
`_snow_current_season_reference`) so the plotted window and the fetched data share the same
reference date. That change does not touch `_snow_season_label` or its call sites — cite the
mechanism (the snow-window reference-date work), not a commit hash, since it has not landed
yet. Not a duplicate of FD-014 (configurable year start/units/labels, In Progress) — FD-014
is about making the start-day configurable and its display correct in general; this issue is
a specific arithmetic bug in the label computation that exists regardless of what FD-014
lands.
