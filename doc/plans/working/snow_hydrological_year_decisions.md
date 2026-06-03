# Snow Hydrological-Year Display — Committed Design Decisions

Source: `doc/plans/issues/high_prio_gi_draft_dashboard_snow_hydrological_year.md`
§Decisions (rounds 1-2 reviewed; approved 2026-06-02).

These nine decisions are committed. Phase 1, Phase 2, and Phase 3
agents implement to them; do not re-litigate.

## Decisions (User)

### D1 — Semantics

Keep `current_year` / `last_year` on calendar-date alignment. No
changes to `recalculate_snow_norms.py` or `previous` / `current`
field semantics.

### D2 — Fetch window

`_get_snow_single()` derives `start_date` / `end_date` from
`snow_display_window()` instead of the fixed `PREVIOUS_YEAR-01-01` /
`CURRENT_YEAR-12-31` pattern.

### D3 — Climatology grouping

Stay calendar-DOY. `calculate_snow_stats_from_api()` in
`apps/preprocessing_gateway/dg_utils.py` is not changed.

### D4 — Labels

Switch legend labels to season-aware wording when display start is
not `01-01`; add i18n keys in both `en_CH` and `ru_KG`.

## Decisions Committed (Planner Defaults)

### D-Q1 — Helper location

DEFAULT: Move snow window logic into new
`apps/forecast_dashboard/src/snow_window.py` as public
`snow_display_window(...)`.

RATIONALE: Avoids `src/db.py` importing heavy visualization code
and lets tests import the real helper.

### D-Q2 — Config threading

DEFAULT: Thread optional display-start values from
`DashboardConfig` through `DataManager`, `db.get_data(...)`,
horizon loaders, `get_snow_data(...)`, and `_get_snow_single(...)`.

RATIONALE: `dashboard/config.py:45` remains the single env-parse
source. `plot_manager.py:505-506` already threads config to
`viz.plot_daily_snow_data()`; only data-fetch side needs threading.
`plot_manager.py` is intentionally absent from Phase 1 Files.

### D-Q3 — `ref_date` parameter

DEFAULT: Add optional `ref_date` / `snow_ref_date` parameters for
deterministic tests.

RATIONALE: Production callers pass `None`; `_get_snow_single`
resolves to `date.today()`. `get_snow_data` snapshots the date once
before HS / ROF / SWE sub-fetches. Future tickets may thread
dashboard date-picker semantics if needed; out of scope here.

### D-Q4 — Label format

DEFAULT: Use `Current season {YYYY}/{YY+1}` and
`Previous season {YYYY}/{YY+1}`, e.g. `Current season 2025/26`.

RATIONALE: Slash notation is compact and ASCII-only. Current season
is determined from the latest non-null current snow curve date in
the displayed window, falling back to `date_picker`; previous
season is one season earlier. If neither exists, fall back to
`display_begin` year.

### D-Q5 — Predicate location

DEFAULT: If display start is `01-01`, preserve "Current year" /
"Last year". Add public `is_hydrological_year_display(month, day) ->
bool` in `apps/forecast_dashboard/src/snow_window.py`.

RATIONALE: Co-locating the predicate with `snow_display_window(...)`
avoids duplicate `(1, 1)` checks; public naming wins for clarity.
