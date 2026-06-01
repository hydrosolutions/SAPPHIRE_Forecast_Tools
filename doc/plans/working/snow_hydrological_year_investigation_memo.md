# Snow Hydrological-Year Display Investigation Memo

## TL;DR

- Snow display-window configuration exists: the dashboard reads `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD`, defaults it to `01-01`, stores parsed month/day in `DashboardConfig`, and passes those values into `plot_daily_snow_data()` (`apps/forecast_dashboard/dashboard/config.py:45`, `apps/forecast_dashboard/dashboard/config.py:65`, `apps/forecast_dashboard/dashboard/plot_manager.py:505`).
- The kghm operator env sets the snow display start to September 1 (`$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm:94`; same relevant key also appears in the Dropbox-backed env open in the IDE).
- The end-to-end gap is not that Phase 2 dropped the display-window parameters; the plot applies them. The unresolved gap is that the snow stat/write-side product is calendar-year shaped (`Jan 1` to `Dec 31`) and computes `previous` by same calendar date in `year - 1`, so the hydrological-year x-axis does not imply hydrological-year-aligned current/previous comparison semantics (`apps/preprocessing_gateway/recalculate_snow_norms.py:142`, `apps/preprocessing_gateway/recalculate_snow_norms.py:201`, `apps/preprocessing_gateway/recalculate_snow_norms.py:256`).

## Existing Configuration Mechanism

The configuration is an optional env var, documented as `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD` with scope `dashboard` and purpose "Snow viz start day (MM-DD)" (`doc/configuration.md:241`).

At dashboard initialization, `apps/forecast_dashboard/dashboard/config.py` reads that env var with calendar-year default `01-01` (`config.py:45`), parses month/day from the string (`config.py:47`), validates it by constructing a non-leap-year date (`config.py:48`-`config.py:49`), and falls back to `(1, 1)` on parse/validation failure (`config.py:50`-`config.py:51`).

The parsed values are part of the immutable dashboard config: `DashboardConfig` has `snow_display_start_month` and `snow_display_start_day` fields (`config.py:25`-`config.py:26`), and `init_dashboard()` populates them (`config.py:65`-`config.py:66`).

The dashboard only renders snow if snow data is configured and weather/forecast data is displayed: `display_weather_and_snow_data()` reads `ieasyhydroforecast_HRU_SNOW_DATA_DASHBOARD` (`config.py:219`) and returns `display_snow_data` based on that value plus `display_weather_data` (`config.py:221`-`config.py:227`).

The parsed snow display-start config is threaded into the snow plot caller. `PlotManager._render_predictors_tab()` calls `viz.plot_daily_snow_data()` and passes `snow_display_start_month=self._cfg.snow_display_start_month` plus `snow_display_start_day=self._cfg.snow_display_start_day` (`apps/forecast_dashboard/dashboard/plot_manager.py:502`-`plot_manager.py:506`).

Related implementation-plan breadcrumbs also recognize the hydrological-year setting. The earlier snow visualization enhancement plan explicitly called for this env var and default (`doc/plans/issues/mid_prio_gi_draft_dashboard_snow_visualization_enhancements.md:125`, `doc/plans/issues/mid_prio_gi_draft_dashboard_snow_visualization_enhancements.md:143`), and its acceptance text mentions `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD=09-01` (`doc/plans/issues/mid_prio_gi_draft_dashboard_snow_visualization_enhancements.md:294`).

No runtime widget for changing the snow hydrological-year start was found in `apps/forecast_dashboard/`; the mechanism is env-driven at dashboard initialization.

## kghm Operator Setting

The requested kghm env file sets `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD` to `09-01`, i.e. September 1 (`$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm:94`). The comment immediately above it describes the format and dashboard snow-visualization intent (`$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm:93`). I did not include any other env values because the file contains operational credentials and station lists.

The IDE-open Dropbox-backed env path also has the same relevant snow display setting, also set to September 1. This suggests the operator-side configuration is present and not merely planned.

## How Adjacent Plots Handle This

The daily hydrograph uses a calendar-year convention. Its dashboard data fetch requests `start_date=f"{CURRENT_YEAR}-01-01"` and `end_date=f"{CURRENT_YEAR}-12-31"` (`apps/forecast_dashboard/src/db.py:165`-`db.py:170`). The backend fields `previous` and `current` are renamed to string labels for `PREVIOUS_YEAR` and `CURRENT_YEAR` (`db.py:180`-`db.py:183`), and `plot_daily_hydrograph_data()` chooses `current_year = int(data['date'].dt.year.max())` and `last_year = current_year - 1` (`apps/forecast_dashboard/src/vizualization.py:1733`-`vizualization.py:1734`). It then renames those calendar-year columns for plotting (`vizualization.py:1763`-`vizualization.py:1776`).

I did not find a dashboard-wide hydrological-year abstraction used by runoff/daily hydrograph. The adjacent daily runoff/hydrograph pattern is calendar year plus calendar-year previous/current comparisons, so changing snow to hydrological-year-aligned comparisons would be snow-specific unless the product decision also changes runoff conventions.

The snow write-side follows a similar calendar-year convention. `recalculate_snow_norms.py` builds one target-year date range from `Jan 1` for `365/366` days (`apps/preprocessing_gateway/recalculate_snow_norms.py:139`-`recalculate_snow_norms.py:143`). It reads existing target-year snow records from `year-01-01` to `year-12-31` (`recalculate_snow_norms.py:177`-`recalculate_snow_norms.py:187`), reads prior-year records from `prior_year-01-01` to `prior_year-12-31` (`recalculate_snow_norms.py:201`-`recalculate_snow_norms.py:213`), sets `current` from the target-year row's own value (`recalculate_snow_norms.py:253`-`recalculate_snow_norms.py:254`), and computes `previous` via same calendar date in `year - 1` (`recalculate_snow_norms.py:256`-`recalculate_snow_norms.py:264`).

The snow climatology stats also group by calendar day-of-year. `calculate_snow_stats_from_api()` documents grouping by `(snow_type, code, dayofyear)` (`apps/preprocessing_gateway/dg_utils.py:484`-`dg_utils.py:490`), computes `df["dayofyear"] = df["date"].dt.dayofyear` (`dg_utils.py:593`), and groups by `["snow_type", "code", "dayofyear"]` (`dg_utils.py:596`-`dg_utils.py:599`).

## Current Snow Plot Window Handling

Phase 2 did not drop the snow display-start parameters. `plot_daily_snow_data()` still accepts `snow_display_start_month=1` and `snow_display_start_day=1` defaults (`apps/forecast_dashboard/src/vizualization.py:2250`-`vizualization.py:2252`).

The helper `_snow_display_window(start_month, start_day, ref_date)` returns a calendar-year range only for `1/1` (`vizualization.py:2235`-`vizualization.py:2240`). For non-Jan-1 starts, it chooses the current or prior start date relative to `ref_date` and ends the window on the day before the next start (`vizualization.py:2241`-`vizualization.py:2247`). The companion test copy verifies that `9/1` and a spring 2026 reference returns `2025-09-01` to `2026-08-31` (`apps/forecast_dashboard/tests/test_snow_display_window.py:83`-`test_snow_display_window.py:87`).

Inside `plot_daily_snow_data()`, the function converts the plot date (`vizualization.py:2279`-`vizualization.py:2280`), filters data to the selected station (`vizualization.py:2282`-`vizualization.py:2283`), calls `_snow_display_window()` with the configured month/day and selected date (`vizualization.py:2295`-`vizualization.py:2298`), and filters the plotted frame to dates between `display_begin` and `display_end` (`vizualization.py:2299`-`vizualization.py:2302`). Forecasts are then derived from that filtered frame (`vizualization.py:2305`-`vizualization.py:2306`).

The plot uses the same hydrological display bounds for the x-axis: `figure.opts(... xlim=(display_begin, display_end), ...)` (`vizualization.py:2393`-`vizualization.py:2405`).

Within that filtered window, the plot overlays statistical and comparison fields from the already-fetched records. It chooses `mean` or `norm` (`vizualization.py:2342`-`vizualization.py:2343`), chooses `current_year` if present otherwise the raw variable (`vizualization.py:2344`), and plots `last_year` plus `current_year_col` against the filtered `date` column (`vizualization.py:2376`-`vizualization.py:2383`). Those `last_year`/`current_year` columns originate from backend `previous`/`current` fields renamed in `_get_snow_single()` (`apps/forecast_dashboard/src/db.py:28`-`db.py:35`, `db.py:275`).

The data fetch range for snow is `PREVIOUS_YEAR-01-01` through `CURRENT_YEAR-12-31` (`apps/forecast_dashboard/src/db.py:255`-`db.py:263`). On June 1, 2026, a September-start current hydrological-year window (`2025-09-01` to `2026-08-31`) is inside that range. The previous hydrological-year window (`2024-09-01` to `2025-08-31`) is not fully inside that range, but the current plot does not separately fetch or render a prior hydrological-year date range; it renders the backend `last_year`/`previous` value on the current window's dates.

Phase 0/Phase 1 evidence kept the snow fetch window at `PREVIOUS_YEAR-01-01` to `CURRENT_YEAR-12-31` (`doc/plans/working/snow_field_population_check.md:88`), while the later snow display plan warned that a September-to-August display can ask for dates not fetched by `db.py` if the window extends beyond `CURRENT_YEAR-12-31` (`doc/plans/issues/high_prio_gi_draft_dashboard_snow_percentile_display.md:223`).

## The Gap

The chain does not break at config read, caller threading, or Phase-2 plot parameter use. The configured kghm value is available, the caller passes it, and the plot filters/x-limits by `_snow_display_window()`.

The break is at the semantic/data-product layer: hydrological-year display has been implemented as an x-axis/date filter over a calendar-year-shaped snow product. The most important lines are:

- `date_range = pd.date_range(start=f"{year}-01-01", periods=n_days, freq="D")` (`apps/preprocessing_gateway/recalculate_snow_norms.py:142`)
- `# Compute \`previous\` via calendar-date alignment to year-1` (`apps/preprocessing_gateway/recalculate_snow_norms.py:256`)
- `prior_date = _date(prior_year, target_date.month, target_date.day)` (`apps/preprocessing_gateway/recalculate_snow_norms.py:259`)

That means the visual window can run September 2025 through August 2026, but the overlaid `current_year` and `last_year` series are still the target calendar date's own value and the same calendar date in the prior calendar year. There is no current code path that defines "current hydrological year" and "previous hydrological year" as complete September-August seasonal records, nor a fetch of the full prior hydrological year (`2024-09-01` to `2025-08-31`) as a separate comparison curve.

There is also a secondary future-date fetch gap. For a September-start display on June 1, 2026, `_get_snow_single()` fetches through `2026-12-31`, so the current hydrological-year x-axis is covered. But after September 1, 2026, `_snow_display_window(9, 1, 2026-09-01)` returns `2026-09-01` to `2027-08-31`; `_get_snow_single()` would still only fetch through `CURRENT_YEAR-12-31` for the process year, so dates in 2027 would be missing unless the fetch window is widened or keyed to display bounds.

## Proposed Change Shape

- Decide the intended semantics: either keep hydrological-year support as display-only x-axis configuration, or make `current_year`/`last_year` mean current/prior hydrological year for snow. This is a product decision because adjacent runoff hydrograph conventions are calendar-year based.
- If display-only is enough, make the current behavior explicit in labels/docs and verify the kghm deployment actually loads the env file before dashboard initialization. The relevant files are `dashboard/config.py`, `dashboard/plot_manager.py`, and `src/vizualization.py`.
- If hydrological-year comparison is required, update the snow data product and/or dashboard shaping so prior hydrological-year comparison data is available for the selected hydrological window. The affected areas are `_get_snow_single()` in `apps/forecast_dashboard/src/db.py`, the comparison-column assumptions in `plot_daily_snow_data()`, and the snow stat writer in `apps/preprocessing_gateway/recalculate_snow_norms.py`.
- Revisit the fetch window. A September-to-August display needs either display-bound-aware fetches or a wider fixed range than `PREVIOUS_YEAR-01-01` to `CURRENT_YEAR-12-31`, especially once the selected/current date is after the hydrological-year start and the display extends into the next calendar year.
- Adjust labels/ticks only after semantics are decided: the x-axis already uses month/day formatting and `xlim`, but the plot title/legend still says "Current year" and "Last year"; hydrological-year comparison likely needs wording such as season/window labels rather than calendar-year labels.

## Scope Estimate

**Medium.** The config-to-plot threading is already in place, so this is not a broad dashboard architecture change. But making the display truly hydrological-year end-to-end requires coordinated decisions and changes across the dashboard fetch range, snow plot comparison semantics, and possibly the preprocessing snow-stat writer/backfill product, with tests around September/January boundary behavior.

## Open Questions for the User

1. Should the snow plot's "current year" and "last year" curves follow the hydrological year, or is the current behavior acceptable where only the visible x-axis window follows September-August?
2. For kghm, is September 1 always the intended snow hydrological-year start, or should deployments be free to set other starts such as October 1 through the existing env var?
3. If hydrological-year comparison is required, should the backend `previous/current` snow fields be redefined for snow only, or should the dashboard compute/shape hydrological-year comparison curves from raw date ranges while leaving backend fields calendar-based?
4. Should `_get_snow_single()` widen to cover at least previous hydrological year plus next-calendar-year dates, or should it derive fetch bounds from `_snow_display_window()` and the configured start?
5. Should climatological stats remain grouped by calendar `dayofyear`, or should snow stats gain hydrological-day-of-year alignment for deployments with non-Jan-1 starts?

## References

- Snow display config read/parse/default: `apps/forecast_dashboard/dashboard/config.py:45`-`config.py:51`
- Dashboard config fields and return values: `apps/forecast_dashboard/dashboard/config.py:25`-`config.py:26`, `apps/forecast_dashboard/dashboard/config.py:65`-`config.py:66`
- Snow plot caller threading: `apps/forecast_dashboard/dashboard/plot_manager.py:502`-`plot_manager.py:506`
- Snow display-window helper: `apps/forecast_dashboard/src/vizualization.py:2235`-`vizualization.py:2247`
- Snow plot configured-window filter and xlim: `apps/forecast_dashboard/src/vizualization.py:2295`-`vizualization.py:2302`, `apps/forecast_dashboard/src/vizualization.py:2402`-`vizualization.py:2405`
- Snow fetch range and field rename: `apps/forecast_dashboard/src/db.py:255`-`db.py:263`, `apps/forecast_dashboard/src/db.py:28`-`db.py:35`, `apps/forecast_dashboard/src/db.py:275`
- Daily hydrograph calendar-year convention: `apps/forecast_dashboard/src/db.py:165`-`db.py:183`, `apps/forecast_dashboard/src/vizualization.py:1733`-`vizualization.py:1776`
- Snow stat writer calendar-year records and calendar-date `previous`: `apps/preprocessing_gateway/recalculate_snow_norms.py:139`-`recalculate_snow_norms.py:143`, `apps/preprocessing_gateway/recalculate_snow_norms.py:201`-`recalculate_snow_norms.py:213`, `apps/preprocessing_gateway/recalculate_snow_norms.py:253`-`recalculate_snow_norms.py:264`
- Snow climatology day-of-year grouping: `apps/preprocessing_gateway/dg_utils.py:484`-`dg_utils.py:490`, `apps/preprocessing_gateway/dg_utils.py:593`-`dg_utils.py:599`
- Operator env location checked: `$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm:93`-`:94`
- Upstream dashboard plan date-window note: `doc/plans/issues/high_prio_gi_draft_dashboard_snow_percentile_display.md:223`
- Snow field population DATE_WINDOW_DECISION: `doc/plans/working/snow_field_population_check.md:88`
