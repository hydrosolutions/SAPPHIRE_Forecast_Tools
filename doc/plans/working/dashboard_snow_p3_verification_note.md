# Dashboard Phase 3 - Wiring & Layout Verification

**Date:** 2026-06-01
**Branch:** develop_dashboard_snow_display
**Upstream commits:** P1 f10d1d9, P2 05985f2, P4 e8ba095

## 1. db.py snow_data callsites

| Callsite | Function | Line | Consumer | Enriched contract preserved? |
|---|---|---:|---|---|
| Short-horizon loader | `get_data()` | 684 | Assigned directly to `data["snow_data"]`; `DataManager.snow_data` returns this dict unchanged, and `PlotManager.update_plots()` passes `dm.snow_data` into `viz.plot_daily_snow_data()` for each variable. | Yes. No column filtering or projection at the callsite; downstream `plot_daily_snow_data()` reads the stat columns if present. |
| Monthly loader | `_get_data_monthly()` | 743 | Assigned directly to `data["snow_data"]`; monthly long-forecast merge logic only touches forecast tables, not snow data. `PlotManager.update_plots()` passes `dm.snow_data` through unchanged. | Yes. No snow DataFrame columns are dropped or filtered before plotting. |
| Quarterly loader | `_get_data_quarter()` | 778 | Returned directly as `"snow_data"` in the loader result; the dashboard data manager exposes it unchanged and the plot manager passes the full dict to `plot_daily_snow_data()`. | Yes. No consumer at this callsite accesses only legacy `<variable>`/`norm` columns. |
| Seasonal loader | `_get_data_season()` | 794 | Returned directly as `"snow_data"` in the loader result; the dashboard data manager exposes it unchanged and the plot manager passes the full dict to `plot_daily_snow_data()`. | Yes. No consumer at this callsite accesses only legacy `<variable>`/`norm` columns. |

Static handoff check:

- `apps/forecast_dashboard/dashboard/data_manager.py:128-130` exposes `self._data.get("snow_data")` without transformation.
- `apps/forecast_dashboard/dashboard/plot_manager.py:499-507` iterates `dm.snow_data.keys()` and passes the complete `dm.snow_data` dict to `viz.plot_daily_snow_data(...)`.
- `apps/forecast_dashboard/src/vizualization.py:2257` selects the variable DataFrame with `snow_data.get(variable)`, and lines 2342-2349 include `mean`, `min`, `max`, `5%`, `25%`, `75%`, `95%`, `last_year`, and `current_year` when present.

## 2. layout.py snow-card structure

`_create_snow_card()` at `apps/forecast_dashboard/src/layout.py:97-124` orders plots as `SWE`, `HS`, `RoF`, filters out panes whose `.object` is `None`, and returns either a no-data `pn.Card` titled `Snow Data (SnowMapper)` or a populated `pn.Card` titled `Snow Data`.

For populated cards, the helper computes `card_height = len(valid_plots) * 400 + 100` and applies `sizing_mode="stretch_both"` plus `min_height=card_height`. It does not set a restrictive fixed `height` or `max_height`. The Phase 2 plot itself uses `height=400`, so the layout still allocates one plot-height per snow pane plus 100 px padding.

Callsites:

- `layout.py:167` wraps `_create_snow_card(_, daily_snow_plot)` in a `pn.Row` in the inactive `no_date_overlap_flag == False` branch. The row has no explicit height; the card controls its own minimum height.
- `layout.py:225` wraps `_create_snow_card(_, daily_snow_plot)` in a `pn.Row` with `sizing_mode="stretch_both"` in the active predictors branch. The row has no fixed height or maximum height.
- `layout.py:416` wraps `_create_snow_card(_, pm.snow_plots)` in a `pn.Row` with `sizing_mode="stretch_both"` and `visible=cfg.display_snow_data` in the dashboard-manager layout. The row has no fixed height or maximum height.

The card labels are short (`Snow Data`, `Snow Data (SnowMapper)`) and are not made longer by Phase 2. Static review found no concrete label clipping, fixed-height clipping, or max-height constraint that would visibly break the larger hydrograph-style overlay. Because this phase forbids executing the dashboard, no runtime visual defect was established.

## 3. Decision

`LAYOUT_UNCHANGED: YES`

## 4. Implications for downstream phases

Phase 5 integration tests can proceed with the assurance that all four dashboard data-loading paths preserve the enriched snow contract through to `plot_daily_snow_data()`, and no Phase 3 layout fix is required from static review.
