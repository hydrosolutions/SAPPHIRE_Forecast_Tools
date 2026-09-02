# Manual re-run / refresh path never re-renders the Predictors tab

**Priority:** mid — no crash, no data corruption, self-corrects the moment the operator
switches tabs. But it is a live staleness bug on the one action an operator takes specifically
to get fresh data (the manual forecast re-run), and it silently shows stale plots with no
"outdated" indicator — the same severity class as FD-020 (Low, but that case degrades to a
*missing/misleading warning*; this one degrades to *silently stale primary plots*, which is a
step worse), so this is scoped mid rather than low.
**Module:** `apps/forecast_dashboard` (fair game).
**Found:** 2026-09-02, while mapping the render call graph for an unrelated snow-window
defect (the `snow_ref_date` reference-date change — see Notes) to confirm the Predictors tab's
snow pane is rendered co-temporally with the data it displays. Pre-existing on trunk; not
introduced or touched by that change.
**Depends on:** none to file.

## What is implemented today

Four things cause `dm`'s data (`forecasts_all`, `snow_data`, the snow reference date, etc.) to
be (re)loaded, and each is expected to trigger a re-render of whichever tab is currently
active:

1. **Bootstrap** (`apps/forecast_dashboard/forecast_dashboard.py`): `dm.load_station(...)`
   runs at `:76`, before `PlotManager` is constructed at `:83` (so no render call is possible
   yet there). After `wm.wire(...)` / `pm.wire(...)` / `dm.wire_data_reload(...)` at
   `:101-103`, `pm.render_active_tab(dashboard_tabs)` is called explicitly at `:106` — an
   unconditional first render. Fine.
2. **Station or period change** — `WidgetManager._wire_station_period_change`'s `_on_change`
   (`apps/forecast_dashboard/dashboard/widget_manager.py:214-261`) calls `dm.load_station(...)`
   (`:219`), `dm.invalidate_render_cache()` (`:221`), and then explicitly
   `pm.render_active_tab(self._dashboard_tabs)` (`:239`), with a comment
   (`:234-238`) noting this is deliberate so the Predictors-tab panes refresh on station
   change even under long horizons. Fine.
3. **Horizon change** — routed through the same `_on_change` (all four widgets —
   `horizon_selector`, `station_selector`, `pentad_selector`, `decad_selector` — are wired to
   one `@pn.depends(..., watch=True)` handler, `widget_manager.py:213`). Same call path as
   (2). Fine.
4. **Manual forecast re-run / refresh** — `DataManager.wire_data_reload`'s `_do_reload`
   (`apps/forecast_dashboard/dashboard/data_manager.py:456-489`), invoked when
   `processing.data_reloader.data_needs_reload` fires. It calls `self.load_station(...)`
   (`:467`, refetching all station data, including `dm.snow_data` and advancing the snow
   reference date), `self.invalidate_render_cache()` (`:468`), and then
   `pm.refresh_all_visualizations()` (`:477`) — **not** `pm.render_active_tab(...)`.

`PlotManager.refresh_all_visualizations` (`apps/forecast_dashboard/dashboard/plot_manager.py:
493-496`):

```python
def refresh_all_visualizations(self):
    """Re-render every forecast-related visualisation."""
    self.update_forecast_plots()
    self.update_skill_table()
```

`update_forecast_plots` (`:433-453`) rebuilds the Forecast tab's hydrograph/skill/tabulator
panes; `update_skill_table` (`:455-488`) rebuilds the all-stations skill table. Neither calls
`_render_predictors_tab` (`:525-554`), which is the only method that rebuilds the Predictors
tab's four panes (`daily_hydrograph`, `daily_rainfall`, `daily_temperature`, `snow_plots`).
`_render_predictors_tab` is reached only through `render_active_tab`
(`:501-523`, when `active == 0`), which `refresh_all_visualizations` never calls.

`invalidate_render_cache` (`apps/forecast_dashboard/dashboard/data_manager.py:385-388`) resets
`_last_rendered_predictors_station`/`_last_rendered_forecast_station` to `None`, so the *next*
call to `render_active_tab` will pass its `dm.should_render_predictors(...)` guard
(`data_manager.py:373-377`) and re-render. But nothing in `_do_reload` calls
`render_active_tab` — the only thing that does is the `dashboard_tabs.param.watch("active",
...)` watcher installed in `PlotManager.wire` (`plot_manager.py:134-135`), which fires only on
a **tab-switch event** (the `active` index changing), not as a side effect of data reload.

## What is broken

If the operator is already sitting on the Predictors tab (`dashboard_tabs.active == 0`) when
they trigger a manual re-run, `_do_reload` refetches fresh data and invalidates the render
cache, but nothing re-renders the currently-visible tab: `refresh_all_visualizations` doesn't
touch the Predictors panes, and no tab-switch event occurs to fire the `active`-watcher. The
four Predictors panes (`daily_hydrograph`, `daily_rainfall`, `daily_temperature`, each
`snow_plots[var]`) keep showing objects built from the pre-reload data. There is no visual
indicator that they are stale — the render cache being invalidated is invisible to the
operator; only a subsequent tab switch (away and back, or to the Forecast tab and back) causes
`render_active_tab` to see the reset cache and rebuild them.

If the operator is on the Forecast tab (`active == 1`) when they trigger a reload,
`refresh_all_visualizations`'s `update_forecast_plots`/`update_skill_table` do refresh what
they're looking at directly — so the bug is specific to being on the Predictors tab during a
reload, not to reload in general.

## Reproduction conditions

1. Load the dashboard normally (bootstrap renders the Predictors tab, `active == 0` by
   default per `plot_manager.py:511`'s comment `# 0 = Predictors, 1 = Forecast`).
2. Stay on the Predictors tab (do not switch tabs).
3. Trigger `processing.data_reloader.data_needs_reload = True` (the manual forecast re-run /
   refresh action).
4. Observe: `_do_reload` runs, `dm.snow_data` / `dm.snow_ref_date` / `dm.hydrograph_day_all`
   etc. advance to the freshly fetched data, but `self.daily_hydrograph.object`,
   `self.daily_rainfall.object`, `self.daily_temperature.object`, and
   `self.snow_plots[var].object` are unchanged — still holding pre-reload panes — until the
   operator changes `dashboard_tabs.active` (e.g. clicks the Forecast tab and back).

## Consequence

This matters more now that the snow card's correctness depends on the render being
co-temporal with the fetch: the working-tree `snow_ref_date` mechanism
(`data_manager.py:136,154-161,247,254`, not yet committed) exists specifically so
`plot_daily_snow_data`'s display window and `dm.snow_data`'s fetch window always agree. A
reload that refreshes `dm.snow_data`/`dm.snow_ref_date` but leaves the rendered snow pane
built from the *previous* `snow_ref_date` reintroduces exactly the kind of fetch/render
mismatch that mechanism is meant to prevent — invisibly, on the one tab this bug affects.

## Fix direction

Have `_do_reload` (or `refresh_all_visualizations` itself) call
`pm.render_active_tab(self._dashboard_tabs)` — or equivalently `pm.render_active_tab(...)`
called from `wire_data_reload` with the tabs object it already has access to via `pm` — the
same way `_on_change` does at `widget_manager.py:239`, so the reload path performs the same
"invalidate then immediately render whatever is active" sequence as the other three entry
points, rather than relying on cache invalidation plus a future, uncorrelated tab-switch
event.

## Acceptance criteria

- After a manual reload while `dashboard_tabs.active == 0`, `daily_hydrograph.object`,
  `daily_rainfall.object`, `daily_temperature.object`, and each `snow_plots[var].object` are
  rebuilt from the post-reload `dm` data without requiring a tab switch — verified by a test
  that stubs/spies `_render_predictors_tab` (or checks `_last_rendered_predictors_station`
  reflects the post-reload station immediately after `_do_reload` returns, not only after a
  subsequent `render_active_tab` call).
- No change to behaviour when `dashboard_tabs.active == 1` at reload time (Forecast tab
  already refreshes correctly via `refresh_all_visualizations`).
- No change to the three already-correct entry points (bootstrap, station change, horizon
  change) — their existing tests must still pass unmodified.
- No real station codes/discharge in code, tests, or fixtures (`19999` placeholder if needed).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero unexpected
  skips.

## Notes

Found in the same review pass as the snow-season-label bug (sibling issue,
`mid_prio_gi_draft_fd_snow_season_label_off_by_day.md`), but independent — different files,
different mechanism. Filed separately because a fix to one has no bearing on the other.
