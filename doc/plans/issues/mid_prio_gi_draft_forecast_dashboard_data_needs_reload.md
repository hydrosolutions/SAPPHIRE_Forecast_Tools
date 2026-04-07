---
priority: mid
status: draft
module: forecast_dashboard
assignee: maxatp
---

# Dashboard does not reload forecast data after save or trigger operations

## Problem

After a bulletin save or a "Trigger forecasts" pipeline run, the forecast
dashboard does not display updated data. The visualizations re-render with the
same stale `forecasts_all` snapshot that was loaded at session start. Both
flows work correctly only after a manual browser reload.

## Root Cause

There are two separate gaps, one in each flow:

**Flow 1 — `data_needs_reload` watcher (save flow)**

In `apps/forecast_dashboard/dashboard/data_manager.py`, the
`_on_data_needs_reload` handler (lines 307–334, wired via `wire_data_reload`)
calls `pm.refresh_all_visualizations()` without first calling
`self.load_station()`. This means `forecasts_all` is not re-fetched from the
API before the plots are redrawn. The debug log tags D5/D6/D7 (added in commit
f7a3a6c) confirm that the row count and maximum date of `forecasts_all` are
identical before and after `refresh_all_visualizations()` returns, i.e. no
new data is loaded.

```python
# data_manager.py ~line 307
def _on_data_needs_reload(event):
    ...
    # Missing: self.load_station()   <-- data is never re-fetched
    pm.refresh_all_visualizations()  # renders with stale forecasts_all
```

**Flow 2 — "Trigger forecasts" button (retrigger flow)**

In `apps/forecast_dashboard/src/vizualization.py`, the `run_docker_pipeline`
function (lines 4076–4204) runs all Docker containers sequentially and then
calls `reset_ui_after_pipeline()` in its `finally` block. That function only
resets UI widgets (spinner, buttons, app state). It does **not** set
`processing.data_reloader.data_needs_reload = True`, so the watcher in
`data_manager.py` is never triggered and the dashboard is never refreshed.

```python
# vizualization.py ~line 4193
finally:
    @pn.io.with_lock
    def reset_ui_after_pipeline():
        loading_spinner.visible = False
        progress_message.visible = False
        warning_message.visible = False
        reload_button.disabled = False
        app_state.pipeline_running = False
        # Missing: processing.data_reloader.data_needs_reload = True
```

Note: the existing assignment `processing.data_reloader.data_needs_reload =
True` at line 3928 belongs to a different (save-button) flow; the retrigger
flow has no equivalent assignment.

## Suggested Fix

**Fix 1** — In `data_manager.py`, call `self.load_station()` inside
`_on_data_needs_reload` before calling `pm.refresh_all_visualizations()`, so
that `forecasts_all` (and any other station-scoped data) is re-fetched from
the API first.

**Fix 2** — In `vizualization.py`, set
`processing.data_reloader.data_needs_reload = True` inside
`reset_ui_after_pipeline()` (or immediately after calling it) so that the
watcher fires and the dashboard refreshes once all containers have completed.
This should be done only on the success path (or at minimum guarded so it does
not fire if the pipeline errored out without producing new results).

Taken together, Fix 1 and Fix 2 ensure the complete chain: pipeline finishes →
flag set → watcher fires → station data re-fetched → visualizations redrawn.

## Workaround

Manual browser reload after save/trigger operations.
