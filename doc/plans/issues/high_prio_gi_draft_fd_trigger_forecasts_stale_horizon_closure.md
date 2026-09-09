# FD-028: "Trigger forecasts" always re-runs the pentad pipeline, even when the operator has decade (or month/season) selected

**Status**: Draft
**Module**: `apps/forecast_dashboard` (fair game)
**Priority**: High — deterministic on every use of a real, currently-shipped button whenever the
operator has anything other than pentad selected; silently runs the wrong horizon's pipeline
while showing no indication that the wrong horizon was used.
**Labels**: `forecast-dashboard`, `silent-wrong-behavior`, `stale-closure`
**Found**: 2026-09-09, out-of-loop review of a PR built from LR-013
(`high_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md`) and FD-027
(`high_prio_gi_draft_fd_decade_horizon_prediction_mode_spelling.md`), which had both wrongly
assumed the dashboard's "Trigger forecasts" button forwards the currently-selected horizon.
Verified directly against trunk `3791fa31`.
**Related**: **LR-013** / **FD-027** — those two issues originally (and wrongly) attributed part
of their own defect to "Trigger forecasts"; that attribution has been corrected there and the
actual defect split out here. This issue does **not** depend on LR-013/FD-027 shipping, and
fixing LR-013/FD-027 does **not** fix this. **FD-008**
(`high_prio_gi_draft_fd_inner_run_docker_error_handling.md`) is why this defect's own failures
(and any other container failure in the "Trigger forecasts" flow) would go unnoticed even once
this issue starts producing visible symptoms — not a dependency, but the reason this has likely
been silent.

---

## Summary

The forecast dashboard's **"Trigger forecasts"** button ("Save Changes" is a separate, unaffected
button) is wired to a callback, `run_pipeline`, that was built **once**, when the dashboard was
constructed, using whatever horizon was selected **at that moment** — always `"pentad"`, the
widget's default. The callback never re-reads the horizon selector afterward. So no matter what
horizon an operator has selected when they click "Trigger forecasts" — decade, month, season —
the button always re-runs the **pentad** pipeline (`linreg`, ML models, postprocessing), silently.
An operator who selects decade and clicks "Trigger forecasts," believing they are re-running a
decade forecast, is actually re-running (and potentially overwriting outputs of) the pentad
forecast instead, with no indication anything went differently than requested.

## Defect

`create_horizon_selector` (`apps/forecast_dashboard/dashboard/widgets.py:90-110`) defaults the
horizon selector's value to `"pentad"` (`:107`):

```python
horizon_selector = pn.widgets.Select(
    name=_("Select forecast horizon:"),
    options=horizon_types,
    value="pentad",
    ...
)
```

`WidgetManager`'s constructor (`apps/forecast_dashboard/dashboard/widget_manager.py:107`) builds
the "Trigger forecasts" button **exactly once**, at dashboard-construction time, passing the
selector's *current* value as a plain argument:

```python
self.reload_card = cfg.viz.create_reload_button(self.horizon_selector.value)
```

At this point in construction, `self.horizon_selector.value` is still `"pentad"` — nothing has
run that could have changed it yet. `create_reload_button(horizon)`
(`apps/forecast_dashboard/src/vizualization.py:4248`) takes this as a plain function parameter
and defines its nested `run_pipeline` callback as a closure over it:

```python
def create_reload_button(horizon):
    ...
    def run_pipeline(event):
        ...
        environment = [
            ...
            f'SAPPHIRE_PREDICTION_MODE={horizon.upper()}',   # :4355 — always "PENTAD"
            ...
        ]
        ...
        mode = horizon.upper()                                # :4415 — always "PENTAD"
        ...
    reload_button.on_click(run_pipeline)
    return reload_card
```

`create_reload_button` is **never called again**. The code that shows or hides the resulting
`reload_card` based on which dashboard tab is active
(`apps/forecast_dashboard/src/vizualization.py:125-143`) only ever toggles its `.visible`
attribute:

```python
reload_card.visible = True   # or False, depending on active_tab
```

It never rebuilds the card, never re-invokes `create_reload_button`, and there is no `@pn.depends`
or watcher anywhere that would cause `run_pipeline` to be redefined when the horizon selector
changes. `horizon` inside `run_pipeline` is therefore a **permanently stale** closure variable,
fixed at `"pentad"` for the entire dashboard session, regardless of any later selection.

## Consequence

Every click of "Trigger forecasts," for the life of the dashboard session, launches:

1. `mabesa/sapphire-preprunoff:latest` (`:4390`) — does not read
   `SAPPHIRE_PREDICTION_MODE` at all, unaffected either way.
2. `mabesa/sapphire-linreg:latest` (`:4400`) with `SAPPHIRE_PREDICTION_MODE=PENTAD` — **always**,
   never the selected horizon.
3. `mabesa/sapphire-ml:latest`, once per configured model (`:4410-4422`, on by default), also
   with `SAPPHIRE_PREDICTION_MODE=PENTAD` — **always**.
4. `mabesa/sapphire-postprocessing:latest` (`:4432`) — **always** with `PENTAD`.

If the operator has pentad selected, this happens to be correct by coincidence — the button
"works" in that case, which is very likely why this has not been reported: pentad is also the
default and, anecdotally, the more commonly used horizon. If the operator has **decade** selected
(or, since the same widget also offers `month`/`season` when ML forecasts are enabled, one of
those), clicking "Trigger forecasts":

- Silently re-runs the **pentad** forecast pipeline for every configured station, potentially
  **overwriting current pentad LR/ML/postprocessing outputs** with a re-run the operator did not
  ask for and may not expect at that moment (e.g. if pentad data changed since the last real
  pentad run, this creates an unrequested pentad update as a side effect of a decade action).
- Does **not** run anything for the decade (or month/season) horizon the operator actually
  selected and believes they are re-triggering.
- Shows no error, no warning, and no indication that a different horizon than selected was used —
  the progress bar and any success messaging behave identically to a correct pentad run, because
  from the code's perspective, that is exactly what happened.

This is a different failure shape from LR-013/FD-027 (which are about an *unrecognised* value
being silently dropped) — this is a **stale, wrong-but-valid** value being silently substituted
for the operator's actual selection, which arguably has worse consequences: LR-013/FD-027 produce
no forecast; this produces a **different, real forecast than requested**, potentially overwriting
current data for the wrong horizon.

## Why this has likely gone unnoticed

Per **FD-008**, both of the dashboard's `run_docker_container` implementations swallow container
failures rather than surfacing them — but that is not even needed to explain this defect's
invisibility, since nothing here fails: the pentad pipeline runs successfully, every time,
regardless of selection. There is no error for FD-008 to swallow. The invisibility here comes
entirely from the UI not reflecting which horizon was actually used, and from pentad being both
the default and (per this issue's Consequence section) not visibly wrong if the operator's mental
model of "which forecast is stale" happens not to have caught it yet.

## Desired outcome

`run_pipeline` must use the *live* horizon selection at the moment it runs, not the value
captured at dashboard-construction time. Two approaches (owner to choose — not decided here):

**Option A — Read the selector live inside `run_pipeline`.** Pass the `WidgetManager` (or the
selector itself) into `create_reload_button` instead of a resolved string value, and have
`run_pipeline` read `wm.horizon_selector.value` (or equivalent) at call time, mirroring how
`save_to_database` already does this correctly. Smallest change in shape, consistent with the
one already-correct pattern in the same file.

**Option B — Rebuild or re-wire the button when the horizon selector changes.** Add a watcher
(`@pn.depends(horizon_selector.param.value, watch=True)` or equivalent) that reconstructs
`reload_card`/`run_pipeline` (or rebinds its closure) whenever the selection changes, alongside
the existing tab-driven `.visible` toggling. More invasive, keeps the "build once" structure
elsewhere in the file.

Either approach must also fix the ML branch's independent `mode = horizon.upper()` at `:4415`,
since it currently re-derives from the same stale `horizon` parameter rather than the (also
stale) `environment` list.

## What a fix must not break

- Pentad "Trigger forecasts" behavior must remain correct — it is the one case that already
  happens to work.
- The tab-driven `.visible` show/hide behavior at `vizualization.py:125-143` must be unaffected.
- This fix does **not** need to touch `linear_regression.py`, `postprocessing_operational.py`,
  `recalculate_skill_metrics.py`, or `make_forecast.py` — once `run_pipeline` forwards the live
  horizon correctly, "Trigger forecasts" becomes exposed to the *same* `DECADE`-spelling mismatch
  LR-013/FD-027 already describe for "Save Changes." **This fix and LR-013/FD-027's normalization
  should ideally land together or in the right order** — fixing this issue alone, before
  LR-013/FD-027 ship, would convert today's "silently runs pentad instead" into "silently writes
  nothing" (LR-013's own defect) for a decade selection — a different but still-silent failure,
  not a full fix. Sequencing this after LR-013/FD-027 avoids that intermediate state.

## Out of scope

- LR-013 and FD-027's own normalization fixes — independent of this issue.
- FD-008's swallowed-failure fix — independent; this issue's own defect does not currently
  produce any failure for FD-008 to swallow (see "Why this has likely gone unnoticed").
- Whether `month`/`season` "Trigger forecasts" clicks are even meant to be supported by this
  button (the ML model loop suggests they might be, but this issue does not investigate whether a
  month/season pipeline re-run via this button is otherwise functional once the horizon is read
  live — only that today it silently substitutes pentad).

## Acceptance criteria

1. Selecting decade and clicking "Trigger forecasts" launches containers with
   `SAPPHIRE_PREDICTION_MODE` reflecting **decade** (post-normalization, whatever LR-013/FD-027
   land as the correct spelling), not `PENTAD`.
2. Selecting pentad and clicking "Trigger forecasts" continues to work exactly as today.
3. The ML loop's `mode` variable (`:4415`) reflects the same live selection as the main
   `environment` list, not a second, independently-stale read.
4. A test exercises a horizon-selection change **followed by** a "Trigger forecasts" click and
   asserts the resulting container environment reflects the *new* selection, not the value at
   dashboard construction — this is the regression guard that matters most, since the bug is
   invisible under any test that only ever constructs the dashboard once and clicks without
   changing the selector first (exactly the shape that let this go undetected).
5. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` — zero failures, zero
   new skips.

## References

- `apps/forecast_dashboard/dashboard/widgets.py:90-110` (`create_horizon_selector`), `:107`
  (default value `"pentad"`)
- `apps/forecast_dashboard/dashboard/widget_manager.py:107` (`create_reload_button` called once,
  at construction, with the selector's then-current value)
- `apps/forecast_dashboard/src/vizualization.py:4248` (`create_reload_button` definition), `:4317`
  (`run_pipeline`, the stale closure), `:4355` (`SAPPHIRE_PREDICTION_MODE={horizon.upper()}`,
  always `PENTAD`), `:4390` (preprunoff, unaffected — doesn't read the variable), `:4400` (linreg,
  always receives `PENTAD`), `:4410-4422` (ML loop, `mode = horizon.upper()` at `:4415`, always
  `PENTAD`), `:4432` (postprocessing, always receives `PENTAD`)
- `apps/forecast_dashboard/src/vizualization.py:125-143` (`reload_card.visible` toggled by active
  tab — never rebuilt, confirming no mechanism re-creates the stale closure)
- `apps/forecast_dashboard/src/vizualization.py:3449` (`horizon = wm.horizon_selector.value`
  inside `select_and_plot_data` — the *correct* pattern `save_to_database` uses, contrasted here)
- `doc/plans/issues/high_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md` (LR-013 — the
  sibling defect this issue was split out of; see its "Fourth-pass corrections")
- `doc/plans/issues/high_prio_gi_draft_fd_decade_horizon_prediction_mode_spelling.md` (FD-027 —
  same; see its "Correction: 'Trigger forecasts' never sends `DECADE`" section)
- `doc/plans/issues/high_prio_gi_draft_fd_inner_run_docker_error_handling.md` (FD-008 — why any
  failure in this flow, including a future one from this issue's own fix landing before
  LR-013/FD-027, would go unnoticed)
