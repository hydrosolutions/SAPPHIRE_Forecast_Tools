# Dashboard robustness: stale cross-horizon metadata reaches warnings; flag parser can raise on the quarter read path

**Priority:** low — Bug 1 is a cosmetic/misleading-warning defect on a real, if narrow, user
path (no crash, no bad data written). Bug 2 is a crash-on-typo defect on a data path that is
currently **latent** (unreachable from the UI), matching FD-019's finding for the same
`"quarter"` horizon. Neither loses or corrupts persisted data.
**Module:** `apps/forecast_dashboard` (fair game) and `apps/iEasyHydroForecast` (fair game;
`skill_lead_aware_flag.py` is scaffolding for `SAPPHIRE_SKILL_LEAD_AWARE`, not a
`sapphire/services/` boundary).
**Found:** 2026-07-14, adversarial review of `develop_ltf_monthly_horizon_value`. Both bugs
are **pre-existing on trunk** (`origin/maxat_sapphire_2`) — neither was introduced by that
branch. The branch deliberately did not fix either because it is held to a hard "flag-OFF
behaviour must be byte-identical to trunk" contract (`SAPPHIRE_SKILL_LEAD_AWARE`, default
OFF); fixing either bug touches shared (non-flag-gated) code, which the branch was scoped to
avoid.
**Depends on:** none to file. A future fix must state explicitly whether it changes
flag-OFF behaviour (see Acceptance criteria) — expected answer for both is "no", but that
must be verified, not assumed.

## Bug 1 — stale cross-horizon metadata still reaches the forecast warning pane

**Files:** `apps/forecast_dashboard/dashboard/widget_manager.py`
**Call chain:** `_on_change` (`widget_manager.py:213-260`, the horizon-selector watcher) →
`refresh_warnings` (`widget_manager.py:342-355`) →
`widgets.refresh_forecast_warning` (`widgets.py:367-375`) →
`widgets.get_period_warning` (`widgets.py:325-364`).

### What is implemented today

`_on_change` refreshes `self.last_date` / `self.forecast_horizon` / `self.forecast_year` from
`dm.get_bulletin_metadata(horizon)` on every horizon/station/period change
(`widget_manager.py:245-247`). If that call raises (`get_bulletin_metadata` itself raises
`ValueError` for no forecast data, or the caller's `except` also covers `KeyError`,
`IndexError`, `TypeError` for malformed frames — `widget_manager.py:249`), the handler
**deliberately** leaves the three attributes untouched — i.e. still holding the
**previous** horizon's values — and instead sets `self._metadata_horizon = None`
(`widget_manager.py:255`), a marker added on this branch specifically to record that the
triple no longer belongs to the currently-selected horizon. The comment at
`widget_manager.py:251-254` states this is deliberate: "self.last_date/forecast_horizon/
forecast_year deliberately stay stale ... mark them as not belonging to `horizon`".

The header consumer already does the right thing with that marker:
`_refresh_horizon_info_pane` (`widget_manager.py:362-373`) passes
`metadata_is_current=(self._metadata_horizon == current_horizon)` into
`widgets.format_horizon_info` (`widgets.py:587-603`), whose docstring
(`widgets.py:592-600`) explains exactly why — "trusting a stale cross-horizon period number
as a month index can crash or silently mislabel the month" — and the function is documented
to render nothing (return `""`) when the metadata is not current.

### What is broken

`refresh_warnings` (`widget_manager.py:342-355`), called from the **same** `_on_change`
handler one line after the marker is set (`widget_manager.py:256`), does not consult
`self._metadata_horizon` at all. It unconditionally forwards
`getattr(self, "forecast_horizon", None)` and `getattr(self, "forecast_year", None)` — the
stale values — into `widgets.refresh_forecast_warning`, which passes them straight into
`widgets.get_period_warning(horizon, forecast_period, forecast_year, today=None)`
(`widgets.py:373`). `get_period_warning` has no way to know the triple is stale: it only
checks `forecast_period is not None and forecast_year is not None`
(`widgets.py:330-331`), computes `current` from the **currently selected** `horizon`
(`widgets.py:334-345`), and compares `(forecast_year, forecast_period) < (today.year,
current)` (`widgets.py:346-349`) to decide whether to render the "may be outdated" alert
(`widgets.py:350-364`).

### Concrete failure scenario

1. Station is on horizon `"pentad"`; `dm.get_bulletin_metadata("pentad")` succeeds, setting
   `self.forecast_horizon` to a small `pentad_in_year`, e.g. `3` (early January), and
   `self._metadata_horizon = "pentad"`.
2. User switches to horizon `"month"` for a station/date combination where monthly forecast
   data is not yet available for this station — an anticipated case per the
   `widget_manager.py:250` comment ("No data for this horizon yet; bulletin callback handles
   it."). `dm.get_bulletin_metadata("month")` raises; the `except` fires;
   `self.forecast_horizon` stays `3`, `self.forecast_year` stays whatever it was;
   `self._metadata_horizon` becomes `None`.
3. `_refresh_horizon_info_pane` correctly renders the header blank
   (`metadata_is_current=(None == "month")` is `False`).
4. `refresh_warnings` calls `get_period_warning(horizon="month", forecast_period=3,
   forecast_year=<stale year>, today=<e.g. July>)`. The `"month"` branch sets
   `current = today.month` (e.g. `7`). `(stale_year, 3) < (today.year, 7)` is `True` for a
   stale year `<= today.year`, so `outdated` is `True`, and the pane renders: "The displayed
   month forecast is for month 3 of `<stale_year>`, but the current month is 7 of
   `<today.year>`. This forecast may be outdated." — a bogus warning built from a
   `pentad_in_year` value (`3`) that was never a month number, misrepresented as "month 3".

### Fix vehicle

`self._metadata_horizon` is the natural fix vehicle — `refresh_warnings` should skip (or
pass `None` for) `forecast_horizon`/`forecast_year` when
`self._metadata_horizon != self.horizon_selector.value`, the same test
`_refresh_horizon_info_pane` already applies. This is **not** gated by
`SAPPHIRE_SKILL_LEAD_AWARE` — `_metadata_horizon` is set unconditionally in `_on_change`
regardless of flag state — so applying the same check in `refresh_warnings` would not change
flag-OFF behaviour; it only changes what happens after a failed metadata refresh, which is
already an off-nominal path today (the "outdated" warning is currently just wrong, not
absent, in that case). This was left out of `develop_ltf_monthly_horizon_value` to keep that
branch's diff scoped to the flag-gated monthly-lead work; it is a general dashboard
robustness fix, not specific to the branch's feature.

## Bug 2 — the fail-loud flag parser can raise on the quarter data-read path

**Files:** `apps/iEasyHydroForecast/skill_lead_aware_flag.py`,
`apps/forecast_dashboard/src/db.py`.
**Call chain:** `db.get_data(horizon="quarter")` (`db.py:878-886`) →
`_get_data_quarter` (`db.py:1121-1166`) → `skill_lead_aware_enabled()`
(`db.py:1135`, inside `_get_data_quarter`) →
`skill_lead_aware_flag.skill_lead_aware_enabled()` (`skill_lead_aware_flag.py:38-60`).

### What is implemented today

`skill_lead_aware_enabled()` is a deliberately fail-loud parser
(`skill_lead_aware_flag.py:38-60`): it returns `True` for a recognised truthy token
(`1`/`true`/`yes`/`on`), `False` for unset or a recognised falsey token
(`0`/`false`/`no`/`off`), and otherwise **raises `ValueError`**
(`skill_lead_aware_flag.py:57-60`) — by design, so a typo'd env value (e.g.
`SAPPHIRE_SKILL_LEAD_AWARE=tru`) fails loudly instead of silently resolving to OFF. The
docstring (`skill_lead_aware_flag.py:47-52`) states this explicitly as a `Raises` contract,
not an oversight.

The branch already fixed the equivalent leak on the monthly bulletin-metadata path:
`data_manager.get_bulletin_metadata` only calls `skill_lead_aware_enabled()` inside
`if horizon == "month" and skill_lead_aware_enabled():` (`data_manager.py:415`, inside
`get_bulletin_metadata`; verify the current line — this file is under active edit by a
concurrent branch task as of this writing), so
Python's short-circuit evaluation means the flag is never read for `"quarter"` or
`"season"` bulletins at that call site.

### What is broken

`db.py`'s `_get_data_quarter` (`db.py:1121-1166`) was **not** given the same treatment,
because `db.py` is deliberately kept byte-identical to trunk on
`develop_ltf_monthly_horizon_value`. Its call is unconditional within the function body:

```python
merge_keys = ["code", hin, "model_short"]
if skill_lead_aware_enabled():          # db.py:1135
    merge_keys = merge_keys + ["horizon_value"]
```

Unlike Bug 1, this call is not gated by a `horizon ==` check at the call site itself —
`_get_data_quarter` has no other horizon to branch on, since the function only runs for
`horizon == "quarter"`. So a typo'd flag value raises on **any** call to
`_get_data_quarter`, regardless of whether the lead-aware feature (which only concerns the
monthly path) is relevant.

### Is this reachable today? — checked, and it is **not**, matching FD-019

I re-checked `widgets.create_horizon_selector` (`widgets.py:90-110`) independently of
FD-019: the `horizon_types` dict only ever contains `"pentad"`, `"decade"`, and — when
`display_ML_forecasts` is true — `"month"` and `"season"` (`widgets.py:97-103`).
`"quarter"` is never a selectable value. `db.get_data`'s dispatcher
(`db.py:856-889`) only routes to `_get_data_quarter` when `horizon == "quarter"`
(`db.py:878-886`), and I found no caller in `apps/forecast_dashboard` that passes
`"quarter"` as the `horizon` argument to `get_data` — every live call site derives
`horizon` from `wm.horizon_selector.value` (which per the above can never be `"quarter"`)
or hardcodes `"month"`. This matches FD-019's independent conclusion
(`doc/plans/issues/mid_prio_gi_draft_fd_bulletin_target_year_derivation.md`, "Is this
reachable today? — Latent, not live") for the same `"quarter"` horizon on a sibling code
path. **`_get_data_quarter` is currently dead code on every deployment** — reachable only
from direct unit-test calls or a future UI change that exposes quarter (already flagged as
fragile by FD-011). This sets the priority to low: a real fail-loud crash risk, but with
zero current user-facing exposure. If a quarter option is ever added to the horizon
selector, this bug becomes live immediately.

### Failure scenario (if/when reachable)

`SAPPHIRE_SKILL_LEAD_AWARE=tru` (typo) set in the dashboard's environment. If any future
code path calls `db.get_data("quarter", ...)` — e.g. a quarter option added to the horizon
selector, or a bulletin/report path that reads quarter data directly — `_get_data_quarter`
raises `ValueError` on the very first call, taking down whatever dashboard render triggered
it, even though the lead-aware feature this flag governs is entirely about the monthly
horizon and has no bearing on quarter data loading.

### Open design question (not decided here)

Is fail-loud-on-typo the right contract for a **read** path in a live dashboard, where an
uncaught exception takes down the display for every user, or should the dashboard log loudly
and degrade to OFF on read paths while write/recalc paths (batch jobs in
`apps/postprocessing_forecasts` — `recalculate_skill_metrics.py`,
`postprocessing_operational_long_term.py`, `postprocessing_maintenance_long_term.py`,
`ensemble_calculator.py`, `skill_metrics.py`, all call the same
`skill_lead_aware_enabled()`) keep failing loud by design, since a batch-job crash is
visible in logs and safe to fail?

There is a partial precedent already in the same file, for a *different* exception type on
the same monthly read path: `_safe_lead` (`db.py:960-969`, used inside `_get_data_monthly`)
resolves an operational lead from config and, on `LongTermHorizonResolverError` or
`FileNotFoundError`, logs a warning and falls back to a caller-supplied default rather than
propagating — specifically because (per its comment, `db.py:956-959`) "requiring
`operational_issue_day` here would crash a dashboard READ." That precedent shows the
branch's design instinct already leans toward "reads degrade, don't crash" for *that*
helper, but it was never applied to `skill_lead_aware_enabled()` itself — which is a shared
helper used identically by both read and write callers today, so changing its contract is a
bigger call than this one call site. **This issue surfaces the question; it does not answer
it.** Whoever picks up the fix must pick one of:
1. Change `skill_lead_aware_enabled()`'s contract (affects every caller, read and write — as
   of this writing that is 14 files across `apps/postprocessing_forecasts` and
   `apps/forecast_dashboard` with 40+ individual call sites; re-run
   `grep -rn "skill_lead_aware_enabled(" apps/` to get the current, authoritative list
   before scoping this option — do not trust a stale count).
2. Add a narrower, `db.py`-local guard around this one call site only (mirrors the
   `data_manager.get_bulletin_metadata` `horizon ==` short-circuit pattern described above,
   but there is no other horizon to gate on inside `_get_data_quarter` — the guard would
   have to be a `try/except ValueError` around just this call, falling back to the
   un-lead-aware `merge_keys`).
3. Leave the raise as-is and instead prevent `_get_data_quarter` from ever running until a
   quarter UI option exists with its own explicit flag-typo handling designed in from the
   start.

## Acceptance criteria

- Bug 1: after a horizon switch where `dm.get_bulletin_metadata` raises for the new
  horizon, `refresh_warnings` must not render a period-outdated warning derived from the
  previous horizon's stale `forecast_horizon`/`forecast_year` — either suppress the warning
  entirely (mirroring the header's blank-on-stale behaviour) or otherwise make it unable to
  misinterpret a foreign horizon's period number. Verified via a unit test on
  `refresh_warnings`/`get_period_warning` that reproduces the pentad→month stale-metadata
  scenario above (station/date fixture with `19999` as the placeholder station code, no real
  station codes).
- Bug 1 fix must state explicitly whether it changes flag-OFF behaviour. Expected: **no** —
  `self._metadata_horizon` is set unconditionally regardless of
  `SAPPHIRE_SKILL_LEAD_AWARE`, so gating `refresh_warnings` on it changes only the
  already-off-nominal stale-metadata path, not the flag-ON/OFF split. This must be verified
  by running the existing `develop_ltf_monthly_horizon_value`
  flag-OFF-byte-identical/golden tests (e.g. `test_monthly_lead_golden.py`) unmodified
  against the fix, not merely asserted.
- Bug 2: whichever option from the "open design question" is chosen, it must not change
  `skill_lead_aware_enabled()`'s behaviour for any horizon/call site that is not `"quarter"`
  in `db.py` — the flag's fail-loud contract for the monthly path (and every
  `postprocessing_forecasts` write/recalc caller) is out of scope unless the fix explicitly
  chooses option 1 above and surveys the full call-site list (see open question above). The
  fix must land before (or in the same PR as) any future UI change that exposes a quarter
  horizon option, with a regression test asserting the typo'd-flag case no longer raises out
  of `_get_data_quarter` (or documenting why raising is still correct, if option 3 is
  chosen).
- No real station codes/discharge in code, tests, or fixtures (`19999` placeholder if
  needed).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero unexpected
  skips.

## Notes

Both bugs surfaced from the same `develop_ltf_monthly_horizon_value` adversarial review pass
as FD-019 (bulletin `forecast_year` derivation) and FD-018 (m0 bulletin per-site target
month), but are independent mechanisms in independent code paths — do not conflate. Filed
together because both are dashboard-robustness defects on off-nominal/unreachable paths
discovered in the same review sweep, low severity, and low enough scope to fix in a single
small PR if desired — not because they share a root cause. A fix may address one without the
other.
