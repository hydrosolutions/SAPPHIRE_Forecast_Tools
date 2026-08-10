# m0 bulletin card hydrates from the MAIN panel's target month, not its own (Defect G)

**Priority:** mid — display/data-quality bug scoped to Kyrgyz `month_0` bulletin
entries only; no crash, and the main monthly bulletin path is unaffected.
**Module:** `apps/forecast_dashboard` (fair game).
**Found:** 2026-07-13/14, split out of `develop_ltf_monthly_horizon_value` (originally
tracked as "Defect G" in
`high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md` /
`working/ltf_monthly_horizon_value_implementation_plan.md` P3). The branch's own attempted
fix (`_month0_hydration_params`, gated on the flag) was found ineffective and has been
reverted; see "Why the attempted fix didn't work" below.
**Depends on:** `SAPPHIRE_SKILL_LEAD_AWARE` (existing flag; default OFF). Any fix here
must be gated on it and be byte-identical to trunk when the flag is off.

---

## Resolution (2026-08-10)

Fixed and merged: PR #420 (`16fb9a9b`). The live corruption path — the m0 bulletin's Excel
export writing the **main panel's** target month instead of its own — is fixed:
`bulletin_manager.py` now captures each site's own target period at add time
(`_resolve_month_target_period`) and stores it on the site (`site.bulletin_target_period`),
and the add/write path (`_on_write`) honours that per-site period instead of re-deriving one
bulletin-wide period for every site. **Reload correctness is consciously deferred** — an
attempted reload-time heuristic (`_resolve_reload_month_target_period`) was found to be
actively worse than trunk (could resolve to the wrong frame, could read a stale cached value,
could swallow a raised exception and silently discard the bulletin) and was deleted; reload
now falls back to the bulletin-wide period, byte-identical to pre-fix trunk. The durable fix
for reload requires a schema change on `Bulletin` (colleague-managed) — see the bulletin
target-period-field issue (`doc/plans/issues/mid_prio_gi_draft_pp_bulletin_target_period_field.md`).
Verified against trunk 2026-08-10.

---

## Summary

When a user clicks "Add to bulletin" on the **m0** (lead-0, current-month) forecast
card, the site is added to `bulletin_sites` with attributes hydrated from the **m0
frame's own** target month (`_on_add_m0`, `apps/forecast_dashboard/dashboard/bulletin_manager.py:508-567`,
calling `_month_hydration_params()` at add-time). That part works. The bug is that this
hydration is **silently overwritten** the moment the bulletin is written or reloaded,
because both of those paths re-derive a single, bulletin-wide target month from the
**main** panel and re-run the hydration for **every** site in the bulletin — including
the m0 site.

Concretely, for a Kyrgyz deployment where the main panel is `month_1` (lead 1, target =
issue month + 1) and the m0 card is `month_0` (lead 0, target = issue month): a station
added via the m0 card ends up in the Excel bulletin carrying the **main panel's** target
month's norm and day-count, not its own. E.g. a July (lead-0) forecast is written to the
bulletin dressed up with August's norm and August's day-in-month count.

## Root cause

A bulletin **site** object carries no target month/year of its own — only
`site.forecasts` (the row data) and whatever hydrograph-stat attributes were last set on
it. `_on_write` and `_load_bulletin_from_api` both assume **one target period for the
whole bulletin** and re-derive it from `dm.get_bulletin_metadata(horizon)`, which reads
`self.forecasts_all` — the **main** panel's frame — with no per-site override:

- `_on_write` (`bulletin_manager.py:663-728`, esp. `:685-688` and `:698-716`): calls
  `self.dm.get_bulletin_metadata(horizon)` (no override), then for **every** site in
  `filtered` (all sites in the bulletin for the selected basin, m0 or not) calls
  `rehydrate_sites_hydrograph_stats(filtered, horizon, forecast_horizon, db)` (`:699`)
  and `_populate_forecast_attributes(site, horizon, forecast_year, forecast_horizon)`
  (`:710`) — both driven by the single `forecast_horizon`/`forecast_year` just derived
  from the main panel.
- `_load_bulletin_from_api` (`bulletin_manager.py:202-247`, called from `__init__` at
  `:288` and `_on_horizon_change` at `:346`) does the same on every reload: the caller
  passes one `(forecast_year, forecast_horizon)` pair for the whole bulletin, and
  `_populate_forecast_attributes` (`:239`) is invoked per-site with that single pair.
- `_populate_forecast_attributes` (`bulletin_manager.py:131-199`) is where the wrong
  values actually land for the `month` branch (`:139-160`): `days_in_month =
  calendar.monthrange(forecast_year, forecast_horizon)[1]` (`:140`) and
  `hydrate_month_hydrograph_stats(site, forecast_horizon, db)` (`:141`) both use the
  bulletin-wide `forecast_horizon`, not the site's own target month.

So even if a site is added with correct m0-specific hydration at add-time, the very next
write or reload pass re-hydrates it with the main panel's month. The attempted fix on
this branch (`_month0_hydration_params`, calling `get_bulletin_metadata("month",
forecasts_all=self.dm.long_forecasts_m0)` — see the reverted diff in
`bulletin_manager.py`/`data_manager.py`) only touched `_on_add_m0`'s initial hydration
path; it did not touch `_on_write` or `_load_bulletin_from_api`, so it was overwritten on
the next write/reload and has since been reverted as ineffective.

## Any real fix must

Give each bulletin site its own target month/year — set once when the site is added
(m0 add-path uses the m0 frame's target month; main add-path uses the main panel's), and
**honoured** (not re-derived) by both `_on_write` and `_load_bulletin_from_api`. That
likely means:
- Persisting `(target_month, target_year)` (or the already-available `horizon_value`) as
  part of the saved bulletin record per site, not just per bulletin-horizon-context.
- `_populate_forecast_attributes` taking the site's own target month/year, not a
  bulletin-wide pair threaded in from the caller.
- `_on_write`'s `rehydrate_sites_hydrograph_stats` and the per-site
  `_populate_forecast_attributes` refresh loop reading each site's own stored period
  instead of the single `dm.get_bulletin_metadata(horizon)` result.

This is a real (if narrow) design change to the bulletin data model, not a one-line fix
— scope it as its own implementation, not a quick patch.

## Scope / impact

**Kyrgyz-only in practice.** Tajik has no `month_0` in `ieasyhydroforecast_ml_long_term_supported_modes`
(taj configs are `month_1`–`month_3`, all lead ≥ 0 but none named/routed as the `month_0`
UI card), so the m0 add/write path is not reachable on Tajik. Any fix must gate on
`SAPPHIRE_SKILL_LEAD_AWARE` and keep flag-OFF byte-identical to trunk.

## Open question — is the m0 card ever visible in normal Kyrgyz operation?

`plot_manager.update_forecast_tabulator_m0` (`apps/forecast_dashboard/dashboard/plot_manager.py:304-323`)
hides the m0 summary card (and, transitively, the "Add to bulletin" affordance that
feeds this defect) whenever the main panel's target month and the m0 frame's target
month disagree:

```python
_issue_month = forecasts_all['date'].max().month
summary_target_month = ((_issue_month - 1 + _primary_month_lead()) % 12) + 1  # flag ON
m0_target_month = m0['date'].max().month
if summary_target_month != m0_target_month:
    self.summary_table_m0_card.visible = False
    return
```

A reviewer raised: `summary_target_month` is computed as a **target** month (issue month
+ resolved lead), while `m0_target_month` is read directly off `m0['date'].max().month`
— which looks like an **issue** month, not a target month. If that reading is right, the
comparison is a category error and could mean the m0 card (and this bulletin path) is
never visible in normal operation, which would lower this issue's priority.

**I read the code and I believe the comparison is actually valid, for a specific
reason — but it rests on an implicit invariant worth flagging, not a hard guarantee:**

- `m0['date']` is the forecast's **issue** date (`apps/forecast_dashboard/src/db.py`'s
  `get_long_forecasts`: `df["Date"] = df["date"]`, `df["year"] = df["date"].dt.year`,
  passed straight through from the API's `date` field — the same field `forecasts_all`
  uses for `_issue_month`).
- The m0 frame is fetched at `horizon_value = m0_lead`, and `m0_lead` resolves to **0**
  in both code paths: `_safe_lead("month_0", 0)` when the flag is on (`db.py:1064`,
  falling back to `0` on any resolution error), or hardcoded `0` when the flag is off
  (same line). The card's info text also hardcodes `m0_lead = 0`
  (`plot_manager.py:340`, comment: "the m0 card is always the lead-0 product").
- Per the long-term writer's own target-period formula
  (`apps/long_term_forecasting/post_process_lt_forecast.py:132-174`,
  `adjust_forecast_dates_dynamic`): `target_start_month = (issue_month + lead_time - 1)
  % 12 + 1`. For `lead_time = 0` this collapses to `target_start_month == issue_month`.

So for a true lead-0 product, **issue month and target month are numerically identical
by construction** — `m0['date'].max().month` is simultaneously the m0 row's issue month
and its target month. The comparison is therefore target-vs-target, not target-vs-issue,
and is not a category error. Walking through a normal Kyrgyz monthly cycle: the m0 card
is hidden for most of month `M-1` (before the `month_0`-for-`M` forecast has been
issued, while the main panel's `month_1` still targets `M`), and becomes visible from
the day `month_0` is issued for `M` until the main panel's `month_1` rolls over to
target `M+1` — i.e. visible for part of every month, not "never." This reads as an
intentional same-target-period freshness gate, not a bug, and this defect's bulletin
write-path is reachable during that window.

**Residual caveat (not fully closed):** the lead-0 identity above depends on the
deployment's actual `month_0.json` config really carrying `operational_month_lead_time
== 0`. Nothing in the code enforces this as a hard invariant beyond convention +
`_safe_lead`'s fallback default; `_safe_lead("month_0", 0)` will happily return whatever
`operational_month_lead_time` the config says even if it isn't 0, while the visibility
gate's `summary_target_month` computation and the info-text's `m0_lead` both continue to
*assume* 0. I did not verify a live Kyrgyz `month_0.json` in this pass (no server/config
access from this read-only investigation) — an implementer should `grep
operational_month_lead_time` on the deployment's `month_0.json` before relying on this
conclusion, and if it is ever non-zero, both the visibility gate and this issue's
"target month" framing need re-deriving.

## Acceptance criteria (for whoever picks this up)

- Given a bulletin site added via the m0 card, its target month/year survive both a
  "Write bulletin" and a page/horizon reload unchanged — no norm or day-count from the
  main panel's target month leaks into the m0 site's Excel row.
- Adding the **same station** via both the main panel and the m0 card (two horizons
  active at once) does not clobber one site's target period with the other's.
- Tajik (`month_0 ∉ supported_modes`) unaffected — no m0 add/write path exists to break.
- Flag-OFF (`SAPPHIRE_SKILL_LEAD_AWARE=false`) byte-identical to trunk.
- Confirm (or correct) the "m0 card visible for part of every Kyrgyz month" conclusion
  above against a live deployment before treating it as settled; if `month_0.json`'s
  lead is ever non-zero, re-open the visibility analysis.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero unexpected
  skips. No real station codes/discharge (`19999` placeholder).

## Notes

Sibling to FD-016 (norm N/A labeling) in that both are month/season bulletin
presentation-layer defects, but unrelated in mechanism. Not to be confused with the
main-panel target-month fix (Defect A/J), which is already merged via trunk's M1-P3
(`src/db.py`, `long_term_horizon_resolver.py`) and this branch's `src/month_lead.py` +
header/caption work — those are done; this issue is the one piece (bulletin
per-site hydration) that did not land.
