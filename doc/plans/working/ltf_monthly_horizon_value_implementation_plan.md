# Implementation plan — dashboard monthly lead resolution

**Branch:** `develop_ltf_monthly_horizon_value` (off `origin/maxat_sapphire_2` @ `3be4dbf5`)
**Diagnosis:** `doc/plans/issues/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md` (revision 7)
**Status:** Planning. No implementation code until P0 is signed off.

**Scope (narrowed after re-baselining against `maxat_sapphire_2`):** the **dashboard read-side** only —
resolve the monthly forecast lead from config instead of hard-coding 1, across the five sites that
assume lead 1. The writer/data side is already fixed on `maxat` (the MIG-008 "P-PIPE" work landed) or
is owned by the MIG-008 data-governance track; neither is built here. See the issue draft's
"Re-baseline note" for what was dropped and why.

**Convention (fixed, not re-decided here):** `horizon_value = operational_month_lead_time`
(`doc/prod/longforecast_quarter_season_hv_convention.md`, owner, 2026-06-22).

---

## Ground rules (every phase)

1. **Orchestrator writes no implementation code.** Each phase is delegated to Sonnet agents with an
   explicit file allow-list and the standard constraint: *"Do NOT change existing function signatures,
   data-flow, or control-flow; changes are additive or limited to the specific behaviour described."*
2. **Feature-flagged, default-off.** All behaviour change sits behind a `SAPPHIRE_*` flag (name in P0).
   Flag-off must be **byte-identical** to `maxat`; kghm must not move until the flag flips.
3. **Tests before code (P-TEST).** Golden tests lock current kghm behaviour; red regression tests per
   defect land before the fix.
4. **`apps/forecast_dashboard` only.** No `sapphire/services/`, no `postprocessing_forecasts`, no
   `forecast_skill_eval` edits — this is a read-side dashboard fix.
5. **No real station codes / discharge values.** Fixtures `17999` (taj) / `15999` (kyg).

---

## P0 — Decision gate (NO CODE)

Small, because the convention is already settled. Decisions to record here:

- **D1 — card mapping.** Confirm: main monthly panel = the deployment's **primary** product
  (kghm `hv1`, tjhm `hv0`); the `month_0` card shows lead 0 **only when a distinct lower lead exists**
  (kghm yes; tjhm no). This is the display decision the convention does not cover.
- **D2 — flag name.** Confirm `SAPPHIRE_LTF_DASH_LEAD_AWARE` (or chosen); gates all five sites together,
  default off.
- **D3 — pre-flight (must pass before P1):** confirm the dashboard container has
  `ieasyhydroforecast_configuration_path` + `ieasyhydroforecast_ml_long_term_configuration` at runtime,
  and that `long_term_horizon_resolver` import already used for quarter/season works for month. If the
  config dir is not mounted, the resolver must degrade to current behaviour, not crash — decide the
  fallback here.

**P0 acceptance:** D1–D3 answered in writing in this file.

---

## P-TEST — Pre-code test harness (Depends on: P0)

**Goal:** lock current-correct behaviour; add red regression tests.
**Files:** new tests under `apps/forecast_dashboard/tests/`.
**Agents:** 1.
**Acceptance:**
- Golden (green now): kghm main tile = single lead-1 row; kghm m0 card uses lead-0 skill; kghm header
  month/year today. Under flag-off these must never change.
- Regression (red now, target the phase): tjhm main resolves lead 0 (A); `_format_forecast_info` and
  `format_horizon_info` month+year at lead 0 and Dec-lead-1 (J); m0 merges only lead-0 stats and blanks
  when absent — both `_op_mask.any()` branches (F); m0 bulletin hydration from the m0 frame (G);
  bulletin `forecast_year` rolls to the following year for Dec-lead-1.
- No vacuous skips hide the new assertions.

---

## P1 — Add the month lead resolver + route the display sites (Defects A, J). Depends on: P0, P-TEST.

**Goal:** the dashboard resolves and labels the correct monthly lead per deployment.
**Files (allow-list):** `apps/iEasyHydroForecast/long_term_horizon_resolver.py` (add public
`month_horizon_value`); `apps/forecast_dashboard/src/db.py` (main-panel forecast selection + the
`month_0` gate — forecast frame only, **not** the stats filter, which is P2);
`apps/forecast_dashboard/dashboard/plot_manager.py`; `.../widgets.py`.
**Agents:** 2 — (a) resolver + `db.py` selection; (b) caption `plot_manager` + header `widgets`.
**Guards:** membership-check `month_0 ∈ supported_modes` before calling the resolver
(`_ensure_supported_mode` raises otherwise); resolver failure degrades to current behaviour (D3).
**Acceptance:** A/J reds green; kghm goldens green; flag-off byte-identical.

## P2 — Per-lead skill-stat filtering (Defect F). Depends on: P0, P-TEST. Ships with P1.

**Goal:** each card merges only its displayed lead's skill stats; blank when that lead has none.
**Files:** `apps/forecast_dashboard/src/db.py` (`forecast_stats` handling, `:931-935` and the m0 merge).
**Agents:** 1. Handle both `_op_mask.any()` branches; never merge the unfiltered frame.
**Acceptance:** F reds (both branches) green; no card that shows data today goes blank under flag-off.

## P3 — Bulletin target month/year (Defects G + year-rollover). Depends on: P0, P-TEST.

**Goal:** the m0 bulletin hydrates from its own frame; `forecast_year` reflects the target year.
**Files:** `apps/forecast_dashboard/dashboard/bulletin_manager.py` (`_month_hydration_params`,
`_on_add_m0`), `apps/forecast_dashboard/dashboard/data_manager.py` (`get_bulletin_metadata`).
**Agents:** 1.
**Acceptance:** G red green; a Dec-lead-1 bulletin carries January of the following year in the saved
key and the hydration params.

---

## Sequencing

```json
{
  "phases": {
    "P0":     { "depends_on": [], "parallel_agents": 0 },
    "P-TEST": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P1":     { "depends_on": ["P0", "P-TEST"], "parallel_agents": 2 },
    "P2":     { "depends_on": ["P0", "P-TEST"], "parallel_agents": 1 },
    "P3":     { "depends_on": ["P0", "P-TEST"], "parallel_agents": 1 }
  }
}
```

P1+P2+P3 all ship together as one flag-gated change (they are the same user-visible fix); they are
split only for delegation. All are read-path, reversible by revert / flag-off.

---

## Definition of done

- All P-TEST reds green; all kghm goldens green.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` zero failures / zero unexpected skips.
- Flag-off byte-identical to `origin/maxat_sapphire_2`; kghm dashboard unchanged.
- Manual check (or `apps/run_locally.sh` dashboard): with a tjhm-shaped config + flag on, the main
  panel shows the lead-0 product captioned with the issue month; with kghm, unchanged.
- No `sapphire/services/`, `postprocessing_forecasts`, or `forecast_skill_eval` files touched.
- No real station codes / discharge values in any committed file.

---

## Out of scope (handed off / already done)

- Writer correctness (`api_writer.py`): already fixed on `maxat` (P-PIPE landed).
- Historical `long_forecasts` remediation, the Tajik 32-month gap, the 2026-07 run's missing
  aggregates/`MC_ALD` leads: MIG-008 data-governance track. My prod findings are summarized into
  `doc/prod/longforecast_historical_data_decision_request.md` (addendum 2026-07-13).
- Quarter/season semantics: settled by the owner's convention; quarter/season already resolve their
  lead from config in the dashboard.
