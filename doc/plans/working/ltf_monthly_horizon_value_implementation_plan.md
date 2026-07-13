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
2. **Feature-flagged, default-ON, with a kill-switch.** All behaviour change sits behind
   `SAPPHIRE_LTF_DASH_LEAD_AWARE` (see "Flag specification" below). Setting it to a false value
   (`SAPPHIRE_LTF_DASH_LEAD_AWARE=false`) must be **byte-identical to `maxat`** (the legacy hard-coded
   lead 1) — that is the kill-switch. Absent/on = the corrected behaviour.
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
- **D2 — flag: DECIDED (2026-07-13).** `SAPPHIRE_LTF_DASH_LEAD_AWARE`, **default ON**, gating all five
  sites together, documented as a temporary kill-switch to be removed after a validation cycle. See
  "Flag specification" below. Rationale: kyg is a *beneficiary* (its `month_0` card + bulletins are
  corrected), not just protected, so default-on delivers the fix to both deployments on deploy; the
  switch exists because kyg operators will see the `month_0` skill numbers change, and a runtime
  revert (no redeploy) is cheap insurance for an operator-facing tool.
- **D3 — pre-flight: PASSED (2026-07-13).**
  - The dashboard container **mounts the config dir** (`sapphire/docker-compose.yml:243` →
    `${data_ref_dir}/config:${container_data_ref_dir}/config`) and loads env via
    `ieasyhydroforecast_env_file_path`. Both taj and kyg env files set the three vars the resolver
    reads: `ieasyforecast_configuration_path` (**note: no "hydro" — corrected from the earlier draft,
    which wrongly said `ieasyhydroforecast_configuration_path`**), `ieasyhydroforecast_ml_long_term_configuration`,
    `ieasyhydroforecast_ml_long_term_supported_modes`.
  - The dashboard **already** hard-depends on this resolver for quarter/season with **no fallback**
    (`db.py:725,778` call `quarter_horizon_value()` / `seasonal_horizon_value()` directly). Since
    quarter/season cards render in production, config availability is already proven. Month adds no new
    dependency.
  - **Empirical run** of the resolver's month path against the real configs returns the convention
    exactly: **tjhm** `month_1→0, month_2→1, month_3→2, quarter→0`, `month_0`→raises
    `UnsupportedLongTermModeError`; **kghm** `month_0→0, month_1→1, month_2→2, month_3→3, quarter→1`.
  - **Fallback decision:** because the resolver *raises* for an absent mode (tjhm `month_0`), the
    caller **must membership-check `mode ∈ supported_modes` before calling it** (already in P1 guards).
    A broad try/except that silently returns a default is **not** wanted — it would reintroduce a
    hidden hard-coded lead. Match the existing quarter/season pattern: resolve directly, let a genuine
    config error surface (the dashboard already behaves this way).

**P0 acceptance:** D1–D3 answered in writing in this file. **P0 COMPLETE (2026-07-13):** D1 approved,
D2 decided, D3 passed.

---

## Flag specification — `SAPPHIRE_LTF_DASH_LEAD_AWARE`

| | |
|---|---|
| **Name** | `SAPPHIRE_LTF_DASH_LEAD_AWARE` |
| **Type** | boolean env var (truthy: `true`/`1`/`yes`, case-insensitive; falsy: `false`/`0`/`no`) |
| **Default** | **ON** — *absent or unset ⇒ enabled* (the corrected, config-resolved behaviour) |
| **Kill-switch** | set `=false` ⇒ legacy behaviour, **byte-identical to `maxat`** (hard-coded monthly lead 1) |
| **Scope** | `apps/forecast_dashboard` only; gates all five fixed sites together (main-panel select + caption, m0 stat filter, m0 bulletin hydration, header, bulletin year) |
| **Read location** | one helper (e.g. `dashboard/config.py` or a small `is_dash_lead_aware()`), read once; do not scatter `os.getenv` across the five sites |
| **Lifecycle** | **temporary.** Remove the flag and the legacy branch in a follow-up once both deployments have run a validation cycle. Tracked as a P-CLEANUP follow-up. |

**Behaviour by deployment (flag ON):** kghm main panel unchanged (config resolves lead 1, same as
today); kghm `month_0` card + bulletins **corrected** (lead-0 skill/norms instead of lead-1); tjhm main
panel **corrected** to the lead-0 flagship. Flag OFF reproduces today's behaviour for both.

### Deployment action (when updating deployments) — do NOT action until the PR merges

Because the default is ON, **the fix applies without any `.env` change** — you do *not* need to add the
var for the corrected behaviour to take effect. The var only needs to be present to **disable** it.
Recommended, so operators know the switch exists:

- Add a **commented** line to the env templates and per-deployment env files, documenting the switch
  and its default:
  ```
  # Dashboard monthly-lead resolution (fixes tjhm target month + kghm month_0 skill/bulletins).
  # Default ON when unset. Set to false to revert to the legacy hard-coded lead-1 behaviour.
  # SAPPHIRE_LTF_DASH_LEAD_AWARE=false
  ```
- Files: `apps/config/.env`, `apps/config/.env_develop`, `apps/config/.env_develop_kghm` (templates),
  and the per-deployment `taj_data_forecast_tools/config/.env_develop_tjhm` /
  `kyg_data_forecast_tools/config/.env_develop_kghm` (+ their `*_server` variants).
- If any deployment wants to **stage** the visible kyg `month_0` change, set
  `SAPPHIRE_LTF_DASH_LEAD_AWARE=false` there first, then flip to on after operator sign-off.

This deployment step is an **acceptance item of the implementing PR** (see P1) — the PR must also update
`doc/configuration.md` (the canonical env-var reference) and the deployment checklist
(`doc/prod/update_deployment_checklist.md`). Documenting the var in `doc/configuration.md` is
deliberately deferred to that PR so the reference never describes a var that does nothing yet.

---

## P-TEST — Pre-code test harness (Depends on: P0)

**Goal:** lock current-correct behaviour; add red regression tests.
**Files:** new tests under `apps/forecast_dashboard/tests/`.
**Agents:** 1.
**Acceptance:**
- Golden (green now): kghm main tile = single lead-1 row; kghm m0 card **currently annotated from the
  lead-1 stats frame** (Defect F's bug, locked as the kill-switch/flag-off contract — *not* lead-0);
  kghm header month/year today. Under flag-off these must never change. Plus two **invariant guards**
  (green now *and* after the fix, flag-on): kghm main panel stays lead-1, and tjhm (no `month_0`) has no
  m0 card — these catch a hardcoded-hv0 fix that would pass the tjhm regression but break kghm.
- Regression (red now, target the phase): tjhm main resolves lead 0 (A); `format_horizon_info` uses the
  passed target month+year instead of recomputing (J); m0 merges only lead-0 stats and blanks when
  absent — both `_op_mask.any()` branches (F); m0 bulletin hydration from the m0 frame (G); bulletin
  `forecast_year` rolls to the following year for Dec-lead-1.
- No vacuous skips hide the new assertions.

**Strategy (forward-compatible flag pattern) — decided 2026-07-13.** The flag/`month_horizon_value`
do not exist yet, so tests are written to be stable across the fix:
- **Golden** tests `monkeypatch.setenv("SAPPHIRE_LTF_DASH_LEAD_AWARE", "false")` and assert **current**
  behaviour. The env var is ignored today (green now) and pins the kill-switch path after P1 (still
  green). This includes locking the *current bug* as flag-off behaviour (e.g. kghm m0 card currently
  merges lead-1 stats) — that is intentional; it proves the kill-switch reproduces `maxat`.
- **Regression** tests `monkeypatch.setenv(..., "true")`, assert the **desired** behaviour, and are
  marked `@pytest.mark.xfail(strict=True, reason="<phase>: …")`. They fail now (env ignored → buggy
  path); when the phase lands (flag-on → correct path) they pass, and `strict=True` turns the
  unexpected pass into a failure that forces removing the marker. This is the phase's pass criterion.
- **New-signature** cases (`month_horizon_value(...)`, and any changed `_format_forecast_info`
  signature) are authored in their implementing phase, not here — leave a `@pytest.mark.skip(
  reason="authored in P1: needs month_horizon_value")` stub listing the intended assertions
  (tjhm `month_1`→0, kghm `month_1`→1/`month_0`→0, tjhm `month_0`→raises) so intent is captured.
- Test entry points that need **no** new signature and carry the fix internally: `db.get_data("month",
  …)` (A main-panel selection + F stats filter/m0 merge), `format_horizon_info(...)` (J — the fix makes
  it *use* the passed `forecast_horizon`/`forecast_year` it currently ignores), `get_bulletin_metadata`
  + `_month_hydration_params` (G + year). Prefer these for the xfail-strict regressions.

---

## P1 — Add the month lead resolver + route the display sites (Defects A, J). Depends on: P0, P-TEST.

**Goal:** the dashboard resolves and labels the correct monthly lead per deployment.
**Files (allow-list):** `apps/iEasyHydroForecast/long_term_horizon_resolver.py` (add public
`month_horizon_value`); `apps/forecast_dashboard/src/db.py` (main-panel forecast selection + the
`month_0` gate — forecast frame only, **not** the stats filter, which is P2);
`apps/forecast_dashboard/dashboard/plot_manager.py`; `.../widgets.py`.
**Agents:** 2 — (a) resolver + `db.py` selection; (b) caption `plot_manager` + header `widgets`.
**Guards:** membership-check `month_0 ∈ supported_modes` before calling the resolver
(`_ensure_supported_mode` raises otherwise). Per D3, do **not** wrap in a silent try/except default —
resolve directly and let a genuine config error surface, matching the existing quarter/season pattern.
Introduce the flag helper here (single read point) so all five sites gate on it.
**Acceptance:** A/J reds green; kghm goldens green; **flag-off (`=false`) byte-identical to `maxat`**;
the implementing PR also (a) adds the commented `SAPPHIRE_LTF_DASH_LEAD_AWARE` line to the env
templates + per-deployment env files, (b) documents the var in `doc/configuration.md`, and (c) notes it
in `doc/prod/update_deployment_checklist.md` (see "Flag specification → Deployment action").

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

## P-CLEANUP — Remove the flag (follow-up, after validation). Depends on: P1–P3 deployed + validated.

**Goal:** delete `SAPPHIRE_LTF_DASH_LEAD_AWARE` and the legacy hard-coded-lead-1 branch once both
deployments have run a validation cycle with no operator concerns. Not part of the initial PR —
tracked so the temporary flag does not become permanent dual-path debt.
**Files:** the five sites + the flag helper + the flag's golden tests + the env-template comments +
`doc/configuration.md`.
**Acceptance:** flag gone; only the corrected behaviour remains; full suite green.

---

## Sequencing

```json
{
  "phases": {
    "P0":         { "depends_on": [], "parallel_agents": 0 },
    "P-TEST":     { "depends_on": ["P0"], "parallel_agents": 1 },
    "P1":         { "depends_on": ["P0", "P-TEST"], "parallel_agents": 2 },
    "P2":         { "depends_on": ["P0", "P-TEST"], "parallel_agents": 1 },
    "P3":         { "depends_on": ["P0", "P-TEST"], "parallel_agents": 1 },
    "P-CLEANUP":  { "depends_on": ["P1", "P2", "P3"], "parallel_agents": 1 }
  }
}
```

P1+P2+P3 all ship together as one flag-gated change (they are the same user-visible fix); they are
split only for delegation. All are read-path, reversible by revert / flag-off. P-CLEANUP is a separate
later PR, gated on server validation.

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
