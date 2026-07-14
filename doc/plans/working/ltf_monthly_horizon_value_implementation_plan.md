# Implementation plan — dashboard monthly lead resolution

**Branch:** `develop_ltf_monthly_horizon_value` (off `origin/maxat_sapphire_2` @ `3be4dbf5`)
**Diagnosis:** `doc/plans/issues/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md` (revision 7)
**Status:** **STALE — superseded by the actual merge outcome below.** This file is kept as a
provenance record of the original plan; it does **not** describe current behaviour. See
"What actually shipped" for the ground truth.

**Scope (narrowed after re-baselining against `maxat_sapphire_2`):** the **dashboard read-side** only —
resolve the monthly forecast lead from config instead of hard-coding 1, across the five sites that
assume lead 1. The writer/data side is already fixed on `maxat` (the MIG-008 "P-PIPE" work landed) or
is owned by the MIG-008 data-governance track; neither is built here. See the issue draft's
"Re-baseline note" for what was dropped and why.

**Convention (fixed, not re-decided here):** `horizon_value = operational_month_lead_time`
(`doc/prod/longforecast_quarter_season_hv_convention.md`, owner, 2026-06-22).

---

## What actually shipped (read this first — 2026-07-14)

This branch was written and merged in two independent efforts that landed on the **same fix
via different flags**, and were then reconciled onto one:

1. **This branch** originally implemented the fix behind its **own** new flag
   `SAPPHIRE_LTF_DASH_LEAD_AWARE` (default **ON**), with its own resolver accessor
   `month_horizon_value(mode)` in `long_term_horizon_resolver.py` and its own
   `is_dash_lead_aware()` helper. This is what the rest of this file (below this section)
   describes, and it is **no longer true**.
2. Meanwhile, trunk (`origin/maxat_sapphire_2`, PR #414, "M1 P3") independently shipped the
   **same `src/db.py` main-panel-selection fix** behind the **pre-existing** flag
   `SAPPHIRE_SKILL_LEAD_AWARE` (**default OFF**), using the resolver accessor
   `operational_lead_for_mode(mode)` (`apps/iEasyHydroForecast/long_term_horizon_resolver.py`).
3. This branch was **merged with trunk and converged onto the single flag**
   `SAPPHIRE_SKILL_LEAD_AWARE`. The branch's own flag, `is_dash_lead_aware()`, and
   `month_horizon_value()` were **deleted**. `apps/forecast_dashboard/src/db.py` and
   `apps/iEasyHydroForecast/long_term_horizon_resolver.py` are now **byte-identical to
   trunk**.

**What this branch still uniquely contributes on top of trunk's `db.py` fix:**
- `apps/forecast_dashboard/src/month_lead.py` — a new, importable lead accessor
  (`primary_month_lead()`, `month_lead_for_mode()`) for the **UI layer** (captions, headers,
  bulletin hydration), mirroring the nested `_safe_lead` closure inside `db.py`'s
  `_get_data_monthly` that isn't importable from outside that function.
- The monthly header/caption target month + year fix (Defect J) —
  `apps/forecast_dashboard/dashboard/plot_manager.py`, `.../widgets.py`,
  `.../widget_manager.py`.
- The Dec→Jan bulletin year rollover fix (`apps/forecast_dashboard/dashboard/data_manager.py`
  `get_bulletin_metadata`).
- A stale-cross-horizon-metadata crash guard.
- A golden/regression test harness (kghm/tjhm invariant guards + flag-gated test coverage).

**What did NOT ship from this branch** (attempted, then reverted as ineffective):
- The m0 bulletin per-site target-month hydration fix (`_month0_hydration_params`, calling
  `get_bulletin_metadata("month", forecasts_all=self.dm.long_forecasts_m0)`). It only touched
  `_on_add_m0`'s initial hydration and was silently overwritten by `_on_write` /
  `_load_bulletin_from_api`, which re-derive one bulletin-wide target period from the main
  panel for every site. **Split out as its own issue:**
  `doc/plans/issues/mid_prio_gi_draft_fd_m0_bulletin_per_site_target_month.md` (FD-018).
  `_on_add_m0` now calls the same `_month_hydration_params()` as the main add-path (no
  m0-specific branch); `get_bulletin_metadata` no longer takes a `forecasts_all` override.

**The `P-CLEANUP` phase below (remove the flag) is now moot as originally scoped** — the
branch's own flag no longer exists to remove. What remains is a **shared-flag** cleanup: once
both deployments have validated `SAPPHIRE_SKILL_LEAD_AWARE=true` (write-side AND
display-side), removing the legacy flag-OFF branches is a decision for whoever owns
`SAPPHIRE_SKILL_LEAD_AWARE` end-to-end (it also gates `postprocessing_forecasts` skill/ensemble
behaviour — see `doc/prod/long_term_deploy_runbook.md`), **not something this branch can do
unilaterally**. Do not schedule flag removal from this branch's follow-up backlog without
coordinating with whoever owns the flag's other call sites.

The env-template breadcrumb step described below (adding commented
`SAPPHIRE_LTF_DASH_LEAD_AWARE=false` lines to `.env` templates) is **dropped — not
applicable**. `SAPPHIRE_SKILL_LEAD_AWARE` is documented once, centrally, in
`doc/prod/long_term_deploy_runbook.md` ("Lead-aware skill & ensembles" section); it does not
need a second, dashboard-specific breadcrumb, and no second flag exists to breadcrumb.

---

## Ground rules (every phase) — historical, as originally written

1. **Orchestrator writes no implementation code.** Each phase is delegated to Sonnet agents with an
   explicit file allow-list and the standard constraint: *"Do NOT change existing function signatures,
   data-flow, or control-flow; changes are additive or limited to the specific behaviour described."*
2. **Feature-flagged, default-ON, with a kill-switch.** ~~All behaviour change sits behind
   `SAPPHIRE_LTF_DASH_LEAD_AWARE`~~ **Superseded: converged onto `SAPPHIRE_SKILL_LEAD_AWARE`,
   default OFF (not ON) — see "What actually shipped" above.**
3. **Tests before code (P-TEST).** Golden tests lock current kghm behaviour; red regression tests per
   defect land before the fix.
4. **`apps/forecast_dashboard` only.** No `sapphire/services/`, no `postprocessing_forecasts`, no
   `forecast_skill_eval` edits — this is a read-side dashboard fix.
5. **No real station codes / discharge values.** Fixtures `17999` (taj) / `15999` (kyg).

---

## P0 — Decision gate (NO CODE) — historical

Small, because the convention is already settled. Decisions to record here:

- **D1 — card mapping.** Confirm: main monthly panel = the deployment's **primary** product
  (kghm `hv1`, tjhm `hv0`); the `month_0` card shows lead 0 **only when a distinct lower lead exists**
  (kghm yes; tjhm no). This is the display decision the convention does not cover.
- **D2 — flag: DECIDED (2026-07-13), then superseded (2026-07-14).** Originally
  `SAPPHIRE_LTF_DASH_LEAD_AWARE`, default ON, gating all five sites together. **Converged onto
  `SAPPHIRE_SKILL_LEAD_AWARE`, default OFF**, during the merge with trunk's independently-shipped
  `db.py` fix (PR #414, M1 P3) — see "What actually shipped" above. Rationale for the convergence:
  one flag governing one behaviour (dashboard read-side lead resolution) end-to-end, rather than two
  flags that could disagree with each other across write-side (`postprocessing_forecasts`) and
  read-side (`forecast_dashboard`).
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
    config error surface (the dashboard already behaves this way). **Note:** the shipped
    `month_lead.py` / `db.py`'s `_safe_lead` closure DO catch and fall back with a warning (see "What
    actually shipped") — this is a deliberate deviation from D3's original "no silent fallback"
    instruction, made necessary by trunk's independent `db.py` implementation; flag as a fact, not a
    re-litigation.

**P0 acceptance:** D1–D3 answered in writing in this file. **P0 COMPLETE (2026-07-13);
superseded by the merge on 2026-07-14 — see "What actually shipped."**

---

## Flag specification — HISTORICAL, describes the deleted `SAPPHIRE_LTF_DASH_LEAD_AWARE`

**This entire section describes a flag that no longer exists in the codebase.** It is kept for
provenance only. For the flag that actually ships, see
`doc/prod/long_term_deploy_runbook.md` § "Lead-aware skill & ensembles (`SAPPHIRE_SKILL_LEAD_AWARE`)".

| | |
|---|---|
| **Name** | ~~`SAPPHIRE_LTF_DASH_LEAD_AWARE`~~ *(deleted; converged onto `SAPPHIRE_SKILL_LEAD_AWARE`)* |
| **Type** | boolean env var (truthy: `true`/`1`/`yes`, case-insensitive; falsy: `false`/`0`/`no`) |
| **Default** | ~~ON~~ *(the surviving flag, `SAPPHIRE_SKILL_LEAD_AWARE`, defaults **OFF**)* |
| **Kill-switch** | set `=false` ⇒ legacy behaviour, byte-identical to `maxat` (hard-coded monthly lead 1) — **this part still holds true for `SAPPHIRE_SKILL_LEAD_AWARE=false`** |
| **Scope** | `apps/forecast_dashboard` only; gated all five originally-fixed sites together (main-panel select + caption, m0 stat filter, m0 bulletin hydration, header, bulletin year) — **m0 bulletin hydration did not ship (FD-018); the other four did, under `SAPPHIRE_SKILL_LEAD_AWARE`** |
| **Read location** | ~~`is_dash_lead_aware()`~~ *(deleted)*; the surviving read point is `skill_lead_aware_flag.skill_lead_aware_enabled()` (`apps/iEasyHydroForecast/skill_lead_aware_flag.py`) |
| **Lifecycle** | ~~temporary, this branch's own cleanup item~~ *(N/A — `SAPPHIRE_SKILL_LEAD_AWARE` is a shared flag with other owners; its removal is not this branch's call — see "What actually shipped")* |

### Deployment action — HISTORICAL, do not follow

~~Add a commented `SAPPHIRE_LTF_DASH_LEAD_AWARE=false` line to env templates~~ — **this step is
dropped.** There is no second flag to breadcrumb. `SAPPHIRE_SKILL_LEAD_AWARE`'s enable procedure
(one `.env` line + recalc) is documented once, centrally, in
`doc/prod/long_term_deploy_runbook.md`.

---

## P-TEST — Pre-code test harness (Depends on: P0) — historical plan; see status below

**Status:** implemented, converged onto the shared flag's test suite. The golden/invariant-guard
tests (kghm main panel stays lead-1 under flag-off; tjhm has no m0 card) and the flag-gated
regression tests both exist, keyed on `SAPPHIRE_SKILL_LEAD_AWARE` rather than the originally-planned
`SAPPHIRE_LTF_DASH_LEAD_AWARE`. The `xfail(strict=True)` forward-compatible pattern described below
was the authoring strategy used before the flag existed; by the time of the merge the flag existed
and most of these were converted to plain green assertions under both flag states.

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
  absent — both `_op_mask.any()` branches (F); ~~m0 bulletin hydration from the m0 frame (G)~~ **did not
  ship — see FD-018**; bulletin `forecast_year` rolls to the following year for Dec-lead-1.
- No vacuous skips hide the new assertions.

**Strategy (forward-compatible flag pattern) — decided 2026-07-13, historical wording kept
verbatim below (substitute the flag name mentally):**
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
  **Note: `month_horizon_value` itself was deleted in the merge; the surviving accessor is
  `operational_lead_for_mode` (trunk) / `primary_month_lead` (`src/month_lead.py`, this branch).**
- Test entry points that need **no** new signature and carry the fix internally: `db.get_data("month",
  …)` (A main-panel selection + F stats filter/m0 merge), `format_horizon_info(...)` (J — the fix makes
  it *use* the passed `forecast_horizon`/`forecast_year` it currently ignores), `get_bulletin_metadata`
  + `_month_hydration_params` (G + year). Prefer these for the xfail-strict regressions.

---

## P1 — Add the month lead resolver + route the display sites (Defects A, J) — IMPLEMENTED

**Status:** implemented, but via a **different resolver accessor than planned.** Trunk's
`operational_lead_for_mode` (`apps/iEasyHydroForecast/long_term_horizon_resolver.py`) shipped
instead of this branch's originally-planned `month_horizon_value`; this branch's
`apps/forecast_dashboard/src/month_lead.py` wraps it for UI callers. `plot_manager.py` and
`widgets.py`/`widget_manager.py` were fixed for the caption/header (Defect J) as planned.

**Goal:** the dashboard resolves and labels the correct monthly lead per deployment.
**Files (allow-list):** ~~`apps/iEasyHydroForecast/long_term_horizon_resolver.py` (add public
`month_horizon_value`)~~ *(superseded — trunk's `operational_lead_for_mode` used instead)*;
`apps/forecast_dashboard/src/db.py` (main-panel forecast selection + the
`month_0` gate); `apps/forecast_dashboard/dashboard/plot_manager.py`; `.../widgets.py`;
`.../widget_manager.py`; `apps/forecast_dashboard/src/month_lead.py` (new).
**Agents:** 2 — (a) resolver + `db.py` selection; (b) caption `plot_manager` + header `widgets`.
**Guards:** membership-check `month_0 ∈ supported_modes` before calling the resolver
(`_ensure_supported_mode` raises otherwise). ~~Per D3, do **not** wrap in a silent try/except
default~~ — **superseded: the shipped `_safe_lead`/`month_lead_for_mode` DO catch and fall back
with a warning; see the D3 note above.**
**Acceptance:** A/J reds green; kghm goldens green; **flag-off (`SAPPHIRE_SKILL_LEAD_AWARE=false`)
byte-identical to `maxat`**. ~~The implementing PR also (a) adds the commented
`SAPPHIRE_LTF_DASH_LEAD_AWARE` line to the env templates + per-deployment env files~~ *(dropped —
no second flag)*; (b) documents the var — done centrally in
`doc/prod/long_term_deploy_runbook.md`, not `doc/configuration.md` (the var predates this branch
and is not newly introduced); (c) noted in `doc/prod/long_term_deploy_runbook.md`'s deploy table
row (e) and "What it does when ON" section (done as part of this doc pass).

## P2 — Per-lead skill-stat filtering (Defect F) — IMPLEMENTED. Depends on: P0, P-TEST. Ships with P1.

**Goal:** each card merges only its displayed lead's skill stats; blank when that lead has none.
**Files:** `apps/forecast_dashboard/src/db.py` (`forecast_stats` handling and the m0 merge).
**Agents:** 1. Handle both `_op_mask.any()` branches; never merge the unfiltered frame.
**Acceptance:** F reds (both branches) green; no card that shows data today goes blank under flag-off.

## P3 — Bulletin target month/year (Defects G + year-rollover) — PARTIALLY IMPLEMENTED. Depends on: P0, P-TEST.

**Status:** the **year-rollover** half shipped (`get_bulletin_metadata` in
`apps/forecast_dashboard/dashboard/data_manager.py` rolls a Dec-issued lead≥1 bulletin to the
following year). The **m0 per-site hydration** half (Defect G — "the m0 bulletin hydrates from its
own frame") was attempted (`_month0_hydration_params`) and then **reverted as ineffective**: it only
patched the add-time hydration in `_on_add_m0`; `_on_write` and `_load_bulletin_from_api` still
re-derive one bulletin-wide target period from the main panel for every site, overwriting it. This
needs a real per-site data-model change (each bulletin site remembering its own target month/year),
not a one-line rehydration call. **Split out and re-scoped as its own issue:**
`doc/plans/issues/mid_prio_gi_draft_fd_m0_bulletin_per_site_target_month.md` (FD-018). Not part of
this branch's remaining scope.

**Goal (original):** the m0 bulletin hydrates from its own frame; `forecast_year` reflects the target
year.
**Files:** `apps/forecast_dashboard/dashboard/bulletin_manager.py` (`_month_hydration_params`,
`_on_add_m0`), `apps/forecast_dashboard/dashboard/data_manager.py` (`get_bulletin_metadata`).
**Agents:** 1.
**Acceptance:** ~~G red green~~ **G NOT shipped — see FD-018**; a Dec-lead-1 bulletin carries January
of the following year in the saved key and the hydration params — **this half shipped.**

## P-CLEANUP — Remove the flag (follow-up, after validation) — MOOT AS ORIGINALLY SCOPED

**This branch's own flag (`SAPPHIRE_LTF_DASH_LEAD_AWARE`) no longer exists** — it was deleted in the
merge, so there is nothing of this branch's own to clean up. What remains is the **shared**
`SAPPHIRE_SKILL_LEAD_AWARE` flag's eventual removal, which also gates
`apps/postprocessing_forecasts/` write-side behaviour (see
`doc/prod/long_term_deploy_runbook.md`). Its removal is **not this branch's decision to make
unilaterally** — coordinate with whoever owns the flag's other call sites before scheduling that
work. Do not re-open this phase from this branch without that coordination.

---

## Sequencing — historical (see per-phase status notes above for what actually shipped)

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

P1+P2+P3 all shipped together as one flag-gated change (P3 partial — see status above). P-CLEANUP is
moot as this branch's item (see above); a shared-flag cleanup remains someone else's call.

---

## Definition of done

- All P-TEST reds green; all kghm goldens green.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` zero failures / zero unexpected skips.
- Flag-off (`SAPPHIRE_SKILL_LEAD_AWARE=false`) byte-identical to `origin/maxat_sapphire_2`; kghm
  dashboard unchanged.
- Manual check (or `apps/run_locally.sh` dashboard): with a tjhm-shaped config + flag on, the main
  panel shows the lead-0 product captioned with the issue month; with kghm, unchanged.
- No `sapphire/services/`, `postprocessing_forecasts`, or `forecast_skill_eval` files touched.
- No real station codes / discharge values in any committed file.
- **Known gap, not blocking this branch's done-ness:** m0 bulletin per-site hydration (FD-018) is
  filed as a separate follow-up, not required for this branch's merge.

---

## Out of scope (handed off / already done)

- Writer correctness (`api_writer.py`): already fixed on `maxat` (P-PIPE landed).
- Historical `long_forecasts` remediation, the Tajik 32-month gap, the 2026-07 run's missing
  aggregates/`MC_ALD` leads: MIG-008 data-governance track. My prod findings are summarized into
  `doc/prod/longforecast_historical_data_decision_request.md` (addendum 2026-07-13).
- Quarter/season semantics: settled by the owner's convention; quarter/season already resolve their
  lead from config in the dashboard.
- m0 bulletin per-site target-month hydration: split out as FD-018 (see P3 status above).
