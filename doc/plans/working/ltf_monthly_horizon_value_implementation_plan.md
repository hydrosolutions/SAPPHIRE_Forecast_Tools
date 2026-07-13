# Implementation plan — long-term `horizon_value` semantics

**Branch:** `develop_ltf_monthly_horizon_value` (off `origin/maxat_sapphire_2` @ `3be4dbf5`)
**Diagnosis:** `doc/plans/issues/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md`
(revision 6, settled over five review rounds — do not re-litigate the defects here).
**Status:** Planning. **No implementation code until P0 decisions are ratified by the user and the
services colleague.**

This is the actionable, orchestration-ready companion to the issue draft. The issue draft says *what
is wrong and why*; this plan says *what we build, in what order, who does each piece, and how we prove
it is safe*. It follows the CLAUDE.md orchestration protocol: the orchestrator explores and constrains,
Sonnet agents implement, every phase is verified with `SAPPHIRE_TEST_ENV=True bash run_tests.sh`.

---

## Ground rules (apply to every phase)

1. **Orchestrator writes no implementation code.** Each phase is delegated to Sonnet general-purpose
   agents with an explicit file allow-list and the standard constraint: *"Do NOT change existing
   function signatures, data-flow, or control-flow. Changes are additive or limited to the specific
   behaviour described."* Risky phases use `isolation: "worktree"`.
2. **Readers-first, writers-second, data-last.** Never emit a new `horizon_value` meaning before every
   reader tolerates it. Never mutate published rows before the code that reads them is correct.
3. **Feature-flagged, default-off.** All behaviour change sits behind `SAPPHIRE_LTF_LEAD_AWARE`
   (name TBD in P0). Flag-off must be **byte-identical** to today; kghm must not move until the flag
   flips. A characterization test suite (P-TEST) enforces this.
4. **`sapphire/services/` is colleague-owned.** No edits without an agreed contract. The service
   `data_migrator.py` is a *second* long-forecast writer — treat it as part of the contract surface.
5. **Tests before code.** P-TEST lands the golden + regression suite before any behaviour change, so
   every later phase has a red test to turn green and a green test it must not break.
6. **No real station codes / discharge values** anywhere — fixtures use `17999` (taj) / `15999` (kyg).

---

## P0 — Decision gate (NO CODE). Ratify the contract before anything else.

P0 produces written decisions in this file. Several are the user's and the colleague's to make; the
orchestrator cannot assume them. **Nothing downstream may start until P0 is signed off.**

### D1 — Target `horizon_value` contract (proposed; needs ratification)

| field (`long_forecasts`) | month | quarter | season | proposed rule |
|---|---|---|---|---|
| `horizon_value` | lead-in-months | lead-in-months | lead-in-months | `(yr(vf)-yr(date))*12 + mth(vf)-mth(date)` |
| `date` | issue date | issue date | issue date | quarter writer currently sets `date=valid_from` — must change |
| `valid_from`/`valid_to` | period bounds | period bounds | period bounds | unchanged |
| period-in-year | from `valid_from` | from `valid_from` | from `valid_from` | never from `horizon_value` |

`skill_metrics`: `horizon_value` = lead, `horizon_in_year` = period-in-year (orthogonal, both stored).

### D2 — Season convention (**colleague decision required**)
Season has one product/year (Apr–Sep), so `season_in_year` is degenerate and has been repurposed to
carry the issue lead (`skill_metrics.py:2110`, `db.py:735`, `data_reader.py:3143`). **Decide:** keep
that convention (season lead lives in `season_in_year`, `horizon_value` mirrors it) **or** move to
`horizon_value`=lead + `season_in_year`=1. This changes the shared read/write contract → colleague sign-off.

### D3 — Flag name and scope
Confirm `SAPPHIRE_LTF_LEAD_AWARE` (or chosen name); confirm it gates dashboard + postprocessing +
skill_eval together, default off.

### D4 — Retag vs re-run for historical data (**informed by the collision blocker**)
Verified: a quarter in-place retag collides on 15,935 / 97,462 rows because `date=valid_from`
destroyed the issue date — the true lead is unrecoverable from data. **Decide:** quarter (and any
affected month/season) history is remediated by **re-running** aggregation from source, not `UPDATE`.
Confirm scope and whether historical remediation is in this effort or deferred.

### D5 — Named owner for the service-side changes (contract, `data_migrator`, any schema/CHECK).

### D6 — Pre-flight facts to gather before coding
- Run the **retag collision query on prod** (month-numbered rows) — the local result is inconclusive.
- Confirm the dashboard container has `ieasyhydroforecast_configuration_path` +
  `ieasyhydroforecast_ml_long_term_configuration` at runtime (P1a resolver depends on it).
- Confirm which `SAPPHIRE_*` flags the deployed configs already set.

**P0 acceptance:** D1–D6 answered in writing here; colleague sign-off on D2/D5 recorded.

---

## P-TEST — Pre-code test harness (Depends on: P0). Ships before any behaviour change.

**Goal:** lock current-correct behaviour and add red regression tests, so every later phase is
test-guided.
**Files:** new `tests/` files under `apps/forecast_dashboard/tests/`,
`apps/postprocessing_forecasts/tests/`, `apps/forecast_skill_eval/tests/`. Plus **rewrite** the
existing tests that encode the wrong behaviour (they will otherwise fail the fix):
`test_pp038_writer_reader.py:163-194,431-454,42`, `test_quarterly_data_reader.py:963-980`,
`test_seasonal_integration.py:304-398`, `test_quarterly_api_writer.py:161-194`.
**Agents:** 3 parallel (one per module).
**Acceptance:**
- Golden tests capture today's kghm monthly display (lead-1 main tile single row; m0 uses lead-0
  skill), the healthy month skill merge, and quarter period derived from `valid_from`. Green now.
- Regression tests (red now): tjhm resolves lead 0; Defect F both `_op_mask.any()` branches; Defect G
  m0 hydration source; Defect J month+year at lead 0 and Dec lead-1 → Jan next year; Defect H mixed-lead
  quarter does not pool; Defect I month lead mismatch routed to ledger; quarter normalizer preserves
  issue date and derives lead; season leads {3,2,1,0} → `season_in_year=1`; ensemble skill merge uses
  `horizon_value`.
- No vacuous skips (the `client-import-absent` skip must not hide new assertions).
- Full suite green (new reds are `xfail`-marked against their target phase, or held on a sub-branch).

---

## P1a — Dashboard display lead resolution (Defects A, G, J). Depends on: P0, P-TEST.

**Goal:** the dashboard shows and labels the correct lead per org, flag-gated.
**Files (allow-list):** `apps/iEasyHydroForecast/long_term_horizon_resolver.py` (add public
`month_horizon_value`), `apps/forecast_dashboard/src/db.py` (forecast-frame selection only),
`apps/forecast_dashboard/dashboard/plot_manager.py`, `.../widgets.py`, `.../data_manager.py`,
`.../bulletin_manager.py`. **No skill-stats changes here** (that is P1b).
**Agents:** 2 — (a) resolver + `db.py` selection; (b) the three display sites (caption `plot_manager`,
header `widgets`, bulletin year `data_manager`/`bulletin_manager`).
**Guards:** membership-check `month_0 ∈ supported_modes` **before** calling the resolver
(`_ensure_supported_mode` raises otherwise); resolver failure must degrade to current behaviour, not
crash the dashboard (D6 pre-flight).
**Acceptance:** the P-TEST Defect-A/G/J reds go green; kghm goldens stay green; flag-off is
byte-identical.

## P1b — Per-lead skill-stat filtering (Defect F, structural). Depends on: P0, P-TEST. Ships with P1a.

**Goal:** each card merges only its displayed lead's skill stats; blank when that lead has none.
**Files:** `apps/forecast_dashboard/src/db.py` (`forecast_stats` handling).
**Agents:** 1. Must handle both `_op_mask.any()` branches. **Does not** make metric *values* correct
(that needs P6) — release note states the limitation.
**Acceptance:** Defect-F reds (both branches) green; no card that shows data today goes blank under
flag-off.

## P2 — Writer correctness (Defect C, all horizons). Depends on: P0, P-TEST.

**Goal:** writers never synthesize `horizon_value` from a period number.
**Files:** `apps/postprocessing_forecasts/src/api_writer.py` (monthly `:877-881`, aggregated
`:1066-1090`), and the coordinated change to `sapphire/services/.../data_migrator.py` **via the
colleague** (D5). Audit `skill_metrics.py` / `ensemble_calculator.py` writers.
**Agents:** 1 (apps side) + colleague hand-off for the service writer.
**Safety:** "raise on absent `horizon_value`" is **flag-gated** — unflagged it crashes the green
nightly quarter/season recalc (`skill_metrics.py:2161`, `file_writer.py:621-659`). Under flag-off,
preserve today's fill behaviour.
**Acceptance:** writer round-trips a present `horizon_value`; raises only under the flag when absent;
quarter/season writers covered; full local pipeline run (`bash apps/run_locally.sh all`) completes.

## P3 — Aggregation lead-partitioning + quarter/season read path (Defects H, C-season).
Depends on: P0, **P2** (needs the issue date present at write time).

**Goal:** quarterly/seasonal aggregates never average across leads; period read from `valid_from`.
**Files:** `apps/postprocessing_forecasts/src/aggregation.py`, `src/data_reader.py`,
`apps/forecast_dashboard/src/db.py` (`get_long_forecasts_quarter/_season`,
`_resolve_seasonal_horizon_value`).
**Agents:** 1. **Announced recompute** — partitioning changes already-published quarterly numbers;
flag-gate and treat as a deliberate republish, not a silent side effect.
**Acceptance:** mixed-lead quarter input partitions or refuses; season card period from `valid_from`;
quarter card selects by issue date, not `date=valid_from`.

## P4 — `forecast_skill_eval` month-lead handling (Defect I). Depends on: P0, P-TEST.

**Goal:** month rows whose stored `horizon_value` disagrees with the derived lead are handled
explicitly, not silently trusted.
**Files:** `apps/forecast_skill_eval/src/forecast_skill_eval/pairs.py`.
**Constraint:** skill_eval is the **parity oracle** (derives lead from issue date, `pairs.py:513-560`).
Prefer fixing upstream writers (P2) over changing oracle defaults; if the oracle changes, version/flag
the output. If month joins the derive-lead set, extend `include_lead_in_key` to month (`pairs.py:312`)
or genuine per-lead month rows collapse.
**Acceptance:** Defect-I red green; existing published skill artifacts unchanged under flag-off.

## P5 — Investigate missing aggregates + `MC_ALD` leads in the 2026-07 prod run (Defect E).
Depends on: P0. Diagnostic only; scope follow-up after root cause. Not a code phase yet.

## P6 — Historical data remediation (Defect C data, Defect D hole). Depends on: P2, P3, D4, owner.

**Goal:** correct/complete historical rows. **Not an in-place UPDATE for quarter** (collision blocker
D4) — re-derive from config or re-run aggregation. Month-numbered rows: run the prod collision
preflight first.
**Rollback (mandatory pre-execution):** snapshot affected `id`s + prior values to a backup table;
author and test the reverse predicate; dry-run against a restored snapshot. One-way door — treat as
such.
**Acceptance:** post-remediation, `horizon_value` == derived lead for a sampled audit; row counts
recorded before/after.

---

## Sequencing (dependency graph)

```json
{
  "phases": {
    "P0":     { "depends_on": [], "parallel_agents": 0 },
    "P-TEST": { "depends_on": ["P0"], "parallel_agents": 3 },
    "P1a":    { "depends_on": ["P0", "P-TEST"], "parallel_agents": 2 },
    "P1b":    { "depends_on": ["P0", "P-TEST"], "parallel_agents": 1 },
    "P2":     { "depends_on": ["P0", "P-TEST"], "parallel_agents": 1 },
    "P3":     { "depends_on": ["P0", "P2"], "parallel_agents": 1 },
    "P4":     { "depends_on": ["P0", "P-TEST"], "parallel_agents": 1 },
    "P5":     { "depends_on": ["P0"], "parallel_agents": 1 },
    "P6":     { "depends_on": ["P2", "P3"], "parallel_agents": 1 }
  }
}
```

**Recommended landing order:** P0 → P-TEST → (P1a+P1b together, ship the reported-symptom fix) →
P2 → P3 → P4 → P5 → P6. P1a+P1b deliver the user-visible fix early and reversibly; the data-mutating
P6 lands last.

---

## Blast-radius checklist (every consumer P2/P3/P6 must not break)

From the readiness inventory. Each must be edited-in-scope or explicitly ruled out per phase:

- Second month writer: `sapphire/services/.../data_migrator.py:620-672,760-770,1072-1103` (colleague).
- Skill producers: `skill_metrics.py:1180-1683`; `ensemble_calculator.py:278-405,527-562`.
- READ-DERIVE (period/lead from `horizon_value`): `db.py:735`; `data_reader.py:3143-3147`; `pairs.py:487`.
- Lead-pinned reads: `long_term_forecasting/data_interface.py` (`WHERE horizon_value=1`);
  `dashboard/utils.py`; `db.py` all-station stats dedup; `validate_pipeline.py:511-601`.
- Recalc/nightly: `postprocessing_operational_long_term.py`, `postprocessing_maintenance_long_term.py`,
  `recalculate_skill_metrics.py`.
- Import cutoff map: `bin/utils/migration_py/long_forecast.py:210-275`.

---

## Definition of done (whole effort)

- All P-TEST reds green; all goldens green; `SAPPHIRE_TEST_ENV=True bash run_tests.sh` zero
  failures / zero unexpected skips.
- Flag-off byte-identical to `origin/maxat_sapphire_2`; kghm unchanged.
- `bash apps/run_locally.sh all` and the Docker smoke tests pass after P2/P3.
- Colleague sign-off recorded for the service-side contract (D2/D5).
- P6 executed (or explicitly deferred) with snapshot + tested reverse predicate.
- No real station codes / discharge values in any committed file.

---

## Open items carried from the issue draft
Open questions 1 (non-uniform backfill tagging), 5 (2024–2025 hole cause), 6 (local `hv=3` anomaly),
9 (season data-mixing origin) remain diagnostic; resolve within P5/P6 as they gate remediation.
