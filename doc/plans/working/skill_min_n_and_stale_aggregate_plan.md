# Plan v3 — Reliable long-term aggregate skill: min-n gate + stale-aggregate replacement

**Status:** v3. v1 → GO-WITH-CHANGES (8 findings, folded into v2). v2 → GO-WITH-CHANGES (5 findings,
folded into v3): (1) invalidation broadened to **all** keys no longer emitted incl. **raw** n<K rows
that otherwise poison forecast-side membership; (2) read-side consumer list completed; (3) **P1 now
depends on P2** (invalidation must diff against the post-min-n key set); (4) tombstone rows excluded
from the "no row < K" acceptance; (5) stale-proof boundary documented. v3 → GO-WITH-CHANGES (3
citation/scope fixes, folded): quarter/season SM forecast gate is `ensemble_calculator.py:628` (not
:610; :625 is the fixed-LR EM note); operational-season + maintenance-quarter/season selection
callsites added; `get_forecast_stats_all` must filter tombstones before sort/dedup and include
`horizon_value` in its month dedup. Ready for implementation.
**Base:** `origin/maxat_sapphire_2` at `f6a9c040` (merge of PR #405). Module
`apps/postprocessing_forecasts` + `apps/forecast_dashboard` (both fair game). Service endpoints are
colleague-owned — see Ownership.

---

## 1. Background & evidence

Validating the LT Skilled-Mean NSE>0 relaxation (PR #405) on the local Tajik `postprocessing_db`
surfaced catastrophic aggregate skill rows. **Two independent, pre-existing defects.**

**Defect A — stale rows never overwritten.**
Skill writes are upsert-only (`api_writer.py:_write_skill_metrics_to_api` ~:421 →
`client.write_skill_metrics` ~:671; service CRUD updates/inserts matching keys only,
`sapphire/services/postprocessing/app/crud.py:277-345`; unique key includes `horizon_value`,
`.../models.py:229-236`). When a run stops emitting a key — an aggregate gated down to <2 models
(`_add_skilled_mean` discard, `skill_metrics.py:1724-1728`), **or** (under P2) a raw model row that
falls below the new n-floor — the prior row survives.
**Stale proof (reviewer-reproduced):** the *definitive* indicator is *fewer than 2 stored-composition
members currently pass NSE>0* → **6** monthly SM rows (strictly unproducible). The broader
*stored composition ≠ live NSE>0 pool* diagnostic flags **106/106** bad monthly SM rows but is **not
a perfect freshness oracle** — `_add_skilled_mean` also drops missing-MAE members and intersects with
available forecast rows (`skill_metrics.py:1651-1673`), so a *fresh* composition can legitimately
differ from the raw NSE>0 pool. Use the strict-<2 subset as proof; use 106/106 as a diagnostic, or
build a fuller expected-composition query. Example `17329/mon3/lead0`: stored `LR_Base, LR_SM`,
NSE=−874.6, current `LR_BASE`=−0.050 / `LR_SM`=−0.034, live pool only `LR_SM_DT` → unproducible.
**Membership poisoning:** stale raw low-n rows are not just a display problem — forecast-side EM/SM
selection reads skill rows (`ensemble_calculator.py:285-291`), so a surviving raw `n<K` row can
re-enter a membership pool unless it is both floored at selection (P2) and invalidated (P1). Live DB:
raw rows that stop being emitted at K=4 — MONTH 1222 (411 NSE>0; 112 still pass the default EM gate),
QUARTER 41 (27 NSE>0), SEASON 40 (11 NSE>0).

**Defect B — small-sample noise passes the gate.**
`filter_for_highly_skilled_forecasts` is an AND-filter with **no** `n_pairs` predicate
(`skill_metrics.py:1800-1826`); the only floor is `n_pairs>=2` on outputs (`:1472-1477` /
`:2460-2464`). Raw monthly models at `n_pairs=2`, NSE>0 pass the SM gate; aggregate SM/EM rows exist
at `n_pairs=2` with NSE up to 0.9997 (noise). Old NSE>0.8 masked it; NSE>0 exposes it.

**Not #405-specific — with a nuance:** monthly EM uses the *default* gate (`skill_metrics.py:1360`;
operational `ensemble_calculator.py:285-306`), untouched by #405 yet affected → pre-existing.
Quarter/season EM is a fixed raw-model mean over `AGGREGATED_EM_RAW_MODELS`=LR_BASE/LR_SM
(`skill_metrics.py:2349-2351`, `ensemble_calculator.py:625-643`, `model_names.py:14`) → for
quarter/season EM only the **output floor** applies; membership is not gated and must not change.

### Coverage (month rows surviving a min-n floor)
| model | total | n≥3 | n≥4 | n≥5 | n≥6 |
|---|---|---|---|---|---|
| EM | 501 | 407 | 317 | 276 | 249 |
| Skilled Mean | 776 | 603 | 450 | 375 | 331 |
| Naive Mean | 3335 | 2189 | 1467 | 1098 | 872 |

Quarter SM is thin (56/110 survive at both K=4 and K=5) — a coverage risk to accept, not a blocker.

---

## 2. Decisions locked (owner to confirm)
1. **K = 4 (month), 5 (quarter/season)** — config var, horizon-overridable, never hard-coded.
2. **Staleness = Approach B (client-side invalidation), scope = every long-horizon skill key not in
   the final post-P2 emitted set** — raw `n<K` rows **and** discarded EM/NM/SM aggregates. The recalc
   computes its final emitted key set, reads existing keys (`read_skill_metrics`), and upserts an
   **invalidation/tombstone row** (`n_pairs=0`, metrics NULL — schema permits, `schemas.py:170-181`,
   `models.py:211`; CRUD upsert overwrites, `crud.py:323-327`) for the difference. **B is complete
   only if every read path suppresses tombstones** (P1). Approach A (service delete/replace endpoint)
   is the alternative if the owner wants a server-side clean replace — needs colleague coordination.
3. **Where min-n applies:** monthly **EM** membership (skill-side `:1360` **and** forecast-side gate
   `ensemble_calculator.py:285`) → `n_pairs>=K`; **SM** membership (skill `:1642`/`:2603`, forecast
   monthly `:285` / quarter-season SM gate `:628`) → `n_pairs>=K`; **NM** membership stays
   **ungated**; **all real
   EM/NM/SM output rows** → `n_pairs>=K`; quarter/season EM → **output floor only** (fixed-LR
   membership untouched). Short-term (pentad/decad) untouched. Applying the floor to the forecast-side
   gate is what stops stale/low-n raw rows from entering a membership pool.
4. **Lead-aware alignment is advisory** (`skill_lead_aware_plan_revised.md` absent from this ref, no
   committed min-n) — no hard dependency; sequence to avoid conflicting `skill_metrics.py` diffs.

---

## 3. Phases

### P0 — Diagnosis lock (analysis only)
- **Goal:** reproduce §1 numbers (strict-<2 stale proof + coverage) across month/quarter/season for
  raw + EM/NM/SM; finalize `K` per horizon and Approach B vs A.
- **Files:** none. **Depends on:** —. **Agents:** 1 (read-only).
- **Acceptance:** decision memo appended here for owner sign-off.

### P2 — Min-n reliability gate (Defect B) — *implemented before P1*
- **Goal:** configurable `K` at membership gates (skill- **and** forecast-side) and output floors per
  §2.3.
- **Files:** `src/skill_metrics.py` (gate `:1800`; monthly EM `:1360`; SM `_add_skilled_mean:1612`
  gate `:1642`; inline aggregate EM `:2346-2433`; aggregate SM gate `:2603`; NM `:1506-1535`/
  `:2489-2515` output-floor-only; floors `:1476`/`:2460`); `src/ensemble_calculator.py`
  (forecast-side gate monthly `:285`, quarter/season SM gate `:628`; `:625` = fixed-LR EM note, do
  not gate); config module (add
  `ieasyhydroforecast_min_pairs_long_term`, horizon-overridable).
- **Depends on:** P0. **Agents:** 1 (worktree-isolated).
- **Constraint (agent prompt):** *No signature/data-flow/control-flow changes. Read `n_pairs` from the
  computed skill frame; do not recompute skill. Gate stays an AND-filter; add `n_pairs>=K`. NM
  membership stays ungated. Quarter/season EM membership (fixed LR) unchanged — output floor only.
  Short-term byte-unchanged.*
- **Acceptance:** no **real** (non-tombstone) EM/NM/SM row has `n_pairs<K`; no gated member (skill or
  forecast side) has `n_pairs<K`; short-term byte-identical; monthly EM membership changes only by the
  floor.

### P1 — Stale replacement + read-side suppression (Defect A, Approach B)
- **Goal:** after a full recalc, no stale long-horizon **skill** key survives (raw or aggregate), and
  no consumer treats a tombstone as real skill.
- **Files:** `recalculate_skill_metrics.py`, `src/api_writer.py` (emit tombstones for the
  post-P2 non-emitted key set); **read/consumer suppression:** `src/data_reader.py` (:432-461,
  :2546-2570); `apps/forecast_dashboard/src/db.py` — `get_forecast_stats` (:528-557),
  **`get_forecast_stats_all` (:561-602)** — filter tombstones **before** its sort/dedup at :600 and
  add `horizon_value` to the month dedup (it currently dedups without it, unlike `get_forecast_stats`
  at :552); month-0 (:919), embedded quarter (:888), season (:989);
  operational/maintenance **selection** consumers `postprocessing_operational_long_term.py:103,169,205`
  and `postprocessing_maintenance_long_term.py:135,270,341`. Centralize suppression in
  `data_reader.read_skill_metrics` so the selection callsites are covered transitively; dashboard paths
  still need explicit handling. (Bulletins carry merged skill fields, no direct read — covered
  transitively; confirm.)
- **Depends on:** P0, **P2** (invalidation must diff against the final post-min-n key set).
- **Agents:** 1 (worktree-isolated).
- **Acceptance:** stale query returns 0 (raw and aggregate); tombstones excluded from display **and**
  selection in every listed consumer; a second identical recalc is idempotent (tombstones persist,
  not duplicated).

### P1b — Forecast-side `long_forecasts` staleness
- **Goal:** the same stale problem on **forecast** rows (monthly/quarter/season EM/SM/NM in
  `long_forecasts`) is fixed or explicitly deferred with owner sign-off.
- **Files:** `src/ensemble_calculator.py` (:223-359, :588-694), `src/api_writer.py`
  (`write_long_forecasts` :803-935; upsert-only CRUD `crud.py:105-152`, key `models.py:147-156`),
  plus forecast read/dashboard consumers.
- **Depends on:** P0. **Agents:** 1. **May be deferred** (record + add deferred-state test in P3).
- **Acceptance:** no stale aggregate `long_forecasts` row survives regeneration (or a tested deferral).

### P3 — Locked tests
- **Files:** `apps/postprocessing_forecasts/tests/`, `apps/forecast_dashboard/tests/`.
- **Depends on:** P1, P1b, P2. **Agents:** 1.
- **Acceptance (placeholder code `19999` only):**
  - **Merged P1+P2 behavior** (not phases in isolation): after a recalc with the floor active, an old
    **raw** `n_pairs=K-1` row is invalidated/suppressed **and cannot enter EM/SM membership**; a
    discarded aggregate key is invalidated.
  - Tombstone idempotency: invalidation survives a 2nd identical recalc.
  - `n_pairs=0`/all-NULL rows suppressed by postprocessing readers, dashboard (incl.
    `get_forecast_stats_all` + month0/embedded-quarter/season paths), and operational/maintenance
    selection.
  - Min-n: member `n_pairs=K-1` excluded, `=K` included; aggregate resolving `<K` not emitted.
  - Short-term unchanged; NM ungated-membership but floored-output.
  - `long_forecasts` staleness fixed **or** tested-as-deferred.
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` + dashboard suite — 0 fail / 0 unexpected skip.

### P4 — Recalc + operational verification
- **Depends on:** P3. Orchestrator-run.
- **Acceptance:** stale query = 0 (raw + aggregate); NSE≤0 aggregate rows only genuine high-n; worst
  NSE no longer catastrophic; EM sane; dashboard/bulletins suppress tombstones (visual check).

---

## 4. Orchestration & ownership
Per CLAUDE.md: orchestrator explores/constrains/delegates to Sonnet 4.6 agents; each impl phase in an
isolated worktree; run module + dashboard suites after each phase. Approach B + P1b keep edits in
`apps/`; only Approach A touches colleague-owned `sapphire/services/` (coordinate first). No real
station codes / discharge (use `19999`).

## 5. Risks / open questions
1. `K` blanks thin-data station-months (Tajik); quarter SM 56/110. Confirm K per horizon.
2. Every skill consumer must treat `n_pairs=0`/NULL as "no skill" — P1 enumerates them; missing one
   re-surfaces a bad tile or a poisoned selection.
3. Tombstone rows must not be dropped by the min-n output filter before the API write (P2 filters
   *real* rows only).
4. P1b scope/deferral is an owner decision — deferring leaves stale ensemble *forecasts* visible.
5. Quarter/season EM stays fixed-LR (output-floor-only) unless the owner intends a membership change.

## 6. Dependency graph
```json
{
  "phases": {
    "P0":  { "depends_on": [], "parallel_agents": 1 },
    "P2":  { "depends_on": ["P0"], "parallel_agents": 1 },
    "P1":  { "depends_on": ["P0", "P2"], "parallel_agents": 1 },
    "P1b": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P3":  { "depends_on": ["P1", "P1b", "P2"], "parallel_agents": 1 },
    "P4":  { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```

## 7. For the re-reviewer
Verify against code (`file:line`) in the `f6a9c040` worktree. GO / GO-WITH-CHANGES / NO-GO with
ordered evidence. Confirm: (a) invalidation covers raw `n<K` keys and closes forecast-side membership
poisoning (P2 forecast-side gate + P1 tombstones); (b) the read-side consumer list is complete
(incl. `get_forecast_stats_all`, month0/embedded-quarter/season, operational/maintenance selection);
(c) P1-depends-on-P2 ordering; (d) tombstones excluded from the "no row < K" acceptance and not
dropped before write; (e) the strict-<2 stale proof is the definitive one. Do not implement.

---

## P0 RESULTS (2026-07-07 — completed, orchestrator-validated)

Read-only diagnosis run on local `postprocessing_db` @ `f6a9c040`. Orchestrator independently
reproduced the headline numbers (monthly SM 776/27, monthly EM 501/7, quarter EM 304/304 fixed-LR
present).

**Locked decisions (owner sign-off pending):**
- **K = 4 (MONTH), K = 5 (QUARTER/SEASON)** — confirmed. Monthly K=5 costs ~10pp SM coverage
  (58.0%→48.3%) for little gain; quarter K=5 costs 0 extra SM (56/110 either way) and removes 2× the
  low-n raw rows; season K=5 costs 1 row. Config var, horizon-overridable.
- **Approach B confirmed** — client exposes only `read_skill_metrics`/`write_skill_metrics`
  (`sapphire_api_client/postprocessing_base.py:40,85`, no delete); tombstones schema-viable
  (`schemas.py:170`, `models.py:211` nullable; CRUD upsert overwrites `crud.py:323`).

**CORRECTION to fold into P1 + the reusable stale SQL (§G of the P0 memo):** stale detection must be
**per-aggregate-type/per-horizon**, NOT a single default gate:
- SM (all horizons): member gate `nse>0`.
- EM **monthly**: default gate (`nse>0.8 ∧ sdivsigma<0.6 ∧ accuracy>0.8`).
- EM **quarter/season**: **fixed-LR availability** (LR_BASE & LR_SM present) — *not* a gate. True
  stale ≈ **0** (304/304 quarter EM reproducible); the memo's 285/245 "stale" were gate artifacts.
- NM (all): all current raw models.

**Validated stale/scope inputs for P1 (definitive rule):** monthly SM stale 27, monthly EM stale 7,
monthly NM stale 2960; quarter SM stale 38; season EM/NM/SM effectively reproducible under the
corrected rule. Raw rows dropped by the floor (membership-poisoning set) at the locked K: MONTH K=4
→ 1222 rows (411 nse>0, **112 pass the default gate**); QUARTER K=5 → 41 (27 nse>0, 0 gate);
SEASON K=5 → 40 (11 nse>0, 1 gate).

**Coverage at locked K:** MONTH — EM 317/501 (63.3%), SM 450/776 (58.0%), NM 1467/3335 (44.0%);
QUARTER — EM 300/304 (98.7%), SM 56/110 (50.9%), NM 304/309 (98.4%); SEASON — EM/NM 225/245 (91.8%),
SM 0 (no rows).

**Open owner decisions before coding:** (1) P1b — fix `long_forecasts` forecast-side staleness now,
or defer with sign-off. (2) Confirm K + Approach B above.
