# Plan — Backfill pentad/decad `runoffs.discharge` (LR maintenance refresh)

> ## ⛔ BLOCKED — do not execute this backfill yet (2026-07-14, out-of-loop review)
>
> **1. BLOCKER — [PREPQ-011](../issues/high_prio_gi_draft_runoff_read_merge_pagination_clobber.md):
> the read-merge-write this plan relies on is broken past row 100.** The whole safety argument below
> is "read-merge-write means we never clobber a non-null `discharge`/`predictor` with NULL". In
> shipped code that read is **unpaginated** (`forecast_library.py:3761`) while client and service
> both default to `limit=100` — so every existing row past the 100th reads as *absent*, the outgoing
> `None` is written, and the service's full-column upsert (`crud.py:33-36`) overwrites the stored
> value with `NULL`. Worse, the service orders by `(code, date)` (`crud.py:80`), so the 100 rows that
> *are* protected are the **oldest**. Over a multi-year archive that leaves **~91% of rows exposed**
> — i.e. running this plan today would cause exactly the data loss it was written to prevent.
> **Fix PREPQ-011 first.**
>
> **2. Phase A is ALREADY IMPLEMENTED — do not re-implement.** The early-`sys.exit(0)` root cause
> described below is fixed on `origin/maxat_sapphire_2`: `hindcast_caught_up` is set at
> `apps/linear_regression/linear_regression.py:718-724`, pentad/decad runoff is written at
> `:765-801`, and the exit happens after at `:809-811`. Tests pin it
> (`apps/linear_regression/test/test_integration_main.py:396-425`, and no-LR-forecast-writes at
> `:430-454`). Keep Phase A only as verification/runbook notes.
>
> **3. The one-shot `initial` commands below are WRONG as written.** They prefix
> `SAPPHIRE_SYNC_MODE=initial` onto `maintenance:linear_regression`, but that target **injects
> `SAPPHIRE_SYNC_MODE=maintenance`** itself (`apps/run_locally.sh:765-772`; `run_in_venv` appends
> env at `:440-447`), so the prefix does not win and you silently get a maintenance-mode run. A
> dedicated initial-safe command/target is needed before any full backfill.
>
> **4. The merge is not atomic.** App-side read-then-write against a full-column service upsert has
> no lock. Enforce a single-writer maintenance window, or escalate a service-side
> partial-update/COALESCE design to the service owner (`sapphire/services/` is colleague-managed).
> That would also remove the whole clobber class — see PREPQ-011's "Related".

**Status:** ~~GO-WITH-CHANGES~~ → **BLOCKED on PREPQ-011** (planner → reviewer → orchestrator-amended). Awaiting owner sign-off before execution.
**Target branch:** `develop_forecast_skill_eval` (the current working tree; also contains the `forecast_skill_eval` consumers this affects). All line numbers in this plan were confirmed against this branch.
**Owner protocol:** Orchestrator delegates all code to Sonnet 4.6 agents; reviews every diff. No edits under `sapphire/services/`.

---

## Problem (verified)

The `runoffs` pentad/decade DB column `discharge` (mapped from dataframe `discharge_avg`, `forecast_library.py:3676`) is NULL on many rows that should be computable. `discharge_avg` is the forward-looking target (avg of the upcoming pentad/decad); `predictor` is backward-looking and present.

Root cause confirmed in code: in hindcast/maintenance mode, `linear_regression.py:719-724`
```python
if forecast_date > date_end:
    logger.info("All forecasts are already up to date. Nothing to do.")
    sys.exit(0)
```
fires **before** `get_pentadal_and_decadal_data()` (`:764`) and the runoff writes `write_pentad/decad_time_series_data` (`:787-822`). So once forecasts are caught up, `maintenance:linear_regression` never re-aggregates/re-writes runoff, and previously-NULL elapsed-period `discharge` stays NULL. Operational mode only writes today's slice (`_write_runoff_to_api` sync-mode filter, `forecast_library.py:3608-3645`).

LR forecast production is healthy and must remain behaviorally unchanged. `runoffs.discharge` (pentad/decad) is derived analytics, **not** a model input.

### Live NULL breakdown (2026 pentad, masked)
1212 rows: discharge-null=103, predictor-null=46, both-null=38 ⇒ predictor-present/discharge-null=65, **discharge-present/predictor-null≈8**. Classified: ~1 not-yet-observable, ~41 daily-input-starved, ~61 stale-but-computable. Decade analogous (67 discharge-null: ~1/~10/~56).

---

## Scope rules

- **No code changes in `sapphire/services/`.** If preserving non-null `runoffs.discharge` is found to require API schema/endpoint/CRUD/upsert changes, STOP and raise an open discussion item with the service owner (see Open Items).
- `runoffs.discharge` (pentad/decade) is derived analytics only. **LR forecast production must remain behaviorally unchanged**: no forecast-loop changes, no altered LR inputs/payloads.
- Tests use synthetic station codes only (e.g. `19999`); never operational codes or real discharge values. No operational data committed; mask codes in any shared output.

## Critical service behavior (verified, drives the design)

The runoff upsert is a **blind full-column clobber**, NOT `on_conflict_do_update` (`sapphire/services/preprocessing/app/crud.py:16 create_runoff`):
```python
incoming = [item.dict() for item in bulk_data.data]      # Pydantic emits None fields
if _has_changes(existing, data):                          # crud.py:13 — True if ANY field differs
    for k, v in data.items():
        setattr(existing, k, v)                           # sets EVERY column, including None
```
⇒ an incoming `predictor=None` overwrites an existing non-null `predictor` (and vice-versa). The writer always sends both keys (`forecast_library.py:3676/3679`). Unique key `uq_runoffs_horizon_code_date` on `(horizon_type, code, date)` (`models.py:38`) — upsert is idempotent on that tuple.

**Consequence:** a clobber guard is mandatory for any re-write of existing rows. "Skip if either null" prevents clobber but **silently drops** the ~8 discharge-present/predictor-null rows (data loss vs. our goal). Therefore this plan uses **client-side read-merge-write** (in `apps/`, no service edit, zero data loss) instead.

---

## Phase 0 — Consumer trace & scope confirmation (read-only)

**Goal:** Confirm downstream consumers and lock Part 2 (one-time backfill) urgency. Already traced; this phase records/locks it and closes the one open sub-item.

Confirmed consumers of pentad/decade runoff `discharge`:
- `postprocessing_forecasts/data_reader.py:1458/1473` (reads pentad/decade), normalizes `discharge→discharge_avg` `:1805-1810`, feeds `recalculate_skill_metrics.py:155`.
- `forecast_skill_eval/.../api_readers.py:167`, `observed_truth.py:102`, `norms.py:258`.
- `iEasyHydroForecast/setup_library.py:1952` (pentad) / `:2076` (decade).
- `forecast_dashboard/db.py` uses hydrograph/forecast/skill-metric only — **not** runoff. (Dashboard urgency low; skill/eval urgency real.)

Decision: **Part 2 stays in scope** (stale historical discharge degrades skill/eval outputs).
Open sub-item to close in P0: postprocessing also reads `horizon="day"` (`data_reader.py:900`) for the monthly path — confirm day-horizon discharge is **not** subject to this pentad/decad NULL gap, or scope it out explicitly in one line.

Producer-overlap (resolved, record it): `preprocessing_runoff` maintenance writes only `horizon_type=day` (its own `src._write_runoff_to_api`); LR writes `pentad`/`decade`. Different `(horizon_type,…)` keys ⇒ the two writers never touch the same rows. No conflict.

**Files:** none modified. Read-only: `apps/postprocessing_forecasts/`, `apps/forecast_skill_eval/`, `apps/forecast_dashboard/`, `apps/iEasyHydroForecast/`, read-only behavior check `sapphire/services/preprocessing/`.
**Depends on:** none. **Agents:** 1 (read-only trace).
**Acceptance:** consumer list + call sites recorded; day-horizon scope decision recorded; producer-overlap recorded; no `sapphire/services/` edits.

---

## Phase 1 — Failing TDD coverage (tests first)

**Goal:** Encode the contracts as RED tests on `develop_forecast_skill_eval` before implementation.

**Files:** `apps/linear_regression/test/test_integration_main.py`, `apps/linear_regression/test/test_forecast_library_api.py`, `apps/linear_regression/test/conftest.py` (only if fixture cleanup needed).
**Depends on:** Phase 0. **Agents:** 2 parallel (disjoint files: Agent A → `test_integration_main.py`; Agent B → `test_forecast_library_api.py`).

Tests (behavior-level; mock at the writer boundary; pin `forecast_date`/`date_end`):
- **A1** `test_hindcast_up_to_date_refreshes_runoff_before_clean_exit` — in the caught-up path, `write_pentad/decad_time_series_data` ARE called and `write_linreg_pentad/decad_forecast_data` are NOT. Assert by mocking those four functions.
- **A2** `test_hindcast_up_to_date_does_not_write_lr_forecasts` — explicit forecast-writer-not-called assertion (covers "forecast production unchanged").
- **B1** `test_maintenance_backfill_merges_and_skips_nothing_computable` — maintenance/initial payload preserves existing non-null fields (read-merge-write) and DOES include a row with computed `discharge_avg` even when `predictor` is null (proves no data loss).
- **B2** `test_maintenance_backfill_does_not_clobber_existing_with_null` — incoming `predictor=None` over existing non-null `predictor` results in a merged payload keeping the existing value.
- **B3** `test_operational_mode_still_allows_today_null_discharge` — operational mode unchanged: today's row written with `discharge=None` when target not yet observable, no merge/GET performed.

**Acceptance:** A1/A2/B1/B2 RED on current branch; B3 GREEN (documents preserved behavior). Synthetic codes only; deterministic (pinned dates, mocked writers/clients).

---

## Phase 2 — Implementation (SERIALIZED: A → B)

**Goal:** Make maintenance refresh runoff without changing forecast-loop behavior, and make backfill writes clobber-safe and loss-free.
**Files:** `apps/linear_regression/linear_regression.py` (Agent A), `apps/iEasyHydroForecast/forecast_library.py` (Agent B).
**Depends on:** Phase 1. **Agents:** **1 at a time, serialized** (A then B) — both may need to touch `forecast_library.py` (refresh helper / `mode=` thread), so do NOT run in parallel.

**Agent A — control flow (`linear_regression.py`):**
Relocate the caught-up early exit so the runoff aggregation + time-series writes (`:764`, `:787-822`) run first; if `forecast_date > date_end`, perform the runoff refresh and then exit cleanly **without** entering the forecast loop (`:827+`). When forecasts ARE due, reuse the already-computed `data_pentad/data_decad` (no double-compute, no double-write). Forecast writers stay inside the loop and self-no-op when caught up. *Constraint: do NOT change any existing function signatures, data-flow, or control flow except this specific relocation; changes purely additive/narrowly scoped.*

**Agent B — clobber-safe backfill writes (`forecast_library.py`, `_write_runoff_to_api`):**
For `sync_mode in {maintenance, initial}` only, replace blind writes with **read-merge-write**: GET existing rows for the `(code, date)` set in the window (client `read_runoff`), and for each outgoing row set each of `{discharge, predictor}` to the incoming value if non-null else the existing value; keep `horizon_value`/`horizon_in_year` from incoming; then POST the merged rows. Operational mode unchanged (today's slice, may write `discharge=None`). Add a preflight log of one-null row counts (discharge-non-null & predictor-null, and vice-versa). *Constraint as above; merge logic gated strictly to maintenance/initial.*

**Acceptance:**
- Caught-up hindcast path runs `get_pentadal_and_decadal_data()` + `write_*_time_series_data()` but not `write_linreg_*_forecast_data()`.
- Backfill (maintenance/initial) never clobbers an existing non-null `discharge`/`predictor` with null, and never drops a row that has a computed `discharge_avg` (read-merge-write).
- Operational today-null behavior preserved (B3 green).
- `maintenance` stays a 90-day issue-date window (`forecast_library.py:3631`); recompute re-reads live daily data (`:1265`).
- Idempotent via `(horizon_type, code, date)` upsert; re-running stable.
- Phase 1 tests GREEN; full suites pass (see Test Plan). No `sapphire/services/` edits.

---

## Phase 3 — One-time backfill of pre-window stale rows (docs + run)

**Goal:** Refresh stale rows older than the recurring 90-day window.
**Files:** `apps/linear_regression/README.md`; optional runbook `doc/prod/backfill_period_runoff_discharge.md`.
**Depends on:** Phase 2. **Agents:** 1 (docs + preflight/postflight commands).

**Acceptance:**
- Docs state recurring maintenance covers issue `date >= today-90d`; older rows need the one-shot.
- Quantify one-shot scope (pentad + decade issue rows older than the window, plus earlier years in DB).
- One-shot uses owner-provided `ieasyhydroforecast_env_file_path` (never hardcoded); run pentad and decade separately to bound payload/runtime.
- Preflight counts records + nulls (incl. one-null buckets) before write; if volume too large for the API write, STOP and discuss batching (do not edit services).
- Shared logs mask station codes.

One-shot commands (post Phase 2):
```bash
ieasyhydroforecast_env_file_path=<owner-provided> SAPPHIRE_SYNC_MODE=initial \
  SAPPHIRE_PREDICTION_MODE=PENTAD bash apps/run_locally.sh maintenance:linear_regression
ieasyhydroforecast_env_file_path=<owner-provided> SAPPHIRE_SYNC_MODE=initial \
  SAPPHIRE_PREDICTION_MODE=DECAD  bash apps/run_locally.sh maintenance:linear_regression
```

---

## Phase 4 (optional) — daily-input starvation visibility
Warning-only detection distinguishing not-yet-observable / stale-but-computable / daily-input-starved rows after refresh. Files: `apps/linear_regression/linear_regression.py` (+ test, README). No write-behavior change. Depends on Phase 3.

## Phase 5 (optional) — LR/runoff health checks
Threshold-based warnings for unexpected null `discharge`/`predictor`/forecast fields. Files: `apps/validate_pipeline/` or `apps/linear_regression/`. Warning-only, configurable thresholds, synthetic-code tests. Depends on Phase 3.

---

## Test plan

New/changed tests: `test_hindcast_up_to_date_refreshes_runoff_before_clean_exit`, `test_hindcast_up_to_date_does_not_write_lr_forecasts`, `test_maintenance_backfill_merges_and_skips_nothing_computable`, `test_maintenance_backfill_does_not_clobber_existing_with_null`, `test_operational_mode_still_allows_today_null_discharge`.

Full verification (zero failures, zero unexpected skips):
```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast
```
Baselines: `linear_regression` 327 passed; `iEasyHydroForecast` 720 passed, 1 xfailed. Final counts may rise; failures/unexpected skips must be 0.

## Rollout / verification (local)

> **MUST use maintenance sync mode** — `run_maintenance_linear_regression` (`run_locally.sh`) does NOT set `SAPPHIRE_SYNC_MODE`, so a bare run is operational (today-only) and proves nothing. Production sets it via `pipeline_docker.py:1786`.

Pre-check current nulls:
```bash
curl "http://localhost:8000/api/preprocessing/runoff/?horizon=pentad&start_date=2026-03-21&limit=10000"
curl "http://localhost:8000/api/preprocessing/runoff/?horizon=decade&start_date=2026-03-21&limit=10000"
```
Run recurring refresh in maintenance mode:
```bash
ieasyhydroforecast_env_file_path=<owner-provided> SAPPHIRE_SYNC_MODE=maintenance \
  SAPPHIRE_PREDICTION_MODE=PENTAD bash apps/run_locally.sh maintenance:linear_regression
ieasyhydroforecast_env_file_path=<owner-provided> SAPPHIRE_SYNC_MODE=maintenance \
  SAPPHIRE_PREDICTION_MODE=DECAD  bash apps/run_locally.sh maintenance:linear_regression
```
Post-check: previously-null computable pentad/decade rows in the 90-day window now have `discharge`; not-yet-observable & genuinely-starved rows remain null; no non-null field clobbered to null; no duplicate `(horizon_type,code,date)`; LR forecast rows unchanged when caught up. Mask codes in any shared output.

## Risk register (reassessed)

| Risk | Level | Mitigation |
|---|---|---|
| Null clobber via upsert (full-column overwrite) | **P1** | read-merge-write in maintenance/initial; service off-limits |
| Local verification validity (bare run = operational no-op) | **P1** | prefix `SAPPHIRE_SYNC_MODE=maintenance` + `SAPPHIRE_PREDICTION_MODE` in verify |
| Parallel file conflict in Phase 2 | P2 | serialize A→B (`parallel_agents: 1`) |
| Brittle date tests | P1 | pin dates, mock writers/clients |
| LR forecast behavior change | low | writers inside self-guarding loop; relocation can't reach them when caught up |
| Large one-shot payload | P3 | preflight counts; pentad/decade split; stop-for-batching gate |
| Missed consumer | P3 | consumers enumerated; only open item = day horizon (P0) |

## Open items (do not block; raise separately)
- **Service owner:** runoff upsert (`crud.py:16`) clobbers all columns on conflict; a `COALESCE(EXCLUDED.x, runoffs.x)` partial-update would let single-column refreshes preserve siblings and remove the need for client-side read-merge-write. Out of scope here (service off-limits).

## Dependency graph
```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 2 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```
