# PREPQ-022: Fetch discharge norms for virtual stations (iEH HF SDK `1907a30`)

**Status**: Draft (plan rev 3, 2026-09-25 — rev 1 reviewed out-of-loop by `codex exec` (14 findings),
rev 2 confirm-fixes pass (3 factual corrections applied); D-A decided: (b))
**Module**: `preprocessing_runoff`
**Priority**: High. Removes the recurring kyg `sdk_failed=4` (PREPQ-014) and gives virtual stations
a norm.
**Related**: PREPQ-014 (root cause), PREPQ-015 (norm-less row fall-through, #475), PREPQ-020
(short-horizon row-drop, fixed on trunk `a6b9427e`), PREPQ-019 (backfill discards writer statuses —
stays separate), PREPQ-010 (local norm derivation — not replaced by this).

## Problem

`get_norm_for_site(code, "discharge", ...)` raises `ValueError("No path provided or the provided
path is None")` for every virtual station: the SDK resolves the site UUID from the hydrological
registry only. On kyg, 4 virtual stations are in the preprocessing work list. The long-horizon writer
reports `total_attempted=62 written=53 norm_absent=5 sdk_failed=4`, its CLI exits 4 (partial SDK
failure; `sync_long_horizon_hydrograph.py:799` ff. — 5 = API failure takes precedence, 6 = all SDK
failed). `run_locally.sh:988` deliberately shows exit 4 as neither a failure nor a result row. The SDK
failures surface as per-station warnings and in the full CLI summary (`:953`); the writer's
`DEGRADED:` line (`:809`) counts NORM_ABSENT, not SDK_FAILED, and need not appear on an exit-4 run. The short-horizon writer counts the same stations as
`pentad_sdk_failed`/`decade_sdk_failed` and can still exit 0 (`sync_short_horizon_hydrograph.py:1139`).
Rows are written without norm (PREPQ-015/020), so percent-of-norm is blank.

## What changed upstream (diff `2cc7953..1907a30`, verified by reviewer)

- New keyword `get_norm_for_site(..., virtual=False)`. With `virtual=True`: discharge only, UUID
  looked up at `stations/<org>/virtual`, `virtual=true` query param, path `hydrological-norms/<uuid>`.
- **Opt-in** — the default call is unchanged, so re-pinning alone fixes nothing.
- `_get_site_uuid_for_site_code` now filters by exact `station_code` before the `station_type`
  match. This also affects SDK meteo norm lookups, but no module currently calls one (swept).
- Backend returns `[]` for any period where not every member station has a norm.

## Evidence (P1, done 2026-09-25, read-only, kyg tunnel, old vs new SDK)

- **Regular stations:** 60 codes × 3 periods = 180 lookups, **value-identical** old vs new SDK
  (full value lists compared, not just shapes). New-SDK outcomes: month 55 valid / 5 empty, pentad
  10 / 50, decad 57 / 3.
- **Virtual stations (6):** default call still raises under the new SDK. With `virtual=True`:
  month 3/6, pentad 1/6, decad 4/6 valid; 2/6 return `[]` for all periods; all returned values finite.
- **Registry collisions:** `get_virtual_sites()` ∩ `get_discharge_sites()` codes (string-normalised)
  = **0** on kyg. Not checked on taj.
- Which 4 of the 6 are in the kyg work list was not established (P3 shows it). The virtual SDK values
  are kept locally (scratchpad, never committed) as the P3 reference.

## Scope: call sites (owner D1 — operational + maintenance flows of preprocessing_runoff)

| Call site | Reached by |
|---|---|
| `sync_short_horizon_hydrograph.py:754` `_lookup_short_horizon_norms` (`p`, `d`) | `preprocessing_runoff.py:214` on HF-backed runs in both modes (skipped for legacy iEasyHydro / no HF SDK, `:546`) — this includes Docker/Luigi maintenance, whose image CMD runs only `preprocessing_runoff.py` (`Dockerfile:36`, `pipeline_docker.py:1753`); standalone `main()`; `backfill_discharge_aggregation.py` |
| `sync_long_horizon_hydrograph.py:411` `_lookup_monthly_norms` (`m`) | local `run_locally.sh:961/968` maintenance step; `bin/yearly_runoff_hydrograph_aggregation.sh:202` (server); `backfill_discharge_aggregation.py` |

**Legacy check (owner D1): confirmed legacy, out of scope.** Whole-repo sweep (incl. `apps/pipeline`,
Dockerfiles, `bin/`), independently re-verified by the reviewer:
- `forecast_library.write_pentad_hydrograph_data` (`:4760`) / `write_decad_hydrograph_data`
  (`:5134`): no production caller; API write retired in M2 (`:4901`, `:5394`; guarded by
  `iEasyHydroForecast/tests/test_legacy_short_horizon_writers_retired_m2.py`). Tests only.
- `write_decad_hydrograph_data_first_version` (`:5665`): no caller.
- `write_month_hydrograph_data` (`:5557`): only via `sync_monthly_norms.py`, `DEPRECATED
  (2026-06-02)`, invoked by nothing.

## Design

Keep classification and exception grading **inside the existing lookup functions** (long-horizon
grades the SDK-shaped 404 `ValueError` as NORM_ABSENT; short-horizon grades every exception as
SDK_FAILED — unchanged).

1. **Virtual set, once per writer invocation.** `write_long_horizon_hydrograph` /
   `write_short_horizon_hydrograph` call `get_virtual_sites()` once before the station loop
   (codes normalised with `str(...).strip()`), and pass the set to the lookup via a new optional
   parameter whose default reproduces today's behaviour. The writer does its own discovery because
   the operational cache path (`preprocessing_runoff.py:344`, cache schema `src.py:6053`) carries no
   virtual identity; no cache-schema change. Cost: one extra listing call per writer invocation
   (backfill: per writer per year).
2. **Discovery failure.** If `get_virtual_sites()` raises: WARNING, empty set → lookups behave
   exactly as today. Honest limits: this preserves today's grading *where execution reaches the
   writer*. An earlier discovery failure in uncached station resolution (`setup_library.py:1542`)
   already aborts before the writer; operational preprocessing (`:214`) and backfill (`:108`) ignore
   writer statuses, so a later failure there is a log warning only — as today.
3. **Routing — depends on D-A** (below).
4. Existing run summaries already expose norm outcomes (`long:839`, `short:947`); **no new summary
   field**.

### D-A — DECIDED 2026-09-25 by owner: option (b). Which norm a code gets when it is in both registries

`get_all_forecast_sites_from_HF_SDK` appends virtual sites after regular ones and keeps the first
occurrence (`setup_library.py:1538, 1556`) — but only among **forecast-enabled** objects
(`forecast_library.py:7538`), so a colliding code is the regular station only if its regular entry
is forecast-enabled; otherwise the enabled virtual entry wins. Kyg has 0 collisions (measured); taj
unknown.

- **(b) — recommended: regular first, virtual on failure.** Default call as today; only if it
  raises **and** the code is in the virtual set, retry with `virtual=True` and grade the retry's
  result/exception alone. Preserves today's output for every code whose default call does not raise.
  Caveat: a colliding code whose default call raises (outage, or an unregistered regular entry) gets
  the virtual norm. An outage costs one extra failing call per virtual station.
- (a) membership routing: every code in the virtual set goes straight to `virtual=True`. Simpler,
  one call, but silently switches a colliding code from its regular norm to a weighted member sum,
  contradicting the station identity used everywhere else.

## SDK pin

- `apps/preprocessing_runoff/uv.lock` → `ieasyhydro-sdk` at `1907a30` via
  `uv lock --upgrade-package ieasyhydro-sdk`; **assert the lock resolves exactly `1907a30`** (it tracks
  `@master`, which may have moved). Docker builds with `uv sync --frozen` from this lock
  (`Dockerfile:29`); `iEasyHydroForecast` is an editable dependency, so its own lock is not used here
  and stays unchanged.
- Then `uv sync` the preprocessing_runoff venv (both `run_locally.sh:603` and `run_tests.sh` use the
  existing venv) and verify the installed SDK commit is `1907a30` (install metadata) and has the `virtual`
  parameter before P2 tests and P3.
- Other 10 locks stay on `2cc7953`. Follow-up re-pin only where a module actually calls SDK norm
  lookups (today: none outside preprocessing_runoff).

## Phases

### P1 — SDK probe (DONE) — see Evidence.

### P2 — Implementation (depends on P1; D-A = (b))
- **Agents**: 1 Sonnet general-purpose, `isolation: "worktree"`, branch
  `fix_preprocessing_runoff_virtual_station_norms` from `origin/maxat_sapphire_2`.
- **Files allowed**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`,
  `apps/preprocessing_runoff/sync_short_horizon_hydrograph.py`, `apps/preprocessing_runoff/uv.lock`,
  tests under `apps/preprocessing_runoff/test/`, one paragraph in `apps/preprocessing_runoff/README.md`.
- **Constraint**: "Do NOT change any existing function signatures, data flow logic, or control flow.
  Your changes must be purely additive or modify only the specific behavior described." New optional
  parameters defaulting to today's behaviour are allowed. No station-selection changes, no cache
  schema change, no backfill exit-code change (PREPQ-019). No `git stash`.
- **Tests** — keep existing strict fakes (their `get_norm_for_site` has no `virtual` param, e.g.
  `test_sync_long_horizon_hydrograph.py:50`, `test_short_horizon_norm_decoupling.py:87`,
  `test_backfill_discharge_aggregation_m4.py:70`) so they prove ordinary calls never pass `virtual`;
  add a separate virtual-aware fake with an explicit `get_virtual_sites()` response (never rely on a
  missing method to hit the fallback). Both horizons:
  1. Regular code: call has no `virtual` kwarg; output unchanged.
  2. Virtual code with valid values → VALID, norm written (12 / 72 / 36).
  3. Virtual code `[]` → NORM_ABSENT; a previously stored norm is preserved by the read-merge
     (short-horizon incl. period 1 stamped 31 Dec of Y-1), a never-normed row stays normless, a failed
     preservation read does not write nulls.
  4. `get_virtual_sites()` raises → WARNING, virtual codes graded exactly as today, regular unaffected.
  5. Virtual call raises → SDK_FAILED; the same 404-shaped `ValueError` → NORM_ABSENT in long-horizon
     but SDK_FAILED in short-horizon (existing asymmetry preserved).
  6. `get_virtual_sites()` called once per writer invocation, not per station.
  7. Mixed coverage for one virtual code: pentad `[]`, decad valid.
  8. Code-type robustness: int work-list code vs str virtual-set code and vice versa; a virtual code
     that is also locally "manual" in `config_all_stations_library.json`. Document the existing
     behaviour without changing it: standalone/backfill resolution excludes manual codes, except
     that `resolve_sdk_station_codes` (`:860`) compares before stringifying, so an int code evades
     the str manual set; `preprocessing_runoff.py` passes manual codes through.
  9. D-A collision fixtures: code in both registries (i) default succeeds, (ii) default raises;
     plus the forecast-eligibility case (regular entry disabled, virtual enabled).
  10. One captured-record backfill test: virtual norms present in written records for all horizons;
      dry-run unchanged.
  11. Operational cache-hit path (`preprocessing_runoff.py:344`): writer discovery succeeds → virtual
      norm fetched; discovery fails → today's behaviour.
- **Acceptance**: from repo root, `SAPPHIRE_TEST_ENV=True bash apps/run_tests.sh preprocessing_runoff`
  and `... iEasyHydroForecast` → zero fail / zero unexpected skip. (Full `run_tests.sh` runs once in P4,
  as CLAUDE.md requires before a PR.)

### P3 — Live kyg verification (depends on P2)
Tunnel up, `.env_bea_kghm`, venv synced and SDK signature checked. No fresh baseline run (owner D2).
0. **Before any write**: read back from the preprocessing API the stored norms of a small sample of
   regular stations (month + pentad + decad) as the no-regression reference.
1. `bash apps/run_locally.sh maintenance:preprocessing_runoff`. Pass criteria: long-horizon
   `sdk_failed=0`, `api_failed=0`, `written + norm_absent = 62`, `written ∈ [54, 56]` (53 + k, k = work-list
   virtual stations with monthly coverage, 1–3) — valid only if the roster is still 62 with 4 virtual
   and the regular monthly baseline still 53/5; check both in the run log before applying the numbers; short-horizon: no virtual station in
   `pentad_sdk_failed` / `decade_sdk_failed`. (The absence of an exit-4 FAIL row is not a criterion —
   `run_locally.sh:988` never shows one.)
2. `bash apps/run_locally.sh preprocessing_runoff` (operational, cache likely hit) — same
   short-horizon criterion.
3. **Measure the artefact**, per horizon: for one work-list virtual station with coverage in that
   horizon, API-stored `norm` equals the P1 SDK value. If no work-list virtual station has pentad
   coverage, record that instead of forcing a sample. Regular sample from step 0 unchanged.
4. Record counts only (no station codes) in this file.

### P4 — Review + PR (depends on P3)
Out-of-loop diff review (`adversarial-review` skill), re-review any fix round (re-run affected suites
only if code changed), full `run_tests.sh` once, PR to `maxat_sapphire_2`. Point PREPQ-014 draft here; add the PREPQ-022 row
to `doc/plans/module_issues.md`.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 0 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 0 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```

## Out of scope
Legacy `forecast_library` writers and `sync_monthly_norms.py`; re-pinning other modules; cache-schema
changes; backfill status reporting (PREPQ-019); informing Kyrgyz Hydromet about member-norm gaps
(owner D3); PREPQ-010; dashboard/bulletin presentation.
