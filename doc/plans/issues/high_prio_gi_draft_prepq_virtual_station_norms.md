# PREPQ-022: Fetch discharge norms for virtual stations (iEH HF SDK `1907a30`)

**Status**: In Progress — implemented on branch fix_preprocessing_runoff_virtual_station_norms
(pushed); P2+P3 done; P4 out-of-loop reviews done, PR not yet opened.
**Module**: `preprocessing_runoff`
**Priority**: High. Removes the recurring kyg `sdk_failed=4` (PREPQ-014) and gives virtual stations
a norm.
**Related**: PREPQ-014 (root cause), PREPQ-015 (norm-less row fall-through, #475), PREPQ-020
(short-horizon row-drop, fixed on trunk `a6b9427e`), PREPQ-019 (backfill discards writer statuses —
stays separate), PREPQ-010 (local norm derivation — not replaced by this).

## Problem

`get_norm_for_site(code, "discharge", ...)` raises `ValueError("No path provided or the provided
path is None")` for virtual-only codes that have no usable UUID in the regular hydrological registry
(measured: all 6 kyg virtual codes hit this — see Evidence; not "every virtual station" universally,
since a code present in BOTH registries resolves via its regular UUID and never raises this way). On
kyg, 4 virtual stations are in the preprocessing work list. The long-horizon writer reports
`total_attempted=62 written=53 norm_absent=5 sdk_failed=4`, its CLI exits 4 (partial SDK failure;
`_exit_code_for_long_horizon_summary`, `sync_long_horizon_hydrograph.py:934`, decisions `:957-963` —
5 = API failure takes precedence, 6 = all SDK failed). `run_locally.sh:988` deliberately shows exit 4
as neither a failure nor a result row. The SDK failures surface as per-station warnings and in the
full CLI summary (`print(_format_long_horizon_run_summary_artifact(...))`,
`sync_long_horizon_hydrograph.py:1111`); the writer's `DEGRADED:` line
(`_degraded_long_horizon_summary_line`, `:966-974`) counts NORM_ABSENT, not SDK_FAILED, and need not
appear on an exit-4 run. The short-horizon writer counts the same stations as
`pentad_sdk_failed`/`decade_sdk_failed` and can still exit 0 (`sync_short_horizon_hydrograph.py:1189`).

Rows are written without a norm only when NO previously stored norm can be preserved by the
read-merge (PREPQ-015/020) — a station with a stored norm from an earlier run keeps it across a
norm-absent/SDK-failed rerun, so "normless" and "percent-of-norm blank" describe a never-normed or
first-run station specifically, not every SDK lookup failure. This lookup-level degradation (the norm
call itself raising or returning an unusable shape) is also distinct from a short-horizon WRITE
failure such as `_ShortHorizonDailyReadError` (raised when a station has no usable daily runoff at
all across the climatology window): that drops the horizon's write entirely, rather than writing a
normless row.

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
- The SDK's new exact-`station_code` UUID filter (see "What changed upstream" below) was measured
  value-identical on kyg (180/180 regular-station lookups); taj unmeasured — measure before deploying
  there.

## Scope: call sites (owner D1 — operational + maintenance flows of preprocessing_runoff)

| Call site | Reached by |
|---|---|
| `_lookup_short_horizon_norms` (`sync_short_horizon_hydrograph.py:749`; SDK call `:772`) (`p`, `d`) | `preprocessing_runoff.py:214` on HF-backed runs in both modes (skipped for legacy iEasyHydro / no HF SDK, `:546`) — this includes Docker/Luigi maintenance, whose image CMD runs only `preprocessing_runoff.py` (`Dockerfile:36`, `pipeline_docker.py:1753`); standalone `main()`; `backfill_discharge_aggregation.py` |
| `_lookup_monthly_norms` (`sync_long_horizon_hydrograph.py:499`; SDK call `:550`) (`m`) | local `run_locally.sh:961/968` maintenance step; `bin/yearly_runoff_hydrograph_aggregation.sh:202` (server); standalone `sync_long_horizon_hydrograph.py main()` CLI; `backfill_discharge_aggregation.py` |

**Legacy check (owner D1): confirmed legacy, out of scope.** Whole-repo sweep (incl. `apps/pipeline`,
Dockerfiles, `bin/`), independently re-verified by the reviewer:
- `forecast_library.write_pentad_hydrograph_data` (`:4665`) / `write_decad_hydrograph_data`
  (`:4953`): no production caller; API write retired in M2 (`:4901`, `:5394`; guarded by
  `iEasyHydroForecast/tests/test_legacy_short_horizon_writers_retired_m2.py`). Tests only.
- `write_decad_hydrograph_data_first_version` (`:5609`): no caller.
- `write_month_hydrograph_data` (`:5504`): only via `sync_monthly_norms.py`, `DEPRECATED
  (2026-06-02)`, invoked by nothing.

## Design

Keep classification and exception grading **inside the existing lookup functions** (long-horizon
grades the SDK-shaped 404 `ValueError` as NORM_ABSENT; short-horizon grades every exception as
SDK_FAILED — unchanged).

1. **Virtual set, once per writer invocation.** `write_long_horizon_hydrograph` /
   `write_short_horizon_hydrograph` call `get_virtual_station_codes()` once before the station loop
   (codes normalised with `str(...).strip()`), and pass the set to the lookup via a new optional
   parameter whose default reproduces today's behaviour. The writer does its own discovery because
   the operational cache path (`preprocessing_runoff.py:344`, cache schema `src.py:6053`) carries no
   virtual identity; no cache-schema change. Cost: two extra listing calls per writer invocation
   (backfill: per writer per year) — `get_virtual_sites()` AND `get_discharge_sites()`, the latter for
   the collision exclusion below.
2. **Discovery failure.** If EITHER `get_virtual_sites()` or `get_discharge_sites()` raises: WARNING
   naming which listing failed, empty set → lookups behave exactly as today for EVERY code, virtual or
   not. Honest limits: this preserves today's grading *where execution reaches the writer*. An earlier
   discovery failure in uncached station resolution (`setup_library.py:1542`) already aborts before the
   writer; operational preprocessing (`:214`) and backfill (`:108`) ignore writer statuses, so a later
   failure there is a log warning only — as today.
3. **Routing — depends on D-A** (below).
4. Existing run summaries already expose norm outcomes (`_format_long_horizon_run_summary_artifact`,
   `sync_long_horizon_hydrograph.py:984-1003`; `_log_short_horizon_run_summary`,
   `sync_short_horizon_hydrograph.py:974-998`); **no new summary field**.

### D-A — DECIDED 2026-09-25 by owner: option (b), then AMENDED same day. Which norm a code gets when it is in both registries

`get_all_forecast_sites_from_HF_SDK` appends virtual sites after regular ones and keeps the first
occurrence (`setup_library.py:1545` extend, `:1551-1560` dedup-keeps-first loop) — but only among
**forecast-enabled** objects (`forecast_library.py:7538`), so a colliding code is the regular station
only if its regular entry is forecast-enabled; otherwise the enabled virtual entry wins. Kyg has 0
collisions (measured); taj unknown. Note this registry-merge logic is itself untouched by this
change — see the amendment below for why the collision exclusion this issue implements does not
depend on it.

**The (b)/(a) framing immediately below is SUPERSEDED decision history, kept only for context** — the
amended rule further below (virtual ∧ ¬regular, fail-closed on either listing's failure) is the SOLE
current behaviour; nothing routes purely on original-(b)'s "regular first, virtual on ANY failure"
rule any more.

- **(b) — originally decided, labelled "recommended" at the time: regular first, virtual on
  failure.** Default call as today; only if it raises **and** the code is in the virtual set, retry
  with `virtual=True` and grade the retry's result/exception alone. Preserves today's output for
  every code whose default call does not raise. Caveat, closed by the amendment below: under this
  ORIGINAL framing, a colliding code whose default call raised (outage, or an unregistered regular
  entry) got the virtual norm. An outage costs one extra failing call per virtual station.
- (a) — rejected at the time, unaffected by the amendment: membership routing, every code in the
  virtual set goes straight to `virtual=True`. Simpler, one call, but silently switches a colliding
  code from its regular norm to a weighted member sum, contradicting the station identity used
  everywhere else.

**Amended 2026-09-25 by owner after out-of-loop diff review: codes present in the regular registry
are excluded from the virtual retry; if either listing fails, no retry.** Out-of-loop review of the
P2 diff (finding #1) identified that option (b) as originally decided still let a colliding code fall
through to the virtual retry on a default-call **raise** (the caveat above) — including a transient
failure (e.g. a 500) on that code's own regular lookup, which would then let the virtual
(weighted-sum-of-members) norm silently overwrite its stored regular norm while the run still reports
success. The owner amended (b): `get_virtual_station_codes` now excludes any code present in BOTH
`get_virtual_sites()` and `get_discharge_sites()` (the regular hydrological registry) from the set it
returns, so a colliding code's default-call exception is graded exactly as if the code were never
virtual — on a raise as well as on success. If either listing itself fails, the whole retry mechanism
degrades to "retry nobody" (fail-closed), since excluding nothing when the regular registry can't be
listed would risk letting an undetected collision through.

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
  lookups: **no active production norm-lookup flow exists outside `preprocessing_runoff`**; legacy
  `forecast_library` writers still call `get_norm_for_site` (see Scope's Legacy check above —
  confirmed legacy, no production caller). Because `iEasyHydroForecast`/`forecast_library` is
  installed as an editable dependency INTO the `preprocessing_runoff` venv, any of its OWN
  `get_norm_for_site` calls executed from within that venv (a test, a manual invocation) already get
  the new SDK's exact-`station_code` UUID filter too — they simply have no reachable production
  caller to benefit from it.

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
     that `resolve_sdk_station_codes` (`:1011`, compare-before-stringify `:1017`) compares before
     stringifying, so an int code evades
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
- **Scope as implemented** (added post-P2, verified against the final diff):
  (i) Test item 9's "forecast-eligibility case (regular entry disabled, virtual enabled)" is NOT a
  unit test in this change — it lives entirely in `setup_library.get_all_forecast_sites_from_HF_SDK`'s
  registry-merge logic (see D-A above), outside `sync_long_horizon_hydrograph.py` /
  `sync_short_horizon_hydrograph.py`. The collision exclusion actually implemented in
  `get_virtual_station_codes` is independent of forecast eligibility: it compares the FULL
  `get_virtual_sites()`/`get_discharge_sites()` listings directly, never the forecast-enabled subset
  `get_all_forecast_sites_from_HF_SDK` merges.
  (ii) Test item 11 ("operational cache-hit path") is exercised at the WRITER level only — the writer
  does its own virtual-station discovery regardless of the station-list cache. The real cached
  station-list handoff (`preprocessing_runoff.py:344`) was verified live in P3's operational run, not
  by a unit test.
  (iii) Test results: `preprocessing_runoff` 557 passed / 2 skipped — the 2 skips are the pre-existing
  unconditional placeholders tracked as **PREPQ-017** (`test_src.py`), an accepted pre-existing
  exception to the zero-skip gate, not introduced by this change. Full `run_tests.sh` on the final
  code: all 16 suites passed, 0 failures.

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

## P3 results (kyg, 2026-09-25)

### First run, commits `62d5d454`+`021889e8` (pre-exclusion build)

Counts only, no station codes, from the maintenance run on the build BEFORE the D-A collision
exclusion (above) was added:

- **Long-horizon**: `total_attempted=62 written=55 norm_absent=7 norm_absent_via_404=0 sdk_failed=0
  api_failed=0` (was `53/5/4/0` before the virtual-station retry).
- **Short-horizon pentad**: `pentad_written=11 pentad_norm_absent=50 pentad_sdk_failed=0
  pentad_api_failed=1`.
- **Short-horizon decade**: `decade_written=58 decade_norm_absent=3 decade_sdk_failed=0
  decade_api_failed=1`.
- The `api_failed=1` station (both horizons) is a virtual station with no daily runoff in the DB
  (`_ShortHorizonDailyReadError`, pre-existing, same error appears in trunk logs from 2026-09-10) and
  no norms in iEH HF.
- API read-back: 3 of 4 work-list virtual stations have stored norms equal to the SDK values for
  every horizon where iEH HF has norms (month 2, pentad 1, decade 3).
- 5 sampled regular stations' stored month/pentad/decade norms are unchanged before vs after the run.

### Final build (`45b2b8c8`; `fef7aa5f` is test-only)

Counts only, no station codes. Evidence logs are local, not committed.

**Operational run** (default mode, 62 sites loaded from the station cache): short-horizon
`pentad_written=11 pentad_norm_absent=50 pentad_sdk_failed=0 pentad_api_failed=1`,
`decade_written=58 decade_norm_absent=3 decade_sdk_failed=0 decade_api_failed=1` — identical to the
first run above; module passed. `run_locally.sh` exited 1 only because the follow-up
`api_validation` step found no `postprocessing_forecasts` venv in this fresh worktree (an environment
gap in this checkout, not a code defect from this change).

**Maintenance run**: long-horizon `62/55/7/0/0/0` (`total_attempted/written/norm_absent/
norm_absent_via_404/sdk_failed/api_failed`); short-horizon identical to the operational-run counts
above; exit 0. Stored norms of the 11 sampled stations (5 regular, 6 virtual) are identical to the
pre-exclusion run above. No collision warning was logged (kyg has 0 collisions, as measured in
Evidence).

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
