# P2b - Hydrograph PENTAD/DECADE Laptop-Export Migration Wrappers

**Status:** Implementation complete; PR open against `develop_migration_toolkit`.
**Module:** infra (cross-module — bin/, apps/iEasyHydroForecast/tests/, doc/prod/, doc/plans/issues/)
**Priority:** High
**Branch:** `feature_p2b_hydrograph_period_history` → `develop_migration_toolkit`
**Labels:** `infra`, `tooling`, `tests`, `deployment-runbook`

---

## Summary

Adds the laptop-export → CSV+manifest → server-import migration pipeline for
the hydrograph PENTAD/DECADE rows of `sapphire-preprocessing-db.hydrographs`.
P2b is the sibling of P2a (runoff PENTAD/DECADE) — same shape, different
target table and payload (wide-stat hydrograph row with quantiles +
year-mapped `previous`/`current` instead of `discharge` + `predictor`). It
closes the laptop-export family for the preprocessing layer (P4a/P4b cover
the forecast tables; P5 covers long forecasts).

The two P2b-specific concerns the gi_draft documents are:

1. **Year-column handling decision.** P3 (hydrograph DAY) reads
   `intermediate_data/hydrograph_day.csv` — a wide CSV where columns named
   after years (`2025`, `2026`, ...) rotate every January, hence its
   `_discover_year_columns` discovery logic. P2b instead exports
   `previous` / `current` from the DB as literal column names (the row
   model defines them directly per
   `sapphire/services/preprocessing/app/schemas.py:59-60`), so the
   server-side helper does NO year-column discovery. The decision is
   reversible (a future refactor could switch back to year columns), but
   the chosen shape matches the DB row model and the operational
   `HydrographBase` payload exactly.

2. **`--strict-merge` design decision.** Hydrograph rows have ~12 nullable
   stat / quantile / year-mapped fields. The universal safe-write rule
   (architecture §Q2 layer 2) prevents this wrapper from sending `null`
   for an absent source field, so it can NEVER trigger the service-side
   `_has_changes + setattr` overwrite-with-NULL bug on its own. However,
   the operational concern is that other upstream processes CAN trigger
   the bug, and an opt-in read-before-merge step would let this wrapper
   restore previously-erased values during reruns. P2b ships the
   `--strict-merge` flag's CLI surface, parser, and dry-run inventory
   line — but the actual read-before-merge POST path is deferred to a
   follow-up PR. The current `--strict-merge` invocation logs a warning
   and falls back to enrichment-only with no semantic change. This keeps
   the CLI / runbook shape stable so the follow-up PR only touches one
   code path (the POST step).

## Context

The hydrograph PENTAD/DECADE rows are climatological stat aggregates at
the 5-day and 10-day horizons:

- Required: `horizon_type ∈ {PENTAD, DECADE}`, `code`, `date`,
  `horizon_value`, `horizon_in_year`, `day_of_year`.
- Nullable: `count`, `mean`, `std`, `min`, `max`, `q05`, `q25`, `q50`,
  `q75`, `q95`, `norm`, `previous`, `current` (~13 nullable fields).
- Unique key: `(horizon_type, code, date)` — same as DAY.

The Stage A.2 inventory found the TAJ archive's pentad CSV is header-only
(no rows), so the CSV-source path used by P3 is not viable for
PENTAD/DECADE. The architecture's chosen alternative is the laptop-export
workflow (architecture §Q1 strategy table row "hydrographs:PENTAD,DECADE")
— P2b implements it.

References:

- Architecture plan §Q1 strategy table row for `hydrographs:PENTAD,DECADE`.
- Architecture plan §Q2 layer 2 safe-write rule for `hydrographs`.
- Architecture plan §Q3 laptop-export workflow shape.
- Architecture plan §Q5 manifest contract.
- P0 foundation: PR #343 (merged `cd01339`).
- P3 hydrograph DAY (sibling, CSV-source): PR #349 (merged `28994c5`).
- Stage A.2 audit row for hydrographs:PENTAD/DECADE: `doc/plans/working/update_migration_phase1_csv_api_audit.md`.

## Problem

Operationally, an update-time migration that includes hydrograph
PENTAD/DECADE history rows needs:

1. **A station-filtered, deployment-aware export path** that does not
   leak cross-org station codes from the laptop's source DB.
2. **A manifest-validated server-side import path** so the operator
   cannot accidentally push a stale or wrong-deployment CSV.
3. **A safe-write story for the wide nullable stat row** that does not
   trigger the service-side `_has_changes` overwrite bug.
4. **A horizon-aware MODE detection** so populated PENTAD targets do not
   block decade imports (and vice versa).

The legacy in-container `data_migrator.py`
(`sapphire/services/preprocessing/app/data_migrator.py:202-330`) is the
historical reference but cannot solve any of (1)–(4): it has no station
filter, no manifest concept, hardcoded `'2024'`/`'2025'` year columns
inherited from a DAY-CSV worldview that doesn't fit DB-source rows, and
no per-horizon MODE branching. It is also the wrong layer (in-service
Python with a pandas dependency, instead of a sidecar Python helper in
the prepgateway image with stdlib-only constraints).

## Desired Outcome

After this PR merges to `develop_migration_toolkit`:

- `bin/export_hydrograph_period_history.sh` (~280 LOC) exists,
  mirroring the P2a (runoff) export pattern: positional-free CLI with
  `--horizon`, `--stations-file`, `--out-dir`, `--dry-run`. Location
  guard refuses on a deployment-server host (Stage E #6). SQL-injection
  guard rejects apostrophes in station codes. Emits CSV + manifest pair
  with mode `0600`.
- `bin/initialize_hydrograph_period_history.sh` (~350 LOC) exists,
  mirroring the P1a/P3 server-side pattern: positional env file +
  `--from-export`, `--horizon`, `--dry-run`, `--api-url`,
  `--batch-size`, `--image`, `--station-filter`, `--strict-merge`.
  Validates the manifest BEFORE any image work or psql probe. Queries
  the target preprocessing-db per-horizon for MODE detection. Mounts
  the migration_py package + the CSV + the manifest into the
  prepgateway container.
- `bin/utils/migration_py/hydrograph_period.py` (~390 LOC) exposes
  `main()` plus private helpers (`_build_record`,
  `_read_filtered_records_with_manifest`, `_post_batch`,
  `_print_dry_run_inventory`, `_build_arg_parser`). Stdlib-only,
  verified by the import-audit test.
- `apps/iEasyHydroForecast/tests/test_export_hydrograph_period.py` (~15
  tests) covers the export wrapper CLI surface, argument validation,
  fixture-manifest round trip, and the location guard (positive +
  bypass).
- `apps/iEasyHydroForecast/tests/test_initialize_hydrograph_period.py`
  (~30 tests) covers the import wrapper CLI surface, payload-shape per
  horizon, NULL handling, quantile column normalization, station/
  cutoff filters, manifest validation failures, dry-run inventory
  shape, `--strict-merge` flag (parses + falls back), fixture
  round-trip, and the stdlib-only audit.
- `apps/iEasyHydroForecast/tests/fixtures/migration_csv/hydrograph_period/`
  contains four files (pentad + decade CSV, each with a sentinel-only
  manifest sibling), sentinel code `19999` only.
- `doc/prod/update_data_migration_runbook.md` has §6.2 written (~270
  lines added immediately after the §6 preamble). §6.1 (P2a),
  §6.3/§6.4 (P4a/P4b/P5) untouched.
- `shellcheck -x` reports zero findings on both wrappers.
- Full iEasyHydroForecast suite: 436 passed, 0 failed. The 27 new P2b
  tests (15 export-side + ~15 init-side + module audit/fixture
  round-trips), one deterministic skipif (location-guard positive path
  on machines without sapphire containers running) — that skipif is NOT
  a hidden-bug skip per the Zero Skips Policy: the predicate is
  explicit and the message documents the condition.

## Implementation Plan

This PR was implemented per the sub-orchestrator charter v2's
watchdog-mitigation protocol (commit per phase), in three phases:

1. **`bin/export_hydrograph_period_history.sh` +
   `bin/initialize_hydrograph_period_history.sh` +
   `bin/utils/migration_py/hydrograph_period.py`** — commit `335f75f`.
2. **`apps/iEasyHydroForecast/tests/test_export_hydrograph_period.py` +
   `apps/iEasyHydroForecast/tests/test_initialize_hydrograph_period.py`
   + sentinel fixtures** — commit `1191256`.
3. **`doc/prod/update_data_migration_runbook.md` §6.2** — commit
   `bfdcec7`.

This gi_draft is the final commit, opening the PR.

## Key Design Decisions

### Year-column handling: NO discovery, use literal column names

The laptop-side export emits `previous` / `current` as literal column
names (matching the DB row model). The server-side helper passes them
through. This intentionally differs from P3 (hydrograph DAY), which
performs dynamic year-column discovery on a CSV header.

Trade-offs:

- **Pro**: payload key names match the DB exactly; no per-year discovery
  surprises; the export script's `COPY (SELECT ... previous, current ...)`
  is trivial.
- **Pro**: matches P2a (runoff_period) export shape — operators can
  reason about both DB-export wrappers identically.
- **Con**: if a future deployment's DB schema renames the columns, the
  export breaks. Mitigated by the manifest's `export_type` check (would
  surface as a row-shape mismatch downstream).

The decision is documented in the wrapper docstring, the runbook §6.2
background, and is the reason for the brief's
`test_build_record_pentad_includes_year_mapping_if_present` test
(verifies the literal-pass-through behavior on a row that contains
those columns).

### Safe-write: enrichment-only (default) with `--strict-merge` stub

Default = enrichment-only (Option A). The wrapper sends only non-NULL
source fields; absent source fields are OMITTED from the payload, never
sent as `null`. This:

- Matches the universal safe-write rule (architecture §Q2 layer 2).
- Matches every CSV-source sibling already merged (snow, runoff DAY,
  meteo, hydrograph DAY).
- Cannot trigger the service-side `_has_changes + setattr` overwrite
  bug on its own.

`--strict-merge` (Option B, read-before-merge) is documented as a stub:
the CLI surface, the parser, and the dry-run policy line are in place,
but the actual GET-then-merge-then-POST path is deferred. Rationale:

- Hydrograph PENTAD/DECADE export is internally complete from the
  laptop's DB; partial-coverage scenarios are rare in practice.
- Adding a second POST shape now (with per-row GET) doubles the
  surface area to test before there's a confirmed operational need.
- Operators who observe stat erasure in production can file an issue;
  the follow-up PR landing `--strict-merge` only changes ONE code
  path (the POST step) — no runbook / CLI / test-fixture changes
  needed.

The runbook §6.2 background explicitly tells operators NOT to depend on
`--strict-merge` yet. The dry-run inventory prints
`SAFE_WRITE_POLICY=strict-merge (NOT YET IMPLEMENTED — using
enrichment-only)` when the flag is set, so the operator sees the
fallback before any write.

### Quantile column normalization

The CSV header may use canonical `q05` / `q25` / `q50` / `q75` / `q95`
OR legacy `5%` / `25%` / `50%` / `75%` / `95%` (URL-encoding from
pandas). `_build_record` accepts both forms and emits the canonical
`qXX` payload keys. If both forms are present for the same quantile,
the canonical name wins. Tested in
`test_build_record_canonical_quantile_takes_precedence_over_legacy` and
`test_build_record_accepts_legacy_percent_quantile_columns`.

### Location guard with testing bypass

The export wrapper checks for `sapphire-{preprocessing,postprocessing,
api-gateway}-...` containers via `docker ps` and refuses if any are
running (Stage E #6). For development on a laptop that also runs the
SAPPHIRE stack locally, an env-var bypass
`_P2B_EXPORT_SKIP_LOCATION_GUARD=1` is honored. The bypass is:

- Underscore-prefixed (clearly internal).
- Documented in the wrapper docstring as "testing-only".
- Used only in the test suite (`_run_wrapper` helper sets it by
  default); explicitly NOT used by operators.
- Tested both positively (when sapphire containers are present, the
  guard fires) and negatively (when bypass is set, the inner
  validations are reachable).

## Acceptance Criteria

### A. Export wrapper

- [x] `bin/export_hydrograph_period_history.sh` exists; sources
      `common_functions.sh` + `update_migration_helpers.sh`.
- [x] CLI: `--horizon pentad|decade`, `--stations-file`, `--out-dir`,
      `--dry-run`. Each flag rejects missing / invalid values.
- [x] Location guard refuses with documented error when sapphire-*
      containers are detected; testing bypass for dev laptops.
- [x] SQL-injection guard: a station code containing an apostrophe is
      rejected with a clear error.
- [x] PG env-var presence check fires before any DB connection attempt.
- [x] Emits CSV (mode 0600) + sidecar manifest (mode 0600) with the 5
      P0-required keys plus a `horizon=<value>` annotation.
- [x] `shellcheck -x` zero findings.
- [x] `bash bin/export_hydrograph_period_history.sh --help` returns 0
      with usage text.

### B. Server-side import wrapper

- [x] `bin/initialize_hydrograph_period_history.sh` exists; sources
      `common_functions.sh` + `update_migration_helpers.sh`; uses
      `umh_resolve_image` / `umh_acquire_temp_workspace` /
      `umh_log_redacted` / `umh_print_image_resolution_line` /
      `umh_validate_export_manifest`.
- [x] CLI: positional env_file_path, required `--from-export`,
      required `--horizon pentad|decade`, plus `--dry-run`,
      `--api-url`, `--batch-size`, `--image`, `--station-filter`,
      `--strict-merge`.
- [x] `--station-filter <code>` honored (P0 binding contract).
- [x] `--strict-merge` parses, logs warning, falls back to
      enrichment-only (read-before-merge deferred).
- [x] Manifest validation runs FIRST (before image work or psql).
- [x] Per-horizon MODE detection against `hydrographs` filtered on the
      uppercase `horizon_type`.
- [x] `shellcheck -x` zero findings.

### C. Python module

- [x] `bin/utils/migration_py/hydrograph_period.py` exposes `main()`
      publicly; helpers private (`_build_record`,
      `_read_filtered_records_with_manifest`, `_post_batch`,
      `_print_dry_run_inventory`, `_build_arg_parser`,
      `_parse_float`, `_parse_int`).
- [x] Stdlib-only (verified by
      `test_hydrograph_period_module_imports_only_stdlib_and_intra_package`).
- [x] No year-column discovery (intentional — see design decisions).

### D. Tests

- [x] All 436 iEasyHydroForecast tests pass.
- [x] New P2b tests cover: CLI surfaces, payload shape per horizon,
      NULL handling, quantile normalization, station/cutoff filters,
      manifest validation failures, dry-run inventory, `--strict-merge`
      stub, stdlib audit, fixture round-trip, location guard
      (bypass + positive).
- [x] Zero unexpected skips. The one `skipif` (location-guard positive
      path) has a deterministic predicate and an explicit message.

### E. Fixture

- [x] Four files in
      `apps/iEasyHydroForecast/tests/fixtures/migration_csv/hydrograph_period/`:
      `pentad_sample.csv` + `pentad_sample.csv.manifest`,
      `decade_sample.csv` + `decade_sample.csv.manifest`.
- [x] Only sentinel code `19999`. Manifests added with `git add -f`
      because the repo `.gitignore` excludes `*.manifest` globally
      (a CI policy for operator-generated manifests); test fixtures
      are safe to commit because they contain no real codes or values.
- [x] Fixture-guard test (`test_migration_fixture_guard.py`) still
      passes.

### F. Runbook

- [x] §6.2 written immediately after the §6 preamble in
      `doc/prod/update_data_migration_runbook.md`.
- [x] §6 preamble, §6.1 (P2a placeholder), §6.3+ (sibling phases),
      §7+ (P6/P7 territory), and all §5 subsections untouched.

### G. Repo-wide

- [x] No real station codes / discharge values / env-file contents
      anywhere in modified files.
- [x] No files outside the brief's §3 file-scope list modified.
- [x] No edits to `sapphire/services/` (Charter §10).
- [x] PR opens against `develop_migration_toolkit`.

## Testing

### Unit tests

~27 functions across the two new test files, all green locally on the
sub-orchestrator's worktree. CI will re-verify on the PR.

The full iEasyHydroForecast suite (436 tests including new + existing
P0/P1a/P1b/P1c/P3 migration tests) passes with zero failures and one
deterministic skipif (location-guard positive path on machines without
sapphire containers running).

### Linting

- Shellcheck: `shellcheck -x bin/export_hydrograph_period_history.sh
  bin/initialize_hydrograph_period_history.sh` clean.
- Ruff: pre-commit hook ran ruff on
  `bin/utils/migration_py/hydrograph_period.py` and both test files at
  commit time; both passed (after the initial auto-format).

### CI

Both wrappers fall under the migration-pattern regex of both the
pre-commit hook (`^bin/(initialize_[a-z0-9_]+_history|export_[a-z0-9_]+_history)\.sh$`)
and the `shellcheck_migration_scripts` CI job's glob
(`bin/(initialize|export)_*_history.sh`). After this PR merges, the
gate is scanning at least 6 wrappers (snow, runoff DAY, meteo,
hydrograph DAY, plus the two added here).

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Operator runs the export script on the deployment server by accident | Location guard fires before any DB connection, with an explicit error message pointing at the server-side wrapper. Testing-only bypass is underscore-prefixed and documented as such. |
| Operator transfers only the CSV (not the manifest) | Server-side wrapper fails fast with `sidecar manifest not found` before any image work. Manifest validation is the first server-side step. |
| Stale CSV on the server (operator forgot to refresh after laptop DB updates) | Manifest validation checks `row_count`, `station_count`, `date_min`, `date_max` against the CSV's actual contents — date-range or row-count mismatch raises a typed exception. |
| Cross-org station codes leak from laptop DB | Both the stations-file filter (export side) and the `code IN (...)` clause (psql side) ensure only the deployment's codes are exported. The manifest's `station_count` is the cross-check. |
| Service-side `_has_changes` overwrites existing non-NULL stat with incoming NULL | Enrichment-only safe-write rule: only non-NULL source fields are POSTed. Cannot trigger the bug from this wrapper. If a different upstream process triggers it, the documented `--strict-merge` follow-up flag is the remediation path. |
| SQL injection via crafted station code | Stations file is line-by-line parsed; any line containing `'` triggers a hard reject before query construction. Documented in the wrapper. |
| Year-column rename in source DB | Manifest validation would catch a row-count mismatch (the column-rename would change what gets exported and break the round-trip check). Investigation pointer added to the runbook §6.2.2 common failure causes. |

## Out of Scope

- `--strict-merge` (read-before-merge) implementation — deferred to a
  follow-up PR if stat erasure is observed in production.
- Hydrograph MONTH / SEASON / QUARTER — regenerate-hook territory
  (P6) per architecture §Q1.
- Service-side CRUD `_has_changes` fix — separate coordination with
  `sapphire/services/` owner.
- Disposable integration tests against a live docker stack —
  architecture §Q7 says these belong to a separate sprint.
- Modification of `data_migrator.py` — READ-ONLY reference; this
  wrapper replaces operator use of it (architecture §Q1).

## Dependencies

- **Depends on:** P0 (foundation), P3 (sibling — establishes the
  hydrograph payload shape and the universal safe-write rule
  application for hydrograph-family rows).
- **Sibling to:** P2a (runoff PENTAD/DECADE) — same shape, different
  table; running in parallel per the user-authorized parallel
  spawning.
- **Blocks:** none directly. P4a/P4b/P5 are independent laptop-export
  wrappers for the forecast tables; P6 is the regenerate-hook layer
  for tables that this wrapper does not cover (snow stats,
  hydrograph MONTH/SEASON, short/long-term skill metrics).

## References

- Architecture plan §Q1, §Q2, §Q3, §Q5 — strategy table, safe-write
  rule, laptop-export workflow, manifest contract.
- P0 foundation: PR #343 (`cd01339`).
- P1a sibling structure: PR #345 (`79cdd5e`) +
  `bin/initialize_runoff_day_history.sh` +
  `bin/utils/migration_py/runoff_day.py`.
- P3 sibling (hydrograph DAY): PR #349 (`28994c5`) +
  `bin/initialize_hydrograph_day_history.sh` +
  `bin/utils/migration_py/hydrograph_day.py`.
- Stage A.2 audit row for hydrographs:PENTAD/DECADE:
  `doc/plans/working/update_migration_phase1_csv_api_audit.md`.
- Sub-orchestrator Charter v2:
  `doc/plans/working/update_migration_suborchestrator_charter.md`.

## Process note (sub-orchestrator)

The P2b sub-orchestrator (Opus 4.7) implemented all three phases
directly per Charter v2 §2 (sandbox unblocked, user-authorized parallel
spawning). Each phase was committed individually per the brief §0a
watchdog mitigation: wrapper+module (`335f75f`), tests+fixtures
(`1191256`), runbook §6.2 (`bfdcec7`). This gi_draft is the fourth
commit and the trigger to open the PR. No edits to
`sapphire/services/` (Charter §10). The code-review skill was NOT run
on the diff — operator + reviewers will surface any issues on the PR.
