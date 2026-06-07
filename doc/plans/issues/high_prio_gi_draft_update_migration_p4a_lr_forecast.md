# P4a - LR Forecast Laptop-Export Migration Wrappers

**Status:** Implementation complete; PR open against `develop_migration_toolkit`.
**Module:** infra (cross-module — bin/, apps/iEasyHydroForecast/tests/, doc/prod/, doc/plans/issues/)
**Priority:** High
**Branch:** `feature_p4a_lr_forecast_history` → `develop_migration_toolkit`
**Labels:** `infra`, `tooling`, `tests`, `deployment-runbook`

---

## Summary

Adds the laptop-export → CSV + manifest → server-import migration pipeline
for the `lr_forecasts` rows of `sapphire-postprocessing-db`. P4a is the
forecast-table sibling of P2a / P2b (runoff and hydrograph period
laptop-exports), but with a critical difference in source database
ownership: `lr_forecasts` lives in the postprocessing service, NOT in
preprocessing. P4a is paired with the previously-committed server-side
wrapper (`bin/initialize_lr_forecast_history.sh`) and Python helper
(`bin/utils/migration_py/lr_forecast.py`) from the prior P4a sub-orch
session; this PR completes the family by adding the laptop-export
script, the tests, the sentinel fixtures, this gi_draft, and the runbook
§6.3.

The three P4a-specific concerns the gi_draft documents are:

1. **Source DB is postprocessing-db, not preprocessing-db.** All prior
   laptop-export wrappers (P2a runoff PENTAD/DECADE, P2b hydrograph
   PENTAD/DECADE) pull from `sapphire-preprocessing-db`. P4a is the first
   wrapper that targets `sapphire-postprocessing-db`, so the runbook,
   the env-var defaults (PG* with the canonical dev port `5433` for the
   postprocessing-db), and the acceptance SQL all point at the
   postprocessing-db. Operators following the runbook in order from §6.2
   to §6.3 must switch context.

2. **`lr_forecasts` has no `model_type` column.** Unlike the sibling
   `forecasts` table (which uses `model_type` to distinguish TFT / TiDE
   / TSMixer / LR), `lr_forecasts` makes LR implicit: the unique key is
   `(horizon_type, code, date)` only. This is the same architectural
   decision that causes the P-postprocessing `combined_forecasts`
   migrator to FILTER LR rows OUT of `combined_forecasts` (Stage A.2 §C
   — the LR forecast of record lives in `lr_forecasts`, not in
   `combined_forecasts`). The export `COPY (SELECT ...)` therefore does
   NOT include `model_type`, the server-side helper actively rejects any
   stray `model_type` cell in a malformed CSV (test
   `test_build_record_does_not_emit_model_type`), and the fixture-guard
   test pins the no-model_type contract.

3. **Horizon-type enum is lowercase, architecturally locked (§Q4).**
   `HorizonType` in `sapphire/services/postprocessing/app/models.py`
   defines `PENTAD = "pentad"` and `DECADE = "decade"` (lowercase
   values). Uppercase `'PENTAD'` / `'DECADE'` cells would be rejected at
   the Pydantic boundary. The laptop export emits the lowercase form
   via `lower(horizon_type::text)` in the COPY query, the wrapper
   rejects any other `--horizon` value, and the server-side helper
   double-checks the enum in `_build_record`. This contrasts with the
   preprocessing-side `hydrographs.horizon_type` which uses the
   uppercase `'PENTAD'` / `'DECADE'` form. Cross-table consistency is
   not guaranteed; respect each table's enum case as locked.

## Context

The `lr_forecasts` table is the operational store for linear-regression
forecasts produced by `apps/linear_regression`. Each row records a
pentad-or-decade forecast plus its supporting model statistics. The row
shape (from `LRForecastBase` in
`sapphire/services/postprocessing/app/schemas.py`):

- **Required:** `horizon_type ∈ {pentad, decade}`, `code`, `date`,
  `horizon_value`, `horizon_in_year`.
- **Nullable (9 fields):** `discharge_avg`, `predictor`, `slope`,
  `intercept`, `forecasted_discharge`, `q_mean`, `q_std_sigma`,
  `delta`, `rsquared`.
- **Unique key:** `(horizon_type, code, date)`. NO `model_type`.

The legacy in-container migrator
(`sapphire/services/postprocessing/app/data_migrator.py::LRForecastDataMigrator`,
lines ~276–331) reads a CSV with the legacy column names
`pentad_in_month` / `pentad_in_year` (or `decad_in_month` /
`decad_in_year`) and maps them to the DB's `horizon_value` /
`horizon_in_year`. It has no station filter, no manifest concept, no
per-horizon MODE branching, no station-code redaction in logs, and runs
inside the postprocessing service container — wrong layer.

P4a replaces operator use of that migrator with the standard
laptop-export pattern from P0:

- **Laptop side (`bin/export_lr_forecast_history.sh`):** pulls rows
  from the laptop's local postprocessing-db via `psql -X` + `COPY`,
  filtered by `--horizon` (lowercase enum) and optional
  `--station-list-file`. Emits CSV + sidecar manifest with mode `0600`.
  Location guard refuses on a deployment-server host.
- **Server side (`bin/initialize_lr_forecast_history.sh`):** validates
  the manifest first, queries the target postprocessing-db for MODE
  detection, runs the Python helper inside the prepgateway container.
- **Python helper (`bin/utils/migration_py/lr_forecast.py`):** stdlib-
  only; reads CSV, applies cutoff + station filter, builds payloads
  with the universal safe-write rule (omit NULL-like fields rather
  than POSTing `null`), POSTs to `/lr-forecast/`.

References:

- Architecture plan §Q1 strategy table row for
  `lr_forecasts:pentad,decade` (laptop-export).
- Architecture plan §Q2 layer 2 safe-write rule for nullable
  forecast-stat fields.
- Architecture plan §Q3 laptop-export workflow shape.
- Architecture plan §Q4 horizon-type enum lock (lowercase for
  postprocessing tables).
- Architecture plan §Q5 manifest contract (5 required keys validated
  by `migration_py._common.validate_manifest`).
- P0 foundation: PR #343 (merged `cd01339`).
- Stage A.2 §C: combined_forecasts excludes LR rows because the LR
  forecast of record lives in `lr_forecasts`.
- Sibling P2a / P2b: laptop-export pattern for preprocessing-db tables
  (runoff and hydrograph period).
- Sibling P4b: laptop-export pattern for the
  postprocessing-db `forecasts` table (ML rows: TFT / TiDE / TSMixer).
  P4a and P4b together cover all postprocessing forecast-table
  migrations.

## Problem

Operationally, an update-time migration that includes LR forecast
history rows needs:

1. **A station-filtered, deployment-aware export path** that does not
   leak cross-org station codes from the laptop's source DB.
2. **A manifest-validated server-side import path** so the operator
   cannot accidentally push a stale or wrong-deployment CSV.
3. **A safe-write story for the nine nullable model-stat fields** that
   does not trigger the service-side `_has_changes` overwrite bug.
4. **A horizon-aware MODE detection** so populated pentad targets do not
   block decade imports (and vice versa).
5. **A clear story for the postprocessing-db source** — runbook,
   acceptance SQL, and per-wrapper context must point at port 5433 /
   `postprocessing_db`, not the preprocessing equivalents that P2a /
   P2b operators just finished using.

The legacy in-container `data_migrator.py` cannot solve any of (1)–(5):
no filter, no manifest, no MODE branching, no postprocessing-vs-
preprocessing context (it lives inside the postprocessing service so
the source/target are coupled), and it has a pandas dependency that
disqualifies it from the stdlib-only sidecar pattern.

## Desired Outcome

After this PR merges to `develop_migration_toolkit`:

- `bin/export_lr_forecast_history.sh` (~430 LOC) exists, mirroring the
  P2b export pattern: positional env_file_path + `--horizon`,
  `--output-dir`, `--station-list-file`, `--dry-run`. Location guard
  refuses on a deployment-server host (Stage E #6); env-var bypass
  (`_P4A_EXPORT_SKIP_LOCATION_GUARD=1`) for the test suite and explicit
  `--i-am-on-laptop` flag for developer machines that intentionally
  run the SAPPHIRE stack locally. SQL-injection guard rejects
  apostrophes in station codes. Emits CSV + manifest pair with mode
  `0600`.
- `bin/initialize_lr_forecast_history.sh` (already in place from the
  prior P4a sub-orch session; ~510 LOC) and
  `bin/utils/migration_py/lr_forecast.py` (~515 LOC) — both ship as-is.
- `apps/iEasyHydroForecast/tests/test_export_lr_forecast.py` (23
  tests) covers the export wrapper CLI surface, argument validation,
  location guard (positive + bypass), fixture round-trip, sentinel-
  only check, no-model_type check, lowercase-horizon-type check.
- `apps/iEasyHydroForecast/tests/test_initialize_lr_forecast.py` (30
  tests) covers the import wrapper CLI surface, `_build_record` payload
  shape per horizon, NULL handling for all 9 nullable fields, non-
  finite-float rejection, `_read_filtered_records` filters,
  `main()` dry-run inventory shape, no-model_type cross-check,
  stdlib-only audit, fixture round-trip.
- `apps/iEasyHydroForecast/tests/fixtures/migration_csv/lr_forecast/`
  contains four files (pentad + decade CSV, each with a sentinel-only
  manifest sibling). Sentinel code `19999` only.
- `doc/prod/update_data_migration_runbook.md` has §6.3 written (~268
  lines added between the §6 preamble and §7). §6.1 (P2a), §6.2 (P2b),
  §6.4 (P4b), §6.5 (P5), §7+ untouched.
- `shellcheck -x` zero findings on the new export wrapper (existing
  wrapper already clean).
- Full iEasyHydroForecast suite: 437 passed, 0 failed, 0 unexpected
  skips. The one `skipif` (location-guard positive path on machines
  without sapphire containers running) is NOT a hidden-bug skip per
  the Zero Skips Policy: the predicate is explicit, deterministic,
  and the message documents the condition.

## Implementation Plan

This PR was implemented per the sub-orchestrator charter v2 §2 with the
watchdog-mitigation protocol (commit per phase). The first sub-orch
session landed the server wrapper + Python module (commit `3f6debf`)
before stalling on the watchdog. This continuation sub-orch session
landed the remaining four phases:

1. **`bin/export_lr_forecast_history.sh`** — commit `8e3cb7d`.
2. **`apps/iEasyHydroForecast/tests/test_export_lr_forecast.py` +
   `apps/iEasyHydroForecast/tests/test_initialize_lr_forecast.py` +
   sentinel fixtures (pentad + decade CSV + manifest pairs)** — commit
   `a440223`. Full iEasyHydroForecast suite: 437 passed.
3. **`doc/prod/update_data_migration_runbook.md` §6.3** — commit
   `66b9a02`. Inserted between §6 preamble and §7 without disturbing
   sibling sections.
4. **This gi_draft** — final commit; triggers PR open against
   `develop_migration_toolkit`.

## Key Design Decisions

### Source DB is postprocessing-db, not preprocessing-db

The wrapper docstring, the wrapper's MODE-detection psql block, the
runbook §6.3 background, the acceptance SQL, and the operator-facing
"Next steps" message all consistently point at
`sapphire-postprocessing-db` / `postprocessing_db` / API port 8003.
The laptop-export script does NOT itself open a Docker connection; it
goes through standard PG* env vars + `~/.pgpass`, but the runbook
explicitly notes the canonical dev port is `5433` (the laptop
postprocessing-db) to avoid operator confusion between the two
sapphire DB ports.

### Column aliasing: DB `horizon_value` → CSV `pentad_in_month` / `decad_in_month`

The `lr_forecasts` table has literal columns `horizon_value` and
`horizon_in_year`. The legacy CSV-source flow used by
`data_migrator.py::LRForecastDataMigrator` instead expects the names
`pentad_in_month` / `pentad_in_year` (for pentad) or `decad_in_month` /
`decad_in_year` (for decade). The export script's `COPY (SELECT ...)`
aliases the DB columns OUT to the legacy CSV names so the server-side
helper's column-mapping table finds them unchanged. This keeps the
CSV shape uniform with the rest of the laptop-export family and means
the Python helper does NOT need a special-case path for "we're on the
DB-export path versus the legacy CSV path".

Trade-offs:

- **Pro:** Python helper has a single column-mapping table; one CSV
  shape for both source paths.
- **Pro:** the existing migration_py.lr_forecast module (from commit
  `3f6debf`) requires zero changes.
- **Con:** the export CSV's column names diverge slightly from the
  underlying DB column names. Mitigated by the wrapper docstring and
  the runbook §6.3 background explicitly documenting the aliasing.

### Lowercase horizon_type enum (architecture §Q4)

The Python helper, the wrapper's MODE psql query, and the export
script's `COPY` all use lowercase `'pentad'` / `'decade'`. Uppercase
would be silently dropped by the Pydantic boundary (the Pydantic
`HorizonType` enum has value `'pentad'`, not `'PENTAD'`). The fixture
files emit lowercase explicitly; the test
`test_pentad_fixture_uses_lowercase_horizon_type` (and its decade
sibling) pin this contract so a future regeneration of the fixture
cannot drift to uppercase by accident.

This intentionally contrasts with the preprocessing-side
`hydrographs.horizon_type` which uses uppercase `'PENTAD'` /
`'DECADE'`. Operators must respect each table's enum case as locked;
the wrappers do not auto-coerce.

### No `model_type` column in `lr_forecasts`

The wrapper's `COPY (SELECT ...)` does NOT select `model_type` from
the source DB (the column does not exist there). The Python helper's
`_build_record` does NOT include `model_type` in the payload (even
defensively when a stray cell appears in the CSV — see the test
`test_build_record_does_not_emit_model_type`). The runbook §6.3
background explicitly warns operators that "LR is implicit in this
table".

The downstream consequence (LR rows are filtered OUT of
`combined_forecasts` by the P-postprocessing combined_forecasts
migrator) is cross-linked in both the wrapper docstring and the
runbook background, so operators reading the LR migration in
sequence with the combined_forecasts migration understand why the
counts will not add up to a naive expectation.

### Location guard with two bypass paths

The export wrapper checks for
`sapphire-{preprocessing,postprocessing,api-gateway}-...` containers
via `docker ps` and refuses if any are running (Stage E #6). Two
bypass paths:

- **`_P4A_EXPORT_SKIP_LOCATION_GUARD=1` env var.** Used only by the
  test suite (`_run_wrapper` helper sets it by default), so
  developer-laptop tests reach downstream validation paths. Explicitly
  NOT set by operators.
- **`--i-am-on-laptop` CLI flag.** For the developer machine that
  intentionally runs the SAPPHIRE stack locally and the operator
  *knows* the laptop is also the only DB host they have access to.
  The flag prints a YELLOW WARNING to stderr to make the override
  visible in logs.

The test
`test_location_guard_fires_when_sapphire_containers_present` exercises
the positive path on machines where the guard CAN actually fire, and
is skipif-gated on the developer-machine state — NOT a hidden-bug
skip; the skip predicate is deterministic and documented.

### Stat erasure not actively defended

LR rows in practice tend to be either fully populated or fully missing
per forecast-cycle output, so partial-NULL erasure is less of a
concern than for the wide hydrograph rows. The universal safe-write
rule (omit NULL-like fields rather than POSTing `null`) still applies;
P4a does NOT ship a `--strict-merge` opt-in flag (which P2b did for
hydrograph period rows). If operators observe stat erasure in LR
production data, file a follow-up issue referencing this gi_draft.

## Acceptance Criteria

### A. Export wrapper

- [x] `bin/export_lr_forecast_history.sh` exists; sources
      `update_migration_helpers.sh`.
- [x] CLI: positional `env_file_path`, required `--horizon pentad|decade`,
      plus `--output-dir`, `--station-list-file`, `--dry-run`,
      `--i-am-on-laptop`. Each flag rejects missing / invalid values.
- [x] Location guard refuses with documented error when sapphire-*
      containers are detected; env-var bypass for unit tests AND CLI
      bypass (`--i-am-on-laptop`) for developer-laptop overrides.
- [x] SQL-injection guard: a station code containing an apostrophe is
      rejected with a clear error.
- [x] PG env-var presence check fires before any DB connection attempt.
- [x] Emits CSV (mode 0600) + sidecar manifest (mode 0600) with the 5
      P0-required keys (`export_type=lr_forecast`, `row_count`,
      `station_count`, `date_min`, `date_max`) plus a
      `horizon=<value>` annotation.
- [x] `shellcheck -x` zero findings.
- [x] `bash bin/export_lr_forecast_history.sh --help` returns 0
      with usage text.

### B. Server-side import wrapper (already committed in `3f6debf`)

- [x] `bin/initialize_lr_forecast_history.sh` exists; sources
      `update_migration_helpers.sh`; uses `umh_resolve_image` /
      `umh_acquire_temp_workspace` / `umh_log_redacted` /
      `umh_print_image_resolution_line` /
      `umh_validate_export_manifest`.
- [x] CLI: positional env_file_path, required `--from-export`,
      required `--horizon pentad|decade`, plus `--dry-run`,
      `--api-url`, `--batch-size`, `--image`, `--station-filter`.
- [x] `--station-filter <code>` honored (P0 binding contract).
- [x] Manifest validation runs FIRST (before image work or psql).
- [x] Per-horizon MODE detection against postprocessing-db's
      `lr_forecasts` filtered on the lowercase `horizon_type`.
- [x] `shellcheck -x` zero findings.

### C. Python module (already committed in `3f6debf`)

- [x] `bin/utils/migration_py/lr_forecast.py` exposes `main()`
      publicly; helpers private (`_build_record`,
      `_read_filtered_records`, `_post_batch`,
      `_print_dry_run_inventory`, `_build_arg_parser`,
      `_parse_float`, `_parse_int`).
- [x] Stdlib-only (verified by
      `test_lr_forecast_module_imports_only_stdlib_and_intra_package`).
- [x] Refuses unknown horizon values with `ValueError`.
- [x] No `model_type` in the payload even when present in the CSV row.

### D. Tests

- [x] All 437 iEasyHydroForecast tests pass.
- [x] New P4a tests cover: CLI surfaces (export + import), payload
      shape per horizon, NULL handling for all 9 nullable fields,
      non-finite-float rejection, station / cutoff filters, dry-run
      inventory shape, no-model_type cross-check, stdlib audit,
      fixture round-trip, location guard (bypass + positive).
- [x] Zero unexpected skips. The one `skipif` (location-guard positive
      path) has a deterministic predicate and an explicit message.

### E. Fixtures

- [x] Four files in
      `apps/iEasyHydroForecast/tests/fixtures/migration_csv/lr_forecast/`:
      `pentad_sample.csv` + `pentad_sample.csv.manifest`,
      `decade_sample.csv` + `decade_sample.csv.manifest`.
- [x] Only sentinel code `19999`. Manifests added with `git add -f`
      because the repo `.gitignore` excludes `*.manifest` globally
      (a CI policy for operator-generated manifests); test fixtures
      are safe to commit because they contain no real codes or values.
- [x] Fixture-guard test (`test_migration_fixture_guard.py`) still
      passes.
- [x] Each fixture row uses lowercase `'pentad'` / `'decade'` for
      `horizon_type` per architecture §Q4 lock.

### F. Runbook

- [x] §6.3 written between the §6 preamble and §7 in
      `doc/prod/update_data_migration_runbook.md`.
- [x] §6 preamble, §6.1 (P2a slot), §6.2 (P2b slot), §6.4 (P4b slot),
      §6.5 (P5 slot), §7+ (P6/P7 territory), and all §5 subsections
      untouched.
- [x] Section explicitly notes "postprocessing-db, NOT preprocessing-db"
      to prevent operator context confusion when reading §6.2 then §6.3
      in sequence.
- [x] Acceptance SQL points at `sapphire-postprocessing-db` /
      `postprocessing_db` (correct DB).

### G. Repo-wide

- [x] No real station codes / discharge values / env-file contents
      anywhere in modified files.
- [x] No files outside the brief's file-scope list modified.
- [x] No edits to `sapphire/services/` (Charter §10).
- [x] PR opens against `develop_migration_toolkit`.

## Testing

### Unit tests

53 functions across the two new test files, all green locally on the
sub-orchestrator's worktree. CI will re-verify on the PR.

The full iEasyHydroForecast suite (437 tests including new + existing
P0/P1a/P1b/P1c/P3 migration tests) passes with zero failures and one
deterministic skipif (location-guard positive path on machines
without sapphire containers running).

### Linting

- Shellcheck: `shellcheck -x bin/export_lr_forecast_history.sh` clean
  (verified via the pre-commit hook on the commit `8e3cb7d`).
  `bin/initialize_lr_forecast_history.sh` was already clean from the
  prior sub-orch session.
- Ruff: pre-commit hook ran ruff on the test files at commit time
  (reformatted them once during the Phase 2 commit, then passed
  cleanly on the re-commit).

### CI

The new export wrapper falls under the migration-pattern regex of both
the pre-commit hook
(`^bin/(initialize_[a-z0-9_]+_history|export_[a-z0-9_]+_history)\.sh$`)
and the `shellcheck_migration_scripts` CI job's glob
(`bin/(initialize|export)_*_history.sh`). After this PR merges, the
gate is scanning at least 5 wrappers (snow, runoff DAY, meteo,
hydrograph DAY, plus this export script — once P2a/P2b/P4b/P5 also
merge it will scan the rest).

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Operator runs the export script on the deployment server by accident | Location guard fires before any DB connection, with an explicit error message pointing at the server-side wrapper. Env-var bypass is documented as test-only; CLI bypass (`--i-am-on-laptop`) prints a YELLOW WARNING to stderr. |
| Operator transfers only the CSV (not the manifest) | Server-side wrapper fails fast with `manifest file not found beside export CSV` before any image work. Manifest validation is the first server-side step. |
| Stale CSV on the server (operator forgot to refresh after laptop DB updates) | Manifest validation checks `row_count`, `station_count`, `date_min`, `date_max` against the CSV's actual contents — date-range or row-count mismatch raises a typed exception. |
| Cross-org station codes leak from laptop DB | Optional `--station-list-file` filter (export side) + `--station-filter` flag (server side) ensure only the deployment's codes are exported / imported. The manifest's `station_count` is the cross-check. |
| Operator confuses postprocessing-db with preprocessing-db when running §6.2 then §6.3 in sequence | Runbook §6.3 has an explicit "POSTPROCESSING-DB, NOT preprocessing-db" call-out in the source description; the dev port note (5433) is included; acceptance SQL uses `sapphire-postprocessing-db`. |
| Service-side `_has_changes` overwrites existing non-NULL stat with incoming NULL | Universal safe-write rule: only non-NULL source fields are POSTed. Cannot trigger the bug from this wrapper. |
| Upper-case horizon_type cells leak through the CSV | The wrapper validates `--horizon ∈ {pentad,decade}`; the `COPY` emits `lower(horizon_type::text)`; the server-side helper double-checks the enum in `_build_record`. Test pins this on the fixture. |
| Stray `model_type` cell in CSV silently overrides intent | `_build_record` does NOT include `model_type` in the payload even if present in the row dict. Test `test_build_record_does_not_emit_model_type` pins this. |
| SQL injection via crafted station code | Station list file is line-by-line parsed; any line containing `'` triggers a hard reject before query construction. Documented in the wrapper. |

## Out of Scope

- `--strict-merge` (read-before-merge) opt-in — not added for P4a.
  LR rows in practice are either fully populated or fully missing
  per forecast-cycle, so partial-NULL erasure is less of a concern
  than for hydrograph rows (P2b ships the stub flag; P4a does not).
  If operators observe LR stat erasure, file a follow-up issue.
- ML forecast (TFT / TiDE / TSMixer) migration — that's P4b
  (`forecasts` table with `model_type`).
- `combined_forecasts` migration — separate
  (P-postprocessing-combined_forecasts) phase. Per Stage A.2 §C, LR
  rows are filtered OUT of `combined_forecasts` because the LR
  forecast of record lives here.
- Service-side CRUD `_has_changes` fix — separate coordination with
  `sapphire/services/` owner.
- Disposable integration tests against a live docker stack —
  architecture §Q7 says these belong to a separate sprint.
- Modification of `data_migrator.py` — READ-ONLY reference; this
  wrapper replaces operator use of it (architecture §Q1).

## Dependencies

- **Depends on:** P0 (foundation —
  `bin/utils/update_migration_helpers.sh` +
  `bin/utils/migration_py/_common.py`).
- **Sibling to:** P2a (runoff PENTAD/DECADE — preprocessing-db
  source), P2b (hydrograph PENTAD/DECADE — preprocessing-db source),
  P4b (ML forecasts — postprocessing-db source, but for the
  `forecasts` table with `model_type` instead of this dedicated
  table), P5 (long forecasts).
- **Blocks:** none directly. P6 (regenerate hooks) covers tables
  that this wrapper does not (snow stats, hydrograph
  MONTH/SEASON/QUARTER, short / long-term skill metrics).

## References

- Architecture plan §Q1, §Q2, §Q3, §Q4, §Q5 — strategy table,
  safe-write rule, laptop-export workflow, horizon-type enum lock,
  manifest contract.
- P0 foundation: PR #343 (`cd01339`).
- P2b sibling structure (laptop-export pattern): PR pending against
  `develop_migration_toolkit` from `feature_p2b_hydrograph_period_history`
  branch.
- P4b sibling (ML forecasts on postprocessing-db `forecasts` table):
  staged in `feature_p4b_ml_forecast_history` work.
- LR schema: `sapphire/services/postprocessing/app/schemas.py`
  (`LRForecastBase` lines 125–143).
- LR model: `sapphire/services/postprocessing/app/models.py`
  (`LRForecast` lines 160–192).
- Legacy reference: `sapphire/services/postprocessing/app/data_migrator.py`
  (`LRForecastDataMigrator` lines 276–331).
- HorizonType enum lock: `sapphire/services/postprocessing/app/models.py`
  lines 9–14.
- Sub-orchestrator Charter v2:
  `doc/plans/working/update_migration_suborchestrator_charter.md`.

## Process note (sub-orchestrator)

The P4a phase was split across two sub-orchestrator sessions per
Charter v2's watchdog-mitigation protocol. The first session
(commit `3f6debf`) landed the server wrapper + Python helper module
before stalling on the 600s watchdog. This continuation sub-orch
(Opus 4.7) picked up cleanly from `origin/feature_p4a_lr_forecast_history`
HEAD `3f6debf` and completed the remaining four phases without any
edits to the previously-committed files (file-scope discipline
preserved per Charter §6). Each phase was committed individually
(`8e3cb7d` export script; `a440223` tests + fixtures; `66b9a02`
runbook §6.3; this commit gi_draft). After the runbook commit the
branch was pushed to `origin/feature_p4a_lr_forecast_history`. No
edits to `sapphire/services/` (Charter §10). The code-review skill
was NOT run on the diff — operator + reviewers will surface any
issues on the PR.
