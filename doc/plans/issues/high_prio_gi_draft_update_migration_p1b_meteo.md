# P1b - Update Migration: Meteo T/P CSV-to-API wrapper (HRU discovery)

**Status:** Draft v1 — implementation complete, awaiting review.
**Module:** infra (cross-module — `bin/`, `apps/iEasyHydroForecast/tests/`, `doc/prod/`)
**Priority:** High
**Branch:** `feature_p1b_meteo_history` (PR target: `develop_migration_toolkit`)
**Labels:** `infra`, `tooling`, `tests`, `deployment-runbook`, `migration`

## Forward interface contracts honored

- **`--station-filter <code>`** (locked in P0): P1b accepts this flag and
  forwards it into the embedded Python helper as a single-HRU filter. Tests
  cover the contract end-to-end.
- **`--meteo-type T|P|both`** (new in P1b): operator-side filter to restrict
  processing to one meteo type. Default is `both`.

## Summary

Ship `bin/initialize_meteo_history.sh`, the update-time migration wrapper for
historical meteorological reanalysis CSVs (Temperature and Precipitation) into
the preprocessing `meteo` table. P1b replaces the old in-container
`sapphire/services/preprocessing/app/data_migrator.py` MeteoDataMigrator
pathway — which hardcoded a single HRU literal — with a wrapper that
discovers every HRU CSV under the deployment data root via filename glob,
optionally merges per-HRU dashboard sidecars for the `norm` field, and POSTs
minimal non-null payloads to `/meteo/`.

The wrapper builds on the P0 foundation:

- Sources `bin/utils/update_migration_helpers.sh` for image resolution,
  uniform log format, and temp-hygiene helpers.
- Uses `bin/utils/migration_py/_common.detect_mode` to choose `full-import`
  vs. `pre-cutoff` per `meteo_type` independently from the live target table
  state (queried via `docker exec sapphire-preprocessing-db psql`).
- Emits the Stage E §4.4 dry-run inventory before any write (mode, target
  count, cutoff, distinct HRU count redacted, per-file row counts and date
  ranges).

P1b ships in its own PR against `develop_migration_toolkit` and runs in
parallel with P1a (runoff DAY) and P1c (snow validation), per the
architecture plan dependency graph (`update_migration_toolkit_architecture.md`
§Q6 + §Dependency Graph).

## Context

The architecture plan §Per-Data-Type-Strategy-Table specifies for
`meteo:T/P`:

| Wrapper / hook | MODE | Payload contract | Safe-write rule |
|---|---|---|---|
| `bin/initialize_meteo_history.sh` | full-import or pre-cutoff | `meteo_type`, `code`, `date`, `day_of_year`, non-null `value` and/or `norm` | Minimal non-null payload; never send missing side as null |

The Stage A.2 §C audit verified that the old `data_migrator.py` MeteoDataMigrator
hardcodes a single HRU filename literal (`hindcast_forcing/00003_<T|P>_reanalysis.csv`,
`sapphire/services/preprocessing/app/data_migrator.py:504-505`). For
deployments with multiple HRUs (TAJ archives have several distinct HRUs in
`hindcast_forcing/`), this drops every HRU but one. P1b fixes that by
discovering HRU codes from filenames with a regex:

```python
REANALYSIS_RE = re.compile(r"^(?P<hru>[A-Za-z0-9]+)_(?P<mt>[TP])_reanalysis\.csv$")
```

This is the explicit anti-pattern correction documented in the architecture
plan §Q3 row "Archive CSV files": *Glob mounted data root under
`intermediate_data/`; no hardcoded HRU/station names.*

## Architecture quirk: why historical meteo lives only in CSV

The operational pipeline's `extend_era5_reanalysis.py` writes reanalysis
data to disk but skips the API write step. This is documented in the Stage
A.2 §C discovery and is the reason a dedicated historical-migration wrapper
is needed: without P1b, the API's `meteo` table is permanently empty for
reanalysis-era dates regardless of how long the deployment has been running.

P1b is therefore the canonical path from CSV to `meteo` for any deployment
that did not run the (broken) in-container `data_migrator.py` at install
time.

## Problem

Concrete reasons the existing tooling cannot ship the meteo historical data:

1. **Single-HRU hardcode.** `data_migrator.py:504-505` glues
   `hindcast_forcing/00003_<T|P>_reanalysis.csv` to a Path object with the
   HRU literal embedded. Other HRUs in the same directory are silently
   ignored. Detected in Stage A.2 §C hypothesis #3 ("Meteo hardcodes one
   redacted HRU filename under `hindcast_forcing/`: VERIFIED").

2. **Anti-pattern entry point.** The architecture plan §Q1 marks
   in-container `data_migrator.py` use as an explicit anti-goal because it
   depends on container settings, cannot solve the cross-org station-code
   filter problem, and bypasses universal safe-write discipline.

3. **No mode gate.** The old migrator does not check whether the target
   table is empty, so it cannot promote to `full-import` on a fresh
   deployment or restrict to `pre-cutoff` on an existing one. P0's
   `detect_mode()` is the universal fix and P1b is the first multi-HRU
   wrapper to use it.

4. **No `norm` partial-payload safety.** The old migrator sends `norm: None`
   when the dashboard CSV is absent. Combined with the service-side
   `_has_changes + setattr` bug (Stage A.2 §D), a subsequent value-only
   write can erase an existing `norm`. P1b sends only non-null fields per
   architecture §Q2 layer 2.

## Desired Outcome

After P1b merges to `develop_migration_toolkit`:

- `bin/initialize_meteo_history.sh` exists, is shellcheck-clean (`-x`), and
  passes `--help` returning exit code 0 with usage text.
- The wrapper sources `bin/utils/update_migration_helpers.sh` (P0 helper)
  and uses `migration_py._common.detect_mode` for MODE selection.
- Discovery globs `<data_ref_dir>/intermediate_data/hindcast_forcing/` for
  `<HRU>_T_reanalysis.csv` / `<HRU>_P_reanalysis.csv`; HRU codes are
  extracted from filenames; no HRU literals appear anywhere in the wrapper
  or its embedded helper.
- Dashboard sidecars at `<HRU>_<T|P>_reanalysis_dashboard.csv` are
  detected per-HRU and used to merge the `norm` field; absence is
  permissive (the wrapper falls back to value-only payloads).
- Per-meteo-type MODE detection runs independently:
  `meteo_type='T'` and `meteo_type='P'` may have different cutoffs.
- The wrapper accepts `--station-filter <CODE>` (P0 forward contract) and
  `--meteo-type T|P|both` (new flag).
- Tests under `apps/iEasyHydroForecast/tests/test_initialize_meteo.py`
  exercise: `--help`, missing-env-file rejection, `--meteo-type` validation,
  HRU discovery against fixture CSVs (≥2 HRUs), station filter, meteo-type
  filter, combined filters, the wrapper-regex contract, end-to-end dry-run
  inventory, and the embedded helper's syntax-check.
- Fixtures under
  `apps/iEasyHydroForecast/tests/fixtures/migration_csv/meteo/` are
  sentinel-only: HRU codes `00001` and `00002` (in the P0 sentinel
  allowlist `00000..00009`).
- Runbook §5.2 documents the canary dry-run, canary write, full-population,
  meteo-type filter, acceptance SQL, and idempotent rerun pattern.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast` reports zero
  failures and zero unexpected skips.
- No real station/HRU codes anywhere in the diff (sentinel-fixture guard
  test still passes).
- No edits to `sapphire/services/`.

## Implementation Plan

### Files

| Path | Purpose |
|---|---|
| `bin/initialize_meteo_history.sh` | NEW. CLI wrapper, image resolution, MODE detection, embedded Python helper, Docker invocation. |
| `apps/iEasyHydroForecast/tests/fixtures/migration_csv/meteo/00001_T_reanalysis.csv` | NEW. Sentinel HRU `00001` Temperature, 3 rows. |
| `apps/iEasyHydroForecast/tests/fixtures/migration_csv/meteo/00001_P_reanalysis.csv` | NEW. Sentinel HRU `00001` Precipitation, 3 rows. |
| `apps/iEasyHydroForecast/tests/fixtures/migration_csv/meteo/00001_T_reanalysis_dashboard.csv` | NEW. Sentinel HRU `00001` T dashboard sidecar with `T_norm`. |
| `apps/iEasyHydroForecast/tests/fixtures/migration_csv/meteo/00002_T_reanalysis.csv` | NEW. Sentinel HRU `00002` Temperature, 2 rows. |
| `apps/iEasyHydroForecast/tests/fixtures/migration_csv/meteo/00002_P_reanalysis.csv` | NEW. Sentinel HRU `00002` Precipitation, 2 rows. |
| `apps/iEasyHydroForecast/tests/test_initialize_meteo.py` | NEW. 16 pytest tests covering CLI, discovery, filters, embedded-helper end-to-end dry run. |
| `doc/prod/update_data_migration_runbook.md` | MODIFY. Add §5.2 only; do NOT touch §5.1 (P1a) or §5.3 (P1c). |
| `doc/plans/issues/high_prio_gi_draft_update_migration_p1b_meteo.md` | NEW. This file. |

### `bin/initialize_meteo_history.sh` requirements

- Shebang `#!/usr/bin/env bash`, `set -eo pipefail` (no `set -u`; documented
  in the wrapper header and in `update_migration_helpers.sh`).
- Sources `bin/utils/update_migration_helpers.sh` (which transitively
  sources `common_functions.sh` for `read_configuration` /
  `print_banner`).
- Positional argument: `env_file_path` (required).
- Optional flags:
  - `--dry-run`
  - `--api-url URL` (default: `http://localhost:8002/meteo/`)
  - `--batch-size N` (default: 500)
  - `--image IMAGE` (default: resolved by `umh_resolve_image` from
    `ieasyhydroforecast_backend_docker_image_tag`)
  - `--station-filter CODE` (single HRU filter — forward contract)
  - `--meteo-type T|P|both` (default: `both`)
  - `-h, --help`
- Validates the env file exists; runs `read_configuration` to load
  `ieasyhydroforecast_data_root_dir`, `ieasyhydroforecast_data_ref_dir`,
  `ieasyhydroforecast_backend_docker_image_tag`.
- Resolves the Docker image via `umh_resolve_image` and logs the
  resolution line via `umh_print_image_resolution_line` (uniform format
  across all wrappers).
- Creates the log dir `${ieasyhydroforecast_data_root_dir}/logs/meteo_history_init/`
  and a timestamped log file.
- **MODE detection (NOT in dry-run):** for each requested meteo type,
  runs `docker exec sapphire-preprocessing-db psql -t -A -P pager=off -c
  "SELECT COUNT(*) || '|' || COALESCE(MIN(date)::text, '') FROM meteo WHERE meteo_type='<MT>';"`,
  parses the result, and calls `_common.detect_mode` via inline `python3`
  with `sys.path` patched to `bin/utils/`. Result is exported as
  `CUTOFF_T` / `CUTOFF_P` to the Python helper's environment.
- Writes the Python helper as a heredoc into the log dir, mounts it
  read-only into the prepgateway container at `/script.py`, mounts the
  data dir read-only at `/meteo_data`, and runs `python3 /script.py`.
- Post-run: prints the acceptance SQL block for the operator to verify
  rows landed.

### Embedded Python helper requirements

- Stdlib-only: `csv`, `json`, `os`, `re`, `sys`, `time`, `urllib.error`,
  `urllib.request`, `datetime`, `pathlib`. No pandas, no third-party.
- Reads env vars: `METEO_DATA_DIR`, `API_URL`, `BATCH_SIZE`,
  `STATION_FILTER`, `METEO_TYPE_FILTER`, `CUTOFF_T`, `CUTOFF_P`.
- Detects `--dry-run` as a CLI argument.
- Discovery function `discover_reanalysis_files(root)`:
  - Globs `<root>/*_reanalysis.csv`.
  - Matches each filename against the regex above; skips non-matching.
  - Applies `STATION_FILTER` and `METEO_TYPE_FILTER`.
  - Detects per-HRU dashboard sidecar at `<HRU>_<MT>_reanalysis_dashboard.csv`.
  - Returns `[(hru, meteo_type, reanalysis_path, dashboard_path_or_None), ...]`.
- Per-HRU row build: reads the CSV, applies cutoff filter, drops rows
  where both `value` and `norm` are null, merges norm from the dashboard
  if available, computes `day_of_year` from the date.
- Batched POST via `urllib.request.urlopen` with JSON body
  `{"data": [...]}`, batch size from env.
- Idempotent on `(meteo_type, code, date)` (service-side upsert).
- Dry-run mode prints inventory + first/last record per HRU; never POSTs.

### Tests

`apps/iEasyHydroForecast/tests/test_initialize_meteo.py` ships 16 tests
in five logical groups:

1. **CLI parsing** (4 tests): `--help` exits 0 with usage; missing
   env-file rejected; no args rejected; bad `--meteo-type` rejected.
2. **Discovery unit** (5 tests): the regex + glob walk finds both
   sentinel HRUs; station filter restricts to one; meteo-type filter
   restricts to one type; combined filters narrow to one file.
3. **Contract** (2 tests): the wrapper's heredoc embeds the same regex
   the unit tests use (drift detection); the wrapper accepts
   `--station-filter` as a known flag (forward contract).
4. **End-to-end dry-run** (4 tests): extract the embedded helper from
   the wrapper, execute it directly via `python3 helper.py --dry-run`
   against tmp_path copies of the fixtures, assert the inventory
   mentions both HRUs / restricts under station filter / restricts
   under meteo-type filter / exits cleanly on empty directory.
5. **Sanity** (1 test): the embedded helper compiles under
   `compile(src, ..., "exec")`.

### Fixtures

All five fixture files use sentinel HRU codes `00001` and `00002` (in
the P0 sentinel allowlist `00000..00009`). The fixture file names match
the production glob pattern exactly so the test exercises the real
discovery logic. The dashboard sidecar exists only for `00001` T —
intentional, so the test can verify both the "dashboard present" and
"dashboard absent" branches.

The existing `test_migration_fixture_guard.py` enforces the
sentinel-only policy across all fixtures under `migration_csv/`, so
P1b's fixtures must (and do) pass that guard.

### Runbook §5.2

Touches ONLY §5.2 — does not modify §5.1 (P1a runoff DAY) or §5.3
(P1c snow). The new section covers:

- 5.2.1 Canary dry-run (sentinel HRU)
- 5.2.2 Canary write (single HRU)
- 5.2.3 Full population
- 5.2.4 Filtering by meteo type
- 5.2.5 Acceptance SQL
- 5.2.6 Idempotency and rerun

The canary flow ties into the §4.3 acceptance criterion (Stage E #7).

## Acceptance Criteria

### A. Wrapper

- [x] `bin/initialize_meteo_history.sh` exists.
- [x] `shellcheck -x bin/initialize_meteo_history.sh` reports zero
      findings.
- [x] `bash bin/initialize_meteo_history.sh --help` exits 0 and prints
      the required option strings (`--dry-run`, `--station-filter`,
      `--meteo-type`, `--api-url`, `--batch-size`).
- [x] Sources `bin/utils/update_migration_helpers.sh`.
- [x] Uses `umh_resolve_image`, `umh_print_image_resolution_line`, and
      `_common.detect_mode`.
- [x] Per-meteo-type MODE detection logic runs independently for T and
      P (skipped in dry-run to keep dry-run host-independent).
- [x] Discovery uses a filename regex (no HRU literals).
- [x] Embedded Python helper is stdlib-only.
- [x] Payload sends `meteo_type`, `code`, `date`, and conditional
      `value`, `norm`, `day_of_year` — never null for any sent field
      (universal safe-write).

### B. Tests

- [x] 16 new tests in `test_initialize_meteo.py`.
- [x] HRU-discovery tests cover ≥2 distinct HRUs.
- [x] `--station-filter` round-trips through the wrapper.
- [x] `--meteo-type T` and `--meteo-type P` filters covered.
- [x] End-to-end dry-run executes the embedded helper without Docker.
- [x] `SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast`
      passes — 337 passed, 0 failed, 0 unexpected skips.

### C. Fixtures

- [x] Five fixture files under
      `apps/iEasyHydroForecast/tests/fixtures/migration_csv/meteo/`.
- [x] All HRU codes are sentinels (`00001`, `00002`).
- [x] `test_migration_fixture_guard.py` still passes.

### D. Runbook

- [x] §5.2 added with subsections 5.2.1 through 5.2.6.
- [x] §5.1 and §5.3 untouched (parallel P1a / P1c siblings own those).
- [x] Acceptance SQL block mirrors architecture §Q9 meteo block.

### E. Repo-wide hygiene

- [x] No real station / HRU codes anywhere in committed files
      (`grep -RE` against the sentinel exclusion finds nothing).
- [x] No edits outside the P1b file-scope list.
- [x] No edits to `sapphire/services/`.

## Testing

### Unit + subprocess

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast
```

Expected: 337 passed (321 baseline + 16 new), 0 failed.

### Linting

```bash
shellcheck -x bin/initialize_meteo_history.sh
pre-commit run --files bin/initialize_meteo_history.sh \
                       apps/iEasyHydroForecast/tests/test_initialize_meteo.py
```

Both must pass.

### Manual operator validation (server)

After the PR merges and the wrapper reaches a deployment server, the
operator runs:

```bash
# 1. Canary dry-run on a single HRU (sentinel or low-risk real code)
bash bin/initialize_meteo_history.sh "$ENV_FILE" --dry-run \
    --station-filter <code>

# 2. Canary write (one HRU)
bash bin/initialize_meteo_history.sh "$ENV_FILE" --station-filter <code>

# 3. Verify via acceptance SQL (runbook §5.2.5)
# 4. Full population
bash bin/initialize_meteo_history.sh "$ENV_FILE"
```

CI does not exercise the Docker layer; that is verified at the canary
step by the operator on the deployment server.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Filename regex too narrow (excludes real HRU codes) | `[A-Za-z0-9]+` allows alphanumeric HRU codes (TAJ uses 5-digit numeric; future deployments may use mixed). Tested with sentinel `00001` / `00002`. If a deployment uses HRUs with hyphens or underscores, the regex will need to broaden — caught at canary dry-run by `distinct HRUs discovered` being lower than expected. |
| Dashboard CSV missing on production deployment | Wrapper falls back to value-only payloads. `norm` remains NULL in the target row, which is acceptable per the meteo schema (`norm` is nullable). Operator sees `dashboard=no` in the dry-run inventory. |
| Meteo target table already populated with bad data from old `data_migrator.py` run | Pre-cutoff MODE preserves the existing rows; new rows are written only for `date < cutoff`. Operator must decide whether to scoped-purge the bad rows before re-importing (rollback path in runbook §9-§10). |
| `extend_era5_reanalysis.py` later starts writing reanalysis data through the API | The natural key `(meteo_type, code, date)` is upserted; this wrapper and the operational path co-exist safely. |
| Per-meteo-type MODE detection desync (T cutoff differs from P cutoff) | This is intentional: each meteo type's import history is independent. The wrapper logs both modes prominently before invoking the helper. |
| Embedded Python helper drifts from the regex tested in `test_initialize_meteo.py` | `test_wrapper_embeds_expected_regex` greps the wrapper for the exact regex literal; drift fails CI. |

## Out of Scope (P1b explicitly does not ship)

- Meteo pentad / decade aggregation (architecture §Per-Data-Type table
  marks these as `NotImplementedError` upstream).
- Quarterly hydrograph norms (separate issue).
- Snow value migration (P1c).
- Hydrograph DAY (P3 — depends on P1a).
- Migration of dashboard CSV's standalone records (the dashboard is used
  ONLY as a `norm` source for the reanalysis rows; non-merge rows are
  intentionally dropped, matching old `data_migrator.py` behavior).

## Dependencies

- **Depends on:** P0 (helper shell + Python package + test scaffold).
- **Blocks:** Nothing direct. P1b is a peer of P1a (runoff DAY) and P1c
  (snow). P3 (hydrograph DAY) depends on P1a, not on P1b.

## References

- Architecture plan: `doc/plans/working/update_migration_toolkit_architecture.md`
  (§Per-Data-Type table row "Meteo T/P", §Q1, §Q2, §Q3, §P1a/P1b/P1c
  section)
- Sub-orchestrator charter v2: `doc/plans/working/update_migration_suborchestrator_charter.md`
- P0 gi_draft: `doc/plans/issues/high_prio_gi_draft_update_migration_p0_foundation.md`
- Stage A.2 discovery: `doc/plans/working/update_migration_phase1_discovery.md`
  §A (TAJ archive shape), §C (old migrator audit, hypothesis #3
  "Meteo hardcodes one redacted HRU filename: VERIFIED")
- Snow precedent: commit `75c0c8f` — `bin/initialize_snow_history.sh`
  (per-CSV-file discovery loop, embedded Python heredoc pattern)
- P0 helpers: `bin/utils/update_migration_helpers.sh`,
  `bin/utils/migration_py/_common.py`
- Read-only reference for payload shape: `sapphire/services/preprocessing/app/data_migrator.py`
  class `MeteoDataMigrator` (especially `prepare_day_data:394-422`)
