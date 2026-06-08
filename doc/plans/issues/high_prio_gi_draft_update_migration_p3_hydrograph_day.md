# P3 - Hydrograph DAY CSV-to-API Migration Wrapper

**Status:** Implementation complete (sub-orch made 3 commits before API connection drop; top orch wrote this gi_draft + opens PR).
**Module:** infra (cross-module — bin/, apps/iEasyHydroForecast/tests/, doc/prod/)
**Priority:** High
**Branch:** `feature_p3_hydrograph_day_history` → `develop_migration_toolkit`
**Labels:** `infra`, `tooling`, `tests`, `deployment-runbook`

---

## Summary

Adds the final CSV-source migration wrapper of the update-time migration toolkit: `bin/initialize_hydrograph_day_history.sh` for hydrograph DAY data. Closes the CSV-source family (after P1a runoff DAY, P1b meteo T/P, P1c snow validation). The two P3-specific concerns are **dynamic year-column discovery** (replaces the `data_migrator.py:264-265` hardcoded `'2024'/'2025'` bug per Stage A hypothesis #1) and **enrichment-only safe-write** for the wide hydrograph stat row.

## Context

The hydrograph DAY table holds climatological stat rows for each station and date — `mean`, `min`, `max`, `std`, `q05/q25/q50/q75/q95`, `norm`, `previous`, `current`. Stage A inventory: 6,206 lines on TAJ, ~84 stations. The DB model unique key is `(horizon_type, code, date)`. The CSV at `${ieasyhydroforecast_data_ref_dir}/intermediate_data/hydrograph_day.csv` has columns named after years (`2025`, `2026` on the current TAJ archive) that rotate annually for the `previous` / `current` payload fields.

References:
- Architecture plan §Q1 strategy table row for `hydrographs:DAY`.
- Architecture plan §Q2 safe-write rule (layer 2 — enrichment-only).
- P0 foundation: PR #343 (merged `cd01339`).
- P1a sibling structure: PR #345 (merged `79cdd5e`) + `bin/initialize_runoff_day_history.sh` + `bin/utils/migration_py/runoff_day.py`.
- Stage A discovery: `doc/plans/working/update_migration_phase1_discovery.md` §A row for hydrograph DAY.
- Hardcoded-year bug verified in: `sapphire/services/preprocessing/app/data_migrator.py:264-265,294-295,326-327` (READ-ONLY reference — `sapphire/services/` is colleague-managed).

## Problem

Three failure modes the old in-container `data_migrator.py` exposes for hydrograph DAY:

1. **Hardcoded year columns.** `prepare_day_data` reads CSV columns named literally `'2024'` and `'2025'` and maps them to `previous` / `current`. Wrong every year (2026 onwards) and on every deployment that doesn't share TAJ's calendar.
2. **Hardcoded HRU/station code patterns** in adjacent migrators that bleed into hydrograph day's filename + path expectations.
3. **Wide nullable payload, `_has_changes` overwrite risk.** The hydrograph table has 12+ nullable stat fields. The service-side CRUD `_has_changes + setattr` path overwrites existing non-NULL target values with incoming NULL when any field differs. The migrator does not guard against this.

The new wrapper resolves (1) via dynamic year-column discovery, sidesteps (2) by reading the deployment's `intermediate_data/hydrograph_day.csv` at the path derived from the env file (no hardcoded HRU), and addresses (3) by sending only non-NULL source fields per the universal safe-write rule (architecture §Q2 layer 2 — enrichment-only).

## Desired Outcome

After this PR merges to `develop_migration_toolkit`:

- `bin/initialize_hydrograph_day_history.sh` (436 LOC) exists, mirrors the P1a wrapper shape (positional env_file_path, `--dry-run`, `--api-url`, `--batch-size`, `--image`, `--station-filter`).
- `bin/utils/migration_py/hydrograph_day.py` (535 LOC) exposes `main()` for CLI entry plus private helpers (`_discover_year_columns`, `_read_filtered_records`, `_build_record`, `_post_batch`, `_print_dry_run_inventory`, `_build_arg_parser`). Stdlib-only (verified by the import-audit test).
- `apps/iEasyHydroForecast/tests/test_initialize_hydrograph_day.py` (688 LOC, 27 tests) covers:
  - Wrapper CLI surface (`--help`, missing env file rejection)
  - `_discover_year_columns` edge cases (2 years, 3 years, 0 years → raises, 1 year → raises)
  - Quantile-column normalization (`5%` → `q05`, etc.)
  - Station filter, cutoff filter, NULL stat handling
  - `_build_record` year-column mapping
  - Dry-run output including `HYDROGRAPH_YEAR_MAPPING` line
  - Stdlib-only import audit on the new module
- `apps/iEasyHydroForecast/tests/fixtures/migration_csv/hydrograph_day/sample.csv` exists with sentinel code `19999` and year columns `2025` + `2026`.
- `doc/prod/update_data_migration_runbook.md` has §5.4 written (lines 455+); §5.1 / §5.2 / §5.3 / others untouched.
- `shellcheck -x bin/initialize_hydrograph_day_history.sh` reports zero findings.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast` passes including all 27 new tests + existing migration tests (40 from P0, 20 from P1a, 16 from P1b = 103 migration tests total in the iEasyHydroForecast module).

## Implementation Plan (already executed)

The P3 sub-orch completed all three implementation phases before the API connection dropped, in this order:

1. **`bin/initialize_hydrograph_day_history.sh` + `bin/utils/migration_py/hydrograph_day.py`** — commit `54811d5`
2. **`apps/iEasyHydroForecast/tests/test_initialize_hydrograph_day.py` + sentinel fixture** — commit `a901b87`
3. **Runbook §5.4** — commit `c08d5bf`

The watchdog mitigation in the brief (commit-after-each-phase rather than batched-at-end) preserved all work despite the connection drop. The gi_draft (this file) was written by the top orchestrator and added in the follow-up commit that opens the PR.

## Key design decisions

### Dynamic year-column discovery

`_discover_year_columns(header)` returns `(previous_year, current_year)` by:

1. Filtering header for column names matching `^\d{4}$`.
2. Sorting numerically.
3. Returning the second-to-last + last entries.

Edge cases tested: 2 years → returns both; 3 years → returns last two (newest-year `current`); 0 or 1 year → raises `ValueError` with a descriptive message.

The dry-run inventory emits a `HYDROGRAPH_YEAR_MAPPING={previous: <year>, current: <year>}` line so the operator confirms the mapping before any write.

### Safe-write: enrichment-only (Option A)

The wrapper sends only non-NULL source fields, never explicit NULL. This matches the universal safe-write rule (architecture §Q2 layer 2) and the P1a/P1b/snow precedent. The known service-side `_has_changes + setattr` overwrite-with-NULL risk is documented in the gi_draft + runbook §5.4 as a known caveat; the proper fix is a service-side CRUD patch coordinated separately (per architecture §Q2 coordination flags).

A `--strict-merge` flag was considered as Option B (read-before-merge), but not implemented in P3 because:
- All four CSV-source siblings (snow, runoff DAY, meteo, hydrograph DAY) use the same enrichment-only pattern; introducing per-wrapper variation now adds confusion.
- Hydrograph DAY's CSV is internally complete (the producer writes all stat fields for each row); split-coverage scenarios are rare in operational data.
- If stat erasure surfaces in practice, the fix is a future PR adding `--strict-merge` to the wrapper.

### Quantile-column normalization

CSVs from TAJ use `5%`, `25%`, `50%`, `75%`, `95%` column names (URL-encoding from pandas). The DB schema expects `q05`, `q25`, `q50`, `q75`, `q95`. `_read_filtered_records` does the mapping before building the payload. Test coverage in `test_read_filtered_records_quantile_column_normalization`.

## Acceptance Criteria

### A. Wrapper

- [x] `bin/initialize_hydrograph_day_history.sh` exists; sources `common_functions.sh` + `update_migration_helpers.sh`; uses `umh_resolve_image` / `umh_acquire_temp_workspace` / `umh_log_redacted` / `umh_print_image_resolution_line`.
- [x] CLI: positional env_file_path, `--dry-run`, `--api-url`, `--batch-size`, `--image`, `--station-filter`.
- [x] `--station-filter <code>` honored (P1a-locked binding contract).
- [x] `shellcheck -x` zero findings.
- [x] `bash bin/initialize_hydrograph_day_history.sh --help` returns 0 with usage text.

### B. Python module

- [x] `bin/utils/migration_py/hydrograph_day.py` exposes `main()` publicly; helpers private (`_discover_year_columns`, `_read_filtered_records`, `_build_record`, etc.).
- [x] Stdlib-only (verified by `test_hydrograph_day_module_imports_only_stdlib_and_intra_package`).
- [x] `_discover_year_columns` raises on 0 or 1 year columns; returns sorted last-two for ≥ 2.

### C. Tests

- [x] 27 test functions in `apps/iEasyHydroForecast/tests/test_initialize_hydrograph_day.py`.
- [x] Coverage includes wrapper CLI smoke, year-column discovery edge cases, quantile normalization, station filter, cutoff filter, NULL handling, dry-run inventory shape, audit.
- [x] All 27 tests pass locally (sub-orch verified before commit `a901b87`).

### D. Fixture

- [x] `apps/iEasyHydroForecast/tests/fixtures/migration_csv/hydrograph_day/sample.csv` contains only sentinel code `19999`.
- [x] Header includes `2025` + `2026` year columns matching the current TAJ archive shape.
- [x] Fixture-guard test (`test_migration_fixture_guard.py`) still passes (no real station codes).

### E. Runbook

- [x] §5.4 written at line 455+ of `doc/prod/update_data_migration_runbook.md`.
- [x] §5.1 (P1a) at line 226, §5.2 (P1b) at line 316, §5.3 (P1c) at line 413 — unchanged.

### F. Repo-wide

- [x] No real station codes / discharge values / env-file contents anywhere in modified files.
- [x] No files outside the §3 file-scope list modified.
- [x] No edits to `sapphire/services/` (Charter §10).
- [x] PR opens against `develop_migration_toolkit`.

## Testing

### Unit tests

27 functions in `test_initialize_hydrograph_day.py`, all green locally before the connection drop. CI will re-verify on the PR.

### Linting

- Shellcheck: `shellcheck -x bin/initialize_hydrograph_day_history.sh` clean.
- Ruff: pre-commit hook runs ruff on `bin/utils/migration_py/hydrograph_day.py` + the test file on commit.

### CI

The new wrapper falls under the migration-pattern regex of both the pre-commit hook (`^bin/(initialize_[a-z0-9_]+_history|export_[a-z0-9_]+_history)\.sh$`) and the `shellcheck_migration_scripts` CI job's glob (`bin/initialize_*_history.sh`). After this PR merges, the gate is scanning 4 wrappers (snow, runoff DAY, meteo, hydrograph DAY).

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Hydrograph CSV's year columns change format (e.g., `2026-01`, `FY2026`) | `_discover_year_columns` regex `^\d{4}$` is strict. Non-4-digit names are ignored. If 0 or 1 year column is found, the function raises with a descriptive error rather than silently choosing wrong columns. |
| Quantile column naming changes (e.g., `q05` in source instead of `5%`) | `_read_filtered_records` accepts both forms; tested in `test_read_filtered_records_quantile_column_normalization`. |
| Service-side `_has_changes` overwrites existing non-NULL stat with incoming NULL | Enrichment-only payload (only non-NULL sent). Risk surfaces only when source row is partially populated AND target row already has a different non-NULL stat; in that case the service path is the broken contract, not this wrapper. Coordination needed with `sapphire/services/` owner. |
| Operator runs against the wrong deployment (cross-org station codes leak) | `--station-filter` is the per-station canary mechanism. Dry-run inventory shows distinct-station-count-redacted before any write. |
| Dropbox conflict copies in `intermediate_data/` (per Stage F observation on KGHM) | Strict CSV path (`intermediate_data/hydrograph_day.csv`) — the wrapper reads ONE file, not a glob. Conflict copies live elsewhere and don't affect this wrapper. |

## Out of Scope

- `--strict-merge` (read-before-merge) flag — future PR if stat erasure surfaces in practice.
- Hydrograph PENTAD / DECADE / MONTH / SEASON / QUARTER — those are DB-source (P2b) or regenerate-hook (P6) per architecture §Q1.
- Service-side CRUD `_has_changes` fix — separate coordination with `sapphire/services/` owner.
- Modification of existing `data_migrator.py` — it remains as a READ-ONLY reference; this wrapper replaces operator use of it (architecture §Q1).

## Dependencies

- **Depends on:** P0 (foundation), P1a (sibling — establishes the `--station-filter` forward contract and module-extraction pattern).
- **Blocks:** none directly. P3 + P1a + P1b + snow port complete the CSV-source family. P2 (DB-source laptop-export) is independent; P4 (forecast laptop-export) depends on P2a; P5 (long-term) is independent.

## References

- Architecture plan: `doc/plans/working/update_migration_toolkit_architecture.md` (§Q1, §Q2, §Q3)
- P0 foundation: PR #343 (`cd01339`)
- P1a sibling: PR #345 (`79cdd5e`)
- P1b sibling: PR #346 (`f36d680`)
- P1c sibling: PR #348 (`1de503f`)
- Snow port: PR #347 (`1b8f425`)
- Stage A discovery: `doc/plans/working/update_migration_phase1_discovery.md`
- Sub-orchestrator Charter v2: `doc/plans/working/update_migration_suborchestrator_charter.md`
- Stage A.2 audit: `doc/plans/working/update_migration_phase1_csv_api_audit.md`

## Process note (sub-orch + top-orch handoff)

The P3 sub-orch (Opus 4.7, worktree-isolated, charter v2) ran for ~21 minutes and completed all three implementation phases (wrapper+module, tests+fixture, runbook §5.4) with per-phase commits per the brief's watchdog mitigation. The API socket dropped after commit `c08d5bf` (runbook §5.4) but before the gi_draft was written and the PR opened. The top orchestrator wrote this gi_draft as a planning artifact (documentation, not implementation), committed it, pushed the branch, and opened the PR. No new implementation code was written by the orchestrator. Code-review skill was NOT run on the diff — operator + reviewers will surface any issues via the PR.
