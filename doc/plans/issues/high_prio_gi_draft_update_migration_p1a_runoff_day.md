# P1a - Runoff DAY CSV-to-API Migration Wrapper

**Status:** Draft v1 — implementation in progress on `feature_p1a_runoff_day_history`.
**Module:** infra (cross-module — `bin/`, `apps/iEasyHydroForecast/tests/`, `doc/prod/`).
**Priority:** High
**Branch:** `develop_migration_toolkit` (integration branch; this PR opens against it)
**Labels:** `infra`, `tooling`, `tests`, `deployment-runbook`

## Summary

Ship the first CSV-source migration wrapper of the update-time toolkit:
`bin/initialize_runoff_day_history.sh`, which migrates rows from
`<data_ref>/intermediate_data/runoff_day.csv` (or any CSV with header
`code,date,discharge`) into the `runoffs` table via the preprocessing API,
using the P0 shared helper, the stdlib-only `migration_py._common` module, and
the binding `--station-filter` flag contract that P0 locked in.

The wrapper supports both MODE branches: `full-import` when the target
`runoffs` rows of `horizon_type='DAY'` are empty, and `pre-cutoff (cutoff=...)`
when populated. It runs via `docker run --network host
mabesa/sapphire-prepgateway:<tag>` and mounts the P0 `migration_py/` package
read-only into the container — no new Docker image variant. Idempotent on the
service-side unique key `(horizon_type, code, date)`.

## Context

The architecture plan
(`doc/plans/working/update_migration_toolkit_architecture.md` §Q1, §Q2, §Q3 +
table at §Q6 "P1a/P1b/P1c - CSV Source Wrappers") names this wrapper as the
first concrete migration after P0. Stage A discovery
(`doc/plans/working/update_migration_phase1_discovery.md` §A) confirmed the
TAJ archive shape: `runoff_day.csv` with header `code,date,discharge`,
~437k rows, dates `1940-01-01` to current operational date.

The forward interface contract from the P0 gi_draft is binding here:

> `--station-filter <code>`: P1a (`bin/initialize_runoff_day_history.sh`) MUST
> expose this flag accepting a single sentinel/real station code. Subsequent
> CSV-source wrappers (P1b, P1c, P3) MUST honor the same flag name. This locks
> the §4 runbook canary template; deviating in P1+ rots the acceptance
> criterion.

P0 (PR #343, merged 2026-06-05) shipped:

- `bin/utils/update_migration_helpers.sh` — `umh_resolve_image`,
  `umh_acquire_temp_workspace`, `umh_log_redacted`,
  `umh_print_image_resolution_line`, `umh_require_env_var`.
- `bin/utils/migration_py/_common.py` — `detect_mode`, `resolve_image`,
  `acquire_temp_workspace`, `log_redacted_station_count`, manifest helpers.
- Shellcheck pre-commit + CI gate (carved-out scope, applies to
  `bin/initialize_*_history.sh`).
- Sentinel-fixture guard test.

P1a consumes all of the above and adds no new helpers — only the wrapper,
its tests, the fixture, and the §5.1 runbook entry.

## Problem

Without this wrapper:

- `runoffs` DAY rows older than the operational window cannot be re-populated
  on a deployment that has lost or never imported its historical CSV.
- There is no operator-facing, dry-run-capable, station-filterable path to
  push CSV runoff DAY rows into the API that respects the universal safe-write
  rule (`_has_changes`/setattr bug → never send NULL fields).
- The §5 runbook placeholder remains unactionable — operators cannot proceed
  with the post-Alembic migration step.

## Desired Outcome

After P1a merges to `develop_migration_toolkit`:

- `bin/initialize_runoff_day_history.sh` exists, sources
  `bin/utils/update_migration_helpers.sh`, supports `--dry-run`, `--api-url`,
  `--batch-size`, `--image`, `--station-filter`, and emits the full §4.4
  dry-run inventory plus the resolved image tag line.
- The wrapper runs the per-row POST helper inside the prepgateway container
  with `migration_py/` mounted read-only — no embedded heredoc Python beyond a
  minimal entry shim.
- The §5.1 runbook entry contains literal dry-run, canary, and full-population
  commands plus a link to the acceptance SQL block.
- A sentinel-fixture CSV under `apps/iEasyHydroForecast/tests/fixtures/migration_csv/runoff_day/`
  + a unit test module
  `apps/iEasyHydroForecast/tests/test_initialize_runoff_day.py` cover help
  output, env-file rejection, dry-run empty-CSV behavior, and station-filter
  behavior.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast` passes with
  zero failures and zero unexpected skips.
- `shellcheck -x bin/initialize_runoff_day_history.sh` is clean.
- `ruff check` and `ruff format --check` clean on the new Python test file
  and any new module under `bin/utils/migration_py/`.

## Implementation Plan

### Phase 1: wrapper + Python POST helper module

**Files:**

```
bin/initialize_runoff_day_history.sh                           (NEW)
bin/utils/migration_py/runoff_day.py                           (NEW; stdlib-only POST helper)
```

**Wrapper CLI (mirrors the snow precedent + binding P0 contract):**

```bash
bash bin/initialize_runoff_day_history.sh <env_file_path> \
    [--dry-run] \
    [--api-url URL] \
    [--batch-size N] \
    [--image IMAGE] \
    [--station-filter CODE]
```

**Required behaviors:**

1. **Positional env file**, followed by optional flags. Rejects unknown
   flags. `--help` / `-h` returns 0 with usage text including "Usage:".
2. **`--station-filter`** (BINDING from P0 forward-contract): when set,
   filter source CSV rows to a single `code` before any POST. Honors the
   same flag name as P1b/P1c will.
3. **`--dry-run`**: read CSV, optionally apply station filter and cutoff, and
   print the §4.4 inventory plus the resolved-image line. NO POSTs. Exits 0
   on success.
4. **MODE detection**:
   - Query target via `docker exec sapphire-preprocessing-db psql -U postgres
     -d preprocessing_db -P pager=off -t -A -c "SELECT COUNT(*),
     MIN(date)::text FROM runoffs WHERE horizon_type='DAY';"`
   - Parse `count, min_date` (handle NULL for min_date as "no rows").
   - Pass to `migration_py._common.detect_mode`. Result is `("full-import",
     None)` or `("pre-cutoff", "YYYY-MM-DD")`. Log the resulting MODE line
     prominently.
5. **Image resolution**: `umh_resolve_image` (CLI override → configured
   `ieasyhydroforecast_backend_docker_image_tag` → `latest` fallback);
   `umh_print_image_resolution_line` emits the uniform log line.
6. **Temp workspace**: `umh_acquire_temp_workspace runoff_day` produces a
   strict-perm `umask 077` dir under `${data_root}/logs/runoff_day_tmp/`.
   Trap cleanup on `EXIT INT TERM` via the helper.
7. **POST batches** via the existing `mabesa/sapphire-prepgateway:<tag>`
   image with `migration_py/` mounted read-only. The container runs:
   ```bash
   python3 -m migration_py.runoff_day \
       --csv-path /runoff_day.csv \
       --api-url "$API_URL" \
       --batch-size "$BATCH_SIZE" \
       [--cutoff "$CUTOFF"] \
       [--station-filter "$STATION_FILTER"] \
       [--dry-run]
   ```
   API endpoint: `${SAPPHIRE_API_URL:-http://localhost:8002}/runoff/`
   (POSTs the `RunoffBulkCreate` envelope `{"data": [...]}`).
8. **Cutoff filter**: when `MODE=pre-cutoff`, filter CSV rows to `date <
   cutoff`. When `MODE=full-import`, no date filter.
9. **Payload** (universal safe-write rule from architecture §Q2 layer 2 —
   never send NULL fields the service might overwrite):
   ```json
   {
     "horizon_type": "day",
     "code": "<str>",
     "date": "YYYY-MM-DD",
     "discharge": <float>,
     "horizon_value": <day-of-month>,
     "horizon_in_year": <day-of-year>
   }
   ```
   Required fields per the service Pydantic schema (`RunoffBase`); rows
   without a parseable `discharge` are skipped (logged as `skipped_null` /
   `skipped_parse`). `predictor` is intentionally NOT sent for DAY rows.
10. **Idempotency**: the preprocessing service upserts on `(horizon_type,
    code, date)` — reruns are safe.
11. **Logging**: file under
    `${ieasyhydroforecast_data_root_dir}/logs/runoff_day_history_init/runoff_day_history_init_<UTC>.log`.
    All log lines go through `umh_log_redacted`; the redacted station-count
    line goes through `migration_py._common.log_redacted_station_count`.

**`bin/utils/migration_py/runoff_day.py`** is a small stdlib-only entry
module callable as `python3 -m migration_py.runoff_day`. It accepts the CLI
args above, reads the mounted CSV, builds payloads per §9, and POSTs in
batches via `urllib.request`. It uses `_common.log_redacted_station_count`
for the count line. No third-party imports — the stdlib-import audit
(`_audit.audit_stdlib_only`) verifies this on every test run.

### Phase 2: fixture + tests

**Files:**

```
apps/iEasyHydroForecast/tests/fixtures/migration_csv/runoff_day/sample.csv  (NEW)
apps/iEasyHydroForecast/tests/test_initialize_runoff_day.py                 (NEW)
```

**Fixture content (sentinel only):**

```csv
code,date,discharge
19999,2026-01-01,12.34
19999,2026-01-02,11.22
00000,2026-01-03,5.0
```

Codes are restricted to `19999` and `00000..00009` (HRU sentinels) per the
P0 fixture guard policy.

**Tests:**

1. `test_wrapper_help_returns_zero_and_prints_usage` — `bash
   bin/initialize_runoff_day_history.sh --help` exits 0 and stdout contains
   `"Usage"`.
2. `test_wrapper_rejects_missing_env_file` — invoke with a non-existent
   env-file path; expect non-zero exit and stderr contains a descriptive
   error (e.g. "env file not found").
3. `test_wrapper_dry_run_empty_csv_emits_full_import_mode` — synthetic
   empty CSV fixture (header only) + minimal env file pointing at it;
   expect MODE=full-import in dry-run output and no POSTs attempted.
4. `test_wrapper_dry_run_with_station_filter_reduces_filtered_count` —
   synthetic CSV with sentinel codes `19999` + `00000` (2+1 rows), run with
   `--station-filter 19999`, expect `FILTERED_ROW_COUNT=2` and
   `SOURCE_ROW_COUNT=3`.
5. `test_runoff_day_module_payload_shape` — direct unit test of the
   `migration_py.runoff_day._build_record` helper (if extracted), verifies
   the payload shape on a 2-row CSV (`horizon_type='day'`, no `predictor`
   key, ISO date).

Tests use `subprocess.run([...], capture_output=True)` to invoke the
wrapper because shell behavior is the unit. Tests are skipped only if the
env-file fixture cannot be set up (zero acceptable skips in normal CI).

### Phase 3: §5.1 runbook entry

**File:** `doc/prod/update_data_migration_runbook.md` (MODIFY — only §5.1
between the existing §5 header and the §5.2 placeholder line; do NOT touch
§5.2 / §5.3 / §6 / §7 / §8 / §9 / §10).

**Required content:**

- Header: `### 5.1 Runoff DAY`.
- One-paragraph context (what this migrates, idempotency, MODE branch).
- Literal dry-run command:
  ```bash
  bash bin/initialize_runoff_day_history.sh "$ENV_FILE" --dry-run
  ```
- Literal canary (single-station) command — references the binding
  contract:
  ```bash
  bash bin/initialize_runoff_day_history.sh "$ENV_FILE" --station-filter <code>
  ```
- Literal full-population command:
  ```bash
  bash bin/initialize_runoff_day_history.sh "$ENV_FILE"
  ```
- Acceptance SQL pointer (links forward to §8 once P6/P7 fills it):
  ```bash
  docker exec -i sapphire-preprocessing-db \
    psql -U postgres -d preprocessing_db -P pager=off <<SQL
  SELECT horizon_type, COUNT(*) AS rows, MIN(date), MAX(date)
  FROM runoffs WHERE horizon_type='DAY' GROUP BY horizon_type;
  SQL
  ```

## Acceptance Criteria

### A. Wrapper

- [ ] `bin/initialize_runoff_day_history.sh` exists, executable bit set
      (or invoked via `bash`), sources `bin/utils/update_migration_helpers.sh`.
- [ ] `--help` / `-h` returns 0 and prints `"Usage"`.
- [ ] `--station-filter <code>` accepts a single code and is honored in
      both dry-run and write paths.
- [ ] `--dry-run` emits MODE, TARGET_COUNT, TARGET_MIN_DATE, CUTOFF,
      SOURCE_FILES, SOURCE_ROW_COUNT, FILTERED_ROW_COUNT, SOURCE_DATE_MIN,
      SOURCE_DATE_MAX, DISTINCT_STATION_COUNT_REDACTED, IMAGE.
- [ ] `--image` override wins; otherwise resolves
      `mabesa/sapphire-prepgateway:${ieasyhydroforecast_backend_docker_image_tag}`;
      otherwise falls back to `:latest`. Resolution line logged via
      `umh_print_image_resolution_line`.
- [ ] Pre-cutoff branch filters source rows to `date < cutoff`.
- [ ] Full-import branch applies no date filter.
- [ ] Payload contains only non-NULL fields per universal safe-write rule:
      `horizon_type, code, date, discharge, horizon_value, horizon_in_year`.
      No `predictor` key for DAY rows.
- [ ] `shellcheck -x bin/initialize_runoff_day_history.sh` clean.

### B. Python helper

- [ ] `bin/utils/migration_py/runoff_day.py` exists and is stdlib-only
      (verified by `migration_py._audit.audit_stdlib_only`).
- [ ] Module runs as `python3 -m migration_py.runoff_day` with the CLI
      flags above.
- [ ] `ruff check bin/utils/migration_py/runoff_day.py` clean.
- [ ] `ruff format --check bin/utils/migration_py/runoff_day.py` clean.

### C. Tests

- [ ] `apps/iEasyHydroForecast/tests/test_initialize_runoff_day.py` lives
      alongside the existing `test_migration_*.py` modules.
- [ ] `apps/iEasyHydroForecast/tests/fixtures/migration_csv/runoff_day/sample.csv`
      exists and contains only sentinel codes (`19999`, `00000..00009`).
- [ ] The four wrapper tests in Phase 2 pass.
- [ ] `SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast` passes
      with zero failures and zero unexpected skips.

### D. Runbook

- [ ] §5.1 in `doc/prod/update_data_migration_runbook.md` is filled with
      the literal commands listed in Phase 3.
- [ ] §5.2, §5.3, §6, §7, §8, §9, §10 are untouched.
- [ ] No emoji anywhere in the runbook.

### E. Repo-wide hygiene

- [ ] No real station codes, env-file contents, or discharge values
      anywhere in modified files. (Sentinel `19999` and `00000..00009`
      only.)
- [ ] No files outside the P1a file-scope list were modified.
- [ ] No edits to `sapphire/services/`.
- [ ] PR opens against `develop_migration_toolkit` referencing this
      gi_draft + the P0 forward-contract.

## Testing

### Unit tests

`apps/iEasyHydroForecast/tests/test_initialize_runoff_day.py` runs under
`SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast`. The
existing 321 baseline tests plus the new module collectively must show
zero failures, zero unexpected skips.

### Linting

```bash
shellcheck -x bin/initialize_runoff_day_history.sh
cd apps/iEasyHydroForecast && uv run ruff check ../../bin/utils/migration_py/ tests/test_initialize_runoff_day.py
cd apps/iEasyHydroForecast && uv run ruff format --check ../../bin/utils/migration_py/runoff_day.py tests/test_initialize_runoff_day.py
```

### CI verification

After the PR opens on `develop_migration_toolkit`:

- `test_ieasyhydroforecast` job exercises the new test module.
- `shellcheck_migration_scripts` job exercises
  `bin/initialize_runoff_day_history.sh` and the existing helper.
- Both jobs must report green before review.

### No integration tests in P1a

Disposable docker-compose integration tests are out of scope per
architecture §Q7 (separate sprint). Dry-run output and unit-level payload
verification are sufficient for P1a.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Service-side `_has_changes` bug overwrites existing non-NULL discharge with NULL | Wrapper sends only non-NULL fields (universal safe-write rule). Rows without a parseable discharge are skipped, not sent with `discharge=null`. |
| Cross-org station codes leak in via an unfiltered archive CSV | Wrapper logs only redacted counts (`migration_py._common.log_redacted_station_count`). Operator filters via `--station-filter` for canary; full-population implicitly trusts the deployment's own `intermediate_data/runoff_day.csv` which is per-deployment. |
| Image tag resolves to `:local` or `:latest` on a deployment server | `umh_resolve_image` logs a WARNING (Python side); the shell helper additionally elevates the warning to stderr when `sapphire-preprocessing-db` container is detected via `docker ps`. |
| psql query returns NULL min_date but non-zero count (corrupted/cleared table) | `migration_py._common.detect_mode` returns `("full-import", None)` for this case — never produces `WHERE date < NULL`. Covered by P0 test `test_detect_mode_null_min_date_returns_full_import`. |
| Wrapper test fixture leaks real station codes via copy-paste | P0 fixture guard test `test_migration_fixture_guard.py` walks the fixtures dir and fails on any 5-digit code outside `{19999} ∪ {00000..00009}`. Tested at every test run. |

## Out of Scope (P1a explicitly does not ship)

- Runoff PENTAD/DECADE migration (P2a).
- Meteo CSV migration (P1b — parallel sibling phase).
- Snow CSV migration (P1c — parallel sibling phase).
- Hydrograph DAY migration (P3 — depends on P1a).
- Live integration tests against a real docker-compose stack.
- Service-side `_has_changes` fix (`sapphire/services/` is colleague-managed).
- Modification of the existing snow wrapper (P0 left it as-is).

## Dependencies

- **Depends on:** P0 (PR #343, merged 2026-06-05). Requires
  `bin/utils/update_migration_helpers.sh` and
  `bin/utils/migration_py/_common.py` on the integration branch.
- **Blocks:** P3 (hydrograph DAY — `Depends on: P1a` per architecture §Q6).
- **Sibling-parallel with:** P1b (meteo), P1c (snow) — those edit §5.2 /
  §5.3 of the same runbook; P1a touches ONLY §5.1.

## References

- Architecture plan: `doc/plans/working/update_migration_toolkit_architecture.md`
  (especially §Q1, §Q2 layer 2, §Q3, §Q4, §Q6 P1a row).
- Stage A discovery: `doc/plans/working/update_migration_phase1_discovery.md` §A.
- P0 foundation gi_draft: `doc/plans/issues/high_prio_gi_draft_update_migration_p0_foundation.md`
  (especially "Forward interface contracts" + §4.3 canary template).
- Sub-orchestrator Charter v2:
  `doc/plans/working/update_migration_suborchestrator_charter.md`.
- P0 helper (do not modify): `bin/utils/update_migration_helpers.sh`.
- P0 Python primitives (do not modify): `bin/utils/migration_py/_common.py`.
- Snow precedent (do not modify): `bin/initialize_snow_history.sh` on
  `infra_taj_sapphire_2_deployment`.
- Runoff schema (read-only): `sapphire/services/preprocessing/app/schemas.py`
  `RunoffCreate`.
- Runoff API endpoint (read-only): `sapphire/services/preprocessing/app/main.py:58`.
