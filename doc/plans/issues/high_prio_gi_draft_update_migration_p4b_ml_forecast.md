# P4b - ML Forecast Laptop-Export Migration Wrappers (TFT / TiDE / TSMixer)

**Status:** Implementation complete; PR open against `develop_migration_toolkit`.
**Module:** infra (cross-module — `bin/`, `apps/iEasyHydroForecast/tests/`, `doc/prod/`, `doc/plans/issues/`)
**Priority:** High
**Branch:** `feature_p4b_ml_forecast_history` → `develop_migration_toolkit`
**Labels:** `infra`, `tooling`, `tests`, `deployment-runbook`

---

## Summary

Adds the laptop-export → CSV+manifest → server-import migration pipeline for
ML forecast rows of `sapphire-postprocessing-db.forecasts`, filtered to
`model_type IN ('TFT','TiDE','TSMixer')`. P4b is the postprocessing-DB sibling
of P4a (LR forecasts) — same shape, different `model_type` enum subset and
two architectural quirks (enum case + default horizon storage) that the
in-service migrator silently mishandled.

The P4b implementation lands three new artifacts and one runbook section:

1. `bin/export_ml_forecast_history.sh` — laptop-side wrapper that pulls rows
   from `sapphire-postprocessing-db.forecasts` into a CSV + sidecar manifest
   pair, with a Stage E item #6 location guard refusing to run on a
   deployment-server host.
2. `bin/initialize_ml_forecast_history.sh` — server-side wrapper that
   validates the manifest, resolves the deployment Docker image, runs MODE
   detection (filtered to ML model_types only), and dispatches to the Python
   helper module.
3. `bin/utils/migration_py/ml_forecast.py` — stdlib-only Python helper that
   builds API payloads, applies station/cutoff/model/horizon filters, and
   POSTs records to the postprocessing API.
4. `doc/prod/update_data_migration_runbook.md §6.4` — operator-facing
   procedure: laptop export with secret hygiene, manifest validation,
   canary single-station, full population, legacy-row migration,
   acceptance SQL.

Plus 24 pytest cases under `apps/iEasyHydroForecast/tests/`:
`test_export_ml_forecast.py` (committed in `07a3ff5`) and
`test_initialize_ml_forecast.py` (this PR's last commit).

The two P4b-specific quirks the gi_draft documents are:

1. **`MODEL_DIR_TO_API` enum case mapping.** The API `ModelType` enum values
   are MIXED-CASE — `TFT`, `TiDE`, `TSMixer` — not the legacy uppercase
   on-disk directory spellings (`TIDE`, `TSMIXER`). The Python helper
   exposes the `MODEL_DIR_TO_API` constant to map both forms to the
   canonical API spelling. Unknown spellings raise `UnknownMLModelTypeError`
   and the affected row is counted as `SKIPPED_UNKNOWN_MODEL` — no silent
   fallback. The legacy in-service migrator's hardcoded model strings
   silently mis-stored TiDE rows as `TIDE` (which the API rejected on
   ingestion); this wrapper detects + reports the mismatch instead.

2. **Default `horizon_type='day'` per user-lock L6.** Modern ML CSV-derived
   writes go in as `horizon_type='day'` regardless of any source-row
   `horizon_type` cell. This matches the operational writer at
   `apps/machine_learning/scr/utils_ml_forecast.py:_write_ml_forecast_to_api`
   (commit `1cb3495`) — the modern ML pipeline only stores `day`. The opt-in
   `--preserve-legacy-ml-horizons` flag instead preserves the source row's
   `horizon_type` (`pentad` / `decade`) for the pre-1cb3495 legacy rows;
   when active, the wrapper emits a prominent WARNING log line so the
   operator cannot accidentally bypass user-lock L6.

## Context

The update-time migration toolkit (Stage E of the broader
`update_migration_toolkit_architecture.md` plan) moves historical data from
laptop databases / CSVs into the deployment server's API-backed services
during the cutover-to-services transition. P4b covers the third of three
forecast-table sources:

| Phase | Source                             | Target table                         |
|-------|------------------------------------|--------------------------------------|
| P4a   | laptop `postprocessing_db` LR rows | `postprocessing.forecasts` (LR rows) |
| P4b   | laptop `postprocessing_db` ML rows | `postprocessing.forecasts` (ML rows) |
| P5    | laptop long-term forecast CSV      | `postprocessing.long_forecasts`      |

Both forecast-row phases use the same target table (`forecasts`) and the
same upsert key `(horizon_type, code, model_type, date, target)`, so they
are operationally independent — running P4a does not affect P4b's MODE
detection because the wrapper's `query_target_state` filters to its three
model_types only.

The legacy in-service migrator (`sapphire/services/postprocessing/app/
data_migrator.py:350-354`) hardcoded the three ML model strings as uppercase
(`TFT`, `TIDE`, `TSMIXER`) when the API enum is mixed-case. Stage A §E
documented this as a live-test result; the wrapper's `MODEL_DIR_TO_API`
constant maps both forms to the canonical API spelling so the legacy
on-disk artifacts (and any operator who copies the legacy migrator's
strings) work without code changes elsewhere.

## Goals

- Migrate ML forecast history (TFT / TiDE / TSMixer) from laptop DB to
  deployment-server API safely and idempotently.
- Enforce the universal safe-write rule (architecture §Q2 layer 2): never
  send `null` for an absent source field; let the service-side upsert
  preserve any pre-existing non-NULL target value.
- Surface the two architectural quirks (enum case mismatch, default `day`
  horizon storage) in operator-facing logs and runbook so neither can be
  silently bypassed.
- Reuse the P0 helpers (`bin/utils/update_migration_helpers.sh`) for image
  resolution, manifest validation, temp workspace acquisition, and redacted
  logging — no new helper functions added to `_common.py`.

## Non-goals

- Live-DB integration tests against a running sapphire postprocessing stack
  (architecture §Q7 — disposable integration belongs to a separate sprint).
- Read-before-merge (`--strict-merge`-style) flag for ML forecasts —
  forecast rows have many optional quantile fields, but the operational
  concern that motivated `--strict-merge` for hydrographs has not surfaced
  for forecasts. Out of scope for P4b; reopen if observed in production.
- Migration of long-term forecasts (P5) or LR forecasts (P4a) — those have
  their own gi_drafts.
- Updates to `sapphire/services/postprocessing/` — read-only per CLAUDE.md
  ownership boundary; this PR adds no schema or service changes.

## Implementation

### File scope

The PR adds:

- `bin/export_ml_forecast_history.sh` — laptop-side export (committed
  in `7698141`).
- `bin/initialize_ml_forecast_history.sh` — server-side import (committed
  in `6c7b70d`).
- `bin/utils/migration_py/ml_forecast.py` — Python helper module (committed
  in `6c7b70d`).
- `apps/iEasyHydroForecast/tests/test_export_ml_forecast.py` — export
  wrapper tests + sentinel fixture guard (committed in `07a3ff5`).
- `apps/iEasyHydroForecast/tests/fixtures/migration_csv/ml_forecast/TFT_sample.csv`
- `apps/iEasyHydroForecast/tests/fixtures/migration_csv/ml_forecast/TiDE_sample.csv`
- `apps/iEasyHydroForecast/tests/test_initialize_ml_forecast.py` — import
  wrapper + helper tests (this PR's last code commit).
- `doc/prod/update_data_migration_runbook.md` §6.4 — runbook section (this
  PR's last doc commit).
- `doc/plans/issues/high_prio_gi_draft_update_migration_p4b_ml_forecast.md`
  — this gi_draft.

Manifest sidecars (`*.manifest`) are intentionally absent from the shipped
fixtures: project gitignore excludes `*.manifest` so the operational
`.manifest` files never leak into git, and the import tests generate
fresh manifests via `tmp_path` to exercise both happy-path and failure
cases without storing canned manifests in the repo.

No edits to `sapphire/services/`, no schema migrations, no changes to the
P0 helpers.

### Key design decisions

#### `MODEL_DIR_TO_API` constant (Stage A §E mitigation)

The Python module exposes:

```python
MODEL_DIR_TO_API: dict[str, str] = {
    "TFT": "TFT",
    "TIDE": "TiDE",
    "TSMIXER": "TSMixer",
    "TiDE": "TiDE",          # idempotent for API form
    "TSMixer": "TSMixer",    # idempotent for API form
}
```

The `resolve_model_type(raw)` helper raises `UnknownMLModelTypeError` on
empty / None / unrecognized input. Both the `--model` CLI flag (operator
filter) and per-row `model_type` parsing go through this helper. Per-row
unknown values are caught by the reader and counted as
`SKIPPED_UNKNOWN_MODEL` rather than raised, so a partial-bad export still
migrates its good rows. The `--model` flag value is resolved once at
start; an operator typo there fails fast with a clear error before any
docker run.

#### Default `horizon_type='day'` (user-lock L6)

`_build_record(row, *, preserve_legacy_horizons)` selects horizon_type
per the following table:

| `preserve_legacy_horizons` | Source `horizon_type` | Payload `horizon_type` | `horizon_value`     |
|----------------------------|-----------------------|------------------------|---------------------|
| `False` (default)          | `day` or unset        | `day`                  | day-of-year(target) |
| `False` (default)          | `pentad` / `decade`   | row dropped, counted as `SKIPPED_HORIZON` | n/a    |
| `False` (default)          | other                 | row dropped, counted as `SKIPPED_HORIZON` | n/a    |
| `True`                     | `pentad`              | `pentad`               | `0` (legacy zero-fill) |
| `True`                     | `decade`              | `decade`               | `0` (legacy zero-fill) |
| `True`                     | `day` or other        | `day`                  | day-of-year(target) |

Note that `horizon_value=0` for legacy rows matches the legacy
`ForecastDataMigrator` convention; the modern pipeline writes day-of-year
for `day` rows.

#### Universal safe-write (architecture §Q2 layer 2)

Every quantile (`q05`/`q25`/`q75`/`q95`), `forecasted_discharge`, and `flag`
field is only included in the payload when the source CSV cell parses to a
non-null value. Empty / `nan` / `null` / unparseable cells are silently
omitted from the record dict. `Q50` is a special case: it parses out
identically but maps to `forecasted_discharge` (the API schema has no
separate `q50` field — the median quantile IS the central estimate, per
the legacy `ForecastDataMigrator` convention).

Float parse explicitly rejects `inf` / `-inf` / `NaN` so `json.dumps` cannot
emit non-RFC-7159 values to the API (the schema would reject them anyway,
but failing locally surfaces the problem earlier with the offending CSV row
identified in the wrapper log).

#### MODE detection filtered to ML model_types

The wrapper's `query_target_state` runs:

```sql
SELECT COUNT(*), COALESCE(MIN(date)::text, '')
  FROM forecasts
 WHERE model_type IN ('TFT','TiDE','TSMixer');
```

so a populated `forecasts` table with only LR rows still triggers
`MODE=full-import` for the ML migration (and vice versa). Reruns are
idempotent on the upsert key.

#### Manifest contract (P0)

The wrapper invokes `umh_validate_export_manifest <CSV> ml_forecast` before
any docker run. The 5 required keys (`export_type`, `row_count`,
`station_count`, `date_min`, `date_max`) are enforced via
`migration_py._common.validate_manifest`. Failure modes:

- `ManifestMissingError` — sidecar file absent.
- `ManifestExportTypeMismatchError` — manifest declares a different
  `export_type` (catches wrong wrapper used for the CSV).
- `ManifestRowCountMismatchError` — manifest `row_count` does not equal
  parsed CSV row count (catches partial transfers).
- `ManifestStationCountMismatchError` — manifest `station_count` does not
  equal distinct `code` count in CSV (catches unfiltered / cross-org
  exports — security-relevant per v2 R6).
- `ManifestDateRangeMismatchError` — manifest `date_min` / `date_max` do
  not match CSV's min/max of `date` column (catches stale-file rerun).

#### Stdlib-only audit

The Python helper imports `_common` via relative import (`from . import
_common`) and has no third-party dependencies. The `test_migration_audit.py`
test (P0) walks every `*.py` under `bin/utils/migration_py/` and rejects
any non-stdlib root import via `ast.parse` +
`sys.stdlib_module_names`; the new `ml_forecast.py` passes the audit
unchanged. `test_initialize_ml_forecast.py::test_ml_forecast_module_imports_only_stdlib_and_intra_package`
re-runs the audit explicitly so a future module addition that breaks the
rule surfaces immediately.

## Stage E item coverage

- **Stage E item #2** (manifest travels with export, validated by wrapper):
  the export script emits `<csv>.manifest`; the import wrapper calls
  `umh_validate_export_manifest` before any docker run. Tests exercise the
  4 documented failure modes.
- **Stage E item #6** (location guard on laptop scripts): the export
  script detects deployment-server markers via
  `docker ps --filter name=sapphire-postprocessing-db` and refuses to run
  unless `--i-am-on-laptop` is set (charter-compliant bypass; the prior
  `--allow-server-host` flag was renamed in the round-1 review). The
  location-guard documentation is surfaced in `--help` (see
  `test_wrapper_help_documents_location_guard` in
  `test_export_ml_forecast.py`).
- **Stage E item #7** (canary single-station acceptance criterion):
  runbook §6.4.3 mandates the canary step before full population; the
  acceptance SQL block scopes the verification to a single station.
- **Stage E item #8** (stdlib import audit): `_audit.audit_stdlib_only`
  covers `ml_forecast.py` automatically as a new sibling under
  `migration_py/`.
- **Stage E item #11** (no `test_*.py` or `conftest.py` inside fixture
  dirs): the shipped fixtures are only `*.csv` files — no Python.
- **User-lock L6** (default ML CSV writes use `horizon_type='day'`): the
  `_build_record` helper enforces this; opting out requires explicit
  `--preserve-legacy-ml-horizons` flag + dedicated WARNING log line.

## Test plan

`apps/iEasyHydroForecast/tests/test_initialize_ml_forecast.py` adds 24 new
test cases across 8 sections:

1. Wrapper CLI surface (5 tests): wrapper exists, `--help` returns 0 and
   lists all P4b flags, rejects missing env file, rejects missing
   `--from-export`, documents the binding `--station-filter` interface
   contract, documents the `--preserve-legacy-ml-horizons` WARNING.
2. Manifest validation (5 tests): happy path + 4 documented failure modes
   (missing manifest, row_count mismatch, wrong export_type,
   station_count mismatch).
3. `MODEL_DIR_TO_API` enum case mapping (4 tests): TFT, TIDE→TiDE,
   TSMIXER→TSMixer, unknown raises.
4. `_build_record` payload shape (5 tests): default horizon='day' from
   target day-of-year; legacy pentad/decade preserved with flag; default
   skips non-day source rows; NULL quantile fields omitted; missing
   required fields return None.
5. `_read_filtered_records` filters (4 tests): station filter, cutoff
   filter, model filter normalizes case (TIDE→TiDE), unknown source
   model is counted not raised.
6. `main()` dry-run inventory (4 tests): per-model breakdown emitted,
   model filter restricts breakdown, legacy flag emits WARNING in
   stdout, missing CSV returns non-zero.
7. Stdlib-only audit (1 test).
8. Fixture round-trip (1 test): the shipped `TFT_sample.csv` parses
   through the helper end-to-end.

Combined with the 7 tests in `test_export_ml_forecast.py` (committed in
`07a3ff5`), the PR adds 31 P4b-specific test cases.

Full module test run passes (424 total in `iEasyHydroForecast`):

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast
# => 424 passed, 0 failed, 0 unexpected skips
```

## Acceptance criteria

- [x] All commits passed the pre-commit hooks (ruff legacy + format + run
      integration tests + shellcheck where applicable).
- [x] `SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast` returns
      zero failures, zero unexpected skips.
- [x] No real station codes in any committed file (sentinel `19999` and
      `00000`-class only — `test_export_ml_forecast.py` enforces this on
      fixtures; the runbook examples use `19999`).
- [x] No edits to `sapphire/services/`.
- [x] Wrapper `--help` documents `--from-export`, `--station-filter`,
      `--preserve-legacy-ml-horizons`, and `--model` (verified by tests).
- [x] Manifest validation happens BEFORE any docker run (no API POST is
      attempted with an invalid manifest).
- [x] Dry-run inventory emits the per-model breakdown line
      `ML_PER_MODEL_COUNTS={TFT: <n>, TiDE: <n>, TSMixer: <n>}`.
- [x] `--preserve-legacy-ml-horizons` emits a prominent WARNING log line
      both at the wrapper-shell layer and in the dry-run inventory.
- [x] `MODEL_DIR_TO_API` mapping covers both legacy uppercase dir spellings
      and canonical mixed-case API spellings, idempotently.
- [x] Stdlib-only audit passes for `ml_forecast.py`.
- [x] Runbook §6.4 covers Background, Laptop-side export, Manifest
      validation, Canary, Full population, Legacy-row migration, and
      Acceptance SQL.

## Reversibility

The wrapper is idempotent on the postprocessing service's upsert key
`(horizon_type, code, model_type, date, target)`. A rerun does not
duplicate rows. To roll back a migration entirely, restore the pre-migration
backup taken in §4.1 of the runbook (`pg_restore` against the
`pre_update_migration_<UTC>` dump) — this is the literal block mandated by
user-lock #2.

The legacy `--preserve-legacy-ml-horizons` rows can be selectively cleared
by:

```sql
DELETE FROM forecasts
 WHERE model_type IN ('TFT','TiDE','TSMixer')
   AND horizon_type IN ('pentad','decade');
```

so a future operator who accidentally migrated legacy rows can clean up
without touching the modern `day` rows.

## Risks and mitigations

| Risk                                                       | Mitigation                                                  |
|-----------------------------------------------------------|-------------------------------------------------------------|
| Operator runs without `--preserve-legacy-ml-horizons`, expecting legacy rows | Default-OFF reader drops them as `SKIPPED_HORIZON` (visible in inventory); runbook §6.4.3 reads dry-run before write |
| Operator runs WITH the legacy flag by mistake             | Wrapper + helper both emit prominent WARNING log lines      |
| Source CSV has typo in `model_type` (e.g. `TFt`)           | Row counted as `SKIPPED_UNKNOWN_MODEL`; visible in inventory |
| Manifest sidecar lost during transfer                      | `ManifestMissingError` aborts BEFORE docker run             |
| Unfiltered laptop export (cross-org station codes)         | `ManifestStationCountMismatchError` catches the mismatch    |
| Stale export reused from prior run                         | `ManifestDateRangeMismatchError` catches stale dates        |
| LR forecasts (P4a) and ML forecasts (P4b) share `forecasts` | MODE detection query filters to ML model_types only         |
| Existing non-NULL field gets erased by NULL incoming        | Universal safe-write rule omits NULL fields from the payload |

## Follow-ups

- Add a `--strict-merge` flag if forecast-stat erasure surfaces in
  production (mirrors the deferred P3 hydrograph DAY follow-up; not needed
  preemptively).
- After P4a + P4b both ship, consider unifying the per-row payload builder
  into a shared `migration_py/_forecast_record.py` if the LR and ML
  builders are 90%+ identical. Not in scope for this PR — both wrappers
  ship independently first.

## References

- Architecture plan: `doc/plans/working/update_migration_toolkit_architecture.md`
- Sub-orchestrator charter v2:
  `doc/plans/working/update_migration_suborchestrator_charter.md`
- P4a sibling: `doc/plans/issues/high_prio_gi_draft_update_migration_p4a_lr_forecast.md`
- P0 helpers: `bin/utils/update_migration_helpers.sh`,
  `bin/utils/migration_py/_common.py`
- Modern operational ML writer (commit `1cb3495`):
  `apps/machine_learning/scr/utils_ml_forecast.py:_write_ml_forecast_to_api`
- Legacy in-service migrator (decommissioning):
  `sapphire/services/postprocessing/app/data_migrator.py:350-354`
- Runbook §6.4: `doc/prod/update_data_migration_runbook.md`
- User-lock L6 (default ML CSV writes use `horizon_type='day'`).
- Stage A §E (enum case live-test result).
