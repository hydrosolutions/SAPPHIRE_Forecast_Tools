# P2a: runoff PENTAD/DECADE laptop-export -> CSV -> API push

## Summary

Adds the laptop-export -> CSV+manifest -> server-import pipeline for the
narrow `runoffs` PENTAD/DECADE table, completing the runoff family for the
update-time migration toolkit (sibling to P1a which covers runoff DAY via
the CSV-source path, and to P2b which covers hydrograph PENTAD/DECADE via
the same DB-source shape but with a much wider payload).

This is the **first DB-source phase to merge end-to-end on
`develop_migration_toolkit`** with the P0 manifest contract exercised on
both the emit side (export script writes `<csv>.manifest`) and the
validate side (server-side wrapper rejects mismatches before any POST).

## Scope

Files added or modified:

- `bin/export_runoff_period_history.sh` (NEW) — laptop-side export
- `bin/initialize_runoff_period_history.sh` (NEW) — server-side import
- `bin/utils/migration_py/runoff_period.py` (NEW) — stdlib-only POST helper
- `apps/iEasyHydroForecast/tests/test_export_runoff_period.py` (NEW) — 18 tests
- `apps/iEasyHydroForecast/tests/test_initialize_runoff_period.py` (NEW) — 32 tests
- `apps/iEasyHydroForecast/tests/fixtures/migration_csv/runoff_period/` (NEW)
  - `pentad_sample.csv` + `pentad_sample.csv.manifest`
  - `decade_sample.csv` + `decade_sample.csv.manifest`
- `doc/prod/update_data_migration_runbook.md` (MOD) — §6 preamble + §6.1
- `doc/plans/issues/high_prio_gi_draft_update_migration_p2a_runoff_period.md` (NEW, this file)

Out of scope (deliberately, per brief):

- No edits under `sapphire/services/` — the API contract is fixed and
  served by the existing `RunoffBulkCreate` endpoint.
- No edits to §6.2 of the runbook — that section is P2b's territory and
  lands on its own branch (PR #350).
- No placeholders for §6.3+ siblings — P4a / P4b / P5 add their own
  sections.

## Load-bearing schema correction: `discharge`, not `discharge_avg`

An earlier sub-orch brief mistakenly named the runoff value column
`discharge_avg`. The actual DB column on the `runoffs` model is
`discharge`:

```python
# sapphire/services/preprocessing/app/models.py:16-28
class Runoff(Base):
    __tablename__ = "runoffs"
    ...
    # Runoff values
    discharge = Column(Float)
    predictor = Column(Float)
```

The API payload key on `RunoffBase` is also `discharge`
(`sapphire/services/preprocessing/app/schemas.py:7-14`). The
`discharge_avg` name does appear in the codebase — but only as the
CSV-source migrator's column name when ingesting the wide
`intermediate_data/runoff_pentad.csv` shape:

```python
# sapphire/services/preprocessing/app/data_migrator.py:202-216
def prepare_pentad_data(self, df: pd.DataFrame) -> List[Dict]:
    ...
    "discharge": float(row['discharge_avg']) if pd.notna(row['discharge_avg']) else None,
    ...
```

The CSV column `discharge_avg` -> payload key `discharge` rename happens
inside that migrator; the DB-source path that this PR adds reads the
canonical DB column directly and forwards it under the same payload key.
There is no rename anywhere in the P2a pipeline.

The correction is enforced at three levels in this PR:

1. **Export script** — the `COPY (SELECT ...)` lists `discharge` as a
   column name. ShellCheck-clean, runs against the laptop's DB.
2. **Python helper** — `_FLOAT_FIELDS = ("discharge", "predictor")`.
   `_build_record()` reads `row.get("discharge")` and forwards under
   the same key.
3. **Test suite** — `test_build_record_uses_discharge_not_discharge_avg`
   plus `test_pentad_csv_uses_discharge_not_discharge_avg` /
   `test_decade_csv_uses_discharge_not_discharge_avg` lock in the
   correct name at both the code and the fixture level, so a future
   contributor cannot silently rename it back.

## Design decisions

### No `--strict-merge` follow-up

P2b ships `--strict-merge` as a parsed-but-deferred CLI flag (logs a
warning, falls back to enrichment-only) because the wide hydrograph
PENTAD/DECADE row carries a real stat-erasure risk under the
service-side `_has_changes` + `setattr` overwrite-with-NULL path
(architecture §Q2 layer 2).

P2a deliberately does NOT introduce a `--strict-merge` flag. The runoff
PENTAD/DECADE row is narrow — only `discharge` and `predictor` are
nullable, and neither is computed from quantiles or year-rotated source
data. The wide-stat-erasure scenario does not apply. The enrichment-only
default is sufficient and the dry-run inventory's
`SAFE_WRITE_POLICY=enrichment-only (default)` line confirms it to the
operator before any write.

If a future need arises (e.g., a new wide `runoffs` schema variant), the
P2b `--strict-merge` shape is the documented forward path; a follow-up
PR can copy the flag verbatim.

### Manifest contract validated end-to-end

The P0 helper `migration_py._common.validate_manifest` enforces 5
required keys on the sidecar: `export_type`, `row_count`,
`station_count`, `date_min`, `date_max`. This PR is the first DB-source
phase to wire both sides of the contract:

- The export script computes `row_count`, `station_count`, `date_min`,
  `date_max` from the CSV it just wrote (NOT from the stations-file
  input count) so the contract catches mid-flight tampering.
- The server-side wrapper calls `umh_validate_export_manifest` BEFORE
  any image work or psql probes, so a stale or cross-org CSV is
  rejected up front (the security-relevant leak case from P0 §Q5).

The shipped fixture pair (pentad + decade) round-trips through the
parser in the test suite — proof that the contract holds across the
manifest write -> validate boundary.

### Lowercase `horizon_type` on payload

The DB row uses uppercase `'PENTAD'` / `'DECADE'` (SQLAlchemy enum
storage), but the API accepts lowercase via the FastAPI Pydantic
HorizonType enum (`pentad`, `decade`). The export script emits
`lower(horizon_type::text)` in the `COPY (SELECT ...)` to keep the
serialized form consistent across the Python helper and the wire
payload — matching the convention established by P1a and P2b.

### MODE detection per-horizon

The wrapper queries the target preprocessing-db once via `docker exec
psql`, filtered on `horizon_type='PENTAD'` or `'DECADE'`. This avoids
the cross-horizon cutoff bug where a populated PENTAD table would
suppress writes to an empty DECADE table (or vice versa). Each wrapper
invocation handles one horizon and emits the corresponding MODE label
to the dry-run inventory.

### Stdlib-only Python helper

`bin/utils/migration_py/runoff_period.py` imports only stdlib modules
plus the intra-package `_common`. The `_audit.audit_stdlib_only` test
enforces this rule across the whole `migration_py` package; the P2a
test suite re-runs it as a regression guard. This keeps the helper
self-contained for the prepgateway image without dragging a numpy /
pandas dependency tree along.

## CLI surface

### Export (laptop side)

```text
bash bin/export_runoff_period_history.sh \
    --horizon pentad|decade \
    --stations-file PATH \
    --out-dir PATH \
    [--dry-run]
```

- `--horizon`: which subset of `runoffs` to export. Mandatory; rejects
  any other value.
- `--stations-file`: deployment's station-code list, one per line.
  Empty lines + whitespace trimmed. SQL-injection guard rejects any
  line containing an apostrophe.
- `--out-dir`: created with mode `0700` if missing; CSV + manifest are
  written with mode `0600`.
- `--dry-run`: runs the COUNT(*) shape; emits nothing to disk.

Location guard: if any
`sapphire-{preprocessing,postprocessing,api-gateway}-...` container is
visible via `docker ps`, the script refuses with a clear message
pointing the operator at the server-side wrapper instead. Testing-only
bypass via `_P2A_EXPORT_SKIP_LOCATION_GUARD=1`.

### Import (server side)

```text
bash bin/initialize_runoff_period_history.sh <env_file_path> \
    --from-export <csv_path> \
    --horizon pentad|decade \
    [--dry-run] [--api-url URL] [--batch-size N] [--image IMAGE] \
    [--station-filter CODE]
```

- `env_file_path`: positional; the `.env_<org>` file.
- `--from-export`: path to the transferred CSV. The sibling
  `<PATH>.manifest` must exist.
- `--horizon`: `pentad` or `decade`. Mandatory.
- `--api-url`: default `http://localhost:8002/runoff/`.
- `--batch-size`: default 500.
- `--image`: docker image override; falls back to the P0 helper
  resolution (CLI -> configured tag -> `:latest`).
- `--station-filter`: P0 binding interface contract; honored
  identically by P1a / P1b / P1c / P2b / P3.

## Test coverage

50 tests total across the two new test files (24 above the brief's
18-minimum target). All pass with zero skips except the
`test_location_guard_fires_when_sapphire_containers_present` predicate,
which auto-skips when the SAPPHIRE stack is not running locally — a
deterministic-skip permitted by the Zero Skips Policy because the guard
physically cannot fire without the trigger condition.

Highlights of the test surface:

- CLI surface: `--help`, missing-arg rejection, invalid-horizon
  rejection, station-filter contract documented in help text.
- Record building: required-field enforcement, optional-field omission
  when NULL/NaN/blank/garbage, legacy `discharge_avg` column ignored,
  non-finite-float rejection.
- Filtering: station-filter + cutoff combinations, distinct-code
  counter, date-range tracking pre-filter.
- Manifest validation: missing-manifest, row-count mismatch, wrong
  `export_type`, missing-column CSV.
- Dry-run inventory shape: every documented `KEY=value` line is
  asserted, including `TARGET_TABLE=runoffs`, `HORIZON_TYPE=PENTAD`,
  `MODE=pre-cutoff (cutoff=...)`, `DISTINCT_STATION_COUNT_REDACTED=N`,
  `SAFE_WRITE_POLICY=enrichment-only (default)`.
- Stdlib-only audit: re-runs the P0 `_audit.audit_stdlib_only` check on
  the whole `migration_py` package.
- Fixture round-trip: shipped pentad + decade CSV+manifest pairs parse
  through `_read_filtered_records_with_manifest` and produce the
  expected record + counter + date-range tuple.
- Schema correction lock-ins: dedicated tests at both the code level
  (payload key) and fixture level (CSV header) that fail loudly if
  anyone renames `discharge` back to `discharge_avg`.

Full `iEasyHydroForecast` suite at the end of the PR: 434 passed, 0
failed, 0 unexpected skips.

## Sentinel discipline

Every fixture and test uses sentinel codes only (`19999` and the
`00000`..`00009` HRU range). No real station codes appear in any
committed artifact. The shipped fixture CSVs were grepped for any 5-digit
sequence that isn't in the sentinel allowlist as part of the test suite
(`test_pentad_sample_fixture_only_sentinel_codes` /
`test_decade_sample_fixture_only_sentinel_codes`).

## Operational guidance

The runbook §6.1 walks the operator through the full pipeline:

- `awk` recipe to derive the stations file from
  `intermediate_data/runoff_day.csv` on the deployment server (then scp
  to the laptop for the export).
- Pre-flight `umask 077` / `~/.pgpass` setup.
- Two-step laptop export (dry-run for COUNT(*); real run if counts
  match).
- scp transfer recipe.
- Server-side dry-run with the full expected inventory shape and
  enumerated common failure causes.
- Single-station canary per §4.3 acceptance criterion.
- Full population per horizon with all tunables documented.
- Acceptance SQL block for runoff (per-horizon GROUP BY with
  `discharge_rows` and `predictor_rows` non-null counts).
- Rerun / idempotency notes pointing operators at
  `--station-filter <code>` + lower `--batch-size` for narrow reruns.

## Sibling coordination

- P2b (hydrograph PENTAD/DECADE) is in flight on
  `feature_p2b_hydrograph_period_history`. Both branches modify
  `doc/prod/update_data_migration_runbook.md` (this PR adds §6
  preamble + §6.1; P2b adds §6.2). A merge conflict is expected and is
  trivial to resolve in either direction: the two §6.x subsections do
  not overlap.
- P4a (LR forecasts), P4b (ML forecasts), P5 (long forecasts) will add
  §6.3 / §6.4 / §6.5 respectively. No placeholders are added in this
  PR to avoid stale-section pollution.
- No edits to shared P0 files (`bin/utils/update_migration_helpers.sh`,
  `bin/utils/migration_py/_common.py`, `_audit.py`).

## Verification checklist (pre-merge)

- [x] `shellcheck` clean on both bash wrappers (verified via pre-commit
  hook; second invocation in tests run subprocess against `bash`
  successfully).
- [x] Both `--help` flags return exit 0 with documented usage block.
- [x] `iEasyHydroForecast` suite: 434 passed, 0 failed.
- [x] All P2a payload keys use `discharge` (greppable: there is no
  `discharge_avg` anywhere under `bin/` or in the runoff_period
  fixtures).
- [x] No real station codes anywhere; sentinel discipline enforced via
  the fixture-only-sentinel-codes tests.
- [x] No edits to `sapphire/services/`.
- [x] Manifest validation exercised end-to-end (round-trip test on the
  shipped fixtures).
- [x] Runbook §6 preamble + §6.1 in place; §6.2 untouched (P2b
  territory); §6.3+ not stubbed.

## PR target

`develop_migration_toolkit`. Branch `feature_p2a_runoff_period_history`
already pushed.
