# Remove hardcoded DB password and document required connection env vars

**Status**: Review
**Module**: long_term_forecasting
**Priority**: High
**Labels**: `security`, `long-term-forecasting`, `configuration`

---

## Summary

Remove a hardcoded database password from source code and add fail-fast
validation so missing connection string env vars produce a clear error
instead of a cryptic SQLAlchemy crash.

## Context

`data_interface.py` contains two classes that connect directly to PostgreSQL
via SQLAlchemy — bypassing the SAPPHIRE API layer used by every other module.
This was introduced as part of the `DataInterfaceDB` and `BasePredictorDataInterface`
implementations. The full migration to API-client reads is tracked in **API-005**,
but that depends on bulk-read endpoints (**API-001**, blocked on colleague work).

Until API-005 lands, these direct DB connections will remain. The immediate
problem is that one of them has a hardcoded password in committed source code,
and neither provides a useful error when the required env var is absent.

## Problem

**Problem 1 — hardcoded password in committed code**

`BasePredictorDataInterface.__init__` at `data_interface.py:699-702`:

```python
self.postprocessing_connection_string = os.getenv(
    "POSTPROCESSING_DB_CONNECTION_STRING",
    "postgresql://postgres:password@localhost:5434/postprocessing_db",  # ← plaintext password
)
```

The default value `"postgresql://postgres:password@..."` is committed to
GitHub. Anyone who clones the repo sees a credential (even if it is a
default/dev credential, it establishes a bad pattern and may match
production credentials on some deployments).

**Problem 2 — cryptic crash when env var is missing**

`DataInterfaceDB.__init__` at `data_interface.py:39-40`:

```python
self.connection_string = connection_string or os.getenv("DB_POSTPROCESS_CONNECTION_STRING")
self.engine = create_engine(self.connection_string)  # crashes with ArgumentError if None
```

When `DB_POSTPROCESS_CONNECTION_STRING` is not set (e.g., wrong `.env` file
used), the container crashes with:

```
sqlalchemy.exc.ArgumentError: Expected string or URL object, got None
```

No indication of which env var is missing. Same applies to
`BasePredictorDataInterface` if `POSTPROCESSING_DB_CONNECTION_STRING` is unset
and the hardcoded default is removed.

## Desired Outcome

- No passwords or connection strings exist as defaults in source code
- Missing required env vars produce an immediate, clear error message that
  names the missing variable
- The two required env vars are documented in the module README and in a
  comment in the `.env` template

---

## Technical Analysis

### Current Implementation

**File**: `apps/long_term_forecasting/data_interface.py`

| Class | Line | Issue |
|-------|------|-------|
| `DataInterfaceDB.__init__` | 32–42 | Reads `DB_POSTPROCESS_CONNECTION_STRING`; crashes with opaque error if `None` |
| `BasePredictorDataInterface.__init__` | 698–702 | Has hardcoded `postgresql://postgres:password@localhost:5434/postprocessing_db` as default |

**Where these classes are instantiated:**

- `DataInterfaceDB` — `run_forecast.py:501`, `calibrate_and_hindcast.py:324`
  (both only when `SAPPHIRE_API_AVAILABLE=True`)
- `BasePredictorDataInterface` — `run_forecast.py:262`
  (**instantiated unconditionally** when a model has dependencies; the
  `if SAPPHIRE_API_AVAILABLE` branch that decides DB vs CSV is evaluated
  *after* instantiation)

`DataInterfaceDB` is only reached when `SAPPHIRE_API_AVAILABLE=True`.

`BasePredictorDataInterface` is instantiated **regardless** of
`SAPPHIRE_API_AVAILABLE`. When the flag is `False`, only CSV methods are
called — the `postprocessing_engine` property (which creates the DB
connection) is never accessed. The engine is lazy-initialized, so the
hardcoded default connection string is never actually used in the CSV path.
This means a `ValueError` in `__init__` would **break the CSV-only path**.

### Root Cause

The hardcoded default was added as a convenience during development and was
never removed before being committed. The missing-env-var crash is a
consequence of `create_engine(None)` rather than an explicit guard.

---

## Implementation Plan

### Approach (as implemented)

Replaced both dedicated connection string env vars (`DB_POSTPROCESS_CONNECTION_STRING`
and `POSTPROCESSING_DB_CONNECTION_STRING`) with a shared helper function
`_build_postprocessing_db_url()` that builds the URL from the same component
env vars the SAPPHIRE services use: `POSTGRES_USER`, `POSTGRES_PASSWORD`,
`POSTPROCESSING_DB`. Host/port is selected automatically by `IN_DOCKER`.

- `DataInterfaceDB.__init__`: calls `_build_postprocessing_db_url()` (or uses
  explicit `connection_string` parameter if provided)
- `BasePredictorDataInterface.__init__`: calls `_build_postprocessing_db_url()`
  wrapped in `try/except ValueError` — stores `None` if components are missing
  so the CSV-only path (`SAPPHIRE_API_AVAILABLE=False`) doesn't crash
- `BasePredictorDataInterface.postprocessing_engine` property: keeps the lazy
  `ValueError` guard for when the DB is actually needed but URL is `None`

### Files Modified

| File | Changes |
|------|---------|
| `apps/long_term_forecasting/data_interface.py` | Added `_build_postprocessing_db_url()` helper; removed hardcoded password; both classes now use the helper |
| `apps/long_term_forecasting/tests/test_data_interface.py` | Added `TestBuildPostprocessingDbUrl` (9 tests) |
| `apps/long_term_forecasting/README.md` | Updated env var docs to reference component vars |

### Implementation (completed)

- [x] **Step 1**: Added `_build_postprocessing_db_url()` helper that reads
  `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTPROCESSING_DB`, selects host/port
  via `IN_DOCKER`, and uses `urllib.parse.quote_plus` for safe encoding.
  Optional `POSTPROCESSING_DB_PORT` env var (default 5434, ignored in Docker).

- [x] **Step 2**: `DataInterfaceDB.__init__` now uses
  `connection_string or _build_postprocessing_db_url()`.

- [x] **Step 3**: `BasePredictorDataInterface.__init__` calls
  `_build_postprocessing_db_url()` in try/except, stores `None` on failure.

- [x] **Step 4**: `postprocessing_engine` property guard updated to reference
  component env vars in its error message.

- [x] **Step 5**: Added 9 tests covering: default port, Docker mode, custom
  port, port ignored in Docker, special chars in password, missing individual
  components, all missing.

- [x] **Step 6**: README updated to document component env vars.

---

## Testing

### Test Cases

- [ ] Unit: `DataInterfaceDB(connection_string=None)` with env var unset
  raises `ValueError` containing `"DB_POSTPROCESS_CONNECTION_STRING"`
- [ ] Unit: `BasePredictorDataInterface()` with env var unset can be
  instantiated without error (guard is deferred to property)
- [ ] Unit: accessing `BasePredictorDataInterface().postprocessing_engine`
  with env var unset raises `ValueError` containing
  `"POSTPROCESSING_DB_CONNECTION_STRING"`
- [ ] Regression: existing `test_data_interface.py` tests still pass
  (they use `__new__` to bypass `__init__` — unaffected by either guard)

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting
```

### Manual Verification

Run the simulate_forecasts command on the server without the env var set —
confirm the error message names the missing variable clearly.

---

## Documentation Impact

- [ ] `apps/long_term_forecasting/README.md` — add section listing required
  env vars for direct DB access, with a note that these will be removed when
  API-005 is complete
- [ ] No other documentation impact — this is an internal credential/config
  change with no user-facing behavior change

---

## Out of Scope

- Migrating `DataInterfaceDB` and `BasePredictorDataInterface` reads to the
  API client (tracked as **API-005**, blocked on **API-001**)
- Changing the DB connection architecture or adding secrets management
  (Vault, AWS Secrets Manager, etc.)
- Auditing other modules for similar patterns

## Dependencies

- None — this is a standalone cleanup
- **API-005** will eventually make both env vars unnecessary by replacing
  direct DB access with API client calls

## Acceptance Criteria

- [ ] No passwords or connection string defaults exist in `data_interface.py`
  source code
- [ ] `DataInterfaceDB.__init__` raises `ValueError` with the env var name
  when connection string is missing
- [ ] `BasePredictorDataInterface.postprocessing_engine` property raises
  `ValueError` with the env var name when connection string is missing
  (NOT in `__init__` — that would break the CSV-only fallback path)
- [ ] `BasePredictorDataInterface` can still be instantiated without
  `POSTPROCESSING_DB_CONNECTION_STRING` when only CSV methods are used
- [ ] All tests pass: `SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting`
- [ ] `apps/long_term_forecasting/README.md` documents the two env vars

---

## References

- Related issues: **API-005** (full migration away from direct DB access),
  **API-001** (bulk-read endpoints, prerequisite for API-005)
- Offending code: `apps/long_term_forecasting/data_interface.py:39–42`, `:699–702`
