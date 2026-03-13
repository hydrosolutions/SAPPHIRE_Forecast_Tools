# Plan: Long-Term Forecasting SQL Org-Scoping (LTF-ORG-001)

**Status**: Draft (v4 — simplified type handling, orchestration protocol)
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`
**Module**: `long_term_forecasting`
**Depends on**: INFRA-009 (complete), INFRA-012 (complete)

## Context

The `long_term_forecasting` module uses `DataInterfaceDB` with raw SQL queries
against a PostgreSQL database. Unlike the rest of the pipeline (which uses
`sapphire_api_client` with org-scoped reads), these SQL queries have **no station
code filter**. In a shared-DB multi-org deployment, this means:

- **Training contamination**: `calibrate_and_hindcast.py` trains models on ALL
  orgs' discharge, meteo, and snow data — producing incorrect forecasts.
- **Forecast contamination**: `run_forecast.py` loads ALL orgs' data into
  `temporal_data`, so models predict for stations belonging to other orgs.
- **BasePredictorDataInterface**: Reads long_forecasts from the postprocessing DB
  without a station code filter, pulling in other orgs' model outputs as
  dependency inputs.

> **Note**: `DataInterfaceDB.__init__()` reads `DB_POSTPROCESS_CONNECTION_STRING`
> but queries preprocessing tables (`meteo`, `runoffs`, `snow`). Either the env
> var is misnamed or both datasets share one DB. This plan does not rename the
> env var — just documents the discrepancy for a future cleanup.

## Design Decision: Instance Attribute vs Per-Method Parameter

The original plan threaded `station_codes` through 8+ method signatures. After
code review, a **cleaner approach** is to store `station_codes` on the
`DataInterfaceDB` instance at construction time:

```python
class DataInterfaceDB:
    def __init__(self, connection_string=None, station_codes=None):
        ...
        self.station_codes = station_codes  # list[str] | None
```

**Rationale**:
- Station codes don't change within a pipeline run — they're read once at startup
- Avoids modifying 8+ method signatures and every call site
- Consistent with how the class already stores `self.connection_string` and paths
- Internal query methods use `self.station_codes` automatically when set
- Individual method `code` param still works for single-station convenience methods
  (`get_rain()`, `get_temperature()`) — no conflict since they serve different
  use cases (single lookup vs org-wide filter)

Same pattern for `BasePredictorDataInterface`:
```python
class BasePredictorDataInterface:
    def __init__(self, station_codes=None):
        ...
        self.station_codes = station_codes
```

## Affected Files

| File | Change |
|------|--------|
| `data_interface.py` | Add `station_codes` to `DataInterfaceDB.__init__()` and `BasePredictorDataInterface.__init__()`; apply `self.station_codes` filter in `get_meteo_data()`, `get_runoff_data()`, `get_snow_data()`, `get_base_predictor_data_database()`; filter `_prepare_static_data()` output |
| `run_forecast.py` | Read station codes at entry; pass to `DataInterfaceDB(station_codes=...)` and `BasePredictorDataInterface(station_codes=...)` |
| `calibrate_and_hindcast.py` | Read station codes at entry; pass to `DataInterfaceDB(station_codes=...)` |
| `tests/test_data_interface.py` | Add `TestDataInterfaceDBOrgScoping` class with mocked SQL tests |

## Non-Goals

- Changing the CSV-based `DataInterface` class (only used as fallback when
  `SAPPHIRE_API_AVAILABLE=False`). **Caveat**: The CSV path
  (`PATH_TO_PAST_DISCHARGE`) is assumed to be per-org because preprocessing
  writes org-scoped files. If this assumption is wrong, CSV contamination is a
  separate issue.
- Migrating `DataInterfaceDB` to `sapphire_api_client` (that's API-005, blocked
  on API-001 bulk read endpoints).
- Scoping `load_snow_data()` separately — it delegates entirely to
  `get_snow_data()` (line 233), which is scoped in Phase 1c. HRU validation
  is already per-org via env vars (`ieasyhydroforecast_HRU_SNOW_DATA`).
- Renaming `DB_POSTPROCESS_CONNECTION_STRING` (pre-existing naming issue).
- Consolidating `_read_station_codes()` across postprocessing modules (6+ copies
  exist already — worth doing but out of scope for this issue; tracked as a
  future cleanup).

---

## Phase 1: Add `station_codes` instance attribute to `DataInterfaceDB`

**Goal**: Store station codes on the instance and apply filtering in all SQL
query methods automatically.

**File**: `data_interface.py`

### 1a. Constructor change

```python
class DataInterfaceDB:
    def __init__(self, connection_string=None, station_codes=None):
        sl.load_environment()
        self.connection_string = connection_string or os.getenv(
            'DB_POSTPROCESS_CONNECTION_STRING'
        )
        self.engine = create_engine(self.connection_string)
        self.station_codes = station_codes  # list[str] | None
        self._get_paths()
```

### 1b. Private helper for SQL `IN` clause

Add a reusable helper to build parameterized `IN` clauses:

```python
def _add_station_filter(self, conditions: list, params: dict) -> None:
    """Append station_codes IN clause if instance has station_codes set.

    Treats None and empty list as "no filter" (returns all stations).
    """
    if self.station_codes is not None and len(self.station_codes) > 0:
        placeholders = ", ".join(
            f":sc_{i}" for i in range(len(self.station_codes))
        )
        conditions.append(f"code IN ({placeholders})")
        for i, c in enumerate(self.station_codes):
            params[f"sc_{i}"] = str(c)
```

**Edge case**: An empty list `[]` is treated identically to `None` — no filter
applied. Without this guard, `code IN ()` would be invalid SQL in PostgreSQL.

### 1c. Apply in `get_meteo_data()`, `get_runoff_data()`, `get_snow_data()`

Each method already builds a `conditions` list and `params` dict. Insert one call
after the existing filters:

```python
self._add_station_filter(conditions, params)
```

The existing single `code` param is left untouched — `get_rain()` and
`get_temperature()` still work as before. If both `code` and `self.station_codes`
are set, both conditions apply (AND logic). No precedence logic needed.

### 1d. Filter `_prepare_static_data()` output

After loading the CSV, filter to org stations if set:

```python
def _prepare_static_data(self):
    static_features = pd.read_csv(self.PATH_TO_STATIC_FEATURES)
    if "CODE" in static_features.columns:
        static_features.rename(columns={"CODE": "code"}, inplace=True)
    if (
        self.station_codes is not None
        and len(self.station_codes) > 0
        and "code" in static_features.columns
    ):
        # Compare as strings: cast CSV int codes to str to match
        # self.station_codes (always list[str] from config).
        static_features = static_features[
            static_features["code"].astype(str).isin(self.station_codes)
        ].reset_index(drop=True)
    return static_features
```

**Why**: Static CSV may contain stations from other orgs. Without filtering,
`_clean_data()` (which does `MultiIndex.from_product([all_codes, ...])`) would
expand foreign codes into full time series.

**Why `.astype(str)` instead of casting config codes to `int`**: Station codes
are identifiers, not numbers. They flow as `list[str]` from JSON config through
the entire pipeline. Casting the CSV column to str for comparison is a one-liner
that avoids try/except complexity. The underlying `code` column dtype is
unchanged — only the `.isin()` comparison uses the str cast.

### 1e. Type consistency note

Station codes are **always `str`** from config (`_read_station_codes()` returns
`list[str]`). The existing query methods already send codes as strings to SQL:
`params["code"] = str(code)` (lines 82, 133, 177). `_add_station_filter()`
follows the same pattern. PostgreSQL handles implicit text→integer comparison
in parameterized queries.

After query execution, result DataFrames cast codes to int
(`df['code'].astype(int)`) for downstream processing. This is pre-existing
behavior and unchanged by this issue. The `_prepare_static_data()` filter
uses `.astype(str).isin()` to bridge the gap without altering the column dtype.

---

## Phase 2: Add `station_codes` to `BasePredictorDataInterface`

**Goal**: Scope postprocessing DB reads to org's stations.

**File**: `data_interface.py` (same file as Phase 1 — must run sequentially)

### 2a. Constructor change

```python
class BasePredictorDataInterface:
    def __init__(self, station_codes=None):
        logger.info("Initialized BasePredictorDataInterface")
        sl.load_environment()
        self.station_codes = station_codes  # list[str] | None
        self.postprocessing_connection_string = os.getenv(...)
        self._postprocessing_engine = None
```

### 2b. `get_base_predictor_data_database()`

**⚠ The current query ends with `ORDER BY code, date`.** The station filter must
be inserted *before* the ORDER BY, not appended to the query string.

Refactor the query to use a conditions list (same pattern as `DataInterfaceDB`):

```python
def get_base_predictor_data_database(self, model_name, horizon_type="month",
                                      horizon_value=1):
    today = get_today()

    conditions = [
        "UPPER(model_type::text) = UPPER(:model_type)",
        "UPPER(horizon_type::text) = UPPER(:horizon_type)",
        "horizon_value = :horizon_value",
        "date <= :today",
    ]
    params = {
        "model_type": model_name,
        "horizon_type": horizon_type,
        "horizon_value": horizon_value,
        "today": today.strftime("%Y-%m-%d"),
    }

    # Add org-scoping filter (same guard as DataInterfaceDB)
    if self.station_codes is not None and len(self.station_codes) > 0:
        placeholders = ", ".join(
            f":sc_{i}" for i in range(len(self.station_codes))
        )
        conditions.append(f"code IN ({placeholders})")
        for i, c in enumerate(self.station_codes):
            params[f"sc_{i}"] = str(c)

    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT date, code, q, q_xgb, q_lgbm, q_catboost, q_loc
        FROM long_forecasts
        WHERE {where_clause}
        ORDER BY code, date
    """

    df = self._execute_postprocessing_query(query, params)
    # ... rest unchanged ...
```

**Why refactor instead of string-insert**: The original query had a hardcoded
multi-line string. Inserting before `ORDER BY` via string manipulation is
fragile. The conditions-list pattern is already proven in `DataInterfaceDB`
and makes the code consistent across both classes.

---

## Phase 3: Wire station codes through entry points

**Goal**: Read org-scoped station codes once at the entry point and pass to both
data interface classes.

### 3a. `run_forecast.py` — `run_forecast()` function

Add `_read_station_codes()` at module level (same pattern as
`postprocessing_operational.py:90-107`):

```python
def _read_station_codes():
    """Read station codes from the station selection config file.

    Handles both list format (``[12345, 67890]``) and dict format
    (``{"12345": {...}, "67890": {...}}``). The dict format is used by
    some ML configs — iterating a dict yields its keys.
    """
    config_path = os.path.join(
        os.getenv("ieasyforecast_configuration_path", ""),
        os.getenv("ieasyforecast_config_file_station_selection", ""),
    )
    with open(config_path) as f:
        config = json.load(f)
    raw = config.get("stationsID", [])
    codes = [str(c) for c in raw]
    if not codes:
        logger.warning("No station codes found in %s — no org filter applied",
                       config_path)
    else:
        logger.info("Read %d station codes for org-scoped filtering",
                    len(codes))
    return codes
```

**Note on dict format**: `[str(c) for c in dict]` iterates over keys, which
is correct. This matches all 5+ existing `_read_station_codes()` copies in
the codebase.

In `run_forecast()`, read codes and pass to constructors:

```python
def run_forecast(...):
    sl.load_environment()
    station_codes = _read_station_codes()

    # ... existing config setup ...

    if SAPPHIRE_API_AVAILABLE:
        data_interface = DataInterfaceDB(station_codes=station_codes)
    else:
        data_interface = DataInterface()  # CSV path unchanged

    # ... existing code ...
```

In `run_single_model()`, pass `station_codes` to `BasePredictorDataInterface`:

```python
base_predictor_interface = BasePredictorDataInterface(
    station_codes=station_codes
)
```

This requires adding `station_codes` to `run_single_model()` signature and
passing it from the `run_forecast()` loop. The `data_interface` passed to
`run_single_model()` already carries `station_codes` on the instance — the
parameter is only needed for constructing `BasePredictorDataInterface` (line 110),
which is a separate class.

### 3b. `calibrate_and_hindcast.py` — entry point

Same pattern in `calibrate_and_hindcast()`:

```python
def calibrate_and_hindcast(...):
    sl.load_environment()
    station_codes = _read_station_codes()

    if SAPPHIRE_API_AVAILABLE:
        data_interface = DataInterfaceDB(station_codes=station_codes)
    else:
        data_interface = DataInterface()
```

**Important**: `tune_hyperparameters_model()` (line 56) also calls
`data_interface.extend_base_data_with_snow()`. Since `data_interface` is now
constructed with `station_codes`, the snow queries are automatically scoped — no
additional changes needed inside `tune_hyperparameters_model()` or
`calibrate_model()`.

Add `_read_station_codes()` as a module-level function (same implementation as
3a — yes, this duplicates, but consolidation is out of scope).

---

## Phase 4: Tests

**Files**: `tests/conftest.py` (new), `tests/test_data_interface.py`

### 4-pre. Create `tests/conftest.py`

Neighboring modules (`postprocessing_forecasts`, `iEasyHydroForecast`) have
`conftest.py` with autouse fixtures. Create one for `long_term_forecasting`:

```python
import pytest

@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Prevent test pollution from real env files.

    Sets SAPPHIRE_TEST_ENV=True so tests don't attempt real DB connections
    or file I/O unless explicitly configured.
    """
    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "True")
```

This is minimal — can be extended later. The key purpose is establishing the
fixture infrastructure for the test module.

### 4a. New test class: `TestDataInterfaceDBOrgScoping`

The existing tests require a live environment and only test the CSV `DataInterface`.
Add a new test class that works without a DB by mocking `_execute_query`:

```python
class TestDataInterfaceDBOrgScoping:
    """Test org-scoping SQL filters in DataInterfaceDB.

    Uses monkeypatching to avoid needing a real DB connection.
    """

    @pytest.fixture
    def patched_db(self, monkeypatch):
        """Create DataInterfaceDB with mocked engine and env."""
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", "/dev/null")
        # ... mock sl.load_environment, create_engine, _get_paths ...
        db = DataInterfaceDB.__new__(DataInterfaceDB)
        db.station_codes = ["12345", "67890"]
        db.engine = MagicMock()
        db.PATH_TO_STATIC_FEATURES = "/dev/null"
        return db
```

**Tests to include**:

1. `test_station_filter_adds_in_clause` — mock `_execute_query`, call
   `get_runoff_data()`, verify the SQL string contains `code IN (:sc_0, :sc_1)`
   and params contain `{"sc_0": "12345", "sc_1": "67890"}`
2. `test_no_station_codes_no_filter` — `station_codes=None`, verify no `IN`
   clause in SQL
3. `test_single_code_and_station_codes_both_apply` — set `station_codes` on
   instance AND pass `code="12345"` to method, verify both conditions present
4. `test_static_data_filtered_by_station_codes` — create a temp CSV with codes
   `[12345, 67890, 99999]`, set `station_codes=["12345", "67890"]`, verify
   `_prepare_static_data()` excludes `99999`. Uses `.astype(str).isin()` so
   the CSV's int codes match the str station_codes.
5. `test_static_data_unfiltered_when_no_station_codes` — `station_codes=None`,
   verify all codes returned
6. `test_empty_station_codes_no_filter` — `station_codes=[]`, verify no `IN`
   clause in SQL (same as `None` behavior)
7. `test_station_filter_applied_to_all_query_methods` — verify `IN` clause
   appears in `get_meteo_data()`, `get_runoff_data()`, and `get_snow_data()`
   (parametrized test across all three methods)
8. `test_get_base_data_propagates_station_filter` — mock `_execute_query`,
   call `get_base_data()`, verify ALL emitted SQL strings (runoff + 2x meteo)
   contain the `IN` clause. This is an integration-level test ensuring the
   full `get_base_data()` → `get_runoff_data()` / `_load_forcing_data()` →
   `get_meteo_data()` chain respects `station_codes`.

### 4b. `BasePredictorDataInterface` tests

1. `test_base_predictor_station_filter` — mock `_execute_postprocessing_query`,
   verify `code IN (...)` present in WHERE clause when `station_codes` set
2. `test_base_predictor_no_filter_when_none` — verify query has no `IN`
   clause when `station_codes=None`
3. `test_base_predictor_empty_list_no_filter` — `station_codes=[]`, verify
   no `IN` clause (same as `None`)
4. `test_base_predictor_order_by_preserved` — verify `ORDER BY code, date`
   appears AFTER the WHERE clause (regression test for the v2 SQL bug)

### 4c. `_read_station_codes()` tests

```python
def test_read_station_codes_list_format(tmp_path, monkeypatch):
    """stationsID as list of ints → returns list[str]."""
    config = {"stationsID": [12345, 67890]}
    config_file = tmp_path / "config_station_selection.json"
    config_file.write_text(json.dumps(config))
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_config_file_station_selection",
                       "config_station_selection.json")
    codes = _read_station_codes()
    assert codes == ["12345", "67890"]

def test_read_station_codes_empty(tmp_path, monkeypatch):
    """Empty stationsID returns empty list and logs warning."""
    config = {"stationsID": []}
    # ... same env setup ...
    codes = _read_station_codes()
    assert codes == []
```

**Note**: The `_read_station_codes()` docstring mentions dict format compatibility
(`[str(c) for c in dict]` iterates keys), but the long-term forecasting module's
config is always list-format. No dedicated dict-format test — the `[str(c) for c
in raw]` pattern handles both implicitly.

---

## Verification

1. Run existing tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting`
2. Run new tests from Phase 4 (same command — tests are in the module's test dir)
3. Manual spot-check with DB: construct `DataInterfaceDB(station_codes=["12345"])`
   and call `get_runoff_data()` — verify only that code is returned
4. Verify `_prepare_static_data()` filtering: construct with `station_codes`,
   confirm the returned DataFrame only contains those codes

---

## Risks and Caveats

1. **CSV `DataInterface` is assumed org-scoped** — `PATH_TO_PAST_DISCHARGE` is a
   single CSV file. If preprocessing ever writes multi-org data to it, the CSV
   path would need the same filtering. Not addressed here.
2. **`_clean_data()` amplification** — `_clean_data()` builds
   `MultiIndex.from_product([all_codes, full_date_range])`. After this fix, the
   upstream SQL queries return only org codes, so `all_codes` is already scoped.
   No change needed in `_clean_data()` itself, but if any unfiltered data source
   introduces foreign codes, they'll be expanded into full time series.
3. **Type coercion** — Station codes flow as `list[str]` from config. SQL
   parameterized queries send them as strings (matching existing
   `params["code"] = str(code)` pattern). PostgreSQL handles implicit
   text→integer comparison. `_prepare_static_data()` uses
   `.astype(str).isin(self.station_codes)` to compare without altering the
   underlying column dtype.
4. **`_read_station_codes()` duplication** — This adds 2 more copies (now 8+
   across the codebase). A future issue should consolidate these into
   `setup_library.py`.
5. **`load_snow_data()` is auto-scoped** — It delegates to `get_snow_data()`
   (line 233), which receives the `_add_station_filter()` in Phase 1c. No
   separate scoping needed. Verified during v3 review.
6. **Empty station codes list** — Both `_add_station_filter()` and the
   `BasePredictorDataInterface` filter guard against `len(self.station_codes) > 0`
   to prevent generating invalid `IN ()` SQL. An empty list behaves identically
   to `None` (no filter applied).
7. **Backward compatibility** — All constructor changes add optional params with
   `None` defaults. Existing callers that don't pass `station_codes` get current
   (unfiltered) behavior. No breaking changes to public API.

---

## Orchestration Protocol

**CRITICAL: The orchestrator must NEVER write code directly.**

Responsibilities:
1. **Explore** the codebase before each phase to gather context for agent prompts
2. **Delegate** all implementation work to Sonnet 4.6 general-purpose agents
3. **Coordinate** parallel vs sequential execution based on dependencies
4. **Review** all changes at the end via `git diff`
5. **Iterate** by delegating fixes to subagents if issues found
6. **Commit** only when all tests pass

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "description": "Add station_codes instance attribute + SQL filtering to DataInterfaceDB",
      "files": ["data_interface.py"],
      "depends_on": [],
      "agents": "1 sonnet agent (sequential edits within single file)",
      "steps": [
        "1a: Add station_codes param to __init__()",
        "1b: Add _add_station_filter() helper with empty-list guard",
        "1c: Apply filter in get_meteo_data(), get_runoff_data(), get_snow_data()",
        "1d: Filter _prepare_static_data() output with .astype(str).isin()",
        "1e: Verify type consistency (str throughout)"
      ],
      "critical_notes": [
        "_add_station_filter must guard: if station_codes is not None AND len > 0",
        "_prepare_static_data uses .astype(str).isin(self.station_codes) — NO int() cast, NO try/except",
        "load_snow_data() delegates to get_snow_data() — no separate change needed"
      ]
    },
    "phase_2": {
      "description": "Add station_codes to BasePredictorDataInterface + refactor query to conditions list",
      "files": ["data_interface.py"],
      "depends_on": ["phase_1"],
      "agents": "1 sonnet agent (same file as Phase 1, must run sequentially)",
      "steps": [
        "2a: Add station_codes param to __init__()",
        "2b: Refactor get_base_predictor_data_database() to use conditions list pattern",
        "2c: Add station filter with empty-list guard"
      ],
      "critical_notes": [
        "MUST refactor query to conditions-list pattern — original query ends with ORDER BY, appending AND would produce invalid SQL",
        "Empty-list guard required: if station_codes is not None AND len > 0"
      ]
    },
    "phase_3a": {
      "description": "Wire station codes through run_forecast.py",
      "files": ["run_forecast.py"],
      "depends_on": ["phase_1", "phase_2"],
      "agents": "1 sonnet agent",
      "steps": [
        "Add _read_station_codes() function (same pattern as postprocessing_operational.py:90-107)",
        "Add empty-codes warning log",
        "Pass station_codes to DataInterfaceDB() constructor",
        "Add station_codes to run_single_model() signature (needed only for BasePredictorDataInterface construction at line 110)",
        "Pass station_codes to BasePredictorDataInterface() constructor"
      ]
    },
    "phase_3b": {
      "description": "Wire station codes through calibrate_and_hindcast.py",
      "files": ["calibrate_and_hindcast.py"],
      "depends_on": ["phase_1"],
      "agents": "1 sonnet agent",
      "note": "Does NOT need Phase 2 — calibrate_and_hindcast.py doesn't use BasePredictorDataInterface. tune_hyperparameters_model() is auto-scoped via the instance attribute."
    },
    "phase_4": {
      "description": "Add conftest.py and unit tests for org-scoping",
      "files": ["tests/conftest.py", "tests/test_data_interface.py"],
      "depends_on": ["phase_1", "phase_2"],
      "agents": "1 sonnet agent",
      "steps": [
        "4-pre: Create tests/conftest.py with autouse SAPPHIRE_TEST_ENV fixture",
        "4a: TestDataInterfaceDBOrgScoping class (8 tests, including empty-list, parametrized, and get_base_data integration)",
        "4b: BasePredictorDataInterface filter tests (4 tests, including ORDER BY regression)",
        "4c: _read_station_codes() tests (2 tests: list format, empty list)"
      ],
      "critical_notes": [
        "test_base_predictor_order_by_preserved is a regression test for the v2 SQL bug — must verify ORDER BY comes after WHERE",
        "test_empty_station_codes_no_filter verifies the empty-list guard works",
        "test_get_base_data_propagates_station_filter is an integration test — mock _execute_query and verify ALL emitted SQL strings contain IN clause"
      ]
    }
  },
  "execution_order": [
    {"step": 1, "run": ["phase_1"]},
    {"step": 2, "run": ["phase_2"]},
    {"step": 3, "run": ["phase_3a", "phase_3b", "phase_4"], "parallel": true}
  ],
  "post_implementation": {
    "step": 4,
    "description": "Orchestrator reviews git diff, runs tests, iterates if needed",
    "commands": [
      "cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting",
      "git diff --stat"
    ]
  }
}
```

## Changes from v2 → v3

| # | Issue | Fix |
|---|-------|-----|
| 1 | **BUG**: Phase 2b appended `AND code IN (...)` after `ORDER BY` → invalid SQL | Refactored to conditions-list pattern; ORDER BY added after WHERE construction |
| 2 | **BUG**: Empty `station_codes=[]` → `IN ()` invalid SQL | Added `len(self.station_codes) > 0` guard in both classes |
| 3 | **GAP**: No `conftest.py` for test module | Added Phase 4-pre to create conftest.py |
| 4 | **GAP**: `stationsID` can be dict, not just list | Documented in `_read_station_codes()` docstring; added dict-format test |
| 5 | **RISK**: `int()` cast in `_prepare_static_data()` unguarded | Wrapped in try/except with warning log |
| 6 | **VERIFIED**: `load_snow_data()` delegates to `get_snow_data()` | Documented in Non-Goals and Risks — no separate scoping needed |
| 7 | Tests expanded | 8→14 tests: empty-list guards, parametrized query methods, ORDER BY regression, dict config |

## Changes from v3 → v4

| # | Issue | Fix |
|---|-------|-----|
| 1 | **SIMPLIFY**: `_prepare_static_data()` used `int()` cast with try/except | Replaced with `.astype(str).isin(self.station_codes)` one-liner — station codes are identifiers, not numbers |
| 2 | **FIX**: Phase 3a referenced wrong file (`postprocessing_operational_long_term.py:56-66`) | Corrected to `postprocessing_operational.py:90-107` |
| 3 | **GAP**: No integration test for `get_base_data()` chain | Added test 8: `test_get_base_data_propagates_station_filter` verifying all emitted SQL strings contain IN clause |
| 4 | **CLARITY**: `run_single_model()` signature change lacked rationale | Added note: parameter needed only for `BasePredictorDataInterface` construction |
| 5 | **OVER-ENGINEERING**: Dict-format test for config that's always list-format | Dropped dedicated test; documented that `[str(c) for c in raw]` handles both implicitly |
| 6 | **STRUCTURE**: Added orchestration protocol section | Plan now follows sub-agent delegation pattern with dependency graph |
| 7 | Tests adjusted | 14→14 tests: dropped dict-format test, added get_base_data integration test |
