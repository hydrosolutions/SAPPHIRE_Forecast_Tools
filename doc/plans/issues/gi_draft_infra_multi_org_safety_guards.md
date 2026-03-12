# Plan: Multi-Org Safety Guards (INFRA-012)

**Status**: draft
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`
**Module**: postprocessing_forecasts, iEasyHydroForecast, pipeline
**Depends on**: None (all phases are independent of INFRA-009 and PP-025)

## Context

The three existing multi-org plans (INFRA-009, PP-025, INFRA-011) scope reads
by station code. This plan addresses **three safety gaps** not covered there:

1. **Write-side guard** — no validation that writes only contain the current
   org's stations. If a read is accidentally unscoped, contaminated data flows
   silently into the DB.
2. **Station code collision detection** — station code disjointness across orgs
   is by naming convention only. No runtime check warns if codes overlap.
3. **Multi-org integration test** — no test verifies end-to-end isolation when
   two orgs' data coexist in the same DB.

### Why These Are Independent of INFRA-009 / PP-025

- **Write guard** (Phase 1): Reads `config_station_selection.json` directly
  (already available in all environments). Doesn't need `codes=` parameter on
  reads — it validates at the *write* boundary.
- **Collision check** (Phase 2): Uses `config_all_stations_library.json` which
  already has `organization_id` (currently `[1]` for all orgs — useless) but
  can still detect duplicate station codes across orgs by checking if the same
  code appears in multiple config files. After INFRA-009 Phase 1a adds the
  `organization` field, the check becomes stronger.
- **Integration test** (Phase 3): Tests the read/write isolation contract using
  mocked API clients. Doesn't require PP-025's `codes=` parameter — it verifies
  the *calling pattern* (that `codes=` is passed) and the write guard (Phase 1).

---

## Phase 1: Write-Side Guard in `api_writer.py`

**Goal**: Before every API write, log a warning if the batch contains station
codes not in the current org's `config_station_selection.json`.

**Why**: Defense-in-depth. Read-side scoping (PP-025) is the primary control,
but if a read is accidentally unscoped, the write guard catches contamination
before it reaches the DB. Without this, a missed `codes=` parameter silently
corrupts another org's data.

**Files to modify**:

### 1a. `apps/postprocessing_forecasts/src/api_writer.py`

**Add** a module-level helper (near the singleton pattern at line 72):

```python
_configured_codes: set[str] | None = None

def _load_configured_codes() -> set[str]:
    """Load station codes from config_station_selection.json.

    Returns empty set if config is unavailable (non-blocking).
    """
    global _configured_codes
    if _configured_codes is not None:
        return _configured_codes
    try:
        config_path = os.path.join(
            os.getenv("ieasyforecast_configuration_path", ""),
            os.getenv("ieasyforecast_config_file_station_selection", ""),
        )
        if not config_path.strip("/"):
            _configured_codes = set()
            return _configured_codes
        with open(config_path) as f:
            data = json.load(f)
        _configured_codes = {str(c) for c in data.get("stationsID", [])}
        # Also load decad if available
        decad_file = os.getenv(
            "ieasyforecast_config_file_station_selection_decad", ""
        )
        if decad_file:
            decad_path = os.path.join(
                os.getenv("ieasyforecast_configuration_path", ""),
                decad_file,
            )
            if os.path.exists(decad_path):
                with open(decad_path) as f:
                    decad_data = json.load(f)
                _configured_codes |= {
                    str(c) for c in decad_data.get("stationsID", [])
                }
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        logger.debug("Could not load station selection config for write guard")
        _configured_codes = set()
    return _configured_codes


def _check_write_codes(batch_codes: set[str], context: str) -> None:
    """Warn if batch contains codes outside the configured station list.

    Non-blocking: logs warning only, never raises. Silently returns if
    config is unavailable (empty configured set).
    """
    configured = _load_configured_codes()
    if not configured:
        return  # Config unavailable — skip check
    unexpected = batch_codes - configured
    if unexpected:
        logger.warning(
            "WRITE GUARD [%s]: batch contains %d code(s) not in station "
            "selection config: %s (configured: %d codes). This may indicate "
            "cross-org data leakage.",
            context,
            len(unexpected),
            sorted(unexpected)[:5],  # Show max 5 to avoid log spam
            len(configured),
        )
```

**Known limitation**: The guard unions pentad + decad station codes, making it
too permissive for horizon-specific writes (e.g., a pentad-only write won't
flag decad-only stations). This is acceptable because the guard is advisory —
a false negative (missed warning) is tolerable. A future improvement could
accept a `horizon` parameter and load only the relevant config file.

**Add** `_reset_configured_codes()` to the existing `_reset_api_client()`:

```python
def _reset_api_client():
    global _postprocessing_client, _configured_codes
    _postprocessing_client = None
    _configured_codes = None
```

**Wire** into each write function. Before the `client.write_*()` call, extract
codes from the records list and call the guard:

| Function | Line (approx) | Code to add before `client.write_*()` |
|----------|---------------|---------------------------------------|
| `_write_combined_forecast_to_api()` | Before line 333 | `_check_write_codes({r["code"] for r in records}, "combined_forecast")` |
| `_write_skill_metrics_to_api()` | Before line 567 | `_check_write_codes({r["code"] for r in records}, "skill_metrics")` |
| `_write_threshold_skill_metrics_to_api()` | Before line 668 | `_check_write_codes({r["code"] for r in records}, "threshold_skill_metrics")` |
| `_write_monthly_ensemble_to_api()` | Before line 803 | `_check_write_codes({r["code"] for r in records}, "monthly_ensemble")` |
| `_write_aggregated_forecasts_to_api()` | Before line 977 | `_check_write_codes({r["code"] for r in records}, "aggregated_forecasts")` |

**Note**: `_write_quarterly_ensemble_to_api()` and `_write_seasonal_ensemble_to_api()`
delegate to `_write_aggregated_forecasts_to_api()`, so only the aggregated helper
needs the guard.

### 1b. Tests

**File**: `apps/postprocessing_forecasts/tests/test_write_guard.py` (new)

**Test cases**:

1. `test_check_write_codes_no_unexpected` — configured `{99001, 99002}`,
   batch `{99001}` → no warning logged
2. `test_check_write_codes_unexpected_warns` — configured `{99001}`,
   batch `{99001, 88001}` → warning logged with `88001`
3. `test_check_write_codes_empty_config_skips` — configured `set()` (no
   config file) → no warning (non-blocking)
4. `test_check_write_codes_all_unexpected` — configured `{99001}`,
   batch `{88001, 88002}` → warning logged
5. `test_load_configured_codes_from_file` — write a temp
   `config_station_selection.json`, set env vars, call
   `_load_configured_codes()` → returns correct set
6. `test_load_configured_codes_merges_pentad_and_decad` — both config
   files exist with different codes → union returned
7. `test_load_configured_codes_missing_file` — env var points to
   nonexistent file → returns empty set (no crash)
8. `test_reset_clears_cache` — call `_load_configured_codes()`, then
   `_reset_api_client()`, modify file, call again → picks up new data
9. `test_write_combined_forecast_triggers_guard` — mock
   `_check_write_codes`, call `_write_combined_forecast_to_api()` with
   valid data, assert guard was called with correct codes
10. `test_write_skill_metrics_triggers_guard` — same pattern for skill
    metrics write

**Test pattern**: Use `tmp_path` for config files, `patch.dict(os.environ)`
for env vars, `caplog` or `patch("...logger.warning")` for warning assertions.

---

## Phase 2: Foreign Org Contamination Detection at Startup

**Goal**: At pipeline startup, warn if `config_all_stations_library.json`
contains stations tagged with an organization that doesn't match the current
org. This catches config files copied between orgs, shared config directories,
or bugs in `write_config_all_stations()`.

**Why**: Station code disjointness (Kyrgyz start with "1", Tajik with "2") is
by convention only. If a config file is copied between orgs or a shared config
directory is used, stations from the wrong org silently enter the pipeline.

**Design note — why not "duplicate code detection"**: The original design
checked for the same station code appearing under multiple orgs within a
single JSON file. This is impossible: JSON dict keys are unique, so the
same code can't appear twice. The useful check is instead: "does this
config file contain stations tagged with an org that doesn't match the
current org?"

**Behavior**:
- Navigate to `stations_available_for_forecast` wrapper in the JSON
- Get current org from `ieasyhydroforecast_organization` env var
- For each station: extract `organization` field (unwrap list if needed)
- Warn if any station has org != current org AND org != None
- If `organization` field is missing or all-None (pre-INFRA-009): skip check
  gracefully. Log debug message: "Organization field not available in station
  config — skipping collision check."
- Non-blocking: warning log only, never raises.

**Files to modify**:

### 2a. `apps/iEasyHydroForecast/setup_library.py`

**Add** a new function near `check_organization()` (line 255):

```python
def check_station_code_collisions() -> None:
    """Warn if config contains stations from a different organization.

    Reads config_all_stations_library.json, navigates to the
    stations_available_for_forecast wrapper, and checks that no station
    is tagged with an organization other than the current one. Gracefully
    skips if the organization field is not yet populated (pre-INFRA-009).
    """
    try:
        config_path = os.path.join(
            os.getenv("ieasyforecast_configuration_path", ""),
            os.getenv("ieasyforecast_config_file_all_stations", ""),
        )
        with open(config_path) as f:
            raw = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        return  # Config unavailable — skip silently

    # Navigate to stations wrapper
    stations = raw.get("stations_available_for_forecast", raw)
    current_org = os.getenv("ieasyhydroforecast_organization")
    if not current_org:
        return  # No org configured — can't check

    # Check each station's org tag
    foreign_stations: dict[str, str] = {}
    has_any_org = False
    for code, metadata in stations.items():
        if code in ("comment", "metadata"):  # Skip non-station keys
            continue
        if not isinstance(metadata, dict):
            continue
        org_value = metadata.get("organization", [None])
        if isinstance(org_value, list):
            org_value = org_value[0] if org_value else None
        if org_value is None:
            continue  # Pre-INFRA-009 data — can't check this entry
        has_any_org = True
        if org_value != current_org:
            foreign_stations[str(code)] = org_value

    if not has_any_org:
        logger.debug(
            "Organization field not available in station config — "
            "skipping collision check"
        )
        return

    if foreign_stations:
        logger.warning(
            "FOREIGN ORG CONTAMINATION: %d station(s) in config are tagged "
            "with a different organization than '%s': %s. This may indicate "
            "a config file copied between orgs or a shared config directory.",
            len(foreign_stations),
            current_org,
            dict(list(foreign_stations.items())[:5]),  # Show max 5
        )
```

**Wire** into `load_environment()` (line 316+). Add after the existing
`check_organization()` call:

```python
check_station_code_collisions()
```

### 2b. Tests

**File**: `apps/iEasyHydroForecast/tests/test_setup_library.py` (add to existing)

**Test cases**:

1. `test_collision_check_no_foreign_stations` — all stations tagged with
   current org → no warning
2. `test_collision_check_detects_foreign_org` — station tagged "tjhm" in
   config where current org is "kghm" → warning logged with the station code
3. `test_collision_check_skips_when_no_org_field` — all `organization`
   values are `[None]` → debug message, no warning
4. `test_collision_check_skips_when_file_missing` — config path doesn't
   exist → no crash, no warning
5. `test_collision_check_handles_list_wrapped_org` — `organization: ["kghm"]`
   (list-wrapped) → correctly unwrapped and checked
6. `test_collision_check_ignores_metadata_keys` — JSON has a `"comment"`
   key → not treated as a station code
7. `test_collision_check_navigates_wrapper` — JSON has
   `stations_available_for_forecast` wrapper → correctly navigated

**Test pattern**: Write temp JSON files to `tmp_path`, set env vars with
`patch.dict(os.environ)`, use `caplog` for log assertions.

---

## Phase 3: Multi-Org Isolation Integration Test

**Goal**: Verify that when two orgs' data coexist in the (mocked) API, each
org's pipeline run only reads its own data and the write guard catches leakage.

**Why**: INFRA-009 and PP-025 add org-scoping at the read and call-site level.
But no existing test verifies the full contract: "if the API contains data from
orgs A and B, a pipeline run for org A sees only org A's data." Without this
test, a regression (missed `codes=` parameter) goes undetected.

**Design decisions**:
- Uses **mocked API client** (not a real DB) — fast, deterministic, no
  infrastructure dependency.
- Tests the **data_reader** layer (where scoping happens) and the **api_writer**
  layer (where the write guard lives).
- Uses two sets of station codes: `99001-99003` for "demo" org,
  `88001-88003` for "other_org".
- Does NOT test individual module entry points — those are covered by
  existing wiring integration tests. This test verifies the data_reader
  and api_writer contracts that all modules depend on.

**Files to create**:

### 3a. `apps/postprocessing_forecasts/tests/test_multi_org_isolation.py` (new)

**Fixture setup**:

```python
DEMO_CODES = ["99001", "99002", "99003"]
OTHER_CODES = ["88001", "88002", "88003"]
ALL_CODES = DEMO_CODES + OTHER_CODES


@pytest.fixture
def multi_org_env(tmp_path):
    """Set up environment for demo org with both orgs' data available."""
    # Write config_station_selection.json for demo org
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    selection = {"stationsID": DEMO_CODES}
    (config_dir / "config_station_selection.json").write_text(
        json.dumps(selection)
    )
    (config_dir / "config_station_selection_decad.json").write_text(
        json.dumps(selection)
    )

    env_vars = {
        "ieasyforecast_configuration_path": str(config_dir),
        "ieasyforecast_config_file_station_selection": (
            "config_station_selection.json"
        ),
        "ieasyforecast_config_file_station_selection_decad": (
            "config_station_selection_decad.json"
        ),
        "ieasyhydroforecast_organization": "demo",
        "SAPPHIRE_API_ENABLED": "true",
        "SAPPHIRE_API_URL": "http://localhost:8000",
    }
    with patch.dict(os.environ, env_vars):
        yield tmp_path
```

**Test categories**:

#### Read isolation tests (verify data_reader scoping contract)

These tests mock the API client and verify that when `codes=` is passed,
only the specified codes' data is returned.

1. `test_read_skill_metrics_with_codes_filters_correctly` — Mock
   `client.read_skill_metrics()` to return data for all 6 stations.
   Call `data_reader.read_skill_metrics("pentad", codes=DEMO_CODES)`.
   Assert result contains only demo codes.

2. `test_read_combined_forecasts_with_codes_filters_correctly` — Same
   pattern for `read_combined_forecasts()`.

3. `test_read_monthly_combined_with_codes_filters_correctly` — Same
   pattern for `read_monthly_combined_forecasts()`.

4. `test_read_skill_metrics_without_codes_returns_all` — Call with
   `codes=None` → all 6 stations returned (backward compat).

5. `test_read_observed_and_modelled_with_codes` — Verify
   `read_observed_and_modelled_data()` (already supports `codes`) correctly
   filters.

**Note**: These tests verify the **data_reader contract** established by
PP-025. If PP-025 is not yet implemented, these tests serve as the **spec**
and will fail until PP-025 is done. Mark them with
`@pytest.mark.xfail(reason="Requires PP-025 org-scoped reads")` so they
are tracked in test output but don't break CI. This is preferred over a
custom `@pytest.mark.multi_org` marker because:
- `xfail` is built-in — no marker registration needed
- Tests appear as `XFAIL` in output (visible, not hidden)
- When PP-025 is implemented, they'll flip to `XPASS` (unexpected pass),
  signaling it's time to remove the `xfail` marker
- No need to add `-m "not multi_org"` to default test configuration

#### Write guard tests (verify api_writer catches cross-org writes)

6. `test_write_guard_catches_cross_org_combined_forecast` — Build a
   DataFrame with codes from both orgs. Call
   `_write_combined_forecast_to_api()`. Assert warning logged for
   `88001-88003`.

7. `test_write_guard_passes_single_org_combined_forecast` — Build a
   DataFrame with only demo codes. Call write. Assert no warning.

8. `test_write_guard_catches_cross_org_skill_metrics` — Same pattern
   for `_write_skill_metrics_to_api()`.

#### End-to-end isolation scenario

9. `test_full_isolation_scenario` — Simulate a mini pipeline run:
   a. Mock API client to return data for all 6 stations.
   b. Read skill metrics with `codes=DEMO_CODES`.
   c. Compute a trivial ensemble (or just pass through).
   d. Write result via `_write_combined_forecast_to_api()`.
   e. Assert: read returned only 3 stations, write received only 3 stations,
      no write guard warning.

10. `test_contaminated_read_triggers_write_guard` — Simulate a bug where
    `codes=` is not passed:
    a. Mock API client to return data for all 6 stations.
    b. Read skill metrics with `codes=None` (simulating missing scoping).
    c. Pass all data to write function.
    d. Assert: write guard warns about unexpected codes `88001-88003`.

**Test pattern**: Use `@patch` on `SapphirePostprocessingClient`, configure
mock returns with multi-org DataFrames. Use `caplog` for warning assertions.
Use `multi_org_env` fixture for environment setup. Reset singletons via
`conftest.py` autouse fixture (already exists).

### 3b. Test data helpers

Add to the test file (not a separate module):

```python
def _make_skill_metrics_df(codes: list[str], n_pentads: int = 3) -> pd.DataFrame:
    """Create a skill metrics DataFrame for the given station codes."""
    rows = []
    for code in codes:
        for pentad in range(1, n_pentads + 1):
            rows.append({
                "code": code,
                "pentad_in_year": pentad,
                "model_short": "LR",
                "n_pairs": 10,
                "nse": 0.7,
                "accuracy": 0.8,
            })
    return pd.DataFrame(rows)


def _make_combined_forecast_df(
    codes: list[str], date_str: str = "2026-01-06"
) -> pd.DataFrame:
    """Create a combined forecast DataFrame for the given station codes."""
    rows = []
    for code in codes:
        for model in ["LR", "TFT", "EM"]:
            rows.append({
                "code": code,
                "date": date_str,
                "pentad_in_month": 1,
                "pentad_in_year": 1,
                "forecasted_discharge": 42.0,
                "model_short": model,
            })
    return pd.DataFrame(rows)
```

---

## Deferred: `long_term_forecasting` Raw SQL Scoping

**Status**: Deferred to another developer (responsible for `long_term_forecasting`
module).

**Problem**: `data_interface.py` uses `DataInterfaceDB` with raw SQL queries
(`get_meteo_data()`, `get_runoff_data()`) that pull ALL orgs' data without
`code` filters. Models trained on contaminated data produce incorrect forecasts.

**Handoff note for LT developer**: The fix pattern is:
1. Add `station_codes: list[str] | None = None` param to `get_meteo_data()`
   and `get_runoff_data()` in `data_interface.py`.
2. If `station_codes` is not None, add `WHERE code IN (...)` to the SQL query.
3. Pass station codes from `run_forecast.py` (available from
   `config_station_selection.json` after INFRA-009 Phase 3).
4. Test with multi-org data in the DB to verify isolation.

This is tracked separately — see PP-025 out-of-scope table and INFRA-011
out-of-scope modules.

---

## Verification

1. **Phase 1 tests**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
2. **Phase 2 tests**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast`
3. **Phase 3 tests**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
4. **Full suite**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1a": {
      "name": "Write-side guard helpers in api_writer.py",
      "depends_on": [],
      "note": "Add _load_configured_codes(), _check_write_codes(), wire into all 5 write functions. Update _reset_api_client() to clear cache.",
      "files": [
        "apps/postprocessing_forecasts/src/api_writer.py"
      ]
    },
    "phase_1b": {
      "name": "Write guard unit tests",
      "depends_on": ["phase_1a"],
      "note": "10 test cases covering guard logic, config loading, cache reset, and wiring into write functions.",
      "files": [
        "apps/postprocessing_forecasts/tests/test_write_guard.py (new)"
      ]
    },
    "phase_2a": {
      "name": "Station code collision check in setup_library.py",
      "depends_on": [],
      "soft_depends_on": ["INFRA-009 Phase 1a"],
      "note": "Functions without INFRA-009 (graceful skip when org field missing) but provides full value only after INFRA-009 Phase 1a populates the organization field.",
      "files": [
        "apps/iEasyHydroForecast/setup_library.py"
      ]
    },
    "phase_2b": {
      "name": "Collision check unit tests",
      "depends_on": ["phase_2a"],
      "note": "6 test cases covering collision detection, graceful skip, list-wrapped values, missing file.",
      "files": [
        "apps/iEasyHydroForecast/tests/test_setup_library.py (add to existing)"
      ]
    },
    "phase_3": {
      "name": "Multi-org isolation integration test",
      "depends_on": ["phase_1a"],
      "note": "10 test cases. Read isolation tests serve as spec for PP-025 (may fail until PP-025 implemented). Write guard tests verify Phase 1. Mark with @pytest.mark.multi_org.",
      "files": [
        "apps/postprocessing_forecasts/tests/test_multi_org_isolation.py (new)"
      ]
    }
  },
  "execution_order": [
    {
      "wave_1_parallel": ["phase_1a", "phase_2a"],
      "note": "Independent — different modules, different files."
    },
    {
      "wave_2_parallel": ["phase_1b", "phase_2b", "phase_3"],
      "note": "phase_1b depends on 1a, phase_2b depends on 2a, phase_3 depends on 1a. All satisfied after wave 1."
    }
  ],
  "notes": [
    "Phase 1a and 2a touch different modules (postprocessing vs iEasyHydroForecast) — safe to parallelize.",
    "Phase 3 read isolation tests will XFAIL until PP-025 is implemented. This is intentional — they serve as the acceptance spec. Mark with @pytest.mark.xfail(reason='Requires PP-025'). When PP-025 lands, they flip to XPASS — remove the marker then.",
    "Phase 3 write guard tests will PASS immediately (they test Phase 1a).",
    "All phases are independent of INFRA-009 and PP-025 — can be implemented now.",
    "Phase 2a gracefully degrades pre-INFRA-009 (skips check if org field missing)."
  ]
}
```

Phase 1a and Phase 2a can run in parallel (different files/modules).
Phase 1b, Phase 2b, and Phase 3 can run in parallel after wave 1 completes.
Total: 2 sequential waves, with parallelism within each wave.
