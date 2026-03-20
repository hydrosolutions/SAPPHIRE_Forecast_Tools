# ML Hindcast: Replace Subprocess+CSV IPC with Direct Function Call

**Status**: Draft
**Module**: `machine_learning`
**Priority**: Low (cleanup, no operational impact)
**Labels**: `enhancement`, `api-migration`, `refactor`
**Branch**: TBD (branch from `main` after prerequisites land)
**Prerequisites**:
- `high_prio_gi_draft_ml_csv_schema_corruption_fix.md` (for `ML_CANONICAL_CSV_COLUMNS`)
- `mid_prio_gi_draft_ml_hindcast_api_consistency.md` (for write-order fix in hindcast script)

---

## Summary

Three scripts communicate with `hindcast_ML_models.py` via subprocess + CSV file:

```
caller  ──subprocess.run()──►  hindcast_ML_models.py
                                    │
                                    ├── writes to API  (line 495)
                                    └── writes to CSV  (line 483)
                                            │
caller  ◄──pd.read_csv()───────────────────┘
```

The intermediate CSV is a process-boundary artifact. Since `hindcast_ML_models.py`
already writes to the API, the CSV exists only as an IPC channel between the
subprocess and its caller. This creates:

1. **Duplicate API writes** — hindcast writes all rows to API, then the caller
   reads from CSV, processes, and writes a subset to API again
2. **Filesystem dependency** — callers fail with `FileNotFoundError` if the CSV
   path is wrong or the filesystem isn't shared (e.g., containerized callers)
3. **Fragile filename contract** — callers must reconstruct the exact filename
   `{MODEL}_{MODE}_hindcast_daily_{start}_{end}.csv`

### Target State

Extract the core logic of `hindcast_ML_models.py` into an importable function
that returns a DataFrame. Callers import and call it directly — no subprocess,
no intermediate CSV, no duplicate API writes.

```
caller  ──function call──►  run_hindcast(...)  ──►  returns DataFrame
                                │
                                └── writes to API  (single write, by caller after processing)
```

### Affected Files

| File | Current role | Change |
|------|-------------|--------|
| `hindcast_ML_models.py` | Entry point (subprocess) | Extract core logic into `run_hindcast()` function; keep `__main__` for CLI/Docker |
| `fill_ml_gaps.py` | Calls subprocess, reads CSV | Import and call `run_hindcast()` directly |
| `recalculate_nan_forecasts.py` | Calls subprocess, reads CSV | Import and call `run_hindcast()` directly |
| `add_new_station.py` | Calls subprocess, reads CSV | Import and call `run_hindcast()` directly |
| `initialize_ml_tool.py` | Calls subprocess (line 73) | Import and call `run_hindcast()` directly |

### Who Calls the Hindcast Script Today

| Caller | Location | `call_hindcast_script()` signature | Extra params |
|--------|----------|-----------------------------------|-------------|
| `fill_ml_gaps.py` | line 66 | `(min_date, max_date, MODEL, path, MODE)` | — |
| `recalculate_nan_forecasts.py` | line 65 | `(min_date, max_date, MODEL, path, codes_with_nan, MODE)` | `codes_with_nan` (list) |
| `add_new_station.py` | line 52 | `(start_date, end_date, codes_hindcast, MODEL, path, MODE)` | `codes_hindcast` (list) |
| `initialize_ml_tool.py` | line 73 | Direct `subprocess.run()` | Sets `ieasyhydroforecast_NEW_STATIONS` |

---

## Current `hindcast_ML_models.py` Structure

The script's `main()` function (line 130) does:

1. **Environment setup** (lines 134-256): Read env vars, load `.env`, set paths
2. **Data loading** (lines 257-350): Read ERA5 meteo (API/CSV), discharge (API/CSV),
   static features (CSV), scalers (CSV)
3. **Model loading** (lines 351-380): Load trained model and scalers
4. **Hindcast loop** (lines 381-460): For each station code, run `predictor.hindcast()`
   and accumulate results into `hindecast_daily_df`
5. **Flag assignment** (lines 463-465): Set flag=4 (valid) or flag=3 (NaN)
6. **Output** (lines 483-510): Write CSV, then write API

Steps 1-5 become the body of `run_hindcast()`. Step 6 changes: the function
returns the DataFrame; the caller decides where to write it.

---

## Implementation Plan

### Phase 1: Extract `run_hindcast()` Function

**File**: `apps/machine_learning/hindcast_ML_models.py`

Refactor the current `main()` into two pieces:

```python
def run_hindcast(
    model_to_use: str,
    hindcast_mode: str,
    start_date: str,
    end_date: str,
    new_stations: str | None = None,
) -> pd.DataFrame:
    """Run hindcast for the given model and date range.

    Args:
        model_to_use: Model name (TFT, TIDE, TSMIXER, ARIMA).
        hindcast_mode: PENTAD or DECAD.
        start_date: ISO date string (YYYY-MM-DD), inclusive.
        end_date: ISO date string (YYYY-MM-DD), inclusive.
        new_stations: Comma-separated station codes, or None for all.

    Returns:
        DataFrame with hindcast predictions (canonical ML columns).
        Empty DataFrame if no predictions could be made.
    """
    # Steps 1-5 from current main(), using function args instead of env vars
    # for the 5 parameters above. All other config (paths, thresholds) still
    # comes from env vars loaded by sl.load_environment().
    ...
    return hindecast_daily_df
```

**Key design decisions**:

1. **Only the 5 subprocess-communicated params become function args.** The rest
   (paths, thresholds, HRU codes) stay as env var reads inside the function.
   This minimizes the refactor scope — callers already set these env vars.

2. **`run_hindcast()` does NOT write to API or CSV.** It returns the DataFrame.
   The caller is responsible for writing, because each caller processes the data
   differently before writing (gap-filling, NaN-replacement, deduplication).

3. **`run_hindcast()` does NOT call `sl.load_environment()` itself.** The caller
   is responsible for ensuring the environment is loaded before calling. All four
   callers already do this.

4. **The `__main__` block stays for CLI/Docker backward compatibility:**

```python
if __name__ == "__main__":
    sl.load_environment()
    df = run_hindcast(
        model_to_use=os.getenv("SAPPHIRE_MODEL_TO_USE"),
        hindcast_mode=os.getenv("SAPPHIRE_HINDCAST_MODE"),
        start_date=os.getenv("ieasyhydroforecast_START_DATE"),
        end_date=os.getenv("ieasyhydroforecast_END_DATE"),
        new_stations=os.getenv("ieasyhydroforecast_NEW_STATIONS"),
    )
    if not df.empty:
        # Write to API (primary)
        if SAPPHIRE_API_AVAILABLE:
            horizon = "pentad" if os.getenv("SAPPHIRE_HINDCAST_MODE") == "PENTAD" else "decade"
            _write_ml_forecast_to_api(df, horizon, os.getenv("SAPPHIRE_MODEL_TO_USE"))
        # Write to CSV (archive/fallback)
        ...
```

5. **Error handling**: If data loading or model loading fails, `run_hindcast()`
   should raise an exception (not silently return empty). The callers already
   have `try/except` blocks that catch `FileNotFoundError` and `RuntimeError`.

### Phase 2: Update Callers — Replace `call_hindcast_script()` with Direct Call

Each caller has its own `call_hindcast_script()` wrapper. Replace each with a
direct call to `run_hindcast()`.

**Phase 2a — `fill_ml_gaps.py`**

Current (lines 66-124):
```python
def call_hindcast_script(min_missing_date, max_missing_date, MODEL_TO_USE,
                         intermediate_data_path, PREDICTION_MODE):
    env = os.environ.copy()
    env["SAPPHIRE_MODEL_TO_USE"] = MODEL_TO_USE
    ...
    result = subprocess.run(command, capture_output=True, text=True, env=env)
    ...
    hindcast = pd.read_csv(os.path.join(PATH_HINDCAST, file_name))
    return hindcast
```

Replace with:
```python
from hindcast_ML_models import run_hindcast

# At the call site (line 312):
hindcast = run_hindcast(
    model_to_use=MODEL_TO_USE,
    hindcast_mode=PREDICTION_MODE,
    start_date=min_missing_date,
    end_date=max_missing_date,
)
```

Remove the entire `call_hindcast_script()` function and `import subprocess`.

**Phase 2b — `recalculate_nan_forecasts.py`**

Same pattern. Its `call_hindcast_script()` has an extra `codes_with_nan` param
that maps to `ieasyhydroforecast_NEW_STATIONS` env var:

```python
hindcast = run_hindcast(
    model_to_use=MODEL_TO_USE,
    hindcast_mode=PREDICTION_MODE,
    start_date=min_missing_date,
    end_date=max_missing_date,
    new_stations=",".join(str(c) for c in codes_with_nan),
)
```

Remove `call_hindcast_script()` and `import subprocess`.

**Phase 2c — `add_new_station.py`**

Its `call_hindcast_script()` has a `codes_hindcast` param:

```python
hindcast = run_hindcast(
    model_to_use=MODEL_TO_USE,
    hindcast_mode=PREDICTION_MODE,
    start_date=start_date,
    end_date=end_date,
    new_stations=",".join(str(c) for c in codes_hindcast),
)
```

Remove `call_hindcast_script()` and `import subprocess`.

**Phase 2d — `initialize_ml_tool.py`**

Currently calls `subprocess.run()` directly (line 73). Replace with:

```python
from hindcast_ML_models import run_hindcast

df = run_hindcast(
    model_to_use=MODEL_TO_USE,
    hindcast_mode=mode,
    start_date=start_date,
    end_date=end_date,
    new_stations=new_stations if new_stations != "None" else None,
)
# Write to API + CSV as needed
```

### Phase 3: Remove Duplicate API Writes

After Phase 2, the hindcast data follows this path:
```
run_hindcast() → returns DataFrame → caller processes → caller writes to API
```

The `run_hindcast()` function no longer writes to API (that moved to `__main__`).
But check each caller to ensure it writes the **processed** result to API, not
the raw hindcast. Current state:

| Caller | What it writes to API | Correct after refactor? |
|--------|----------------------|------------------------|
| `fill_ml_gaps.py` | Only gap-filled rows (`filled_df`) | Yes — writes subset, not full hindcast |
| `recalculate_nan_forecasts.py` | Entire hindcast DF | Review — should it write only updated rows? |
| `add_new_station.py` | Hindcast DF (after dedup) | Yes — writes deduped result |

**Action**: Verify `recalculate_nan_forecasts.py` API write scope. If it
currently writes the full hindcast (including rows that weren't NaN), that's
a pre-existing issue, not introduced by this refactor. Document but don't fix.

### Phase 4: Remove Intermediate CSV Write from `run_hindcast()`

After all callers are updated, `run_hindcast()` should not write the
intermediate CSV at all. The CSV write stays only in `__main__` for CLI usage.

**Risk check**: Are there any other consumers of the hindcast CSV files in
`{PATH}/hindcast/{MODEL}/`? Search the codebase for `hindcast/` path patterns.
If no other consumers exist, the intermediate CSV can be safely eliminated
from the function (callers that need CSV can write it themselves).

### Phase 5: Tests

**5a — Unit test for `run_hindcast()` return type and columns:**
- Mock data loading (discharge, ERA5, scalers)
- Mock `predictor.hindcast()` to return a known DataFrame
- Assert return has expected columns and dtypes
- Assert empty DataFrame when no codes match

**5b — Integration test: caller gets DataFrame directly:**
- Mock `run_hindcast()` in `fill_ml_gaps.py`
- Assert no `subprocess.run()` call
- Assert no `pd.read_csv()` call for hindcast file
- Assert returned DataFrame is used for gap filling

**5c — CLI backward compatibility:**
- Test `python hindcast_ML_models.py` still works with env vars
- Verify it writes both API and CSV from `__main__`

**5d — Error propagation:**
- Mock `run_hindcast()` to raise `FileNotFoundError`
- Verify callers catch it (existing `try/except` blocks)

### Phase 6: Verify End-to-End

1. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`
2. Verify all tests pass with zero skips
3. `git diff` review — no unintended changes
4. If environment available: run `fill_ml_gaps.py` locally to verify hindcast
   is called and data flows correctly without CSV intermediary

---

## Risks & Mitigations

### 1. Module-level side effects in `hindcast_ML_models.py` (MEDIUM)

The script has module-level code: logger setup, warning filters, sys.path
manipulation. When callers `import hindcast_ML_models`, these execute at import
time. This is already the case for the callers' own module-level code, but
importing hindcast may introduce duplicate loggers or path entries.

**Mitigation**: Move module-level setup into `__main__` or guard with
`if __name__ == "__main__"`. The `run_hindcast()` function should not depend
on module-level state beyond standard imports.

### 2. Environment variable coupling (LOW)

`run_hindcast()` still reads most config from env vars. This is intentional —
the env vars are set by `sl.load_environment()` which all callers already call.
The 5 function params replace only the vars that callers previously set via
`env[...] = ...` before `subprocess.run()`.

**Verify**: List all env vars read inside `run_hindcast()` body. Confirm all
callers have called `sl.load_environment()` before the call site.

### 3. Docker invocation (LOW)

`fill_ml_gaps.py` line 86 has a Docker-specific command path:
```python
if os.getenv("IN_DOCKER") == "True":
    command = ["python", "apps/machine_learning/hindcast_ML_models.py"]
```
After this refactor, Docker containers import the function directly — no
subprocess, no path issue. The `IN_DOCKER` branch becomes dead code in the
caller. Remove it.

### 4. Concurrent hindcast runs (LOW)

The current subprocess model provides process isolation — two concurrent
`fill_ml_gaps` runs get separate memory spaces. With direct function calls,
concurrent runs share the same process. This is fine because:
- The pipeline is sequential (fill_ml_gaps runs once per model/mode)
- `run_hindcast()` should be stateless (reads data, computes, returns)

### 5. `new_stations` param semantics (LOW)

`hindcast_ML_models.py` reads `ieasyhydroforecast_NEW_STATIONS` and, if not
"None", filters to only those station codes (lines 238-256). The function param
`new_stations` replaces this. Callers that don't filter pass `None`.

**Verify**: `add_new_station.py` and `recalculate_nan_forecasts.py` pass station
code lists; `fill_ml_gaps.py` sets `"None"` explicitly. Map each to the new
function signature.

### 6. No service code changes needed (OWNERSHIP RESPECTED)

Everything stays within `apps/machine_learning/`. No changes to
`sapphire/services/`, no new API endpoints.

---

## Scope Boundaries

**In scope**: Eliminating subprocess+CSV IPC, extracting importable function,
updating 4 callers, removing duplicate API writes.

**Out of scope** (separate issues):
- CSV schema corruption fix (`high_prio_gi_draft_ml_csv_schema_corruption_fix.md`)
- Write order in hindcast script (`mid_prio_gi_draft_ml_hindcast_api_consistency.md`)
- `_write_ml_daily_forecast_to_api()` dead code cleanup
- Making `run_hindcast()` fully parameterized (removing all env var reads) —
  that's a larger refactor beyond the IPC elimination goal

---

## Dependency Graph

```json
{
  "prerequisites": [
    {
      "name": "high_prio_gi_draft_ml_csv_schema_corruption_fix.md",
      "reason": "Provides ML_CANONICAL_CSV_COLUMNS and normalize_ml_csv_columns()"
    },
    {
      "name": "mid_prio_gi_draft_ml_hindcast_api_consistency.md",
      "reason": "Fixes write order in hindcast script; this issue changes who does the writing"
    }
  ],
  "phases": {
    "P1": {
      "name": "Extract run_hindcast() function from hindcast_ML_models.py main()",
      "file": "apps/machine_learning/hindcast_ML_models.py",
      "depends_on": [],
      "parallel_group": "A"
    },
    "P2a": {
      "name": "Update fill_ml_gaps.py: replace subprocess+CSV with run_hindcast() call",
      "file": "apps/machine_learning/fill_ml_gaps.py",
      "depends_on": ["P1"],
      "parallel_group": "B"
    },
    "P2b": {
      "name": "Update recalculate_nan_forecasts.py: replace subprocess+CSV with run_hindcast() call",
      "file": "apps/machine_learning/recalculate_nan_forecasts.py",
      "depends_on": ["P1"],
      "parallel_group": "B"
    },
    "P2c": {
      "name": "Update add_new_station.py: replace subprocess+CSV with run_hindcast() call",
      "file": "apps/machine_learning/add_new_station.py",
      "depends_on": ["P1"],
      "parallel_group": "B"
    },
    "P2d": {
      "name": "Update initialize_ml_tool.py: replace subprocess.run() with run_hindcast() call",
      "file": "apps/machine_learning/initialize_ml_tool.py",
      "depends_on": ["P1"],
      "parallel_group": "B"
    },
    "P3": {
      "name": "Verify no duplicate API writes; remove intermediate CSV from run_hindcast()",
      "files": [
        "apps/machine_learning/hindcast_ML_models.py",
        "apps/machine_learning/fill_ml_gaps.py",
        "apps/machine_learning/recalculate_nan_forecasts.py",
        "apps/machine_learning/add_new_station.py"
      ],
      "depends_on": ["P2a", "P2b", "P2c", "P2d"],
      "parallel_group": "C"
    },
    "P4": {
      "name": "Write tests (unit, integration, CLI backward compat, error propagation)",
      "file": "apps/machine_learning/test/",
      "depends_on": ["P1"],
      "parallel_group": "B",
      "note": "Tests can be written in parallel with P2 (mock run_hindcast())"
    },
    "P5": {
      "name": "Run full test suite, verify end-to-end",
      "depends_on": ["P3", "P4"],
      "parallel_group": "D"
    }
  },
  "execution_order": [
    {"sequential": ["P1"]},
    {"parallel": ["P2a", "P2b", "P2c", "P2d", "P4"]},
    {"sequential": ["P3"]},
    {"sequential": ["P5"]}
  ]
}
```
