# ML-002: Investigate hindcast subprocess root cause (why hindcast_ML_models.py fails)

**Status**: Draft — investigation complete, findings documented, fix requires coordination with Sandro
**Module**: machine_learning
**Priority**: High
**Labels**: `bug`, `investigation`, `api-migration`, `error-handling`

---

## Summary

`hindcast_ML_models.py` has multiple unguarded failure paths that cause the subprocess to crash before writing its output CSV. The production failure observed 2026-02-11 was a symptom (see ML-001); this issue documents the root causes inside the hindcast script itself.

## Context

`hindcast_ML_models.py` is invoked as a subprocess by three callers:
- `recalculate_nan_forecasts.py` — nightly maintenance (NaN gap-fill)
- `fill_ml_gaps.py` — on-demand gap-fill
- `initialize_ml_tool.py` — initial hindcast for new stations

The script has been **partially migrated** to the SAPPHIRE API: discharge reads (`fl.read_daily_discharge_data()`) and ERA5 meteo reads (`read_meteo_data_combined()`) both respect `SAPPHIRE_API_ENABLED`. The output also writes to the API via `_write_ml_forecast_to_api()`. However, the script lacks defensive error handling around these API calls and has several other unguarded crash paths.

## Problem

The script has **8 distinct failure vectors**, any of which prevents the output CSV from being written. The caller then crashes on `FileNotFoundError` when trying to read the missing CSV (fixed by ML-001's error handling, but the underlying failures remain).

### Environment variable note

The in-repo dev config (`apps/config/.env_develop_kghm`) defines ML-related variables **without** the `ieasyhydroforecast_` prefix (e.g. `PATH_TO_SCALER_TFT` instead of `ieasyhydroforecast_PATH_TO_SCALER_TFT`). This causes `TypeError` crashes in local dev. However, the production-like config (`kyg_data_forecast_tools/config/.env_kghm_bea`) has all variables correctly prefixed — so this is a **dev-config-only issue**, not the production crash cause.

---

## Technical Analysis

### Failure Vector 1 — `initialize_ml_tool.py` does not set `ieasyhydroforecast_NEW_STATIONS`

**File**: `hindcast_ML_models.py:230-232`
```python
NEW_STATIONS = os.getenv("ieasyhydroforecast_NEW_STATIONS")
if NEW_STATIONS != "None":
    new_stations = [int(code) for code in NEW_STATIONS.split(",")]
```

When `NEW_STATIONS` is Python `None` (not the string `"None"`), the condition `None != "None"` is `True`, and `None.split(",")` raises `AttributeError`.

**Callers**:
- `fill_ml_gaps.py:85` — sets `env["ieasyhydroforecast_NEW_STATIONS"] = "None"` (safe)
- `recalculate_nan_forecasts.py:86` — sets it to comma-separated codes (safe)
- `initialize_ml_tool.py:66-76` — **never sets it** (crash)

**Fix**: Guard with `if NEW_STATIONS is not None and NEW_STATIONS != "None":`, or have `initialize_ml_tool.py` set it explicitly.

### Failure Vector 2 — Unguarded API calls

**File**: `hindcast_ML_models.py:260` and `hindcast_ML_models.py:306`

Both `read_meteo_data_combined()` and `fl.read_daily_discharge_data()` can raise `SapphireAPIError` if the API is unreachable. Neither call has a try/except. Script crashes before writing any CSV.

**Fix**: Wrap in try/except with clear error message, or implement CSV fallback.

### Failure Vector 3 — No `.pt` model file found (IndexError)

**File**: `hindcast_ML_models.py:197-198`
```python
PATH_TO_MODEL = glob.glob(os.path.join(PATH_TO_SCALER, "*.pt"))[0]
```

If the scaler directory exists but contains zero `.pt` files, `glob.glob(...)` returns `[]` and `[0]` raises `IndexError`. No guard.

**Fix**: Check list length before indexing, raise descriptive error.

### Failure Vector 4 — Working directory mismatch in Docker

**File**: `recalculate_nan_forecasts.py:99` / `fill_ml_gaps.py:96`

All callers use `subprocess.run(command, ...)` without a `cwd` parameter. In Docker (`IN_DOCKER=True`), the command is `["python", "apps/machine_learning/hindcast_ML_models.py"]` — a relative path. If the container's working directory is `apps/machine_learning/` rather than the repo root, the path resolves incorrectly.

**Fix**: Pass explicit `cwd` to `subprocess.run()`, or use absolute paths.

### Failure Vector 5 — Static features index type mismatch

**File**: `hindcast_ML_models.py:292` and `hindcast_ML_models.py:316`
```python
static_features.index = static_features["code"]  # dtype from CSV (likely str)
# ...
for code in era5_data_transformed["code"].unique():  # int (cast at line 310)
    lat = static_features.loc[code]["LAT"]  # KeyError if str != int
```

If `static_features["code"]` is string and the lookup uses int, `KeyError` at line 316.

**Fix**: Ensure consistent dtype: `static_features.index = static_features["code"].astype(int)`.

### Failure Vector 6 — Empty API response causes crash on `.min()`

**File**: `hindcast_ML_models.py:267`
```python
if pd.to_datetime(start_date) < era5_data_transformed["date"].min() + pd.DateOffset(days=60):
```

If the API returns no meteo data, `era5_data_transformed` is empty. `.min()` on an empty series returns `NaT`, and the comparison raises `TypeError`.

**Fix**: Check `if era5_data_transformed.empty:` before the comparison.

### Failure Vector 7 — Missing scaler CSV files

**File**: `hindcast_ML_models.py:297-302`
```python
scaler_discharge = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_discharge.csv"))
scaler_era5 = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_era5.csv"))
scaler_static = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_static.csv"))
```

No existence check, no try/except. If any scaler CSV is missing, `FileNotFoundError` kills the script.

**Fix**: Check existence or wrap in try/except with clear error message.

### Failure Vector 8 — Exception swallowing in predictor

**File**: `BaseDartsDLPredictor.py` (in `hindcast()` method, approximately line 467)
```python
except Exception as e:
    print(e)
    print(f"Error in hindcasting for code {code}")
    return pd.DataFrame()
```

Exceptions are swallowed with `print()` only — no `logger.error()`, no traceback. This causes silent data loss: the hindcast loop produces an empty DataFrame, which is written as a headers-only CSV. The caller reads it successfully (no `FileNotFoundError`) but gets no data.

**Fix**: Use `logger.error(...)` with `exc_info=True` for full traceback.

---

### Failure Vector 9 — Predictor RUNS but emits all-NaN output (no crash, no empty df)

**Observed**: local review 2026-06-19 (tjhm + kghm), `make_forecast.py` operational + hindcast.

Distinct from Vectors 1–8 (which crash) and from Vector 8's swallowed-exception → empty DataFrame.
Here `predictor.predict()` / `predictor.hindcast()` **execute successfully** (darts logs
`Predicting DataLoader 0: 100%`) but return a frame whose value/quantile columns are **all NaN**.
`make_forecast.py:745-752` then re-derives `flag=1` from the NaN output (placeholder), and
`recalculate_nan_forecasts.py` later marks the still-NaN rows `flag=3` (permanent failure). No
exception is ever raised, so none of the Vector 1–8 guards fire.

**Root cause (verified 2026-06-19 with sentinel code `19999`):** the future-covariate block contains
a NaN inside the model-required lookback/forecast window. `make_forecast.py:736-741` passes
`qmapped_era5_code` straight to `predictor.predict()`; `BaseDartsDLPredictor._prepare_data()` then
scales `P`, `T`, `PET`, and `daylight_hours` without filling or rejecting NaNs (`:284-288`), and
`_create_time_series()` builds the Darts `future_covariates` series (`:337-340`). Darts does not
raise; `model.predict()` (`:386-394`) returns a full raw prediction tensor of NaNs, which
`create_prediction_df()` converts into all-NaN quantile columns.

The decisive missing block was `P` and `T` on **2026-05-28** in the control-member forcing. `PET` was
finite (`0.0`) and daylight was finite. Target discharge, rolling past covariates, static features,
discharge scalers, ERA5 scalers, and static scalers were finite.

**Why the cross-site/model pattern looked split:**
- **TFT failed on both sites** because the Decad TFT artifact has `input_chunk_length=30`; for an
  issue date of 2026-06-19 its covariate window starts 2026-05-19, so it includes the 2026-05-28
  P/T NaN on both deployments.
- **TiDE/TSMixer failed on tjhm but not kghm** because the tjhm Decad TiDE/TSMixer artifacts also use
  `input_chunk_length=30`, while the kghm Decad TiDE artifact uses `input_chunk_length=20`; the kghm
  TiDE covariate window starts 2026-05-29 and avoids the 2026-05-28 NaN. The failing tjhm
  TiDE/TSMixer runs include the same 2026-05-28 P/T NaN as TFT.

**Falsified hypotheses:**
- Not a `TFT.pt` artifact/load/version failure: the kghm Decad `TFT.pt` produced finite raw output on
  synthetic finite target/covariates, and the same operational TFT run became finite after only
  interpolating/filling the covariate NaNs.
- Not scaler/static/PET: scaler parameters and static rows were finite; PET was finite even on the
  bad date.

**Smallest proposed fix:** add a covariate finite-input gate in `BaseDartsDLPredictor` before Darts
series creation, scoped to the required model window (`input_chunk_length + forecast_horizon + 1`):
first log per-code NaN origins by block/column/date, then either (a) fail fast to flag an input-data
problem, or (b) interpolate isolated `P`/`T` gaps with bounded `ffill`/`bfill` before scaling. The
verified remediation is (b): filling `P`/`T` covariate NaNs made TFT, TiDE, and TSMixer emit finite
raw predictions without changing model artifacts, target discharge, static features, or scalers.

**NaN-origin diagnostic to keep:** when predictions contain NaN, log a compact per-code diagnostic:
model, horizon, input/output lengths, target NaN count, future-covariate NaN rows by date/column for
the required window, past-covariate NaN counts, static NaN columns, scaler zero-range/NaN columns,
and raw prediction NaN count. Do not log real station codes or discharge values in committed
artifacts; tests should use sentinel `19999`.

This NaN-output vector is the likely reason ML-015's timeout fix alone does not restore forecasts
(remediation completes but still yields NaN). See ML-015 "Field evidence — 2026-06-19".

## ERA5 CSV Filename Divergence (secondary)

When `SAPPHIRE_API_ENABLED=false`, the CSV fallback paths differ between scripts:

| Script | ERA5 CSV pattern |
|--------|-----------------|
| `hindcast_ML_models.py:257-258` | `{HRU}_T_reanalysis.csv` / `{HRU}_P_reanalysis.csv` |
| `make_forecast.py:391-392` | `{HRU}_T_control_member.csv` / `{HRU}_P_control_member.csv` |

These are different files from different pipeline stages. Irrelevant when the API is enabled (both use `read_meteo_data_combined()`), but the CSV fallback is broken for hindcast.

---

## Implementation Plan

### Approach

Prioritize fixes by likelihood of causing the observed production failure. Group into phases by dependency.

### Phase 1 — Critical guards (no behavior change, purely additive)

**Files to modify**: `hindcast_ML_models.py`

| Step | Fix | Lines |
|------|-----|-------|
| 1a | Guard `NEW_STATIONS`: `if NEW_STATIONS is not None and NEW_STATIONS != "None":` | 230-232 |
| 1b | Guard `.pt` glob: check list non-empty before `[0]`, raise descriptive `FileNotFoundError` | 197-198 |
| 1c | Guard empty meteo data: `if era5_data_transformed.empty: raise RuntimeError(...)` | before 267 |
| 1d | Guard empty discharge data: check before entering per-station loop | before 393 |
| 1e | Guard scaler CSVs: existence check before `pd.read_csv` | 297-302 |
| 1f | Cast static features index to int: `static_features.index = static_features["code"].astype(int)` | 292 |

### Phase 2 — API error handling

**Files to modify**: `hindcast_ML_models.py`

| Step | Fix | Lines |
|------|-----|-------|
| 2a | Wrap `read_meteo_data_combined()` in try/except, log error and exit cleanly | 260 |
| 2b | Wrap `fl.read_daily_discharge_data()` in try/except, log error and exit cleanly | 306 |

### Phase 3 — Observability

**Files to modify**: `BaseDartsDLPredictor.py` (or equivalent predictor base class)

| Step | Fix | Lines |
|------|-----|-------|
| 3a | Replace `print(e)` with `logger.error("Hindcast failed for code %s", code, exc_info=True)` | ~467 |

### Phase 4 — Subprocess invocation hardening

**Files to modify**: `recalculate_nan_forecasts.py`, `fill_ml_gaps.py`, `initialize_ml_tool.py`

| Step | Fix | Lines |
|------|-----|-------|
| 4a | Add explicit `cwd` parameter to `subprocess.run()` calls | varies |
| 4b | In `initialize_ml_tool.py`, set `env["ieasyhydroforecast_NEW_STATIONS"] = "None"` | ~73 |

### Phase 5 — Dev config alignment (separate, low priority)

**Files to modify**: `apps/config/.env_develop_kghm`

| Step | Fix |
|------|-----|
| 5a | Add `ieasyhydroforecast_` prefix to the 10 variables that lack it |
| 5b | Add 3 missing variables: `ieasyhydroforecast_models_and_scalers_path`, `ieasyhydroforecast_OUTPUT_PATH_REANALYSIS`, `ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts` |

### Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1, "note": "Critical guards in hindcast_ML_models.py" },
    "P2": { "depends_on": [], "parallel_agents": 1, "note": "API error handling in hindcast_ML_models.py" },
    "P3": { "depends_on": [], "parallel_agents": 1, "note": "Logging in predictor base class" },
    "P4": { "depends_on": [], "parallel_agents": 2, "note": "Subprocess call hardening" },
    "P5": { "depends_on": [], "parallel_agents": 1, "note": "Dev config alignment" }
  }
}
```

All phases are independent and can be executed in parallel.

---

## Testing

### Test Cases

- [ ] Test `NEW_STATIONS=None` (Python None) does not crash — falls through to default behavior
- [ ] Test empty meteo data returns clean error, not `TypeError`
- [ ] Test empty discharge data returns clean error
- [ ] Test missing `.pt` file raises descriptive `FileNotFoundError`, not `IndexError`
- [ ] Test missing scaler CSVs raise descriptive error
- [ ] Test static features with string codes still work (int cast)
- [ ] Test API unreachable produces clear error log, not raw traceback
- [ ] Test predictor exception produces full traceback in logs

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

### Investigation with Sandro

To determine which specific failure vector caused the 2026-02-11 production crash:

1. Check Docker container logs from that date for the actual exception type/message
2. Confirm which caller triggered the failure (`recalculate_nan_forecasts.py` vs `initialize_ml_tool.py`)
3. Check if the SAPPHIRE API was reachable from inside the ML Docker container
4. Verify `.pt` model files exist in the expected scaler directories on the server

---

## Out of Scope

- Refactoring `hindcast_ML_models.py` into smaller functions (tech debt, separate issue)
- Adding retry logic for transient API failures
- Removing the CSV fallback path entirely (depends on API stability confirmation)
- Fixing ERA5 CSV filename divergence (low priority while API is primary)

## Dependencies

- **ML-001** (complete): Caller-side error handling for subprocess failure
- **ML-003**: API-primary reads migration — related but separate scope
- **ML-004**: Hindcast API write bugs — related but separate scope
- Sandro's input needed for: production log analysis, confirming which failure vector hit

## Acceptance Criteria

- [ ] `hindcast_ML_models.py` does not crash with `AttributeError`, `IndexError`, `TypeError`, or `KeyError` on any of the identified paths
- [ ] Every failure produces a clear, logged error message with enough context to diagnose
- [ ] `predictor.hindcast()` exceptions include full traceback in logs
- [ ] All three callers pass valid `ieasyhydroforecast_NEW_STATIONS` values
- [ ] Existing successful hindcast flow is unaffected
- [ ] New tests cover each failure vector
- [ ] All tests pass with zero unexpected skips

---

## References

- ML-001: Caller-side error handling (complete) — `review_gi_draft_ml_maintenance_hindcast_file_not_found.md`
- Production error logs from 2026-02-11
- Dev config: `apps/config/.env_develop_kghm` (missing prefixes)
- Production config: `kyg_data_forecast_tools/config/.env_kghm_bea` (correct prefixes)
