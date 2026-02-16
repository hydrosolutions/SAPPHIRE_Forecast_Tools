# INFRA-005: Single source of truth for model name mappings

**Status**: Draft
**Module**: infra (cross-module)
**Priority**: Medium
**Labels**: `refactoring`, `cross-module`, `maintainability`

---

## Summary

Consolidate the 5 independent model-name mapping dictionaries (scattered across `postprocessing_forecasts`, `iEasyHydroForecast`, `forecast_dashboard`, and test constants) into a single canonical registry in `iEasyHydroForecast/model_registry.py`. Adding a new model should require editing one file, not five.

## Context

Model names appear as two columns throughout the pipeline: `model_short` (e.g., `"TFT"`) and `model_long` (e.g., `"Temporal Fusion Transformer (TFT)"`). These mappings are currently duplicated in multiple places, and they are already drifting — for example, `data_reader.py` knows about both `RRMAMBA` and `RRAM`, while `setup_library.py` only knows `RRMAMBA`, and the dashboard only knows 4 ML models.

## Problem

Five independent copies of the mapping exist:

| # | Location | Form | Models covered |
|---|----------|------|----------------|
| 1 | `postprocessing_forecasts/src/data_reader.py:17-27` | `MODEL_SHORT_TO_LONG` dict | 9 (LR, TFT, TiDE, TSMixer, ARIMA, RRMAMBA, RRAM, EM, NE) |
| 2 | `postprocessing_forecasts/src/data_reader.py:30-38` | `API_MODEL_TYPE_TO_SHORT` dict | 7 (LR, TFT, TiDE, TSMixer, EM, NE, RRAM) |
| 3 | `iEasyHydroForecast/setup_library.py:1232-1238` | `model_mapping` dict (local variable) | 5 ML models only (TFT, TIDE, TSMIXER, ARIMA, RRMAMBA) |
| 4 | `forecast_dashboard/src/processing.py:189-206` | if/elif chain with hardcoded strings | 4 ML models (TFT, TIDE, TSMIXER, ARIMA) |
| 5 | `postprocessing_forecasts/tests/test_constants.py:8-15` | `MODEL_LONG_NAMES` dict | 6 (LR, TFT, TiDE, TSMixer, EM, NE) |

Additionally, inline string literals are used in:
- `setup_library.py:1175-1176` — `"Linear regression (LR)"` / `"LR"` (pentad LR)
- `setup_library.py:1705-1706` — same (pentad observed→LR format)
- `setup_library.py:1855-1857` — same (decad observed→LR format)
- `forecast_dashboard/src/processing.py:261` — `"Neural Ensemble (NE)"` / `"NE"`

**Consequences of current state:**
- Adding a new model requires editing 3–5 files across different modules
- Drift has already happened (RRMAMBA/RRAM inconsistency; dashboard missing models)
- The dashboard if/elif chain raises `ValueError` for any model it doesn't know — adding an upstream model without updating the dashboard causes a crash

## Desired Outcome

- One canonical file defines all model names
- All consumers import from that file
- Adding a new model = adding one entry to one dict
- The dashboard handles unknown models gracefully (or discovers them from the registry)
- Tests validate registry completeness (all models in registry have both short and long names)

---

## Technical Design

### New file: `iEasyHydroForecast/model_registry.py`

```python
"""Single source of truth for model name mappings.

Every module that needs to map between model_short, model_long, and
API model_type values should import from here.
"""

# Canonical mapping: model_short -> model_long
MODEL_REGISTRY: dict[str, str] = {
    "LR":      "Linear regression (LR)",
    "TFT":     "Temporal Fusion Transformer (TFT)",
    "TiDE":    "Time-series Dense Encoder (TiDE)",
    "TSMixer": "Time-Series Mixer (TSMixer)",
    "ARIMA":   "AutoRegressive Integrated Moving Average (ARIMA)",
    "RRAM":    "Rainfall-Runoff Mamba (RRAM)",
    "EM":      "Ensemble Mean (EM)",
    "NE":      "Neural Ensemble (NE)",
    "Obs":     "Observed (Obs)",
}

# API model_type values that differ from the canonical short name.
# Used when reading from the SAPPHIRE API, which may use uppercase
# or legacy names.
API_TYPE_ALIASES: dict[str, str] = {
    "RRMAMBA": "RRAM",
    "TIDE":    "TiDE",
    "TSMIXER": "TSMixer",
}


def short_to_long(short: str) -> str:
    """Return the long display name for a model short name.

    Raises:
        KeyError: If short is not in MODEL_REGISTRY.
    """
    return MODEL_REGISTRY[short]


def api_type_to_short(api_type: str) -> str:
    """Normalize an API model_type to the canonical short name.

    Handles case differences and legacy names (e.g., RRMAMBA -> RRAM).
    Returns the input unchanged if no alias exists and the value is
    already a valid short name.
    """
    return API_TYPE_ALIASES.get(api_type, api_type)
```

**Why `iEasyHydroForecast/`**: All active modules already import from this package (`forecast_library`, `setup_library`, `tag_library`). No new cross-module dependency is introduced.

### Migration: file-by-file changes

#### 1. `postprocessing_forecasts/src/data_reader.py`

**Before:**
```python
MODEL_SHORT_TO_LONG = {
    "LR": "Linear regression (LR)",
    ...
}
API_MODEL_TYPE_TO_SHORT = {
    "LR": "LR",
    ...
}
```

**After:**
```python
from model_registry import MODEL_REGISTRY as MODEL_SHORT_TO_LONG
from model_registry import API_TYPE_ALIASES, api_type_to_short
```

The local names `MODEL_SHORT_TO_LONG` stay the same so callers within the file don't change. `API_MODEL_TYPE_TO_SHORT` is replaced by `api_type_to_short()` at the call site (only used in `_normalize_api_skill_metrics()`).

#### 2. `iEasyHydroForecast/setup_library.py:1232-1238`

**Before:** Local `model_mapping` dict with `(model_long, model_short)` tuples.

**After:**
```python
from model_registry import MODEL_REGISTRY, api_type_to_short

# In read_daily_probabilistic_ml_forecasts_pentad_from_api():
model_short = api_type_to_short(model.upper())
model_long = MODEL_REGISTRY[model_short]
```

Also replace inline `"Linear regression (LR)"` at lines 1175, 1705, 1855 with `MODEL_REGISTRY["LR"]`.

#### 3. `forecast_dashboard/src/processing.py:189-206`

**Before:** if/elif chain mapping model name to `model_long`/`model_short`.

**After:**
```python
from model_registry import MODEL_REGISTRY, api_type_to_short

model_short = api_type_to_short(model.upper())
if model_short not in MODEL_REGISTRY:
    raise ValueError(
        f"Unknown model '{model}'. Known models: "
        f"{list(MODEL_REGISTRY.keys())}"
    )
model_long = MODEL_REGISTRY[model_short]
```

Also replace the inline `"Neural Ensemble (NE)"` at line 261 with `MODEL_REGISTRY["NE"]`.

#### 4. `postprocessing_forecasts/tests/test_constants.py`

**Before:** Duplicate `MODEL_LONG_NAMES` dict.

**After:**
```python
from model_registry import MODEL_REGISTRY

# Subset used in tests (EM, NE are generated, not input models)
MODEL_LONG_NAMES = {
    k: v for k, v in MODEL_REGISTRY.items()
    if k in ('LR', 'TFT', 'TiDE', 'TSMixer', 'EM', 'NE')
}
```

#### 5. Deprecation of `RRMAMBA`

The API alias `RRMAMBA -> RRAM` handles backward compatibility. The canonical short name is `RRAM`. Remove `RRMAMBA` from `MODEL_REGISTRY` (it only exists in the alias table). Grep for any remaining `RRMAMBA` references and update them.

---

## Implementation Steps

1. Create `iEasyHydroForecast/model_registry.py` with the registry dict, alias dict, and two helper functions
2. Add unit tests in `iEasyHydroForecast/tests/test_model_registry.py`:
   - `short_to_long()` returns correct long name for each model
   - `short_to_long()` raises `KeyError` for unknown model
   - `api_type_to_short()` resolves aliases (`RRMAMBA` -> `RRAM`, `TIDE` -> `TiDE`)
   - `api_type_to_short()` passes through known short names unchanged
   - Registry completeness: every value matches the pattern `"<Name> (<SHORT>)"`
3. Migrate `data_reader.py` — delete local dicts, import from registry
4. Migrate `setup_library.py` — replace local `model_mapping` and inline strings
5. Migrate `forecast_dashboard/src/processing.py` — replace if/elif chain
6. Migrate `test_constants.py` — import from registry
7. Run full test suite (`SAPPHIRE_TEST_ENV=True bash run_tests.sh`) — no test changes expected beyond imports
8. Grep for any remaining hardcoded model name strings; update stragglers

## Acceptance Criteria

- [ ] `model_registry.py` is the only file that defines model short/long name mappings
- [ ] All 5 former locations import from the registry (no local dicts or inline strings)
- [ ] `api_type_to_short()` handles legacy API names (RRMAMBA, TIDE, TSMIXER)
- [ ] Adding a test model to the registry makes it available in all consumers without further changes
- [ ] All existing tests pass with 0 skips
- [ ] New unit tests for the registry (at least 5 tests covering happy path, error, aliases, completeness)

## Risks

- **Low**: All modules already depend on `iEasyHydroForecast`, so no new dependency edges
- **Low**: The mappings are string constants — import-time failures would be caught immediately by any test run
- **Watch**: The `forecast_dashboard` uses `internationalize_forecast_model_names()` which applies `_()` (gettext) to model names. Verify that i18n still works after the migration (the translated strings are in `.po` files keyed on the English long names, which don't change)

---

*Estimated effort: ~2 hours (mostly mechanical find-and-replace + test verification)*
