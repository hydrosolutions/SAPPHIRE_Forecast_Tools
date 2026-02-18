# INFRA-005: Remove `model_long` from app pipeline

**Status**: Draft
**Module**: infra (cross-module)
**Priority**: Medium
**Labels**: `refactoring`, `cross-module`, `maintainability`, `incremental`

---

## Summary

Remove `model_long` from the internal data flow of all app modules. Long display names should be defined only in sapphire services (`ModelType.description`) and resolved at presentation boundaries (API responses, dashboard display). App modules work with `model_short` exclusively.

This is an **incremental** refactoring — each module is cleaned up when it is next refactored for other reasons.

## Architectural Decision

Three approaches were evaluated:

| Approach | Verdict | Reason |
|----------|---------|--------|
| Python registry module (`model_registry.py`) | Rejected | Consolidates the dicts but keeps `model_long` flowing through the pipeline — treats a symptom, not the cause |
| Shared `config.yaml` | Rejected | Still version-controlled, adds file I/O complexity, no type safety |
| Database table / sapphire services | **Adopted** | `model_long` is a display concern; the server-side `ModelType.description` is the single source of truth |

**Key principle:** `model_long` is display metadata, not data. It should not be carried through the pipeline as a DataFrame column. When a presentation boundary needs it, it gets it from the API response (`model_type_description` field) or from `ModelType.description` on the server side.

## Problem

13+ independent locations define or carry `model_long`:

| # | Location | Form | Models covered |
|---|----------|------|----------------|
| 1 | `postprocessing_forecasts/src/data_reader.py:14-27` | `MODEL_SHORT_TO_LONG` dict | 8 (LR, TFT, TiDE, TSMixer, ARIMA, RRAM, EM, NE) |
| 2 | `postprocessing_forecasts/src/data_reader.py:30-38` | `API_MODEL_TYPE_TO_SHORT` dict | 7 (unused in production) |
| 3 | `postprocessing_forecasts/src/api_writer.py:118-126` | `model_type_map` dict | 7 (duplicate of #4) |
| 4 | `postprocessing_forecasts/src/api_writer.py:318-326` | `model_type_map` dict | 7 (duplicate of #3) |
| 5 | `iEasyHydroForecast/setup_library.py:1232-1238` | `model_mapping` dict | 5 ML models |
| 6 | `iEasyHydroForecast/setup_library.py` (6 inline sites) | Hardcoded strings | LR, Obs, RRAM |
| 7 | `iEasyHydroForecast/setup_library.py:2796-2929` | Two if/elif chains | 4 ML models × 2 (pentad + decad) |
| 8 | `forecast_dashboard/src/processing.py:189-206` | if/elif chain | 4 ML models |
| 9 | `forecast_dashboard/src/processing.py:277` | Inline string | NE |
| 10 | `forecast_dashboard/src/db.py:276-277, 348-349` | Inline strings | NE, LR |
| 11 | `forecast_dashboard/src/vizualization.py:2471` | Inline string | RRAM |
| 12 | `postprocessing_forecasts/tests/test_constants.py:8-15` | `MODEL_LONG_NAMES` dict | 6 |
| 13 | `postprocessing_forecasts/tests/generate_test_data.py:70` | Local `MODEL_LONG_NAMES` | 6 |

**Known bugs in current mappings:**

| Bug | Current (wrong) | Correct |
|-----|-----------------|---------|
| RRAM mislabeled in `data_reader.py` | "Rainfall-Runoff Mamba (RRAM)" | "Rainfall runoff assimilation model (RRAM)" |
| TFT hyphenated in dashboard `processing.py` | "Temporal-Fusion Transformer (TFT)" | "Temporal Fusion Transformer (TFT)" |
| RRAM mislabeled as Rainfall-Runoff Mamba | See RRAM row above | RRMAMBA removed; RRAM = Rainfall runoff assimilation model (conceptual) |

---

## Per-Module Removal Checklist

Each entry is addressed when that module is next refactored. Check off items as they are completed.

### `iEasyHydroForecast/setup_library.py` (~10 locations)

- [ ] Lines 1175-1176: `_read_lr_forecasts_from_api()` — remove `model_long = "Linear regression (LR)"`
- [ ] Lines 1232-1238: `_read_ml_forecasts_from_api()` — remove `model_mapping` dict, stop setting `model_long`
- [ ] Lines 1440-1441, 1465-1466: observed data readers (pentad) — remove `model_long = "Observed (Obs)"`
- [ ] Lines 1556-1557, 1581-1582: observed data readers (decade) — same
- [ ] Lines 1705, 1856: `read_forecast_data_pentad/decade()` — remove `model_long = "Linear regression (LR)"`
- [ ] Lines 2462, 2512: conceptual model readers — remove `model_long = "Rainfall runoff assimilation model (RRAM)"`
- [ ] Lines 2796-2818, 2907-2929: `read_probabilistic_forecast_pentad/decade()` — simplify if/elif chains, remove model_long assignment

### `postprocessing_forecasts/src/data_reader.py`

- [ ] Lines 14-27: Remove `MODEL_SHORT_TO_LONG` dict
- [ ] Lines 30-38: Remove `API_MODEL_TYPE_TO_SHORT` dict (unused in production)
- [ ] Line 201: Remove `.map(MODEL_SHORT_TO_LONG)` — stop adding model_long column after API read

### `postprocessing_forecasts/src/api_writer.py`

- [ ] Lines 118-126, 318-326: Deduplicate `model_type_map` (two identical dicts). Note: these map `model_short` → API `model_type` (case normalization for TiDE/TSMixer), NOT `model_long`. Keep the mapping but move to module level as a single constant.

### `postprocessing_forecasts/src/skill_metrics.py`

- [ ] Remove `model_long` from `groupby` — group by `model_short` only
- [ ] Lines 516-519, 721-724: Refactor malformed ensemble name filters to use `model_short`

### `postprocessing_forecasts/src/ensemble_calculator.py`

- [ ] `model_long_agg()`: Refactor to build composition string from `model_short` directly instead of regex-extracting short names from `model_long`

### `forecast_dashboard/src/processing.py`

- [ ] Lines 189-206: Remove if/elif chain setting model_long — dashboard gets `model_type_description` from API response instead
- [ ] Line 277: Remove hardcoded `"Neural Ensemble (NE)"`

### `forecast_dashboard/src/db.py`

- [ ] Lines 276-277: Remove hardcoded `"Neural Ensemble (NE)"` — use API `model_type_description`
- [ ] Lines 348-349: Remove hardcoded `"Linear regression (LR)"` — same

### `forecast_dashboard/src/vizualization.py`

- [ ] Line 2471: Remove hardcoded `"Rainfall runoff assimilation model (RRAM)"`

### `postprocessing_forecasts/tests/`

- [ ] `test_constants.py:8-15`: Remove `MODEL_LONG_NAMES` dict — tests assert on `model_short`
- [ ] `generate_test_data.py:70`: Remove local `MODEL_LONG_NAMES`
- [ ] `test_ensemble_calculator.py`: Update `TestModelNameConsistency` tests

### `long_term_forecasting/lt_utils.py`

- [ ] Lines 134-145: `MODEL_NAME_TO_MODEL_TYPE` — identity mapping, no model_long (low priority, no change needed unless module is refactored)

---

## Server-Side Updates Needed

`sapphire/services/postprocessing/app/models.py` — `ModelType.description` must be complete before apps can rely on it:

- [ ] Add long-term model descriptions (GBT, LR_Base, LR_SM, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_Norm, Skilled Mean, Naive Mean)
- [ ] Fix RRAM description: add "(RRAM)" suffix for consistency
- [ ] Fix TiDE description: currently "(TIDE)" should be "(TiDE)"
- [ ] Verify Obs model has a ModelType entry

---

## Acceptance Criteria (long-term)

- [ ] No `model_long` string literals or mapping dicts in any app module source code
- [ ] No `model_long` column in internal DataFrames (only added at CSV write time or dashboard display, sourced from API)
- [ ] `ModelType.description` in sapphire services is complete and is the single source for all long names
- [ ] All existing tests pass with 0 skips after each module cleanup

## Risks

- **Medium**: CSV backward compatibility — external consumers may expect `model_long` column. Mitigation: keep writing it to CSV during transition, but source the value from API response rather than local dicts.
- **Low**: Dashboard CSV fallback — when API is down, dashboard reads CSVs. If CSVs still have `model_long`, no impact. Long-term, dashboard should get long names from API only.
- **Watch**: i18n in dashboard — `internationalize_forecast_model_names()` applies gettext to model_long strings. Translation `.po` files are keyed on English long names. Verify i18n still works when source of long names changes from local dicts to API.

---

*This is an incremental refactoring tracked per module, not a single implementation task.*
