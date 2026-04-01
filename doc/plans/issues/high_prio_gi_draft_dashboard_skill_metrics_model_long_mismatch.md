# GitHub Issue: FD-006

**Title**: `fix(forecast_dashboard): Skill metrics missing for LR and NE due to model_long merge key mismatch`

**Labels**: `bug`, `forecast_dashboard`, `high-priority`

**Assignee**: @maxatp

**Status**: Draft

---

## Summary

The skill metrics plots (effectiveness and accuracy) show no data for Linear Regression (LR) and Neural Ensemble (NE) models. The root cause is that `model_long` is included in the merge key between `forecasts_all` and `forecast_stats`, and the `model_long` values differ between the two DataFrames for these models.

## Root Cause

In `src/db.py:get_data()` (lines 428, 436-440), `forecasts_all` is merged with `forecast_stats`:

```python
merge_keys = ["code", hin, "model_short", "model_long"]
...
data["forecasts_all"] = forecasts_all.merge(
    forecast_stats,
    on=merge_keys,
    how="left",
    suffixes=("", "_stats"),
)
```

The `model_long` values differ between the two DataFrames:

| Model | In `forecasts_all` (hardcoded in dashboard) | In `forecast_stats` (from API `model_type_description`) |
|-------|---------------------------------------------|--------------------------------------------------------|
| **LR** | `"Linear regression (LR)"` (db.py:372) | `"Linear Regression"` |
| **NE** | `"Neural Ensemble (NE)"` (db.py:293) | `"Neural Ensemble with TIDE, TFT, TSMixer (NE)"` |
| TFT | `"Temporal Fusion Transformer (TFT)"` | `"Temporal Fusion Transformer (TFT)"` — match |
| TiDE | `"Time-Series Dense Encoder (TIDE)"` | `"Time-Series Dense Encoder (TIDE)"` — match |
| TSMixer | `"Time-Series Mixer (TSMixer)"` | `"Time-Series Mixer (TSMixer)"` — match |
| EM | `"Ens. Mean with LR, TFT, TIDE (EM)"` | `"Ens. Mean with LR, TFT, TIDE (EM)"` — match |

Because LR and NE don't match on `model_long`, the LEFT merge produces NaN for all skill columns (`sdivsigma`, `accuracy`, `mae`, `delta`). The skill plot function `plot_forecast_skill()` (vizualization.py:4255) then drops NaN rows:

```python
.apply(lambda x: x.dropna(subset=['sdivsigma', 'accuracy']).head(1))
```

This removes all LR and NE rows from the plot entirely.

### Where the mismatches originate

- **LR**: `get_forecasts_all()` hardcodes `model_long = "Linear regression (LR)"` at db.py:372. The API's `ModelType.description` for LR is `"Linear Regression"` (defined in `sapphire/services/postprocessing/app/models.py:53`).

- **NE**: `get_forecasts_all()` synthesizes the Neural Ensemble in-code and sets `model_long = "Neural Ensemble (NE)"` at db.py:293. The API's `ModelType.description` for NE is `"Neural Ensemble with TIDE, TFT, TSMixer (NE)"` (models.py:50).

### Verified: data exists in the database

Skill metrics for LR and NE are present and complete (pentads 1-72):

```
LR:  216 rows, pentads 1-72, model_type_description="Linear Regression"
NE:  216 rows, pentads 1-72, model_type_description="Neural Ensemble with TIDE, TFT, TSMixer (NE)"
```

The data is fetched correctly but discarded during the merge.

## Proposed Fix

Remove `model_long` from the merge key. The `model_short` values (`"LR"`, `"NE"`, `"TFT"`, etc.) are stable identifiers that match between both DataFrames. `model_long` is a display concern and should not be used as a join key.

In `src/db.py:get_data()`, change line 428 from:

```python
merge_keys = ["code", hin, "model_short", "model_long"]
```

to:

```python
merge_keys = ["code", hin, "model_short"]
```

This is the minimal fix. No other code needs to change — `model_long` remains available in both DataFrames for display purposes; it just won't participate in the join.

### Note on "missing first part of year"

For models other than LR/NE, the skill plots may show gaps in early pentads. This is mostly due to real data gaps:
- TSMixer forecasts start at pentad 6 (late January)
- EM forecasts start at pentad 2

These are upstream data gaps, not a dashboard display bug. However, the fan-out issue from FD-003 (duplicate rows from skill metric merge) may cause confusing overlapping entries in the plots — fixing FD-003 will clean that up.

## Related Issues

- **FD-003** (skill metric merge fan-out): same merge, different bug — multiple skill metric rows per key cause row multiplication
- **FD-004** (LR delta dropped): LR bounds also missing due to a separate column drop issue

## Tasks

- [ ] Remove `"model_long"` from `merge_keys` in `get_data()` (`src/db.py:428`)
- [ ] Verify locally: run `panel serve` and confirm LR and NE appear in the effectiveness and accuracy skill plots
- [ ] Check that skill values displayed match the database values for LR and NE
- [ ] Run tests: `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`
