# ML-003: Fix broken `quantile_timeseries()` call after darts upgrade

**Status**: Draft
**Module**: machine_learning
**Priority**: High
**Assignee**: @sandrohuni
**Labels**: `bug`, `dependency`, `production`

---

## Summary

The TFT forecast silently fails for every station because `darts` removed `TimeSeries.quantile_timeseries()` in version 0.36.0 (2025-06-29). The project now pins `darts==0.43.0`, seven major versions past the removal. The broad exception handler in the prediction loop catches the `AttributeError` and returns an empty DataFrame, so the pipeline completes without crashing — but **all probabilistic forecasts are silently dropped**.

## Evidence

Server log from 2026-04-01 (`ml_TFT`):

```
'TimeSeries' object has no attribute 'quantile_timeseries'
Error in predicting for code 15214
```

**Reproduced locally** (macOS, same darts==0.43.0) with `SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD`. Every station fails identically — codes 16096, 16153, 15312, 16101, 15051 all hit the same error. The pipeline still exits with "Forecast saved successfully" because the exception is caught at `BaseDartsDLPredictor.py:398-401`.

## Root Cause

**File**: `apps/machine_learning/scr/BaseDartsDLPredictor.py`, line 239

```python
for q in self.quantiles:
    quantile_pred = predictions.quantile_timeseries(q)  # <-- removed in darts 0.36.0
```

The `quantile_timeseries()` method was removed without a deprecation period in [darts PR #2826](https://github.com/unit8co/darts/pull/2826), released in darts 0.36.0 (2025-06-29). The last version that had it was darts 0.35.0 (2025-04-18).

## Fix

One-line change in `BaseDartsDLPredictor.py:239`:

```python
# Before (darts <= 0.35.0):
quantile_pred = predictions.quantile_timeseries(q)

# After (darts >= 0.36.0):
quantile_pred = predictions.quantile(q=q)
```

The return type and behavior are identical — `quantile()` returns a `TimeSeries` with the same `.values()` method used on line 240.

### Verification

After applying the fix:

1. Run TFT forecast for at least one station and confirm quantile columns (Q10, Q50, Q90) are populated in the output DataFrame
2. Verify the log no longer shows `Error in predicting for code` messages
3. Run existing ML tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning`

## Impact

- **All TFT probabilistic forecasts are currently broken** — quantile predictions are silently dropped for every station
- The pipeline does not crash (exception is caught), so this failure is invisible unless logs are inspected
- Affects all models that inherit from `BaseDartsDLPredictor` and use `create_prediction_df()`

## Why not downgrade darts?

The bump from 0.35.0 to 0.43.0 was introduced in commit `95427f1` (2026-03-30) as part of a broader dependency sweep. The darts bump itself was **not security-motivated** — there are zero CVEs or security advisories for darts/u8darts. It was bundled opportunistically alongside security-relevant bumps (httpx, GitHub Actions).

Despite the lack of security pressure, downgrading is still not recommended:

- It would revert ~10 months of bug fixes and features across 8 major releases
- `torch==2.11.0` and `pytorch-lightning>=2.6.0` (pinned in the same `pyproject.toml`) may not be compatible with darts 0.35.0
- The forward fix is a single line change with no risk

## Notes

- No other files in the codebase use `quantile_timeseries`
