# BUG: `--today` flag in `run_forecast.py` runs zero models

**Module:** `long_term_forecasting`
**File:** `apps/long_term_forecasting/run_forecast.py`
**Severity:** Medium — blocks date-override functionality; workaround exists (`--all`)
**Status:** Implemented (2026-03-13)

## Summary

When `run_forecast.py` is invoked with `--today YYYY-MM-DD`, the date override
is applied correctly via `initialize_today()`, but zero models are executed.
The `--today` flag is in a mutually exclusive group with `--all` and `--models`,
so selecting it leaves both `args.all=False` and `args.models=None`. This
causes `forecast_all=False` and `models_to_run=[]`, which filters the model
list down to nothing.

## Steps to Reproduce

```bash
cd apps/long_term_forecasting
ieasyhydroforecast_env_file_path=path/to/.env \
  lt_forecast_mode=month_1 \
  uv run python run_forecast.py --today 2025-06-01
```

**Observed:** The script completes immediately with "Forecast run completed."
but no models are executed (no model log output, no forecasts written).

**Expected:** All configured models run using 2025-06-01 as the forecast date.

## Root Cause

In the `if __name__ == "__main__"` block (lines 373-427):

1. `--all`, `--models`, and `--today` are in a `mutually_exclusive_group`.
   Selecting `--today` means `--all` is not set.
2. Line 414: `recalibrate_all = args.all` evaluates to `False`.
3. Line 415: `models_to_run = args.models if args.models else []` evaluates
   to `[]` (since `args.models` is `None`).
4. `run_forecast()` is called with `forecast_all=False, models_to_run=[]`.
5. Inside `run_forecast()` (line 320-322): because `forecast_all` is `False`,
   the model list is filtered to only models in `models_to_run` — which is
   empty. The `for` loop on line 331 iterates over nothing.

## Expected Behavior

`--today DATE` should run all models (same behavior as `--all`) but with the
forecast date overridden to DATE. The intended use case is testing and
backtesting on arbitrary dates.

## Suggested Fix

Change line 414 from:

```python
recalibrate_all = args.all
```

to:

```python
recalibrate_all = args.all or args.today is not None
```

This makes `--today` imply `forecast_all=True`, so all models are discovered
via `forecast_config.get_models_to_run()` and executed with the overridden date.

## Workaround

Use `--all` instead of `--today`. This runs all models using the real system
date. The module's `check_valid_forecast_issue_date()` validates that the
current date is within the +/-5 day window of a valid forecast issue date,
so operational runs still work — but the date cannot be overridden for
backtesting via the CLI.

## Additional Context

`dev_code/simulate_forecasts.py` is not affected by this bug because it calls
`run_forecast()` programmatically with explicit `forecast_all=True` and a
populated `models_to_run` list, bypassing the CLI argument parsing entirely.
