# High priority: postprocessing maintenance `model_short` KeyError on empty DECAD read

## Problem

`bash apps/run_locally.sh daily` can fail in `postprocessing_forecasts`
maintenance for the DECAD horizon with:

```text
KeyError: 'model_short'
```

The crash happens in `_fill_gaps_for_horizon(DECAD, ...)` after
`read_individual_model_forecasts_for_dates()` returns an empty, columnless
DataFrame. The maintenance flow calls the DECAD neural ensemble function before
checking `modelled.empty`, so `calculate_neural_ensemble_forecast_decade()`
indexes `forecasts["model_short"]` on a frame whose columns are a default
`RangeIndex`.

Confirmed local contributing condition: DECAD combined forecasts were read from
CSV fallback with historical rows through `2025-10-20`, but the scoped
individual-model reader returned no rows for the affected historical dates and
codes. The reader returned a bare `(0, 0)` DataFrame instead of a schemaful empty
frame.

## Goals

- Prevent maintenance from crashing when no modelled rows exist for affected
  gap-fill dates.
- Make the short-term individual forecast reader contract predictable for empty
  results.
- Harden neural ensemble helpers against malformed or empty input.
- Keep maintenance gap-fill bounded to the configured lookback window.

## Non-goals

- Do not redesign short-term postprocessing or gap detection.
- Do not migrate historical CSV archives in this change.
- Do not change API write semantics beyond avoiding the crash and stale-date
  over-selection.

## Proposed changes

### 1. Add a pre-ensemble empty guard in maintenance

In `apps/postprocessing_forecasts/postprocessing_maintenance.py`, check
`modelled.empty` immediately after
`data_reader.read_individual_model_forecasts_for_dates(...)` and before:

- `sl.calculate_virtual_stations_data(modelled)`
- `config.neural_ensemble_func(modelled)`

The warning should include enough context to diagnose data availability:

- horizon label
- number of affected dates
- number of gap codes

This is the direct production crash fix.

### 2. Return schemaful empty frames from short-term individual readers

In `apps/postprocessing_forecasts/src/data_reader.py`, define expected short-term
forecast columns for each horizon:

- common: `code`, `date`, `forecasted_discharge`, `model_short`
- pentad: `pentad_in_month`, `pentad_in_year`
- decad: `decad_in_month`, `decad_in_year`
- optional but common when present: `q05`, `q25`, `q75`, `q95`, `flag`

Use these columns for empty returns from:

- `_normalize_lr_forecasts()`
- `_normalize_ml_forecasts()`
- `read_individual_model_forecasts()`
- `read_individual_model_forecasts_for_dates()`

The key contract: an empty successful read for a valid horizon should still
contain `model_short`, `date`, and `code`.

### 3. Harden neural ensemble functions

In `apps/iEasyHydroForecast/setup_library.py`, update both:

- `calculate_neural_ensemble_forecast()`
- `calculate_neural_ensemble_forecast_decade()`

They should return the input unchanged if:

- `forecasts.empty` is true
- `model_short` is not in `forecasts.columns`

Log a warning when the schema is missing. This is defensive hardening; it should
not be the only fix.

### 4. Add the same guard to operational short-term postprocessing

`apps/postprocessing_forecasts/postprocessing_operational.py` has the same
ordering: it calls virtual-station and neural-ensemble logic before any empty
guard. Add an equivalent pre-ensemble empty guard after
`read_observed_and_modelled_data(...)`.

If modelled data is empty, skip ensemble creation and save the empty/no-op output
according to the existing operational behavior. Update tests to reflect the new
expected no-op behavior.

### 5. Apply lookback scoping to stale EM detection

In `postprocessing_maintenance.py`, `stale_em` currently scans all combined rows
where `model_short == "EM"`, `forecasted_discharge` is present, and `q05` is
null. This bypasses the configured lookback window and can pull thousands of old
dates into `affected_dates`.

Filter `stale_em` consistently with the same cutoff used by
`detect_stale_quantiles()`.

## Tests

Add or update focused tests:

1. Maintenance: `read_individual_model_forecasts_for_dates()` returns a bare
   empty DataFrame and `_fill_gaps_for_horizon()` exits cleanly before calling
   neural ensemble logic.
2. Data reader: empty PENTAD and DECAD individual forecast reads return
   schemaful empty frames with `model_short`.
3. Neural ensemble helpers: empty or missing-`model_short` input returns
   unchanged without raising.
4. Operational workflow: empty modelled data no longer calls neural ensemble
   creation.
5. Maintenance stale EM: stale EM rows outside the lookback window are ignored.

## Verification

Run targeted tests first:

```bash
cd apps/postprocessing_forecasts
.venv/bin/python -m pytest \
  tests/test_maintenance_workflow.py \
  tests/test_data_reader.py \
  tests/test_operational_workflow.py
```

Then run lint:

```bash
uvx ruff check apps/postprocessing_forecasts apps/iEasyHydroForecast
```

Optional local confirmation after implementation:

```bash
ieasyhydroforecast_env_file_path=/Users/bea/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm \
SAPPHIRE_PREDICTION_MODE=DECAD \
apps/postprocessing_forecasts/.venv/bin/python \
apps/postprocessing_forecasts/postprocessing_maintenance.py
```

## Risks and review notes

- Step 1 is the minimum safe production fix.
- Step 2 changes the reader contract for empty results. This should make callers
  safer, but tests should verify no caller relies on a columnless empty frame.
- Step 5 changes maintenance behavior by preventing old stale EM rows outside
  the lookback window from triggering refresh attempts. This is likely correct,
  but it should be called out explicitly during review.

