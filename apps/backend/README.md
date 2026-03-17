# Backend Module — DEPRECATED

> **This module is deprecated.** Its functionality has been split into dedicated modules. See the table below for migration guidance.

## Migration Guide

| Old (backend) | New Module | Description |
|---------------|------------|-------------|
| `src/data_processing.py` | `preprocessing_runoff/` | Runoff data preprocessing |
| `src/forecasting.py` | `linear_regression/` | Linear regression forecasting |
| `src/output_generation.py` | `postprocessing_forecasts/` | Output formatting and bulletins |
| `src/config.py` | `iEasyHydroForecast/` | Configuration and utilities |
| `forecast_script.py` | `linear_regression/linear_regression.py` | Main forecast script |

## What Remains

The `tests/` directory contains **special test utilities** (notebooks and scripts prefixed with `testspecial_`) that are useful for:

- Visualizing forecasts and skill metrics
- Debugging data pipelines
- Exploring remote database connections
- Comparing model outputs

These are **developer tools**, not part of the production pipeline.

### Available Special Tests

| File | Purpose |
|------|---------|
| `testspecial_plot_forecasts.ipynb` | Visualize forecast outputs |
| `testspecial_plot_skill_metrics.ipynb` | Plot skill metrics |
| `testspecial_plot_hydrographs.ipynb` | Plot hydrograph data |
| `testspecial_plot_daily_discharge_timeseries.ipynb` | Daily discharge visualization |
| `testspecial_visualize_pentadal_forecasts.ipynb` | Pentadal forecast visualization |
| `testspecial_visualize_decadal_forecasts.ipynb` | Decadal forecast visualization |
| `testspecial_visualize_probab_forecasts.ipynb` | Probabilistic forecast visualization |
| `testspecial_visualize_operational_report.ipynb` | Operational report visualization |
| `testspecial_model_comparison.ipynb` | Compare different models |
| `testspecial_compare_skill_metrics.py` | Compare skill metrics programmatically |
| `testspecial_remote_db_access.ipynb` | Test remote database connections |
| `testspecial_remote_db_access.py` | Remote DB access utilities |
| `testspecial_get_site_properties.ipynb` | Query site properties |
| `testspecial_gateway_api.ipynb` | Test data gateway API |
| `testspecial_inspect_gateway_forcing.ipynb` | Inspect gateway forcing data |
| `testspecial_raster_download.ipynb` | Download raster data |
| `testspecial_query_op_data_ch.py` | Query Swiss operational data |
| `testspecial_wetbulbtemp.ipynb` | Wet bulb temperature calculations |
| `testspecial_from_new_forecast_to_old_format.ipynb` | Format conversion utilities |
| `testspecial_remove_invalid_forecasts_ml.py` | Clean up invalid ML forecasts |
| `helper_analyse_run_times.ipynb` | Analyze pipeline run times |

### Test Data Files

The `tests/test_files/` directory contains sample data for the special tests.

## Do Not Use

- Do not add new functionality to this module
- Do not reference `backend/src/` in new code (it has been removed)
- Do not use the old Dockerfile (it has been removed)

For new development, use the dedicated modules listed in the migration guide above.
