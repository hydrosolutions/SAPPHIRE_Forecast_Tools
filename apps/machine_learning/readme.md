# Hydrological Forecast with Machine Learning Tools

This module generates hydrological forecasts using machine learning models.

> **Data I/O transition**: This module uses `sapphire_api_client` for reading
> and writing forecast data via the SAPPHIRE REST API, with CSV file fallback.
> CSV-only I/O will be removed once API integration is fully tested.

## Flagging System

| Flag | Meaning |
|------|---------|
| 0 | Successful forecast |
| 1 | NaN values in the forecast (e.g. missing operational data) |
| 2 | Forecast was not successful (code error) |
| 3 | NaN value after hindcasting (no data available at all) |
| 4 | Hindcast value produced successfully |

## Operational Pipeline

`recalculate_nan_forecasts.py` → `make_forecast.py` → `fill_ml_gaps.py`

Note: See point 4 to decide when to run `recalculate_nan_forecasts.py`.

### 1. make_forecast.py

Makes a forecast for the selected basin with the selected model for either pentadal or decadal mode. This file writes or updates the forecast file: `{MODE}_{MODEL_TO_USE}_forecast.csv` (e.g. `pentad_TFT_forecast.csv`). It uses old forecasts to fill recent gaps up to a specified threshold (`ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END`), and interpolates other missing values up to another threshold (`ieasyhydroforecast_THRESHOLD_MISSING_DAYS_TFT`). The forecast also gets flagged.

Output: `{MODE}_{MODEL_TO_USE}_forecast.csv`

### 2. hindcast_ML_models.py

Makes historical forecasts. Behaviour is controlled by these environment variables:
- `SAPPHIRE_MODEL_TO_USE` — which model
- `SAPPHIRE_HINDCAST_MODE` — `pentad` (5 days ahead) or `decad` (10 days ahead)
- `ieasyhydroforecast_START_DATE` — start of the hindcast
- `ieasyhydroforecast_END_DATE` — last day of the hindcast
- `ieasyhydroforecast_NEW_STATIONS` — controls which stations the hindcast is made for. If set to `'None'`, a hindcast is produced for all configured stations.

The output gets flagged automatically according to the flagging system (flag 3 or 4).

Output: `{MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_daily_{start_date_string}_{end_date_string}.csv`

### 3. fill_ml_gaps.py

Checks for missing forecast dates in the `{MODE}_{MODEL_TO_USE}_forecast.csv` file. A missing date indicates the system was not running (otherwise the forecasted value would be written and flagged accordingly). This script calls the hindcast script with the data gap as min and max date and fills the forecast file with the hindcasted values.

Output: Updated `{MODE}_{MODEL_TO_USE}_forecast.csv`

### 4. recalculate_nan_forecasts.py

Checks for NaN values in forecasts and recalculates them. NaN values from operational forecasts have flag 1, while NaN values from hindcasts have flag 3. This script recalculates entries with flag 1 or 2 by calling the hindcast script, which returns already-flagged results.

Note: If this script is called immediately after `make_forecast.py`, the missing operational data responsible for the NaN values will most likely not be available yet, which would lead to reflagging (1 → 3). Once the flag is 3, it will not be recalculated.

Output: Updated `{MODE}_{MODEL_TO_USE}_forecast.csv`

### 5. add_new_stations.py

If new stations are added to the config file, this script calculates the hindcast for newly added stations. Depending on how many stations are added, this can take some time. It needs to be manually run.

Output: Updated `{MODE}_{MODEL_TO_USE}_forecast.csv`

### 6. initialize_ml_tool.py

Initializes the `{MODE}_{MODEL_TO_USE}_forecast.csv` file by calculating a hindcast. This ensures we have hindcast data to properly evaluate the models. The script will ask the user for the time period. Note that the forcing data from `preprocessing_gateway` need to be available for this to work properly.

Output: `{MODE}_{MODEL_TO_USE}_forecast.csv`

## How to Run

### As part of the full pipeline

The recommended way to run this module is via the pipeline runner:

```bash
cd apps
bash run_locally.sh machine_learning            # operational forecasts
bash run_locally.sh maintenance:machine_learning # NaN recalc + gap-fill + new stations
```

This runs each script for all configured models (TFT, TIDE, TSMIXER) automatically.

### Running individual scripts

Scripts can also be run directly within the module's virtual environment:

```bash
cd apps/machine_learning
SAPPHIRE_MODEL_TO_USE=TFT uv run python make_forecast.py
SAPPHIRE_MODEL_TO_USE=TFT uv run python fill_ml_gaps.py
```

Key environment variables (set in the `.env` file or exported before running):
- `SAPPHIRE_MODEL_TO_USE` — model to use (`TFT`, `TIDE`, `TSMIXER`)
- `SAPPHIRE_PREDICTION_MODE` — `pentad` or `decad`
- `ieasyhydroforecast_env_file_path` — path to the `.env` configuration file

### Running tests

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

## Predictor Classes

For a model to work in the ML-forecasting system it must inherit from the `BasePredictor` class, which defines the interface each model's predictor class should provide:
- `get_input_chunk_length` — required input sequence length
- `get_max_forecast_horizon` — maximum forecast horizon
- `predict` — produce a forecast
- `hindcast` — produce a historical forecast

The `BaseDartsDLPredictor` class is a wrapper for global Darts forecasting models (TFT, TiDE, TSMixer, etc.).

## Model Folder Setup

Each model has its own model folder (the path is configured in the `.env` file).

### Setup Darts Deep Learning Models (TFT, TSMixer and TiDE)

Folder structure:
- **model/**
  - `scaler_stats_discharge.csv`
  - `scaler_stats_era5.csv`
  - `scaler_stats_static.csv`
  - `model.pt`
  - `model.pt.ckpt`
  - `model_config.json`
  - other additional information (description, train/val loss, etc.)

The scaler files save the statistics to normalize the input data.

#### model_config.json

Note: The comments below are for documentation only — JSON does not support comments. Remove them before use.

```json
{
  "num_samples": 200,
  "quantiles": [0.1, 0.5, 0.9],
  "scaling_type": "standard",
  "scaling_type_covariates": "minmax",
  "scaling_type_static": "standard",
  "exogene_covariates_cols": ["P", "T", "PET"],
  "past_covariates_cols": ["moving_avr_dis_3", "moving_avr_dis_5", "moving_avr_dis_10"],
  "future_covariates_cols": ["P", "T", "PET", "daylight_hours"],
  "window_sizes": [3, 5, 10],
  "trainer_config": {
    "accelerator": "cpu",
    "logger": false
  }
}
```

| Field | Description |
|-------|-------------|
| `num_samples` | Number of samples to draw |
| `quantiles` | Quantiles to save (should cover ranges the model was trained on) |
| `scaling_type` | Scaling for discharge: `"standard"` or `"minmax"` |
| `scaling_type_covariates` | Scaling for covariates |
| `scaling_type_static` | Scaling for static features |
| `exogene_covariates_cols` | Exogenous variables from ERA5-Land or Snowmapper |
| `past_covariates_cols` | Covariates known until the forecast date |
| `future_covariates_cols` | Future covariates (forecasted values) |
| `window_sizes` | Window sizes for past discharge moving averages |
| `trainer_config` | Trainer config for sampling — generally do not change |
