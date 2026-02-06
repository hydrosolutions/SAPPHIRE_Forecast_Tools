# Long Term Forecasting

## LT Package
Core implementation in "lt-forecasting @ git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.1.1"
pip install git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.1.1


### Fast Development
For fast development use the local code base - for this do:
pip uninstall -y lt-forecasting

pip install -e "/Users/sandrohunziker/hydrosolutions Dropbox/Sandro Hunziker/SAPPHIRE_Central_Asia_Technical_Work/code/machine_learning_hydrology/monthly_forecasting"

and if ready for online:
pip install git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.0.0

Change the version accordingly

Functions here act more like an interface.

For more detailed implementation specifics refer to the [Long-Term-Forecasting](https://github.com/hydrosolutions/long-term-forecasting) Documentation and code base.


## Run CLI Commands:

### Run Forecast
```bash
# Run Forecast For All Models For Month 1
ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path lt_forecast_mode=month_1 python run_forecast.py --all

# Run Forecast Only for Specific Model(s)
ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path lt_forecast_mode=month_1 python run_forecast.py --models LR_Base GBT LR_SM SM_GBT

# Run forecasts for each month, continue even if one fails
for month in  0 1 2 3 4 5 6 7 8 9; do
    echo "Running forecast for month_$month" >> ../../summary.log
    if ! ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path lt_forecast_mode=month_$month python run_forecast.py --all; then
        echo "WARNING: month_$month forecast failed, continuing with next month" >> ../../summary.log
    fi
done
```

### Calibrate and Hindcast

```bash
# Calibrate all models for a month lead time
ieasyhydroforecast_env_file_path="path_to_env" lt_forecast_mode=month_1 python calibrate_and_hindcast.py --all

# Calibrate only one model
ieasyhydroforecast_env_file_path="path_to_env" lt_forecast_mode=month_1 python calibrate_and_hindcast.py --models LR_Base

# Calibrate and Tune Hyperparameters
ieasyhydroforecast_env_file_path="path_to_env" lt_forecast_mode=month_1 python calibrate_and_hindcast.py --all --tune_hyperparameters
```

### Simulate Past Forecasts

```bash
ieasyhydroforecast_env_file_path="your.env" lt_forecast_mode=month_1 python dev_code/simulate_forecasts.py --years 2025 --models GBT 
```


## How to setup the Environment

1. Create local python environment
 ```bash  
python3.11 -m venv myenv
# activate your environment (here macos)
source myenv/bin/activate
```
2. Download Custom Github packages and Libraries
```bash  
# iEasy Hydro SDK library
pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
# long term forecasting library (use latest version)
pip install git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.1.1

# During Development - make changes directly in the codebase (if code base is locally available)
pip install -e "path/to/lt_forecasting/dir"

# SQL access
pip install psycopg2-binary sqlalchemy
```
If you work on macOS you might need to install lightgbm via homebrew. Or use conda to install the lightgbm package this should also handle the installation. On Windows and Linux system this should not be required.

## Data Interface

The `data_interface.py` module provides three data interface classes for loading and managing forecast data:

### DataInterfaceDB (Primary - Database)
Retrieves data from PostgreSQL database via SAPPHIRE services.

**Environment Variables:**
- `DB_POSTPROCESS_CONNECTION_STRING` - PostgreSQL connection string
- `ieasyhydroforecast_models_and_scalers_path` - Path to models/scalers
- `ieasyhydroforecast_ml_long_term_path_to_static` - Static features CSV path
- `ieasyhydroforecast_SNOW_VARS` - Available snow variables (SWE, ROF, HS)
- `ieasyhydroforecast_HRU_SNOW_DATA` - Available HRU codes for snow data

**Key Methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `get_base_data(forcing_HRU, start_date)` | Dict | Merged temporal (discharge + forcing) + static data with offsets |
| `extend_base_data_with_snow(base_data, HRUs_snow, snow_variables)` | Dict | Extends base data with snow variables |
| `get_meteo_data(meteo_type, code, start_date, end_date)` | DataFrame | Precipitation (P) or Temperature (T) |
| `get_runoff_data(code, start_date, end_date)` | DataFrame | Discharge observations |
| `get_snow_data(variable, code, start_date, end_date)` | DataFrame | Snow variables (SWE, ROF, HS) |

**Output from `get_base_data()`:**
```python
{
    "temporal_data": DataFrame,        # merged discharge + forcing
    "static_data": DataFrame,          # station characteristics
    "offset_date_base": int,           # days from today to latest data
    "offset_date_discharge": int       # days from today to latest discharge
}
```

### DataInterface (Legacy - CSV)
CSV-based interface retained for backward compatibility; being phased out.

### BasePredictorDataInterface
Retrieves forecast predictions from postprocessing database for models that depend on other models' outputs.

**Key Methods:**
- `get_base_predictor_data_database(model_name, horizon_type, horizon_value)` - Load predictions from `long_forecasts` table
- `load_all_dependencies_database(models, horizon_type, horizon_value)` - Merge predictions from multiple base models

**Testing:**
```bash
ieasyhydroforecast_env_file_path="../../../forecast/config/.env_develop_xyxy" python -m tests.test_data_interface
```

## Forecast Configuration

The `config_forecast.py` module provides the `ForecastConfig` class for managing model configurations and execution dependencies.

### File Structure

```
config_<mode>.json                    # Main config
└── models_and_scalers/long_term_forecasting/<mode>/
    └── <family>/
        └── <model_name>/
            ├── model_config.json     # Hyperparameters (GBT models)
            ├── general_config.json   # Training/inference settings
            ├── feature_config.json   # Feature engineering specs
            └── data_paths.json       # Snow data paths
```

### Main Config Parameters (`config_<mode>.json`)

| Parameter | Type | Description |
|-----------|------|-------------|
| `prediction_horizon` | int | Forecast window in days |
| `offset` | int | Days to offset forecast start; if null, equals horizon |
| `forecast_days` | list | Days to issue forecasts during calibration (e.g., [5, 10, 15, 20, 25, "end"]) |
| `operational_issue_day` | int | Day of month to issue operational forecasts |
| `operational_month_lead_time` | int | Months ahead for operational forecast |
| `allowable_missing_value_operational` | int | Max missing values before skipping forecast |
| `model_folder` | str | Path to model storage |
| `forcing_HRU` | str | HRU code for forcing data |
| `models_to_use` | dict | Models grouped by family (Base, SnowMapper, Uncertainty) |
| `model_dependencies` | dict | Model → list of dependency models |
| `data_dependencies` | dict | Model → data source with lag offset |
| `is_calibrated` | dict | Calibration status per model |
| `is_hyperparameter_tuned` | dict | Tuning status per model |

### Forecast Horizon & Offset Math

Forecast period = `[t + offset - horizon + 1, t + offset]` where `t` = issue date.

**Example:** `horizon=30, offset=35` predicts days `[t+6, t+35]` (issues on 10th, valid from 15th for 30 days).

### Model-Specific Configs

**`general_config.json`** - Training settings:
| Parameter | Description |
|-----------|-------------|
| `model_type` | Model class (e.g., "sciregressor") |
| `models` | Ensemble algorithms (xgb, lgbm, catboost) |
| `feature_cols` | Features (discharge, P, T, SWE, ROF) |
| `static_features` | Basin properties (LAT, LON, gl_fr, h_mean, aridity) |
| `use_lr_predictors` | Include LR outputs as features |
| `normalize` | Normalize features |
| `test_years` | Years for validation |

**`feature_config.json`** - Feature engineering:
```json
{
  "discharge": [{
    "operation": "mean",           // mean, sum, slope, max
    "windows": [15, 30],           // window sizes in days
    "lags": {"30": [30, 60, 90]}   // lag values per window
  }]
}
```

**`model_config.json`** - Hyperparameters (GBT models):
- XGBoost: `n_estimators`, `learning_rate`, `max_depth`, `subsample`, etc.
- LightGBM: `num_leaves`, `min_child_samples`, `lambda_l1`, `lambda_l2`, etc.
- CatBoost: `iterations`, `depth`, `l2_leaf_reg`, `bagging_temperature`, etc.

**`data_paths.json`** - Snow data paths:
| Parameter | Description |
|-----------|-------------|
| `model_home_path` | Base path (e.g., "SnowMapper") |
| `snow_HRUs` | HRU identifiers for snow data |
| `snow_variables` | Variables to load (SWE, RoF) |

### Dependencies

**Model dependencies:** Models execute in topological order. Dependencies are auto-resolved; circular dependencies raise errors.

**Data dependencies:** Specifies lag offset for external data freshness:
- Negative value: data expected recent (e.g., -5 = max 5 days old)
- Positive value: older data acceptable

**Key methods:**
- `load_forecast_config(forecast_mode)`: Loads configuration for a specific forecast mode
- `get_model_specific_config(model_name)`: Returns all config files for a model
- `get_model_execution_order()`: Returns models ordered by dependency resolution

## Forecast Output Flagging System

Forecast outputs include a flag column indicating the data source and execution status:

- **Flag 0**: Prediction stems from operational forecast 
- **Flag 1**: Prediction stems from hindcast / LOOCV calibration 
- **Flag 2**: Failure in forecast - not executed (due to insufficient data)

## Model Calibration and Hindcasting

### Overview

The `calibrate_and_hindcast.py` script provides a command-line interface for calibrating forecast models and generating hindcasts. It manages the entire calibration workflow, including dependency resolution, data preparation, model execution, and output generation.

### How It Works

The script orchestrates the following workflow:

1. **Environment Setup**: Loads environment variables from `.env` file
2. **Configuration Loading**: Reads forecast configuration for the specified mode (e.g., monthly)
3. **Dependency Resolution**: Determines model execution order based on inter-model dependencies
4. **Data Preparation**: Retrieves temporal data, static features, and optional snow data via `DataInterface`
5. **Model Calibration**: For each model in dependency order:
   - Checks if already calibrated (skips if status is True)
   - Verifies all model dependencies are satisfied
   - Loads model-specific configurations
   - Prepares input data including any dependent model outputs
   - Creates model instance and runs calibration + hindcast
   - Saves outputs and updates calibration status
6. **Status Tracking**: Maintains calibration status in configuration to prevent redundant re-runs

### Usage

The script supports two modes of operation via command-line arguments:

#### Calibrate All Models
```bash
# Set environment variables and run
ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly \
python calibrate_and_hindcast.py --all 
```

#### Calibrate Specific Models
```bash
# Calibrate only selected models
ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly \
python calibrate_and_hindcast.py --models LR_Base GBT_Base LR_SM
```

### Calibrate and Tune Hyperparameters
```bash
# Set environment variables and run
ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly \
python calibrate_and_hindcast.py --all --tune_hyperparameters
```

**Required Environment Variables:**
- `ieasyhydroforecast_env_file_path`: Path to the environment configuration file
- `lt_forecast_mode`: Forecast mode (e.g., "monthly", "seasonal")

**Command-line Options:**
- `--all`: Calibrate all models defined in the forecast configuration
- `--models MODEL1 MODEL2 ...`: Calibrate only specified models (space-separated list)

Note: The two options are mutually exclusive - use either `--all` or `--models`, not both.


### Outputs

The script generates the following outputs:

#### 1. Hindcast CSV Files
For each successfully calibrated model, a CSV file is saved to:
```
{output_path}/{model_name}_hindcast.csv
```
 
**File structure:**
- Columns contain hindcast predictions with associated metadata
- Includes a `flag` column set to `1` (indicating hindcast/calibration data)
- Contains historical predictions for validation and performance assessment

**Example path:**
```
models_and_scalers/long_term_forecasting/monthly/Base/LR_Base/LR_Base_hindcast.csv
```

#### 2. Updated Configuration File
The script updates the forecast configuration with calibration status:
```json
{
  "is_calibrated": {
    "LR_Base": true,
    "GBT_Base": true,
    "LR_SM": false
  }
}
```
This prevents redundant re-calibration on subsequent runs.

#### 3. Console Output
The script provides detailed logging and a summary of results:
```
==================================================
CALIBRATION SUMMARY
==================================================
LR_Base: ✓ SUCCESS
GBT_Base: ✓ SUCCESS
LR_SM: ✗ FAILED
==================================================
```

#### 4. Exit Codes
- `0`: All models calibrated successfully
- `1`: One or more models failed to calibrate

### Dependency Handling

The script intelligently manages model dependencies:

- **Execution Order**: Models are processed in topological order based on dependencies
- **Dependency Files**: Models that depend on other models automatically load their hindcast outputs as predictors
- **Validation**: Before running a model, all dependencies are verified to be successfully calibrated
- **Skip Logic**: Already-calibrated models are skipped unless `--all` flag triggers re-calibration

**Example dependency chain:**
```
LR_Base (no deps) → runs first
LR_SM (no deps) → runs first
SM_GBT_LR (deps: LR_Base, LR_SM) → runs after both dependencies complete
```

### Error Handling

The script includes robust error handling:
- **Missing Dependencies**: If a required dependency model is not calibrated, the dependent model is skipped with an error message
- **Calibration Failures**: Individual model failures are logged but don't stop the entire workflow
- **Data Issues**: Missing data or configuration errors are caught and reported
- **Full Error Traces**: Complete stack traces are logged for debugging


## Operational Forecasting

### Overview

The `run_forecast.py` script generates operational forecasts using calibrated models. It loads the most recent data, executes models in dependency order, and produces forecast outputs with data freshness validation.

### Usage

```bash
# Run forecasts for all models
ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly \
python run_forecast.py --all

# Run forecasts for specific models only
ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly \
python run_forecast.py --models LR_Base GBT_Base
```

**Required Environment Variables:**
- `ieasyhydroforecast_env_file_path`: Path to the environment configuration file
- `lt_forecast_mode`: Forecast mode (e.g., "monthly")

### Workflow

1. Loads latest temporal data (forcing, discharge, optional snow)
2. Checks data freshness against configured thresholds
3. Executes models in dependency order
4. Validates data availability before running each model
5. Saves forecast outputs and appends to hindcast files

### Outputs

**Forecast CSV Files:**
```
{output_path}/{model_name}_forecast.csv
```
- Contains predictions with `flag` column set to `0` (operational forecast) or `2` (failed due to insufficient data)
- Forecasts are also appended to the corresponding hindcast files for continuous validation

**Console Summary:**
```
==================================================
FORECAST SUMMARY
==================================================
LR_Base: SUCCESS
GBT_Base: SUCCESS
LR_SM: FAILED
==================================================
```

## Simulate Past Forecasts

The `dev_code/simulate_forecasts.py` script runs forecasts retroactively for past dates to enable simulation and validation.

### Purpose

Test forecasting models against historical periods by simulating what forecasts would have been on specific dates.

### Command-Line Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `--years` | int (multiple) | Yes | Years to simulate (e.g., `--years 2024 2025`) |
| `--all` | flag | No | Run for all available models |
| `--models` | str (multiple) | No | Specific models to run |
| `--num_months` | int | No | Months per year to simulate (default: 12) |

### Workflow

1. Loads forecast config and determines operational issue day
2. For each year/month combination:
   - Sets "today" to the specified date (at operational issue day)
   - Calls `run_forecast()` to generate forecasts
3. Outputs forecast files via `run_forecast()`

## Post-Processing Forecasts

The `post_process_lt_forecast.py` module adjusts raw forecasts from forecast periods to calendar months using ratio-based scaling with long-term climatology.

### Purpose

Raw forecasts are generated over non-aligned forecast periods (e.g., May 31 - June 29), but agencies require calendar month values (June 1-30). This module performs the adjustment.

### Processing Steps

1. **Calculate Forecast Period Climatology**: Long-term mean/std for the forecast period day-range
2. **Calculate Calendar Month Climatology**: Long-term mean/std for the target calendar month
3. **Map Forecast to Calendar Month**: Determine target month from issue date + lead time
4. **Adjust Forecast**: Apply ratio-based scaling with confidence interval bounds

**Adjustment Formula:**
```
log_ratio = log(Q_forecast / fc_period_lt_mean)
log_ratio_clipped = clip(log_ratio, bounds)
Q_adjusted = calendar_month_lt_mean * exp(log_ratio_clipped)
```

### Key Features

- **Leave-One-Out**: Excludes prediction year from climatology (prevents data leakage)
- **Clipping**: ±2σ for N<5 years, Student's t 95% CI for N≥5
- **Multi-Quantile**: Adjusts all Q columns (Q5, Q10, ..., Q95) independently
- **Non-Negative**: Ensures all adjusted values ≥ 0

### Usage

Called automatically by `run_forecast.py`:
```python
from post_process_lt_forecast import post_process_lt_forecast

forecast = post_process_lt_forecast(
    forecast_config=forecast_configs,
    observed_discharge_data=temporal_data,
    raw_forecast=forecast,
)
```