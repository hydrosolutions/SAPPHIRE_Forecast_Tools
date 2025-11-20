# Long Term Forecasting

Core implementation in "lt-forecasting @ git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.0.0"
pip install git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.0.0

For fast development use the local code base - for this do:
pip uninstall -y lt-forecasting

pip install -e "/Users/sandrohunziker/hydrosolutions Dropbox/Sandro Hunziker/SAPPHIRE_Central_Asia_Technical_Work/code/machine_learning_hydrology/monthly_forecasting"

and if ready for online:
pip install git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.0.0

Change the version accordingly

Functions here act more like an interface.

## Data Interface

The `data_interface.py` module provides the `DataInterface` class for loading and managing forecast data. It retrieves forcing data (precipitation and temperature), discharge observations, static features, and optional snow data from the data gateway.

**Key methods:**
- `get_base_data(forcing_HRU, HRUs_snow, snow_variables)`: Returns temporal data (forcing + discharge + optional snow), static features, and date offsets for data freshness monitoring
- `load_snow_data(HRU, variable)`: Loads snow variables (SWE, ROF, etc.) for a specific HRU from preprocessed data gateway files

**Testing:** Run tests with:
```bash
ieasyhydroforecast_env_file_path="../../../forecast/config/.env_develop_xyxy" python -m tests.test_data_interface
```

## Forecast Configuration

The `config_forecast.py` module provides the `ForecastConfig` class for managing model configurations and execution dependencies.

**Expected file structure:**
Each forecast mode (e.g., "monthly") requires a configuration folder containing:
```
<forecast_mode>.json              # Main config: model paths, dependencies, forecast horizon
└── model_folder/
    └── <family>/
        └── <model_name>/
            ├── model_config.json      # Model-specific parameters
            ├── general_config.json    # General settings
            ├── feature_config.json    # Feature engineering specs
            └── path_paths.json        # Data path configurations
```

**Model ordering:** Models are automatically ordered using topological sorting based on their dependencies. This ensures that models are executed after all their dependencies are satisfied. Circular dependencies are detected and raise an error. Models without dependencies can run in parallel (same execution level).

**Example configuration (`config_monthly.json`):**
```json
{
    "model_folder" : "models_and_scalers/long_term_forecasting/monthly/",
    "models_to_use" : {
        "Base" : ["LR_Base", "GBT_Base"],
        "SnowMapper" : ["LR_SM", "SM_GBT_LR"]
    },
    "model_order" : ["Base", "SnowMapper"],
    "forecast_horizon" : 30,
    "offset" : null,
    "model_dependencies" : {
        "SM_GBT_LR" : ["LR_Base", "LR_SM"]
    },
    "data_dependencies" : {
        "SM_GBT_LR" : {"SnowMapper": -5}
    },
    "forcing_HRU" : "00003"
}
```
The data dependency checks the difference of the max date with the current date (here of the SnowMapper data (today - max_SM)). If the SnowMapper is up to date this value is -10 (10 days lead time), with values greater than -5 we say that we don't continue with forecasts because the information is not recent enough (As an example).

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