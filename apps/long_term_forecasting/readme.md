# Long Term Forecasting

Core implementation in "lt-forecasting @ git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.0.0"
pip install git+https://github.com/hydrosolutions/long-term-forecasting.git@v1.0.0

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
 