# Post-processing of Hydrological Forecasts

This module post-processes hydrological forecasts from all forecast modules to generate data formats optimized for the forecast dashboard. It also calculates forecast skill metrics for quality assessment.

## Features

- Supports two prediction horizons: **Pentadal** (5-day) and **Decadal** (10-day)
- Processes forecasts from multiple models (Linear Regression, ML models)
- Calculates comprehensive skill metrics (NSE, correlation, accuracy)
- Generates pivot tables of recent forecasts by model
- Robust error handling with error accumulation pattern
- Performance monitoring with timing statistics

## Supported Models

- **Linear Regression** (statistical baseline)
- **TIDE** - Temporal Information Decomposition Encoder
- **TFT** - Temporal Fusion Transformer
- **TSMIXER** - Time Series Mixer
- **ARIMA** - AutoRegressive Integrated Moving Average
- **RRMAMBA** - Random Forest Mamba
- **CM** - Consensus Model
- **STATS** - Statistical Model

## Input

- Configuration via environment variables (see below)
- Forecast result files from forecast modules
- Observed discharge data (pentadal and/or decadal)

## Output

- Combined forecast data CSV (all models merged)
- Latest forecast CSV (most recent predictions)
- Skill metrics CSV (performance evaluation)
- Recent model forecasts pivot table

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `ieasyhydroforecast_env_file_path` | Path to environment configuration file |
| `ieasyforecast_intermediate_data_path` | Root directory for data storage |

### Prediction Mode

| Variable | Description | Default |
|----------|-------------|---------|
| `SAPPHIRE_PREDICTION_MODE` | Forecast horizon: `PENTAD`, `DECAD`, or `BOTH` | `BOTH` |

### Output Files

| Variable | Description |
|----------|-------------|
| `ieasyforecast_combined_forecast_pentad_file` | Pentadal forecast output |
| `ieasyforecast_combined_forecast_decad_file` | Decadal forecast output |
| `ieasyforecast_pentadal_skill_metrics_file` | Pentadal skill metrics |
| `ieasyforecast_decadal_skill_metrics_file` | Decadal skill metrics |

### Skill Metric Thresholds

| Variable | Description | Default |
|----------|-------------|---------|
| `ieasyhydroforecast_efficiency_threshold` | sdivsigma threshold | 0.6 |
| `ieasyhydroforecast_accuracy_threshold` | Accuracy threshold | 0.8 |
| `ieasyhydroforecast_nse_threshold` | Nash-Sutcliffe threshold | 0.8 |

## Usage

### Local Execution

```bash
# Process both pentadal and decadal forecasts
SAPPHIRE_PREDICTION_MODE=BOTH python postprocessing_forecasts.py

# Process only pentadal forecasts
SAPPHIRE_PREDICTION_MODE=PENTAD python postprocessing_forecasts.py

# Process only decadal forecasts
SAPPHIRE_PREDICTION_MODE=DECAD python postprocessing_forecasts.py
```

### Docker Execution

The Docker image uses Python 3.12 with uv package manager. The `:latest` tag is built from `Dockerfile` with Python 3.12.

## Dependencies

See `pyproject.toml` for the complete list. Key dependencies:
- `pandas>=2.2.2` - Data manipulation
- `numpy>=1.26.4` - Numerical computing
- `iEasyHydroForecast` - Core forecast library (local package)

## Testing

Run tests from the module directory:

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_postprocessing_tools.py -v -s
```

Test files:
- `test_postprocessing_tools.py` - Utility function tests
- `test_mock_postprocessing_forecasts.py` - Integration tests
- `test_error_accumulation.py` - Error handling tests

## Logging

Logs are written to the `logs/` directory with daily rotation and 30-day retention. Both file and console logging are enabled at INFO level.




