# iEasyHydroForecast
A collection of methods in python that are used by several apps in the SAPPHIRE Forecast Tools.

## Installation

Install dependencies and the package in editable mode using uv:
```bash
cd apps/iEasyHydroForecast
uv sync --all-extras
```

This uses `pyproject.toml` and `uv.lock` for reproducible dependency management.

## Usage
To use the package, import the desired methods in your python script. For example:
```python
# Local libraries, installed with uv sync (see Installation above)
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the modules from the iEasyHydroForecast package
import setup_library as sl
import forecast_library as fl
import tag_library as tl
```

## Testing

Tests are run from the `iEasyHydroForecast` directory using pytest.

Run all tests:
```bash
cd apps/iEasyHydroForecast
SAPPHIRE_TEST_ENV=True uv run pytest tests/ -v
```

Run tests for a specific file:
```bash
SAPPHIRE_TEST_ENV=True uv run pytest tests/test_forecast_library.py -v
```

Run a specific test class or method:
```bash
SAPPHIRE_TEST_ENV=True uv run pytest tests/test_forecast_library.py::TestCalculateSkillMetricsPentad::test_skill_metrics_calculation -v
```
