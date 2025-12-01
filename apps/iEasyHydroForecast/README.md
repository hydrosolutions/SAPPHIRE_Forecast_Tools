# iEasyHydroForecast
A collection of methods in python that are used by several apps in the SAPPHIRE Forecast Tools.

## Installation

### Option 1: Python 3.11 + pip (current production)
To install the package with pip, run the following command:
```bash
pip install -e .
```

### Option 2: Python 3.12 + uv (migration in progress)
To install the package with uv, run the following commands:
```bash
# Install dependencies and the package in editable mode
uv sync --all-extras
```

This uses `pyproject.toml` and `uv.lock` for reproducible dependency management.

## Usage
To use the package, import the desired methods in your python script. For example:
```python
# Local libraries, installed with pip install -e ./iEasyHydroForecast
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

All tests must be run from the `apps/` directory (parent of `iEasyHydroForecast/`).

### Option 1: Python 3.11 + pip (unittest)
To run all tests with unittest:
```bash
cd apps
SAPPHIRE_TEST_ENV=True python -m unittest discover -s iEasyHydroForecast/tests -p 'test_*.py'
```

To test a specific method in a class:
```bash
cd apps
SAPPHIRE_TEST_ENV=True python -m unittest iEasyHydroForecast.tests.test_forecast_library.TestCalculateSkillMetricsPentad.test_skill_metrics_calculation
```

### Option 2: Python 3.12 + uv (pytest)
To run all tests with pytest:
```bash
cd apps
SAPPHIRE_TEST_ENV=True uv run --directory iEasyHydroForecast pytest iEasyHydroForecast/tests/
```

To test a specific file:
```bash
cd apps
SAPPHIRE_TEST_ENV=True uv run --directory iEasyHydroForecast pytest iEasyHydroForecast/tests/test_forecast_library.py
```

To test a specific test class or method:
```bash
cd apps
SAPPHIRE_TEST_ENV=True uv run --directory iEasyHydroForecast pytest iEasyHydroForecast/tests/test_forecast_library.py::TestCalculateSkillMetricsPentad::test_skill_metrics_calculation
```


