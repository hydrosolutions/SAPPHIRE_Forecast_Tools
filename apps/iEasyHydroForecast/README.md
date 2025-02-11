# iEasyHydroForecast
A collection of methods in python that are used by several apps in the SAPPHIRE Forecast Tools.

## Installation
To install the package, run the following command:
```bash
pip install -e .
```

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
To run the tests, run the following command from the apps directory:
```bash
SAPPHIRE_TEST_ENV=True python -m unittest discover -s iEasyHydroForecast/tests -p 'test_*.py'
```

To test a specific method in a class:
```bash
SAPPHIRE_TEST_ENV=True python -m unittest iEasyHydroForecast.tests.test_forecast_library.TestCalculateSkillMetricsPentad.test_skill_metrics_calculation
```


