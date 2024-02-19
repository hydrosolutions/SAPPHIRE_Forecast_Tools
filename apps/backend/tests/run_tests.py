import os
import sys
import unittest

class CustomTestLoader(unittest.TestLoader):
    def loadTestsFromName(self, name, module=None):
        # Get the absolute path of the directory containing the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the path to the iEasyHydroForecast directory
        forecast_dir = os.path.abspath(os.path.join(script_dir, '..', '..', '..', 'iEasyHydroForecast'))

        # Add the forecast directory to the Python path
        sys.path.append(forecast_dir)

        return super().loadTestsFromName(name, module)

if __name__ == "__main__":
    # Get the absolute path of the directory containing the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(script_dir)
    # Construct the path to the iEasyHydroForecast directory
    forecast_dir = os.path.abspath(os.path.join(script_dir, '..', '..', 'iEasyHydroForecast'))
    print(forecast_dir)
    # Add the forecast directory to the Python path
    sys.path.append(forecast_dir)
    # Discover and run tests
    suite = unittest.defaultTestLoader.discover('tests', pattern='test_*.py')
    runner = unittest.TextTestRunner()
    runner.run(suite)

