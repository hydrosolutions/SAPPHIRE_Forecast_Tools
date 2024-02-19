import unittest
import subprocess
import datetime as dt
from dotenv import load_dotenv
import sys

print(sys.path)

from iEasyHydroForecast import tag_library as tl

'''class TestGitHubRepo(unittest.TestCase):
    def test_writing_intermediate_forecast_data(self):
        test_period_start = dt.date(2022, 4, 1)
        test_period_end = dt.date(2022, 4, 1)
        env_file = "test_files/.env_develop_test"

        # Simulate we're in the main backend directory
        cwd = os.getcwd()
        os.chdir("../")

        # Use a .env_test file for testing
        res = load_dotenv(dotenv_path=env_file)

        # Iterate over the dates
        current_day = test_period_start
        while current_day <= test_period_end:
            # Call main.py with the current date as a command-line argument
            subprocess.run(["python", "forecast_script.py",
                            "date", str(current_day),
                            "--calling_script", __file__,
                            "--env", env_file])
            # Increment the current day by one day
            current_day += dt.timedelta(days=1)

        # Change the working directory back to the original
        os.chdir(cwd)'''

class TestTestingForBackend(unittest.TestCase):
    def test_method(self):
        self.assertTrue(tl.is_gregorian_date('2022-05-15'))

if __name__ == '__main__':
    unittest.main()