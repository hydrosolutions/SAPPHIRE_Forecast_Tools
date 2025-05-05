The forecast dashboard is a web application that provides a user interface for the operational hydrolgoical forecast service. The dashboard allows users to view and manage forecasts, as well as view forecast metrics and performance. The dashboard is built using the Panel web application framework for Python.

# How to run
Running the dashboard locally:
(Options for sapphire_forecast_horizon are pentad and decad)
`bash
ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port <port number>
`

## Updating translations
Manually add translation strings in the forecast_dashboard.po file. Then run the following command to compile the translations:
msgfmt -o /path/to/locale/en_CH/LC_MESSAGES/forecast_dashboard.mo /path/to/locale/en_CH/LC_MESSAGES/forecast_dashboard.po

# Testing
Additional packages need to be installed for testing:
## Installing Playwright Pytest
Install the Pytest plugin:
`bash
conda config --add channels conda-forge
conda config --add channels microsoft
conda install pytest-playwright
`
Install the required browsers:
`bash
pip install --force-reinstall playwright
python -m playwright install
`
## Running tests
In the forecast_dashboard directory, run: 
`bash
pytest
pytest --headed
pytest --headed -s  
`  

## Install pre-commit hooks
Pre-commit hooks are used to automatically format code and run tests before committing changes. To install pre-commit hooks, run the following command:
`bash
pip install pre-commit
`
Then, run the following command to install the pre-commit hooks:
`bash
pre-commit install
`

## To run the integration tests for the dashboard: 
Pre-commit hooks only work when the dashboard is running locally and make your commits from the terminal (not GitHub Desktop or VSCode). 
1. Make sure the dashboard is running locally.
2. Adapt test_integration.py to the local environment.
3. git stash changes
4. Commit changes
The integration tests will run automatically before the commit is made. If any of the tests fail, the commit will be aborted and you will need to fix the issues before committing again.

To commit changes without running the pre-commit hooks, use the following command:
`bash
git commit -m "your commit message" --no-verify
`

