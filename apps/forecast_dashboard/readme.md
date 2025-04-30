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
