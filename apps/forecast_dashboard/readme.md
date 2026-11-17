The forecast dashboard is a web application that provides a user interface for the operational hydrolgoical forecast service. The dashboard allows users to view and manage forecasts, as well as view forecast metrics and performance. The dashboard is built using the Panel web application framework for Python.

# How to run
Running the dashboard locally:
(Options for sapphire_forecast_horizon are pentad and decad)
`bash
ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port <port number> --static-dirs help=/absolute/path/to/data_folder/help
`

The `help` directory holds the two user guide HTML files (`forecast_dashboard_user_guide_ru.html` and `forecast_dashboard_user_guide_en.html`) and is served at `/help/` for the header's Help link.

## Updating translations
Manually add translation strings in the forecast_dashboard.po file. Then run the following command to compile the translations:
msgfmt -o /path/to/locale/en_CH/LC_MESSAGES/forecast_dashboard.mo /path/to/locale/en_CH/LC_MESSAGES/forecast_dashboard.po

## Period forecast ranges (PP-075 checkpoint)

The range selector has stable Native / Delta / Manual / Mixed choices, with Delta initially selected.
`src/processing.py::calculate_forecast_range` supplies table and period-chart bounds. Long-term readers
attach independent `range_delta` from current-publication-year Hydrograph statistics; the underlying
skill delta and accuracy frames remain unchanged. The monthly reservoir quarter card uses this same
range calculation. Separate daily model overlays retain their native bounds.

The reference period label comes from the data loaded for the station. Both producer and dashboard
must share the [reference settings](../../doc/configuration.md#long-term-range-reference-period).
See the [user guide](../../doc/user_guide.md#forecast-tab) for formulas and missing-data behavior.

**Checkpoint limitation:** the monthly reservoir bulletin’s supplementary quarterly section still
reads live native forecasts. Frozen edited quarterly persistence awaits the
[reviewed contract](../../doc/plans/issues/mid_prio_gi_draft_pp_bulletin_quarterly_snapshot.md); do not
claim complete E2 table/export parity or deploy on the assumption that this storage exists.
Deployment/statistics refresh is separate from local validation; see the
[handoff](../../doc/plans/pp075_range_deployment_handoff.md).

For the focused suite, use the repository runner from `apps/`:
`SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`.

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
0. If you need access to iEH HF via ssh, make sure the ssh tunnel is running.
1. Make sure the dashboard is running locally.
2. Adapt test_integration.py to the local environment.
3. git add changes
4. Commit changes  with ieasyhydroforecast_data_dir=/full/absolute/path/to/data/ git commit -m "your message"
   (note the / at the end of the path, path needs to be absolute)
The integration tests will run automatically before the commit is made. If any of the tests fail, the commit will be aborted and you will need to fix the issues before committing again.

To run the integration test locally before committing, run the following command, make sure you have the appropriate python environment running and the dashboard is running locally:
`bash
cd apps/forecast_dashboard
ieasyhydroforecast_data_dir=/full/absolute/path/to/data/ pytest --headed -s --slowmo 300
`

To commit changes without running the pre-commit hooks, use the following command:
`bash
git commit -m "your commit message" --no-verify
`

