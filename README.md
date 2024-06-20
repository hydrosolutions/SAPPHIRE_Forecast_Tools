![deploy forecast config action](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/deploy_forecast_configuration.yml/badge.svg) ![deploy forecast backend](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/deploy_backend.yml/badge.svg) ![deploy forecast dashboard](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/deploy_forecast_dashboard.yml/badge.svg)
![test deploy forecast config action](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/test_deploy_forecast_configuration.yml/badge.svg) ![test deploy forecast backend](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/test_deploy_backend.yml/badge.svg) ![test deploy forecast dashboard](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/test_deploy_forecast_dashboard.yml/badge.svg)

# SAPPHIRE_forecast_tools
Tools for operational hydrological forecasting for Central Asian hydromets. The tools are co-designed with the Kyrgyz Hydrometeorological Services as part of the ongoing [SAPPHIRE project](https://www.hydrosolutions.ch/projects/sapphire-central-asia) and funded by the [Swiss Agency for Development and Cooperation](https://www.eda.admin.ch/eda/en/home/fdfa/organisation-fdfa/directorates-divisions/sdc.html).

The tools are designed to be deployed on a local computer with access to operational hydrometerological data through the [iEasyHydro](https://ieasyhydro.org) database or through excel files. This repository holds data from the public domain for demonstration.

Note that this repository is **WORK IN PROGRESS**.

# Overview
4 tools are currently deployed via Docker and provided in this repository (see folder apps):
  - A dashboard to configure the forecast tools (currently only available in Russian language)
  - A tool to produce forecasts (in the present version linear regressions as currently employed by the Kyrgyz Hydrometeorological Services)
  - A tool for manual re-run of the latest forecast
  - A dashboard to visualize and download the forecasts

## Folder structure
All software components are in the apps directory. Files that need to be reviewed and potentially edited or replaced for local deployment are highlighted with a #. They are discussed in more detail in the file doc/deployment.md.
Potentially sensitive data that needs to be provided by a hydromet (for example daily discharge data used for the development of the forecast models) is stored in the data folder. Here we provide publicly available data examples from Switzerland for demonstration.
<details>
<summary>Click to expand the folder structure</summary>

```
  SAPPHIRE_FORECAST_TOOLS
   |__ apps
       The software components of the SAPPHIRE Forecast Tools.
        |__ backend
            The backend of the forecast tools. This is the component that produces the forecasts.
             |__ src
                 Functions used by the forecast backend.
             |__ tests
                 Tests for the forecast backend. To be extended as the backend is developed.
             |__ .dockerignore
                 Lists files and folders that are not copied to the docker image.
             |__ Dockerfile
                 Dockerfile to build the docker image for the forecast backend.
             |__ forecast_script.py
                 The python script that runs the forecast backend.
             |__ requirements.txt
                 List of python packages that need to be installed in the docker image.
             |__ run_offline_mode.py
                 The python script that runs the forecast backend in offline mode. Used for testing and development and to produce hindcasts.
             |__ setup.py
                 Setup file for the forecast backend. Makes sure the backend finds the iEasyHydroForecast library.
        |__ config
            Configuration of the forecast tools. The content of the files needs to be adapted to deployment conditions.
             |__ locale
                 Translations for the forecast dashboard. Currently only available in English and Russian language.
#            |__ .env
                 Holds file and folder paths as well as access information to the iEasyHydro Database. This file is read by all forecast tools when deployed using Docker.
             |__ .env_develop
                 Same as .env but for local development. This file is read by all forecast tools when run locally as local folder structer differs from deployed folder structure.
#            |__ config_all_stations_library.json
                 Information about all stations that are potentially available for the forecasting tools. This includes station codes, names, and coordinates.
#            |__ config_development_restrict_station_selection.json
                 A list of stations that are available for the development of the forecast models. This file restricts the stations selected by the forecast configuration dashboard to the stations that are actually available for development.
             |__ config_output.json
                 Defines what outputs are generated by the forecast tools. This file is written by the forecast configuration dashboard.
             |__ config_stations_selection.json
                 A list of stations selected for the production of forecasts. This file is written by the forecast configuration dashboard.
        |__ configuration_dashboard
            A user interface to configure for which stations forecasts are produced and what outputs are generated. The dashboard is written in R and uses the Shiny framework.
             |__ www
                 Static files (icon Station.jpg) used by the dashboard.
             |__ dockerfile
                 Dockerfile to build the docker image for the forecast configuration dashboard.
             |__ forecast_configuration.R
                 The R script that runs the forecast configuration dashboard.
        |__ forecast_dashboard
            A user interface to visualize and download the forecasts. The dashboard is written in python and uses the panel framework.
             |__ www
                 Static files (icon Pentad.jpg) used by the dashboard.
             |__ Dockerfile
                 Dockerfile to build the docker image for the forecast dashboard.
             |__ forecast_dashboard.py
                 The python script that runs the forecast dashboard.
        |__ iEasyHydroForecast
            A collection of python functions that are used by the linear regression tool.
        |__ internal_data
            Data that is written and used by the forecast tools.
             |__ forecasts_pentad.csv
                 The forecasts produced by the forecast backend. This file is written by the forecast backend and read by the forecast dashboard.
             |__ hydrograph_day.csv
                 Daily data used for visualization. This file is written by the forecast configuration dashboard and read by the forecast backend.
             |__ hydrograph_pentad.csv
                 Pentad data used for visualization. This file is written by the forecast configuration dashboard and read by the forecast backend.
             |__ latest_successful_run.txt
                 A text file that holds the date of the latest successful run of the forecast backend. This file is written and read by the forecast backend.
        |__ reset_forecast_run_date
            A tool to manually re-run the latest forecast. This is useful if new data becomes available that should be included in the latest forecast.
             |__ Dockerfile
                 Dockerfile to build the docker image for the reset forecast run date tool.
             |__ rerun_forecast.py
                 The python script that runs the reset forecast run date tool.
             |__ requirements.txt
                 List of python packages that need to be installed in the docker image.
   |__ bat
       Batch files for Windows that are used to open a browser window to the dashboards
        |__ backend
#           |__ backend.bat
                 Stops and re-starts the backend.
            |__ rerun_backend.bat
                Stops and re-starts the backend after resetting the latest run date.
            |__ Rerun-Pentadal-Forecast.ico
                Icon for the shortcut to the reset forecast run date bat file.
        |__ configuration_dashboard
#            |__ configuration.bat
                 Opens the forecast configuration dashboard in a Google Chrome browser. The content of the bat file may need to be adapted to deployment conditions.
             |__ Station.ico
                 Icon for the shortcut to the forecast configuration dashboard.
        |__ forecast_dashboard
#            |__ dashboard.bat
                 Opens the forecast dashboard in a Google Chrome browser. The content of the bat file may need to be adapted to deployment conditions.
             |__ Pentad.ico
                 Icon for the shortcut to the forecast dashboard.
#  |__ data
       Example data to demonstrate how the forecast tools work. The Needs to be replaced with data by the hydromet organization for deployment. The data and file formats are described in more detail in the file doc/user_guide.md.
        |__ daily_runoff
            Daily discharge data for the development of the forecast models. The data is stored in Excel files. The paths to these files are configured in the .env file.
        |__ GIS
            GIS data for the forecast configuration dashboard. The data is stored in shape files. The paths to these files are configured in the .env file.
        |__ reports
            Examples of forecast bulletins produced by the forecast tools. Will be generated automatically if it does not exist.
        |__ templates
            Templates for the forecast bulletins. The templates are stored in Excel files. The paths to these files are configured in the .env file.
#            |__ pentad_forecast_bulletin_template.xlsx
                 Template for the pentad forecast bulletin. Edit accoring to your reporting requirements.
   |__ doc
       Documentation of the forecast tools.
        |__ www
            Static files (images) used by the documentation.
        |__ configuration.md
            Configuration instructions
        |__ deployment.md
            Deployment instructions
        |__ development.md
            Instructions for local development
        |__ user_guide.md
            User guide with instructions of how to use the forecast tools once they are deployed.
```
</details>

# Configuration
The SAPPHIRE Forecast Tools interact with each other through a number of files. The configuration of these files and paths is described in detail in the file [doc/configuration.md](doc/configuration.md).

# Deployment
The SAPPHIRE Forecast Tools are deployed using the Docker system. The deployment of the forecast tools is described in detail in the file [doc/deployment.md](doc/deployment.md).

# Development
If you wish to run the forecast tools individually and locally for development purposes, you can do so by following the instructions in the file [doc/development.md](doc/development.md).

# User guide
Once the forecast tools are deployed with the appropriate **input data**, the user can configure the forecast tools and visualize and download the forecasts using the forecast configuration dashboard and the forecast dashboard. The user guide is available in English language in the file [doc/user_guide.md](doc/user_guide.md).

# Data requirements
The forecast tools rely on the availability of daily average discharge data. The data can be read from the iEasyHydro database or from local excel files. We include plublicly available daily discharge data from the Swiss Federal Office for the Environment (FOEN) in this repository. You find the original data [here](https://www.hydrodaten.admin.ch/en/seen-und-fluesse).

## Collaboration
Input from users is welcome. Please use the GitHub issue tracker to report bugs or suggest improvements. If you would like to contribute to the code, please fork the repository and submit a pull request. We will review the pull request and merge it if it fits the scope of the project. Please note that this is an open source project released under the MIT license, under which all contrubutions fall. Contributors are expected to adhere to the [Contributor Covenant code of conduct](https://www.contributor-covenant.org/).
