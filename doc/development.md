# Development

This document describes how to develop the application.

## Prerequisites
The softwar has been developed on a Mac computer and packaged with Ubuntu base images using Docker using GitHub Actions (with workflow instructions in .github/workflows/main.yml).

### Install Git & clone repository
We recomment the installation of GitHub Desktop to manage the repository. The installation instructions can be found [here](https://desktop.github.com/). You can then clone this repository to your computer.

### Install Docker
Albeit not strictly necessary for the further development of the software, we suggest that you install Docker on your computer so you can test dockerization of sofware components locally. The Docker installation instructions can be found [here](https://docs.docker.com/install/).


### Instructions specific to the tools

#### Configuration dashboard
The forecast configuration dashboard is written in R and uses the Shiny framework. To run the dashboard locally, you need to install R. The installation instructions can be found [here](https://rstudio-education.github.io/hopr/starting.html). We recommend RStudio as an IDE for R development. The installation instructions can be found [here](https://posit.co/download/rstudio-desktop/).

You will further need to install the following R packages (you can do so by running the following commands in the R console):
```R
# Data handling libraries
install.packages("readxl")
install.packages("dplyr")
install.packages("jsonlite")
install.packages("sf")
install.packages("here")
## Shiny related libraries
install.packages("shiny")
install.packages("shinydashboard")
install.packages("shinyWidgets")
## Plotting and mapping libraries
install.packages("leaflet")
```

In your Finder or Explorer window, navigate to apps/configuration_dashboard/ and double-click on forecast_dashboard.R. In your RStudio IDE, click on the "Run App" button in the top right corner of the script editor. The dashboard should open in a browser window.

You can verify your confirmed edits in the dashboard in your local copies of the files apps/config/config_output.json and apps/config/config_station_selection.json.

### The backend
We recommend the use of Visual Studio Code for developping the backend. The installation instructions can be found [here](https://code.visualstudio.com/download). You will need to install the Python extension for Visual Studio Code. The installation instructions can be found [here](https://code.visualstudio.com/docs/languages/python).

We further use conda for managing the Python environment. The installation instructions can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/). Once conda is installed, you can create a new conda environment by running the following command in the terminal:
```bash
conda create --name my_environment python=3.10
```
Through the name tag you can specify a recognizable name for the environment (you can replace my_environment with a name of your choosing). For development, python 3.10 was used. We therefore recommend you continue development with python 3.10 as well. You can activate the environment by running the following command in the terminal:
```bash
conda activate my_environment
```
Our workflow may be inconsistent but here is what worked for us: We then installed the following packages in the terminal (note that this will take some time):
```bash
cd apps/linreg
pip install -r requirements.txt
```
The backend can read data from excel and/or from the iEasyHydro database (both from the online and from the local version of the software). If you wish to use the iEasyHydro database, you will need to install the iEasyHydro SKD library. More information on this library that can be used to access your organizations iEasyHydro database can be found [here](https://github.com/hydrosolutions/ieasyhydro-python-sdk). You will further need to manually install the library [iEasyReports](https://github.com/hydrosolutions/ieasyreports) that allows the backend of the forecast tools to write bulletins in a similar fashion as the software iEasyHydro. And finally you will need to load the iEasyHydroForecast library that comes with this package. You will therefore further need to install the following packages in the terminal:
```bash
pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
pip install git+https://github.com/hydrosolutions/ieasyreports.git@main
pip install -e ../iEasyHydroForecast
```
If you wish to use data from your organizations iEasyHydro database, you will need to configure the apps/config/.env_develop file (see [doc/configuration.md](doc/configuration.md) for more detailed instructions). We recommend testing your configuration by running a few example queries from the [documentation of the SDK library](https://github.com/hydrosolutions/ieasyhydro-python-sdk) in a jupyter notebook.

You can then run the forecast backend in the offline mode to simulate opearational forecasting in the past by running the following command in the terminal:
```bash
python run_offline_mode.py 2000 1 1 2000 1 31
```
This will run the linear regression tool for the period of January first 2000 to January 31st 2000. You can change the dates to your liking. The tool will write the results to the file apps/internal_data/forecasts_pentad.csv.

To run the linear regression tool in the online mode, type the following command in the terminal:
```bash
python run_online_mode.py
```

Both online and offline mode will produce the following outputs:
For review by the user:
- A forecast bulletin for each forecast horizon and station in the folder data/bulletins
- If the option is selected in the configuration dashboard, an excel sheet for each station in the folder data/pentadal_forecasts containing the linear regression forecasts as traditionally produced by the Kyrgyz Hydrometeorological Services

Intermediate results for internal use:
- Daily discharge data for each station in the folder apps/internal_data/hydrographs_day.pkl
- Pentadal discharge data for each station in the folder apps/internal_data/hydrographs_pentad.pkl
- A csv file with the forecasts for each station in the folder apps/internal_data/forecasts_pentad.csv

### Forecast dashboard


## Dockerization

### Configuration dashboard
Note that at the time of writing, a Docker base image with R and RShiny is not available for the ARM architecture (the latest Mac processors). The configuration dashboard has, with the current setup, been dockerized in Ubuntu.

The forecast dashboard is dockerized using the Dockerfile in the apps/configuration_dashboard folder. To build the docker image locally, run the following command in the root directory of the repository:
```bash
docker build --no-cache -t station_dashboard -f ./apps/configuration_dashboard/dockerfile .
```
Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.
```bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/config:/app/apps/config -v <full_path_to>/data:/app/data -p 3647:3647 --name station_dashboard
```






