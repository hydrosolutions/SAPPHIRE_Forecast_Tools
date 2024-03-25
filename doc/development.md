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
#### Prerequisites
You will need a Python IDE for development. If you do not alreay have one installed, we recommend the use of Visual Studio Code for developping the backend. The installation instructions can be found [here](https://code.visualstudio.com/download). You will need to install the Python extension for Visual Studio Code. The installation instructions can be found [here](https://code.visualstudio.com/docs/languages/python).

We use conda for managing the Python environment. The installation instructions can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/). Once conda is installed, you can create a new conda environment by running the following command in the terminal:
```bash
conda create --name my_environment python=3.10
```
Through the name tag you can specify a recognizable name for the environment (you can replace my_environment with a name of your choosing). For development, python 3.10 was used. We therefore recommend you continue development with python 3.10 as well. You can activate the environment by running the following command in the terminal:
```bash
conda activate my_environment
```
The name of your environment will now appear in brackets in the terminal.

We then install the following packages in the terminal (note that this will take some time):
```bash
cd apps/backend
pip install -r requirements.txt
```
The backend can read data from excel and/or from the iEasyHydro database (both from the online and from the local version of the software). If you wish to use the iEasyHydro database, you will need to install the iEasyHydro SKD library. More information on this library that can be used to access your organizations iEasyHydro database can be found [here](https://github.com/hydrosolutions/ieasyhydro-python-sdk). You will further need to manually install the library [iEasyReports](https://github.com/hydrosolutions/ieasyreports) that allows the backend of the forecast tools to write bulletins in a similar fashion as the software iEasyHydro. And finally you will need to load the iEasyHydroForecast library that comes with this package. You will therefore further need to install the following packages in the terminal:
```bash
pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
pip install git+https://github.com/hydrosolutions/ieasyreports.git@main
pip install -e ../iEasyHydroForecast
```
If you wish to use data from your organizations iEasyHydro database, you will need to configure the apps/config/.env_develop file (see [doc/configuration.md](configuration.md) for more detailed instructions). We recommend testing your configuration by running a few example queries from the [documentation of the SDK library](https://github.com/hydrosolutions/ieasyhydro-python-sdk) in a jupyter notebook.

#### How to run the backend locally
You can then run the forecast backend in the offline mode to simulate opearational forecasting in the past by running the following command in the terminal:
```bash
python run_offline_mode.py 2010 1 1 2010 1 31
```
This will run the linear regression tool for the period of January first 2010 to January 31st 2010. You can change the dates to your liking but make sure that you have data available for the production of the linear regression models and for forecasting. The tool will write the results to the file *ieasyforecast_results_file*, apps/internal_data/forecasts_pentad.csv.
If you have daily data available from 2000 to the present time, we recommend starting the forecast from 2010 onwards. This gives the backend tool 10 years of data (from 2000 to 2010) to build a linear regression model. Each year, the linear regression model will be updated with the data from the previous year. That means, the parameters of the linear regression model $y=a \cdot x+b$ will change each year. If you are interested in the model parameters, they are provided in the *ieasyforecast_results_file*, apps/internal_data/forecasts_pentad.csv.

### Forecast dashboard
#### Prerequisites
The forecast dashboard is implemented in python using the panel framework. As for the backend development, we recommend the use of a Python IDE and conda for managing the Python environment. Please refer to the instructions above should you require more information on how to install these tools.

If you have already set up a python environment for the backend, you can activate it by running the following command in the terminal and skipp the installation of python_requirements.txt:
```bash
conda activate my_environment
```

#### How to run the forecast dashboard locally
To run the forecast dashboard locally, navigate to the apps/forecast_dashboard folder and run the following command in the terminal:
```bash
panel serve pentad_dashboard.py --show --autoreload --port 5009
```
The options --show, --autoreload, and --port 5009 are optional. The show and autoreload options open your devault browser window (we used chrome) at http://localhost:5009/pentad_dashboard and automatically reload the dashboard if you save changes in the file pentad_dashboard.py. The port option tells you on which port the dashboard is being displayed. Should port 5009 be already occupied on your computer, you can change the number. You can then select the station and view predictors and forecasts in the respective tabs.

## Dockerization

### Configuration dashboard
Note that at the time of writing, a Docker base image with R and RShiny is not available for the ARM architecture (the latest Mac processors). The configuration dashboard has, with the current setup, been dockerized in Ubuntu.

The forecast dashboard is dockerized using the Dockerfile in the apps/configuration_dashboard folder. To build the docker image locally, run the following command in the root directory of the repository:
```bash
docker build --no-cache -t station_dashboard -f ./apps/configuration_dashboard/dockerfile .
```
Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.
```bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/config:/app/apps/config -v <full_path_to>/data:/app/data -p 3647:3647 --name station_dashboard_container station_dashboard
```

### Backend
The backend is dockerized using the Dockerfile in the apps/backend folder. Dockerization has been tested under both Ubuntu running on Windows or Mac OS operating systems. To build the docker image locally, run the following command in the root directory of the repository:
```bash
docker build --no-cache -t forecast_backend -f ./apps/backend/Dockerfile .
```
Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.
```bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/data:/app/data -v <full_path_to>/apps/internal_data:/app/apps/internal_data -p 9000:8801 --name forecast_backend_container forecast_backend
```

### Forecast dashboard
The forecast dashboard is dockerized using the Dockerfile in the apps/forecast_dashboard folder. To build the docker image locally, run the following command in the root directory of the repository:
```bash
docker build --no-cache -t forecast_dashboard -f ./apps/forecast_dashboard/Dockerfile .
```
Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.
```bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/data:/app/data -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/apps/internal_data/:/app/apps/internal_data -p 5006:5006 --name fcboard forecast_dashboard
```
Make sure that the port 5006 is not occupied on your computer. You can change the port number in the command above if necessary but you'll have to edit the port exposed in the docker file and edit the panel serve command in the dockerfile to make sure panel renders the dashboards to your desired port.

You can now access the dashboard in your browser at http://localhost:5006/pentad_dashboard and review it's functionality.

## Development workflow
Development takes place in a git branch created from the main branch. Once the development is finished, the branch is merged into the main branch. This merging requires the approval of a pull requrest by a main developer. The main branch is tested in deployment mode and then merged to the deploy branch. 3rd party users of the forecast tools are requested to pull the tested deploy branch. The deployment is done automatically using GitHub Actions. The workflow instructions can be found in .github/workflows/deploy_*.yml.

## Testing
Testing tools are being developed for each tool. This is work in progress.

To run all tests, navigate to the apps directory in your terminal and type the following command:
```bash
SAPPHIRE_TEST_ENV=True python -m pytest -s
```
SAPPHIRE_TEST_ENV=True defines an environment variable TEST_ENV to true. We use this environment variable to set up temporary test environments. The -s is optional, it will print output from your functions to the terminal.

To run tests in a specific file, navigate to the apps directory in your terminal and type the following command:
```bash
SAPPHIRE_TEST_ENV=True python -m pytest -s tests/test_file.py
```
Replace test_file.py with the name of the file you want to test. To run tests in a specific function, navigate to the apps directory in your terminal and type the following command:
```bash
SAPPHIRE_TEST_ENV=True python -m pytest -s tests/test_file.py::test_function
```
Replace test_function with the name of the function you want to test.

