# Development

This document describes how to develop the application.

- [Development](#development)
  - [Prerequisites](#prerequisites)
    - [Installation instructions for technologies used in the development of the forecast tools](#installation-instructions-for-technologies-used-in-the-development-of-the-forecast-tools)
      - [Python](#python)
      - [Conda](#conda)
      - [Visual Studio Code](#visual-studio-code)
      - [R](#r)
      - [RStudio](#rstudio)
      - [Git Desktop](#git-desktop)
      - [Docker](#docker)
    - [Seting up the working environment](#seting-up-the-working-environment)
      - [Clone the github repository](#clone-the-github-repository)
      - [Test-run the Sapphire forecast tools](#test-run-the-sapphire-forecast-tools)
      - [Activate the conda environment](#activate-the-conda-environment)
      - [Install the required packages](#install-the-required-packages)
    - [Instructions specific to the tools](#instructions-specific-to-the-tools)
      - [Pipeline](#pipeline)
      - [Configuration dashboard](#configuration-dashboard)
    - [The backend](#the-backend)
      - [Prerequisites](#prerequisites-1)
      - [How to run the backend modules locally](#how-to-run-the-backend-modules-locally)
        - [Pre-processing of river runoff data](#pre-processing-of-river-runoff-data)
        - [Pre-processing of forcing data from the data gateway](#pre-processing-of-forcing-data-from-the-data-gateway)
        - [Running the linear regression tool](#running-the-linear-regression-tool)
    - [Forecast dashboard](#forecast-dashboard)
      - [Prerequisites](#prerequisites-2)
      - [How to run the forecast dashboard locally](#how-to-run-the-forecast-dashboard-locally)
  - [Dockerization](#dockerization)
    - [Configuration dashboard](#configuration-dashboard-1)
    - [Backend](#backend)
    - [Forecast dashboard](#forecast-dashboard-1)
  - [How to use private data](#how-to-use-private-data)
  - [Development workflow](#development-workflow)
  - [Testing](#testing)
- [Deployment](#deployment)




## Prerequisites
Note: The software has been developed on a Mac computer and packaged with Ubuntu base images using Docker using GitHub Actions (with workflow instructions in .github/workflows/main.yml). It has been tested extensibly on an Ubuntu server. The software has not been tested on Windows.

The following open-source technologies are used in the development of the forecast tools:
- For scripting and development:
    - Scripting language [Python](#python) and user interface [Visual Studio Code (#visual-studio-code)
    - Python package manager [Conda](#conda)
    - Scripting language [R](#R) and user interface RStudio
    - Code version control: GitHub Desktop
- Containerization: Docker

If you have all of these technologies installed on your computer, you can skip the installation instructions below and proceed to the instructions on the general development workflow.


### Installation instructions for technologies used in the development of the forecast tools

You will find instructions on how to install the technologies used in the development of the forecast tools below. We recommend that you install the technologies in the order they are listed. If you are new to any of these tools it is recommended to run through a quick tutorial to get familiar with the technology befor starting out to work on the SAPPHIRE forecast tools.


#### Python
Install Python on your computer. The installation instructions can be found in many places on the internet, for example [here](https://realpython.com/installing-python/). We recommend the installation of Python 3.11. You can check the version of Python installed on your computer by running the following command in the terminal:
```bash
python --version
```

#### Conda
We use conda for managing the Python environment. The installation instructions can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/). Once conda is installed, you can create a new conda environment by running the following command in the terminal:
```bash
conda create --name my_environment python=3.11
```
Through the name tag you can specify a recognizable name for the environment (you can replace my_environment with a name of your choosing). We use a different environment for each module of the backend. For development, python 3.11 was used. We therefore recommend you continue development with python 3.11 as well.

#### Visual Studio Code
Install Visual Studio Code on your computer. The installation instructions can be found [here](https://code.visualstudio.com/download). We recommend the installation of the Python extension for Visual Studio Code. The installation instructions can be found [here](https://code.visualstudio.com/docs/languages/python).
Note that you can use any other Python IDE for development. We recommend Visual Studio Code as it is free and open-source.

#### R
Install R on your computer. The installation instructions can be found [here](https://cran.r-project.org/). We recommend the installation of R 4.4.1. You can check the version of R installed on your computer by running the following command in the terminal:
```bash
R --version
```

#### RStudio
Install RStudio on your computer. The installation instructions can be found [here](https://rstudio.com/products/rstudio/download/). We recommend the installation of RStudio Desktop.

#### Git Desktop
We recomment the installation of GitHub Desktop to manage the repository. The installation instructions can be found [here](https://desktop.github.com/).

#### Docker
Install Docker Desktop on your computer so you can test dockerization of sofware components locally. The Docker installation instructions can be found [here](https://docs.docker.com/install/).

### Seting up the working environment

#### Clone the github repository
Once you have installed the technologies, you can set up your working environment. We recommend that you create a folder for the repository on your computer. You can then clone the repository to your computer. Open a terminal and navigate to the folder where you want to clone the repository. Run the following command in the terminal:
```bash
git clone https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools.git
```

#### Test-run the Sapphire forecast tools
Navigate to the root directory of the repository in the terminal:
```bash
cd SAPPHIRE_Forecast_Tools
```
You can test-run the forecast tools by running the following command in the terminal:
```bash
source bin/run_sapphire_forecast_tools.sh <path_to_data_root_folder>
```
For the test run, you can use the full path to your SAPPHIRE_Forecast_Tools folder as the path to the data root folder. You can get the absolute path to the folder by running the following command in the terminal:
```bash
pwd
```
An example command for the test run would be:
```bash
bash bin/run_sapphire_forecast_tools.sh /Users/username/forecasting/
```

#### Activate the conda environment
You can list the available conda environments with:
```bash
conda env list
```
You can activate an environment by running the following command in the terminal (replace my_environment with the name of the environment you want to work in):
```bash
conda activate my_environment
```

#### Install the required packages
Each module has a requirements.txt file that lists the required packages. You can install the required packages in your conda environment by running the following command in the terminal:
```bash
cd apps/module_name
pip install -r requirements.txt
```

You now have a working installation of the SAPPHIRE Forecast Tools with a public demo data set.

### Instructions specific to the tools

#### Pipeline
We use the python package Luigi to manage the workflow of the forecast tools. Luigi takes care of running each backend module in sequence or in parallel. You will find detailed information about Luigi [in the Luigi docs](https://luigi.readthedocs.io/en/stable/index.html#).

All modules of the SAPPHIRE Forecast Tools are run in individual docker containers. To run them on your system, please follow the instructions below.

<details>
<summary>Mac OS</summary>
To build and run the pipeline locally on a Mac to the following steps before you run the docker compose up command above:

1. Open a terminal and navigate to the root directory of the repository.

2. Clean up the docker work space (note that this will remove all containers and images in your Docker workspace):

```bash
bash bin/clean_docker.sh
```

3. Build the docker images:

```bash
ieasyhydroforecast_data_root_dir=<ieasyhydroforecast_data_root_dir> bash bin/build_docker_images.sh latest
```

</details>

<details>
<summary>Ubuntu</summary>
To test-run the pipeline on an Ubuntu server, follow the instructions in [doc/deployment.md](deployment.md).
</details>


The pipeline is run inside a Docker container. The Dockerfile is located in the apps/pipeline folder. To build the Docker image for the pipeline locally, run the following command in the root directory of the repository:
```bash
docker compose -f ./bin/docker-compose.yml build
```
To run the Docker containers locally, run the following command in the root directory of the repository:
```bash
docker compose -f ./bin/docker-compose.yml up
```





#### Configuration dashboard
The forecast configuration dashboard is written in R and uses the Shiny framework.

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
The backend consists of a set of tools that are used to produce forecasts. They are structured into:

- Pre-processing:
    - pre-processing of river runoff data: This component reads daily river runoff data from excel files and, if access is available, from the iEasyHydro database. The script is intended to run at 11 o'clock every day. It therefore includes daily average discharge data from all dates prior to today and todays morning measurement of river runoff. The data is stored in a csv file.
    - pre-processing of forcing data: Under development.
- forecast models:
    - linear regression: This component reads the pre-processed river runoff data and builds a linear regression model for each station. The model is updated each year with the data from the previous year. The model is used to forecast the river runoff for the next 5 or 10 days. The forecast is stored in a csv file.
    - LSTM: Under development.
    - Conceptual hydrological models: Under development.
- Post-processing:
    - post-processing of forecasts: Under development.

- iEasyHydroForecast: A helper library that contains functions used by the forecast tools. The library is used to read data from the iEasyHydro database and to write bulletins in a similar fashion as the software iEasyHydro.

#### Prerequisites
You will need a Python IDE for development. If you do not alreay have one installed, we recommend the use of Visual Studio Code for developping the backend. The installation instructions can be found [here](https://code.visualstudio.com/download). You will need to install the Python extension for Visual Studio Code. The installation instructions can be found [here](https://code.visualstudio.com/docs/languages/python).

We use conda for managing the Python environment. The installation instructions can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/). Once conda is installed, you can create a new conda environment by running the following command in the terminal:
```bash
conda create --name my_environment python=3.11
```
Through the name tag you can specify a recognizable name for the environment (you can replace my_environment with a name of your choosing). We use a different environment for each module of the backend. For development, python 3.10 and 3.11 was used. We therefore recommend you continue development with python 3.10 or 3.11 as well. You can activate the environment by running the following command in the terminal:
```bash
conda activate my_environment
```
The name of your environment will now appear in brackets in the terminal.

We show how to proceed with each module based on the example of the preprocessing_runoff tool. The procedure is the same for all modules.

Install the following packages in the terminal (note that this will take some time):
```bash
cd apps/preprocessing_runoff
pip install -r requirements.txt
```
The backend can read data from excel and/or from the iEasyHydro database (both from the online and from the local version of the software). If you wish to use the iEasyHydro database, you will need to install the iEasyHydro SKD library. More information on this library that can be used to access your organizations iEasyHydro database can be found [here](https://github.com/hydrosolutions/ieasyhydro-python-sdk). Some tools require the the library [iEasyReports](https://github.com/hydrosolutions/ieasyreports) that allows the backend of the forecast tools and the forecast dashboards to write bulletins in a similar fashion as the software iEasyHydro. And finally you will need to load the iEasyHydroForecast library that comes with this package. You will therefore further need to install the following packages in the terminal:
```bash
pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
pip install git+https://github.com/hydrosolutions/ieasyreports.git@main
pip install -e ../iEasyHydroForecast
```
If you wish to use data from your organizations iEasyHydro database, you will need to configure the apps/config/.env_develop file (see [doc/configuration.md](configuration.md) for more detailed instructions). We recommend testing your configuration by running a few example queries from the [documentation of the SDK library](https://github.com/hydrosolutions/ieasyhydro-python-sdk) in a jupyter notebook.

#### How to run the backend modules locally
##### Pre-processing of river runoff data
Establish a connection to the iEasyHydro database by configuring the apps/config/.env_develop file (see [doc/configuration.md](configuration.md) for more detailed instructions). You might require an ssh connection to your local iEasyHydro installation, consult your IT admin for this. You can then run the pre-processing of river runoff data tool with the default .env_develop file by running the following command in the preprocessing_runoff folder in the terminal:
```bash
python preprocessing_runoff.py
```
Note, we use different .env files for testing and development. We use an environment variable to specify a .env file we use for testing purposes (SAPPHIRE_TEST_ENV, see chapter on testing below) and we use one for development with private data (SAPPHIRE_OPDEV_ENV). During development, we typically use the command:
```bash
SAPPHIRE_OPDEV_ENV=True python preprocessing_runoff.py
```

##### Pre-processing of forcing data from the data gateway


##### Running the linear regression tool
Edit the file apps/internal_data/last_successful_run.txt to one day before the first day you wish to run the forecast tools for. For example, if you wish to start running the forecast tools from January 1, 2024, write the date 2023-12-31 as last successful run date. You can then run the forecast backend in the offline mode to simulate opearational forecasting in the past by running the following command in the terminal:
```bash
python run_offline_mode.py
```
This will run the linear regression tool for the period of January first 2024 to the current day. You can change the dates to your liking but make sure that you have data available for the production of the linear regression models and for forecasting. Currently, the tools assume that the data availability starts on January 1 2000. The tool will write the results to the file *ieasyforecast_results_file*, apps/internal_data/forecasts_pentad.csv.
If you have daily data available from 2000 to the present time, we recommend starting the forecast from 2010 onwards. This gives the backend tool 10 years of data (from 2000 to 2010) to build a linear regression model. Each year, the linear regression model will be updated with the data from the previous year. That means, the parameters of the linear regression model $y=a \cdot x+b$ will change each year. If you are interested in the model parameters, they are provided in the *ieasyforecast_results_file*, apps/internal_data/forecasts_pentad.csv.

For development, it may be useful to use a different .env file. We use an environment variable to specify a .env file we use for testing purposes (SAPPHIRE_TEST_ENV, see chapter on testing below) and we use one for development with private data (SAPPHIRE_OPDEV_ENV).

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
You can dockerize each module on your local machine or on a server using Github Actions. The Dockerfiles to package each module are located in the apps/module_name folder. The docker-compose.yml file runs the entire containerized workflow and is located in the bin folder.

### Configuration dashboard
Note that at the time of writing, a Docker base image with R and RShiny is not available for the ARM architecture (the latest Mac processors). The configuration dashboard has, with the current setup, been dockerized in Ubuntu.

The forecast dashboard is dockerized using the Dockerfile in the apps/configuration_dashboard folder. To build the docker image locally, run the following command in the root directory of the repository:
```bash
docker build --no-cache -t station_dashboard -f ./apps/configuration_dashboard/dockerfile .
```
Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.
```bash
docker run -e "IN_DOCKER_CONTAINER=True" \
    -v <full_path_to>/config:/app/apps/config \
    -v <full_path_to>/data:/app/data \
    -p 3647:3647 \
    --name station_dashboard_container station_dashboard
```

### Backend
The backend is dockerized using the Dockerfile in the apps/backend folder. Dockerization has been tested under both Ubuntu running on Windows or Mac OS operating systems. To build the docker image locally, run the following command in the root directory of the repository:
```bash
docker build --no-cache -t preprocessing_runoff -f ./apps/preprocessing_runoff/Dockerfile .
```
Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.
```bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/data:/app/data -v <full_path_to>/apps/internal_data:/app/apps/internal_data -p 9000:8801 --name preprocessing_runoff preprocessing_runoff
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

## How to use private data
If you want to use private data for the development of the forecast tools, you can do so by following the instructions below. We recommend tht you use a differenet .env file for development with private data. We use an environment variable to specify a .env file we use for testing purposes (SAPPHIRE_TEST_ENV, see chapter on testing below) and we use one for development with private data (SAPPHIRE_OPDEV_ENV). To make use of the SAPPHIRE_OPDEV_ENV environment variable, you store your environment in a file named .env_develop_kghm in the folder ../sensitive_data_forecast_tools/config (relative to this projects root folder). The folder ../sensitive_data_forecast_tools should contain the following sub-folders:
- bin
- config
- daily_runoff
- GIS
- intermediate_data
- reports
- templates

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

# Deployment
GitHub Actions are used to automatically test the Sapphire Forecast Tools and to build and pull the Docker images to Docker Hub. From there, the images can be pulled to a server and run. To install or update the forecast tools on a server, please follow the instructions in [doc/deployment.md](deployment.md).
