<h1>Development</h1>

This document describes how to develop the application and how to add hydrological forecasting models to your installation of the forecast tools. If you wish to install the demo version of the application, please refer to the [installation instructions in the deployment guide](deployment.md). The document is structured as follows:

- [1. Prerequisites](#1-prerequisites)
  - [1.1 Installation of 3rd party software](#11-installation-of-3rd-party-software)
    - [Python](#python)
    - [Conda](#conda)
    - [Visual Studio Code](#visual-studio-code)
    - [R](#r)
    - [RStudio](#rstudio)
    - [Git Desktop](#git-desktop)
    - [Docker](#docker)
  - [1.2 Test run the demo version](#12-test-run-the-demo-version)
    - [Clone the github repository](#clone-the-github-repository)
    - [Test-run the Sapphire forecast tools](#test-run-the-sapphire-forecast-tools)
  - [1.3 Set up your work environment](#13-set-up-your-work-environment)
    - [Activate the conda environment](#activate-the-conda-environment)
    - [Install the required packages](#install-the-required-packages)
- [2. Development instructions specific to the tools](#2-development-instructions-specific-to-the-tools)
  - [2.1 Configuration dashboard configuration\_dashboard](#21-configuration-dashboard-configuration_dashboard)
  - [2.2 Backend modules](#22-backend-modules)
    - [2.2.1 Pipeline (pipeline)](#221-pipeline-pipeline)
      - [Description](#description)
      - [How to manually run the pipeline](#how-to-manually-run-the-pipeline)
    - [2.2.2 Preprocessing runoff data (preprocessing\_runoff)](#222-preprocessing-runoff-data-preprocessing_runoff)
      - [Description of module](#description-of-module)
      - [I/O](#io)
      - [Prerequisites](#prerequisites)
      - [How to run the tool](#how-to-run-the-tool)
    - [2.2.3 Preprocessing of gridded weather data (preprocessing\_gateway)](#223-preprocessing-of-gridded-weather-data-preprocessing_gateway)
      - [Description](#description-1)
      - [Prerequisites](#prerequisites-1)
      - [I/O](#io-1)
      - [How to run the tool](#how-to-run-the-tool-1)
    - [2.2.4 Linear regression (linear\_regression)](#224-linear-regression-linear_regression)
      - [Description of module](#description-of-module-1)
      - [Prerequisites](#prerequisites-2)
      - [How to run the tool](#how-to-run-the-tool-2)
    - [2.2.5 Conceptual rainfall-runoff assimilation model (conceptual\_model)](#225-conceptual-rainfall-runoff-assimilation-model-conceptual_model)
      - [Description of the Conceptual Model Module](#description-of-the-conceptual-model-module)
      - [Prerequisites](#prerequisites-3)
      - [I/O](#io-2)
      - [How to run the tool](#how-to-run-the-tool-3)
    - [2.2.6 Machine learning (machine\_learning)](#226-machine-learning-machine_learning)
      - [Description of module](#description-of-module-2)
      - [Prerequisites](#prerequisites-4)
      - [I/O](#io-3)
        - [Output Files](#output-files)
      - [How to run the tool](#how-to-run-the-tool-4)
    - [2.2.7 Post-processing of forecasts (postprocessing\_forecasts)](#227-post-processing-of-forecasts-postprocessing_forecasts)
    - [2.2.8 Manual triggering of the forecast pipeline](#228-manual-triggering-of-the-forecast-pipeline)
      - [How to re-run the forecast pipeline manually](#how-to-re-run-the-forecast-pipeline-manually)
    - [2.2.9 Forecast dashboard](#229-forecast-dashboard)
      - [Prerequisites](#prerequisites-5)
      - [How to run the forecast dashboard locally](#how-to-run-the-forecast-dashboard-locally)
      - [How to test the dashboard containers locally](#how-to-test-the-dashboard-containers-locally)
  - [2.3 The backend (note: this module is deprecated)](#23-the-backend-note-this-module-is-deprecated)
      - [Prerequisites](#prerequisites-6)
      - [How to run the backend modules locally {#how-to-run-the-backend-modules-locally}](#how-to-run-the-backend-modules-locally-how-to-run-the-backend-modules-locally)
        - [Pre-processing of river runoff data {#pre-processing-of-river-runoff-data}](#pre-processing-of-river-runoff-data-pre-processing-of-river-runoff-data)
        - [Pre-processing of forcing data from the data gateway {#pre-processing-of-forcing-data-from-the-data-gateway}](#pre-processing-of-forcing-data-from-the-data-gateway-pre-processing-of-forcing-data-from-the-data-gateway)
        - [Running the linear regression tool {#running-the-linear-regression-tool}](#running-the-linear-regression-tool-running-the-linear-regression-tool)
  - [Dockerization](#dockerization)
    - [Configuration dashboard](#configuration-dashboard)
    - [Backend](#backend)
    - [Forecast dashboard](#forecast-dashboard)
  - [How to use private data](#how-to-use-private-data)
  - [Development workflow](#development-workflow)
  - [Testing](#testing)
- [Deployment](#deployment)

# 1. Prerequisites

Note: The software has been developed on a Mac computer and packaged with Ubuntu base images using Docker using GitHub Actions (with workflow instructions in .github/workflows/main.yml). It has been tested extensibly on an Ubuntu server. The software has not been tested on Windows.

The following open-source technologies are used in the development of the forecast tools: - For scripting and development: - Scripting language [Python](#python) and user interface [Visual Studio Code](#visual-studio-code) - Python package manager [Conda](#conda) - Scripting language [R](#R) and user interface RStudio - Code version control: GitHub Desktop - Containerization: Docker

If you have all of these technologies installed on your computer, you can skip the installation instructions below and proceed to the instructions on the general development workflow (TODO: add link to section).

## 1.1 Installation of 3rd party software

You will find instructions on how to install the technologies used in the development of the forecast tools below. We recommend that you install the technologies in the order they are listed. If you are new to any of these tools it is recommended to run through a quick tutorial to get familiar with the technology befor starting out to work on the SAPPHIRE forecast tools.

### Python

Install Python on your computer. The installation instructions can be found in many places on the internet, for example [here](https://realpython.com/installing-python/). We recommend the installation of Python 3.11. You can check the version of Python installed on your computer by running the following command in the terminal:

``` bash
python --version
```

### Conda

We use conda for managing the Python environment. The installation instructions can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/). Once conda is installed, you can create a new conda environment by running the following command in the terminal:

``` bash
conda create --name my_environment python=3.11
```

Through the name tag you can specify a recognizable name for the environment (you can replace my_environment with a name of your choosing). We use a different environment for each module of the backend. For development, python 3.11 was used. We therefore recommend you continue development with python 3.11 as well.

### Visual Studio Code

Install Visual Studio Code on your computer. The installation instructions can be found [here](https://code.visualstudio.com/download). We recommend the installation of the Python extension for Visual Studio Code. The installation instructions can be found [here](https://code.visualstudio.com/docs/languages/python). Note that you can use any other Python IDE for development. We recommend Visual Studio Code as it is free and open-source.

### R

Install R on your computer. The installation instructions can be found [here](https://cran.r-project.org/). We recommend the installation of R 4.4.1. You can check the version of R installed on your computer by running the following command in the terminal:

``` bash
R --version
```

### RStudio

Install RStudio on your computer. The installation instructions can be found [here](https://rstudio.com/products/rstudio/download/). We recommend the installation of RStudio Desktop.

### Git Desktop

We recomment the installation of GitHub Desktop to manage the repository. The installation instructions can be found [here](https://desktop.github.com/).

### Docker

Install Docker Desktop on your computer so you can test dockerization of sofware components locally. The Docker installation instructions can be found [here](https://docs.docker.com/install/).

## 1.2 Test run the demo version

Before starting the development of the Forecast Tools, it is recommended to first try to run the tools with the public demo data set. This will help you to understand the workflow of the tools and to identify the modules you want to work on. The following sections provide instructions on how to test-run the demo version of the forecase tools.

### Clone the github repository

Once you have installed the technologies, you can set up your working environment. We recommend that you create a folder for the repository on your computer. You can then clone the repository to your computer. Open a terminal and navigate to the folder where you want to clone the repository. Run the following command in the terminal:

``` bash
git clone https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools.git
```

### Test-run the Sapphire forecast tools

Navigate to the root directory of the repository in the terminal:

``` bash
cd SAPPHIRE_Forecast_Tools
```

You can test-run the forecast tools by running the following command in the terminal:

``` bash
source bin/run_sapphire_forecast_tools.sh <path_to_data_root_folder>
```

For the test run, you can use the full path to your SAPPHIRE_Forecast_Tools folder as the path to the data root folder. You can get the absolute path to the folder by running the following command in the terminal:

``` bash
pwd
```

An example command for the test run installed under /Users/username/forecasting/SAPPHIRE_FORECAST_TOOLS would be:

``` bash
bash bin/run_sapphire_forecast_tools.sh /Users/username/forecasting/
```

## 1.3 Set up your work environment

If the demo version runs on your system, you can be confident, that you have all the necessary software installed and that the SAPPHIRE Forecast Tools are working on your system. You can now set up your work environment to start developing the forecast tools. Each module comes with a requirements.txt file that lists the required packages. You can install the required packages in your conda environment by following the instructions below. We recommend individual conda environments for each module of the forecast tools.

### Activate the conda environment

You can list the available conda environments with:

``` bash
conda env list
```

You can activate an environment by running the following command in the terminal (replace my_environment with the name of the environment you want to work in):

``` bash
conda activate my_environment
```

### Install the required packages

Each module has a requirements.txt file that lists the required packages. You can install the required packages in your conda environment by running the following command in the terminal:

``` bash
cd apps/module_name
pip install -r requirements.txt
```

You now have a working installation of the SAPPHIRE Forecast Tools with a public demo data set.

# 2. Development instructions specific to the tools

The following sections provide instructions on how to develop the individual modules of the forecast tools. The names of the modules in the apps folder are given in brackets.

TODO: Add a flow chart of the workflow of the forecast tools.

## 2.1 Configuration dashboard configuration_dashboard

The forecast configuration dashboard is written in R and uses the Shiny framework.

You will further need to install the following R packages (you can do so by running the following commands in the R console):

``` r
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

TODO: Aidar, please validate if the above is correct.

## 2.2 Backend modules

### 2.2.1 Pipeline (pipeline)

#### Description

We use the python package Luigi to manage the workflow of the forecast tools. Luigi takes care of running each backend module in sequence or in parallel. You will find detailed information about Luigi [in the Luigi docs](https://luigi.readthedocs.io/en/stable/index.html#).

#### How to manually run the pipeline

All modules of the SAPPHIRE Forecast Tools are run in individual docker containers. To run them on your system, please follow the instructions below.

<details>

<summary>Mac OS</summary>

To build and run the pipeline locally on a Mac to the following steps before you run the docker compose up command above:

1.  Open a terminal and navigate to the root directory of the repository.

2.  Clean up the docker work space (note that this will remove all containers and images in your Docker workspace):

``` bash
bash bin/clean_docker.sh
```

3.  Build the docker images:

``` bash
ieasyhydroforecast_data_root_dir=<ieasyhydroforecast_data_root_dir> bash bin/build_docker_images.sh latest
```

</details>

<details>

<summary>Ubuntu</summary>

To test-run the pipeline on an Ubuntu server, follow the instructions in [doc/deployment.md](deployment.md).

</details>

The pipeline is run inside a Docker container. The Dockerfile is located in the apps/pipeline folder. To build the Docker image for the pipeline locally, run the following command in the root directory of the repository:

``` bash
docker compose -f ./bin/docker-compose.yml build
```

To run the Docker containers locally, run the following command in the root directory of the repository:

``` bash
docker compose -f ./bin/docker-compose.yml up
```

### 2.2.2 Preprocessing runoff data (preprocessing_runoff)

TODO: formulate the text

#### Description of module

-   Reads data from excel files and, if access is available, from the iEasyHydro database. TODO: Describe what happens in this tool step by step.

#### I/O

-   Link to description of required input files (daily runoff data in excel format)
-   Describe what output files are produced

TODO: Bea Add flow chart with detailed I/O

#### Prerequisites

-   How to download & install iEasyHydro (HF) (-\> link to iEasyHydro documentation)

#### How to run the tool

### 2.2.3 Preprocessing of gridded weather data (preprocessing_gateway)

#### Description

The proprocessing_gateway module gets weather forecasts and re-analysis weather data from ECMWF IFS as well as TopoPyScale snow model results that are provided through the SAPPHIRE Data Gateway. The module reads the data and prepares it for the hydrological models. The module is composed of two components that are operationally run in a docker container and one script that can be run to produce hindcasts (which are used to calculate model forecast skill metrics).:

- Operational workflow:
  - Downolading of operational weather forecasts and downscaling of weather forecasts (Quantile_Mapping_OP.py)
  - Updating of re-analysis weather data and downscaling of re-analysis weather data (extend_era5_reanalysis.py)
  - Downloading the operational snow forecast(snow_data_operational.py).
- Production of hindcasts:
  - Downloading of re-analysis weather data and downscaling of re-analysis weather data (get_era5_reanalysis_data.py)
  - Downloading of the re-analysis snow data (snow_data_reanalysis.py)

The Quantile_Mapping_OP.py script accesses the the SAPPHIRE Data Gateway and downloads the ERA5 ECMWF IFS control member and ensemble forecast. Afterwards, it can perform a downscaling using the quantile mapping  method using a set of previously fitted parameters. The fitting of the parameters for downscaling is performed with a parametric transformations where the new transformed value is obtained by the formula $y = a*x^b$ in the [fitQmap](#https://search.r-project.org/CRAN/refmans/qmap/html/fitqmapptf.html) package in R. As we use operational weather forecasts from ECMWF served through the SAPPHIRE Data Gateway, we do not expect to have any gaps in the weather forecast time series. However, in case there should be nan values in the forcing data, the preprocessing_gateway module fills them by taking the last available observation.

TODO: Sandro: the downscaling is optional as I understand. How can a user turn the downscaling on or off?

The extend_era5_reanalysis.py file is used to maintain a set of past and current weather forcing data for each river basin. It is updated operationally by appending the latest ERA5 Land Reanalysis data to the hindcast forcing file. It reads in the hindcast file and the operational forcing data, combines them and removes dublicates.

The snow_data_operational.py script accesses the SAPPHIRE Data Gateway and downloads the forecast from the TopoPyScale snow model. Additionally, it also gets the data for the past 365 days. This process is done for a set of user defined HRU's (ieasyhydroforecast_HRU_SNOW_DATA) (Note that the TopoPyScale snow model is not valid on glaciers, therefore it is suggested to upload shapefiles,where the glaciers were removed). There are three possible variables which can be downloaded (ieasyhydroforecast_SNOW_VARS). SWE (snow water equivalent (mm)), HS (snow water depth (m)) and RoF (runoff from snowmelt and precipitation (mm)). The new operational data is directly appended to the file which also contains the historical data.

The get_era5_reanalysis_data.py is an initialization file. It also accesses the data-gateway and pulls the ERA5 Land Reanalysis data for a provided time window and a given HRU. It also performs the downscaling on this data. The reason behind this script is, to obtain a file where we have past forcing data saved to perform for example hindcasts.

The snow_data_reanalysis.py script is an initialization file. It accesses the SAPPHIRE Data Gateway and downloads the snow data for the user specified HRU's (ieasyhydroforecast_HRU_SNOW_DATA) and variables (ieasyhydroforecast_SNOW_VARS) for the time period of 2000 to (today - 180 days). 

#### Prerequisites

[Open ECMWF weather forecasts](https://www.ecmwf.int/en/forecasts/datasets/open-data) and results of the [TopoPyScale Snow model](https://topopyscale.readthedocs.io/en/latest/) are pre-processed for hydrological modelling with the SAPPHIRE Data Gateway (TODO: publish once development completed). If you wish to use weather and snow forecast data in the SAPPHIRE Forecast Tools, you will have to install at least the SAPPHIRE Data Gateway and the [SAPPHIRE data gateway client](https://github.com/hydrosolutions/sapphire-dg-client) and optionally also [TopoPyScale](https://topopyscale.readthedocs.io/en/latest/) by following the installation instructions provided in the repositories.

<div style="border: 1px solid #ccc; padding: 10px; margin-top: 10px;">
  <strong>Side note on the interaction with the SAPPHIRE Data Gateway
:</strong> The SAPPHIRE Data Gateway aggregates operational gridded weather and snow data to polygons (e.g. basin outlines or hydrological response units) and makes it available as time series through the SAPPHIRE data gateway client. Prior to the operationalization, The user uploads a shapefile with polygons to the SAPPHIRE Data Gateway which, optimally, contains the attribute code corresponding to the hydropost identifier to produce hydrological forecasts for, and subscribes the shapefile to operational data downloads.

The shapefile uploaded to the SAPPHIRE Data Gateway should have the attributes `name`, `Z` and `geometry`. The `name` attribute should contain the code corresponding to the gauge station (also called hydropost) identifier, the `Z` attribute should contain the elevation of the hydropost (or a dummy number) and the `geometry` attribute should contain the geometry of the hydropost. The shape file can have one or multiple features (polygons) and the code attribute should be unique for each feature. Below you see an example of how the shapefile should look like:

| name      | Z   | geometry |
|-----------|-----|----------|
| <code_basin_A> | 1   | ...      |
| <code_basin_B> | 1   | ...      |
|

Should the code attributes in the data gateway not correspond to the code attributes in the forecast tools, you can apply a code mapping in the forecast tools by editing the config file ieasyhydroforecast_config_file_data_gateway_name_twins (see below). For more detailed information, please refer to the SAPPHIRE Data Gateway documentation (TODO: add link).

TODO: Add step-by-step instructions of how to upload shapefile and subscribe to operational data on the data gateway (assuming the data gateway is up and running).
</div>
<br></br>
To run the preprocessing_gateway module locally, you will can install the required packages as follows:

Open a terminal and navigate to the preprocessing_gateway folder in the repository:
``` bash
cd /path/to/SAPPHIRE_Forecast_Tools/apps/preprocessing_gateway
```
Then install the required packages by running the following command in the terminal with either conda or pip:

<details>
<summary>Conda</summary>
```
conda install --file requirements.txt
```
</details>

<details>
<summary>Pip</summary>
```
pip install -r requirements.txt
```
</details>

#### I/O

Here is the folder structure represented which interacts with the module preprocess_gateway:

-   YOUR_DATA_FOLDER

    -   config

        -   model_and_scalers

            -   params_quantile_mapping

                -   YOUR_HRU_P_params.csv

                -   YOUR_HRU_T_params.csv

    -   intermediate_data

        -   control_member_forcing

            -   YOUR_HRU_P_control_member.csv

            -   YOUR_HRU_T_control_memeber.csv

        -   data_gateway

            -   ....

        -   ensemble_forcing

            -   YOUR_HRU_P_ensemble_forecast.csv

            -   YOUR_HRU_T_ensemble_forecast.csv

        -   hindcast_forcing

            -   YOUR_HRU_P_reanalysis.csv

            -   YOUR_HRU_T_reanalysis.csv

        -   snow_data

            -   SWE
                -   YOUR_HRU_SWE.csv
            -   HS
                -   YOUR_HRU_HS.csv
            -   RoF
                -   YOUR_HRU_RoF.csv

The only input file you need to provide, if you would like to make the downscaling, are the files in the params_quantile_mapping folder. This files should have the following structure:

| code  | a                | b                | wet_day             |
|-------|------------------|------------------|---------------------|
| xxxx1 | 1.57473248903875 | 1.08635623528307 | 0.00379749326713768 |
| xxxx2 | 1.32689320186785 | 1.23330279398003 | 0.00384698459002093 |
| xxxx3 | 1.22439440281778 | 1.15911354977616 | 0.0031125073391893  |
| xxxx4 | 1.29536038899781 | 1.18507749883279 | 0.00358122349959776 |
| ...   | ...              | ...              | ...                 |
|

When accessing the SAPPHIRE data gateway, the client downloads a csv file and saves it in the data gateway folder. When you integrate a HRU on the data gateway, you can name the different shapefiles in the HRU. These names correspond to the codes. Should the codes in the data gateway not correspond to the codes in the forecast tools, you can apply a code mapping by editing the config json file ieasyhydroforecast_config_file_data_gateway_name_twins. The file should have the following structure to map codes in the data gateway to codes in the forecast tools:

``` json
{
  "gateway_name_twins": {
    "<basin_A_gateway_code>": "<basin_A_forecast_tools_code>",
    "<basin_B_gateway_code>": "<basin_B_forecast_tools_code>"
  }
}
```

The ensemble forecast files downloaded through the data gateway client have the following format (corresponding to the forcing data file format of the free hydrologic-hydraulic model [RS Minerve](https://crealp.ch/rs-minerve/) developed and maintained by the [CREALP](https://crealp.ch/) foundation):

|                   | name               |
|-------------------|--------------------|
| **Sensor**        | T                  |
| **Category**      | Temperature        |
| **Unit**          | C                  |
| **Interpolation** | linear             |
| **21.08.2024**    | 10.886254967534398 |
| **22.08.2024**    | 10.874600198227881 |
| **23.08.2024**    | 10.76900162280549  |
| **24.08.2024**    | 10.889982012342898 |
| **...**           | ...                |
|

Here the name is defined in the shapefile (see "How to run the model")
And for the control member it looks like this:

| Station           | xxxx1              | xxxx2              | xxxx1.1            | xxxx2.1            |
|------------|------------|------------|------------|------------|
| **X**             | 79.01104020491191  | 78.92687493141804  | 79.01104020491191  | 78.92687493141804  |
| **Y**             | 42.556962256438446 | 42.427748566619805 | 42.556962256438446 | 42.427748566619805 |
| **Z**             | 0                  | 0                  | 0                  | 0                  |
| **Sensor**        | T                  | T                  | P                  | P                  |
| **Category**      | Temperature        | Temperature        | Precipitation      | Precipitation      |
| **Unit**          | C                  | C                  | mm/d               | mm/d               |
| **Interpolation** | Linear             | Linear             | Linear             | Linear             |
| **04.09.2024**    | 2.36               | 1.38               | 0.92               | 2.94               |
| **03.09.2024**    | 3.67               | 2.88               | 2.391              | 4.46               |
| **02.09.2024**    | 8.27               | 6.83               | 5.71               | 8.05               |
| **...**           | ...                | ...                | ...                | ...                |
|

TODO: Sandro: In the files we have on Dropbox, the columns are not named like this (xxx1 and xxx1.1 but 2 times xxx1).

These files are than transformed and, optionally, downscaled. The transformed (and downscaled) files have the format shown below with the example of precipitation. For the temperature, the column P (for precipitation) is replaced with T (for temperature). If the file is the control member, the column "ensemble_member" is non existent.

| date       | P    | code  | ensemble_member |
|------------|------|-------|-----------------|
| 2024-08-21 | 7.26 | xxxx1 | 1               |
| 2024-08-22 | 0.62 | xxxx1 | 1               |
| 2024-08-23 | 8.95 | xxxx1 | 1               |
| ...       | ...  | ...   | ...             |
|

The files in the hindcast folder (YOUR_HRU_P_reanalysis.csv) have the exact same format as the control member file.

The snow data comes in the following format from the SAPPHIRE Data-Gateway, for both the operational and re-analysis data.

|                   | name               |
|-------------------|--------------------|
| **Sensor**        | snow               |
| **Category**      | Height             |
| **Unit**          | mm                 |
| **Interpolation** | linear             |
| **21.08.2024**    | 49.886254967534398 |
| **22.08.2024**    | 46.874600198227881 |
| **23.08.2024**    | 43.76900162280549  |
| **24.08.2024**    | 45.889982012342898 |
| **...**           | ...                |

This data is then transformed into the same format as previously shown. The name of the SWE columns is replaced with the correspondign variable for HS and RoF.

| date       | SWE    | code  |
|------------|------|-------|
| 2024-08-21 | 7.26 | xxxx1 | 
| 2024-08-22 | 7.62 | xxxx1 | 
| 2024-08-23 | 8.95 | xxxx1 |
| ...       | ...  | ...   | 

If the provided HRU contains multiple zones per basin (for example elevation bands), then the format looks slightly different. Here SWE_1 represents the first elevation band, SWE_2 the second and so on. Note that the elevation bands are infered from the name column of the shapefile of the shapefile. The name is expected to be {basin code}_{elevation band}.

| date       | code  | SWE_1 | SWE_2 | SWE_x |
|------------|-------| ------ | -----| ----- |
| 2024-09-21 |  xxxx1 | 12.1 | 13.3 | 15.1 |
| 2024-09-22 |  xxxx1 | 12.2 | 13.6 | 15.7 |
| 2024-09-23 |  xxxx1 | 12.9 | 14.2 | 15.9 |
| ...       |  ...   | 13.5  | 14.9 | 16.2 |

#### How to run the tool
As mentioned above, the preprocessing_gateway module requires access to operational data provided by the SAPPHIRE Data Gateway.
The first thing you need to ensure is that your HRU is on the SAPPHIRE data-gateway (TODO: add link to the documentation). Here you need to upload your HRU with its shapefiles and trigger the ERA5 Land Reanalysis and subscribe to daily calculations and the forecasts. The shapefile you upload should have the following columns:

| name      | Z   | geometry |
|-----------|-----|----------|
| YOUR_CODE | 1   | ...      |

Ensure that you are consistent with the codes and name. If you have a HRU with multiple zones per basins set the name according to this scheme: {basin}_{zone}. If you have processed your HRU on the data-gateway you are ready to proceed.

First you might want to download hindcast data from the SAPPHIRE Data Gateway so that you can produce hindcasts with your models to calculate forecast skill statistics over an extended time period (ensure that you have triggered the ERA5 land reanalysis on the data-gateway for this period). You can do this by running the following command in the terminal:

``` bash
SAPPHIRE_OPDEV_ENV=True ieasyhydroforecast_reanalysis_START_DATE=2009-01-01 ieasyhydroforecast_reanalysis_END_DATE=2023-12-31 python get_era5_reanalysis_data.py
```

If you want to pull the operational data you can run the following command:

``` bash
SAPPHIRE_OPDEV_ENV=True python Quantile_Mapping_OP.py
```

You can specifiy the HRU for which you need the control member forecast and the HRU's for which you need the ensemble forecast in the config file (ieasyhydroforecast_HRU_CONTROL_MEMBER and ieasyhydroforecast_HRU_ENSEMBLE).

In order to keep the hindcast data updated, you can run the extend_era5_reanalysis.py script. This only works if you have operational data and you should ensure that you don't have any gaps longer than 6 months between the end of the hindcast file, and the start of the operational forcing data, or else you will have some forcing gaps.

You can set the HRU for which you need snow data (ieasyhydroforecast_HRU_SNOW_DATA) and which are the variables you are interested in (ieasyhydroforecast_SNOW_VARS).

### 2.2.4 Linear regression (linear_regression)

#### Description of module

TODO: Bea

#### Prerequisites

No prerequisites No external input files required (depends entirely on pre-processing of runoff data) Need to describe which files are read and which files are produced

#### How to run the tool

TODO: Bea

### 2.2.5 Conceptual rainfall-runoff assimilation model (conceptual_model)






#### Description of the Conceptual Model Module

**Conceptual Model Overview:**
The "conceptual_model" module is designed to facilitate operational discharge forecasting, particularly in high-altitude or complex catchment areas. This module integrates rainfall-runoff models with several combinable components such as the GR4J or GR6J model, the CemaNiege component for snow melt. This is provided in the original [airGR](https://cran.r-project.org/web/packages/airGR/index.html) R package. The modified R package [airGR_GM](https://github.com/hydrosolutions/airGR_GM) package also includes a additional glacier module and the possibility to add basin specif temperature and precipitaiton lapse rates (see for more details [here](https://github.com/hydrosolutions/airGR_GM) ).

- **GR4J Model**: This model employs four key parameters to simulate hydrological processes:
  - **X1 (Production Store Capacity)**: Represents the soil's root zone capacity where atmospheric exchanges, including evapotranspiration, occur.
  - **X2 (Groundwater Exchange Coefficient)**: Regulates the water transfer to groundwater, allowing the simulation of both leaky and gaining catchments.
  - **X3 (Routing Store Capacity)**: Influences slower flow processes like interflow, which are tied to the catchment's geology and soil cover.
  - **X4 (Lag Time)**: Determines the lag between rainfall and peak flow, shaping the hydrograph.

- **GR6J Model**: Building on GR4J, this model includes two additional parameters:
  - **X5 (Inter-Catchment Exchange Threshold)**: Enhances the simulation of groundwater exchange processes.
  - **X6 (Exponential Store Depletion Coefficient)**: Improves the simulation of low-flow conditions.

- **CemaNeige Module**: This module simulates snow accumulation and melt processes by introducing two calibration factors, CN1 and CN2. It utilizes elevation bands, which represent equal areas, to account for variations in snow processes across different altitudes. Precipitation and temperature are distributed across these bands using basin-specific lapse rates.

- **Glacier Module**: This extension simulates glacier melt using a temperature index approach, employing the same spatial discretization as CemaNeige. Glacier melt is triggered when Snow Water Equivalent (SWE) drops below a specific threshold, and air temperature exceeds a defined limit in the elevation band. The melt rate is proportional to temperature and an ice melt factor.

The conceptual model framework is flexible and can be calibrated with discharge data or other relevant hydrological data over a specified period. It is possible to calibrate the model also with snow water equivalent data for example from a [Factorial Snow Model](https://github.com/ArcticSnow/TopoPyScale). It can be adapted for various catchments and is implemented using a modified version of the [airGR_GM](https://github.com/hydrosolutions/airGR_GM) package in R.

**Data Assimilation:**
To enhance discharge predictions, the module supports data assimilation techniques that incorporate real-time data into the model. I
 This involves perturbing meteorological forcings and internal model states to generate an ensemble of simulations. The model is typically run multiple times (e.g., 200 iterations) with these perturbations. A Particle Filter (PF) is then applied to update model predictions based on the ensemble runs. The PF assigns weights to each run based on its agreement with observed data and resamples the ensemble to prioritize the most accurate simulations. This process enhances the accuracy of predictions as new data becomes available. Data assimilation within the module is implemented using a modified version of the [airGRdatassim](https://github.com/hydrosolutions/airgrdatassim) package in R.

This module can be applied to various hydrological settings, making it a versatile tool for operational discharge forecasting in diverse environmental conditions.


**Data Assimilation:**
The "conceptual_model" module enhances discharge predictions by incorporating real-time data through an advanced data assimilation process. This process is based on the [airGRdatassim](https://cran.r-project.org/web/packages/airGRdatassim/index.html) package available on CRAN. The module presented here utilizes a modified version of the airGRdatassim package, which can be found [here](https://github.com/hydrosolutions/airgrdatassim). This customized version is specifically tailored to address the unique demands of operational discharge forecasting in diverse hydrological environments.
In this module, data assimilation is implemented using an ensemble approach. Meteorological forcings and internal model states are perturbed to generate multiple simulations. These simulations are then processed using a Particle Filter (PF), a method that assigns weights to each simulation based on its alignment with observed data. The PF subsequently resamples the ensemble, prioritizing the most accurate simulations while discarding less accurate ones. This iterative process allows the model to continually refine its predictions as new data is assimilated.
The modified version of the airGRdatassim package extends its capabilities in several key areas:
- **Glacier Module Integration**: The modification includes the ability to incorporate glacier processes into the data assimilation framework.
- **Operational Mode Functionality**: Supportin operational data assimilation, allowing the model to initialize with real-time conditions and to use basin-specific lapse rates for temperature and precipitation distribution



**Forcing data:**
The operational model uses temperature and precipitation inputs from the [preprocessing_gateway](#preprocessing-of-gridded-weather-data-preprocessing_gateway) module, with quantile-mapped ERA5-Land data for past data and all 51 ensemble members from the ECMWF IFS ensemble forecast for future weather predictions.

**Operational Setup:**
For each run, the model saves the initial condition from `lag_days` days before the current run, making it available for the next forecast. When a new forecast is triggered, the model uses the saved initial condition from the previous run, which stored the initial condition at the current forecast date minus the  `lag_days` (i.e. 180 days) and the time since the last forecast. The model first runs without data assimilation up to today minus `lag_days`, then incorporates data assimilation to the forecast date. Finally, it uses the ensemble weather predictions to run the model for each data assimilation ensemble and ensemble weather forecast, creating a 15-day ahead ensemble daily discharge forecast. From these results, pentadal and decadal discharge forecasts are calculated.

Folder Structure:

-   conceptual_model
    -   run_operation_forecasting_CM.R
    -   run_manual_hindcast.R
    -   run_initial.R
    -   requirements.txt
    -   install_packages.R
    -   functions
        -   functions_hindcast.R
        -   functions_operational.R


The primary function to run the model is `run_operation_forecasting_CM.R`.
As mentioned earlier, the `run_operation_forecasting_CM.R` file reads the initial conditions from the last forecast run. When the model is triggered for the first time, the `run_initial.R` function must be executed to generate the initial conditions for the operational run.

The `run_initial.R` function works as follows:
It creates the initial conditions for the specified basins, running from `start_ini` to `end_ini` for the basin `codes`  with the specified hydrological model in `fun_mod_mapping` defined in the `config_conceptual_model.json` file. The script reads the specific basin information and parameters (as detailed in the I/O documentation). It then retrieves the forcing data, including precipitation and temperature, from the [preprocessing_gateway](#preprocessing-of-gridded-weather-data-preprocessing_gateway). Within the `process_forecast_forcing` function (located in the `functions_operational.R` file), the Potential Evapotranspiration (PET) is calculated using the Oudin method, and the forcing data is structured to meet the model's requirements (also detailed in the I/O documentation). The specified conceptual model is then run from `start_ini` to `end_ini`. Finally, the output of this model run is saved as the initial condition.

Once the initial conditions are obtained from the `run_initial.R` script, a forecast can be triggered using the `run_operation_forecasting_CM.R` file.

The `run_operation_forecasting_CM.R` script operates as follows:

1. **Forecast Trigger**: The forecast is always triggered for the current day.

2. **Load Basin Information**: It loads basin-specific information, calibration parameters, and the output from the previous forecast (as detailed in the I/O documentation).

3. **Process Forcing Data**: The `process_forecast_forcing` function loads the perturbed forcing data and the control member forcing, then calculates the Potential Evapotranspiration (PET) based on temperature and latitude (Oudin).

4. **Distribute Forcing Data**: The forcing data is distributed into elevation bands using temperature and precipitation lapse rates specified in `Basin_Info` (see I/O documentation).

5. **Run Model Without Data Assimilation**: The model is run from the previous forecast date up to today minus `lag_days` using the `runModel_withoutDA.R` function. The output is saved for the next forecast run.

6. **Run Model With Data Assimilation**: Over the `lag_days`, the model is run with data assimilation using the `runModel_withDA.R` function until today's date.

7. **Run Future Weather Prediction**: The model is then run over the future weather prediction period using the `runModel_ForecastPeriod.R` function. All ensembles from the data assimilation are run with all 51 ensembles of the weather prediction.

8. **Calculate Statistics**: The forecast statistics are calculated using the `calculate_stats_forecast.R` function, including standard deviation, Q5, Q95, and other relevant metrics.

9. **Forecast Period**: The forecast runs up to the date provided by the weather forecast, which is up to 15 days ahead for the ECMWF IFS ensemble open data forecast.

10. **Check Previous Forecasts**: The operational model checks the stored forecasts from previous runs in the directory `ieasyhydroforecast_PATH_TO_RESULT/data/daily_BASINCODE.csv`.

11. **Handle Missing Forecast Days**: If there are missing days since the last forecast, the model starts for those days using the `get_hindcast_period.R` function from the `functions_hindcast.R` file. The script also loads the hindcast forcing data (as detailed in the I/O documentation). Hindcasts are run similarly to the `run_manual_hindcast.R` script, with daily timesteps.

12. **Pentadal Decadal**:For pentadal and decadal timesteps, the data is averaged over the corresponding periods and saved as `pentad_15194.csv` and `decad_15194.csv`.

To manually trigger a hindcast for a specific period, the `run_manual_hindcast.R` script can be run. In the configuration file (see configuration.md), you need to define `start_hindcast`, `end_hindcast`, and `hindcast_mode`, which can be `daily`, `pentad`, or `decad`. The hindcast is executed for all the specified `codes` using the hydrological model defined in `fun_mod_mapping`.

- In `daily` mode, the script provides a forecast for each day, 15 days ahead.
- In `pentad` mode, it provides forecasts in approximately 5-day intervals (pentads), calculating the mean Q50 value for each pentad.
- In `decad` mode, it provides forecasts in approximately 10-day intervals (decads), calculating the mean Q50 value for each decadal period.

The `run_manual_hindcast.R` script operates as follows:

1. **Initialization**: The script loads the configuration file and required functions, and sets up the necessary paths.

2. **Input Preparation**: It prepares the forcing input from the hindcast forcing file and the control member forcing file for more recent hindcasts, following the same process as in `run_operation_forecasting_CM.R`.

3. **Hindcast Execution**: The script generates the hindcast for the defined period using the `get_hindcast_period` function. This function iteratively runs the hydrological model for each time step, incorporating the specified `lag_days` and configuration parameters such as `NbMbr`, `DaMethod`, `StatePert`, and `eps` for the data assimilation method. It is important to note that the hindcast method uses ERA5-Land data rather than previously forecasted data.

4. **Function Details**:
    - The `get_hindcast_period` function prepares the input data for the hindcast, including forcing perturbation, distribution over elevation bands, and the defines the time steps for the hindcast based on the selected mode (`daily`, `pentad`, or `decad`).
    - It then calls the `get_hindcast` function, which operates similarly to the operational run in `run_operation_forecasting_CM.R`. The model is first run for one year without data assimilation (as a warm-up phase) to establish initial conditions. Following this, data assimilation is applied for the specified `lag_days` period and the data assimilation is run using the configured `NbMbr`, `DaMethod`, `StatePert`, and `eps` parameters.

5. **Result Saving**: Finally, the hindcast results are saved to the output directory (`ieasyhydroforecast_PATH_TO_RESULT`). The filenames are structured as follows:
    - **Daily**: `hindcast_daily_START_HINDCAST_END_HINDCAST_BASINCODE.csv` (dates in `%Y%m%d` format).
    - **Pentad**: `hindcast_pentad_START_HINDCAST_END_HINDCAST_BASINCODE.csv` (dates in `%Y%m%d` format).
    - **Decad**: `hindcast_decad_START_HINDCAST_END_HINDCAST_BASINCODE.csv` (dates in `%Y%m%d` format).

The output format is described in the I/O documentation.




#### Prerequisites

To set up the environment for running the forecast using the conceptual model you have to do following.

1. **Install Required Packages from GitHub**:
   - You need to install two key R packages from GitHub: `airGR_GM` and `airGRdatassim`.
   - First, install `airGR_GM` by following the instructions provided in its GitHub repository: [airGR_GM](https://github.com/hydrosolutions/airGR_GM).
   - Next, install `airGRdatassim` by following the instructions in its GitHub repository: [airGRdatassim](https://github.com/hydrosolutions/airgrdatassim).
   - Ensure that you install `airGR_GM` before `airGRdatassim`.

2. **Install Additional Required Libraries**:
   - The additional R libraries needed are listed in the `requirements.txt` file.
   - To install these libraries, navigate to the `SAPPHIRE_Forecast_Tools/apps` directory:
     ```bash
     cd /SAPPHIRE_Forecast_Tools/apps
     ```
   - Run the following script to install the required packages:
     ```bash
     Rscript install_packages.R
     ```
3. **Check the configuration of the module**


#### I/O

**Input File**

1. Forcing data:

    Control member forcing: Total precipitation in mm/d and temperature in °C. The files must be separate for precipitation and temperature, with filenames and file paths specified in the .env file.

    | date       | P     | code  |
    |------------|-------|-------|
    | 27.08.2023 | 15.37 | 15194 |
    | 28.08.2023 | 21.26 | 15194 |
    | ...        | ...   | ...   |
    | 09.09.2024 | 0.04  | 15194 |


    | date       | T     | code  |
    |------------|-------|-------|
    | 27.08.2023 | 8.39  | 15194 |
    | 28.08.2023 | 4.39  | 15194 |
    | ...        | ...   | ...   |
    | 09.09.2024 | 7.00  | 15194 |

    Ensemble member forcing: The ensemble member forcing must have columns for date, T (temperature in degree Celcius) or P (precipitation in mm/d), and ensemble_member. The basin code is specified in the filename as code_T_ensemble_forecast.csv code_P_ensemble_forecast.csv. The ensemble_member must be a number from 1 to 50. The file path has to be specified in the .env file.

    | date       | T         | ensemble_member |
    |------------|-----------|-----------------|
    | 26.08.2024 | 12.03     | 1               |
    | 27.08.2024 | 13.60     | 1               |
    | ...        | ...       | ...             |
    | 28.08.2024 | 11.69     | 50              |

    Hindcast forcing: This forcing data has the same structure as the control member forcing but includes more historical data. There should be no gaps between the hindcast forcing and the control member forcing data.


2. Discharge data: Discharge is in m3/s

    | code  | date       | discharge |
    |-------|------------|-----------|
    | 15194 | 01.01.2000 | 1.9       |
    | 15194 | 02.01.2000 | 1.9       |
    | ...   | ...        | ...       |
    | 15013 | 04.01.2000 | 1.9       |

3. Basin Info and Parameter

   For each basin (each `code`), a folder is required with the BasinInfo: ieasyhydroforecast_PATH_TO_BASININFO/BASINCODE

    This folder contains the data files `param.RData` and `Basin_Info.RData`.

    - **param.RData**: This file contains the variable `param`, which holds the calibrated parameter values (numeric) required for the specific hydrological model.

    The `Basin_Info.RData` file contains a list with the name Basin_Info with data for the specific basin. The structure of this file is as follows:

    - **BasinCode**: An integer representing the unique code for the basin.

      - Example: `15194`

    - **BasinName**: A string representing the name of the basin.

      - Example: `"AlaArcha"`

    - **BasinArea_m2**: A numeric value representing the area of the basin in square meters.

      - Example: `272532878`

    - **BasinLat_rad**: A numeric value representing the latitude of the basin's centroid in radians.

      - Example: `0.744`

    - **HypsoData**: A numeric vector of hypsometric data (elevation distribution) for the basin in meters above sea level, with each value representing elevation at 1% intervals of the basin area.

      - Example: `c(1531.00, 1684.00, 1751.16, ..., 4753.00)`

    - **MeanAnSolidPrecip**: Vector giving the annual mean of average solid precipitation for each layer [mm/year]

      - Example: `c(361, 361, 361, 361, 361)`

    - **rel_ice**: A numeric vector representing the relative ice coverage in the basin across different elevation bands.

      - Example: `c(0.000000000, 0.000000000, 0.003908274, 0.043461312, 0.078811896)`

    - **GradT**: A data frame containing temperature gradient data, which includes daily and monthly temperature gradients in degrees Celsius per 100 meters (`grad_Tmean`). Each row corresponds to a specific day of the year, with columns for day, month, and the respective temperature gradients.

      - Structure:

        | day | month | grad_Tmean |
        |-----|-------|------------|
        | 1   | 1     | 0.631      |
        | 2   | 1     | 0.632      |
        | ... | ...   | ...        |
        | 366 | 12    | 0.633      |

    - **k_value**: A numeric representing the altitudinal correction factor (`k`) for the precipitation lapse rate in [m-1]

      - Example: `0.00043`


4. Output folder

   For each basin, a folder is required: ieasyhydroforecast_PATH_TO_INITCOND/BASINCODE
   This folder can be empty. In this folder the initial condition for the next forecast are stored.

5. The configuration file defines the settings and parameters required to run hydrological model simulations for the conceptual model with ensemble data assimilation. The configuration file is is stored in the config folder. Filename and path are configured in the .env file (see documentation [here](configuration.md#json-configuration)).

<div style="margin-left: 40px">

```json
#Example for a configuration file for the conceptual model with ensemble data assimilation:
{
  "fun_mod_mapping": {
    "12345": "RunModel_CemaNeigeGR4J_Glacier",
    "13456": "RunModel_CemaNeigeGR6J"
  },
  "Nb_ens": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
  "NbMbr": 2,
  "DaMethod": "PF",
  "StatePert": ["Rout", "Prod", "UH1", "UH2"],
  "eps": 0.65,
  "lag_days": 180,
  "codes": [15194,16936],
  "start_ini": "2010-01-01",
  "end_ini": "2024-01-01",
  "start_hindcast": "2015-12-31",
  "end_hindcast": "2016-01-10",
  "hindcast_mode": "daily"
}
```

</div>

<div style="margin-left: 40px">

Below is a detailed explanation of each key in the configuration file.
   - `fun_mod_mapping`
      - **Type:** Dictionary
      - **Description:** Maps numerical basin codes to specific model functions that will be used in the simulation. Each code corresponds to a different hydrological model.
      - **Example:**
        - `"15194": "RunModel_CemaNeigeGR4J_Glacier"`: This maps the code `15194` to the `RunModel_CemaNeigeGR4J_Glacier` function.
        - `"16936": "RunModel_CemaNeigeGR6J"`: This maps the code `16936` to the `RunModel_CemaNeigeGR6J` function.
   - `Nb_ens`
      - **Type:** List of integers
      - **Description:** Defines the number of ensemble members used from the ECMWF IFS ensemble forecast. The list includes the range of ensemble member numbers from `1` to `50`.
      - **Example:** `[1, 2, 3, ..., 49, 50]` represents ensemble member numbers from `1` to `50`.
   - `NbMbr`
     - **Type:** Integer
     - **Description:** Specifies the total number of ensemble members (`NbMbr`) to be used in the simulation.
     - **Example:** `200` indicates that 200 ensemble members will be utilized.
   - `DaMethod`
     - **Type:** String
     - **Description:** Indicates the data assimilation method used in the simulation.
     - **Example:** `"PF"` specifies that the Particle Filter (`PF`) method will be employed for data assimilation.
   - `StatePert`
     - **Type:** List of strings
     - **Description:** Lists the state variables that will be perturbed during the data assimilation process.
     - **Example:** `["Rout", "Prod", "UH1", "UH2"]` indicates that the state variables `Rout`, `Prod`, `UH1`, and `UH2` will be perturbed.
   - `eps`
     - **Type:** Float
     - **Description:** Fractional error parameter for precipitation and PET of the first-order autoregressive model. Defines the perturbation of the forcing data. It controls the magnitude of perturbation noise.
     - **Example:** `0.65`
   - `lag_days`
     - **Type:** Integer
     - **Description:** Specifies the number of days the model is running with data assimilation process. This parameter is used to define the temporal window of the data assimilation. The model is started before the data assimilation with the initial conditions fomr the previous run.
     - **Example:** `180` indicates a lag of 180 days.
   - `codes`
     - **Type:** List of integers
     - **Description:** Lists the numerical codes corresponding to the basin code for which a forecast is produced. These codes must match those provided in the `fun_mod_mapping`.
     - **Example:** `[15194, 16936]` corresponds to the codes used to map the models `RunModel_CemaNeigeGR4J_Glacier` and `RunModel_CemaNeigeGR6J`.
   - `start_ini`
     - **Type:** String (Date in `YYYY-MM-DD` format)
     - **Description:** Defines the start date of the initialization period for the simulation. Needed for the very first time the model is run for the speicifc basin to get the first initial condition for the operational run. Used only in the script `run_initial.R`
     - **Example:** `"2010-01-01"` indicates the initialization period starts on January 1, 2010.
   - `end_ini`
     - **Type:** String (Date in `YYYY-MM-DD` format)
     - **Description:** Defines the end date of the initialization period for the simulation. Needed for the very first time the model is run for the speicifc basin to get the first initial condition for the operational run. Used only in the script `run_initial.R`
     - **Example:** `"2024-01-01"` indicates the initialization period ends on January 1, 2024.
   - `start_hindcast`
     - **Type:** String (Date in `YYYY-MM-DD` format)
     - **Description:** Specifies the start date of the hindcast period when triggered manually in the script `run_manual_hindcast.R`
     - **Example:** `"2015-12-31"` indicates that the hindcast period begins on December 31, 2015.
   - `end_hindcast`
     - **Type:** String (Date in `YYYY-MM-DD` format)
     - **Description:** Specifies the end date of the hindcast period when triggered manually in the script `run_manual_hindcast.R`
     - **Example:** `"2023-12-31"` indicates that the hindcast period ends on December 31, 2023.
   - `hindcast_mode`
     - **Type:** String
     - **Description:** Defines the mode of the hindcast simulation, such as daily, pentad or decad. Only used when triggered manually in the script `run_manual_hindcast.R`
     - **Example:** `"pentad"` indicates that the hindcast simulation will be conducted in about five-day intervals.
  </div>

**Output Files**
Three output files are generated in the operational run and stored in this path: ieasyhydroforecast_PATH_TO_RESULT/BASINCODE/data
(For each basin the above folder structure is needed.)
- **Daily**: `daily_BASINCODE.csv`
- **Pentad**: `pentad_BASINCODE.csv`
- **Decad**: `decadal_BASINCODE.csv`
When triggering manually a hindcast files are stored in the same directory with the following names:
 - **Daily**: `hindcast_daily_START_HINDCAST_END_HINDCAST_BASINCODE.csv` (dates in `%Y%m%d` format).
 - **Pentad**: `hindcast_pentad_START_HINDCAST_END_HINDCAST_BASINCODE.csv` (dates in `%Y%m%d` format).
 - **Decad**: `hindcast_decad_START_HINDCAST_END_HINDCAST_BASINCODE.csv` (dates in `%Y%m%d` format).



**daily forecast**: daily_BASINCODE.csv
| forecast_date | date       | sd_Qsim   | Q5        | Q10       | ...       | Q50       | ...       | Q90       | Q95       |
|---------------|------------|-----------|-----------|-----------|-----------|-----------|-----------|-----------|-----------|
| 31.07.2024    | 01.08.2024 | 0.2135    | 4.3921    | 4.4378    | ...       | 4.6603    | ...       | 5.0256    | 5.0439    |
| 31.07.2024    | 02.08.2024 | 0.2860    | 4.3972    | 4.4756    | ...       | 4.7365    | ...       | 5.1828    | 5.3473    |
| ...           | ...        | ...       | ...       | ...       | ...       | ...       | ...       | ...       | ...       |
| 31.07.2024    | 14.08.2024 | 1.3577    | 3.8849    | 4.1469    | ...       | 5.1747    | ...       | 7.8351    | 8.0748    |

**pentadal forecast**: pentad_BASINCODE.csv
| forecast_date | Qsim      |
|---------------|-----------|
| 31.07.2024    | 5.0859    |
| ...           | ...       |
| 15.08.2024    | 6.3975    |

**decadal forecast**: decadal_BASINCODE.csv
| forecast_date | Qsim      |
|---------------|-----------|
| 31.07.2024    | 5.0859    |
| ...           | ...       |
| 20.08.2024    | 6.3975    |



#### How to run the tool

1. **Prepare Configuration Files**:
   - Ensure you have the configuration file with the necessary parameters, as described in configuration.md.
   - Also, set up the `.env` file according to the details provided in configuration.md, including all required paths and filenames.
   - For each `code`in the config file it needs the folder:
     -  ieasyhydroforecast_PATH_TO_BASININFO/BASINCODE with `Basin_Info.RData`and `param.RData` (see I/O)
     -  ieasyhydroforecast_PATH_TO_INITCOND/BASINCODE
     -  ieasyhydroforecast_PATH_TO_RESULT/BASINCODE/data

2. **Initial Setup**:
   - Run the `run_initial.R` script to generate the initial conditions required for the model to run in operational mode:
     ```bash
     cd /path/to/your/SAPPHIRE_Forecast_Tools/apps/conceptual_model

     SAPPHIRE_OPDEV_ENV=True Rscript run_initial.R
     ```

3. **Run Operational Forecasting**:
   - After completing the initial setup, run the operational forecasting script:
     ```bash
     cd /path/to/your/SAPPHIRE_Forecast_Tools/apps/conceptual_model

     SAPPHIRE_OPDEV_ENV=True Rscript run_operation_forecasting_CM.R
     ```
   - **Note**: Hindcasts are automatically created in the `run_operation_forecasting_CM.R` script. When setting up for the first time, hindcasts will not be produced because no previous forecasts are saved. In operational mode, the script will subsequently check for gaps between the last run and the current run, filling in any missing forecasts.

4. **Running Multiple Times a Day**:
   - The script can be run multiple times a day, for example, if new discharge data becomes available. Each run will overwrite the forecast output of the previous run on that day.

5. **Triggering Hindcasts for Specific Days**:
   - To trigger hindcasts for specific days, run the `run_manual_hindcast.R` script and define the `start_hindcast`, `end_hindcast`, and `hindcast_mode` parameters:
     ```bash
     cd /path/to/your/SAPPHIRE_Forecast_Tools/apps/conceptual_model

     SAPPHIRE_OPDEV_ENV=True Rscript run_manual_hindcast.R
     ```

### 2.2.6 Machine learning (machine_learning)

#### Description of module

TODO: Sandro, please provide a detailed description of the module so that laypeople understand what happens. Similarly as if you'd give instructions to Copilot to write a script for you.

Please feel free to add any other information that you think is relevant.

The module machine_learning module integrates machine learning models for operational discharge forecasting. Machine Learning models can derive complex relationship between input variables and the target variable (in this case discharge). These data driven models have been in the focus of recent studies and show very powerful performance accross many domains. In our implementation we used deep learning models, such as Temporal-Fusion Transformer (TFT), Time-Series Dense Encoder (TiDE) and Time-Series Mixer (TSMixer). These models have been trained on multiple rivers and there is one model able to make forecast for all rivers trained on (Global Model). We also implemented ARIMA models which are river specific. All models are implemented with the [darts](#https://unit8co.github.io/darts/index.html) library  and are auto-regressive. Hence they use the past observed discharge as an input. They can take other dynamic and static features as inputs. As additional dynamic features the models use the forcing data obtained by the preprocessing_gateway module. As static features the models can use basin features, such as mean elevation, slope and so on. The static features are only used on Global Models.

Folder Structure:

-   machine_learning
    -   make_forecast.py
    -   fill_ml_gaps.py
    -   initialize_ml_tool.py
    -   hindcast_ML_models.py
    -   scr
        -   init.py
        -   predictor_MODELXY.py
        -   utils_ml_forecast.py
    -   requirements.txt

The core part of this module are the predictor_classes. These classes are wrapped around a darts forecasting model (check out the available models [here](#https://unit8co.github.io/darts/generated_api/darts.models.forecasting.html)). In these classes, the whole feature calculation is done and the input variables are scaled with the same scalers the model was trained on. Note that it is extremely important, that the processing of the inputs is done exactly the same as during the training process. Otherwise the model will produce unexpected outputs. Also make sure that the order of the features in the darts.Timeseries creation is the same as during the training, as this can lead to unexpected behaviour aswell. The predictor class should include the following functions to work: get_input_chunk_length, get_max_forecast_horizon, predict. As these functions are called from the other scripts.

The make_forecast.py script is used to perform the operational forecasting. It takes the forcing forecast from the previously run [process_gateway](#preprocessing-of-gridded-weather-data-preprocessing_gateway) module and the past discharge from the [preprocess_runoff](#preprocessing_runoff) module. It calls the predictor_class to make the predictions and appends the newest forecast to the prediction file ../intermediate_data/predictions/MODELXY. The forecast starts from the last observed discharge date plus one day. The past discharge data can contain a certain amount of nan values in the input chunck. Missing values will be interpolated if there is a observed discharge before and after the gap. If there are nan values at the end of the input, the model can fill the gaps with recursive imputation. Here the model predicts the gap and uses these predictions again for the forecast. In the config file it should be specified for which rivers the model performs such a recursive imputation. Also the total threshold of nan values in the input is defined and the threshold for missing values at the end. If it exceeds the threshold the predictions will be nan values.

The hindcast_ML_models.py script is used to perform a hindcast. It reads in the forcing file ../intermediate_data/hindcast_forcing and the ..intermediate_data/control_member_forcing if it is available and produces hindcast for a given model, hindcast mode (Pentad / Decad) and time period. It calls the predictor_class aswell to make the predictions.

When first initalizing the module, we need to provide a csv file, where the forecasts are appended to. This file is created with the initialize_ml_tool.py script. This script calls the hindcast script for a specific model and a time window. It saves the output file in the right location (../intermediate_results/predictions/MODELXY) so that the new forecast can be appended to it. Furthermore running this script ensures that all necessary files are in place for the pentedal and decadal forecast.

The fill_ml_gaps.py is used to fill any gaps in the forecast files. This step is important to guarantee continious predictions from the models to properly monitor their performance. This script checks for gaps in the column "forecast_date" and than calls the hindcast_ML_models.py script to fill in the gaps. In the utils_ml_forecast.py file are various helper functions.

#### Prerequisites

To run this script you need to install the requirements:

``` bash
cd SAPPHIRE_Forecast_Tools/apps/machine_learning

#conda
conda install --file requirements.txt

#or with pip
#pip install -r requirements.txt
```

#### I/O

**Data Structure and Input Files**

As described in the section above, the machine_learning module is wrapped around the predictor_MODELXY class and can integrate any model with such a class. In order to work, each predictor class needs some specific files, such as the model itself, the scalers and if needed static features. Here is a generall outline on how a folder for a Model should look:

-   YOUR_PRIVAT_DATA_FOLDER
    -   config
        -   models_and_scalers
            -   static_features
                -   STATIC_FEATURES.csv
            -   MODEL_DEEP_LEARING
                -   scaler_stats_discharge.csv
                -   scaler_stats_forcing.csv
                -   scaler_stats.static.csv
                -   MODELXY.pt
                -   MODELXY.pt.ckpt
                -   MODELXY.ckpt
            -   MODEL_ARIMA
                -   ARIMA_river1.pkl
                -   ARIMA_river2.pkl .....
                -   ARIMA_riverxy.pkl
                -   arima_params.csv
                -   daily_mean_discharge.csv

Here we have the example of one deep learning based model and of one ARIMA model. In the first folder static_features are the features of the basin saved. This file is needed if you plan to use Global Models such as the TFT, TiDE and TSMixer. It should have the format as follows:

| CODE  | Feature1 | Feature2 | Feature3 | Feature4 | Feature5 | ... | FeatureX |
|-------|----------|----------|----------|----------|----------|-----|----------|
| xxxx1 | 40.0     | 78.0     | 2968.0   | 4152.0   | 1967.0   | ... | 1        |
| xxxx2 | 42.0     | 60.0     | 3405.0   | 4755.0   | 2044.0   | ... | 4        |
| xxxx3 | 42.0     | 80.0     | 3377.0   | 4708.0   | 1874.0   | ... | 2        |
| xxxx4 | 39.0     | 78.0     | 3374.0   | 4622.0   | 1902.0   | ... | 1        |

Note that the CODE here and the code in the forcing data should match.

For Global Deep Learning models such as the TFT, the folder should contain scalers for the discharge, forcing and the static features. The discharge was scaled for each river individually and the mean and standard deviation was calculated for the training period, as we used the Z-score normalization (y = (x - mean) / std). The file should have this format, altough the scaler column is not needed, but rather a reminder of what scaling was used.

|       | mean   | std    | scaler           |
|-------|--------|--------|------------------|
| xxxx1 | 3.933  | 3.537  | StandardScaler() |
| xxxx2 | 10.521 | 6.200  | StandardScaler() |
| xxxx3 | 15.88  | 14.726 | StandardScaler() |
| ....  | ....   | ....   | ....             |

We scaled the forcing over all training basins with the Min-Max scaler. Therefor we calculated the minimum and maximum value per feature during the training period (y = (x - min) / (max - min)). The scaler_stats_forcing should therefore look like this:

|                | min               | max               |
|----------------|-------------------|-------------------|
| P              | 0.0               | 92.9979184632474  |
| T              | -32.4170298339655 | 26.9036446326219  |
| PET            | 0.0               | 5.242174291636997 |
| daylight_hours | 9.02              | 15.33             |
| ....           | ....              | ....              |

The static features were also scaled globally with the Min-Max Scaler. The scaler_stats_static.csv should look like this:

|          | min    | max    |
|----------|--------|--------|
| Feature1 | 2000.0 | 4000.0 |
| Feature2 | 24.2   | 66.1   |
| Feature3 | 0.1    | 0.75   |
| Feature4 | 6.22   | 7.12   |
| ....     | ....   | ....   |

Last but not least is the model itself. The model has to be trained and saved afterwards. Global Models like TFT, TiDE and TSMixer from the darts library are implemented in PyTorch. When using the command model.save("yourmodel.pt") it should automatically create the files needed. It creates a .pt, .pt.ckpt and .ckpt file.

If there are river specific models like the ARIMA, each river has their own model, saved as ARIMA\_{river_code}.pkl. In the implemented ARIMA we don't have scalers but it can be integrated the same way as for the deep learning models. But additionally, the mean discharge for each day of the year is used as an additional feature. This feature is saved as an csv file (daily_mean_discharge.csv). It has the following format:

| Unnamed: 0 | day_of_year | code    | discharge |
|------------|-------------|---------|-----------|
| 0          | 1           | xxxx1.0 | 1.12      |
| 1          | 1           | xxxx2.0 | 10.11     |
| 2          | 2           | xxxx1.0 | 1.23      |
| 3          | 2           | xxxx2.0 | 11.0      |
| ..         | ...         | ....... | ........  |
| 730        | 365         | xxxx1.0 | 1.06      |
| 731        | 365         | xxxx2.0 | 10.11     |

##### Output Files

The hindcast_ML_models.py and make_forecast.py script produce an output with the same format. If the model gives a probabilistic forecast (like TFT, TiDE and TSMixer) the output format looks like this:

| Q5  | Q10  | ...  | Q50 | ...  | Q90  | Q95  | date       | forecast_date | code  |
|-----|------|------|-----|------|------|------|------------|---------------|-------|
| 1   | 1.1  | ...  | 2   | ...  | 3.5  | 4    | 02.01.2020 | 01.01.2020    | xxxx1 |
| 1   | 1.15 | .... | 2.1 | ...  | 3.6  | 4.2  | 03.01.2020 | 01.01.2020    | xxxx1 |
| 1.5 | 2    |      | 3   | .... | 3.8  | 4.9  | 04.01.2020 | 01.01.2020    | xxxx1 |
| ... | ...  | .... | ... | .... | .... | .... | ....       | ....          | ....  |

Where Q5 represents the 5% - quantile and the quantiles are saved every 5 steps. The date is the time for which the discharge is predicted and the forecast_date is the date, when the forecast was produced.

For a deterministic forecast the format looks almost the same, expect the there is only one predicted discharge with the column name = "Q".

The name convention for the prediction file is pentad_MODEL_forecast.csv and for the decad forecast decad_MODEL_forecast.csv. The most recent forecast is always added to the past forecasts and hindcast file.

#### How to run the tool

If you have a running predictor class for a model, the other script should work with that class. The model and the prediction mode ca be specified in the config file (SAPPHIRE_MODEL_TO_USE, SAPPHIRE_PREDICTION_MODE). The config file also handles for which stations the machine_learning module should produce forecasts (ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts) and where the model, scalers and static features are saved (ieasyhydroforecast_PATH_TO_SCALER_YOURMODEL). Also the number of allowed nan values in the input and at the end of the input is defined (ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END, ieasyhydroforecast_THRESHOLD_MISSING_DAYS_YOURMODEL).

**NOTE:** Currently in the code it raises an Error if the model is not one of these ARIMA, TFT, TIDE, TSMIXER. If a new model is added, this part of the code has to be adjusted or the excepted models need to be specificed in the config file.

To initialize the machine_learning module you first need to initialize the forcing data to obtain the data needed to generate your hindcasts. To run the initialize_ml_tool.py you can run the following command:

``` bash
SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT python initialize_ml_tool.py
```

this will than ask you for a start and end date which define the period for the hindcast. Be sure that you have discharge and forcing data for that period. This data might contain nan values which just induces nan as an output. This script can take quiet some time to run, especially if you want to produce hindcast over a long time period for many rivers.

During operational forecasting the make_forecast.py script has to be called:

``` bash
SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python make_forecast.py
```
NOTE: The operational forecast uses always the last available discharge date to predict the days afterwards. So if the last date in the runoff_day.csv is for example the 17.03.2024, the prediction will be made for the 18.03.2024 onwards, regardless of what date today is. So there might be entries in the file where the forecast_date is greater than the date itself. This cases should be handled seperatly in the post-processing. (TODO: Check if this approach is valid or we just delete all rows in the file where date < forecast_date)

Afterwards the fill_ml_gaps.py will be called, this script will produce a hindcast if there is a day without a forecast (Gap in forecast_date > 1)

``` bash
SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python fill_ml_gaps.py
```

### 2.2.7 Post-processing of forecasts (postprocessing_forecasts)

TODO: Bea

### 2.2.8 Manual triggering of the forecast pipeline

To re-run a forecast (for example to include river runoff data that was not available at the time of the forecast), you can manually trigger the forecast pipeline. This process includes the re-setting of the last successful run date of the linear regression module to the day before the last forecast date. This is done with the module reset_forecast_run_date.

#### How to re-run the forecast pipeline manually

To do so, you can run the following sequence of commands in the terminal:

Pull the latest image from Docker Hub (if not yet available on your server):

``` bash
docker pull mabesa/sapphire-rerun:latest
```

Then we run the reset_forecast_run_date module:

``` bash
nohup bash bin/rerun_latest_forecasts.sh <ieasyhydroforecast_data_root_dir> > rerun.log 2>&1 &
```

Which will reset the last successful run date of the linear regression module to the day before the last forecast date, remove the necessary containers from the last forecast and run the forecast pipeline again.

nohup is used to run the command in the background and rerun.log is used to store the output of the command. The output of the command is stored in the rerun.log file. The 2\>&1 redirects the standard error output to the standard output. This way, all output is stored in the rerun.log file. & runs the command in the background so you can continue to use the terminal after starting the process.

You can check up on the progress of your forecast by running the following command in the terminal:

``` bash
tail -f rerun.log
```

to read the output of the command in the terminal and

``` bash
docker ps -a
```

to check the status of the docker containers.

To inspect individual docker container logs you type:

``` bash
docker logs <container_id>
```

where <container_id> is the id of the container you want to inspect. You can find the container id by running the docker ps -a command.

### 2.2.9 Forecast dashboard

#### Prerequisites 
The forecast dashboard is implemented in python using the panel framework. As for the backend development, we recommend the use of a Python IDE and conda for managing the Python environment. Please refer to the instructions above should you require more information on how to install these tools.

If you have already set up a python environment for the forecast dashboard, you can activate it by running the following command in the terminal and skipp the installation of python_requirements.txt:

``` bash
conda activate my_environment
```

#### How to run the forecast dashboard locally

To run the forecast dashboard locally, navigate to the apps/forecast_dashboard folder and run the following command in the terminal:

``` bash
ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port <port number>
```

Currently available options for the `sapphire_forecast_horizon` are `pentad` and `decad` to display forecasts for pentadal and decadal forecast horizons. If you specify `sapphire_forecast_horizon=pentad`, the dashboard will display the pentadal forecast. If you specify `sapphire_forecast_horizon=decad`, the dashboard will display the decadal forecast.    

The options --show, --autoreload, and --port <port number> are optional. Please replace <port number> with a number of your choice. The default port is 5006. If you do not specify a port number, the dashboard will be displayed on port 5006.

The show and autoreload options open your devault browser window (we used chrome) at <http://localhost:<port number>/forecast_dashboard> and automatically reload the dashboard if you save changes in the file forecast_dashboard.py. The port option tells you on which port the dashboard is being displayed. Should your port of choice be already occupied on your computer, you can change the number. You can then select the station and view predictors and forecasts in the respective tabs.

#### How to test the dashboard containers locally
From the project root, run: 

``` bash
bash bin/daily_update_sapphire_frontend.sh <path/to/your>/.env_develop_kghm
```

This will download the latest image and run the conatiners for pentadal and decadal dashboards. 

## 2.3 The backend (note: this module is deprecated)

The backend consists of a set of tools that are used to produce forecasts. They are structured into:

-   Pre-processing:
    -   pre-processing of river runoff data: This component reads daily river runoff data from excel files and, if access is available, from the iEasyHydro database. The script is intended to run at 11 o'clock every day. It therefore includes daily average discharge data from all dates prior to today and todays morning measurement of river runoff. The data is stored in a csv file.
    -   pre-processing of forcing data: Under development.
-   forecast models:
    -   linear regression: This component reads the pre-processed river runoff data and builds a linear regression model for each station. The model is updated each year with the data from the previous year. The model is used to forecast the river runoff for the next 5 or 10 days. The forecast is stored in a csv file.
    -   LSTM: Under development.
    -   Conceptual hydrological models: Under development.
-   Post-processing:
    -   post-processing of forecasts: Under development.
-   iEasyHydroForecast: A helper library that contains functions used by the forecast tools. The library is used to read data from the iEasyHydro database and to write bulletins in a similar fashion as the software iEasyHydro.

#### Prerequisites

You will need a Python IDE for development. If you do not alreay have one installed, we recommend the use of Visual Studio Code for developping the backend. The installation instructions can be found [here](https://code.visualstudio.com/download). You will need to install the Python extension for Visual Studio Code. The installation instructions can be found [here](https://code.visualstudio.com/docs/languages/python).

We use conda for managing the Python environment. The installation instructions can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/). Once conda is installed, you can create a new conda environment by running the following command in the terminal:

``` bash
conda create --name my_environment python=3.11
```

Through the name tag you can specify a recognizable name for the environment (you can replace my_environment with a name of your choosing). We use a different environment for each module of the backend. For development, python 3.10 and 3.11 was used. We therefore recommend you continue development with python 3.10 or 3.11 as well. You can activate the environment by running the following command in the terminal:

``` bash
conda activate my_environment
```

The name of your environment will now appear in brackets in the terminal.

We show how to proceed with each module based on the example of the preprocessing_runoff tool. The procedure is the same for all modules.

Install the following packages in the terminal (note that this will take some time):

``` bash
cd apps/preprocessing_runoff
pip install -r requirements.txt
```

The backend can read data from excel and/or from the iEasyHydro database (both from the online and from the local version of the software). If you wish to use the iEasyHydro database, you will need to install the iEasyHydro SKD library. More information on this library that can be used to access your organizations iEasyHydro database can be found [here](https://github.com/hydrosolutions/ieasyhydro-python-sdk). Some tools require the the library [iEasyReports](https://github.com/hydrosolutions/ieasyreports) that allows the backend of the forecast tools and the forecast dashboards to write bulletins in a similar fashion as the software iEasyHydro. And finally you will need to load the iEasyHydroForecast library that comes with this package. You will therefore further need to install the following packages in the terminal:

``` bash
pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
pip install git+https://github.com/hydrosolutions/ieasyreports.git@main
pip install -e ../iEasyHydroForecast
```

If you wish to use data from your organizations iEasyHydro database, you will need to configure the apps/config/.env_develop file (see [doc/configuration.md](configuration.md) for more detailed instructions). We recommend testing your configuration by running a few example queries from the [documentation of the SDK library](https://github.com/hydrosolutions/ieasyhydro-python-sdk) in a jupyter notebook.

#### How to run the backend modules locally {#how-to-run-the-backend-modules-locally}

##### Pre-processing of river runoff data {#pre-processing-of-river-runoff-data}

Establish a connection to the iEasyHydro database by configuring the apps/config/.env_develop file (see [doc/configuration.md](configuration.md) for more detailed instructions). You might require an ssh connection to your local iEasyHydro installation, consult your IT admin for this. You can then run the pre-processing of river runoff data tool with the default .env_develop file by running the following command in the preprocessing_runoff folder in the terminal:

``` bash
python preprocessing_runoff.py
```

Note, we use different .env files for testing and development. We use an environment variable to specify a .env file we use for testing purposes (SAPPHIRE_TEST_ENV, see chapter on testing below) and we use one for development with private data (SAPPHIRE_OPDEV_ENV). During development, we typically use the command:

``` bash
SAPPHIRE_OPDEV_ENV=True python preprocessing_runoff.py
```

##### Pre-processing of forcing data from the data gateway {#pre-processing-of-forcing-data-from-the-data-gateway}

##### Running the linear regression tool {#running-the-linear-regression-tool}

Edit the file apps/internal_data/last_successful_run.txt to one day before the first day you wish to run the forecast tools for. For example, if you wish to start running the forecast tools from January 1, 2024, write the date 2023-12-31 as last successful run date. You can then run the forecast backend in the offline mode to simulate opearational forecasting in the past by running the following command in the terminal:

``` bash
python run_offline_mode.py
```

This will run the linear regression tool for the period of January first 2024 to the current day. You can change the dates to your liking but make sure that you have data available for the production of the linear regression models and for forecasting. Currently, the tools assume that the data availability starts on January 1 2000. The tool will write the results to the file *ieasyforecast_results_file*, apps/internal_data/forecasts_pentad.csv. If you have daily data available from 2000 to the present time, we recommend starting the forecast from 2010 onwards. This gives the backend tool 10 years of data (from 2000 to 2010) to build a linear regression model. Each year, the linear regression model will be updated with the data from the previous year. That means, the parameters of the linear regression model $y=a \cdot x+b$ will change each year. If you are interested in the model parameters, they are provided in the *ieasyforecast_results_file*, apps/internal_data/forecasts_pentad.csv.

For development, it may be useful to use a different .env file. We use an environment variable to specify a .env file we use for testing purposes (SAPPHIRE_TEST_ENV, see chapter on testing below) and we use one for development with private data (SAPPHIRE_OPDEV_ENV).

## Dockerization

You can dockerize each module on your local machine or on a server using Github Actions. The Dockerfiles to package each module are located in the apps/module_name folder. The docker-compose.yml file runs the entire containerized workflow and is located in the bin folder.

### Configuration dashboard

Note that at the time of writing, a Docker base image with R and RShiny is not available for the ARM architecture (the latest Mac processors). The configuration dashboard has, with the current setup, been dockerized in Ubuntu.

The forecast dashboard is dockerized using the Dockerfile in the apps/configuration_dashboard folder. To build the docker image locally, run the following command in the root directory of the repository:

``` bash
docker build --no-cache -t station_dashboard -f ./apps/configuration_dashboard/dockerfile .
```

Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.

``` bash
docker run -e "IN_DOCKER_CONTAINER=True" \
    -v <full_path_to>/config:/app/apps/config \
    -v <full_path_to>/data:/app/data \
    -p 3647:3647 \
    --name station_dashboard_container station_dashboard
```

### Backend

The backend is dockerized using the Dockerfile in the apps/backend folder. Dockerization has been tested under both Ubuntu running on Windows or Mac OS operating systems. To build the docker image locally, run the following command in the root directory of the repository:

``` bash
docker build --no-cache -t preprocessing_runoff -f ./apps/preprocessing_runoff/Dockerfile .
```

Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.

``` bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/data:/app/data -v <full_path_to>/apps/internal_data:/app/apps/internal_data -p 9000:8801 --name preprocessing_runoff preprocessing_runoff
```

### Forecast dashboard

The forecast dashboard is dockerized using the Dockerfile in the apps/forecast_dashboard folder. To build the docker image locally, run the following command in the root directory of the repository:

``` bash
docker build --no-cache -t forecast_dashboard -f ./apps/forecast_dashboard/Dockerfile .
```

Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.

``` bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/data:/app/data -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/apps/internal_data/:/app/apps/internal_data -p 5006:5006 --name fcboard forecast_dashboard
```
You might have to add a few additional environment variables to the command above. Please refer to the file `bin/docker-compose-dashboards.yml` for an up-to-date list of the environment variables used to run the forecast dashboard.

Make sure that the port 5006 is not occupied on your computer. You can change the port number in the command above if necessary but you'll have to edit the port exposed in the docker file and edit the panel serve command in the dockerfile to make sure panel renders the dashboards to your desired port.

You can now access the dashboard in your browser at <http://localhost:5006/forecast_dashboard> and review it's functionality.

## How to use private data

If you want to use private data for the development of the forecast tools, you can do so by following the instructions below. We recommend tht you use a differenet .env file for development with private data. We use an environment variable to specify a .env file we use for testing purposes (SAPPHIRE_TEST_ENV, see chapter on testing below) and we use one for development with private data (SAPPHIRE_OPDEV_ENV). To make use of the SAPPHIRE_OPDEV_ENV environment variable, you store your environment in a file named .env_develop_kghm in the folder ../sensitive_data_forecast_tools/config (relative to this projects root folder). The folder ../sensitive_data_forecast_tools should contain the following sub-folders: - bin - config - daily_runoff - GIS - intermediate_data - reports - templates

## Development workflow

Development takes place in a git branch created from the main branch. Once the development is finished, the branch is merged into the main branch. This merging requires the approval of a pull requrest by a main developer. The main branch is tested in deployment mode and then merged to the deploy branch. 3rd party users of the forecast tools are requested to pull the tested deploy branch. The deployment is done automatically using GitHub Actions. The workflow instructions can be found in .github/workflows/deploy\_\*.yml.

## Testing

Testing tools are being developed for each tool. This is work in progress.

To run all tests, navigate to the apps directory in your terminal and type the following command:

``` bash
SAPPHIRE_TEST_ENV=True python -m pytest -s
```

SAPPHIRE_TEST_ENV=True defines an environment variable TEST_ENV to true. We use this environment variable to set up temporary test environments. The -s is optional, it will print output from your functions to the terminal.

To run tests in a specific file, navigate to the apps directory in your terminal and type the following command:

``` bash
SAPPHIRE_TEST_ENV=True python -m pytest -s tests/test_file.py
```

Replace test_file.py with the name of the file you want to test. To run tests in a specific function, navigate to the apps directory in your terminal and type the following command:

``` bash
SAPPHIRE_TEST_ENV=True python -m pytest -s tests/test_file.py::test_function
```

Replace test_function with the name of the function you want to test.

# Deployment

GitHub Actions are used to automatically test the Sapphire Forecast Tools and to build and pull the Docker images to Docker Hub. From there, the images can be pulled to a server and run. To install or update the forecast tools on a server, please follow the instructions in [doc/deployment.md](deployment.md).
