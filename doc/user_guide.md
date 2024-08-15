<h1>User Guide</h1>

This user guide provides a general overview of the SAPPHIRE Forecast Tools and user instructions. Mode detailed information about how the modules are implemented can be found in the [development guide](development.md) and installation instructions are provided in the [deployment guide](deployment.md).

The structure of this document is as follows:
<!-- How to add TOC: Install Extension Markdown All in One, then press Cmd+Shift+P to select Markdown All in One: Create Table of Contents -->

- [What are the SAPPHIRE Forecast Tools](#what-are-the-sapphire-forecast-tools)
  - [Overview](#overview)
  - [Important user information, limitations and special use cases](#important-user-information-limitations-and-special-use-cases)
  - [Installation](#installation)
  - [Development](#development)
- [Input data](#input-data)
  - [Daily discharge data](#daily-discharge-data)
  - [Shape file layers of the area of interest](#shape-file-layers-of-the-area-of-interest)
  - [Templates for the forecast bulletins](#templates-for-the-forecast-bulletins)
- [Output data](#output-data)
  - [Forecast bulletins](#forecast-bulletins)
  - [Internal output used for visualization of forecasts](#internal-output-used-for-visualization-of-forecasts)
- [The modules of the SAPPHIRE Forecast Tools](#the-modules-of-the-sapphire-forecast-tools)
  - [Forecast configuration](#forecast-configuration)
  - [Backend](#backend)
    - [Pre-processing runoff data (preprocessing\_runoff)](#pre-processing-runoff-data-preprocessing_runoff)
    - [Pre-processing publicly available meteorological data (preprocessing\_gateway)](#pre-processing-publicly-available-meteorological-data-preprocessing_gateway)
    - [Forecasting using linear regression models (linear\_regression)](#forecasting-using-linear-regression-models-linear_regression)
    - [Forecasting using conceptual rainfall-runoff models (conceptual\_rainfall\_runoff)](#forecasting-using-conceptual-rainfall-runoff-models-conceptual_rainfall_runoff)
    - [Forecasting using machine learning models (machine learning)](#forecasting-using-machine-learning-models-machine-learning)
    - [Post-processing of forecasts (postprocessing\_forecasts)](#post-processing-of-forecasts-postprocessing_forecasts)
    - [Pipeline manager for the modules (pipeline)](#pipeline-manager-for-the-modules-pipeline)
  - [Forecast dashboard](#forecast-dashboard)
    - [Predictor tab](#predictor-tab)
    - [Forecast tab](#forecast-tab)
  - [Forecast horizons](#forecast-horizons)



# What are the SAPPHIRE Forecast Tools
The SAPPHIRE Forecast Tools are a collection of software components that are used to produce forecasts of streamflow. The tools are designed to be used by hydromet organizations that have access to a database of historical discharge data and a database of meteorological forecasts through the software [iEasyHydro](www.ieasyhydro.org) and through the [SAPPHIRE data gateway]()(TODO insert link once repo is public) but the reduced demo version of the tools also work with data read from excel documents. The tools are written in Python and R and are deployed using the Docker system. The tools are designed to be run on a server and to be accessed through a web browser.

## Overview
Forecasts are produced in two steps:
1. At the beginning of the forecast season, select the stations for which forecasts are to be produced. This is done using the [forecast configuration dashboard](#forecast-configuration). The dashboard is accessed by double-clicking on the station configuration icon on your desktop. The dashboard is currently only available in Russian language.
You may configure the Forecast Tools to generate excell documents with the forecasts for each station. These documents are similar to the documents produced by the Kyrgyz Hydrometeorological Services. Please note that the writing of these forecast sheets takes some time. We therefore recommend to use this option only during the validation phase of the forecast tools.

<p align="center"><img src="www/Station.png" alt="config_icon" width="50"/></p>

1. The [forecast backend](#backend) consists of multiple modules which are automatically run every day at a given time (typically 10 a.m.). The tool reads the discharge data from the iEasyHydro database and/or from a local folder. The tool then produces forecasts for the selected stations and writes the results to forecast bulletins. Depending on the model, daily, pentadal and decadal operational runoff forecasts are available. Unless you configure the tool differently, you will find the forecast bulletins under SAPPHIRE_Forecast_Tools/data/bulletins/. The bulletins are written in follow a template that the user provides (see data/templates/pentad_forecast_bulletin_template.xlsx for an example for a template). Terms in the template bulletin indicated by {{}} are replaced by values by the linear regression tool. The following table shows which models and forecast horizons can be used in the forecasting tools:
   - Linear regression models (available in demo version): pentadal and decadal forecasts
   - Conceptual rainfall-runoff models: daily, pentadal and decadal forecasts
   - Machine learning models: daily, pentadal and decadal forecasts

2. [The forecast dashboard](#forecast-dashboard) reads the forecast results file and visualizes the forecasts. You can access the forecast dashboard by double-clicking on the forecast dashboard icon on your desktop. The dashboard can be configured to run in Russian or English.

<p align="center"><img src="www/Pentad.png" alt="pentad_icon" width="50"/></p>


## Important user information, limitations and special use cases
- Currently, only pentadal forecasts are implemented, follwowing the method currently employed by Kyrgyz Hydromet. Further forecast horizons and forecast methods will be implemented in the coming months and years.
- We assume that discharge stations start with the character '1'. This is currently hard-coded in the software. If your station codes do not start with '1', please contact us.
- To save runtime, the current implementation checks the iEasyHydro database for new predictor data only after January 2020. If you need to change this, edit the date in the file apps/backend/src/data_processing.py in the section getting predictor.
- Special use cases, like the discharge for virtual reservoirs are currently hardcoded in the backend. This affects station code 16936. If required, this code section with the special case can be commented in the files apps/backend/src/data_processing.py and apps/backend/src/forecasting.py.

## Installation
For installation instructions, please refer to the [deployment guide](deployment.md).

## Development
For development instructions, please refer to the [development guide](development.md).

# Input data
TODO: Update this section, differentiate between demo version and full version

The SAPPHIRE Forecast Tools require the following input files to be available which will be further described in the linked or following sections:
- A complete configuration under apps/config (see [doc/configuration.md](configuration.md) for more detailed instructions)
- Either access to daily discharge data as excel files in data/daily_discharge and/or access to the iEasyHydro database (either the online or the locally installed version of the software)
- Shape file layers of administrative boundaries in the area of your interest in data/GIS
- Templates for the forecast bulletins in data/templates
Examples of these files are provided in the repository. You can use them as a template for your own configuration.

## Daily discharge data
Daily discharge data for the stations for which forecasts are to be produced. The data must be in the iEasyHydro database or in a local folder. Assuming the iEasyHydro contains operational data and the excel sheets contain data validated by the regime departement, precedence is given to data read from the excel sheets should both data sources be available and overlaps occur. The daily data can be read from 2 formats:
- One excel document per station with excel sheets for each year with dates in the first column and discharge values in m3/s in the second column. We assume that each sheet has a header row. The excel files must be named with the station code (a 5-digit integer) followed by an underscore and then any name. For example: 12345_river_styx_2000-2020.xlsx (see data/daily_discharge_data for an example).
- Excel documents with daily river runoff data in single sheets for each station. The format of these excel files is as follows. For each river, a header column, starting with the hydropost code must be present in cell A1. A second header column indicates the date and the river runoff columns. The expected date format is %d.%m.%Y and river runoff is assumed to be in cubic meters per second. Missing values of river runoff can be indicated either by blank cells or by '-'. Files containing multiple rivers daily runoff data must not start with a digit but with a character.

Data from all excel files in the daily discharge data folder are read by the preprocessing_runoff module of the SAPPHIRE Forecast Tools and used for forecasting.



## Shape file layers of the area of interest
Shape file layers of the area of interest in the folder data/GIS. The shape files must be in the WGS84 projection. Typically this will be the shape files of the administrative boundaries of the country. Please make sure to make available shp, shx, dbf and prj files.

## Templates for the forecast bulletins
Templates for the forecast bulletins in the folder data/templates. The templates must be in the xlsx format. The templates can contain several sheets but only the first sheet of the bulletin template is used by the forecast tools to write to. The same logic is used for the bulletin template as in the iEasyHydro software. Terms in the template bulletin indicated by {{}} are replaced by values by the linear regression tool. Please see the list in [bulletin_template_tags.md](bulletin_template_tags.md) for a list of available tags and use the available templates as reference.

Please note that the template files available in this repository are currently only available in Russian language. They assume , as decimal separator and space as thousands separator. If you use different separators, calculations programmed in the excel file will not work and you will have to adjust the template or your language settings (File > Options > Advanced for Windows or Excel > Preferences > Edit for Mac).



# Output data
A successful run of the SAPPHIRE Forecast Tools will produce the forecast bulletins in the .xlsx format and visualizations of the forecasts and forecast errors in the forecast dashboard. The bulletins are written to the folder data/bulletins. The visualizations are produced in the forecast dashboard and can be exported as .png files.

## Forecast bulletins
- A forecast bulletin for each forecast horizon and station in the folder data/bulletins
- If the option is selected in the configuration dashboard, an excel sheet for each station in the folder data/pentadal_forecasts containing the linear regression forecasts as traditionally produced by the Kyrgyz Hydrometeorological Services

## Internal output used for visualization of forecasts
Intermediate results for visualization on the forecast dashboard:
- Daily discharge data for each station in the folder apps/internal_data/hydrographs_day.pkl
- Pentadal discharge data for each station in the folder apps/internal_data/hydrographs_pentad.pkl
- A csv file with the forecasts for each station in the folder apps/internal_data/forecasts_pentad.csv. This file also contains the parameters of the linear regression model.
Please note that these internal files should not be edited manually by the user. You are, however, welcome to copy them to your local machine for further analysis.


# The modules of the SAPPHIRE Forecast Tools
According to the 3 steps described in the [overview](#overview), the SAPPHIRE Forecast Tools consist of the following modules:
- The [forecast configuration dashboard](#forecast-configuration) is used to select the stations for which forecasts are to be produced. The dashboard is accessed by double-clicking on the station configuration icon on your desktop. The dashboard is currently only available in Russian language.
- The [backend modules](#backend) are used to produce the forecasts. The backend consists of multiple modules which take care of pre-processing data from various sources, producing forecasts and writing the results to forecast bulletins. The following main modules are available in the backend:
  - Preprocessing runoff data (preprocessing_runoff)
  - Preprocessing publicly available meteorological data (preprocessing_station_forcing)
  - Preprocessing meteorological data from the iEasyHydro database (preprocessing_gateway)
  - Forecasting using linear regression models (linear_regression)
  - Forecasting using conceptual rainfall-runoff models (conceptual_rainfall_runoff)
  - Forecasting using machine learning models (machine learning)
  - Post-processing of forecasts (postprocessing_forecasts)
  - Pipeline manager for the modules (pipeline)
- The [forecast dashboard](#forecast-dashboard) is used to visualize the forecasts. The dashboard is accessed by double-clicking on the forecast dashboard icon on your desktop. The dashboard can be configured to run in Russian or English.

TODO: Insert a simplified flowchart of the modules

These modules are described in the following sections. For detailed information about how the modules are implemented, please refer to the [development guide](development.md).

## Forecast configuration
Once deployed, the forecast dashboard is accessed by double-clicking on the forecast dashboard icon on your desktop. This dashboard is available in Russian language only.
The forecast configuration dashboard is used to select the stations for which forecasts are to be produced. The dashboard is accessed by double-clicking on the station configuration icon on your desktop. The dashboard is currently only available in Russian language. Detailed user instructions are available by clicking on the help button in upper right corner of the dashboard window.

## Backend

### Pre-processing runoff data (preprocessing_runoff)
The module reads daily discharge data from the iEasyHydro database and/or from a local folder. The data is read from the iEasyHydro database using the iEasyHydro API. The data is read from the local folder as described in the section [daily discharge data](#daily-discharge-data). The module reads the data and writes it to a time series data and hydrograph data to files in the folder apps/internal_data. The data is then used by the forecasting modules to produce forecasts.

### Pre-processing publicly available meteorological data (preprocessing_gateway)
The module reads meteorological data from the SAPPHIRE data gateway. The data is read from the gateway using the gateway API. The data is read and written to a time series data file in the folder apps/internal_data. The data is then used by the forecasting modules to produce forecasts. The demo version of the forecast tools works without this module.

TODO: Sandro, please review the section above

### Forecasting using linear regression models (linear_regression)
The module reads the daily river runoff data processed in the preprocessing_runoff module and produces forecasts of the average runoff for the next pentad or for the next decad using the linear regression method. Thereby, a linear relationship between predictors and forecast variables is established for each pentad and decad of the year using past observations of river runoff. These linear relationsips are subsequently used to produce operational forecasts or river runoff based on recent runoff measurements. See for example [Wikipedia](https://en.wikipedia.org/wiki/Linear_regression) for a more detailed description of the linear regression method. Currently, the predictor for pentadal forecasts consists of the sum of the daily average discharge of the last 3 days and the predictor for the decadal forecasts consists of the average discharge of the last decade. The linear regression models are the current state-of-the-art method used by Central Asian Hydrometerological Services to produce operational forecasts.

The linear regression models are re-created operationally based on the input data. We can therefore provide this module in the demo version of the Forecast Tools.

TODO: Add a section on linear regression to our course book and link it here

### Forecasting using conceptual rainfall-runoff models (conceptual_rainfall_runoff)
Conceptual rainfall runoff models are a simplified numerical representation of the main hydrological processes in a basin. The forecast skill of conceptual rainfall-runoff models is improved through the assimilation of measured runoff data. Conceptual models assimilating runoff data are the current state-state-of-the-art river runoff forecasting method by European Hydrometeorological Services. Conceptual models can be linked to the SAPPHIRE Forecast Tools to produce daily, pentadal and decadal forecasts. Because of the complexity of the models, we can not currently provide a conceptual model in the demo version of the Forecast Tools.

TODO: Adrian, please review the section above. We might link to the course book or to your vignette here.

### Forecasting using machine learning models (machine learning)
Machine learning models are a set of algorithms that can learn from and make predictions on data. They are currently at the center of research for operational hydrological forecasting. Machine learning models can be linked to the SAPPHIRE Forecast Tools to produce daily, pentadal and decadal forecasts. Because of the complexity of the models, we can not currently provide a machine learning model in the demo version of the Forecast Tools.

TODO: Sandro, please review the section above. You might want to add a couple of sentences that describe generally how machine learning models work and maybe provide a link to a general description of machine learning models.

### Post-processing of forecasts (postprocessing_forecasts)
TODO

### Pipeline manager for the modules (pipeline)
TODO

## Forecast dashboard
Once deployed, the forecast dashboard is accessed by double-clicking on the forecast dashboard icon on your desktop. The dashboard can be configured to display Russian or English language (see [doc/configuration.md](configuration.md)). Please note that upon reboot of the computer, the dashboard may not display correctly from the start. In this case, please wait a second and reload the browser window.
The forecast dashboard is used to visualize the forecasts. The dashboard is accessed by double-clicking on the forecast dashboard icon on your desktop. The dashboard can be configured to run in Russian or English. The current version of the dashboard has the following features:
- A predictor tab that allows to visualize the predictor data for the station selected in the side pane. The predictor data is read from the iEasyHydro database or from the excel sheets.
- A forecast tab that visualizes the hydrograph with the forecasted discharge for the station selected in the side pane. The forecast is produced by the forecast tools based on past discharge data using the linear regression method.
The tabs are described in more detail in the following sections.

### Predictor tab
In the current version of the software, the predictor tab shows

### Forecast tab
The method to calculate the forecast range can be selected in the side pane. Following options are available:
a) The forecast range is calculated based on 0.674 times the standard deviation of the observed discharge data.
b) The forecast range is plus/minus a manually selected percentage of the forecasted discharge.



## Forecast horizons
The following forecast horizons are available in the forecast tools:
- Daily forecasts (for conceptual rainfall-runoff models and machine learning models)
- Pentadal forecasts (for linear regression models, conceptual rainfall-runoff models and machine learning models)
- Decadal forecasts (for linear regression models)

Note that a pentad is a period of 5 days and a decadal is a period of 10 days. The last pentad and decad of each month has variable length, depending on the number of days in the month.



