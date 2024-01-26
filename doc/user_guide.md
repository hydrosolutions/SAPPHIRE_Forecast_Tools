# User Guide

## Introduction
The SAPPHIRE Forecast Tools are a collection of software components that are used to produce forecasts of streamflow. The tools are designed to be used by hydromet organizations that have access to a database of historical discharge data and a database of meteorological forecasts through the software [iEasyHydro](www.ieasyhydro.org) but the demo version of the tools also work with data read from excel documents. The tools are written in Python and R and are deployed using the Docker system. The tools are designed to be run on a server and to be accessed through a web browser.

## Overview
Forecasts are produced in two steps:
1. At the beginning of the forecast season, select the stations for which forecasts are to be produced. This is done using the forecast configuration dashboard. The dashboard is accessed by double-clicking on the station configuration icon on your desktop. The dashboard is currently only available in Russian language.
You may configure the Forecast Tools to generate excell documents with the forecasts for each station. These documents are similar to the documents produced by the Kyrgyz Hydrometeorological Services. Please note that the writing of these forecast sheets takes some time. We therefore recommend to use this option only during the validation phase of the forecast tools.

<p align="center"><img src="www/Station.png" alt="config_icon" width="50"/></p>

2. The forecast backend automatically runs every day at a given time (typically 10 a.m.). The tool reads the discharge data from the iEasyHydro database and/or from a local folder. The tool then produces forecasts for the selected stations and writes the results to forecast bulletins. Unless you configure the tool differently, you will find the forecast bulletins under SAPPHIRE_Forecast_Tools/data/bulletins/. The bulletins are written in Russian language and follow a template that the user provides (see data/templates/pentad_forecast_bulletin_template.xlsx for an example for a template). Terms in the template bulletin indicated by {{}} are replaced by values by the linear regression tool.

3. The forecast dashboard reads the forecast results file and visualizes the forecasts. You can access the forecast dashboard by double-clicking on the forecast dashboard icon on your desktop. The dashboard can be configured to run in Russian or English.

<p align="center"><img src="www/Pentad.png" alt="pentad_icon" width="50"/></p>

# Important user information
- Currently, only pentadal forecasts are implemented, follwowing the method currently employed by Kyrgyz Hydromet. Further forecast horizons and forecast methods will be implemented in the coming months and years.
- We assume that discharge stations start with the character '1'. This is currently hard-coded in the software. If your station codes do not start with '1', please contact us.
- To save runtime, the current implementation checks the iEasyHydro database for new predictor data only after January 2020. If you need to change this, edit the date in the file apps/forecast_backend/forecast_backend.py in the section getting predictor.
- Special use cases, like the discharge for virtual reservoirs are currently hardcoded in the backend. This affects station code 16936. If required, this code section with the special case can be commented in the file apps/forecast_backend/forecast_backend.py.

# Input data
The SAPPHIRE Forecast Tools require the following input files to be available which will be further described in the linked or following sections:
- A complete configuration under apps/config (see [doc/configuration.md](doc/configuration.md) for more detailed instructions)
- Either access to daily discharge data as excel files in data/daily_discharge and/or access to the iEasyHydro database (either the online or the locally installed version of the software)
- Shape file layers of administrative boundaries in the area of your interest in data/GIS
- Templates for the forecast bulletins in data/templates
Examples of these files are provided in the repository. You can use them as a template for your own configuration.

## Daily discharge data
Daily discharge data for the stations for which forecasts are to be produced. The data must be in the iEasyHydro database or in a local folder. Assuming the iEasyHydro contains operational data and the excel sheets contain data validated by the regime departement, precedence is given to data read from the excel sheets should both data sources be available and overlaps occur. The daily data must be in the format of one excel document per station with excel sheets for each year with dates in the first column and discharge values in m3/s in the second column. The excel files must be named with the station code followed by an underscore and then any name. For example: 12345_river_styx_2000-2020.xlsx (see data/daily_discharge_data for an example).

## Shape file layers of the area of interest
Shape file layers of the area of interest in the folder data/GIS. The shape files must be in the WGS84 projection. Typically this will be the shape files of the administrative boundaries of the country. Please make sure to make available shp, shx, dbf and prj files.

## Templates for the forecast bulletins
Templates for the forecast bulletins in the folder data/templates. The templates must be in the xlsx format. The templates can contain several sheets but only the first sheet of the bulletin template is used by the forecast tools to write to. The same logic is used for the bulletin template as in the iEasyHydro software. Terms in the template bulletin indicated by {{}} are replaced by values by the linear regression tool. Please see the list in [doc/bulletin_template_tags.md](doc/bulletin_template_tags.md) for a list of available tags and use the available templates as reference.



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


# Forecast configuration
The forecast configuration dashboard is used to select the stations for which forecasts are to be produced. The dashboard is accessed by double-clicking on the station configuration icon on your desktop. The dashboard is currently only available in Russian language. Detailed user instructions are available by clicking on the help button in upper right corner of the dashboard window.

# Forecast dashboard
The forecast dashboard is used to visualize the forecasts. The dashboard is accessed by double-clicking on the forecast dashboard icon on your desktop. The dashboard can be configured to run in Russian or English. The current version of the dashboard has the following features:
- A predictor tab that allows to visualize the predictor data for the station selected in the side pane. The predictor data is read from the iEasyHydro database or from the excel sheets.
- A forecast tab that visualizes the hydrograph with the forecasted discharge for the station selected in the side pane. The forecast is produced by the forecast tools based on past discharge data using the linear regression method.
The tabs are described in more detail in the following sections.

## Predictor tab
In the current version of the software, the predictor tab shows

## Forecast tab
The method to calculate the forecast range can be selected in the side pane. Following options are available:
a) The forecast range is calculated based on 0.674 times the standard deviation of the observed discharge data.
b) The forecast range is plus/minus a manually selected percentage of the forecasted discharge.


# Installation
For the installation of the SAPPHIRE Forecast Tools, we use the Docker system. The deployment of the forecast tools is described in detail in the file [doc/deployment.md](doc/deployment.md).