# Configuration

The different software components of the SAPPHIRE Forecast Tools interact with each other through input and output files (see following figure for an overview)

TODO: UPDATE FIGURE

<img src="www/io.png" alt="IO" width="700"/>

## Configuration of the forecast tools
We recommend not changing the path ieasyforecast_configuration_path nor the names of the configuration files. You will need to edit the contents of the ieasyforecast_config_file_all_stations and make sure that the station codes given in ieasyforecast_config_file_station_selection are present also in ieasyforecast_config_file_all_stations. Please have a look at the example files in the config folder for guidance.
```
# Snipped of .env. We recommend NOT editing the following lines.
ieasyforecast_configuration_path=../config
ieasyforecast_config_file_all_stations=config_all_stations_library.json
ieasyforecast_config_file_station_selection=config_station_selection.json
ieasyforecast_config_file_output=config_output.json
```

### The config all stations library file
The SAPPHIRE forecast tools need to have an overview over which stations are available for forecasting. This information is stored in the config_all_stations_library.json file. The file is a list of dictionaries, where each dictionary contains information about one station. The station code is used as the key of the dictionary. Please note that the present version of the software it is assumed that gauge stations start with the character '1'. Currently, only the Russian river and site names are used in the forecast dashboard. Please refer to the file config/config_all_station_library.json for a working example. All entries marked with an * are exported by the iEasyHydro SKD library from the iEasyHydro database by default but the values are not used in the Forecast Tools. If you have to set up the all stations configuration file manually, you may use dummy data for the entries marked with *. The following information is stored for each station:
- *id (float): Site identifier exported from iEasyHydro SKD. Example value: 1.0
- basin (string): Name of the river basin. Example value: "Sihl"
- lat (float): Latitude of station. Example value: 47.368327
- long (float): Longitude of station. Example value: 8.527410
- *country (string): Name of the country the gauge is located. Example value: "Switzerland"
- *is_virtual (bool): Whether the station is a virtual station. Example value: false
- region (string): Name of the region the gauge is attributed to. Example value: "Mittelland"
- *site_type (string): Type of site. Example value: "automatic-discharge". Example value: "automatic-discharge"
- name_ru (string): Name of the gauge in Russian language. Example value: "Зиль - Цюрих, Зильхёльцли"
- *organization_id (int): Identifyer of organization. Example value: 1
- *elevation (float): Elevation in meters above mean sea level of gauge. Example value: 0.0
- river_ru (string): Name of the river in Russian. Example value: "Зиль"
- punkt_ru (string): Name of the gauge location in Russian. Example value: "Цюрих, Зильхёльцли"
- code (int): Gauge station code. Example value: 12176

### Intermediate results of the forecast tools
Intermediate results are written by the linear regression tool and read by the forecast dashboard. We recommend not changing the path ieasyforecast_intermediate_data_path nor the names of the intermediate files and we further recommend not manually editing any files in the path ieasyforecast_intermediate_data_path. The files are written by the backend tool and read by the forecast dashboard.
```
# Snipped of .env. We recommend NOT editing the following lines.
ieasyforecast_intermediate_data_path=../internal_data
ieasyforecast_hydrograph_day_file=hydrograph_day.pkl
ieasyforecast_hydrograph_pentad_file=hydrograph_pentad.pkl
ieasyforecast_results_file=offline_forecasts_pentad.csv
```
The backend further stores the date of the last successful run in the file ieasyforecast_last_successful_run_file. The file is stored under ieasyforecast_intermediate_data_path and is used to determine from which date the forecast should be run. It is updated by the backend.
```
# Snipped of .env. We recommend NOT editing the following lines.
ieasyforecast_last_successful_run_file=last_successful_run.txt
```

### Configuration of the forecast configuration dashboard
You will have to change the file name to match the administrative boundaries of your country. We recommend that you do not change the path ieasyforecast_gis_directory_path but rather copy your administrative boundary layers to ieasyforecast_gis_directory_path. You can use official shapefile layers by your countries administration or download publicly available layers from the [GADM website](https://gadm.org/data.html). The layers must be in the WGS84 coordinate system (EPSG:4326).
```
# In .env adapt the name of the administrative boundaries file to one of your country.
ieasyforecast_country_borders_file_name=gadm41_CHE_shp/gadm41_CHE_1.shp
```
Please note that we do not recommend changing the paths and names of the configuration files in apps/config.

### Configuration of the iEasyHydro SDK library
The SAPPHIRE Forecast Tools are designed to be able to use the iEasyHydro database (either the online or the locally installed version) as a source of discharge data. The iEasyHydro SDK library is used to access the iEasyHydro database. For the following instructions, we assume that you have access to the iEasyHydro database. If you do not have access, you should discuss with you IT administration.
```
# In .env_develop, configure the iEasyHydro SDK library to access your
# organizations iEasyHydro database.
IEASYHYDRO_HOST=http://localhost:9000
IEASYHYDRO_USERNAME=<user_name>
IEASYHYDRO_PASSWORD=<password>
ORGANIZATION_ID=1
```
You will need to adapt the port, user_name and password.

If you need to configure the forecast tools for the deployed version of the software, you will need to adapt the following lines in .env:
```
# In .env, configure the iEasyHydro SDK library to access your
# organizations iEasyHydro database.
IEASYHYDRO_HOST=http://host.docker.internal:9000
IEASYHYDRO_USERNAME=<user_name>
IEASYHYDRO_PASSWORD=<password>
ORGANIZATION_ID=1
```
You will need to adapt the port, user_name and password.

### Configuration of the iEasyReports library

### Configuration to facilitate testing of the tools
During development or deployment, you may want to focus only on selected stations. While all stations can be selected in the forecast configuration dashboard, you may want to limit the stations for which the actual forecast is produced. We use this option during the development of the forecast tools where we focus on a few stations for the implementation of the backend and the forecast dashboard. List the stations you wish to produce forecasts for in the file config_development_restrict_station_selection.json. The file has the same format as config_station_selection.json.
```
ieasyforecast_restrict_stations_file=../config/config_development_restrict_station_selection.json
```
For the deployment of the software or to not filter for a subset of the stations, you can set the value to null. The backend will check if the station selection is restricted and prints a warning to the console if this is the case so that it is not forgotten during deployment.