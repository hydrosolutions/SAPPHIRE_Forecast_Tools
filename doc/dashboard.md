# Forecast Dashboard

The forecast dashboard is a web-based dashboard that allows users to visualize and analyze operational  hydrological forecast data for real and virtual gauging stations. It has been developed for Central Asian hydromets and is currently available for pentadal (5-day daily and average) and decadal (10-day daily and average) forecast horizons.

The components of the forecast dashboard are are described in the chapters below. 

## Header
The header of the dashboard allows the configuration of the view and the user to log out. 
<details>
<summary>Unfold to read about the elements</summary>

- <img src="www/figures/menu_icon.png" alt="Menu Icon" width="24" height="24" style="vertical-align: middle; margin-right: 8px;"> **Menu**: By clicking the menu icon, the user can open and close the sidebar. The sidebar contains options to configure the visualizations of the data.   
- <img src="www/figures/language_selection.png" alt="Menu Icon" width="140" height="24" style="vertical-align: middle; margin-right: 8px;"> **Language**: The user can select the language of the dashboard. Currently, the dashboard is available in English and Russian.   
- <img src="www/figures/logout.png" alt="Menu Icon" width="44" height="24" style="vertical-align: middle; margin-right: 8px;"> **Logout**: The user can log out of the dashboard by clicking the logout icon. This will redirect the user to the login page. The dashboard is designed for single user access and users will be logged out automatically after 10 minutes of inactivity. 

</details>

## Sidebar
The sidebar of the dashboard allows the user to configure the visualizations of the data. 

<details>
<summary>Unfold to read about the elements</summary>

</details>

## Predictor tab


## Forecast tab

### Linear regression card
This card allows the user to select and de-select points used to derive the linear regression model. It shows all available data that was used to create the hindcasts.

# Troubbleshooting
The following section describes common issues that may arise when using the forecast dashboard and how to resolve them.

<details>
<summary>Why don't I see machine learning forecasts for a site</summary>
Typically, this happens if there is no recent discharge data in iEasyHydro HF. Please update the data in iEasyHydro HF, wait for the next regular update of the forecasts or manually trigger a rerun of the forecasts. 
</details>
  

<details>
<summary>I have added new data to the `daily_runoff` directory, but I don't see the data in the Linear regression card.</summary>
The forecast dashboard reads the data for the linear regression card from the results of the linear regression module in the `intermediate_data` directory in the data folder. If you add older data to the `daily_runoff` directory in the data folder, you therefore need to manually update the hindcasts by triggering a manual re-run in the linear regression card of the forecast dashboard or by waiting for the next regular automatic forecast.
</details>