# Hydrological forecast with machine learning tools
This repository contains the code to generate hydrological forecasts using machine learning tools. 

Possible Operational Pipeline:
recalculate_nan_forecasts.py -> make_forecast.py -> fill_ml_gaps.py 
Note: See point 4. to decide when to run recalculate_nan_forecasts.py

1. make_forecast.py:
    makes a forecast for the selected basin with the selected model for either pentadal or decadal mode.
    This file writes or updates the forecast file: {MODE}_{MODEL_TO_USE}_forecast.csv (eg. pentad_TFT_forecast.csv).
    It uses old forecasts to fill recent gaps up to a specified (ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END), it also interpolates other missing values up to a threshold (ieasyhydroforecast_THRESHOLD_MISSING_DAYS_TFT).
    The forecast also get flagged:
    
    Output File: {MODE}_{MODEL_TO_USE}_forecast.csv

2. hindcast_ML_models.py:
    This file makes historical forecasts and the behaviour can be specified with these params:
        SAPPHIRE_MODEL_TO_USE = which model
        SAPPHIRE_HINDCAST_MODE = pentad; 5 days ahead, decad; 10 days ahead
        ieasyhydroforecast_START_DATE = start of the hindcast
        ieasyhydroforecast_END_DATE = last day of the hindcast
        ieasyhydroforecast_NEW_STATIONS = this controlls for which specific stations the hindcast should be made. If it is set to 'None', a hindcast is produced for all configured stations.
    In the hindcast script the output gets flagged automatically according to the flagging system, with either 3 or 4.

    Output File: {MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_daily_{start_date_string}_{end_date_string}.csv

3. fill_ml_gaps.py:
    This script checks if there are any missing forecast dates in the {MODE}_{MODEL_TO_USE}_forecast.csv file: A missing date would indicate that the system was not working, otherwise the forecasted value for this date would be written and flagged accordingly. 
    This script then calls the hindcast script with the data gap as min and max date and fills the forecast file with these hindcasted values:

    Output: Updated {MODE}_{MODEL_TO_USE}_forecast.csv file.

4. recalculate_nan_forecasts.py
    This script checks if there are any nan values in the forecasts and then recalculates them. Nan values from operational forecasts have flag == 1, while nan values from hindcasts have flag == 3. This script checks if there are nan values in the forecasts and then recalculates them (nan values in the forecast are indicated by 1 or 2 (if code failure)), by calling the hindcast script. The hindcast will return a file which is already flagged. 
    Note: If this script is called imidiatly after make_forecast.py, the missing operational data, responsible for the nan values, will most likely not be available, which would lead to a reflaging of the nan values (1 -> 3). Once the flag is 3, it will not be recalculated.

    Output: Updated {MODE}_{MODEL_TO_USE}_forecast.csv file.

Flagging System:
    Flag == 0: Successful Forecast
    Flag == 1: Nan Values in the forecast, due to missing data for example - indicates not available operational data.
    Flag == 2: Forecast was not successful - other error in the code.
    Flag == 3: Nan value after hindcasting - this indicates no available data at all.
    Flag == 4: A hindcast value was produces successfully. 

5. add_new_stations.py
   If new stations are added to the config file, this script will calculate the hindcast for newly added stations. Depending on how many stations are added, this script can take some time. It needs to be manually run.

   Output: Updated {MODE}_{MODEL_TO_USE}_forecast.csv file.

6. initialize_ml_tool.py
    This file will initialize the {MODE}_{MODEL_TO_USE}_forecast.csv file by calculating a hindcast. This script ensures that we have hindcast to properly evaluate the models. This script will ask the user for the time period for which the hindcast should be calculated. Note that the forcing data from the preprocessing_gateway need to be available in order for this to work properly.

    Output: {MODE}_{MODEL_TO_USE}_forecast.csv file.




## Predictor Classes

In order for a model to work in the ML-forecasting system it should Inherit fromt the BasePredictor class. Here the functions are definined which each model's PredictorClass should provide [get_input_chunk_length, get_max_forecast_horizon, predict, hindcast].
The BaseDartsDLPredictor Class is a wrapper for Global Darts forecasting Models (TFT, TiDE, TSMixer...). 


## Model Folder Setup

Each model has it's own model folder (the path is configured in the .env file).

### Setup Darts Deep Learning Models (TFT, TSMixer and TiDE)

Folder Structure \
- **model/**
  - scaler_stats_discharge.csv
  - scaler_stats_era5.csv
  - scaler_stats_static.csv
  - model.pt
  - model.pt.ckpt
  - model_config.json
  - other_additional_information (description, train - val loss etc)

the scaler files save the statistics to normalize the input data.

#### model_config.json 
```json
{
  "num_samples": 200, // Number of Samples to draw 
  "quantiles": [0.1, 0.5, 0.9], // Quantiles to save (Note that these should cover ranges the model was trained on..)
  "scaling_type": "standard",          // or "minmax"
  "scaling_type_covariates": "minmax",
  "scaling_type_static": "standard",
  "exogene_covariates_cols": ["P", "T", "PET"], // exogene variables either from ERA5-Land or Snowmapper
  "past_covariates_cols": ["moving_avr_dis_3", "moving_avr_dis_5", "moving_avr_dis_10"], // covariates to use which are known until the forecast date
  "future_covariates_cols": ["P", "T", "PET", "daylight_hours"], // future covariates to use - here we have forecasted values
  "window_sizes": [3, 5, 10], // window sizes over which the averages of the past discharge should be calculated
  "trainer_config": {
    "accelerator": "cpu",
    "logger": false
  } // trainer config for sampling through the data - generally you do not change this.
}
```
### Setup for Arima Model

