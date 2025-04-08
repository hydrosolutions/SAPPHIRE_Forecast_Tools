# Monthly Forecasting

## Folder Structure

This module interacts with the following folders and files.
- config
  - monthly_models
    - MODEL XY
      - HORIZON_1
        - general_config.json 
        - feature_config.json
        - model_config.json
        - ...
      - HORIZON_N
- intermediate_data
  - runoff_day.csv
  - snow_data
    - SWE
    - HS
    - RoF
  - control_member_forcing
  - hindcast_forcing
  - monthly_predictions
    - MODEL_XY
      - model_forecast_latest.csv
      - model_forecast.csv
  

## Scripts

make_forecast_monthly.py

calibrate_and_hincast.py

tune_hyperparams.py

## Output File

The output forecast.csv files, where the most recent and the historical forecast and the hindcast is stored has the following format.

| forecast_date | code | model_name | valid_from | valid_to  | Q      | Q05 | Q50 | Q90 | flag |
| --------------| -----| ---------- | -----------|-----------|--------|-----|-----|-----| -----|
| 2025-03-27    | xxxx1| LR_SWE     | 2025-03-28 | 2025-03-26| 35     | 20  | 35  | 50  | 0    |

forecast_date:
    At which date the forecast was issued.
code:
    unique identifier for the basin.
model_name:
    name of the model that produced the forecast
valid_from:
    The date from which the forecast is valid.
valid_to:
    Until when the forecast is valid.
Q_pred:
    This will be the column name of the prediction if the model is deterministic.
Q05, Q50, Q90 etc:
    Will be the columns of the predicted quantiles if the model is deterministic.
flag:
    Indicates if it is a real forecast, a hindcast or it was not possible to produce a forecast.
    Flagging System:
        flag == 0: Successful Forecast
        flag == 1: Nan Values in the forecast, due to missing data for example - indicates not available operational data.
        flag == 2: Forecast was not successful - other error in the code.
        flag == 3: Nan value after hindcasting - this indicates no available data at all.
        flag == 4: A hindcast value was produces successfully. 
