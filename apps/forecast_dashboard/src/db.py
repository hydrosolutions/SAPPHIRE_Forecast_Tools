import requests
import pandas as pd
import numpy as np
import time
import os
# from cachetools import TTLCache # pip install cachetools
# from typing import Optional, Dict, Any, Tuple

horizon = os.getenv("sapphire_forecast_horizon", "pentad")
if horizon == "decad":
    horizon = "decade"

# # cache up to 512 unique requests, each valid for 5 minutes
# _api_cache = TTLCache(maxsize=512, ttl=300)

# def _freeze_params(params: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
#     """
#     Turn params dict into a stable, hashable key.
#     - Sort keys
#     - Convert values to strings (handles dates, numpy types, etc.)
#     - Supports list/tuple values by repeating keys in sorted order
#     """
#     if not params:
#         return ()
#     items = []
#     for k in sorted(params.keys()):
#         v = params[k]
#         if isinstance(v, (list, tuple)):
#             for vv in v:
#                 items.append((k, str(vv)))
#         else:
#             items.append((k, str(v)))
#     return tuple(items)

def read_data(service_type:str, data_type: str, params: dict = None):
    """Read data from the API
    
    Args:
        service_type: 'preprocessing' or 'postprocessing'
        data_type: 'runoff', 'hydrograph', 'meteo', 'forecast', 'lr-forecast', 'skill-metric'
        params: Query parameters (filters like horizon, code, model, start_date, end_date)
    """
    # key = (service_type, data_type, _freeze_params(params))

    # if key in _api_cache:
    #     # return a copy so callers don't accidentally mutate cached df
    #     return _api_cache[key].copy()

    response = requests.get(
        f"http://localhost:8000/api/{service_type}/{data_type}/",
        params=params,
        timeout=30
    )
    response.raise_for_status()

    # response_data = response.json()
    df = pd.DataFrame(response.json())
    df['date'] = pd.to_datetime(df['date'])
    df = df.convert_dtypes()

    # _api_cache[key] = df
    return df.copy()


def get_hydrograph_day_all(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "horizon": "day", 
        "code": station, 
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "limit": 1000
    }
    start_time = time.time()
    hydrograph_day_all = read_data("preprocessing", "hydrograph", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read hydrograph_day_all data:", round(end_time - start_time, 3))
    # hydrograph_day_all.rename(columns={"q05": "5th percentile", "q25": "25th percentile", "q75": "75th percentile", "q95": "95th percentile"}, inplace=True)
    hydrograph_day_all.rename(columns={"q05": "5%", "q25": "25%", "q50": "50%", "q75": "75%", "q95": "95%"}, inplace=True)
    hydrograph_day_all.rename(columns={"previous": "2025", "current": "2026"}, inplace=True)
    hydrograph_day_all = hydrograph_day_all.drop(['horizon_type', 'horizon_value', 'horizon_in_year', 'norm', 'id'], axis=1)
    # print("??????????????????????: hydrograph_day_all", hydrograph_day_all)
    # print("??????????????????????: hydrograph_day_all.columns", hydrograph_day_all.columns)
    return convert_na_to_nan(hydrograph_day_all)


def get_hydrograph_pentad_all(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "horizon": horizon, 
        "code": station, 
        "start_date": "2025-12-25",
        "end_date": "2026-12-25",
        "limit": 1000
    }
    start_time = time.time()
    hydrograph_pentad_all = read_data("preprocessing", "hydrograph", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read hydrograph_pentad_all data:", round(end_time - start_time, 3))
    if horizon == "decade":
        hydrograph_pentad_all.rename(columns={"previous": "2025", "current": "2026", "horizon_in_year": "decad_in_year"}, inplace=True)
    else:
        hydrograph_pentad_all.rename(columns={"previous": "2025", "current": "2026", "horizon_in_year": "pentad_in_year"}, inplace=True)
    hydrograph_pentad_all = hydrograph_pentad_all.drop(['horizon_type', 'horizon_value', 'count', 'std', 'q50', 'id'], axis=1)
    # print("??????????????????????: hydrograph_pentad_all", hydrograph_pentad_all)
    # print("??????????????????????: hydrograph_pentad_all.columns", hydrograph_pentad_all.columns)
    return convert_na_to_nan(hydrograph_pentad_all)

def get_rain(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "meteo_type": "P",
        "code": station, 
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "limit": 1000
    }
    start_time = time.time()
    rain = read_data("preprocessing", "meteo", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read rain data:", round(end_time - start_time, 3))
    rain.rename(columns={"P": "Precipitation"}, inplace=True)
    rain.rename(columns={"value": "P", "norm": "P_norm"}, inplace=True)
    rain["P"] = rain["P"].astype(float)
    rain["P_norm"] = rain["P_norm"].astype(float)
    rain = rain.drop(['meteo_type', 'day_of_year', 'id'], axis=1)
    # print("??????????????????????: rain", rain)
    return convert_na_to_nan(rain)

def get_temp(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "meteo_type": "T",
        "code": station, 
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "limit": 1000
    }
    start_time = time.time()
    temp = read_data("preprocessing", "meteo", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read temp data:", round(end_time - start_time, 3))
    temp.rename(columns={"T": "Temperature"}, inplace=True)
    temp.rename(columns={"value": "T", "norm": "T_norm"}, inplace=True)
    temp["T"] = temp["T"].astype(float)
    temp["T_norm"] = temp["T_norm"].astype(float)
    temp = temp.drop(['meteo_type', 'day_of_year', 'id'], axis=1)
    # print("??????????????????????: temp", temp)
    return convert_na_to_nan(temp)

def get_snow_data(station):
    if not isinstance(station, str):
        station = station.value.split()[0]

    snow_data = {}
    columns = [f"value{i}" for i in range(1, 15)]

    params = {
        "snow_type": "HS",
        "code": station, 
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "limit": 10000
    }
    start_time = time.time()
    snow_data_hs = read_data("preprocessing", "snow", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read snow data (HS):", round(end_time - start_time, 3))
    snow_data_hs.rename(columns={"value": "HS"}, inplace=True)
    snow_data_hs = snow_data_hs.drop(['snow_type', *columns, 'id'], axis=1)
    # print("??????????????????????: snow_data_hs", snow_data_hs)

    params["snow_type"] = "ROF"
    start_time = time.time()
    snow_data_rof = read_data("preprocessing", "snow", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read snow data (ROF):", round(end_time - start_time, 3))
    snow_data_rof.rename(columns={"value": "RoF"}, inplace=True)
    snow_data_rof = snow_data_rof.drop(['snow_type', *columns, 'id'], axis=1)
    # print("??????????????????????: snow_data_rof", snow_data_rof)

    params["snow_type"] = "SWE"
    start_time = time.time()
    snow_data_swe = read_data("preprocessing", "snow", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read snow data (SWE):", round(end_time - start_time, 3))
    snow_data_swe.rename(columns={"value": "SWE"}, inplace=True)
    snow_data_swe = snow_data_swe.drop(['snow_type', *columns, 'id'], axis=1)
    # print("??????????????????????: snow_data_swe", snow_data_swe)

    snow_data['HS'] = convert_na_to_nan(snow_data_hs)
    snow_data['RoF'] = convert_na_to_nan(snow_data_rof)
    snow_data['SWE'] = convert_na_to_nan(snow_data_swe)

    return snow_data

def get_ml_forecast(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "horizon": horizon, 
        "code": station, 
        # "model_type": "TSMixer",
        "start_date": "2025-12-01", 
        "end_date": "2026-12-31",
        "limit": 1000
    }
    start_time = time.time()
    ml_forecast = read_data("postprocessing", "forecast", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read ml_forecast data:", round(end_time - start_time, 3))
    ml_forecast.rename(columns={"date": "forecast_date"}, inplace=True)
    ml_forecast.rename(columns={"target": "date", "model_type": "model_short", "model_type_description": "model_long"}, inplace=True)
    ml_forecast.rename(columns={"q05": "Q5", "q25": "Q25", "q75": "Q75", "q95": "Q95", 'forecasted_discharge': 'E[Q]'}, inplace=True)
    ml_forecast = ml_forecast.drop(['horizon_type', 'horizon_value', 'horizon_in_year', 'q50', 'id'], axis=1)
    
    # Only keep the rows where forecast_date is equal to the most recent forecast date for each station
    latest_forecast_date = ml_forecast['forecast_date'].max()
    ml_forecast = ml_forecast[ml_forecast['forecast_date'] == latest_forecast_date]

    # Calculate the Neural Ensemble (NE) forecast as the average of the TFT, TIDE, and TSMIXER forecasts
    models_for_ne = ["TFT", "TiDE", "TSMixer"]
    quant_cols = ["Q5", "Q25", "Q75", "Q95", "E[Q]"]
    group_cols = ["code", "date", "forecast_date"]

    # keep only the 3 base models
    base = ml_forecast[ml_forecast["model_short"].isin(models_for_ne)].copy()

    # compute NE as the average of the quantiles across the 3 models
    ne = base.groupby(group_cols, as_index=False)[quant_cols].mean()

    # add model metadata
    ne["model_short"] = "NE"
    ne["model_long"]  = "Neural Ensemble (NE)"
    ne["flag"] = 0

    # match column order and append
    ne = ne[ml_forecast.columns]
    ml_forecast = pd.concat([ml_forecast, ne], ignore_index=True)

    # print("??????????????????????: ml_forecast", ml_forecast)
    # print("??????????????????????: ml_forecast columns", ml_forecast.columns)
    return convert_na_to_nan(ml_forecast)

def get_linreg_predictor(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "horizon": horizon, 
        "code": station, 
        "start_date": "2000-01-01", 
        "end_date": "2026-12-31",
        "limit": 1000
    }
    start_time = time.time()
    linreg_predictor = read_data("postprocessing", "lr-forecast", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read linreg_predictor data:", round(end_time - start_time, 3))
    if horizon == "decade":
        linreg_predictor.rename(columns={"horizon_in_year": "decad_in_year"}, inplace=True)
    else:
        linreg_predictor.rename(columns={"horizon_in_year": "pentad_in_year"}, inplace=True)
    linreg_predictor = linreg_predictor.drop(['horizon_type', 'horizon_value', 'id'], axis=1)
    linreg_predictor['Date'] = linreg_predictor['date'] + pd.Timedelta(days=1)
    # print("??????????????????????: linreg_predictor", linreg_predictor)
    # print("??????????????????????: linreg_predictor columns", linreg_predictor.columns)
    return convert_na_to_nan(linreg_predictor)

def get_forecasts_all(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "horizon": horizon, 
        "code": station, 
        "start_date": "2025-12-20",
        "end_date": "2026-12-31",
        "target": "null",
        "limit": 1000
    }
    start_time = time.time()
    forecasts_all = read_data("postprocessing", "forecast", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read forecasts_all data:", round(end_time - start_time, 3))
    if horizon == "decade":
        forecasts_all.rename(columns={"horizon_value": "decad", "horizon_in_year": "decad_in_year", "model_type": "model_short", "model_type_description": "model_long"}, inplace=True)
    else:
        forecasts_all.rename(columns={"horizon_value": "pentad_in_month", "horizon_in_year": "pentad_in_year", "model_type": "model_short", "model_type_description": "model_long"}, inplace=True)
    forecasts_all.rename(columns={"q05": "Q5", "q25": "Q25", "q75": "Q75", "q95": "Q95"}, inplace=True)
    forecasts_all = forecasts_all.drop(['horizon_type', 'target', 'id'], axis=1)
    forecasts_all['Date'] = forecasts_all['date'] + pd.Timedelta(days=1)
    forecasts_all['year'] = forecasts_all['Date'].dt.year
    # print("??????????????????????: forecasts_all", forecasts_all)

    del params["target"]
    start_time = time.time()
    forecasts_lr = read_data("postprocessing", "lr-forecast", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read forecasts_lr data:", round(end_time - start_time, 3))
    if horizon == "decade":
        forecasts_lr.rename(columns={"horizon_value": "decad", "horizon_in_year": "decad_in_year"}, inplace=True)
    else:
        forecasts_lr.rename(columns={"horizon_value": "pentad", "horizon_in_year": "pentad_in_year"}, inplace=True)
    forecasts_lr = forecasts_lr.drop(['horizon_type', 'discharge_avg', 'q_mean', 'q_std_sigma', 'delta', 'id'], axis=1)
    forecasts_lr['model_short'] = 'LR'
    forecasts_lr['model_long'] = 'Linear regression (LR)'
    forecasts_lr['flag'] = None
    forecasts_lr['Date'] = forecasts_lr['date'] + pd.Timedelta(days=1)
    forecasts_lr['year'] = forecasts_lr['Date'].dt.year
    # print("??????????????????????: forecasts_lr", forecasts_lr)

    # Union of columns, missing columns will become NaN
    forecasts_combined = pd.concat([forecasts_all, forecasts_lr], ignore_index=True, sort=False)
    forecasts_combined = forecasts_combined.sort_values('Date')
    # print("??????????????????????: forecasts_combined", forecasts_combined)
    # print("??????????????????????: forecasts_combined.columns", forecasts_combined.columns)
    return convert_na_to_nan(forecasts_combined)

def get_forecast_stats(station):
    if not isinstance(station, str):
        station = station.value.split()[0]
    params = {
        "horizon": horizon, 
        "code": station, 
        "start_date": "2025-12-31",
        "end_date": "2026-12-31",
        "limit": 1000
    }
    start_time = time.time()
    forecast_stats = read_data("postprocessing", "skill-metric", params)
    end_time = time.time()
    print("??????????????????????: Time taken to read forecast_stats data:", round(end_time - start_time, 3))
    if horizon == "decade":
        forecast_stats.rename(columns={"horizon_in_year": "decad_in_year", "model_type": "model_short", "model_type_description": "model_long"}, inplace=True)
    else:
        forecast_stats.rename(columns={"horizon_in_year": "pentad_in_year", "model_type": "model_short", "model_type_description": "model_long"}, inplace=True)
    forecast_stats = forecast_stats.drop(['horizon_type', 'date', 'id'], axis=1)
    # print("??????????????????????: forecast_stats", forecast_stats)
    # print("??????????????????????: forecast_stats columns", forecast_stats.columns)
    return convert_na_to_nan(forecast_stats)


def convert_na_to_nan(df):
    """Convert pd.NA to np.nan and revert to numpy dtypes."""
    result = df.copy()
    for col in result.columns:
        mask = result[col].isna()
        result[col] = result[col].astype(object)
        result.loc[mask, col] = np.nan
    return result.infer_objects()