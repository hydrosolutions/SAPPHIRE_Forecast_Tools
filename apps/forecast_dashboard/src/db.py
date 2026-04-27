import os
import time
from datetime import datetime
from functools import wraps

import numpy as np
import pandas as pd
import requests

from src import processing
from src.gettext_config import _
from dashboard.logger import setup_logger

logger = setup_logger()

api_gateway_url = os.getenv("API_GATEWAY_URL", "http://localhost:8000")
API_BASE = f"{api_gateway_url}/api"
API_TIMEOUT = 30
CURRENT_YEAR = datetime.now().year
PREVIOUS_YEAR = CURRENT_YEAR - 1

SNOW_VALUE_COLS = [f"value{i}" for i in range(1, 15)]

# Neural Ensemble config
NE_BASE_MODELS = ["TFT", "TiDE", "TSMixer"]
NE_QUANTILE_COLS = ["Q5", "Q25", "Q75", "Q95", "E[Q]"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _horizon_in_year_col(horizon: str) -> str:
    if horizon == "decade":
        return "decad_in_year"
    if horizon == "month":
        return "month_in_year"
    return "pentad_in_year"


def _resolve_station(station) -> str:
    return station if isinstance(station, str) else station.value.split()[0]


def _timed(func):
    """Log execution time of decorated function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time.time()
        result = func(*args, **kwargs)
        logger.debug("%s completed in %.3fs", func.__name__, time.time() - t0)
        return result
    return wrapper


def _convert_na_to_nan(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pd.NA to np.nan and revert to numpy dtypes."""
    result = df.copy()
    for col in result.columns:
        mask = result[col].isna()
        result[col] = result[col].astype(object)
        result.loc[mask, col] = np.nan
    return result.infer_objects()

# ---------------------------------------------------------------------------
# API layer
# ---------------------------------------------------------------------------

def _read_data(service_type: str, data_type: str, params: dict = None) -> pd.DataFrame:
    """Fetch data from the backend API and return a DataFrame.

    Args:
        service_type: 'preprocessing' or 'postprocessing'
        data_type: 'runoff', 'hydrograph', 'meteo', 'forecast',
                   'lr-forecast', 'skill-metric', 'snow', 'bulletin'
        params: Query parameters forwarded to the API.
    """
    url = f"{API_BASE}/{service_type}/{data_type}/"
    response = requests.get(url, params=params, timeout=API_TIMEOUT)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    # print("### dbg: _read_data:", df)
    return df.convert_dtypes()


def _sanitize_records(records: list[dict]) -> list[dict]:
    """Replace float NaN / ±Inf with None so records are JSON-serializable."""
    import math
    def _clean(v):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    return [{k: _clean(v) for k, v in rec.items()} for rec in records]


def _save_data(service_type: str, data_type: str, records: list[dict]) -> None:
    """Upsert a list of records via POST to the backend API.

    Args:
        service_type: 'preprocessing' or 'postprocessing'
        data_type: API resource name, e.g. 'bulletin'
        records: List of dicts to send as {"data": records}
    """
    if not records:
        logger.info("_save_data called with empty records — nothing to send.")
        return

    records = _sanitize_records(records)
    url = f"{API_BASE}/{service_type}/{data_type}/"
    try:
        resp = requests.post(url, json={"data": records}, timeout=API_TIMEOUT)
        resp.raise_for_status()
        logger.info("Saved %d records to %s/%s/", len(records), service_type, data_type)
    except Exception as e:
        logger.error("Error saving records to %s/%s/: %s", service_type, data_type, e)
        raise


def _delete_data(service_type: str, data_type: str, params: dict) -> None:
    """Delete a single record via DELETE from the backend API.

    Treats HTTP 204 (deleted) and 404 (already gone) as success.

    Args:
        service_type: 'preprocessing' or 'postprocessing'
        data_type: API resource name, e.g. 'bulletin'
        params: Query parameters that identify the record to delete.
    """
    url = f"{API_BASE}/{service_type}/{data_type}/"
    try:
        resp = requests.delete(url, params=params, timeout=API_TIMEOUT)
        if resp.status_code not in (204, 404):
            resp.raise_for_status()
        logger.info("Deleted record from %s/%s/ params=%s", service_type, data_type, params)
    except Exception as e:
        logger.error(
            "Error deleting record from %s/%s/ params=%s: %s",
            service_type, data_type, params, e,
        )
        raise

# ---------------------------------------------------------------------------
# Individual data fetchers
# ---------------------------------------------------------------------------

@_timed
def get_hydrograph_day_all(station) -> pd.DataFrame:
    code = _resolve_station(station)
    df = _read_data("preprocessing", "hydrograph", {
        "horizon": "day",
        "code": code,
        "start_date": f"{CURRENT_YEAR}-01-01",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    })

    if df.empty or "code" not in df.columns:
        logger.warning("get_hydrograph_day_all: no data or missing 'code' for station %s", code)
        df = pd.DataFrame(columns=["code", "date", "5%", "25%", "50%", "75%", "95%",
                                     str(PREVIOUS_YEAR), str(CURRENT_YEAR)])
        df["date"] = pd.to_datetime(df["date"])
        return df

    df.rename(columns={
        "q05": "5%", "q25": "25%", "q50": "50%", "q75": "75%", "q95": "95%",
        "previous": str(PREVIOUS_YEAR), "current": str(CURRENT_YEAR),
    }, inplace=True)
    df.drop(columns=["horizon_type", "horizon_value", "horizon_in_year", "norm", "id"],
            inplace=True, errors="ignore")
    return _convert_na_to_nan(df)


@_timed
def get_hydrograph_pentad_all(horizon, station) -> pd.DataFrame:
    code = _resolve_station(station)
    df = _read_data("preprocessing", "hydrograph", {
        "horizon": horizon,
        "code": code,
        "start_date": f"{PREVIOUS_YEAR}-12-25",
        "end_date": f"{CURRENT_YEAR}-12-25",
        "limit": 1000,
    })

    if df.empty or "code" not in df.columns:
        logger.warning("get_hydrograph_pentad_all: no data or missing 'code' for station %s", code)
        df = pd.DataFrame(columns=["code", "date", "5%", "25%", "50%", "75%", "95%",
                                     _horizon_in_year_col(horizon),
                                     str(PREVIOUS_YEAR), str(CURRENT_YEAR)])
        df["date"] = pd.to_datetime(df["date"])
        return df

    renames = {
        "previous": str(PREVIOUS_YEAR),
        "current": str(CURRENT_YEAR),
        "horizon_in_year": _horizon_in_year_col(horizon),
    }
    df.rename(columns=renames, inplace=True)
    df.drop(columns=["horizon_type", "horizon_value", "count", "std", "q50", "id"],
            inplace=True, errors="ignore")
    return _convert_na_to_nan(df)


def _get_meteo(station, meteo_type: str) -> pd.DataFrame:
    code = _resolve_station(station)
    df = _read_data("preprocessing", "meteo", {
        "meteo_type": meteo_type,
        "code": code,
        "start_date": f"{CURRENT_YEAR}-01-01",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    })

    if df.empty or "value" not in df.columns:
        logger.warning(
            "_get_meteo: no '%s' data for station %s — returning empty DataFrame",
            meteo_type, code,
        )
        df = pd.DataFrame(columns=["code", "date", meteo_type, f"{meteo_type}_norm"])
        df["date"] = pd.to_datetime(df["date"])
        return df

    df.rename(columns={"value": meteo_type, "norm": f"{meteo_type}_norm"}, inplace=True)
    df[meteo_type] = df[meteo_type].astype(float)
    df[f"{meteo_type}_norm"] = df[f"{meteo_type}_norm"].astype(float)
    df.drop(columns=["meteo_type", "day_of_year", "id"], inplace=True, errors="ignore")
    return _convert_na_to_nan(df)


@_timed
def get_rain(station) -> pd.DataFrame:
    return _get_meteo(station, "P")


@_timed
def get_temp(station) -> pd.DataFrame:
    return _get_meteo(station, "T")


def _get_snow_single(station_code: str, snow_type: str, col_name: str) -> pd.DataFrame:
    df = _read_data("preprocessing", "snow", {
        "snow_type": snow_type,
        "code": station_code,
        "start_date": f"{PREVIOUS_YEAR}-01-01",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 10000,
    })
    df.rename(columns={"value": col_name}, inplace=True)
    df.drop(columns=["snow_type", *SNOW_VALUE_COLS, "id"], inplace=True, errors="ignore")
    return _convert_na_to_nan(df)


@_timed
def get_snow_data(station) -> dict[str, pd.DataFrame]:
    code = _resolve_station(station)
    return {
        "HS":  _get_snow_single(code, "HS",  "HS"),
        "RoF": _get_snow_single(code, "ROF", "RoF"),
        "SWE": _get_snow_single(code, "SWE", "SWE"),
    }


@_timed
def get_ml_forecast(horizon, station) -> pd.DataFrame:
    code = _resolve_station(station)
    df = _read_data("postprocessing", "forecast", {
        "horizon": "day",
        "code": code,
        "start_date": f"{PREVIOUS_YEAR}-12-01",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    })

    if df.empty or "date" not in df.columns:
        logger.warning(
            "get_ml_forecast: no forecast data for station %s — returning empty DataFrame", code
        )
        df = pd.DataFrame(columns=[
            "code", "date", "forecast_date", "model_short", "model_long",
            "Q5", "Q25", "Q75", "Q95", "E[Q]", "flag", "composition",
        ])
        df["date"] = pd.to_datetime(df["date"])
        return df

    df.rename(columns={
        "date": "forecast_date", "target": "date",
        "model_type": "model_short", "model_type_description": "model_long",
        "q05": "Q5", "q25": "Q25", "q75": "Q75", "q95": "Q95",
        "forecasted_discharge": "E[Q]",
    }, inplace=True)
    df.drop(columns=["horizon_type", "horizon_value", "horizon_in_year", "q50", "id"],
            inplace=True, errors="ignore")

    # Keep only the latest forecast date
    df = df[df["forecast_date"] == df["forecast_date"].max()]

    # Build Neural Ensemble as mean of base models (TFT, TIDE, and TSMIXER)
    # keep only the 3 base models
    base = df[df["model_short"].isin(NE_BASE_MODELS)]
    # compute NE as the average of the quantiles across the 3 models
    ne = base.groupby(["code", "date", "forecast_date"], as_index=False)[NE_QUANTILE_COLS].mean()
    # add model metadata
    ne["model_short"] = "NE"
    ne["model_long"] = "Neural Ensemble (NE)"
    ne["flag"] = 0
    ne["composition"] = ",".join(NE_BASE_MODELS)
    ne = ne.reindex(columns=df.columns)

    return _convert_na_to_nan(pd.concat([df, ne], ignore_index=True))


@_timed
def get_linreg_predictor(horizon, station) -> pd.DataFrame:
    code = _resolve_station(station)
    df = _read_data("postprocessing", "lr-forecast", {
        "horizon": horizon,
        "code": code,
        "start_date": "2000-01-01",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    })

    if df.empty or "date" not in df.columns:
        logger.warning("get_linreg_predictor: no LR forecast data for station %s", code)
        df = pd.DataFrame(columns=[
            "code", "date", "Date", _horizon_in_year_col(horizon),
        ])
        df["date"] = pd.to_datetime(df["date"])
        return df

    df.rename(columns={"horizon_in_year": _horizon_in_year_col(horizon)}, inplace=True)
    df.drop(columns=["horizon_type", "horizon_value", "id"], inplace=True, errors="ignore")
    df["Date"] = df["date"] + pd.Timedelta(days=1)
    return _convert_na_to_nan(df)

@_timed
def get_forecasts_all(horizon, station=None) -> pd.DataFrame:
    hin = _horizon_in_year_col(horizon)
    hv_col = "decade" if horizon == "decade" else "pentad_in_month"

    code = None
    if station is not None:
        code = _resolve_station(station)

    # --- ML / deep-learning forecasts ---
    ml_params = {
        "horizon": horizon,
        "start_date": f"{PREVIOUS_YEAR}-12-20",
        "end_date": f"{CURRENT_YEAR}-12-31",
        # "target": "null",
        "limit": 1000,
    }
    if code:
        ml_params["code"] = code

    df_ml = _read_data("postprocessing", "forecast", ml_params)
    if df_ml.empty or "date" not in df_ml.columns:
        logger.warning("get_forecasts_all: no ML forecast data for station %s", code)
        df_ml = pd.DataFrame()
    else:
        df_ml.rename(columns={
            "horizon_value": hv_col, "horizon_in_year": hin,
            "model_type": "model_short", "model_type_description": "model_long",
            "q05": "Q5", "q25": "Q25", "q75": "Q75", "q95": "Q95",
        }, inplace=True)
        df_ml.drop(columns=["horizon_type", "target", "id"], inplace=True, errors="ignore")
        df_ml["Date"] = df_ml["date"] + pd.Timedelta(days=1)
        df_ml["year"] = df_ml["Date"].dt.year

    # --- Linear regression forecasts ---
    lr_params = {k: v for k, v in ml_params.items() if k != "target"}
    df_lr = _read_data("postprocessing", "lr-forecast", lr_params)
    lr_hv = "decade" if horizon == "decade" else "pentad"

    if df_lr.empty or "date" not in df_lr.columns:
        logger.warning("get_forecasts_all: no LR forecast data for station %s", code)
        df_lr = pd.DataFrame()
    else:
        df_lr.rename(columns={
            "horizon_value": lr_hv, "horizon_in_year": hin,
        }, inplace=True)
        df_lr.drop(columns=["horizon_type", "discharge_avg", "q_mean", "q_std_sigma", "delta", "id"],
                inplace=True, errors="ignore")
        df_lr["model_short"] = "LR"
        df_lr["model_long"] = "Linear regression (LR)"
        df_lr["flag"] = None
        df_lr["Date"] = df_lr["date"] + pd.Timedelta(days=1)
        df_lr["year"] = df_lr["Date"].dt.year
    
    if df_ml.empty and df_lr.empty:
        logger.warning("get_forecasts_all: no forecast data at all for station %s", code)
        return pd.DataFrame(columns=[
            "code", "date", "Date", "forecast_date", "year",
            "model_short", "model_long",
            "forecasted_discharge", "flag",
            "Q5", "Q25", "Q75", "Q95", "E[Q]",
            hin,
        ])

    # Union of columns, missing columns will become NaN
    combined = pd.concat([df_ml, df_lr], ignore_index=True, sort=False)
    # --- Normalize horizon-in-month columns across ML / LR rows ---
    if "pentad_in_month" in combined.columns and "pentad" in combined.columns:
        combined["pentad_in_month"] = combined["pentad_in_month"].fillna(combined["pentad"])
    elif "pentad" in combined.columns:
        combined["pentad_in_month"] = combined["pentad"]
    if "decade" in combined.columns:
        combined["decad_in_month"] = combined["decade"]
    if code == "15013" and not combined.empty:
        logger.warning(
            "D14 get_forecasts_all code=15013: rows=%d, max_date=%s, "
            "unique_dates=%s, models=%s",
            len(combined), combined["date"].max(),
            sorted(combined["date"].dropna().dt.date.unique()),
            list(combined["model_short"].unique()),
        )
    return _convert_na_to_nan(combined.sort_values("Date"))

@_timed
def get_forecast_stats(horizon, station) -> pd.DataFrame:
    code = _resolve_station(station)
    df = _read_data("postprocessing", "skill-metric", {
        "horizon": horizon,
        "code": code,
        "start_date": f"{PREVIOUS_YEAR}-12-31",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    })
    if df.empty or "model_type" not in df.columns:
        logger.warning("get_forecast_stats: no skill-metric data for station %s", code)
        return pd.DataFrame(columns=[
            "code", _horizon_in_year_col(horizon),
            "model_short", "model_long",
        ])
    df.rename(columns={
        "horizon_in_year": _horizon_in_year_col(horizon),
        "model_type": "model_short",
        "model_type_description": "model_long",
    }, inplace=True)
    df.sort_values("date", inplace=True)  # keep only the latest recalculation run per key
    df.drop_duplicates(subset=["code", _horizon_in_year_col(horizon), "model_short"], keep="last", inplace=True)
    df.drop(columns=["horizon_type", "date", "id"], inplace=True, errors="ignore")
    return _convert_na_to_nan(df)


@_timed
def get_forecast_stats_all(horizon) -> pd.DataFrame:
    """Fetch skill metrics for ALL stations, paginating through the API."""
    page_size = 1000
    skip = 0
    frames = []
    while True:
        df = _read_data("postprocessing", "skill-metric", {
            "horizon": horizon,
            "start_date": f"{PREVIOUS_YEAR}-12-31",
            "end_date": f"{CURRENT_YEAR}-12-31",
            "skip": skip,
            "limit": page_size,
        })
        if df.empty:
            break
        frames.append(df)
        if len(df) < page_size:
            break
        skip += page_size

    if not frames:
        logger.warning("get_forecast_stats_all: no skill-metric data")
        return pd.DataFrame(columns=[
            "code", _horizon_in_year_col(horizon),
            "model_short", "model_long",
        ])

    df = pd.concat(frames, ignore_index=True)
    if "model_type" not in df.columns:
        return pd.DataFrame(columns=[
            "code", _horizon_in_year_col(horizon),
            "model_short", "model_long",
        ])
    df.rename(columns={
        "horizon_in_year": _horizon_in_year_col(horizon),
        "model_type": "model_short",
        "model_type_description": "model_long",
    }, inplace=True)
    df.sort_values("date", inplace=True)
    df.drop_duplicates(subset=["code", _horizon_in_year_col(horizon), "model_short"], keep="last", inplace=True)
    df.drop(columns=["horizon_type", "date", "id"], inplace=True, errors="ignore")
    return _convert_na_to_nan(df)

# ---------------------------------------------------------------------------
# Long-term (monthly) forecasts
# ---------------------------------------------------------------------------

@_timed
def get_long_forecasts(station=None, horizon_value=1) -> pd.DataFrame:
    """Fetch long-term monthly forecasts and reshape to match short-term format."""
    code = _resolve_station(station) if station else None
    params = {
        "horizon_type": "month",
        "horizon_value": horizon_value,
        "start_date": f"{PREVIOUS_YEAR}-12-20",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    }
    if code:
        params["code"] = code

    df = _read_data("postprocessing", "long-forecast", params)
    if df.empty or "date" not in df.columns:
        logger.warning("get_long_forecasts: no data for station %s", code)
        return pd.DataFrame(columns=[
            "code", "date", "Date", "year",
            "model_short", "model_long",
            "forecasted_discharge", "flag",
            "Q5", "Q25", "Q75", "Q95", "E[Q]",
            "valid_from", "month_in_year",
        ])

    df.rename(columns={
        "model_type": "model_short",
        "model_type_description": "model_long",
        "q": "forecasted_discharge",
        "q05": "Q5", "q10": "Q10", "q25": "Q25",
        "q50": "Q50", "q75": "Q75", "q90": "Q90", "q95": "Q95",
    }, inplace=True)
    df.drop(columns=["id", "horizon_type", "horizon_value"], inplace=True, errors="ignore")
    df["valid_from"] = pd.to_datetime(df["valid_from"])
    df["month_in_year"] = df["valid_from"].dt.month
    df["Date"] = df["date"]
    df["year"] = df["date"].dt.year
    # print("### dbg: get_long_forecasts:", df)
    return _convert_na_to_nan(df.sort_values("Date"))


@_timed
def get_long_forecasts_quarter(station=None, horizon_value=1) -> pd.DataFrame:
    """Fetch long-term quarterly forecasts and reshape to match monthly format."""
    code = _resolve_station(station) if station else None
    params = {
        "horizon_type": "quarter",
        "horizon_value": horizon_value,
        "start_date": f"{PREVIOUS_YEAR}-12-20",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    }
    if code:
        params["code"] = code

    df = _read_data("postprocessing", "long-forecast", params)
    if df.empty or "date" not in df.columns:
        logger.warning("get_long_forecasts_quarter: no data for station %s", code)
        return pd.DataFrame(columns=[
            "code", "date", "Date", "year",
            "model_short", "model_long",
            "forecasted_discharge", "flag",
            "Q5", "Q25", "Q75", "Q95", "E[Q]",
            "valid_from", "month_in_year",
        ])

    df.rename(columns={
        "model_type": "model_short",
        "model_type_description": "model_long",
        "q": "forecasted_discharge",
        "q05": "Q5", "q10": "Q10", "q25": "Q25",
        "q50": "Q50", "q75": "Q75", "q90": "Q90", "q95": "Q95",
    }, inplace=True)
    df.drop(columns=["id", "horizon_type", "horizon_value"], inplace=True, errors="ignore")
    df["valid_from"] = pd.to_datetime(df["valid_from"])
    df["month_in_year"] = df["valid_from"].dt.month
    df["Date"] = df["date"]
    df["year"] = df["date"].dt.year
    # Keep only the latest-by-date row per (code, model_short)
    if not df.empty and "date" in df.columns and "code" in df.columns and "model_short" in df.columns:
        df = (
            df.sort_values("date", ascending=False)
              .drop_duplicates(subset=["code", "model_short"], keep="first")
              .reset_index(drop=True)
        )
    return _convert_na_to_nan(df.sort_values("Date"))


@_timed
def get_long_forecasts_season(station=None) -> pd.DataFrame:
    """Fetch long-term seasonal forecasts and reshape to match monthly format."""
    code = _resolve_station(station) if station else None
    params = {
        "horizon_type": "season",
        "start_date": f"{PREVIOUS_YEAR}-12-20",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    }
    if code:
        params["code"] = code

    df = _read_data("postprocessing", "long-forecast", params)
    if df.empty or "date" not in df.columns:
        logger.warning("get_long_forecasts_season: no data for station %s", code)
        return pd.DataFrame(columns=[
            "code", "date", "Date", "year",
            "model_short", "model_long",
            "forecasted_discharge", "flag",
            "Q5", "Q25", "Q75", "Q95", "E[Q]",
            "valid_from", "month_in_year",
        ])

    df.rename(columns={
        "model_type": "model_short",
        "model_type_description": "model_long",
        "q": "forecasted_discharge",
        "q05": "Q5", "q10": "Q10", "q25": "Q25",
        "q50": "Q50", "q75": "Q75", "q90": "Q90", "q95": "Q95",
    }, inplace=True)
    df.drop(columns=["id", "horizon_type", "horizon_value"], inplace=True, errors="ignore")
    df["valid_from"] = pd.to_datetime(df["valid_from"])
    df["month_in_year"] = df["valid_from"].dt.month
    df["Date"] = df["date"]
    df["year"] = df["date"].dt.year
    # Keep only the latest-by-date row per (code, model_short)
    if not df.empty and "date" in df.columns and "code" in df.columns and "model_short" in df.columns:
        df = (
            df.sort_values("date", ascending=False)
              .drop_duplicates(subset=["code", "model_short"], keep="first")
              .reset_index(drop=True)
        )
    return _convert_na_to_nan(df.sort_values("Date"))


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def get_data(horizon, station, all_stations) -> dict:
    add_labels = lambda df: processing.add_labels_to_hydrograph(df, all_stations)
    i18n_models = lambda df: processing.internationalize_forecast_model_names(_, df)

    if horizon == "month":
        return _get_data_monthly(station, all_stations, add_labels, i18n_models)
    if horizon == "quarter":
        return _get_data_quarter(station, all_stations, add_labels, i18n_models)
    if horizon == "season":
        return _get_data_season(station, all_stations, add_labels, i18n_models)

    hin = _horizon_in_year_col(horizon)

    data = {
        "hydrograph_day_all":   add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": add_labels(get_hydrograph_pentad_all(horizon, station)),
        "rain":                 get_rain(station),
        "temp":                 get_temp(station),
        "snow_data":            get_snow_data(station),
        "ml_forecast":          add_labels(get_ml_forecast(horizon, station)),
        "linreg_predictor":     add_labels(get_linreg_predictor(horizon, station)),
        "forecasts_all":        i18n_models(add_labels(get_forecasts_all(horizon, station))),
        "forecast_stats":       i18n_models(get_forecast_stats(horizon, station)),
    }

    # Only merge if both sides have data and the required join keys
    forecasts_all = data["forecasts_all"]
    forecast_stats = data["forecast_stats"]
    merge_keys = ["code", hin, "model_short"]
    can_merge = (
        not forecasts_all.empty
        and not forecast_stats.empty
        and all(k in forecasts_all.columns for k in merge_keys)
        and all(k in forecast_stats.columns for k in merge_keys)
    )
    if can_merge:
        data["forecasts_all"] = forecasts_all.merge(
            forecast_stats,
            on=merge_keys,
            how="left",
            suffixes=("", "_stats"),
        )

    return data


def _get_data_monthly(station, all_stations, add_labels, i18n_models) -> dict:
    """Load data for monthly horizon — only long forecasts + daily hydrograph."""
    supported_modes = os.getenv(
        "ieasyhydroforecast_ml_long_term_supported_modes", ""
    ).split(",")

    forecasts_all = i18n_models(add_labels(get_long_forecasts(station, horizon_value=1)))
    forecast_stats = i18n_models(get_forecast_stats("month", station))

    # Merge skill metrics into forecasts (same pattern as pentad/decad in get_data)
    hin = "month_in_year"
    merge_keys = ["code", hin, "model_short"]
    can_merge = (
        not forecasts_all.empty
        and not forecast_stats.empty
        and all(k in forecasts_all.columns for k in merge_keys)
        and all(k in forecast_stats.columns for k in merge_keys)
    )
    if can_merge:
        forecasts_all = forecasts_all.merge(
            forecast_stats,
            on=merge_keys,
            how="left",
            suffixes=("", "_stats"),
        )

    data = {
        "hydrograph_day_all":   add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": pd.DataFrame(),
        "rain":                 get_rain(station),
        "temp":                 get_temp(station),
        "snow_data":            get_snow_data(station),
        "ml_forecast":          pd.DataFrame(),
        "linreg_predictor":     pd.DataFrame(),
        "forecasts_all":        forecasts_all,
        "forecast_stats":       forecast_stats,
        "long_forecasts_m0":    pd.DataFrame(),
        "long_forecasts_quarter": i18n_models(add_labels(get_long_forecasts_quarter(station, horizon_value=1))),
    }
    if "month_0" in supported_modes:
        m0 = i18n_models(add_labels(get_long_forecasts(station, horizon_value=0)))
        can_merge_m0 = (
            not m0.empty
            and not forecast_stats.empty
            and all(k in m0.columns for k in merge_keys)
            and all(k in forecast_stats.columns for k in merge_keys)
        )
        if can_merge_m0:
            m0 = m0.merge(
                forecast_stats,
                on=merge_keys,
                how="left",
                suffixes=("", "_stats"),
            )
        data["long_forecasts_m0"] = m0
    return data


def _get_data_quarter(station, all_stations, add_labels, i18n_models) -> dict:
    """Load data for quarterly horizon — only long forecasts + daily hydrograph."""
    forecasts_all = i18n_models(add_labels(get_long_forecasts_quarter(station)))
    return {
        "hydrograph_day_all":    add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": pd.DataFrame(),
        "rain":                  get_rain(station),
        "temp":                  get_temp(station),
        "snow_data":             get_snow_data(station),
        "ml_forecast":           pd.DataFrame(),
        "linreg_predictor":      pd.DataFrame(),
        "forecasts_all":         forecasts_all,
        "forecast_stats":        pd.DataFrame(),
    }


def _get_data_season(station, all_stations, add_labels, i18n_models) -> dict:
    """Load data for seasonal horizon — only long forecasts + daily hydrograph."""
    forecasts_all = i18n_models(add_labels(get_long_forecasts_season(station)))
    return {
        "hydrograph_day_all":    add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": pd.DataFrame(),
        "rain":                  get_rain(station),
        "temp":                  get_temp(station),
        "snow_data":             get_snow_data(station),
        "ml_forecast":           pd.DataFrame(),
        "linreg_predictor":      pd.DataFrame(),
        "forecasts_all":         forecasts_all,
        "forecast_stats":        pd.DataFrame(),
    }
