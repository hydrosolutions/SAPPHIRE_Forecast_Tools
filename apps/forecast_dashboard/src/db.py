import os
import time
from datetime import date, datetime
from functools import wraps

import numpy as np
import pandas as pd
import requests
from dashboard.logger import setup_logger
from src import processing
from src.discharge_formatting import round_3sf
from src.environment import is_dash_lead_aware
from src.gettext_config import _
from src.snow_window import snow_display_window

from long_term_horizon_resolver import (
    month_horizon_value,
    quarter_horizon_value,
    seasonal_config_name,
    seasonal_horizon_value,
    supported_long_term_modes,
)

logger = setup_logger()

api_gateway_url = os.getenv("API_GATEWAY_URL", "http://localhost:8000")
API_BASE = f"{api_gateway_url}/api"
API_TIMEOUT = 30
CURRENT_YEAR = datetime.now().year
PREVIOUS_YEAR = CURRENT_YEAR - 1

SNOW_VALUE_COLS = [f"value{i}" for i in range(1, 15)]
SNOW_STAT_COLS = [
    "norm", "mean", "min", "max",
    "5%", "25%", "50%", "75%", "95%",
    "last_year", "current_year",
]
SNOW_RENAME_MAP = {
    "previous": "last_year",
    "current": "current_year",
    "q05": "5%",
    "q25": "25%",
    "q50": "50%",
    "q75": "75%",
    "q95": "95%",
}

HYDROGRAPH_VALUE_COLS = [
    "q05", "q25", "q50", "q75", "q95",
    "5%", "25%", "50%", "75%", "95%",
    "previous", "current", str(PREVIOUS_YEAR), str(CURRENT_YEAR),
    "norm",
]

SEASONAL_ISSUE_MONTHS = (1, 2, 3, 4)

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
    if horizon == "quarter":
        return "quarter_in_year"
    if horizon == "season":
        return "season_in_year"
    return "pentad_in_year"


def _resolve_station(station) -> str:
    return station if isinstance(station, str) else station.value.split()[0]


def _resolve_quarter_horizon_value(horizon_value: int | None = None) -> int:
    if horizon_value is not None:
        return int(horizon_value)
    return quarter_horizon_value()


def _resolve_primary_month_lead(supported_modes: list[str]) -> int:
    """Resolve the deployment's primary monthly lead (the ``month_1`` product).

    Returns the configured ``operational_month_lead_time`` for ``month_1``
    (kghm → 1, tjhm → 0). When ``month_1`` is not offered by this deployment,
    returns the legacy lead 1 (the D3-endorsed membership guard).

    A genuine config-read error is NOT caught: the resolver is allowed to raise,
    exactly like the quarter/season resolvers (``db.py`` calls
    ``quarter_horizon_value`` / ``seasonal_horizon_value`` with no fallback).
    Swallowing it would silently reintroduce a hidden hard-coded lead — the very
    bug this change fixes.
    """
    if "month_1" not in supported_modes:
        return 1
    return month_horizon_value("month_1")


def _filter_month_stats_to_lead(forecast_stats: pd.DataFrame, lead: int) -> pd.DataFrame:
    """Filter monthly skill stats to a single displayed lead (Defect F).

    Returns only the rows whose ``horizon_value`` equals ``lead`` so a card
    merges its own lead's skill and nothing else; an empty result leaves the
    card's metric columns blank after the left merge. When the stats carry no
    ``horizon_value`` column (pre-PP-038) or are empty, the frame is returned
    unchanged — there is no per-lead distinction to make.
    """
    if forecast_stats.empty or "horizon_value" not in forecast_stats.columns:
        return forecast_stats
    return forecast_stats[forecast_stats["horizon_value"] == lead].copy()


def _supported_seasonal_issue_months() -> list[int]:
    modes = set(supported_long_term_modes())
    return [
        issue_month
        for issue_month in SEASONAL_ISSUE_MONTHS
        if seasonal_config_name(issue_month) in modes
    ]


def _default_seasonal_issue_month(ref_date: date | None = None) -> int:
    supported = _supported_seasonal_issue_months()
    if not supported:
        raise ValueError("No seasonal long-term modes are supported by this deployment.")

    month = (ref_date or date.today()).month
    eligible = [issue_month for issue_month in supported if issue_month <= month]
    return max(eligible or supported)


def _resolve_seasonal_horizon_value(
    issue_month: int | None = None,
    horizon_value: int | None = None,
) -> int:
    if horizon_value is not None:
        return int(horizon_value)
    return seasonal_horizon_value(issue_month or _default_seasonal_issue_month())


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


def _round_hydrograph_values(df: pd.DataFrame) -> pd.DataFrame:
    """Round hydrograph value columns from the API to display 3sf."""
    result = df.copy()
    for column in HYDROGRAPH_VALUE_COLS:
        if column not in result.columns:
            continue
        rounded = result[column].map(round_3sf)
        result[column] = pd.to_numeric(rounded, errors="coerce")
    return result

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


def _post_bulletin_share(payload: dict) -> dict:
    """POST an assembled bulletin snapshot to the share endpoint.

    Args:
        payload: The share-request body, e.g.
            ``{"horizon", "year", "horizon_value", "expires_at", "payload",
            "station_codes"}`` (see dashboard.bulletin_publish and
            doc/plans/publish_bulletin_api_design.md).

    Returns:
        The parsed JSON response body: ``{"token", "url", "expires_at"}``.

    Raises:
        requests.HTTPError / requests.RequestException: on any failure —
        the caller (widget_manager's Generate-links handler) treats a
        raised exception as "no partial links" and aborts the whole batch.
    """
    # Path matches the service route exactly: POST /bulletin/share (no
    # trailing slash), proxied via /api/postprocessing/. A trailing slash
    # would 307-redirect through the gateway, so keep it exact.
    url = f"{API_BASE}/postprocessing/bulletin/share"
    resp = requests.post(url, json=payload, timeout=API_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


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
    df = _round_hydrograph_values(df)
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
    df = _round_hydrograph_values(df)
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


def _get_snow_single(
    station_code: str,
    snow_type: str,
    col_name: str,
    display_start_month: int = 1,
    display_start_day: int = 1,
    ref_date: date | None = None,
) -> pd.DataFrame:
    contract_columns = ["code", "date", col_name, *SNOW_STAT_COLS]
    effective_ref = ref_date or date.today()
    display_begin, display_end = snow_display_window(
        display_start_month,
        display_start_day,
        effective_ref,
    )
    df = _read_data("preprocessing", "snow", {
        "snow_type": snow_type,
        "code": station_code,
        "start_date": display_begin.strftime("%Y-%m-%d"),
        "end_date": display_end.strftime("%Y-%m-%d"),
        "limit": 10000,
    })
    if df.empty:
        return pd.DataFrame({
            "code": pd.Series(dtype=object),
            "date": pd.Series(dtype="datetime64[ns]"),
            **{
                column: pd.Series(dtype="float64")
                for column in contract_columns
                if column not in {"code", "date"}
            },
        })

    sort_columns = ["date", *(["id"] if "id" in df.columns else [])]
    df.sort_values(sort_columns, inplace=True, kind="mergesort")
    df.rename(columns={"value": col_name, **SNOW_RENAME_MAP}, inplace=True)
    df.drop(columns=["snow_type", *SNOW_VALUE_COLS, "id"], inplace=True, errors="ignore")
    for column in contract_columns:
        if column not in df.columns:
            df[column] = np.nan
    df = df.reindex(columns=contract_columns)
    return _convert_na_to_nan(df)


@_timed
def get_snow_data(
    station,
    display_start_month: int = 1,
    display_start_day: int = 1,
    snow_ref_date: date | None = None,
) -> dict[str, pd.DataFrame]:
    code = _resolve_station(station)
    effective_ref = snow_ref_date or date.today()
    snow_data = {
        "HS": _get_snow_single(
            code, "HS", "HS", display_start_month, display_start_day, effective_ref
        ),
        "RoF": _get_snow_single(
            code, "ROF", "RoF", display_start_month, display_start_day, effective_ref
        ),
        "SWE": _get_snow_single(
            code, "SWE", "SWE", display_start_month, display_start_day, effective_ref
        ),
    }
    hs_stat_columns = ["HS", *SNOW_STAT_COLS]
    snow_data["HS"][hs_stat_columns] = snow_data["HS"][hs_stat_columns] * 100
    return snow_data


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
        "limit": 10000,
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


def _drop_tombstone_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop tombstone rows (n_pairs == 0) from a skill metrics DataFrame.

    Tombstones are upserted by the write-side to mark stale long-horizon
    skill keys (n_pairs = 0, all metric columns NULL). Legitimate rows always
    have n_pairs > 0, so that is a clean separator. If n_pairs is absent the
    DataFrame is returned unchanged (short-term rows are never affected).
    """
    if df.empty or "n_pairs" not in df.columns:
        return df
    return df[df["n_pairs"].notna() & (df["n_pairs"] > 0)].copy()


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
    if df.empty or "model_type" not in df.columns or "horizon_in_year" not in df.columns:
        logger.warning("get_forecast_stats: no skill-metric data for station %s", code)
        return pd.DataFrame(columns=[
            "code", _horizon_in_year_col(horizon),
            "model_short", "model_long",
        ])
    # Drop tombstones before sort/dedup so a stale-key row can never be
    # selected or displayed.
    df = _drop_tombstone_rows(df)
    df.rename(columns={
        "horizon_in_year": _horizon_in_year_col(horizon),
        "model_type": "model_short",
        "model_type_description": "model_long",
    }, inplace=True)
    df.sort_values("date", inplace=True)  # keep only the latest recalculation run per key
    # PP-038: month skill metrics include per-lead rows (one per horizon_value).
    # Include horizon_value in the dedup subset when present so that distinct
    # leads are NOT collapsed to a single arbitrary row.
    dedup_cols = ["code", _horizon_in_year_col(horizon), "model_short"]
    if "horizon_value" in df.columns:
        dedup_cols = dedup_cols + ["horizon_value"]
    df.drop_duplicates(subset=dedup_cols, keep="last", inplace=True)
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
    if "model_type" not in df.columns or "horizon_in_year" not in df.columns:
        return pd.DataFrame(columns=[
            "code", _horizon_in_year_col(horizon),
            "model_short", "model_long",
        ])
    # Drop tombstones before sort/dedup so a stale-key row can never be
    # selected or displayed.
    df = _drop_tombstone_rows(df)
    df.rename(columns={
        "horizon_in_year": _horizon_in_year_col(horizon),
        "model_type": "model_short",
        "model_type_description": "model_long",
    }, inplace=True)
    df.sort_values("date", inplace=True)
    # PP-038 (mirrors get_forecast_stats): include horizon_value in the dedup
    # subset when present so distinct per-lead rows are NOT collapsed.
    dedup_cols = ["code", _horizon_in_year_col(horizon), "model_short"]
    if "horizon_value" in df.columns:
        dedup_cols = dedup_cols + ["horizon_value"]
    df.drop_duplicates(subset=dedup_cols, keep="last", inplace=True)
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
def get_long_forecasts_quarter(station=None, horizon_value=None) -> pd.DataFrame:
    """Fetch long-term quarterly forecasts and reshape to match monthly format."""
    code = _resolve_station(station) if station else None
    resolved_horizon_value = _resolve_quarter_horizon_value(horizon_value)
    params = {
        "horizon_type": "quarter",
        "horizon_value": resolved_horizon_value,
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
            "valid_from", "month_in_year", "quarter_in_year",
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
    df["quarter_in_year"] = ((df["valid_from"].dt.month - 1) // 3 + 1)
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
def get_long_forecasts_season(
    station=None,
    issue_month: int | None = None,
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Fetch long-term seasonal forecasts and reshape to match monthly format."""
    code = _resolve_station(station) if station else None
    resolved_horizon_value = _resolve_seasonal_horizon_value(issue_month, horizon_value)
    params = {
        "horizon_type": "season",
        "horizon_value": resolved_horizon_value,
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
            "valid_from", "month_in_year", "season_in_year",
        ])

    df.rename(columns={
        "model_type": "model_short",
        "model_type_description": "model_long",
        "q": "forecasted_discharge",
        "q05": "Q5", "q10": "Q10", "q25": "Q25",
        "q50": "Q50", "q75": "Q75", "q90": "Q90", "q95": "Q95",
    }, inplace=True)
    df["season_in_year"] = pd.to_numeric(df["horizon_value"], errors="coerce").astype("Int64")
    df.drop(columns=["id", "horizon_type", "horizon_value"], inplace=True, errors="ignore")
    df["valid_from"] = pd.to_datetime(df["valid_from"])
    df["month_in_year"] = df["valid_from"].dt.month
    df["Date"] = df["date"]
    df["year"] = df["date"].dt.year
    # Keep only the latest-by-date row per (code, season lead, model_short)
    if (
        not df.empty
        and "date" in df.columns
        and "code" in df.columns
        and "season_in_year" in df.columns
        and "model_short" in df.columns
    ):
        df = (
            df.sort_values("date", ascending=False)
              .drop_duplicates(subset=["code", "season_in_year", "model_short"], keep="first")
              .reset_index(drop=True)
        )
    return _convert_na_to_nan(df.sort_values("Date"))


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def get_data(
    horizon,
    station,
    all_stations,
    snow_display_start_month: int = 1,
    snow_display_start_day: int = 1,
) -> dict:
    def add_labels(df):
        return processing.add_labels_to_hydrograph(df, all_stations)

    def i18n_models(df):
        return processing.internationalize_forecast_model_names(_, df)

    if horizon == "month":
        return _get_data_monthly(
            station,
            all_stations,
            add_labels,
            i18n_models,
            snow_display_start_month,
            snow_display_start_day,
        )
    if horizon == "quarter":
        return _get_data_quarter(
            station,
            all_stations,
            add_labels,
            i18n_models,
            snow_display_start_month,
            snow_display_start_day,
        )
    if horizon == "season":
        return _get_data_season(
            station,
            all_stations,
            add_labels,
            i18n_models,
            snow_display_start_month,
            snow_display_start_day,
        )

    hin = _horizon_in_year_col(horizon)

    data = {
        "hydrograph_day_all":   add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": add_labels(get_hydrograph_pentad_all(horizon, station)),
        "rain":                 get_rain(station),
        "temp":                 get_temp(station),
        "snow_data":            get_snow_data(
            station, snow_display_start_month, snow_display_start_day
        ),
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


def _get_data_monthly(
    station,
    all_stations,
    add_labels,
    i18n_models,
    snow_display_start_month: int = 1,
    snow_display_start_day: int = 1,
) -> dict:
    """Load data for monthly horizon — only long forecasts + daily hydrograph."""
    supported_modes = os.getenv(
        "ieasyhydroforecast_ml_long_term_supported_modes", ""
    ).split(",")
    lead_aware = is_dash_lead_aware()

    # Defect A: resolve the deployment's primary monthly lead from config when
    # lead-aware (kghm → 1, tjhm → 0); otherwise the legacy hard-coded lead 1.
    primary_lead = _resolve_primary_month_lead(supported_modes) if lead_aware else 1

    forecasts_all = i18n_models(add_labels(get_long_forecasts(station, horizon_value=primary_lead)))
    forecast_stats = i18n_models(get_forecast_stats("month", station))

    # Merge skill metrics into forecasts (same pattern as pentad/decad in get_data)
    hin = "month_in_year"
    merge_keys = ["code", hin, "model_short"]

    # PP-038: get_forecast_stats preserves per-lead rows (one per horizon_value).
    # Defect F: each card merges only its displayed lead's skill so the tile
    # merge is 1:1 and forecast rows are not duplicated.
    if lead_aware:
        # Main panel merges the primary lead's stats; the m0 card merges lead-0
        # stats from a SEPARATE frame; a card whose lead has no stats stays blank
        # (never another lead's, never an unfiltered merge, never dropped).
        main_stats = _filter_month_stats_to_lead(forecast_stats, primary_lead)
        m0_stats = _filter_month_stats_to_lead(forecast_stats, 0)
    else:
        # Legacy kill-switch: a single lead-1 filter is reused for BOTH the main
        # panel and the m0 card (Defect F's bug, locked as the flag-off contract).
        if not forecast_stats.empty and "horizon_value" in forecast_stats.columns:
            _op_lead = 1
            _op_mask = forecast_stats["horizon_value"] == _op_lead
            if _op_mask.any():
                forecast_stats = forecast_stats[_op_mask].copy()
        main_stats = forecast_stats
        m0_stats = forecast_stats

    can_merge = (
        not forecasts_all.empty
        and not main_stats.empty
        and all(k in forecasts_all.columns for k in merge_keys)
        and all(k in main_stats.columns for k in merge_keys)
    )
    if can_merge:
        forecasts_all = forecasts_all.merge(
            main_stats,
            on=merge_keys,
            how="left",
            suffixes=("", "_stats"),
        )

    long_forecasts_quarter = pd.DataFrame()
    quarter_forecast_stats = pd.DataFrame()
    quarter_hin = _horizon_in_year_col("quarter")
    quarter_merge_keys = ["code", quarter_hin, "model_short"]
    if "quarter" in supported_modes:
        long_forecasts_quarter = i18n_models(add_labels(get_long_forecasts_quarter(station)))
        quarter_forecast_stats = i18n_models(get_forecast_stats("quarter", station))
        can_merge_quarter = (
            not long_forecasts_quarter.empty
            and not quarter_forecast_stats.empty
            and all(k in long_forecasts_quarter.columns for k in quarter_merge_keys)
            and all(k in quarter_forecast_stats.columns for k in quarter_merge_keys)
        )
        if can_merge_quarter:
            long_forecasts_quarter = long_forecasts_quarter.merge(
                quarter_forecast_stats,
                on=quarter_merge_keys,
                how="left",
                suffixes=("", "_stats"),
            )

    data = {
        "hydrograph_day_all":   add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": pd.DataFrame(),
        "rain":                 get_rain(station),
        "temp":                 get_temp(station),
        "snow_data":            get_snow_data(
            station, snow_display_start_month, snow_display_start_day
        ),
        "ml_forecast":          pd.DataFrame(),
        "linreg_predictor":     pd.DataFrame(),
        "forecasts_all":        forecasts_all,
        "forecast_stats":       main_stats,
        "long_forecasts_m0":    pd.DataFrame(),
        "long_forecasts_quarter": long_forecasts_quarter,
    }
    if "month_0" in supported_modes:
        m0 = i18n_models(add_labels(get_long_forecasts(station, horizon_value=0)))
        can_merge_m0 = (
            not m0.empty
            and not m0_stats.empty
            and all(k in m0.columns for k in merge_keys)
            and all(k in m0_stats.columns for k in merge_keys)
        )
        if can_merge_m0:
            m0 = m0.merge(
                m0_stats,
                on=merge_keys,
                how="left",
                suffixes=("", "_stats"),
            )
        data["long_forecasts_m0"] = m0
    return data


def _get_data_quarter(
    station,
    all_stations,
    add_labels,
    i18n_models,
    snow_display_start_month: int = 1,
    snow_display_start_day: int = 1,
) -> dict:
    """Load data for quarterly horizon — only long forecasts + daily hydrograph."""
    forecasts_all = i18n_models(add_labels(get_long_forecasts_quarter(station)))
    forecast_stats = i18n_models(get_forecast_stats("quarter", station))

    hin = _horizon_in_year_col("quarter")
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

    return {
        "hydrograph_day_all":    add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": pd.DataFrame(),
        "rain":                  get_rain(station),
        "temp":                  get_temp(station),
        "snow_data":             get_snow_data(
            station, snow_display_start_month, snow_display_start_day
        ),
        "ml_forecast":           pd.DataFrame(),
        "linreg_predictor":      pd.DataFrame(),
        "forecasts_all":         forecasts_all,
        "forecast_stats":        forecast_stats,
    }


def _get_data_season(
    station,
    all_stations,
    add_labels,
    i18n_models,
    snow_display_start_month: int = 1,
    snow_display_start_day: int = 1,
) -> dict:
    """Load data for seasonal horizon — only long forecasts + daily hydrograph."""
    forecasts_all = i18n_models(add_labels(get_long_forecasts_season(station)))
    forecast_stats = i18n_models(get_forecast_stats("season", station))

    hin = _horizon_in_year_col("season")
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

    return {
        "hydrograph_day_all":    add_labels(get_hydrograph_day_all(station)),
        "hydrograph_pentad_all": pd.DataFrame(),
        "rain":                  get_rain(station),
        "temp":                  get_temp(station),
        "snow_data":             get_snow_data(
            station, snow_display_start_month, snow_display_start_day
        ),
        "ml_forecast":           pd.DataFrame(),
        "linreg_predictor":      pd.DataFrame(),
        "forecasts_all":         forecasts_all,
        "forecast_stats":        forecast_stats,
    }
