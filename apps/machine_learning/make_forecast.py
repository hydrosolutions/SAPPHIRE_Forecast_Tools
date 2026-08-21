# --------------------------------------------------------------------
# FORECASTING WITH MACHINE LEARNING MODELS
# --------------------------------------------------------------------
#        _
#      _( )_
#    _(     )_      /\
#   (_________)    /  \/\            /\
#     \  \  \     /      \_____/\   /  \
#       \  \     /                \/    \
#         \  \                           \
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#       ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# --------------------------------------------------------------------
# DESCRIPTION:
# This script produces forecasts using machine learning models (TFT, TiDE , TSMixer )(and ARIMA -> yet to come)
# --------------------------------------------------------------------
# INPUTS:
# - Data:
#       Autoregressive Discharge Time Series
#       The Quantile Mapped ERA5 data
#       Static Basin Features
#       The Normalization Parameters
# - Model:
#       The trained model for TFT and TiDE
# --------------------------------------------------------------------
# OUTPUTS:
# - Forecasts:
#       The forecasts for today and the next 5 or 10 days with a resolution of 1 day (csv)
# --------------------------------------------------------------------
# Missing Values:
# - If there are missing values in the input of discharge data, there are 3 possible outcomes:
#       1. The number of missing values exceeds the threshold for the model ( This value is set in the .env file), then the model will not be able to make a forecast
#       2. There are missing values in the middle of the input data, these will be imputed using a simple linear interpolation
#       3. There are missing values at the end of the input data, these will be imputed using a recursive imputation, the model will be used to forecast the missing values
#          and the forecasted values will be used as input for the next forecast, There is also a threshold for the number of missing values at the end of the input data
#
# --------------------------------------------------------------------
# TODO:
# - Select only the codes which have the flag true for the model.
# --------------------------------------------------------------------

# Useage:
# ieasyhydroforecast_env_file_path=/path/to/.env SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD python make_forecast.py
# Possible values for MODEL_TO_USE: TFT, TIDE, TSMIXER
# Possible values for PREDICTION_MODE: PENTAD, DECAD


# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import datetime
import glob
import json
import os
import sys

import pandas as pd
import pytorch_lightning as pl
import torch

# from pe_oudin.PE_Oudin import PE_Oudin
# from suntime import Sun, SunTimeException
from darts.models import TFTModel, TiDEModel, TSMixerModel
from darts.utils.likelihood_models import QuantileRegression
from darts.utils.likelihood_models.base import LikelihoodType
from pytorch_lightning.callbacks import Callback
from torch.nn.modules.loss import MSELoss
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torchmetrics.collections import MetricCollection

torch.serialization.add_safe_globals(
    [QuantileRegression, LikelihoodType, Adam, ReduceLROnPlateau, MSELoss, MetricCollection]
)

import logging
from logging.handlers import TimedRotatingFileHandler

logging.getLogger("pytorch_lightning.utilities.rank_zero").setLevel(logging.WARNING)
logging.getLogger("pytorch_lightning.accelerators.cuda").setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# Ensure the logs directory exists
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
file_handler = TimedRotatingFileHandler("logs/log", when="midnight", interval=1, backupCount=30)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger("make_ml_forecast")
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)
# logger.addHandler(console_handler)

import warnings

warnings.filterwarnings("ignore")

# Print logging level of the logger
logger.info("Logging level: %s", logger.getEffectiveLevel())
# Level 10: DEBUG, Level 20: INFO, Level 30: WARNING, Level 40: ERROR, Level 50: CRITICAL
logger.debug("Debug message for logger level 10")

# Custom Libraries
from scr import TFTPredictor, TiDEPredictor, TSMixerPredictor, predictor_ARIMA, utils_ml_forecast
from scr.utils_ml_forecast import (
    SAPPHIRE_API_AVAILABLE,
    _check_ml_forecast_consistency,
    _read_ml_forecasts_from_api,
    _write_ml_forecast_to_api,
    normalize_ml_csv_columns,
)

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import forecast_library as fl
import setup_library as sl


# --------------------------------------------------------------------
# CALLBACKS
# --------------------------------------------------------------------
class LossLogger(Callback):
    # This class is needed for the models initialization
    def __init__(self):
        self.train_loss = []
        self.val_loss = []

    # will automatically be called at the end of each epoch
    def on_train_epoch_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule") -> None:
        self.train_loss.append(float(trainer.callback_metrics["train_loss"]))

    def on_validation_epoch_end(
        self, trainer: "pl.Trainer", pl_module: "pl.LightningModule"
    ) -> None:
        self.val_loss.append(float(trainer.callback_metrics["val_loss"]))


def write_pentad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast_pentad, api_data=None):
    """Save pentad forecast data to API (primary) and CSV (archive).

    Writes to the SAPPHIRE API first since it is the primary data path.
    Then appends the new forecast to the existing CSV archive, handling
    mixed date formats that arise when concatenating old CSV strings
    with new pandas Timestamps.

    Args:
        OUTPUT_PATH_DISCHARGE: Path to the output directory.
        MODEL_TO_USE: Name of the model used for the forecast.
        forecast_pentad: New forecast data to be saved.
        api_data: Data to write to API. If None, uses
            forecast_pentad. For operational mode, this should be
            today's forecasts only.
    """
    # --- 1. Write to SAPPHIRE API (primary path, clean data) ---
    if SAPPHIRE_API_AVAILABLE:
        try:
            data_for_api = api_data if api_data is not None else forecast_pentad
            _write_ml_forecast_to_api(data_for_api, "pentad", MODEL_TO_USE)
            _check_ml_forecast_consistency(forecast_pentad, "pentad", MODEL_TO_USE)
        except Exception as e:
            logger.error(f"Failed to write pentad forecast to API: {e}")
            # Don't fail the whole process - continue to CSV

    # --- 2. Write to CSV (archive/fallback) ---
    try:
        forecast_file_path = os.path.join(
            OUTPUT_PATH_DISCHARGE,
            f"pentad_{MODEL_TO_USE}_forecast.csv",
        )
        try:
            forecast_pentad_old = pd.read_csv(forecast_file_path)
        except FileNotFoundError:
            forecast_pentad_old = pd.DataFrame()

        forecast_combined = pd.concat([forecast_pentad_old, forecast_pentad], axis=0)
        forecast_combined["date"] = pd.to_datetime(forecast_combined["date"], format="mixed")
        forecast_combined["forecast_date"] = pd.to_datetime(
            forecast_combined["forecast_date"], format="mixed"
        )
        forecast_combined = forecast_combined.drop_duplicates(
            subset=["forecast_date", "date", "code"], keep="last"
        )
        forecast_combined = normalize_ml_csv_columns(forecast_combined)
        forecast_combined.to_csv(forecast_file_path, index=False)
    except Exception as e:
        logger.error(f"Failed to write pentad forecast to CSV: {e}")


def write_decad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast_decad, api_data=None):
    """Save decad forecast data to API (primary) and CSV (archive).

    Writes to the SAPPHIRE API first since it is the primary data path.
    Then appends the new forecast to the existing CSV archive, handling
    mixed date formats that arise when concatenating old CSV strings
    with new pandas Timestamps.

    Args:
        OUTPUT_PATH_DISCHARGE: Path to the output directory.
        MODEL_TO_USE: Name of the model used for the forecast.
        forecast_decad: New forecast data to be saved.
        api_data: Data to write to API. If None, uses
            forecast_decad. For operational mode, this should be
            today's forecasts only.
    """
    # --- 1. Write to SAPPHIRE API (primary path, clean data) ---
    if SAPPHIRE_API_AVAILABLE:
        try:
            data_for_api = api_data if api_data is not None else forecast_decad
            _write_ml_forecast_to_api(data_for_api, "decade", MODEL_TO_USE)
            _check_ml_forecast_consistency(forecast_decad, "decade", MODEL_TO_USE)
        except Exception as e:
            logger.error(f"Failed to write decad forecast to API: {e}")
            # Don't fail the whole process - continue to CSV

    # --- 2. Write to CSV (archive/fallback) ---
    try:
        forecast_file_path = os.path.join(
            OUTPUT_PATH_DISCHARGE,
            f"decad_{MODEL_TO_USE}_forecast.csv",
        )
        try:
            forecast_decad_old = pd.read_csv(forecast_file_path)
        except FileNotFoundError:
            forecast_decad_old = pd.DataFrame()

        forecast_combined = pd.concat([forecast_decad_old, forecast_decad], axis=0)
        forecast_combined["date"] = pd.to_datetime(forecast_combined["date"], format="mixed")
        forecast_combined["forecast_date"] = pd.to_datetime(
            forecast_combined["forecast_date"], format="mixed"
        )
        forecast_combined = forecast_combined.drop_duplicates(
            subset=["forecast_date", "date", "code"], keep="last"
        )
        forecast_combined = normalize_ml_csv_columns(forecast_combined)
        forecast_combined.to_csv(forecast_file_path, index=False)
    except Exception as e:
        logger.error(f"Failed to write decad forecast to CSV: {e}")


def prepare_forecast_data(
    past_discharge: pd.DataFrame,
    threshold_missing_days: int,
    threshold_missing_days_end: int,
    old_forecast: pd.DataFrame,
    code: int,
    forecast_horizon: int,
    input_chunk_length: int,
) -> (pd.DataFrame, int):
    """
    Workflow for data preparation for forecasting.
    1. Make time series continuous by reindexing -> missing days will be nan
    2. Check the nan values in the time series
    3. Check the conditions: if we exceed the threshold of missing days, or missing_days_end, we will not make a forecast
        -> this will return a dataframe with nans in it and the flag 1
    4. We take the old forecast file and replace the missing days with the latest forecasted values
    5. if we still have missing days we interpolate or if it is at the end we take the last value
    6. We return the prepared data and the flag 0
    """
    discharge_df = past_discharge.copy()

    try:
        prev_forecast = old_forecast[old_forecast["code"] == code].copy()
        prev_forecast["date"] = pd.to_datetime(prev_forecast["date"])
        prev_forecast["forecast_date"] = pd.to_datetime(prev_forecast["forecast_date"])
    except Exception:
        prev_forecast = None

    # 1: Make time series continouus.
    today = pd.to_datetime(datetime.datetime.now().date())
    lockback_start = today - pd.Timedelta(days=input_chunk_length + 1)
    discharge_df = discharge_df[
        (discharge_df["date"] >= lockback_start) & (discharge_df["date"] <= today)
    ]
    date_range = pd.date_range(start=lockback_start, end=today, freq="D")
    discharge_df.set_index("date", inplace=True)
    discharge_df = discharge_df.reindex(date_range)
    discharge_df.reset_index(inplace=True)
    discharge_df.rename(columns={"index": "date"}, inplace=True)

    # 2: Check for missing values
    missing_values, nans_at_end = utils_ml_forecast.check_for_nans(
        discharge_df.iloc[-input_chunk_length:], threshold_missing_days
    )

    window = discharge_df.iloc[-input_chunk_length:]
    total_nans = int(window["discharge"].isna().sum())
    interior_nans = total_nans - nans_at_end
    logger.info(
        "[code=%s] discharge input window (last %d d): %d/%d NaN (%d interior, %d trailing) "
        "| thresholds: total>%d skips, trailing>=%d skips",
        code, input_chunk_length, total_nans, len(window), interior_nans, nans_at_end,
        threshold_missing_days, threshold_missing_days_end,
    )

    # 3: Check the conditions
    if missing_values["exceeds_threshold"] or nans_at_end >= threshold_missing_days_end:
        logger.warning(
            "[code=%s] SKIP: too many discharge gaps (total NaN exceeds threshold=%s, or trailing NaN=%d >= %d) "
            "-> forecast will be NaN (missing discharge)",
            code, missing_values["exceeds_threshold"], nans_at_end, threshold_missing_days_end,
        )
        return discharge_df, 1

    # 4: Replace missing values with the latest forecasted values (Q50)
    if prev_forecast is not None:
        days_with_nan = discharge_df[discharge_df["discharge"].isna()]["date"]
        prev_forecast = prev_forecast[prev_forecast["date"].isin(days_with_nan)]
        # sort by forecast_date
        prev_forecast = prev_forecast.sort_values(by="forecast_date")
        prev_forecast = prev_forecast.drop_duplicates(subset=["date"], keep="last")

        col_name = "Q50"
        # check if the column exists
        if col_name not in prev_forecast.columns:
            col_name = "Q"
        try:
            # First method: Update all dates at once
            discharge_df.loc[discharge_df["date"].isin(prev_forecast["date"]), "discharge"] = (
                prev_forecast[col_name].values
            )
            logger.debug(f"Nans replaced with forecasted values 1st method: {len(prev_forecast)}")
        except Exception as e1:
            logger.debug(f"First method failed: {e1}")
            try:
                # Second method: Update date by date
                counter = 0
                for missing_date in days_with_nan:
                    discharge_df.loc[discharge_df["date"] == missing_date, "discharge"] = (
                        prev_forecast[prev_forecast["date"] == missing_date][col_name].values[0]
                    )
                    counter += 1
                logger.debug(f"Nans replaced with forecasted values 2nd method: {counter}")
            except Exception as e2:
                logger.debug(f"Second method failed: {e2}")
                pass  # Both methods failed, moving on

        logger.info(
            "[code=%s] step 4: %d discharge NaN days targeted for replacement from previous forecast",
            code, len(days_with_nan),
        )

    # 5: Interpolate missing values and ffill missing values at the end
    # check again for missing values
    missing_values, nans_at_end = utils_ml_forecast.check_for_nans(
        discharge_df.iloc[-input_chunk_length:], threshold_missing_days
    )
    if missing_values["exceeds_threshold"] or nans_at_end >= threshold_missing_days_end:
        logger.warning(
            "[code=%s] SKIP: still too many discharge gaps after imputation (total NaN exceeds threshold=%s, "
            "or trailing NaN=%d >= %d) -> forecast will be NaN (missing discharge)",
            code, missing_values["exceeds_threshold"], nans_at_end, threshold_missing_days_end,
        )
        return discharge_df, 1

    if missing_values["nans_in_between"]:
        logger.info("[code=%s] interpolating interior discharge gaps", code)
        discharge_df = utils_ml_forecast.gaps_imputation(discharge_df)

    if missing_values["nans_at_end"]:
        logger.info("[code=%s] forward-filling %d trailing discharge NaN", code, nans_at_end)
        discharge_df = discharge_df.ffill(limit_area="outside")

    # 6: Return the prepared data
    remaining = int(discharge_df["discharge"].iloc[-input_chunk_length:].isna().sum())
    if remaining == 0:
        logger.info("[code=%s] discharge gaps fully resolved (0 NaN in input window)", code)
    else:
        logger.warning(
            "[code=%s] %d NaN remain in discharge input window after imputation "
            "-> model will output NaN (missing discharge)", code, remaining,
        )
    return discharge_df, 0


def prepare_static_data(path_to_static_features: str):
    """Load and prepare static features data."""
    static_features = pd.read_csv(path_to_static_features)

    if "cluster" in static_features.columns:
        static_features = static_features.drop(columns=["cluster"])
    if "log_q" in static_features.columns:
        static_features = static_features.drop(columns=["log_q"])
    if "CODE" in static_features.columns:
        static_features = static_features.rename(columns={"CODE": "code"})

    static_features.index = static_features["code"]

    return static_features


def load_control_member_data(path_to_qmapped_era5, hru_ml_models):
    """
    Load and prepare ERA5 meteo data (temperature and precipitation).

    Reads from SAPPHIRE API by default, falls back to CSV if API is disabled.
    Set SAPPHIRE_API_ENABLED=false to use CSV files.

    Parameters:
    -----------
    path_to_qmapped_era5 : str
        Path to directory containing CSV files (used only for CSV fallback).
    hru_ml_models : str
        HRU identifier for constructing CSV filenames (used only for CSV fallback).

    Returns:
    --------
    pandas.DataFrame
        Merged meteo data with columns 'code', 'date', 'P', 'T'.
    """
    # Construct CSV paths for fallback
    path_p = os.path.join(path_to_qmapped_era5, hru_ml_models + "_P_control_member.csv")
    path_t = os.path.join(path_to_qmapped_era5, hru_ml_models + "_T_control_member.csv")

    # Use the utility function which handles API vs CSV fallback
    qmapped_era5 = utils_ml_forecast.read_meteo_data_combined(
        csv_path_t=path_t,
        csv_path_p=path_p,
    )

    # Convert code to int (API returns strings, ML module expects integers)
    qmapped_era5["code"] = qmapped_era5["code"].astype(int)

    return qmapped_era5


def prepare_forcing_data(
    intermediate_data_path: str,
    path_to_qmapped_era5: str,
    hru_ml_models: str,
    load_forcing_hindcast=False,
):
    """Prepares the forcing data for the model.
    In this function ERA5 data gets loaded (operational file and on demand hindcast file)
    Additionally the snow data can be loaded aswell."""

    qmapped_era5 = load_control_member_data(path_to_qmapped_era5, hru_ml_models)

    return qmapped_era5


def get_predictor_class(MODEL_TO_USE: str):
    available_ML_models = os.getenv("ieasyhydroforecast_available_ML_models")
    available_ML_models = available_ML_models.split(",")

    if MODEL_TO_USE not in available_ML_models:
        raise ValueError(
            f"Model {MODEL_TO_USE!r} is not supported.\nPlease choose one of the following models: TFT, TIDE, TSMIXER, ARIMA"
        )
    else:
        logger.debug("Model to use: %s", MODEL_TO_USE)
        # print('Model to use: ', MODEL_TO_USE)
        if MODEL_TO_USE == "TFT":
            predictor_class = TFTPredictor.TFTPredictor
        elif MODEL_TO_USE == "TIDE":
            predictor_class = TiDEPredictor.TiDEPredictor
        elif MODEL_TO_USE == "TSMIXER":
            predictor_class = TSMixerPredictor.TSMIXERPredictor
        elif MODEL_TO_USE == "ARIMA":
            predictor_class = predictor_ARIMA.PREDICTOR

    return predictor_class


def get_rivers_to_predict(
    MODEL_TO_USE: str,
):
    rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting = (
        utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts()
    )
    # Combine rivers_to_predict_pentad and rivers_to_predict_decad to get all rivers to predict, only keep unique values
    rivers_to_predict = list(set(rivers_to_predict_pentad + rivers_to_predict_decad))
    # select only codes which the model can predict.
    mask_predictable = hydroposts_available_for_ml_forecasting[MODEL_TO_USE] == True  # noqa: E712 — pandas Series needs == not `is`
    codes_model_can_predict = hydroposts_available_for_ml_forecasting[mask_predictable][
        "code"
    ].tolist()
    rivers_to_predict = list(set(rivers_to_predict) & set(codes_model_can_predict))
    # convert to int
    rivers_to_predict = [int(code) for code in rivers_to_predict]
    logger.debug("Rivers to predict pentad: %s", rivers_to_predict_pentad)
    logger.debug("Rivers to predict decad: %s", rivers_to_predict_decad)
    logger.debug("Rivers to predict: %s", rivers_to_predict)
    logger.debug(
        "Hydroposts available for ML forecasting: \n%s", hydroposts_available_for_ml_forecasting
    )

    return rivers_to_predict, hydroposts_available_for_ml_forecasting


# --------------------------------------------------------------------
# MAIN FUNCTION
# --------------------------------------------------------------------
def make_ml_forecast():
    logger.info("--------------------------------------------------------------------")
    logger.info("Starting make_forecast.py")
    print("--------------------------------------------------------------------")
    print("Starting make_forecast.py")
    # Load the environment variables
    sl.load_environment()

    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv("SAPPHIRE_MODEL_TO_USE")
    logger.debug("Model to use: %s", MODEL_TO_USE)
    predictor_class = get_predictor_class(MODEL_TO_USE)

    # --------------------------------------------------------------------
    # DEFINE THE PREDICTION MODE
    # --------------------------------------------------------------------
    PREDICTION_MODE = os.getenv("SAPPHIRE_PREDICTION_MODE")
    logger.debug("Prediction mode: %s", PREDICTION_MODE)
    if PREDICTION_MODE not in ["PENTAD", "DECAD"]:
        raise ValueError(
            f"Prediction mode {PREDICTION_MODE!r} is not supported.\nPlease choose one of the following prediction modes: PENTAD, DECAD"
        )
    else:
        logger.debug("Prediction mode: %s", PREDICTION_MODE)
        if PREDICTION_MODE == "PENTAD":
            forecast_horizon = 6
        else:
            forecast_horizon = 11

    # --------------------------------------------------------------------
    # INITIALIZE THE PATHS
    # --------------------------------------------------------------------
    # Access the environment variables
    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")
    MODELS_AND_SCALERS_PATH = os.getenv("ieasyhydroforecast_models_and_scalers_path")
    PATH_TO_STATIC_FEATURES = os.getenv("ieasyhydroforecast_PATH_TO_STATIC_FEATURES")
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE")
    # Downscaled weather data
    PATH_TO_QMAPPED_ERA5 = os.getenv("ieasyhydroforecast_PATH_TO_QMAPPED_ERA5")
    HRU_ML_MODELS = os.getenv("ieasyhydroforecast_HRU_CONTROL_MEMBER")

    logger.debug("Current working directory: %s", os.getcwd())
    logger.debug("MODELS_AND_SCALERS_PATH: %s", MODELS_AND_SCALERS_PATH)
    logger.debug("PATH_TO_STATIC_FEATURES: %s", PATH_TO_STATIC_FEATURES)
    logger.debug("OUTPUT_PATH_DISCHARGE: %s", OUTPUT_PATH_DISCHARGE)
    logger.debug("PATH_TO_QMAPPED_ERA5: %s", PATH_TO_QMAPPED_ERA5)
    logger.debug("HRU_ML_MODELS: %s", HRU_ML_MODELS)

    PATH_TO_SCALER = os.getenv("ieasyhydroforecast_PATH_TO_SCALER_" + MODEL_TO_USE)
    # Append Decad to the scaler path if the prediction mode is DECAD
    if PREDICTION_MODE == "DECAD" and MODEL_TO_USE != "ARIMA":
        PATH_TO_SCALER = PATH_TO_SCALER + "_Decad"

    PATH_TO_SCALER = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_SCALER)
    # Test if the path exists
    if not os.path.exists(PATH_TO_SCALER):
        raise FileNotFoundError(f"Directory {PATH_TO_SCALER} not found.")
    logger.debug("PATH_TO_SCALER: %s", PATH_TO_SCALER)

    if MODEL_TO_USE != "ARIMA":
        # select the file which ends on .pt
        PATH_TO_MODEL = glob.glob(os.path.join(PATH_TO_SCALER, "*.pt"))[0]
    else:
        PATH_TO_MODEL = os.getenv("ieasyhydroforecast_PATH_TO_" + MODEL_TO_USE)
        PATH_TO_MODEL = os.path.join(PATH_TO_SCALER, PATH_TO_MODEL)

    # Test if the directory exists
    if not os.path.exists(PATH_TO_MODEL):
        raise FileNotFoundError(f"Directory {PATH_TO_MODEL} not found.")
    logger.debug("PATH_TO_MODEL: %s", PATH_TO_MODEL)

    PATH_TO_STATIC_FEATURES = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES)
    OUTPUT_PATH_DISCHARGE = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    # Extend the OUTPUT_PATH_DISCHARGE with the model name
    OUTPUT_PATH_DISCHARGE = os.path.join(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE)

    PATH_TO_QMAPPED_ERA5 = os.path.join(intermediate_data_path, PATH_TO_QMAPPED_ERA5)

    logger.debug("joined path_to_static_features: %s", PATH_TO_STATIC_FEATURES)
    logger.debug("joined output_path_discharge: %s", OUTPUT_PATH_DISCHARGE)
    logger.debug("joined path_to_qmapped_era5: %s", PATH_TO_QMAPPED_ERA5)

    # --------------------------------------------------------------------
    # GET THE RIVERS TO PREDICT
    rivers_to_predict, hydroposts_available_for_ml_forecasting = get_rivers_to_predict(MODEL_TO_USE)

    # --------------------------------------------------------------------
    # LOAD AND PREPARE DATA
    # --------------------------------------------------------------------
    # Read discharge data from API (default) or CSV fallback
    # The function uses SAPPHIRE_API_ENABLED env var to determine data source
    past_discharge = fl.read_daily_discharge_data()
    # Convert code to int (API returns strings, ML module expects integers)
    past_discharge["code"] = past_discharge["code"].astype(int)

    qmapped_era5 = prepare_forcing_data(
        intermediate_data_path=intermediate_data_path,
        path_to_qmapped_era5=PATH_TO_QMAPPED_ERA5,
        hru_ml_models=HRU_ML_MODELS,
    )

    static_features = prepare_static_data(PATH_TO_STATIC_FEATURES)

    # get the codes to use
    codes_to_use = utils_ml_forecast.get_codes_to_use(past_discharge, qmapped_era5, static_features)
    logger.debug("codes_to_use: %s", codes_to_use)

    # Gap-fill forcing data before PET is calculated so that PET (derived from T)
    # inherits the filled T values and does not propagate NaNs downstream.
    FORCING_GAP_RECENT_THRESHOLD = int(
        os.getenv("ieasyhydroforecast_forcing_gap_fill_recent_day_threshold", 7)
    )
    FORCING_GAP_LIMIT_RECENT = int(os.getenv("ieasyhydroforecast_forcing_gap_limit_recent", 1))
    FORCING_GAP_LIMIT_PAST = int(os.getenv("ieasyhydroforecast_forcing_gap_limit_past", 3))
    qmapped_era5 = utils_ml_forecast.fill_forcing_gaps(
        qmapped_era5,
        reference_date=pd.to_datetime(datetime.datetime.now().date()),
        recent_day_threshold=FORCING_GAP_RECENT_THRESHOLD,
        gap_limit_recent=FORCING_GAP_LIMIT_RECENT,
        gap_limit_past=FORCING_GAP_LIMIT_PAST,
    )

    # --------------------------------------------------------------------
    # Calculate PET Oudin and Daylight Hours
    # --------------------------------------------------------------------
    for code in codes_to_use:
        lat = static_features[static_features["code"] == code]["LAT"].values[0]
        lon = static_features[static_features["code"] == code]["LON"].values[0]
        pet_oudin = utils_ml_forecast.calculate_pet_oudin(
            qmapped_era5[qmapped_era5["code"] == code], lat
        )
        qmapped_era5.loc[qmapped_era5["code"] == code, "PET"] = pet_oudin
        qmapped_era5.loc[qmapped_era5["code"] == code, "daylight_hours"] = (
            utils_ml_forecast.calculate_daylight_hours(
                lat, lon, qmapped_era5[qmapped_era5["code"] == code]
            )
        )

    # --------------------------------------------------------------------
    # LOAD SCALER
    # --------------------------------------------------------------------
    if MODEL_TO_USE == "ARIMA":
        _scaler = None
    else:
        scaler_discharge = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_discharge.csv"))
        scaler_discharge.index = scaler_discharge["Unnamed: 0"].astype(int)
        scaler_era5 = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_era5.csv"))
        scaler_era5.index = scaler_era5["Unnamed: 0"]
        scaler_static = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_static.csv"))
        scaler_static.index = scaler_static["Unnamed: 0"]

    # --------------------------------------------------------------------
    # LOAD MODELS AND MAKE PREDICTORS
    # --------------------------------------------------------------------
    # Load pre-trained model
    if MODEL_TO_USE == "TFT":
        model = TFTModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device("cpu"))
    elif MODEL_TO_USE == "TIDE":
        model = TiDEModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device("cpu"))
    elif MODEL_TO_USE == "TSMIXER":
        model = TSMixerModel.load(os.path.join(PATH_TO_MODEL), map_location=torch.device("cpu"))
    elif MODEL_TO_USE == "ARIMA":
        model = None

    if MODEL_TO_USE == "ARIMA":
        predictor = predictor_class(PATH_TO_MODEL)
    else:
        scalers = {
            "scaler_discharge": scaler_discharge,
            "scaler_covariates": scaler_era5,
            "scaler_static": scaler_static,
        }

        # try the load the model_config.json file
        try:
            model_dir = os.path.dirname(PATH_TO_MODEL)
            with open(os.path.join(model_dir, "model_config.json")) as f:
                model_config = json.load(f)
        except FileNotFoundError as err:
            raise FileNotFoundError(
                "model_config.json not found, Please check the model directory"
            ) from err

        predictor = predictor_class(
            model=model,
            scalers=scalers,
            static_features=static_features,
            dl_config_params=model_config,
            unique_id_col="code",
        )

    # --------------------------------------------------------------------
    # FORECAST
    # --------------------------------------------------------------------
    forecast = pd.DataFrame()

    THRESHOLD_MISSING_DAYS = os.getenv("ieasyhydroforecast_THRESHOLD_MISSING_DAYS_" + MODEL_TO_USE)
    THRESHOLD_MISSING_DAYS_END = os.getenv("ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END")

    # thresholds to ints
    THRESHOLD_MISSING_DAYS = int(THRESHOLD_MISSING_DAYS)
    THRESHOLD_MISSING_DAYS_END = int(THRESHOLD_MISSING_DAYS_END)

    # Load old forecast for missing-value imputation (API-first, CSV fallback)
    prefix = "pentad" if PREDICTION_MODE == "PENTAD" else "decad"
    forecast_csv_path = os.path.join(OUTPUT_PATH_DISCHARGE, f"{prefix}_{MODEL_TO_USE}_forecast.csv")
    lookback_start = (
        pd.to_datetime(datetime.datetime.now().date()) - pd.Timedelta(days=60)
    ).strftime("%Y-%m-%d")

    old_forecast = _read_ml_forecasts_from_api(
        model_type=MODEL_TO_USE,
        horizon_type=prefix,
        start_date=lookback_start,
    )
    if old_forecast.empty:
        logger.info(
            "API returned no %s %s forecasts for imputation — falling back to CSV",
            MODEL_TO_USE,
            prefix,
        )
        try:
            old_forecast = pd.read_csv(forecast_csv_path)
            old_forecast["forecast_date"] = pd.to_datetime(
                old_forecast["forecast_date"], format="mixed"
            )
            old_forecast["date"] = pd.to_datetime(old_forecast["date"], format="mixed")
            old_forecast = normalize_ml_csv_columns(old_forecast)
        except FileNotFoundError:
            old_forecast = pd.DataFrame()
    else:
        logger.info(
            "Read %d %s %s forecast rows from API for imputation",
            len(old_forecast),
            MODEL_TO_USE,
            prefix,
        )

    logger.debug("Predicting for %s rivers", len(rivers_to_predict))
    logger.debug("Rivers to predict: %s", rivers_to_predict)
    for code in rivers_to_predict:
        # Cast code to int.
        code = int(code)

        logger.debug("Code: %s", code)

        # get the data
        past_discharge_code = past_discharge[past_discharge["code"] == code]
        qmapped_era5_code = qmapped_era5[qmapped_era5["code"] == code]

        # reformat the past discharge data
        past_discharge_code["date"] = pd.to_datetime(past_discharge_code["date"])

        # sort by date
        past_discharge_code = past_discharge_code.sort_values(by="date")
        qmapped_era5_code = qmapped_era5_code.sort_values(by="date")

        # get the input chunck length -> this can than be used to determine the relevant allowed missing values
        input_chunk_length = predictor.get_input_chunk_length()
        logger.debug("input_chunk_length: %s", input_chunk_length)

        today = pd.to_datetime(datetime.datetime.now().date())
        cov_window = qmapped_era5_code[
            (qmapped_era5_code["date"] >= today - pd.Timedelta(days=input_chunk_length))
            & (qmapped_era5_code["date"] <= today + pd.Timedelta(days=forecast_horizon))
        ]
        cov_cols = [c for c in ("P", "T", "PET", "daylight_hours") if c in cov_window.columns]
        cov_nans = {c: int(cov_window[c].isna().sum()) for c in cov_cols}
        logger.info(
            "[code=%s] ERA5 covariates over model window [%s..%s]: %s (rows=%d)",
            code, (today - pd.Timedelta(days=input_chunk_length)).date(),
            (today + pd.Timedelta(days=forecast_horizon)).date(), cov_nans, len(cov_window),
        )
        if any(v > 0 for v in cov_nans.values()):
            logger.warning(
                "[code=%s] NaN present in ERA5 covariates over model window %s "
                "-> model may output NaN (missing meteo)", code, cov_nans,
            )

        # prepare the data
        past_discharge_code, flag = prepare_forecast_data(
            past_discharge=past_discharge_code,
            threshold_missing_days=THRESHOLD_MISSING_DAYS,
            threshold_missing_days_end=THRESHOLD_MISSING_DAYS_END,
            old_forecast=old_forecast,
            code=code,
            forecast_horizon=forecast_horizon,
            input_chunk_length=input_chunk_length,
        )

        predictions = predictor.predict(
            df_rivers_org=past_discharge_code,
            df_covariates=qmapped_era5_code,
            code=code,
            n=forecast_horizon,
        )

        if len(predictions) == 0:
            # error in forecast - something else is wrong
            flag = 2
            logger.debug("Error in forecast for code: %s", code)
        elif predictions.isna().sum().sum() > 0:
            # nan values in the forecast
            flag = 1
            logger.debug("Nan values in the forecast for code: %s", code)
        else:
            flag = 0

        # add the code to the predictions
        predictions["code"] = code
        predictions["forecast_date"] = pd.to_datetime(datetime.datetime.now().date())
        if flag != 2:
            predictions["date"] = pd.to_datetime(predictions["date"])
        else:
            predictions["date"] = pd.to_datetime(datetime.datetime.now().date())
        predictions["flag"] = flag

        predictions["date"] = pd.to_datetime(predictions["date"])
        predictions["forecast_date"] = pd.to_datetime(predictions["forecast_date"])

        forecast = pd.concat([forecast, predictions], axis=0, ignore_index=True)

        # Check if for this code we have a twin virtual gauge which is > 0
        test_value = hydroposts_available_for_ml_forecasting.loc[
            hydroposts_available_for_ml_forecasting["code"] == str(code),
            "virtual_station_name_twin",
        ].iloc[0]
        logger.debug("Twin virtual gauge test value: %s", test_value)
        if test_value is not False:
            logger.debug("Forecast for twin virtual gauge: %s", predictions)

            predictions["code"] = int(test_value)
            predictions["forecast_date"] = datetime.datetime.now().date()
            forecast = pd.concat([forecast, predictions], axis=0)

            logger.debug("Copied data and appended: %s", predictions)

    # --------------------------------------------------------------------
    # SAVE FORECAST
    # --------------------------------------------------------------------
    if PREDICTION_MODE == "PENTAD":
        # first save the latest forecast
        forecast_today_path = os.path.join(
            OUTPUT_PATH_DISCHARGE, f"pentad_{MODEL_TO_USE}_forecast_latest.csv"
        )
        # Create the directory if it doesn't exist
        if not os.path.exists(OUTPUT_PATH_DISCHARGE):
            os.makedirs(OUTPUT_PATH_DISCHARGE)
        forecast.to_csv(forecast_today_path, index=False)
        # Append the new forecast to the existing forecast file
        # Pass forecast as api_data for operational mode (today's forecasts only)
        write_pentad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast, api_data=forecast)
    else:
        forecast_today_path = os.path.join(
            OUTPUT_PATH_DISCHARGE, f"decad_{MODEL_TO_USE}_forecast_latest.csv"
        )
        # Create the directory if it doesn't exist
        if not os.path.exists(OUTPUT_PATH_DISCHARGE):
            os.makedirs(OUTPUT_PATH_DISCHARGE)
        forecast.to_csv(forecast_today_path, index=False)
        # Pass forecast as api_data for operational mode (today's forecasts only)
        write_decad_forecast(OUTPUT_PATH_DISCHARGE, MODEL_TO_USE, forecast, api_data=forecast)

    logger.info("Forecast saved successfully. Exiting make_forecast.py\n")
    logger.info("--------------------------------------------------------------------")


if __name__ == "__main__":
    make_ml_forecast()
