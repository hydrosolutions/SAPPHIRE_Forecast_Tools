# --------------------------------------------------------------------
# HINDECAST WITH MACHINE LEARNING MODELS
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
# This script produces hindecast using machine learning models (TFT, TiDE, TSMixer)
# --------------------------------------------------------------------
# INPUTS:
# - Data:
#       Autoregressive Discharge Time Series
#       The ERA5 data from data gateaway
#       Static Basin Features
#       The Normalization Parameters
# - Model:
#       The trained model for TFT and TiDE and TSMixer
# --------------------------------------------------------------------
# OUTPUTS:
# - Hindecast (Pentad):
#       The hindecast for a pentad period, average over the pentad for different quantiles
# --------------------------------------------------------------------

# Usage
# SAPPHIRE_OPDEV_ENV=True SAPPHIRE_MODEL_TO_USE=TIDE SAPPHIRE_HINDCAST_MODE=PENTAD python hindcast_ML_models.py

# --------------------------------------------------------------------
# Load Libraries
# --------------------------------------------------------------------
import contextlib
import datetime
import glob
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import numpy as np
import pandas as pd
import pytorch_lightning as pl

# pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git
import torch
from darts.models import TFTModel, TiDEModel, TSMixerModel
from pytorch_lightning.callbacks import Callback
from scr.utils_ml_forecast import (
    SAPPHIRE_API_AVAILABLE,
    _write_ml_forecast_to_api,
    canonical_ml_mode,
    normalize_ml_csv_columns,
    read_meteo_data_combined,
)

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
logger = logging.getLogger("hindcast_ML_models")
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)

import warnings

warnings.filterwarnings("ignore")

from darts.utils.likelihood_models import QuantileRegression
from darts.utils.likelihood_models.base import LikelihoodType
from torch.nn.modules.loss import MSELoss
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torchmetrics.collections import MetricCollection

torch.serialization.add_safe_globals(
    [QuantileRegression, LikelihoodType, Adam, ReduceLROnPlateau, MSELoss, MetricCollection]
)

# Custom Libraries
from scr import TFTPredictor, TiDEPredictor, TSMixerPredictor, predictor_ARIMA, utils_ml_forecast

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

# ─── Recovery compute-only mode (P1b) ────────────────────────────────────────
# Private env-var marker.  When set by compute_recovery_unit() in
# recalculate_nan_forecasts.py, main() runs a single predict() call instead of
# the full historical_forecasts loop, writes a strict JSON result, and returns
# without touching any API or CSV writer.  Absent this marker, main() is
# byte-equivalent to before this code existed.
_RECOVERY_COMPUTE_REQUEST_ENV = "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH"
_RECOVERY_COMPUTE_RESPONSE_ENV = "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH"

# All 19 quantile columns produced by BaseDartsDLPredictor.create_prediction_df.
_RECOVERY_19_Q_COLS = (
    "Q5",
    "Q10",
    "Q15",
    "Q20",
    "Q25",
    "Q30",
    "Q35",
    "Q40",
    "Q45",
    "Q50",
    "Q55",
    "Q60",
    "Q65",
    "Q70",
    "Q75",
    "Q80",
    "Q85",
    "Q90",
    "Q95",
)

# Supported DL models for recovery (ARIMA cannot use predict() with 11 targets).
_RECOVERY_DL_MODELS = frozenset({"TFT", "TIDE", "TSMIXER"})


def _run_recovery_compute(
    request: dict,
    predictor,
    era5_data: pd.DataFrame,
    observed_discharge: pd.DataFrame,
    rivers_to_predict: list,
    response_path: str,
    deadline: "datetime.datetime | None" = None,
) -> int:
    """Run compute-only recovery for one validated candidate.

    Called from main() when _RECOVERY_COMPUTE_REQUEST_ENV is set.
    No API writes, no CSV writes.  Returns 0 on success, non-zero on failure.
    Writes the strict JSON result atomically to response_path on success only.
    """
    invocation_id = request.get("invocation_id")
    req_model = request.get("model")
    req_code = str(request.get("source_code", ""))
    req_issue = request.get("issue_date")

    if not invocation_id or not req_model or not req_code or not req_issue:
        logger.error("Recovery compute: request missing required fields")
        return 2

    # Code must be in the filtered rivers_to_predict (already validated by gate checks).
    try:
        req_code_int = int(req_code)
    except (TypeError, ValueError):
        logger.error("Recovery compute: source_code %r is not an integer", req_code)
        return 2

    if req_code_int not in rivers_to_predict:
        logger.error("Recovery compute: code %s not in permitted rivers_to_predict", req_code)
        return 2

    # Validate output_chunk_length >= 11 before inference.
    try:
        ocl = predictor.get_max_forecast_horizon()
    except Exception as e:
        logger.error("Recovery compute: cannot get output_chunk_length: %s", e)
        return 2
    if ocl < 11:
        logger.error(
            "Recovery compute: output_chunk_length=%d < 11; blocked before inference",
            ocl,
        )
        return 2

    # Deadline check before model call.
    if deadline is not None and datetime.datetime.utcnow() >= deadline:
        logger.error("Recovery compute: deadline expired before inference")
        return 2

    # Compute required source history depth.
    # A rolling window of size W needs the target day plus W-1 preceding days, so
    # the total lookback beyond icl is max(0, max_ws - 1) days, not max_ws.
    # obs_start is derived from count-1 so the closed interval [obs_start, issue_ts]
    # contains exactly required_history rows.
    icl = predictor.get_input_chunk_length()
    window_sizes = getattr(predictor, "window_sizes", [3, 5, 10])
    max_ws = max(window_sizes) if window_sizes else 10
    required_history = icl + max(0, max_ws - 1)

    issue_ts = pd.Timestamp(req_issue)
    obs_start = issue_ts - pd.Timedelta(days=required_history - 1)

    # Filter discharge observations: source code only, ending exactly at issue_date.
    disc = observed_discharge.copy()
    disc["date"] = pd.to_datetime(disc["date"])
    disc_code = (
        disc[
            (disc["code"] == req_code_int)
            & (disc["date"] >= obs_start)
            & (disc["date"] <= issue_ts)
        ]
        .sort_values("date")
        .reset_index(drop=True)
    )

    if disc_code.empty or len(disc_code) < required_history:
        logger.error(
            "Recovery compute: insufficient source history (%d rows, need %d) for code %s",
            len(disc_code),
            required_history,
            req_code,
        )
        return 2

    # Observations must be consecutive day-by-day and end exactly at issue_date.
    diffs = disc_code["date"].diff().iloc[1:]
    if not (diffs == pd.Timedelta(days=1)).all():
        logger.error(
            "Recovery compute: non-consecutive dates in source history for code %s", req_code
        )
        return 2
    if disc_code["date"].iloc[-1] != issue_ts:
        logger.error(
            "Recovery compute: source history ends at %s, not issue_date %s",
            disc_code["date"].iloc[-1],
            req_issue,
        )
        return 2

    # Discharge values must be finite for all required history rows.
    _disc_vals = pd.to_numeric(disc_code["discharge"], errors="coerce")
    if _disc_vals.isnull().any() or not np.isfinite(_disc_vals.values).all():
        logger.error(
            "Recovery compute: non-finite discharge values in source history for code %s", req_code
        )
        return 2

    # Covariates must cover obs_start through issue_date + max(11, ocl).
    cov_end_offset = max(11, ocl)
    cov_end_ts = issue_ts + pd.Timedelta(days=cov_end_offset)

    era = era5_data.copy()
    era["date"] = pd.to_datetime(era["date"])
    era_code = era[
        (era["code"] == req_code_int) & (era["date"] >= obs_start) & (era["date"] <= cov_end_ts)
    ].copy()

    if era_code.empty:
        logger.error("Recovery compute: no covariate rows for code %s in required range", req_code)
        return 2

    era_code = era_code.sort_values("date").reset_index(drop=True)

    # Require a unique, consecutive daily grid from obs_start through cov_end_ts.
    if era_code["date"].duplicated().any():
        logger.error("Recovery compute: duplicate dates in covariate grid for code %s", req_code)
        return 2
    if era_code["date"].iloc[0] > obs_start:
        logger.error(
            "Recovery compute: covariate grid starts at %s, later than required start %s",
            era_code["date"].iloc[0],
            obs_start,
        )
        return 2
    if len(era_code) > 1:
        _cov_diffs = era_code["date"].diff().iloc[1:]
        if not (_cov_diffs == pd.Timedelta(days=1)).all():
            logger.error("Recovery compute: covariate grid has interior gaps for code %s", req_code)
            return 2
    if era_code["date"].iloc[-1] < cov_end_ts:
        logger.error(
            "Recovery compute: covariate coverage ends at %s, need through %s",
            era_code["date"].iloc[-1],
            cov_end_ts,
        )
        return 2

    fut_cols = getattr(predictor, "future_covariates_cols", ["P", "T", "PET", "daylight_hours"])
    for fc in fut_cols:
        if fc not in era_code.columns:
            logger.error("Recovery compute: required covariate column %s missing", fc)
            return 2
        vals = pd.to_numeric(era_code[fc], errors="coerce")
        if vals.isnull().any() or not np.isfinite(vals.values).all():
            logger.error(
                "Recovery compute: covariate %s has non-finite values through forecast horizon", fc
            )
            return 2

    # Single predict() call at ONE origin.
    try:
        df_pred = predictor.predict(
            df_rivers_org=disc_code,
            df_covariates=era_code,
            code=req_code_int,
            n=11,
        )
    except Exception as e:
        logger.error("Recovery compute: predict() raised: %s", e)
        return 2

    # Deadline check after inference (late result is still discarded).
    if deadline is not None and datetime.datetime.utcnow() >= deadline:
        logger.error("Recovery compute: deadline expired after inference; result discarded")
        return 2

    if df_pred is None or (hasattr(df_pred, "empty") and df_pred.empty):
        logger.error("Recovery compute: predict() returned empty DataFrame for code %s", req_code)
        return 2

    if len(df_pred) != 11:
        logger.error("Recovery compute: expected 11 rows, got %d", len(df_pred))
        return 2

    if "date" not in df_pred.columns:
        logger.error("Recovery compute: output missing 'date' column")
        return 2

    # If predictor stamps a code column, it must match the request — no silent relabelling.
    if "code" in df_pred.columns:
        _pred_codes = df_pred["code"].unique().tolist()
        if _pred_codes != [req_code_int]:
            logger.error(
                "Recovery compute: predictor returned code(s) %s, expected %s; rejected",
                _pred_codes,
                req_code_int,
            )
            return 2

    # If predictor stamps a forecast_date column, every row must equal the issue date.
    if "forecast_date" in df_pred.columns:
        _expected_fd = pd.Timestamp(req_issue)
        _bad_fd = df_pred[pd.to_datetime(df_pred["forecast_date"]) != _expected_fd]
        if not _bad_fd.empty:
            logger.error(
                "Recovery compute: predictor returned forecast_date(s) != issue_date %s; rejected",
                req_issue,
            )
            return 2

    # If predictor stamps a model column, every row must equal the request model.
    if "model" in df_pred.columns:
        _bad_model = df_pred[df_pred["model"].astype(str) != req_model]
        if not _bad_model.empty:
            logger.error(
                "Recovery compute: predictor returned model %s != %s; rejected",
                df_pred["model"].unique().tolist(),
                req_model,
            )
            return 2

    # Validate all 19 quantile columns present and finite.
    missing_q = [q for q in _RECOVERY_19_Q_COLS if q not in df_pred.columns]
    if missing_q:
        logger.error("Recovery compute: missing quantile columns %s", missing_q)
        return 2

    # Reject noncanonical timestamps (timezone-aware or non-midnight); preserve naive midnight.
    issue_date_obj = datetime.date.fromisoformat(req_issue)
    expected_targets = {
        (issue_date_obj + datetime.timedelta(days=i)).isoformat() for i in range(1, 12)
    }
    _date_strs_out: list = []
    for _raw_d in pd.to_datetime(df_pred["date"]):
        if getattr(_raw_d, "tzinfo", None) is not None:
            logger.error(
                "Recovery compute: output contains timezone-aware timestamp %s; rejected", _raw_d
            )
            return 2
        _ns = getattr(_raw_d, "nanosecond", 0)
        if (
            _raw_d.hour != 0
            or _raw_d.minute != 0
            or _raw_d.second != 0
            or _raw_d.microsecond != 0
            or _ns != 0
        ):
            logger.error(
                "Recovery compute: output contains non-midnight timestamp %s; rejected", _raw_d
            )
            return 2
        _date_strs_out.append(_raw_d.strftime("%Y-%m-%d"))
    actual_targets = set(_date_strs_out)
    if actual_targets != expected_targets:
        logger.error(
            "Recovery compute: target date mismatch (expected %s, got %s)",
            sorted(expected_targets),
            sorted(actual_targets),
        )
        return 2

    if pd.to_datetime(df_pred["date"]).duplicated().any():
        logger.error("Recovery compute: duplicate target dates in output")
        return 2

    for q in _RECOVERY_19_Q_COLS:
        if df_pred[q].apply(lambda x: isinstance(x, bool)).any():
            logger.error("Recovery compute: quantile %s contains boolean value(s); rejected", q)
            return 2
        col_vals = pd.to_numeric(df_pred[q], errors="coerce")
        if col_vals.isnull().any() or not np.isfinite(col_vals.values).all():
            logger.error("Recovery compute: quantile %s has non-finite values", q)
            return 2

    # Build response — strict JSON (allow_nan=False enforces no NaN/Inf).
    rows_out = []
    df_sorted = df_pred.copy()
    df_sorted["_date_ts"] = pd.to_datetime(df_sorted["date"])
    df_sorted = df_sorted.sort_values("_date_ts")
    for _, row in df_sorted.iterrows():
        row_dict: dict = {"date": row["_date_ts"].strftime("%Y-%m-%d")}
        for q in _RECOVERY_19_Q_COLS:
            row_dict[q] = float(row[q])
        rows_out.append(row_dict)

    response = {
        "invocation_id": invocation_id,
        "model": req_model,
        "source_code": req_code,
        "issue_date": req_issue,
        "flag": 4,
        "rows": rows_out,
    }

    # Atomic write: temp file in the same dir, then os.replace.
    response_dir = os.path.dirname(os.path.abspath(response_path))
    try:
        import tempfile as _tmp

        fd, tmp_path = _tmp.mkstemp(dir=response_dir, suffix=".json.tmp")
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(response, fh, allow_nan=False)
            os.replace(tmp_path, response_path)
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise
    except Exception as e:
        logger.error("Recovery compute: failed to write response JSON: %s", e)
        return 2

    logger.info(
        "Recovery compute: wrote response for %s/%s/%s invocation=%s",
        req_model,
        req_code,
        req_issue,
        invocation_id,
    )
    return 0


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


# --------------------------------------------------------------------
# MAIN FUNCTION
# --------------------------------------------------------------------
def main():
    # --------------------------------------------------------------------
    # DEFINE WHICH MODEL TO USE
    # --------------------------------------------------------------------
    MODEL_TO_USE = os.getenv("SAPPHIRE_MODEL_TO_USE")
    logger.info("Model to use: %s", MODEL_TO_USE)

    if MODEL_TO_USE not in ["TFT", "TIDE", "TSMIXER", "ARIMA"]:
        raise ValueError("Model not supported")
    else:
        if MODEL_TO_USE == "TFT":
            predictor_class = TFTPredictor.TFTPredictor
        elif MODEL_TO_USE == "TIDE":
            predictor_class = TiDEPredictor.TiDEPredictor
        elif MODEL_TO_USE == "TSMIXER":
            predictor_class = TSMixerPredictor.TSMIXERPredictor
        elif MODEL_TO_USE == "ARIMA":
            predictor_class = predictor_ARIMA.PREDICTOR

    # ── Recovery compute-only mode: parse request before environment load ──
    _rc_req_raw = os.environ.get(_RECOVERY_COMPUTE_REQUEST_ENV)
    _rc_resp_raw = os.environ.get(_RECOVERY_COMPUTE_RESPONSE_ENV)
    _either_present = (_rc_req_raw is not None) or (_rc_resp_raw is not None)
    _rc_req_path = (_rc_req_raw or "").strip()
    _rc_resp_path = (_rc_resp_raw or "").strip()
    _recovery_mode = False
    _recovery_request: dict | None = None
    _recovery_deadline: datetime.datetime | None = None

    if _either_present:
        if not _rc_req_path or not _rc_resp_path:
            logger.error(
                "Recovery compute: partial or empty marker (request=%r, response=%r); "
                "both must be non-empty paths — failing closed",
                _rc_req_raw,
                _rc_resp_raw,
            )
            sys.exit(2)
        _recovery_mode = True

    if _recovery_mode:
        if MODEL_TO_USE == "ARIMA":
            logger.error("Recovery compute: ARIMA model blocked for recovery")
            sys.exit(2)
        if MODEL_TO_USE not in _RECOVERY_DL_MODELS:
            logger.error("Recovery compute: model %s not in supported DL models", MODEL_TO_USE)
            sys.exit(2)
        try:
            with open(_rc_req_path) as _f:
                _recovery_request = json.load(_f)
        except Exception as _e:
            logger.error("Recovery compute: cannot read request at %r: %s", _rc_req_path, _e)
            sys.exit(2)
        if not isinstance(_recovery_request, dict):
            logger.error(
                "Recovery compute: request JSON is not a mapping (got %s); required",
                type(_recovery_request).__name__,
            )
            sys.exit(2)
        _req_model = _recovery_request.get("model")
        if _req_model != MODEL_TO_USE:
            logger.error(
                "Recovery compute: request model %r != env model %r",
                _req_model,
                MODEL_TO_USE,
            )
            sys.exit(2)
        _dl_str = _recovery_request.get("deadline_utc", "")
        if not _dl_str:
            logger.error("Recovery compute: deadline_utc missing or empty; required field")
            sys.exit(2)
        try:
            _recovery_deadline = datetime.datetime.fromisoformat(_dl_str)
        except Exception:
            logger.error("Recovery compute: invalid deadline_utc %r; failing closed", _dl_str)
            sys.exit(2)
        if datetime.datetime.utcnow() >= _recovery_deadline:
            logger.error("Recovery compute: deadline already expired before environment load")
            sys.exit(2)

        # ──────────────────────────────────────────────────────────────────

    # --------------------------------------------------------------------
    # DEFINE THE HINDCAST MODE
    # --------------------------------------------------------------------
    HINDCAST_MODE = os.getenv("SAPPHIRE_HINDCAST_MODE")
    logger.debug("Prediction mode: %s", HINDCAST_MODE)
    if HINDCAST_MODE not in ["PENTAD", "DECAD"]:
        raise ValueError(
            f"Prediction mode {HINDCAST_MODE!r} is not supported.\nPlease choose one of the following prediction modes: PENTAD, DECAD"
        )
    else:
        logger.debug("Prediction mode: %s", HINDCAST_MODE)
        gen_mode = canonical_ml_mode(MODEL_TO_USE, HINDCAST_MODE)
        if gen_mode == "PENTAD":
            forecast_horizon = 6
        else:
            forecast_horizon = 11
    # --------------------------------------------------------------------
    # INITIALIZE THE ENVIRONMENT
    # --------------------------------------------------------------------
    # Specify the path to the .env file
    sl.load_environment()

    # ── Budget re-check after environment load (deadline may have passed) ──
    if (
        _recovery_mode
        and _recovery_deadline is not None
        and datetime.datetime.utcnow() >= _recovery_deadline
    ):
        logger.error("Recovery compute: deadline expired after environment load; failing closed")
        sys.exit(2)

    # ── Recovery compute-only gate checks (after env load, before model calls) ──
    if _recovery_mode:
        _api_en = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
        if not _api_en:
            logger.error("Recovery compute: SAPPHIRE_API_ENABLED=false; blocked")
            sys.exit(2)
        _ml_raw = os.getenv("ieasyhydroforecast_run_ML_models", "").strip()
        if _ml_raw.lower() != "true":
            logger.error(
                "Recovery compute: ieasyhydroforecast_run_ML_models not affirmatively true; blocked"
            )
            sys.exit(2)
        _avail_raw = os.getenv("ieasyhydroforecast_available_ML_models", "").strip()
        _avail_set = frozenset(m.strip().upper() for m in _avail_raw.split(",") if m.strip())
        if MODEL_TO_USE not in _avail_set:
            logger.error(
                "Recovery compute: %s not in available_ML_models %s; blocked",
                MODEL_TO_USE,
                _avail_set,
            )
            sys.exit(2)
        from scr.utils_ml_forecast import get_permitted_station_codes as _get_permitted

        _permitted_codes = _get_permitted()
        _req_code_str = str(_recovery_request.get("source_code", ""))
        if _permitted_codes is None or _req_code_str not in _permitted_codes:
            logger.error(
                "Recovery compute: code %s not in permitted station codes; blocked", _req_code_str
            )
            sys.exit(2)
    # ──────────────────────────────────────────────────────────────────────────

    # --------------------------------------------------------------------
    # LOAD IN ENVIROMENT VARIABLES
    # --------------------------------------------------------------------

    intermediate_data_path = os.getenv("ieasyforecast_intermediate_data_path")

    MODELS_AND_SCALERS_PATH = os.getenv("ieasyhydroforecast_models_and_scalers_path")
    PATH_TO_STATIC_FEATURES = os.getenv("ieasyhydroforecast_PATH_TO_STATIC_FEATURES")
    logger.debug("MODELS_AND_SCALERS_PATH: %s", MODELS_AND_SCALERS_PATH)
    logger.debug("PATH_TO_STATIC_FEATURES: %s", PATH_TO_STATIC_FEATURES)
    PATH_TO_STATIC_FEATURES = os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES)
    # Test if path exists
    if not os.path.exists(PATH_TO_STATIC_FEATURES):
        raise FileExistsError(f"Directory {PATH_TO_STATIC_FEATURES} not found.")

    PATH_TO_SCALER = os.getenv("ieasyhydroforecast_PATH_TO_SCALER_" + MODEL_TO_USE)
    # Append Decad to the scaler path if the prediction mode is DECAD
    if gen_mode == "DECAD" and MODEL_TO_USE != "ARIMA":
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

    # PATH_TO_MODEL= os.getenv('ieasyhydroforecast_PATH_TO_' + MODEL_TO_USE)
    # PATH_TO_MODEL = os.path.join(PATH_TO_SCALER, PATH_TO_MODEL)
    # Test if the directory exists
    if not os.path.exists(PATH_TO_MODEL):
        raise FileNotFoundError(f"Directory {PATH_TO_MODEL} not found.")
    logger.debug("PATH_TO_MODEL: %s", PATH_TO_MODEL)

    # CODES_HINDECAST = os.getenv('ieasyhydroforecast_CODES_HINDECAST')
    HRU_ML_MODELS = os.getenv("ieasyhydroforecast_HRU_CONTROL_MEMBER")

    rivers_to_predict_pentad, rivers_to_predict_decad, hydroposts_available_for_ml_forecasting = (
        utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts()
    )
    # Combine rivers_to_predict_pentad and rivers_to_predict_decad to get all rivers to predict, only keep unique values
    rivers_to_predict = list(set(rivers_to_predict_pentad + rivers_to_predict_decad))
    # select only codes which the model can predict.
    mask_predictable = hydroposts_available_for_ml_forecasting[MODEL_TO_USE]
    codes_model_can_predict = hydroposts_available_for_ml_forecasting[mask_predictable][
        "code"
    ].tolist()
    rivers_to_predict = list(set(rivers_to_predict) & set(codes_model_can_predict))
    # convert to int
    rivers_to_predict = [int(code) for code in rivers_to_predict]

    # read the ieasyhydroforecast_NEW_STATIONS variable
    # this variables cantains the codes which should be hindcasted
    # this is only used when a new station is added to the config file and we only hindcast this station to save time
    NEW_STATIONS = os.getenv("ieasyhydroforecast_NEW_STATIONS")
    if NEW_STATIONS != "None":
        new_stations = [int(code) for code in NEW_STATIONS.split(",")]
        rivers_to_predict = list(set(rivers_to_predict) & set(new_stations))

    logger.debug("Rivers to predict pentad: %s", rivers_to_predict_pentad)
    logger.debug("Rivers to predict decad: %s", rivers_to_predict_decad)
    logger.debug("Rivers to predict: %s", rivers_to_predict)
    logger.debug(
        "Hydroposts available for ML forecasting: \n%s", hydroposts_available_for_ml_forecasting
    )

    # ── Recovery: verify requested code is in model-specific config BEFORE model load ──
    if _recovery_mode:
        try:
            _req_code_int_pre = int(str(_recovery_request.get("source_code", "")))
        except (TypeError, ValueError):
            logger.error("Recovery compute: invalid source_code in request; failing closed")
            sys.exit(2)
        if _req_code_int_pre not in rivers_to_predict:
            logger.error(
                "Recovery compute: code %s not available for model %s per config; "
                "blocked before model load",
                _req_code_int_pre,
                MODEL_TO_USE,
            )
            sys.exit(2)
        # Confine to exactly the requested code — no twins, no extra sources.
        rivers_to_predict = [_req_code_int_pre]
    # ──────────────────────────────────────────────────────────────────────────

    # --------------------------------------------------------------------
    # READ IN FORCING DATA
    # --------------------------------------------------------------------
    # start date is the date where the first forecast is made
    # NOTE: The start date should be +lookback days before the first date in the era5 data
    # Here we hardcode the lookback days to 60
    start_date = os.getenv("ieasyhydroforecast_START_DATE")
    # end date is the date where the last forecast is made
    # NOTE: The end date should be at maximum the last date in the era5 data - forecast horizon
    end_date = os.getenv("ieasyhydroforecast_END_DATE")

    # --- Read ERA5 forcing data (API-primary, CSV fallback) ---
    # CSV fallback paths (used only when SAPPHIRE_API_ENABLED=false)
    PATH_ERA5_REANALYSIS = os.getenv("ieasyhydroforecast_OUTPUT_PATH_REANALYSIS")
    PATH_ERA5_REANALYSIS = os.path.join(intermediate_data_path, PATH_ERA5_REANALYSIS)
    csv_path_t = os.path.join(PATH_ERA5_REANALYSIS, f"{HRU_ML_MODELS}_T_reanalysis.csv")
    csv_path_p = os.path.join(PATH_ERA5_REANALYSIS, f"{HRU_ML_MODELS}_P_reanalysis.csv")

    era5_data_transformed = read_meteo_data_combined(
        site_codes=[str(c) for c in rivers_to_predict],
        csv_path_t=csv_path_t,
        csv_path_p=csv_path_p,
    )

    # check if the start and end date is in the era5 data
    # Recovery mode defers all forcing coverage checks to _run_recovery_compute,
    # which validates against actual predictor requirements (icl + max_ws - 1)
    # rather than the ordinary-path hardcoded 60-day minimum and end-date horizon.
    if not _recovery_mode:
        if pd.to_datetime(start_date) < era5_data_transformed["date"].min() + pd.DateOffset(
            days=60
        ):
            raise ValueError(
                f"The start date {start_date} is before the first date in the era5 data {era5_data_transformed['date'].min()} + 60 days"
            )
        if pd.to_datetime(end_date) > era5_data_transformed["date"].max() - pd.DateOffset(
            days=forecast_horizon
        ):
            raise ValueError(
                f"The end date {end_date} is after the last date in the era5 data {era5_data_transformed['date'].max()} - forecast horizon {forecast_horizon}"
            )

    # --------------------------------------------------------------------
    # Preprocessing
    # --------------------------------------------------------------------
    # STATIC FEATURES

    static_features = pd.read_csv(PATH_TO_STATIC_FEATURES)

    if "cluster" in static_features.columns:
        static_features = static_features.drop(columns=["cluster"])
    if "log_q" in static_features.columns:
        static_features = static_features.drop(columns=["log_q"])
    if "CODE" in static_features.columns:
        static_features = static_features.rename(columns={"CODE": "code"})

    static_features.index = static_features["code"]

    if MODEL_TO_USE == "ARIMA":
        pass
    else:
        scaler_discharge = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_discharge.csv"))
        scaler_discharge.index = scaler_discharge["Unnamed: 0"].astype(int)
        scaler_era5 = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_era5.csv"))
        scaler_era5.index = scaler_era5["Unnamed: 0"]
        scaler_static = pd.read_csv(os.path.join(PATH_TO_SCALER, "scaler_stats_static.csv"))
        scaler_static.index = scaler_static["Unnamed: 0"]

    # Read observed discharge data from API (default) or CSV fallback
    # The function uses SAPPHIRE_API_ENABLED env var to determine data source
    observed_discharge = fl.read_daily_discharge_data()

    # Note: read_daily_discharge_data returns 'code' as string, convert to int
    observed_discharge["code"] = observed_discharge["code"].astype(int)
    era5_data_transformed["code"] = era5_data_transformed["code"].astype(int)

    # Fill gaps in forcing data before PET so PET inherits filled T.
    # Hindcast is fully historical -> lenient everywhere (recent limit == past limit).
    FORCING_GAP_RECENT_THRESHOLD = int(
        os.getenv("ieasyhydroforecast_forcing_gap_fill_recent_day_threshold", 7)
    )
    FORCING_GAP_LIMIT_PAST = int(os.getenv("ieasyhydroforecast_forcing_gap_limit_past", 3))
    era5_data_transformed = utils_ml_forecast.fill_forcing_gaps(
        era5_data_transformed,
        reference_date=pd.to_datetime(era5_data_transformed["date"]).max(),
        recent_day_threshold=FORCING_GAP_RECENT_THRESHOLD,
        gap_limit_recent=FORCING_GAP_LIMIT_PAST,  # lenient everywhere
        gap_limit_past=FORCING_GAP_LIMIT_PAST,
    )

    # --------------------------------------------------------------------
    # Calculate PET Oudin and Daylight Hours
    # --------------------------------------------------------------------
    for code in era5_data_transformed["code"].unique():
        lat = static_features.loc[code]["LAT"]
        lon = static_features.loc[code]["LON"]
        pet_oudin = utils_ml_forecast.calculate_pet_oudin(
            era5_data_transformed[era5_data_transformed["code"] == code], lat
        )
        era5_data_transformed.loc[era5_data_transformed["code"] == code, "PET"] = pet_oudin
        era5_data_transformed.loc[era5_data_transformed["code"] == code, "daylight_hours"] = (
            utils_ml_forecast.calculate_daylight_hours(
                lat, lon, era5_data_transformed[era5_data_transformed["code"] == code]
            )
        )

    # --------------------------------------------------------------------
    # LOAD MODELS AND MAKE PREDICTORS
    # --------------------------------------------------------------------
    # MODEL PREDICTOR
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
        predictor = predictor_class(
            model=model,
            scalers=scalers,
            static_features=static_features,
            dl_config_params=None,
            unique_id_col="code",
        )

    # ── Recovery compute-only: early return before hindcast loop/persistence ──
    if _recovery_mode:
        observed_discharge["code"] = observed_discharge["code"].astype(int)
        era5_data_transformed["code"] = era5_data_transformed["code"].astype(int)
        _exit_code = _run_recovery_compute(
            request=_recovery_request,
            predictor=predictor,
            era5_data=era5_data_transformed,
            observed_discharge=observed_discharge,
            rivers_to_predict=rivers_to_predict,
            response_path=_rc_resp_path,
            deadline=_recovery_deadline,
        )
        sys.exit(_exit_code)
    # ──────────────────────────────────────────────────────────────────────────

    # --------------------------------------------------------------------
    # HINDECAST
    # --------------------------------------------------------------------
    THRESHOLD_MISSING_DAYS = os.getenv("ieasyhydroforecast_THRESHOLD_MISSING_DAYS_" + MODEL_TO_USE)
    THRESHOLD_MISSING_DAYS_END = os.getenv("ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END")

    # thresholds to ints
    THRESHOLD_MISSING_DAYS = int(THRESHOLD_MISSING_DAYS)
    THRESHOLD_MISSING_DAYS_END = int(THRESHOLD_MISSING_DAYS_END)

    observed_discharge["date"] = pd.to_datetime(observed_discharge["date"])
    era5_data_transformed["date"] = pd.to_datetime(era5_data_transformed["date"])

    hindecast_daily_df = pd.DataFrame()

    logger.debug(f"Starting Hindcast for the dates: {start_date} to {end_date}")
    input_chunk_length = predictor.get_input_chunk_length()

    start_date_string = start_date
    end_date_string = end_date

    start_date = pd.to_datetime(start_date) - pd.DateOffset(days=input_chunk_length + 1)
    end_date = pd.to_datetime(end_date)

    logger.debug("Last Observation date: %s", observed_discharge["date"].max())

    # filter observed discharge to be only in the range of the start and end date
    observed_discharge = observed_discharge[observed_discharge["date"] >= start_date].copy()
    observed_discharge = observed_discharge[observed_discharge["date"] <= end_date].copy()

    logger.debug("Start date: %s", start_date)
    logger.debug("End date: %s", end_date)

    logger.debug("Observation discharge: %s", observed_discharge)

    timer_start = datetime.datetime.now()
    for code in rivers_to_predict:
        discharge = observed_discharge[observed_discharge["code"] == code].copy()

        # ensure that the discharge data has no missing dates in the range of the start and end date + forecast horizon
        # if it has missing dates, then fill the missing dates with nan
        # Create complete date range
        date_range = pd.date_range(
            start=start_date, end=end_date + pd.Timedelta(days=forecast_horizon), freq="D"
        )

        # ensure that the discharge data has no missing dates in the range of the start and end date + forecast horizon
        # if it has missing dates, then fill the missing dates with nan
        date_range = pd.date_range(
            start=start_date, end=end_date + pd.Timedelta(days=forecast_horizon), freq="D"
        )
        discharge.set_index("date", inplace=True)
        discharge = discharge.reindex(date_range)
        discharge.reset_index(inplace=True)
        discharge.rename(columns={"index": "date"}, inplace=True)
        discharge["code"] = code

        era5 = era5_data_transformed[era5_data_transformed["code"] == code].copy()

        if era5.isnull().values.any():
            logger.debug(f"Era5 data for code {code} has missing values")
            logger.debug("Number of missing values: %s", era5.isnull().sum())

        if discharge.isnull().values.any():
            logger.debug(f"Discharge data for code {code} has missing values")
            logger.debug("Length of discharge: %s", len(discharge))
            logger.debug("Number of missing values: %s", discharge.isnull().sum())

        # make hindcast
        hindcast_code = predictor.hindcast(
            df_rivers_org=discharge,
            df_covariates=era5,
            code=code,
            n=forecast_horizon,
        )

        hindcast_code["code"] = code

        logger.debug("Hindcast for code: %s", code)
        logger.debug("Hindcast: %s", hindcast_code)

        hindecast_daily_df = pd.concat([hindecast_daily_df, hindcast_code], axis=0)

        # Check if for this code we have a twin vitrual gauge which is > 0
        test_value = hydroposts_available_for_ml_forecasting.loc[
            hydroposts_available_for_ml_forecasting["code"] == str(code),
            "virtual_station_name_twin",
        ].iloc[0]
        if test_value is not False:
            logger.debug("Forecast for twin virtual gauge: %s", test_value)

            hindcast_code["code"] = int(test_value)
            hindecast_daily_df = pd.concat([hindecast_daily_df, hindcast_code], axis=0)

            logger.debug("Copied data and appended: %s", hindcast_code)

    timer_end = datetime.datetime.now()

    # Flag convention for hindcast:
    #   3 = NaN in any quantile column (forecast failed for this date)
    #   4 = valid forecast (all quantiles present)
    # Note: make_forecast.py uses 0 (ok), 1 (NaN), 2 (error).
    # Hindcast uses 3/4 to distinguish hindcast rows from operational rows
    # when both are stored in the same API table.
    hindecast_daily_df["flag"] = 4
    pred_cols = [col for col in hindecast_daily_df.columns if "Q" in col]
    hindecast_daily_df.loc[hindecast_daily_df[pred_cols].isna().any(axis=1), "flag"] = 3

    print(f"Time to make hindcast: {timer_end - timer_start}")
    # --------------------------------------------------------------------
    # SAVE HINDECAST
    # --------------------------------------------------------------------
    # Path to the output directory
    OUTPUT_PATH_DISCHARGE = os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE")
    OUTPUT_PATH_DISCHARGE = os.path.join(intermediate_data_path, OUTPUT_PATH_DISCHARGE)
    OUTPUT_PATH_DISCHARGE = os.path.join(OUTPUT_PATH_DISCHARGE, "hindcast", MODEL_TO_USE)
    # Test if path exists and create it if it doesn't
    if not os.path.exists(OUTPUT_PATH_DISCHARGE):
        os.makedirs(OUTPUT_PATH_DISCHARGE, exist_ok=True)

    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    # hindecast_df.to_csv(os.path.join(OUTPUT_PATH_DISCHARGE, f'{MODEL_TO_USE}_{HINDCAST_MODE}_hindcast_{start_date}_{end_date}.csv'), index=False)

    # --- 1. Write to SAPPHIRE API (primary path) ---
    horizon = "pentad" if gen_mode == "PENTAD" else "decade"
    api_ok = False
    if SAPPHIRE_API_AVAILABLE:
        try:
            api_ok = _write_ml_forecast_to_api(hindecast_daily_df, horizon, MODEL_TO_USE)
            if api_ok:
                logger.info(
                    "Wrote %d hindcast rows to API for %s %s",
                    len(hindecast_daily_df),
                    MODEL_TO_USE,
                    HINDCAST_MODE,
                )
            else:
                logger.error(
                    "hindcast_ML_models: API write returned failure for %s %s "
                    "— hindcast rows may not be visible to fill_ml_gaps on next run",
                    MODEL_TO_USE,
                    HINDCAST_MODE,
                )
        except Exception as e:
            logger.error("Failed to write hindcast to API: %s", e)

    # --- 2. Write to CSV (archive/fallback) ---
    hindecast_daily_df = normalize_ml_csv_columns(hindecast_daily_df)
    hindecast_daily_df.to_csv(
        os.path.join(
            OUTPUT_PATH_DISCHARGE,
            f"{MODEL_TO_USE}_{gen_mode}_hindcast_daily_{start_date_string}_{end_date_string}.csv",
        ),
        index=False,
    )

    if not api_ok:
        logger.error(
            "hindcast_ML_models: Hindcast data for %s/%s exists only in CSV "
            "— API write failed or unavailable; fill_ml_gaps gap detection "
            "may be incomplete until data is in the API",
            MODEL_TO_USE,
            HINDCAST_MODE,
        )

    logger.info("Hindcast Done!")


if __name__ == "__main__":
    main()
