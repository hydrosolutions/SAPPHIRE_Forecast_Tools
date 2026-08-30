##################################################
# Run Long Term Forecast
##################################################

## How to run this script:
# Set the environment variable ieasyhydroforecast_env_file_path to point to your .env file
# Then run the script with:
# ieasyhydroforecast_env_file_path="path_to_env" lt_forecast_mode=monthly python run_forecast.py


import json
import logging
from datetime import datetime

# Suppress graphviz debug warnings BEFORE importing any modules that use graphviz
logging.getLogger("graphviz").setLevel(logging.WARNING)

import os
import sys
import time
import traceback
from typing import Any

import numpy as np
import pandas as pd
from __init__ import (
    SAPPHIRE_API_AVAILABLE,
    initialize_today,
    logger,
)
from config_forecast import ForecastConfig
from data_interface import BasePredictorDataInterface, DataInterface, DataInterfaceDB

# Import forecast models
from lt_recovery import apply_success_flag
from lt_utils import (
    check_valid_forecast_issue_date,
    create_model_instance,
    save_forecast,
)
from post_process_lt_forecast import post_process_lt_forecast

# set lt_forecasting logger level
logger_lt = logging.getLogger("lt_forecasting")
logger_lt.setLevel(logging.INFO)

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


def _read_station_codes():
    """Read station codes from the station selection config file.

    Handles both list format ([12345, 67890]) and dict format
    ({"12345": {...}, "67890": {...}}). The dict format is used by
    some ML configs — iterating a dict yields its keys.
    """
    config_path = os.path.join(
        os.getenv("ieasyforecast_configuration_path", ""),
        os.getenv("ieasyforecast_config_file_station_selection", ""),
    )
    with open(config_path) as f:
        config = json.load(f)
    raw = config.get("stationsID", [])
    codes = [str(c) for c in raw]
    if not codes:
        logger.warning(
            "No station codes found in %s — no org filter applied",
            config_path,
        )
    else:
        logger.info("Read %d station codes for org-scoped filtering", len(codes))
    return codes


def _add_climatological_quantile_bounds(
    forecast: pd.DataFrame,
    temporal_data: pd.DataFrame,
    model_name: str,
    today: pd.Timestamp,
) -> pd.DataFrame:
    """Add climatological Q25/Q75 bounds to a GBT forecast DataFrame.

    Uses the standard deviation of historical observed monthly mean discharge
    (the same 0.674 * std pattern used in data_reader.py and aggregation.py)
    to create uncertainty bounds for GBT-family models that lack native
    quantile estimation.

    Args:
        forecast: DataFrame with Q_{model_name}, code, date, valid_from,
            valid_to, flag columns.
        temporal_data: Daily DataFrame with date, code, discharge columns
            (full historical record).
        model_name: Model identifier (e.g. "GBT", "SM_GBT").
        today: Forecast issue date.

    Returns:
        forecast DataFrame with Q25 and Q75 columns added. Groups with
        insufficient data (< 3 years) will have NaN bounds.
    """
    if forecast.empty:
        return forecast

    main_q_col = f"Q_{model_name}"
    if main_q_col not in forecast.columns:
        logger.warning("Cannot add quantile bounds: %s not in forecast columns", main_q_col)
        return forecast

    # Extract target month from the forecast's valid_from column
    if "valid_from" not in forecast.columns:
        logger.warning("Cannot add quantile bounds: valid_from column missing")
        return forecast

    target_months = pd.to_datetime(forecast["valid_from"]).dt.month

    # Filter temporal_data to rows with valid discharge
    discharge = temporal_data[["date", "code", "discharge"]].copy()
    discharge = discharge.dropna(subset=["discharge"])

    if discharge.empty:
        logger.warning("Cannot add quantile bounds: no valid discharge data")
        return forecast

    discharge["date"] = pd.to_datetime(discharge["date"])
    discharge["year"] = discharge["date"].dt.year
    discharge["month"] = discharge["date"].dt.month
    discharge["days_in_month"] = discharge["date"].dt.days_in_month

    # Aggregate to monthly means per (code, year, month)
    monthly = (
        discharge.groupby(["code", "year", "month"])
        .agg(
            discharge_mean=("discharge", "mean"),
            non_missing_days=("discharge", "count"),
            days_in_month=("days_in_month", "first"),
        )
        .reset_index()
    )

    # Filter: require >= 50% non-missing days
    monthly = monthly[monthly["non_missing_days"] >= monthly["days_in_month"] * 0.5]

    # Leave-one-out: exclude the forecast year
    forecast_year = today.year
    monthly = monthly[monthly["year"] != forecast_year]

    if monthly.empty:
        logger.warning("Cannot add quantile bounds: no valid monthly data after filtering")
        return forecast

    # Compute std per (code, month) — ddof=1 (sample std) to match
    # long-term postprocessing convention (data_reader.py, aggregation.py)
    stats = (
        monthly.groupby(["code", "month"])
        .agg(
            std_monthly=("discharge_mean", "std"),
            n_years=("discharge_mean", "count"),
        )
        .reset_index()
    )

    # Require at least 3 years of data
    stats = stats[stats["n_years"] >= 3]

    if stats.empty:
        logger.warning("Cannot add quantile bounds: no (code, month) groups with >= 3 years")
        return forecast

    # Merge std into forecast on (code, month)
    forecast = forecast.copy()
    forecast["_target_month"] = target_months
    forecast = forecast.merge(
        stats[["code", "month", "std_monthly"]],
        left_on=["code", "_target_month"],
        right_on=["code", "month"],
        how="left",
    )

    # Compute Q25 and Q75
    forecast["Q25"] = forecast[main_q_col] - 0.674 * forecast["std_monthly"]
    forecast["Q75"] = forecast[main_q_col] + 0.674 * forecast["std_monthly"]

    # Clip to >= 0 (discharge cannot be negative)
    forecast["Q25"] = forecast["Q25"].clip(lower=0)
    forecast["Q75"] = forecast["Q75"].clip(lower=0)

    # Clean up temporary columns added by this function and the merge
    forecast = forecast.drop(columns=["_target_month", "month", "std_monthly"], errors="ignore")

    n_with_bounds = forecast["Q25"].notna().sum()
    logger.info(
        "Added climatological Q25/Q75 bounds for %d/%d forecast rows (model: %s)",
        n_with_bounds,
        len(forecast),
        model_name,
    )

    return forecast


def run_single_model(
    data_interface: DataInterface | DataInterfaceDB,
    forecast_configs: ForecastConfig,
    model_name: str,
    temporal_data: pd.DataFrame,
    static_data: pd.DataFrame,
    offset_base: int,
    offset_discharge: int,
    station_codes: list[str] | None = None,
    recovery_flag: int | None = None,
) -> dict[str, Any]:
    """
    Run a single forecast model and return the results.

    Args:
        recovery_flag: When set, rows carrying a value are persisted with this
            flag instead of 0. Used by the dated recovery path to mark
            regenerated rows. Rows with a missing/NaN value keep flag 2.
    """

    # Load configurations
    configs = forecast_configs.get_model_specific_config(model_name=model_name)
    model_type = configs["general_config"]["model_type"]

    # Set the model path
    model_path = forecast_configs.all_paths.get(model_name)
    # move up one level to model home path
    model_home_path = os.path.dirname(model_path)
    configs["path_config"]["model_home_path"] = model_home_path

    #################################################
    # This part will be replaced by a database query in future [DATABASE INTEGRATION]
    #################################################
    model_dependencies = forecast_configs.get_model_dependencies()
    all_dependencies_forecast_paths = []
    all_dependencies_hindcast_paths = []
    all_dependencies_models = []
    for dep in model_dependencies.get(model_name, []):
        all_dependencies_models.append(dep)
        dep_path = forecast_configs.get_output_path(model_name=dep)
        dep_file_forecast = os.path.join(dep_path, f"{dep}_forecast.csv")
        dep_file_hindcast = os.path.join(dep_path, f"{dep}_hindcast.csv")
        if not os.path.exists(dep_file_forecast):
            logger.error(f"Dependency file {dep_file_forecast} for model {model_name} not found.")
        if not os.path.exists(dep_file_hindcast):
            logger.error(f"Dependency file {dep_file_hindcast} for model {model_name} not found.")

        all_dependencies_forecast_paths.append(dep_file_forecast)
        all_dependencies_hindcast_paths.append(dep_file_hindcast)

    # Used by the GBT LR models which take the predictions of other models as input features
    configs["path_config"]["path_to_lr_predictors"] = all_dependencies_forecast_paths
    # Used by the Uncertainty Mixture models which take the hindcast of other models as input features
    # This is needed to compute the uncertainty based on past model errors
    configs["path_config"]["path_to_base_predictors"] = all_dependencies_hindcast_paths

    #################################################
    if len(all_dependencies_models) > 0:
        base_predictor_interface = BasePredictorDataInterface(station_codes=station_codes)

        if SAPPHIRE_API_AVAILABLE:
            base_predictor_data, base_model_cols = (
                base_predictor_interface.load_all_dependencies_database(
                    all_dependencies_models=all_dependencies_models,
                    horizon_type="month",
                    horizon_value=forecast_configs.get_operational_month_lead_time(),
                )
            )
            logger.info(f"Loaded base predictor data from database for model {model_name}")
        else:
            base_predictor_data, base_model_cols = (
                base_predictor_interface.load_all_dependencies_csv(
                    all_dependencies_models=all_dependencies_models,
                    all_dependencies_paths=all_dependencies_hindcast_paths,
                )
            )
            logger.info(f"Loaded base predictor data from CSV for model {model_name}")

        logger.info(f"Base predictor columns: {base_model_cols}")
        logger.info(f"Base predictor data shape: {base_predictor_data.shape}")
        logger.info(
            f"Percentage of rows with NaN values in base predictor data: {base_predictor_data.isna().mean().mean() * 100:.2f}%"
        )

        logger.info(f"Running model: {model_name} of type {model_type}")

    else:
        base_predictor_data = None
        base_model_cols = []

    data_dependencies = forecast_configs.get_data_dependencies(model_name=model_name)
    can_be_run = True

    for input_type, offset in data_dependencies.items():
        if input_type == "SnowMapper":
            # Extend base data with snow data
            snow_HRUs = configs["path_config"].get("snow_HRUs", [])
            snow_variables = configs["path_config"].get("snow_variables", [])
            snow_result = data_interface.extend_base_data_with_snow(
                base_data=temporal_data, HRUs_snow=snow_HRUs, snow_variables=snow_variables
            )
            temporal_data = snow_result["temporal_data"]
            offset_snow = snow_result["offset_date_snow"]
            logger.info(f"Extended data with snow. Offset days: {offset_snow}")
            if offset_snow is not None and offset_snow > offset:
                logger.warning(
                    f"Snow data offset ({offset_snow}) is greater than required offset ({offset})"
                )
                can_be_run = False
        elif input_type == "Discharge":
            # Here we could implement additional logic for discharge data if needed
            if offset_discharge > offset:
                logger.warning(
                    f"Discharge data offset ({offset_discharge}) is greater than required offset ({offset})"
                )
                can_be_run = False
        elif input_type == "EMCWF_Forecast":
            # Here we could implement additional logic for EMCWF forecast data if needed
            if offset_base > offset:
                logger.warning(
                    f"Base data offset ({offset_base}) is greater than required offset ({offset})"
                )
                can_be_run = False
        else:
            logger.warning(f"Unknown data dependency type: {input_type}")

    logger.info(
        f"Head of temporal data after processing dependencies for model {model_name}:\n{temporal_data.head()}"
    )

    today = check_valid_forecast_issue_date(
        forecast_configs=forecast_configs, model_name=model_name
    )

    # None means this model is not scheduled for today — skip gracefully
    if today is None:
        logger.warning(f"Model {model_name} not scheduled for today, skipping")
        return False  # skip is a failure

    logger.info(f"Can model {model_name} be run? {'Yes' if can_be_run else 'No'}")

    if can_be_run:
        # Create model instance
        model_instance = create_model_instance(
            model_type=model_type,
            model_name=model_name,
            configs=configs,
            data=temporal_data,
            static_data=static_data,
            base_predictors=base_predictor_data,
            base_model_names=base_model_cols,
        )

        # Run forecast
        forecast = model_instance.predict_operational(today=today)
        forecast = forecast.round(2)

        # where Q_model_name is Nan, set flag to 2, else 0 (0 = forecast produced, 2 = no forecast produced, missing data)
        # A recovery run overrides the "produced" value only (see lt_recovery).
        main_q_col = f"Q_{model_name}"
        if main_q_col not in forecast.columns:
            logger.error(
                f"Expected main Q column {main_q_col} not found in forecast for model {model_name}. Available columns: {forecast.columns}"
            )
            forecast["flag"] = 2
            success = False
        else:
            forecast = apply_success_flag(forecast, main_q_col, recovery_flag)
            success = True

        # Add climatological quantile bounds for GBT-family models (monthly mode)
        is_monthly = forecast_configs.get_calendar_month_adjustment()
        if model_type == "sciregressor" and success and is_monthly:
            forecast = _add_climatological_quantile_bounds(
                forecast=forecast,
                temporal_data=temporal_data,
                model_name=model_name,
                today=today,
            )

    else:
        logger.error(f"Cannot run model {model_name} due to missing or outdated data.")
        forecast = pd.DataFrame()  # Empty DataFrame as placeholder
        forecast["flag"] = 2
        success = False

    logger.info(f"Forecast head before post-processing for model {model_name}:\n{forecast.head()}")
    # Postprocess the forecasts to calendar months.
    forecast = post_process_lt_forecast(
        forecast_config=forecast_configs,
        observed_discharge_data=temporal_data,
        raw_forecast=forecast,
    )
    # Round all numeric columns to 2 decimals after post-processing
    numeric_cols = forecast.select_dtypes(include=[np.number]).columns
    forecast[numeric_cols] = forecast[numeric_cols].round(2)

    logger.info(f"Forecast head after post-processing for model {model_name}:\n{forecast.head()}")
    #################################################
    # Save Forecast to Database and CSV
    #################################################
    output_path = forecast_configs.get_output_path(model_name=model_name)
    horizon_value = forecast_configs.get_operational_month_lead_time()
    horizon_type = forecast_configs.get_horizon_type()

    # Save forecast (DB + CSV parallel track)
    save_success = save_forecast(
        forecast_df=forecast,
        model_name=model_name,
        output_path=output_path,
        horizon_type=horizon_type,
        horizon_value=horizon_value,
        is_hindcast=False,
    )

    if not save_success:
        logger.warning(f"Forecast save had issues for model {model_name}")

    # Return success
    return success


def run_forecast(
    forecast_all: bool = True,
    models_to_run: list[str] | None = None,
    forecast_mode: str = None,
    recovery_flag: int | None = None,
):
    # Setup Environment
    sl.load_environment()
    station_codes = _read_station_codes()

    # Now we setup the configurations
    forecast_config = ForecastConfig()

    if forecast_mode is None:
        forecast_mode = os.getenv("lt_forecast_mode")

    forecast_config.load_forecast_config(forecast_mode=forecast_mode)
    forcing_HRU = forecast_config.get_forcing_HRU()

    if models_to_run is None:
        models_to_run = []

    if forecast_all:
        if len(models_to_run) > 0:
            raise ValueError("If forecast_all is True, models_to_run should be empty.")

        models_to_run = forecast_config.get_models_to_run()

    logger.info(
        f"Starting forecast run. Forecast all: {forecast_all}. Models to run: {models_to_run}"
    )

    # Data Interface - use DB interface if SAPPHIRE API is available
    if SAPPHIRE_API_AVAILABLE:
        logger.info("Using DataInterfaceDB (database backend)")
        data_interface = DataInterfaceDB(station_codes=station_codes)
    else:
        logger.info("Using DataInterface (CSV backend)")
        data_interface = DataInterface()
    base_data_dict = data_interface.get_base_data(forcing_HRU=forcing_HRU)

    temporal_data = base_data_dict["temporal_data"]
    static_data = base_data_dict["static_data"]
    offset_base = base_data_dict["offset_date_base"]
    offset_discharge = base_data_dict["offset_date_discharge"]

    ordered_models = forecast_config.get_model_execution_order()
    execution_is_success = {}
    model_dependencies = forecast_config.get_model_dependencies()

    if not forecast_all:
        # Filter ordered_models to only include those in models_to_run
        ordered_models = [m for m in ordered_models if m in models_to_run]
        # we check dependencies again in the run_single_model function
        ignore_initial_dependencies = True
    else:
        ignore_initial_dependencies = False

    for model_name in ordered_models:
        # Wait 5 seconds between model runs to avoid potential file access conflicts
        time.sleep(5)
        dependencies = model_dependencies.get(model_name, [])
        # Check if dependencies were successful
        deps_success = all(execution_is_success.get(dep, False) for dep in dependencies)

        if not deps_success and not ignore_initial_dependencies:
            logger.error(f"Skipping model {model_name} due to failed dependencies: {dependencies}")
            execution_is_success[model_name] = False
            continue

        try:
            sucess = run_single_model(
                data_interface=data_interface,
                forecast_configs=forecast_config,
                model_name=model_name,
                temporal_data=temporal_data.copy(),
                static_data=static_data,
                offset_base=offset_base,
                offset_discharge=offset_discharge,
                station_codes=station_codes,
                recovery_flag=recovery_flag,
            )
            execution_is_success[model_name] = sucess
        except Exception as e:
            logger.error(f"Error running model {model_name}: {e}")
            # get the full traceback
            traceback_str = traceback.format_exc()
            logger.error(f"Traceback: {traceback_str}")
            execution_is_success[model_name] = False

    # Print summary
    logger.info("\n" + "=" * 50)
    logger.info("FORECAST SUMMARY")
    logger.info("=" * 50)
    for model_name, success in execution_is_success.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"{model_name}: {status}")
    logger.info("=" * 50 + "\n")

    logger.info("Forecast run completed.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run forecasts for long-term models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run forecasts for all models
  python run_forecast.py --all
  
  # Run forecasts for specific models
  python run_forecast.py --models LinearRegressionModel SciRegressor
  
  # With environment variables
  ieasyhydroforecast_env_file_path="path/to/.env" lt_forecast_mode=monthly python run_forecast.py --all
        """,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Run forecasts for all models")
    group.add_argument(
        "--models", nargs="+", metavar="MODEL_NAME", help="List of model names to forecast"
    )

    group.add_argument(
        "--today",
        type=str,
        help='Override the "today" date for the forecast in YYYY-MM-DD format (useful for testing or backtesting)',
    )

    parser.add_argument(
        "--recover",
        action="store_true",
        help=(
            "Operator-invoked recovery of ONE missed long-term month. Requires "
            "--today. Refuses if any member row already exists for that issue "
            "date, marks regenerated rows with flag=1, and reads the rows back "
            "from the database before reporting success. Exit codes: 0 success, "
            "1 ran but nothing proven written, 2 refused (nothing was run)."
        ),
    )

    args = parser.parse_args()

    # Determine recalibrate_all flag and models to run
    recalibrate_all = args.all or args.today is not None
    models_to_run = args.models if args.models else []

    # The recovery path parses its own date, so that a malformed or impossible
    # one (2026-02-31) exits with the documented refusal code instead of
    # crashing in strptime below.
    if args.recover:
        if args.today is None:
            parser.error("--recover requires --today YYYY-MM-DD")

        from lt_recovery import (
            EXIT_REFUSED,
            RecoveryRefused,
            parse_issue_date,
            run_recovery,
        )

        try:
            effective_date = parse_issue_date(args.today)
        except RecoveryRefused as exc:
            logger.error("Long-term recovery REFUSED (nothing was run): %s", exc)
            sys.exit(EXIT_REFUSED)

        initialize_today(effective_date)

        sys.exit(
            run_recovery(
                issue_date=args.today,
                forecast_mode=os.getenv("lt_forecast_mode"),
                run_forecast_fn=run_forecast,
                station_codes_fn=_read_station_codes,
            )
        )

    if args.today is None:
        today = datetime.now().date()
    else:
        today = datetime.strptime(args.today, "%Y-%m-%d").date()

    initialize_today(today)

    run_forecast(forecast_all=recalibrate_all, models_to_run=models_to_run)
