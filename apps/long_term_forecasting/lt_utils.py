import logging

# Suppress graphviz debug warnings BEFORE importing any modules that use graphviz
logging.getLogger("graphviz").setLevel(logging.WARNING)


import os
import pandas as pd
import requests
from typing import List, Dict, Any

# Import forecast models
from lt_forecasting.forecast_models.LINEAR_REGRESSION import LinearRegressionModel
from lt_forecasting.forecast_models.SciRegressor import SciRegressor
from lt_forecasting.forecast_models.deep_models.uncertainty_mixture import (
    UncertaintyMixtureModel,
)

from __init__ import logger, SAPPHIRE_API_AVAILABLE
try:
    from sapphire_api_client import SapphirePostprocessingClient, SapphireAPIError
except ImportError:
    SapphirePostprocessingClient = None
    SapphireAPIError = Exception


# set lt_forecasting logger level
logger_lt = logging.getLogger("lt_forecasting")
logger_lt.setLevel(logging.DEBUG)



def create_model_instance(
    model_type: str,
    model_name: str,
    configs: Dict[str, Any],
    data: pd.DataFrame,
    static_data: pd.DataFrame,
    base_predictors: pd.DataFrame = None,
    base_model_names: List[str] = None,
):
    """
    Create the appropriate model instance based on the model type.

    Args:
        model_type: 'LR' or 'SciRegressor'
        model_name: Name of the model configuration
        configs: All configuration dictionaries
        data: Time series data
        static_data: Static basin characteristics
        base_predictors: DataFrame containing base predictors
        base_model_names: List of base model names
    Returns:
        Model instance
    """
    general_config = configs["general_config"]
    model_config = configs["model_config"]
    feature_config = configs["feature_config"]
    path_config = configs["path_config"]

    # Set model name in general config
    general_config["model_name"] = model_name

    # Create model instance based on type
    if model_type == "linear_regression":
        model = LinearRegressionModel(
            data=data,
            static_data=static_data,
            general_config=general_config,
            model_config=model_config,
            feature_config=feature_config,
            path_config=path_config,
        )
    elif model_type == "sciregressor":
        model = SciRegressor(
            data=data,
            static_data=static_data,
            general_config=general_config,
            model_config=model_config,
            feature_config=feature_config,
            path_config=path_config,
            base_predictors=base_predictors,
            base_model_names=base_model_names,
        )
    elif model_type == "UncertaintyMixture":
        model = UncertaintyMixtureModel(
            data=data,
            static_data=static_data,
            general_config=general_config,
            model_config=model_config,
            feature_config=feature_config,
            path_config=path_config,
            base_predictors=base_predictors,
            base_model_names=base_model_names,
        )
    else:
        raise ValueError(f"Unknown model type: {model_type}")

    return model


def infer_q_columns(df: pd.DataFrame) -> list:
    """
    Automatically infer Q columns from the DataFrame.

    Identifies columns that:
    - Start with 'Q' followed by a digit (e.g., Q5, Q10, Q25, Q50, Q75, Q90, Q95)
    - Start with 'Q_' (e.g., Q_MC_ALD, Q_loc)

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing forecast data

    Returns
    -------
    list
        List of Q column names found in the DataFrame
    """
    q_columns = []
    for col in df.columns:
        # Match Q followed by digit (Q5, Q10, etc.) or Q_ prefix (Q_MC_ALD, Q_loc, etc.)
        if col.startswith('Q') and (len(col) > 1 and (col[1].isdigit() or col[1] == '_')):
            if col not in ("Q_obs",):
                q_columns.append(col)
    return q_columns


# ─────────────────────────────────────────────────────────────────
# DATABASE WRITING FUNCTIONS FOR LONG-TERM FORECASTS
# ─────────────────────────────────────────────────────────────────

# Model name mapping from internal names to API ModelType enum values
MODEL_NAME_TO_MODEL_TYPE = {
    "LR_Base": "LR_Base",
    "LR_SM": "LR_SM",
    "LR_SM_DT": "LR_SM_DT",
    "LR_SM_ROF": "LR_SM_ROF",
    "SM_GBT": "SM_GBT",
    "SM_GBT_LR": "SM_GBT_LR",
    "SM_GBT_Norm": "SM_GBT_Norm",
    "MC_ALD": "MC_ALD",
    "GBT": "GBT",
    # Add more mappings as needed
}


def map_model_name_to_model_type(model_name: str) -> str:
    """
    Map internal model names to API ModelType enum values.

    Args:
        model_name: Internal model name (e.g., "LR_Base", "SM_GBT")

    Returns:
        API ModelType value (e.g., "LR_Base", "SM_GBT")

    Raises:
        ValueError: If model_name is not in the mapping
    """
    if model_name in MODEL_NAME_TO_MODEL_TYPE:
        return MODEL_NAME_TO_MODEL_TYPE[model_name]
    else:
        # Log warning but still return model_name
        logger.warning(
            f"Model name '{model_name}' not in MODEL_NAME_TO_MODEL_TYPE mapping. "
            f"Using model_name directly. Available mappings: {list(MODEL_NAME_TO_MODEL_TYPE.keys())}"
        )
        return model_name


def prepare_long_forecast_records(
    forecast_df: pd.DataFrame,
    model_name: str,
    horizon_type: str = "month",
    horizon_value: int = 1
) -> List[Dict[str, Any]]:
    """
    Convert DataFrame to list of LongForecastCreate-compatible dictionaries.

    This function transforms forecast/hindcast DataFrames into the format
    expected by the SAPPHIRE postprocessing API's /long-forecast/ endpoint.

    Column naming convention:
    - Base models (LR_Base, GBT): Q_{model_name} -> q
    - GBT-type models also have: Q_{model_name}_xgb, Q_{model_name}_lgbm,
      Q_{model_name}_catboost
    - MC_ALD model: Q_MC_ALD -> q, plus Q_loc, Q5, Q10, Q25, Q50, Q75, Q90, Q95

    Args:
        forecast_df: DataFrame with forecast data. Expected columns:
            - date: forecast issue date
            - code: station code
            - valid_from: start of validity period
            - valid_to: end of validity period
            - flag: quality flag (0=forecast, 1=hindcast, 2=error)
            - Q_{model_name}: main model prediction
            - Q_{model_name}_xgb, _lgbm, _catboost: ensemble components (GBT models)
            - Q5, Q10, Q25, Q50, Q75, Q90, Q95: quantiles (MC_ALD model)
            - Q_loc: localized prediction (MC_ALD model)
        model_name: Name of the model (e.g., "LR_Base", "SM_GBT", "MC_ALD")
        horizon_type: Horizon type for the forecast (default: "month")
        horizon_value: Lead time in months from ForecastConfig (default: 1)

    Returns:
        List of dictionaries compatible with LongForecastCreate schema
    """
    records = []
    model_type = map_model_name_to_model_type(model_name)

    # Build dynamic column name for the main model output
    q_model_col = f"Q_{model_name}"

    # Static column mapping for quantiles and special columns
    static_column_mapping = {
        "Q5": "q05",
        "Q10": "q10",
        "Q25": "q25",
        "Q50": "q50",
        "Q75": "q75",
        "Q90": "q90",
        "Q95": "q95",
        "Q_loc": "q_loc",
    }

    # Dynamic column mapping for GBT-type models (ensemble components)
    # e.g., Q_GBT_xgb -> q_xgb, Q_SM_GBT_lgbm -> q_lgbm
    dynamic_suffixes = {
        "_xgb": "q_xgb",
        "_lgbm": "q_lgbm",
        "_catboost": "q_catboost",
    }

    for _, row in forecast_df.iterrows():
        # Parse dates
        date = pd.to_datetime(row['date'])
        valid_from = pd.to_datetime(row['valid_from'])
        valid_to = pd.to_datetime(row['valid_to'])

        # Skip rows with NaT (missing) dates
        if pd.isna(date) or pd.isna(valid_from) or pd.isna(valid_to):
            logger.warning(
                f"Skipping row with missing date values: "
                f"date={date}, valid_from={valid_from}, valid_to={valid_to}"
            )
            continue

        # Build record
        record = {
            "horizon_type": horizon_type,
            "horizon_value": horizon_value,
            "code": str(int(row['code'])),
            "date": date.strftime('%Y-%m-%d'),
            "model_type": model_type,
            "valid_from": valid_from.strftime('%Y-%m-%d'),
            "valid_to": valid_to.strftime('%Y-%m-%d'),
            "flag": int(row['flag']) if pd.notna(row.get('flag')) else None,
        }

        # Main model output: Q_{model_name} -> q
        if q_model_col in row.index and pd.notna(row.get(q_model_col)):
            record["q"] = float(row[q_model_col])
        # Fallback to Q50 for uncertainty models
        elif 'Q50' in row.index and pd.notna(row.get('Q50')):
            record["q"] = float(row['Q50'])
        # Fallback to Q_loc
        elif 'Q_loc' in row.index and pd.notna(row.get('Q_loc')):
            record["q"] = float(row['Q_loc'])

        # Map static quantile columns (Q5, Q10, ..., Q95, Q_loc)
        for df_col, api_col in static_column_mapping.items():
            if df_col in row.index and pd.notna(row.get(df_col)):
                record[api_col] = float(row[df_col])

        # Map dynamic ensemble columns (Q_{model_name}_xgb, etc.)
        for suffix, api_col in dynamic_suffixes.items():
            df_col = f"Q_{model_name}{suffix}"
            if df_col in row.index and pd.notna(row.get(df_col)):
                record[api_col] = float(row[df_col])

        records.append(record)

    return records


def save_forecast_to_db(
    forecast_df: pd.DataFrame,
    model_name: str,
    horizon_type: str = "month",
    horizon_value: int = 1
) -> bool:
    """
    Write long-term forecast to database via SAPPHIRE API.

    Args:
        forecast_df: DataFrame with forecast data
        model_name: Name of the model
        horizon_type: Horizon type for the forecast (default: "month")
        horizon_value: Lead time in months from ForecastConfig (default: 1)

    Returns:
        bool: True if successful, False otherwise
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("SAPPHIRE API client not installed, skipping DB write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    # Get API URL from environment
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)

        # Health check first - fail fast if API unavailable
        if not client.readiness_check():
            logger.warning(f"SAPPHIRE API at {api_url} is not ready, skipping DB write")
            return False

        # Prepare records
        records = prepare_long_forecast_records(
            forecast_df=forecast_df,
            model_name=model_name,
            horizon_type=horizon_type,
            horizon_value=horizon_value
        )

        if not records:
            logger.info("No records to write to DB")
            return True

        # Write to API using the client's write_long_forecasts method if available
        if hasattr(client, 'write_long_forecasts'):
            count = client.write_long_forecasts(records)
            logger.info(
                f"Successfully wrote {count} long-term forecast records to DB "
                f"({model_name}, {horizon_type})"
            )
        else:
            # Fallback: Direct API call if method not available in client
            response = requests.post(
                f"{api_url}/api/postprocessing/long-forecast/",
                json={"data": records},
                timeout=60
            )
            response.raise_for_status()
            count = len(response.json())
            logger.info(
                f"Successfully wrote {count} long-term forecast records to DB "
                f"({model_name}, {horizon_type}) via direct API call"
            )

        return True

    except SapphireAPIError as e:
        logger.error(f"SAPPHIRE API error writing long-term forecasts: {e}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error writing long-term forecasts to API: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing long-term forecasts to DB: {e}")
        return False


def save_forecast_to_csv(
    forecast_df: pd.DataFrame,
    output_path: str,
    model_name: str,
    is_hindcast: bool = False
) -> bool:
    """
    Write long-term forecast to CSV file.

    For forecasts (is_hindcast=False):
        - Saves to {output_path}/{model_name}_forecast.csv
    For hindcasts (is_hindcast=True):
        - Saves to {output_path}/{model_name}_hindcast.csv
        - Creates new file or overwrites existing

    For appending forecast to hindcast:
        - Use append_forecast_to_hindcast() instead

    Args:
        forecast_df: DataFrame with forecast data
        output_path: Directory path for output
        model_name: Name of the model
        is_hindcast: True if this is hindcast data (default: False)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        os.makedirs(output_path, exist_ok=True)

        suffix = "hindcast" if is_hindcast else "forecast"
        output_file = os.path.join(output_path, f"{model_name}_{suffix}.csv")

        # Format date columns for CSV
        df_to_save = forecast_df.copy()
        if 'date' in df_to_save.columns:
            df_to_save['date'] = pd.to_datetime(df_to_save['date']).dt.strftime('%Y-%m-%d')

        df_to_save.to_csv(output_file, index=False)
        logger.info(f"{suffix.capitalize()} for model {model_name} saved to {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error saving {suffix} to CSV: {e}")
        return False


def append_forecast_to_hindcast(
    forecast_df: pd.DataFrame,
    output_path: str,
    model_name: str
) -> bool:
    """
    Append forecast to existing hindcast CSV file.

    Loads existing hindcast, appends new forecast, removes duplicates
    (keeping the most recent), and saves back to CSV.

    Args:
        forecast_df: DataFrame with new forecast data to append
        output_path: Directory path where hindcast file is located
        model_name: Name of the model

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        hindcast_file = os.path.join(output_path, f"{model_name}_hindcast.csv")

        if os.path.exists(hindcast_file):
            df_hindcast = pd.read_csv(hindcast_file)
            df_hindcast['date'] = pd.to_datetime(df_hindcast['date'], format='mixed')
            df_hindcast['code'] = df_hindcast['code'].astype(int)

            forecast = forecast_df.copy()
            forecast['date'] = pd.to_datetime(forecast['date'], format='mixed')
            forecast['code'] = forecast['code'].astype(int)

            df_combined = pd.concat([df_hindcast, forecast], ignore_index=True)
            # Remove duplicates based on date and code, keep the last (most recent)
            df_combined = df_combined.drop_duplicates(
                subset=['date', 'code'], keep='last'
            )
            # Format date to yyyy-mm-dd
            df_combined['date'] = df_combined['date'].dt.strftime('%Y-%m-%d')
            df_combined.to_csv(hindcast_file, index=False)
            logger.info(f"Appended forecast to hindcast file {hindcast_file}")
            return True
        else:
            logger.warning(
                f"Hindcast file {hindcast_file} does not exist. Cannot append forecast."
            )
            return False

    except Exception as e:
        logger.error(f"Error appending forecast to hindcast CSV: {e}")
        return False


def save_forecast(
    forecast_df: pd.DataFrame,
    model_name: str,
    output_path: str,
    horizon_type: str = "month",
    horizon_value: int = 1,
    is_hindcast: bool = False
) -> bool:
    """
    Main save function - saves to BOTH DB and CSV for parallel tracking.

    When SAPPHIRE_API_AVAILABLE=True:
        1. Write to database via API
        2. ALSO write to CSV (parallel track for testing/validation)

    When SAPPHIRE_API_AVAILABLE=False:
        - Write to CSV only (fallback)

    Args:
        forecast_df: DataFrame with forecast data
        model_name: Name of the model
        output_path: Directory path for CSV output
        horizon_type: Horizon type for the forecast (default: "month")
        horizon_value: Lead time in months from ForecastConfig (default: 1)
        is_hindcast: True if this is hindcast data (default: False)
            file (default: False). Only applies when is_hindcast=False.

    Returns:
        bool: True if at least one save operation succeeded
    """
    success_db = False
    success_csv = False

    # 1. Try to save to database if API is available
    if SAPPHIRE_API_AVAILABLE:
        success_db = save_forecast_to_db(
            forecast_df=forecast_df,
            model_name=model_name,
            horizon_type=horizon_type,
            horizon_value=horizon_value
        )
        if success_db:
            logger.info(f"DB save successful for {model_name}")
        else:
            logger.warning(f"DB save failed for {model_name}, will still save to CSV")

    # 2. ALWAYS save to CSV (parallel track for testing/validation)
    success_csv = save_forecast_to_csv(
        forecast_df=forecast_df,
        output_path=output_path,
        model_name=model_name,
        is_hindcast=is_hindcast
    )

    # 3. If this is a forecast also append
    if not is_hindcast:
        append_success = append_forecast_to_hindcast(
            forecast_df=forecast_df,
            output_path=output_path,
            model_name=model_name
        )
        if append_success:
            # Also write appended data to DB
            if SAPPHIRE_API_AVAILABLE:
                save_forecast_to_db(
                    forecast_df=forecast_df,
                    model_name=model_name,
                    horizon_type=horizon_type,
                    horizon_value=horizon_value
                )

    # Return True if at least one save succeeded
    return success_db or success_csv

