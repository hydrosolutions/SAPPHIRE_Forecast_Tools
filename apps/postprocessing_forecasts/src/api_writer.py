"""Write postprocessing data (forecasts and skill metrics) to the SAPPHIRE API.

Extracted from forecast_library.py — these functions are exclusively
used by postprocessing_forecasts.

Follows the same singleton pattern as data_reader.py.
"""

import datetime as dt_module
import os
import logging

import pandas as pd

import tag_library as tl

logger = logging.getLogger(__name__)

# Map uppercased model_short to API model_type format.
# Short-term models (pentad/decad) and long-term models (monthly+).
MODEL_TYPE_MAP = {
    # Short-term forecasting models
    "LR": "LR",
    "TFT": "TFT",
    "TIDE": "TiDE",
    "TSMIXER": "TSMixer",
    "EM": "EM",
    "NE": "NE",
    "RRAM": "RRAM",
    # Long-term forecasting models
    "GBT": "GBT",
    "LR_BASE": "LR_Base",
    "LR_SM": "LR_SM",
    "LR_SM_DT": "LR_SM_DT",
    "LR_SM_ROF": "LR_SM_ROF",
    "MC_ALD": "MC_ALD",
    "SM_GBT": "SM_GBT",
    "SM_GBT_LR": "SM_GBT_LR",
    "SM_GBT_NORM": "SM_GBT_Norm",
    # Baseline models (computed in postprocessing)
    "NAIVE MEAN": "Naive Mean",
    "SKILLED MEAN": "Skilled Mean",
}

# Map internal horizon type names to API enum values.
# Internal code uses "decad"; the PostgreSQL enum uses "decade".
HORIZON_TYPE_TO_API = {
    "pentad": "pentad",
    "decad": "decade",
    "month": "month",
}

# ---------------------------------------------------------------------------
# API client availability
# ---------------------------------------------------------------------------
try:
    from sapphire_api_client.postprocessing import (
        SapphirePostprocessingClient,
    )
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePostprocessingClient = None


# ---------------------------------------------------------------------------
# Singleton client
# ---------------------------------------------------------------------------
_postprocessing_client = None


def _get_postprocessing_client():
    """Return a cached SapphirePostprocessingClient (singleton).

    The client is created lazily on first call, using SAPPHIRE_API_URL.
    Returns None if sapphire-api-client is not installed.
    """
    global _postprocessing_client
    if _postprocessing_client is not None:
        return _postprocessing_client
    if not SAPPHIRE_API_AVAILABLE or SapphirePostprocessingClient is None:
        return None
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    _postprocessing_client = SapphirePostprocessingClient(base_url=api_url)
    return _postprocessing_client


def _reset_api_client():
    """Reset cached API client (for testing)."""
    global _postprocessing_client
    _postprocessing_client = None


# ---------------------------------------------------------------------------
# Write functions
# ---------------------------------------------------------------------------

def _write_combined_forecast_to_api(data: pd.DataFrame, horizon_type: str) -> bool:
    """
    Write combined forecasts (from all models) to SAPPHIRE postprocessing API.

    Args:
        data: DataFrame with forecast data. Expected columns:
            - code: station code
            - date: forecast date
            - pentad_in_month/decad: horizon value
            - pentad_in_year/decad_in_year: horizon in year
            - forecasted_discharge: the forecast value
            - model_short: model identifier (LR, TFT, TIDE, TSMIXER, EM, NE)
            - composition (optional): for ensemble models, which models compose it
        horizon_type: "pentad" or "decad" (translated to API enum at boundary)

    Returns:
        bool: True if successful, False otherwise
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping combined forecast API write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    # Get API URL from environment
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    client = _get_postprocessing_client()

    # Health check - non-blocking, skip if API unavailable
    if not client.readiness_check():
        logger.warning(
            f"SAPPHIRE API at {api_url} is not ready, "
            "skipping combined forecast write"
        )
        return False

    data = data.copy()

    # Translate internal horizon name to API enum value
    api_horizon_type = HORIZON_TYPE_TO_API.get(horizon_type)
    if api_horizon_type is None:
        raise ValueError(
            f"Invalid horizon_type: {horizon_type}. "
            f"Must be one of {list(HORIZON_TYPE_TO_API.keys())}."
        )

    # Determine column names based on horizon_type
    if horizon_type == "pentad":
        horizon_value_col = "pentad_in_month"
        horizon_in_year_col = "pentad_in_year"
    elif horizon_type == "decad":
        # Note: save_forecast_data_decade renames decad_in_month to decad
        horizon_value_col = "decad"
        horizon_in_year_col = "decad_in_year"
    else:
        raise ValueError(
            f"Invalid horizon_type: {horizon_type}. "
            "Must be 'pentad' or 'decad'."
        )

    # Compute missing horizon values from dates before iterating.
    # Virtual station outer merges and ML API reads can produce rows
    # with valid dates but NaN pentad_in_month/pentad_in_year.
    repaired_count = 0
    if horizon_type == "pentad":
        get_period_func = tl.get_pentad
        get_period_in_year_func = tl.get_pentad_in_year
    else:  # decad
        get_period_func = tl.get_decad_in_month
        get_period_in_year_func = tl.get_decad_in_year

    if horizon_value_col in data.columns:
        missing_hv = data[horizon_value_col].isna()
    else:
        missing_hv = pd.Series(True, index=data.index)
        data[horizon_value_col] = None

    if horizon_in_year_col in data.columns:
        missing_hiy = data[horizon_in_year_col].isna()
    else:
        missing_hiy = pd.Series(True, index=data.index)
        data[horizon_in_year_col] = None

    need_repair = missing_hv | missing_hiy
    if need_repair.any():
        dates_for_repair = pd.to_datetime(data.loc[need_repair, 'date'], errors='coerce')
        valid_dates = dates_for_repair.notna()
        if valid_dates.any():
            repair_idx = dates_for_repair[valid_dates].index
            first_day = dates_for_repair[valid_dates] + pd.Timedelta(days=1)
            # tl functions return strings; convert to int for float64 columns
            data.loc[repair_idx, horizon_value_col] = pd.to_numeric(
                first_day.apply(get_period_func), errors='coerce'
            )
            data.loc[repair_idx, horizon_in_year_col] = pd.to_numeric(
                first_day.apply(get_period_in_year_func), errors='coerce'
            )
            repaired_count = len(repair_idx)
            logger.info(
                f"Computed missing horizon values from dates for "
                f"{repaired_count} forecast records ({horizon_type})"
            )

    # Prepare records for API (vectorized)
    df_rec = data.copy()
    # Drop rows with missing horizon values
    n_before = len(df_rec)
    df_rec = df_rec.dropna(subset=[horizon_value_col, horizon_in_year_col])
    skipped_count = n_before - len(df_rec)

    if skipped_count > 0:
        # Identify which codes/dates were dropped so operators can investigate
        dropped = data[data[horizon_value_col].isna() | data[horizon_in_year_col].isna()]
        dropped_detail = (
            dropped[['code', 'date']].drop_duplicates()
            .head(10)
            .to_dict('records')
        )
        logger.warning(
            "Dropped %d forecast records with missing horizon values "
            "after repair attempt (%s). Sample codes/dates: %s",
            skipped_count, horizon_type, dropped_detail,
        )

    if df_rec.empty:
        records = []
    else:
        # Map model_short to API model_type
        df_rec = df_rec.copy()
        df_rec['model_type'] = (
            df_rec['model_short'].astype(str).str.upper()
            .map(MODEL_TYPE_MAP)
            .fillna(df_rec['model_short'].astype(str))
        )
        df_rec['date_str'] = pd.to_datetime(df_rec['date']).dt.strftime('%Y-%m-%d')

        # Ensure composition column exists
        if 'composition' not in df_rec.columns:
            df_rec['composition'] = None
        # Warn about ensemble rows missing composition
        is_ensemble = df_rec['model_short'].astype(str).str.upper().isin(['EM', 'NE'])
        missing_comp = is_ensemble & (
            df_rec['composition'].isna()
            | (df_rec['composition'].astype(str).str.strip() == '')
        )
        if missing_comp.any():
            logger.warning(
                "%d ensemble forecast rows have no composition column; "
                "these will be written with composition=None",
                missing_comp.sum(),
            )

        records_df = pd.DataFrame({
            'horizon_type': api_horizon_type,
            'code': df_rec['code'].astype(str),
            'model_type': df_rec['model_type'],
            'date': df_rec['date_str'],
            'target': df_rec['date_str'],
            'horizon_value': df_rec[horizon_value_col].astype(float).astype(int),
            'horizon_in_year': df_rec[horizon_in_year_col].astype(float).astype(int),
            'composition': df_rec['composition'],
            'forecasted_discharge': df_rec['forecasted_discharge'].where(
                df_rec['forecasted_discharge'].notna()
            ),
        })
        # Convert to records, replacing NaN/NaT with None
        records = [
            {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            for row_dict in records_df.to_dict('records')
        ]

    # Write to API
    if records:
        logger.debug(
            f"Sample record being sent to API ({horizon_type}): {records[0]}"
        )
        try:
            count = client.write_forecasts(records)
        except Exception as e:
            # Log server response body for diagnosis
            response_body = getattr(e, 'response', None)
            if response_body:
                logger.error(
                    f"API response body for {horizon_type} write: "
                    f"{response_body[:1000]}"
                )
            raise
        logger.info(f"Successfully wrote {count} combined forecast records to SAPPHIRE API ({horizon_type})")
        print(f"SAPPHIRE API: Successfully wrote {count} combined forecast records ({horizon_type})")
        return True
    else:
        logger.info(f"No combined forecast records to write to API ({horizon_type})")
        return False


def _write_skill_metrics_to_api(
    data: pd.DataFrame, horizon_type: str, year: int
) -> bool:
    """
    Write skill metrics to SAPPHIRE postprocessing API.

    Args:
        data: DataFrame with skill metrics. Expected columns:
            - code: station code
            - pentad_in_year/decad_in_year/month_in_year: horizon in year
            - model_short: model identifier (LR, TFT, TIDE, TSMIXER, EM, NE)
            - sdivsigma: s/sigma metric
            - nse: Nash-Sutcliffe Efficiency
            - delta: delta metric
            - accuracy: accuracy metric
            - mae: Mean Absolute Error
            - n_pairs: number of data pairs
            - composition (optional): for ensemble models, which models compose it
        horizon_type: "pentad", "decad", or "month"
            (translated to API enum at boundary)
        year: The target year for skill metrics dates. Each row gets a
            date corresponding to the first day of its period (pentad,
            decad, or month) in this year.

    Returns:
        bool: True if successful, False otherwise
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping skill metrics API write")
        return False

    # Check if API writing is enabled (default: enabled)
    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false")
        return False

    # Get API URL from environment
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    client = _get_postprocessing_client()

    # Health check - non-blocking, skip if API unavailable
    if not client.readiness_check():
        logger.warning(
            f"SAPPHIRE API at {api_url} is not ready, "
            "skipping skill metrics write"
        )
        return False

    data = data.copy()

    # Translate internal horizon name to API enum value
    api_horizon_type = HORIZON_TYPE_TO_API.get(horizon_type)
    if api_horizon_type is None:
        raise ValueError(
            f"Invalid horizon_type: {horizon_type}. "
            f"Must be one of {list(HORIZON_TYPE_TO_API.keys())}."
        )

    # Determine column names based on horizon_type
    if horizon_type == "pentad":
        horizon_in_year_col = "pentad_in_year"
    elif horizon_type == "decad":
        horizon_in_year_col = "decad_in_year"
    elif horizon_type == "month":
        horizon_in_year_col = "month_in_year"
    else:
        raise ValueError(
            f"Invalid horizon_type: {horizon_type}. "
            "Must be 'pentad', 'decad', or 'month'."
        )

    # Prepare records for API (vectorized)
    df_rec = data.copy()
    n_before = len(df_rec)
    df_rec = df_rec.dropna(subset=[horizon_in_year_col])
    skipped_count = n_before - len(df_rec)

    if skipped_count > 0:
        dropped = data[data[horizon_in_year_col].isna()]
        dropped_detail = (
            dropped[['code', 'model_short']].drop_duplicates()
            .head(10)
            .to_dict('records')
        )
        logger.warning(
            "Dropped %d skill metric records with missing %s. "
            "Sample codes/models: %s",
            skipped_count, horizon_in_year_col, dropped_detail,
        )

    if df_rec.empty:
        records = []
    else:
        # Map model_short to API model_type
        df_rec['model_type'] = (
            df_rec['model_short'].astype(str).str.upper()
            .map(MODEL_TYPE_MAP)
            .fillna(df_rec['model_short'].astype(str))
        )

        # Extract composition from existing column
        if 'composition' in df_rec.columns:
            df_rec['_composition'] = df_rec['composition'].where(
                df_rec['composition'].notna()
                & (df_rec['composition'].astype(str).str.strip() != '')
            )
        else:
            df_rec['_composition'] = None
        # Warn about ensemble rows missing composition
        is_ensemble = df_rec['model_short'].astype(str).str.upper().isin(['EM', 'NE'])
        missing_comp = is_ensemble & df_rec['_composition'].isna()
        if missing_comp.any():
            logger.warning(
                "%d ensemble skill metric rows have no composition; "
                "these will be written with composition=None",
                missing_comp.sum(),
            )

        # Compute per-row date from the period index and target year
        if horizon_type == "pentad":
            df_rec['_date'] = df_rec[horizon_in_year_col].astype(int).apply(
                lambda p: tl.get_date_for_pentad(p, year)
            )
        elif horizon_type == "decad":
            df_rec['_date'] = df_rec[horizon_in_year_col].astype(int).apply(
                lambda d: tl.get_date_for_decad(d, year)
            )
        else:  # month
            df_rec['_date'] = df_rec[horizon_in_year_col].astype(int).apply(
                lambda m: dt_module.date(year, m, 1).strftime('%Y-%m-%d')
            )

        # Build nullable float columns
        metric_cols = {}
        for col in ('sdivsigma', 'nse', 'delta', 'accuracy', 'mae',
                    'crps', 'pbias', 'kgelf', 'nse_log'):
            if col in df_rec.columns:
                metric_cols[col] = df_rec[col].where(df_rec[col].notna())
            else:
                metric_cols[col] = None
        n_pairs_col = df_rec['n_pairs'].where(df_rec['n_pairs'].notna()) if 'n_pairs' in df_rec.columns else None

        records_df = pd.DataFrame({
            'horizon_type': api_horizon_type,
            'code': df_rec['code'].astype(str),
            'model_type': df_rec['model_type'],
            'date': df_rec['_date'],
            'horizon_in_year': df_rec[horizon_in_year_col].astype(int),
            'composition': df_rec['_composition'],
            **metric_cols,
        })
        if n_pairs_col is not None:
            records_df['n_pairs'] = n_pairs_col
        # Convert to records, replacing NaN/NaT with None
        records = [
            {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            for row_dict in records_df.to_dict('records')
        ]
        # Ensure n_pairs is int where not None
        for r in records:
            if r.get('n_pairs') is not None:
                r['n_pairs'] = int(r['n_pairs'])

    # Write to API
    if records:
        count = client.write_skill_metrics(records)
        logger.info(f"Successfully wrote {count} skill metric records to SAPPHIRE API ({horizon_type})")
        print(f"SAPPHIRE API: Successfully wrote {count} skill metric records ({horizon_type})")
        return True
    else:
        logger.info(f"No skill metric records to write to API ({horizon_type})")
        return False


def _write_monthly_ensemble_to_api(data: pd.DataFrame) -> bool:
    """Write monthly ensemble forecasts (EM, Naive Mean, Skilled Mean)
    to the SAPPHIRE long_forecasts table.

    Filters data to ensemble rows, builds LongForecast-compatible records,
    and upserts via client.write_long_forecasts().

    Args:
        data: DataFrame with monthly joint forecasts. Expects columns:
            code, year, month, forecasted_discharge, model_short,
            and optionally q05-q95, valid_from, valid_to, composition.

    Returns:
        True if successful, False otherwise (never raises).
    """
    import calendar

    ensemble_models = {'EM', 'Naive Mean', 'Skilled Mean'}

    if data is None or data.empty:
        logger.info("No monthly ensemble data to write to API")
        return False

    if not SAPPHIRE_API_AVAILABLE:
        logger.warning(
            "sapphire-api-client not installed, skipping monthly "
            "ensemble API write"
        )
        return False

    api_enabled = (
        os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    )
    if not api_enabled:
        logger.info(
            "SAPPHIRE API writing disabled via SAPPHIRE_API_ENABLED=false"
        )
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = _get_postprocessing_client()

        if not client.readiness_check():
            logger.warning(
                f"SAPPHIRE API at {api_url} is not ready, "
                "skipping monthly ensemble write"
            )
            return False

        # Filter to ensemble rows only
        ens_mask = data['model_short'].isin(ensemble_models)
        ens_data = data[ens_mask].copy()
        if ens_data.empty:
            logger.info("No ensemble rows in monthly forecast data")
            return False

        records = []
        for _, row in ens_data.iterrows():
            year = int(row['year'])
            month = int(row['month'])
            code = str(row['code']).replace('.0', '')

            # Synthesize valid_from/valid_to from year+month if missing
            if pd.notna(row.get('valid_from')):
                valid_from = str(row['valid_from'])[:10]
            else:
                valid_from = f"{year}-{month:02d}-01"

            if pd.notna(row.get('valid_to')):
                valid_to = str(row['valid_to'])[:10]
            else:
                last_day = calendar.monthrange(year, month)[1]
                valid_to = f"{year}-{month:02d}-{last_day:02d}"

            # Map model_short to API model_type
            model_upper = str(row['model_short']).upper()
            model_type = MODEL_TYPE_MAP.get(
                model_upper, str(row['model_short'])
            )

            record = {
                "horizon_type": "month",
                "horizon_value": month,
                "code": code,
                "date": valid_from,
                "model_type": model_type,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "flag": 0,
                "q": (
                    float(row['forecasted_discharge'])
                    if pd.notna(row.get('forecasted_discharge'))
                    else None
                ),
            }

            # Add quantile columns if present and not NaN
            for qcol in (
                'q05', 'q10', 'q25', 'q50', 'q75', 'q90', 'q95'
            ):
                if qcol in row.index and pd.notna(row.get(qcol)):
                    record[qcol] = float(row[qcol])

            # Add composition if present
            comp = row.get('composition', '')
            if pd.notna(comp) and str(comp).strip():
                record['composition'] = str(comp)

            records.append(record)

        if not records:
            logger.info("No monthly ensemble records to write to API")
            return False

        logger.debug(
            "Sample monthly ensemble record: %s", records[0]
        )
        count = client.write_long_forecasts(records)
        logger.info(
            "Successfully wrote %d monthly ensemble forecast records "
            "to SAPPHIRE API", count,
        )
        print(
            f"SAPPHIRE API: Successfully wrote {count} monthly "
            f"ensemble forecast records"
        )
        return True

    except Exception as e:
        logger.error(
            "Failed to write monthly ensemble forecasts to API: %s",
            str(e),
        )
        return False
