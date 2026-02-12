"""Detect missing ensemble forecasts for gap-fill maintenance.

Used by postprocessing_maintenance.py to find (date, code) pairs where
individual model forecasts exist but the ensemble (model_short='EM') is
missing within a lookback window.
"""

import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def detect_missing_ensembles(
    combined_forecasts: pd.DataFrame,
    lookback_days: int = 7,
) -> pd.DataFrame:
    """Find (date, code) pairs missing ensemble forecasts.

    Args:
        combined_forecasts: DataFrame with [date, code, model_short, ...].
        lookback_days: Days to scan back from most recent date.

    Returns:
        DataFrame with [date, code] pairs needing gap-fill.
        Empty DataFrame if no gaps found.
    """
    if combined_forecasts.empty:
        return pd.DataFrame(columns=['date', 'code'])

    # Ensure date is datetime
    if not pd.api.types.is_datetime64_any_dtype(combined_forecasts['date']):
        combined_forecasts = combined_forecasts.copy()
        combined_forecasts['date'] = pd.to_datetime(combined_forecasts['date'])

    # Determine lookback window
    max_date = combined_forecasts['date'].max()
    cutoff = max_date - pd.Timedelta(days=lookback_days)
    recent = combined_forecasts[combined_forecasts['date'] >= cutoff]

    if recent.empty:
        return pd.DataFrame(columns=['date', 'code'])

    # Find all (date, code) pairs with any forecasts
    all_pairs = recent[['date', 'code']].drop_duplicates()

    # Find (date, code) pairs that have ensemble (EM) forecasts
    em_pairs = recent[recent['model_short'] == 'EM'][
        ['date', 'code']
    ].drop_duplicates()

    # Missing = all_pairs - em_pairs
    merged = all_pairs.merge(
        em_pairs, on=['date', 'code'], how='left', indicator=True
    )
    missing = merged[merged['_merge'] == 'left_only'][
        ['date', 'code']
    ].reset_index(drop=True)

    logger.info(
        "Gap detection: %d total pairs, %d with EM, %d missing",
        len(all_pairs), len(em_pairs), len(missing),
    )
    return missing


def read_combined_forecasts(horizon_type: str) -> pd.DataFrame:
    """Read combined forecasts CSV for gap detection.

    Args:
        horizon_type: 'pentad' or 'decad'.

    Returns:
        DataFrame with combined forecasts, or empty DataFrame.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(
            f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}"
        )

    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv(
            "ieasyforecast_combined_forecast_pentad_file", ""
        )
    else:
        filename = os.getenv(
            "ieasyforecast_combined_forecast_decad_file", ""
        )

    if not intermediate_path or not filename:
        logger.debug(
            "Combined forecast env vars not set for %s", horizon_type
        )
        return pd.DataFrame()

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Combined forecasts CSV not found: %s", filepath)
        return pd.DataFrame()

    try:
        df = pd.read_csv(filepath)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        if 'code' in df.columns:
            df['code'] = df['code'].astype(str).str.replace(
                r'\.0$', '', regex=True
            )
        return df
    except Exception as e:
        logger.error(
            "Failed to read combined forecasts CSV %s: %s", filepath, e
        )
        return pd.DataFrame()
