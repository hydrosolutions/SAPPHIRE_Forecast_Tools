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
    ensemble_models: set[str] | None = None,
) -> pd.DataFrame:
    """Find (date, code, model_short) tuples missing ensemble forecasts.

    Args:
        combined_forecasts: DataFrame with [date, code, model_short, ...].
        lookback_days: Days to scan back from most recent date.
        ensemble_models: Set of ensemble model_short values to check.
            Defaults to ``{'EM'}`` for backward compatibility.

    Returns:
        DataFrame with [date, code, model_short] tuples needing gap-fill.
        Empty DataFrame if no gaps found.
    """
    if ensemble_models is None:
        ensemble_models = {'EM'}

    empty = pd.DataFrame(columns=['date', 'code', 'model_short'])

    if combined_forecasts.empty:
        return empty

    # Ensure date is datetime
    if not pd.api.types.is_datetime64_any_dtype(combined_forecasts['date']):
        combined_forecasts = combined_forecasts.copy()
        combined_forecasts['date'] = pd.to_datetime(combined_forecasts['date'])

    # Determine lookback window
    max_date = combined_forecasts['date'].max()
    cutoff = max_date - pd.Timedelta(days=lookback_days)
    recent = combined_forecasts[combined_forecasts['date'] >= cutoff]

    if recent.empty:
        return empty

    # Find all (date, code) pairs with any forecasts
    all_pairs = recent[['date', 'code']].drop_duplicates()

    # Check each ensemble model
    missing_parts = []
    for model in sorted(ensemble_models):
        model_pairs = recent[recent['model_short'] == model][
            ['date', 'code']
        ].drop_duplicates()

        merged = all_pairs.merge(
            model_pairs, on=['date', 'code'],
            how='left', indicator=True,
        )
        gaps = merged[merged['_merge'] == 'left_only'][
            ['date', 'code']
        ].copy()
        if not gaps.empty:
            gaps['model_short'] = model
            missing_parts.append(gaps)

        logger.info(
            "Gap detection (%s): %d total pairs, %d present, %d missing",
            model, len(all_pairs), len(model_pairs), len(gaps),
        )

    if not missing_parts:
        return empty

    return pd.concat(
        missing_parts, ignore_index=True,
    )[['date', 'code', 'model_short']]


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


def detect_missing_monthly_ensembles(
    combined_forecasts: pd.DataFrame,
    lookback_months: int = 3,
    ensemble_models: set[str] | None = None,
) -> pd.DataFrame:
    """Find (year, month, code, model_short) tuples missing ensembles.

    Args:
        combined_forecasts: DataFrame with [year, month, code,
            model_short, ...] from the monthly combined CSV.
        lookback_months: Months to scan back from most recent.
        ensemble_models: Set of ensemble model_short values to check.
            Defaults to ``{'EM'}`` for backward compatibility.

    Returns:
        DataFrame with [year, month, code, model_short] tuples
        needing gap-fill. Empty DataFrame if no gaps found.
    """
    if ensemble_models is None:
        ensemble_models = {'EM'}

    empty = pd.DataFrame(
        columns=["year", "month", "code", "model_short"]
    )

    if combined_forecasts.empty:
        return empty

    required = {"year", "month", "code", "model_short"}
    if not required.issubset(combined_forecasts.columns):
        logger.warning(
            "Monthly combined forecasts missing required columns: %s",
            required - set(combined_forecasts.columns),
        )
        return empty

    df = combined_forecasts.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df = df.dropna(subset=["year", "month"])
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    if df.empty:
        return empty

    # Determine the most recent (year, month)
    max_year = df["year"].max()
    max_month = df[df["year"] == max_year]["month"].max()

    # Build list of recent (year, month) tuples within lookback
    recent_periods = []
    y, m = int(max_year), int(max_month)
    for _ in range(lookback_months):
        recent_periods.append((y, m))
        m -= 1
        if m < 1:
            m = 12
            y -= 1

    # Filter to recent periods
    recent = df[
        df.apply(
            lambda r: (r["year"], r["month"]) in recent_periods,
            axis=1,
        )
    ]

    if recent.empty:
        return empty

    # Find all (year, month, code) pairs with any forecasts
    all_pairs = recent[
        ["year", "month", "code"]
    ].drop_duplicates()

    # Check each ensemble model
    missing_parts = []
    for model in sorted(ensemble_models):
        model_pairs = recent[recent["model_short"] == model][
            ["year", "month", "code"]
        ].drop_duplicates()

        merged = all_pairs.merge(
            model_pairs, on=["year", "month", "code"],
            how="left", indicator=True,
        )
        gaps = merged[merged["_merge"] == "left_only"][
            ["year", "month", "code"]
        ].copy()
        if not gaps.empty:
            gaps["model_short"] = model
            missing_parts.append(gaps)

        logger.info(
            "Monthly gap detection (%s): %d total pairs, "
            "%d present, %d missing",
            model, len(all_pairs), len(model_pairs), len(gaps),
        )

    if not missing_parts:
        return empty

    return pd.concat(
        missing_parts, ignore_index=True,
    )[["year", "month", "code", "model_short"]]
