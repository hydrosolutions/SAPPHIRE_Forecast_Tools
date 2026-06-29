"""Detect missing ensemble forecasts for gap-fill maintenance.

Used by postprocessing_maintenance.py to find (date, code) pairs where
individual model forecasts exist but the ensemble (model_short='EM') is
missing within a lookback window.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def detect_missing_ensembles(
    combined_forecasts: pd.DataFrame,
    max_lookback_months: int = 13,
    ensemble_models: set[str] | None = None,
    horizon_type: str = "pentad",
    modelled_forecasts: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Find (date, code, model_short) tuples missing ensemble forecasts.

    Scans up to ``max_lookback_months`` months back from the most recent
    date in the data.  When ``modelled_forecasts`` is provided, its
    (date, code) pairs are included so that gaps where combined_forecasts
    has nothing but modelled data exists are also detected.

    Args:
        combined_forecasts: DataFrame with [date, code, model_short, ...].
        max_lookback_months: Months to scan back from most recent date.
        ensemble_models: Set of ensemble model_short values to check.
            Defaults to ``{'EM'}`` for backward compatibility.
        horizon_type: 'pentad' or 'decad' (for logging).
        modelled_forecasts: Optional DataFrame with [date, code, ...].
            If provided, its (date, code) pairs are merged into the
            universe of pairs to check, fixing the blind spot where
            postprocessing never ran for a date.

    Returns:
        DataFrame with [date, code, model_short] tuples needing gap-fill.
        Empty DataFrame if no gaps found.
    """
    if ensemble_models is None:
        ensemble_models = {"EM"}

    empty = pd.DataFrame(columns=["date", "code", "model_short"])

    both_empty = combined_forecasts.empty and (
        modelled_forecasts is None or modelled_forecasts.empty
    )
    if both_empty:
        return empty

    # Ensure date is datetime on combined
    if not combined_forecasts.empty and not pd.api.types.is_datetime64_any_dtype(
        combined_forecasts["date"]
    ):
        combined_forecasts = combined_forecasts.copy()
        combined_forecasts["date"] = pd.to_datetime(combined_forecasts["date"])

    # Ensure date is datetime on modelled
    if (
        modelled_forecasts is not None
        and not modelled_forecasts.empty
        and not pd.api.types.is_datetime64_any_dtype(modelled_forecasts["date"])
    ):
        modelled_forecasts = modelled_forecasts.copy()
        modelled_forecasts["date"] = pd.to_datetime(modelled_forecasts["date"])

    # Determine the most recent date across both sources
    dates = []
    if not combined_forecasts.empty:
        dates.append(combined_forecasts["date"].max())
    if modelled_forecasts is not None and not modelled_forecasts.empty:
        dates.append(modelled_forecasts["date"].max())
    max_date = max(dates)

    cutoff = max_date - pd.DateOffset(months=max_lookback_months)

    # Filter combined to lookback window
    if not combined_forecasts.empty:
        recent_combined = combined_forecasts[combined_forecasts["date"] >= cutoff]
    else:
        recent_combined = combined_forecasts

    # Build the universe of (date, code) pairs from both sources
    pair_frames = []
    if not recent_combined.empty:
        pair_frames.append(recent_combined[["date", "code"]])
    if modelled_forecasts is not None and not modelled_forecasts.empty:
        recent_modelled = modelled_forecasts[modelled_forecasts["date"] >= cutoff]
        if not recent_modelled.empty:
            pair_frames.append(recent_modelled[["date", "code"]])

    if not pair_frames:
        return empty

    all_pairs = pd.concat(pair_frames, ignore_index=True).drop_duplicates()

    if all_pairs.empty:
        return empty

    # Treat null-discharge rows as missing — they are phantom records
    # that should not count as valid forecasts.
    if not recent_combined.empty and "forecasted_discharge" in recent_combined.columns:
        recent_combined = recent_combined[recent_combined["forecasted_discharge"].notna()]

    # Check each ensemble model against combined (ensembles live there)
    missing_parts = []
    for model in sorted(ensemble_models):
        if not recent_combined.empty:
            model_pairs = recent_combined[recent_combined["model_short"] == model][
                ["date", "code"]
            ].drop_duplicates()
        else:
            model_pairs = pd.DataFrame(columns=["date", "code"])

        merged = all_pairs.merge(
            model_pairs,
            on=["date", "code"],
            how="left",
            indicator=True,
        )
        gaps = merged[merged["_merge"] == "left_only"][["date", "code"]].copy()
        if not gaps.empty:
            gaps["model_short"] = model
            missing_parts.append(gaps)

        logger.info(
            "Gap detection %s (%s): %d total pairs, %d present, %d missing",
            horizon_type,
            model,
            len(all_pairs),
            len(model_pairs),
            len(gaps),
        )

    if not missing_parts:
        return empty

    return pd.concat(
        missing_parts,
        ignore_index=True,
    )[["date", "code", "model_short"]]


def read_combined_forecasts(horizon_type: str) -> pd.DataFrame:
    """Read combined forecasts for gap detection.

    .. deprecated::
        Delegates to ``data_reader.read_combined_forecasts()``.
        Callers should import from ``data_reader`` directly.

    Args:
        horizon_type: 'pentad' or 'decad'.

    Returns:
        DataFrame with combined forecasts, or empty DataFrame.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    from src import data_reader

    return data_reader.read_combined_forecasts(horizon_type)


def detect_stale_quantiles(
    combined_forecasts: pd.DataFrame,
    max_lookback_months: int = 13,
    horizon_type: str = "pentad",
    quantile_col: str = "q05",
) -> pd.DataFrame:
    """Find (date, code, model_short) with a record but NULL quantiles.

    These are PENTAD/DECADE rows written before quantile propagation was
    implemented. They have ``forecasted_discharge`` but no uncertainty
    bounds, so they need to be refreshed from the individual model data.

    Excludes ENSEMBLE_MEAN (``model_short == 'EM'``) — those require skill
    metrics and are handled separately in ``_fill_gaps_for_horizon``.

    Args:
        combined_forecasts: DataFrame with [date, code, model_short,
            forecasted_discharge, q05, ...].
        max_lookback_months: Months to scan back from most recent date.
        horizon_type: 'pentad' or 'decad' (for logging).
        quantile_col: Column to check for NULL (default 'q05').

    Returns:
        DataFrame with [date, code, model_short]. Empty if none found.
    """
    empty = pd.DataFrame(columns=["date", "code", "model_short"])

    if combined_forecasts.empty:
        return empty

    if quantile_col not in combined_forecasts.columns:
        # No quantile column present at all — nothing to check
        return empty

    df = combined_forecasts.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    max_date = df["date"].max()
    cutoff = max_date - pd.DateOffset(months=max_lookback_months)
    recent = df[df["date"] >= cutoff]

    if recent.empty:
        return empty

    # Stale = has forecasted_discharge but no quantiles, and not EM
    stale = recent[
        recent["forecasted_discharge"].notna()
        & recent[quantile_col].isna()
        & (recent["model_short"] != "EM")
    ][["date", "code", "model_short"]].drop_duplicates()

    logger.info(
        "Stale quantile detection (%s): %d records within lookback window",
        horizon_type,
        len(stale),
    )
    return stale.reset_index(drop=True)


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
        ensemble_models = {"EM"}

    empty = pd.DataFrame(columns=["year", "month", "code", "model_short"])

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
    all_pairs = recent[["year", "month", "code"]].drop_duplicates()

    # Check each ensemble model
    missing_parts = []
    for model in sorted(ensemble_models):
        model_pairs = recent[recent["model_short"] == model][
            ["year", "month", "code"]
        ].drop_duplicates()

        merged = all_pairs.merge(
            model_pairs,
            on=["year", "month", "code"],
            how="left",
            indicator=True,
        )
        gaps = merged[merged["_merge"] == "left_only"][["year", "month", "code"]].copy()
        if not gaps.empty:
            gaps["model_short"] = model
            missing_parts.append(gaps)

        logger.info(
            "Monthly gap detection (%s): %d total pairs, %d present, %d missing",
            model,
            len(all_pairs),
            len(model_pairs),
            len(gaps),
        )

    if not missing_parts:
        return empty

    return pd.concat(
        missing_parts,
        ignore_index=True,
    )[["year", "month", "code", "model_short"]]


def detect_missing_quarterly_ensembles(
    combined_forecasts: pd.DataFrame,
    lookback_quarters: int = 2,
    ensemble_models: set[str] | None = None,
) -> pd.DataFrame:
    """Find (year, quarter_in_year, code, model_short) tuples missing ensembles.

    Args:
        combined_forecasts: DataFrame with [year, quarter_in_year, code,
            model_short, ...].
        lookback_quarters: Quarters to scan back from most recent.
        ensemble_models: Ensemble model_short values to check.
            Defaults to ``{'EM'}``.

    Returns:
        DataFrame with gap tuples. Empty DataFrame if no gaps found.
    """
    if ensemble_models is None:
        ensemble_models = {"EM"}

    cols = ["year", "quarter_in_year", "code", "model_short"]
    empty = pd.DataFrame(columns=cols)

    if combined_forecasts.empty:
        return empty

    required = {"year", "quarter_in_year", "code", "model_short"}
    if not required.issubset(combined_forecasts.columns):
        logger.warning(
            "Quarterly combined forecasts missing columns: %s",
            required - set(combined_forecasts.columns),
        )
        return empty

    df = combined_forecasts.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter_in_year"] = pd.to_numeric(df["quarter_in_year"], errors="coerce")
    df = df.dropna(subset=["year", "quarter_in_year"])
    df["year"] = df["year"].astype(int)
    df["quarter_in_year"] = df["quarter_in_year"].astype(int)

    if df.empty:
        return empty

    max_year = df["year"].max()
    max_q = df[df["year"] == max_year]["quarter_in_year"].max()

    recent_periods = []
    y, q = int(max_year), int(max_q)
    for _ in range(lookback_quarters):
        recent_periods.append((y, q))
        q -= 1
        if q < 1:
            q = 4
            y -= 1

    recent = df[
        df.apply(
            lambda r: (r["year"], r["quarter_in_year"]) in recent_periods,
            axis=1,
        )
    ]

    if recent.empty:
        return empty

    all_pairs = recent[["year", "quarter_in_year", "code"]].drop_duplicates()

    missing_parts = []
    for model in sorted(ensemble_models):
        model_pairs = recent[recent["model_short"] == model][
            ["year", "quarter_in_year", "code"]
        ].drop_duplicates()

        merged = all_pairs.merge(
            model_pairs,
            on=["year", "quarter_in_year", "code"],
            how="left",
            indicator=True,
        )
        gaps = merged[merged["_merge"] == "left_only"][["year", "quarter_in_year", "code"]].copy()
        if not gaps.empty:
            gaps["model_short"] = model
            missing_parts.append(gaps)

        logger.info(
            "Quarterly gap detection (%s): %d total, %d present, %d missing",
            model,
            len(all_pairs),
            len(model_pairs),
            len(gaps),
        )

    if not missing_parts:
        return empty

    return pd.concat(missing_parts, ignore_index=True)[cols]


def detect_missing_seasonal_ensembles(
    combined_forecasts: pd.DataFrame,
    lookback_seasons: int = 1,
    ensemble_models: set[str] | None = None,
) -> pd.DataFrame:
    """Find (season_year, code, model_short) tuples missing ensembles.

    Args:
        combined_forecasts: DataFrame with [season_year, code,
            model_short, ...].
        lookback_seasons: Seasons to scan back from most recent.
        ensemble_models: Ensemble model_short values to check.
            Defaults to ``{'EM'}``.

    Returns:
        DataFrame with gap tuples. Empty DataFrame if no gaps found.
    """
    if ensemble_models is None:
        ensemble_models = {"EM"}

    cols = ["season_year", "season_in_year", "code", "model_short"]
    empty = pd.DataFrame(columns=cols)

    if combined_forecasts.empty:
        return empty

    required = {"season_year", "season_in_year", "code", "model_short"}
    if not required.issubset(combined_forecasts.columns):
        logger.warning(
            "Seasonal combined forecasts missing columns: %s",
            required - set(combined_forecasts.columns),
        )
        return empty

    df = combined_forecasts.copy()
    df["season_year"] = pd.to_numeric(df["season_year"], errors="coerce")
    df["season_in_year"] = pd.to_numeric(df["season_in_year"], errors="coerce")
    df = df.dropna(subset=["season_year", "season_in_year"])
    df["season_year"] = df["season_year"].astype(int)
    df["season_in_year"] = df["season_in_year"].astype(int)

    if df.empty:
        return empty

    max_sy = df["season_year"].max()
    recent_years = list(range(max_sy, max_sy - lookback_seasons, -1))

    recent = df[df["season_year"].isin(recent_years)]

    if recent.empty:
        return empty

    key_cols = ["season_year", "season_in_year", "code"]
    all_pairs = recent[key_cols].drop_duplicates()

    missing_parts = []
    for model in sorted(ensemble_models):
        model_pairs = recent[recent["model_short"] == model][key_cols].drop_duplicates()

        merged = all_pairs.merge(
            model_pairs,
            on=key_cols,
            how="left",
            indicator=True,
        )
        gaps = merged[merged["_merge"] == "left_only"][key_cols].copy()
        if not gaps.empty:
            gaps["model_short"] = model
            missing_parts.append(gaps)

        logger.info(
            "Seasonal gap detection (%s): %d total, %d present, %d missing",
            model,
            len(all_pairs),
            len(model_pairs),
            len(gaps),
        )

    if not missing_parts:
        return empty

    return pd.concat(missing_parts, ignore_index=True)[cols]
