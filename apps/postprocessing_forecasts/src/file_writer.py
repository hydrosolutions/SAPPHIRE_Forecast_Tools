"""CSV write + save orchestration for postprocessing forecasts.

Extracted from forecast_library.py — these functions are exclusively
used by postprocessing_forecasts.
"""

import contextlib
import datetime as dt_module
import logging
import os
import shutil
import tempfile

import forecast_library as fl
import pandas as pd

from . import api_writer, write_diagnostics

logger = logging.getLogger(__name__)


def _resolve_year(year: int | None) -> int:
    """Return *year* if provided, otherwise the current calendar year."""
    if year is None:
        return dt_module.date.today().year
    return year


# ---------------------------------------------------------------------------
# Atomic CSV writer
# ---------------------------------------------------------------------------


def atomic_write_csv(data: pd.DataFrame, filepath: str, **to_csv_kwargs) -> None:
    """
    Write a DataFrame to CSV atomically using temp file + rename pattern.

    This prevents data loss if a crash occurs during the write operation.
    The file is first written to a temporary location, then atomically
    moved to the final destination using os.replace().

    Args:
        data: DataFrame to write
        filepath: Final destination path for the CSV file
        **to_csv_kwargs: Additional arguments to pass to DataFrame.to_csv()

    Raises:
        Exception: If the write operation fails (temp file is cleaned up)
    """
    # Get the directory of the target file
    target_dir = os.path.dirname(filepath) or "."

    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Create a temp file in the same directory (ensures same filesystem for atomic rename)
    temp_fd, temp_path = tempfile.mkstemp(suffix=".tmp", dir=target_dir)

    try:
        # Close the file descriptor (we'll write via pandas)
        os.close(temp_fd)

        # Write to the temp file
        data.to_csv(temp_path, **to_csv_kwargs)

        # Atomic move: os.replace() is atomic on POSIX systems when src and dst
        # are on the same filesystem. On Windows, it's atomic if dst doesn't exist.
        # shutil.move uses os.rename under the hood for same-filesystem moves.
        shutil.move(temp_path, filepath)

        logger.debug(f"Atomically wrote {len(data)} rows to {filepath}")

    except Exception as e:
        # Clean up the temp file if something went wrong
        if os.path.exists(temp_path):
            with contextlib.suppress(OSError):
                os.remove(temp_path)
        raise e


# ---------------------------------------------------------------------------
# Latest-forecast extraction
# ---------------------------------------------------------------------------


def get_latest_forecasts(simulated_df, horizon_column_name="pentad_in_year"):
    """
    Extract the latest forecasts for each unique combination of code, pentad_in_year, and model_short.

    Args:
        simulated_df (pd.DataFrame): DataFrame containing forecast data with columns 'code',
                                    <horizon_column_name>, 'model_short', 'date', and forecast values
        horizon_column_name (str): Name of the column that represents the forecast horizon.
                                    Default is 'pentad_in_year'.

    Returns:
        pd.DataFrame: DataFrame containing only the most recent forecast for each unique
                     combination of code, pentad_in_year, and model_short
    """
    if simulated_df.empty:
        return pd.DataFrame()

    latest_date_temp = simulated_df["date"].max()
    unique_models = simulated_df["model_short"].unique()
    latest_models = simulated_df[simulated_df["date"] == latest_date_temp]["model_short"].unique()
    logger.debug(
        "Getting latest forecasts — latest date: %s, models: %s, models at latest date: %s",
        latest_date_temp,
        unique_models,
        latest_models,
    )

    # Ensure date is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(simulated_df["date"]):
        simulated_df = simulated_df.copy()
        simulated_df["date"] = pd.to_datetime(simulated_df["date"])

    # Sort by date in descending order first
    sorted_df = simulated_df.sort_values("date", ascending=False)
    latest_forecasts = sorted_df.drop_duplicates(
        subset=["code", horizon_column_name, "model_short"], keep="first"
    ).copy()

    # Only keep lines where year of date is equal to the maximum year
    # Here we take data from second to last and last year
    latest_year = simulated_df["date"].max().year
    # Write year into column, derived from date column
    latest_forecasts.loc[:, "year"] = latest_forecasts["date"].dt.year
    latest_forecasts = latest_forecasts[latest_forecasts["year"] >= (latest_year - 1)]

    logger.debug(
        "Latest year filter: %d, years in result: %s",
        latest_year,
        latest_forecasts["year"].unique(),
    )

    # Drop the 'year' column
    latest_forecasts = latest_forecasts.drop(columns=["year"])

    # Round numeric columns to 3 decimal places
    numeric_cols = latest_forecasts.select_dtypes(include=["float64", "float32"]).columns
    latest_forecasts[numeric_cols] = latest_forecasts[numeric_cols].round(3)

    return latest_forecasts


# ---------------------------------------------------------------------------
# Save forecast data
# ---------------------------------------------------------------------------


def save_forecast_data(
    config,
    simulated: pd.DataFrame,
    write_csv: bool = True,
    require_api: bool = False,
):
    """Save combined forecast data (observed + simulated) to CSV and API.

    Parameterized by *config* to handle both pentad and decad horizons.

    Args:
        config: ShortTermHorizonConfig with horizon-specific parameters.
        simulated: DataFrame with the simulated data.
        write_csv: When True (default) write the combined and ``_latest`` CSV
            files atomically and run the CSV-backed consistency check, exactly
            as before. When False, skip all CSV-file operations (both
            ``atomic_write_csv`` calls and the consistency check that re-reads
            the ``_latest`` CSV) but STILL compute the latest-forecast frame and
            STILL perform the SAPPHIRE API write. Used by the API-only backfill
            path so healed period rows reach the API without touching the
            operational CSVs.
        require_api: When True, treat a non-performed or failed API write as a
            hard error: raise ``RuntimeError`` if the API is unavailable, and
            raise if ``_write_combined_forecast_to_api`` returns a falsy value.
            Used by the backfill path so a run cannot report success without
            actually writing. When False (default), behavior is unchanged: the
            API write is best-effort and its return value is ignored.

    Returns:
        None
    """
    horizon = config.name
    period_col = config.period_col

    filename = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv(config.combined_csv_env),
    )

    # Round all float values to 3 decimal places
    simulated = simulated.round(3)

    # Ensure code is string without .0
    if "code" in simulated.columns:
        simulated["code"] = simulated["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Extract latest forecasts BEFORE converting dates to strings,
    # so get_latest_forecasts receives native datetime dates.
    simulated_latest = get_latest_forecasts(simulated, horizon_column_name=period_col)

    # Format dates as strings for CSV output
    if "date" in simulated.columns:
        simulated["date"] = pd.to_datetime(simulated["date"], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
    if "date" in simulated_latest.columns:
        simulated_latest["date"] = pd.to_datetime(
            simulated_latest["date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    # Write the data to csv (atomic to prevent corruption on crash).
    # Skipped entirely when write_csv=False (API-only backfill path).
    # The diagnose(combined) -> write(combined) -> diagnose(latest) ->
    # write(latest) ordering is preserved so a diagnostics failure cannot
    # change which CSVs were already written.
    write_diagnostics.diagnose_forecast_data(simulated, horizon, f"{horizon} combined")
    if write_csv:
        try:
            atomic_write_csv(simulated, filename, index=False)
        except Exception as e:
            logger.error(f"Could not write forecast data to {filename}.")
            raise e

    # Edit filename by appending '_latest' to the filename
    filename_latest = filename.replace(".csv", "_latest.csv")

    write_diagnostics.diagnose_forecast_data(
        simulated_latest, horizon, f"{horizon} combined latest"
    )
    if write_csv:
        try:
            atomic_write_csv(simulated_latest, filename_latest, index=False)
        except Exception as e:
            logger.error(f"Could not write latest forecast data to {filename_latest}.")
            raise e
    else:
        logger.info(
            "save_forecast_data(write_csv=False): skipping %s CSV writes; "
            "performing API write only.",
            horizon,
        )

    # Write to SAPPHIRE API (latest forecasts only)
    if require_api and not api_writer.SAPPHIRE_API_AVAILABLE:
        raise RuntimeError(
            "API unavailable but require_api=True; refusing to report success without writing"
        )
    if api_writer.SAPPHIRE_API_AVAILABLE:
        ok = False
        try:
            ok = api_writer._write_combined_forecast_to_api(simulated_latest, horizon)
        except Exception as e:
            fl._handle_api_write_error(e, f"{horizon} combined forecasts")
        if require_api and not ok:
            raise RuntimeError(f"API write for {horizon} combined forecasts returned failure")

    # --- Consistency Check ---
    # The consistency check re-reads the _latest CSV, so it only applies when
    # the CSV was actually written.
    consistency_check = (
        write_csv and os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    )
    if consistency_check:
        logger.info(
            "SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for %s combined forecasts",
            horizon,
        )

        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=simulated_latest,
            csv_file_path=filename_latest,
            data_type=f"combined forecasts {horizon}",
            key_columns=["code", "date", period_col, "model_short"],
            value_columns=["forecasted_discharge"],
        )

        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return None


# ---------------------------------------------------------------------------
# Save skill metrics
# ---------------------------------------------------------------------------


def save_skill_metrics(config, data: pd.DataFrame, year: int = None) -> bool:
    """Save short-term skill metrics to CSV and API.

    Parameterized by *config* to handle both pentad and decad horizons.

    Args:
        config: ShortTermHorizonConfig with horizon-specific parameters.
        data: The skill metrics DataFrame.
        year: Target year for API skill metric dates. Defaults to the
            current calendar year.

    Returns:
        bool: True unless the API write genuinely failed. A closed
            SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client, a
            required dependency) and a readiness-check failure or a
            raised write exception (WriteOutcome.FAILED) are the only
            failure cases. A disabled write (SAPPHIRE_API_ENABLED=false)
            and nothing left to write after filtering are non-failure
            and return True — "no attempt was made" is never reported
            as a failure. Unlike its siblings, this function has no
            top-level empty-input guard (PP-051 §1b): the CSV write
            (unconditional, raises on its own failure) is unaffected
            by this contract and unchanged by this phase.
    """
    horizon = config.name
    period_col = config.period_col

    # Round all values to 4 decimal places
    data = data.round(4)

    # Ensure code is string without .0
    if "code" in data.columns:
        data["code"] = data["code"].astype(str).str.replace(r"\.0$", "", regex=True)
    # Ensure date is in %Y-%m-%d format
    if "date" in data.columns:
        data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # convert period column to int
    data[period_col] = data[period_col].astype(int)

    # Sort in ascending order by period, code, and model_short
    data = data.sort_values(by=[period_col, "code", "model_short"])

    filepath = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv(config.skill_csv_env),
    )

    # Write atomically (temp file + rename) to prevent data loss on crash
    write_diagnostics.diagnose_skill_metrics(data, horizon, f"{horizon} skill metrics")
    try:
        atomic_write_csv(data, filepath, index=False)
        logger.info(f"Data written to {filepath}.")
    except Exception as e:
        logger.error(f"Could not write the data to {filepath}.")
        raise e

    # Write to SAPPHIRE API
    # Pre-gate default: a closed SAPPHIRE_API_AVAILABLE gate means the
    # required sapphire-api-client dependency is missing — a genuine
    # failure, not a configuration choice. SAPPHIRE_API_ENABLED=false is
    # handled *inside* the writer, below this gate, and maps to
    # SKIPPED_BY_CONFIG there (PP-051 P0a correction).
    outcome = api_writer.WriteOutcome.FAILED
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            outcome = api_writer._write_skill_metrics_to_api(
                data, config.api_horizon_type, _resolve_year(year)
            )
        except Exception as e:
            fl._handle_api_write_error(
                e, f"{horizon} skill metrics"
            )  # may re-raise under fail mode
            outcome = api_writer.WriteOutcome.FAILED

    # --- Consistency Check ---
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if consistency_check:
        logger.info(
            "SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for %s skill metrics",
            horizon,
        )

        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=data,
            csv_file_path=filepath,
            data_type=f"skill metrics {horizon}",
            key_columns=["code", period_col, "model_short"],
            value_columns=[
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
                "pbias",
                "kgelf",
                "nse_log",
                "crps",
            ],
        )

        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return outcome is not api_writer.WriteOutcome.FAILED


def save_monthly_skill_metrics(data: pd.DataFrame, year: int = None) -> bool:
    """Save monthly skill metrics to CSV + API.

    Follows the same pattern as save_pentadal/decadal_skill_metrics:
    round, clean codes, convert month_in_year to int, sort, atomic
    CSV write, then API write.

    Args:
        data: DataFrame with monthly skill metrics. Expected columns:
            month_in_year, code, model_short, sdivsigma, nse, delta,
            accuracy, mae, n_pairs, crps, composition (optional).
        year: Target year for API skill metric dates. Each month gets
            the first day of that month in this year. Defaults to the
            current calendar year.

    Returns:
        bool: True unless the API write genuinely failed. A closed
            SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client, a
            required dependency) and a readiness-check failure or a
            raised write exception (WriteOutcome.FAILED) are the only
            failure cases. An empty/None input, a disabled write
            (SAPPHIRE_API_ENABLED=false), and nothing left to write
            after filtering are all non-failure and return True — "no
            attempt was made" is never reported as a failure. The CSV
            write stays conditional on both
            ieasyforecast_intermediate_data_path and
            ieasyforecast_monthly_skill_metrics_file being set — when
            either is unset it warns and skips, it does not raise
            (distinct from pentad/decad's unconditional-and-raising CSV
            path).
    """
    if data is None or data.empty:
        logger.info("No monthly skill metrics to save")
        return True

    data = data.round(4)

    if "code" in data.columns:
        data["code"] = data["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    data["month_in_year"] = data["month_in_year"].astype(int)

    data = data.sort_values(by=["month_in_year", "code", "model_short"])

    csv_dir = os.getenv("ieasyforecast_intermediate_data_path")
    csv_file = os.getenv("ieasyforecast_monthly_skill_metrics_file")

    write_diagnostics.diagnose_skill_metrics(data, "month", "monthly skill metrics")

    if csv_dir and csv_file:
        filepath = os.path.join(csv_dir, csv_file)
        try:
            atomic_write_csv(data, filepath, index=False)
            logger.info(f"Data written to {filepath}.")
        except Exception as e:
            logger.error(f"Could not write the data to {filepath}.")
            raise e
    else:
        logger.warning("Monthly skill metrics CSV path not configured, skipping CSV save")

    # Pre-gate default: a closed SAPPHIRE_API_AVAILABLE gate means the
    # required sapphire-api-client dependency is missing — a genuine
    # failure, not a configuration choice. SAPPHIRE_API_ENABLED=false is
    # handled *inside* the writer, below this gate, and maps to
    # SKIPPED_BY_CONFIG there (PP-051 P0a correction).
    outcome = api_writer.WriteOutcome.FAILED
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            outcome = api_writer._write_skill_metrics_to_api(data, "month", _resolve_year(year))
        except Exception as e:
            fl._handle_api_write_error(e, "monthly skill metrics")  # may re-raise under fail mode
            outcome = api_writer.WriteOutcome.FAILED

    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if consistency_check:
        logger.info(
            "SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for monthly skill metrics"
        )
        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=data,
            csv_file_path=filepath,
            data_type="skill metrics month",
            key_columns=["code", "month_in_year", "model_short"],
            value_columns=[
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
                "crps",
                "pbias",
                "kgelf",
                "nse_log",
            ],
        )
        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return outcome is not api_writer.WriteOutcome.FAILED


def save_monthly_forecast_data(simulated: pd.DataFrame):
    """Save monthly combined forecasts (joint_forecasts) to CSV and API.

    Writes ensemble rows (EM, Naive Mean, Skilled Mean) to the SAPPHIRE
    API unconditionally, then optionally writes CSV if env vars are configured.

    Args:
        simulated: DataFrame with monthly joint forecasts. Expected
            columns include: code, year, month, month_in_year,
            forecasted_discharge, model_short.

    Returns:
        None
    """
    if simulated is None or simulated.empty:
        logger.info("No monthly forecast data to save")
        return None

    # Round all float values to 3 decimal places
    simulated = simulated.round(3)

    # Ensure code is string without .0
    if "code" in simulated.columns:
        simulated["code"] = simulated["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Write ensemble rows (EM, Naive Mean, Skilled Mean) to API
    # This runs unconditionally — internal guards check API availability
    ret = api_writer._write_monthly_ensemble_to_api(simulated)
    if ret:
        logger.info("Monthly ensemble forecasts written to API successfully.")
    else:
        logger.warning(
            "Monthly ensemble forecasts API write returned False "
            "(disabled, unavailable, or failed)."
        )

    # CSV write — conditional on env vars
    csv_dir = os.getenv("ieasyforecast_intermediate_data_path")
    csv_file = os.getenv("ieasyforecast_monthly_combined_forecast_file")
    if not csv_dir or not csv_file:
        logger.warning(
            "Monthly CSV path not configured "
            "(ieasyforecast_intermediate_data_path=%s, "
            "ieasyforecast_monthly_combined_forecast_file=%s), "
            "skipping CSV save",
            csv_dir,
            csv_file,
        )
        return None

    filename = os.path.join(csv_dir, csv_file)

    # Extract latest forecasts using month_in_year as horizon.
    # EM/Skilled Mean rows from calculate_monthly_skill_metrics have
    # no date — synthesize from year+month so get_latest_forecasts works.
    if "date" in simulated.columns:
        simulated = simulated.copy()
        if simulated["date"].isna().any() and "year" in simulated.columns:
            mask = simulated["date"].isna()
            simulated.loc[mask, "date"] = (
                simulated.loc[mask, "year"].astype(int).astype(str)
                + "-"
                + simulated.loc[mask, "month"].astype(int).astype(str).str.zfill(2)
                + "-01"
            )
        # Ensure consistent datetime type for the entire column
        simulated["date"] = pd.to_datetime(simulated["date"], errors="coerce")
        simulated_latest = get_latest_forecasts(simulated, horizon_column_name="month_in_year")
    else:
        simulated_latest = simulated.copy()

    # Write the data to csv (atomic to prevent corruption on crash)
    write_diagnostics.diagnose_forecast_data(simulated, "month", "monthly combined")
    try:
        atomic_write_csv(simulated, filename, index=False)
    except Exception as e:
        logger.error(f"Could not write monthly forecast data to {filename}.")
        raise e

    # Edit filename by appending '_latest' to the filename
    filename_latest = filename.replace(".csv", "_latest.csv")

    # Write the latest data to a csv file (atomic)
    write_diagnostics.diagnose_forecast_data(simulated_latest, "month", "monthly combined latest")
    try:
        atomic_write_csv(simulated_latest, filename_latest, index=False)
    except Exception as e:
        logger.error(f"Could not write latest monthly forecast data to {filename_latest}.")
        raise e

    # --- Consistency Check ---
    consistency_check = os.getenv("SAPPHIRE_CONSISTENCY_CHECK", "false").lower() == "true"
    if consistency_check:
        logger.info(
            "SAPPHIRE_CONSISTENCY_CHECK: Verifying write consistency for monthly combined forecasts"
        )
        is_consistent, message = fl._verify_preprocessing_write_consistency(
            written_data=simulated_latest,
            csv_file_path=filename_latest,
            data_type="combined forecasts monthly",
            key_columns=[
                "code",
                "month_in_year",
                "model_short",
            ],
            value_columns=["forecasted_discharge"],
        )
        if is_consistent:
            logger.info("CONSISTENCY CHECK PASSED: %s", message)
        else:
            logger.error("CONSISTENCY CHECK FAILED: %s", message)

    return None


def save_daily_skill_metrics(
    fdc_metrics: pd.DataFrame,
    threshold_metrics: pd.DataFrame,
    year: int = None,
) -> None:
    """Save daily (Tier 2) skill metrics to API.

    FHV/FLV are written via the existing skill metrics endpoint
    (horizon_type="day"). Threshold metrics (F1/CSI) are written
    via the threshold skill metrics endpoint.

    No CSV output — Tier 2 metrics are API-only.

    Args:
        fdc_metrics: DataFrame with [code, model_short, fhv, flv].
        threshold_metrics: DataFrame with [code, model_short,
            threshold_type, threshold_value, f1, precision, recall,
            csi, tp, fp, fn, tn, n_years].
        year: Target year for API dates. Defaults to current year.

    Returns:
        None
    """
    resolved_year = _resolve_year(year)

    write_diagnostics.diagnose_daily_skill_metrics(fdc_metrics, threshold_metrics)

    # Write FHV/FLV via skill metrics API (horizon_type="day")
    if fdc_metrics is not None and not fdc_metrics.empty:
        fdc_data = fdc_metrics.copy()
        # Add required columns for _write_skill_metrics_to_api
        fdc_data["day_in_year"] = 1  # placeholder — daily metrics
        # are aggregated across all days, not per-day
        if api_writer.SAPPHIRE_API_AVAILABLE:
            try:
                api_writer._write_skill_metrics_to_api(fdc_data, "day", resolved_year)
            except Exception as e:
                fl._handle_api_write_error(e, "daily FDC skill metrics")
    else:
        logger.info("No daily FDC metrics to save")

    # Write threshold metrics via threshold skill metrics API
    if threshold_metrics is not None and not threshold_metrics.empty:
        if api_writer.SAPPHIRE_API_AVAILABLE:
            try:
                api_writer._write_threshold_skill_metrics_to_api(threshold_metrics, resolved_year)
            except Exception as e:
                fl._handle_api_write_error(e, "daily threshold skill metrics")
    else:
        logger.info("No daily threshold metrics to save")

    return None


# ---------------------------------------------------------------------------
# Quarterly/seasonal save functions (API-only, no CSV)
# ---------------------------------------------------------------------------


def save_quarterly_skill_metrics(data: pd.DataFrame, year: int = None) -> bool:
    """Save quarterly skill metrics to API.

    API-only — no CSV output for quarterly metrics.

    Args:
        data: DataFrame with quarterly skill metrics. Expected columns:
            quarter_in_year, code, model_short, sdivsigma, nse, delta,
            accuracy, mae, n_pairs, crps, composition (optional).
        year: Target year for API skill metric dates. Defaults to
            current calendar year.

    Returns:
        bool: True unless the API write genuinely failed. A closed
            SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client, a
            required dependency) and a readiness-check failure or a
            raised write exception (WriteOutcome.FAILED) are the only
            failure cases. An empty/None input, a disabled write
            (SAPPHIRE_API_ENABLED=false), and nothing left to write
            after filtering are all non-failure and return True — "no
            attempt was made" is never reported as a failure.
    """
    if data is None or data.empty:
        logger.info("No quarterly skill metrics to save")
        return True

    data = data.round(4)

    if "code" in data.columns:
        data["code"] = data["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    data["quarter_in_year"] = data["quarter_in_year"].astype(int)
    data = data.sort_values(by=["quarter_in_year", "code", "model_short"])

    write_diagnostics.diagnose_skill_metrics(data, "quarter", "quarterly skill metrics")

    # Pre-gate default: a closed SAPPHIRE_API_AVAILABLE gate means the
    # required sapphire-api-client dependency is missing — a genuine
    # failure, not a configuration choice. SAPPHIRE_API_ENABLED=false is
    # handled *inside* the writer, below this gate, and maps to
    # SKIPPED_BY_CONFIG there (PP-051 P0a correction).
    outcome = api_writer.WriteOutcome.FAILED
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            outcome = api_writer._write_skill_metrics_to_api(data, "quarter", _resolve_year(year))
        except Exception as e:
            fl._handle_api_write_error(e, "quarterly skill metrics")  # may re-raise under fail mode
            outcome = api_writer.WriteOutcome.FAILED

    return outcome is not api_writer.WriteOutcome.FAILED


def save_seasonal_skill_metrics(data: pd.DataFrame, year: int = None) -> bool:
    """Save seasonal skill metrics to API.

    API-only — no CSV output for seasonal metrics.

    Args:
        data: DataFrame with seasonal skill metrics. Expected columns:
            season_in_year, code, model_short, sdivsigma, nse, delta,
            accuracy, mae, n_pairs, crps, composition (optional).
        year: Target year for API skill metric dates. Defaults to
            current calendar year.

    Returns:
        bool: True unless the API write genuinely failed. A closed
            SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client, a
            required dependency) and a readiness-check failure or a
            raised write exception (WriteOutcome.FAILED) are the only
            failure cases. An empty/None input, a disabled write
            (SAPPHIRE_API_ENABLED=false), and nothing left to write
            after filtering are all non-failure and return True — "no
            attempt was made" is never reported as a failure.
    """
    if data is None or data.empty:
        logger.info("No seasonal skill metrics to save")
        return True

    data = data.round(4)

    if "code" in data.columns:
        data["code"] = data["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    data["season_in_year"] = data["season_in_year"].astype(int)
    data = data.sort_values(by=["season_in_year", "code", "model_short"])

    write_diagnostics.diagnose_skill_metrics(data, "season", "seasonal skill metrics")

    # Pre-gate default: a closed SAPPHIRE_API_AVAILABLE gate means the
    # required sapphire-api-client dependency is missing — a genuine
    # failure, not a configuration choice. SAPPHIRE_API_ENABLED=false is
    # handled *inside* the writer, below this gate, and maps to
    # SKIPPED_BY_CONFIG there (PP-051 P0a correction).
    outcome = api_writer.WriteOutcome.FAILED
    if api_writer.SAPPHIRE_API_AVAILABLE:
        try:
            outcome = api_writer._write_skill_metrics_to_api(data, "season", _resolve_year(year))
        except Exception as e:
            fl._handle_api_write_error(e, "seasonal skill metrics")  # may re-raise under fail mode
            outcome = api_writer.WriteOutcome.FAILED

    return outcome is not api_writer.WriteOutcome.FAILED


def save_quarterly_forecast_data(simulated: pd.DataFrame):
    """Save quarterly forecasts (individual models and ensembles) to API.

    API-only — no CSV output for quarterly forecasts.

    Args:
        simulated: DataFrame with quarterly joint forecasts. Expected
            columns: code, year, quarter_in_year, model_short,
            forecasted_discharge, and optionally q05-q95, composition.
    """
    if simulated is None or simulated.empty:
        logger.info("No quarterly forecast data to save")
        return None

    simulated = simulated.round(3)

    if "code" in simulated.columns:
        simulated["code"] = simulated["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    write_diagnostics.diagnose_forecast_data(simulated, "quarter", "quarterly combined")

    ret = api_writer._write_quarterly_ensemble_to_api(simulated)
    if ret:
        logger.info("Quarterly forecasts written to API successfully.")
    else:
        logger.warning(
            "Quarterly forecasts API write returned False (disabled, unavailable, or failed)."
        )

    return None


def save_seasonal_forecast_data(simulated: pd.DataFrame):
    """Save seasonal forecasts (individual models and ensembles) to API.

    API-only — no CSV output for seasonal forecasts.

    Args:
        simulated: DataFrame with seasonal joint forecasts. Expected
            columns: code, season_year, season_in_year, model_short,
            forecasted_discharge, and optionally q05-q95, composition.
    """
    if simulated is None or simulated.empty:
        logger.info("No seasonal forecast data to save")
        return None

    simulated = simulated.round(3)

    if "code" in simulated.columns:
        simulated["code"] = simulated["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    write_diagnostics.diagnose_forecast_data(simulated, "season", "seasonal combined")

    ret = api_writer._write_seasonal_ensemble_to_api(simulated)
    if ret:
        logger.info("Seasonal forecasts written to API successfully.")
    else:
        logger.warning(
            "Seasonal forecasts API write returned False (disabled, unavailable, or failed)."
        )

    return None
