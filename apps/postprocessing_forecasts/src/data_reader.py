"""Read pre-calculated skill metrics, combined forecasts, and monthly
data from API or CSV (deprecated fallback).

Used by the operational and maintenance entry points to avoid
recalculating skill metrics from scratch, by the maintenance entry
point to read combined forecasts for gap detection, and by the yearly
recalculation entry point to read monthly observations and forecasts.
"""

import calendar
import datetime as dt
import logging
import os
import re

import pandas as pd
from long_term_horizon_resolver import (
    OperationalSchedule,
    operational_schedule_for_mode,
    quarter_horizon_value,
    supported_long_term_modes,
)
from skill_lead_aware_flag import skill_lead_aware_enabled
from src.model_names import (
    AGGREGATED_ENSEMBLE_MODELS,
    AGGREGATED_SUPPORTED_MODELS,
    canonical_model_short_series,
)
from src.postprocessing_tools import count_quantile_crossings

logger = logging.getLogger(__name__)

try:
    from sapphire_api_client.postprocessing import (
        SapphirePostprocessingClient,
    )
    from sapphire_api_client.preprocessing import (
        SapphirePreprocessingClient,
    )

    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False


_SEASONAL_FC_COLS = [
    "code",
    "season_year",
    "season_in_year",
    "horizon_value",
    "date",
    "model_short",
    "q05",
    "q10",
    "q25",
    "q50",
    "q75",
    "q90",
    "q95",
    "forecasted_discharge",
    "valid_from",
    "valid_to",
]

_QUARTERLY_FC_COLS = [
    "code",
    "year",
    "quarter_in_year",
    "model_short",
    "q05",
    "q10",
    "q25",
    "q50",
    "q75",
    "q90",
    "q95",
    "forecasted_discharge",
    "valid_from",
    "valid_to",
]


def _filter_supported_aggregated_forecast_models(df: pd.DataFrame) -> pd.DataFrame:
    """Keep supported quarter/season raw models plus existing ensemble rows."""
    if df.empty or "model_short" not in df.columns:
        return df

    model_keys = canonical_model_short_series(df["model_short"])
    return df[model_keys.isin(AGGREGATED_SUPPORTED_MODELS)].copy()


def _drop_tombstone_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop tombstone rows (n_pairs == 0) from a skill metrics DataFrame.

    Tombstones are upserted by the write-side to mark stale long-horizon
    skill keys.  A tombstone has n_pairs = 0 and all metric columns NULL.
    Legitimate rows always have n_pairs >= K (K >= 4), so n_pairs > 0 is
    a clean separator.

    Args:
        df: Skill metrics DataFrame.  May or may not have an n_pairs column.

    Returns:
        DataFrame with tombstone rows removed.  If n_pairs is absent the
        original DataFrame is returned unchanged (no short-term rows are
        ever affected).
    """
    if df.empty or "n_pairs" not in df.columns:
        return df
    return df[df["n_pairs"].notna() & (df["n_pairs"] > 0)].copy()


# ===================================================================
# M1 P1: lead-aware operational-issuance selection
#
# Flag-gated (SAPPHIRE_SKILL_LEAD_AWARE, default OFF) config-driven
# selection of exactly one "operational" issuance per (code, model,
# target year, target period) from raw long-forecast rows, applied
# immediately after read+normalize and BEFORE aggregation/skill/
# ensemble generation. See
# doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md (P1).
# ===================================================================

_MONTH_MODE_NAME_RE = re.compile(r"^month_\d+$")


def _operational_schedules_for_horizon_type(
    horizon_type: str,
) -> dict[str, OperationalSchedule]:
    """Return configured operational schedules for the modes belonging to

    one long-forecast horizon type, using the deployment mode-naming
    convention: ``month_<N>`` modes for "month", the single ``quarter``
    mode for "quarter", and ``seasonal_*`` modes for "season" (see
    `long_term_horizon_resolver` and the M1 plan's mode taxonomy).

    Args:
        horizon_type: One of "month", "quarter", "season".

    Returns:
        Mapping of mode name -> OperationalSchedule, restricted to modes
        this deployment actually supports (may be empty, e.g. a
        deployment with no seasonal modes configured).

    Raises:
        ValueError: If `horizon_type` is not one of the three supported
            long-forecast horizon types.
        LongTermHorizonResolverError: Propagated from
            `operational_schedule_for_mode` if a relevant mode's config
            is missing `operational_month_lead_time` or
            `operational_issue_day`.
    """
    modes = supported_long_term_modes()
    if horizon_type == "month":
        relevant = [m for m in modes if _MONTH_MODE_NAME_RE.match(m)]
    elif horizon_type == "quarter":
        relevant = [m for m in modes if m == "quarter"]
    elif horizon_type == "season":
        relevant = [m for m in modes if m.startswith("seasonal_")]
    else:
        raise ValueError(
            f"Unsupported horizon_type for operational schedules: {horizon_type!r} "
            f"(expected 'month', 'quarter', or 'season')."
        )
    return {mode: operational_schedule_for_mode(mode) for mode in relevant}


def _read_window_expansion_years(max_lead_months: int) -> int:
    """Return how many extra years to read backward to capture the

    earliest issuance for a maximum configured lead expressed in months.

    The API issue-date read window is expressed in whole years
    (start_year/end_year), while `select_operational_issuances` needs to
    see issuances up to `max_lead_months` before the target period
    starts. Expanding by whole years (ceil-divided) is a conservative
    over-read; callers must trim the SELECTED rows back down to the
    requested target-year range by `valid_from` (or `season_year`)
    afterward -- this function only widens the READ window.

    Args:
        max_lead_months: The largest configured `operational_month_lead_time`
            across the relevant schedules. Non-positive values need no
            expansion.

    Returns:
        Number of years (>= 0) to subtract from `start_year` before
        reading.
    """
    if max_lead_months <= 0:
        return 0
    return -(-max_lead_months // 12)  # ceil division, stdlib-only


def _trim_to_target_year_range(
    df: pd.DataFrame,
    year_col: str,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Trim rows to the requested target-year range after a read-window

    expansion. A no-op if `df` is empty or lacks `year_col`.
    """
    if df.empty or year_col not in df.columns:
        return df
    years = pd.to_numeric(df[year_col], errors="coerce")
    return df[(years >= start_year) & (years <= end_year)].copy()


def select_operational_issuances(
    df: pd.DataFrame,
    schedules: dict[str, OperationalSchedule],
    *,
    target_year_col: str,
    target_period_col: str | None = None,
    lead_output_cols: tuple[str, ...] = ("horizon_value",),
    date_col: str = "date",
    valid_from_col: str = "valid_from",
    code_col: str = "code",
    model_col: str = "model_short",
) -> pd.DataFrame:
    """Select the operational-issuance row(s) per target unit and lead

    from raw long-forecast rows.

    A PURE selection step: applied to raw long-forecast rows immediately
    after read+normalization and BEFORE aggregation/skill/ensemble
    generation (M1 P1). Does not mutate `df`.

    Baseline/ensemble rows (EM/Naive/Skilled Mean -- identified by
    canonical model-name via `AGGREGATED_ENSEMBLE_MODELS`, NOT by a
    missing-issue-date heuristic) carry no independent issue date and are
    DROPPED from the output entirely: they are recomputed downstream by
    the per-lead ensemble generation (P2), so passing the OLD ensemble
    rows through would double-count / stamp them with a stale lead.

    For every remaining (raw model) row, the operational lead is
    *derived* -- never trusted from an existing `horizon_value` column --
    as ``(valid_from.year - date.year) * 12 + (valid_from.month -
    date.month)``. A row is an operational candidate only if BOTH its
    derived lead AND its issue day (``date.day``) exactly match one of
    the configured `schedules` (no implicit tolerance -- an explicit
    tolerance would be a caller-side concern if ever configured).

    The selected UNIT is ``(code, model, target_year[, target_period],
    derived_lead)`` -- crucially INCLUDING the lead, so two distinct
    configured leads for the SAME target period (e.g. monthly month_0 at
    lead 0 and month_1 at lead 1, both targeting the same calendar month)
    are kept as SEPARATE rows rather than collapsed into one. Within a
    single unit:

    - Units with ZERO matching candidates are DROPPED and logged (the
      drop/log is reported at the coarser (code, model, target_year[,
      target_period]) grain, i.e. targets with no operational issuance at
      any configured lead) -- there is NO fallback to a non-operational
      (backfill/hindcast) row.
    - More than one candidate for the same unit (e.g. a duplicate same-day
      reissue) resolves deterministically: latest `date` wins; identical
      `date` keeps the LAST row in input order (stable sort).

    The selected lead is written into every column named in
    `lead_output_cols`, overwriting whatever those columns previously
    held (e.g. ``horizon_value`` for all horizons, plus ``season_in_year``
    for seasonal -- where the "period within year" IS the lead and must
    stay consistent with `horizon_value`).

    Args:
        df: Raw, normalized long-forecast rows (post `_normalize_*`).
        schedules: Mapping of mode name -> OperationalSchedule relevant to
            this horizon type (see `_operational_schedules_for_horizon_type`).
        target_year_col: Column identifying the target year (e.g. "year"
            for month/quarter, "season_year" for season).
        target_period_col: Column identifying an independent target period
            within the year (e.g. "month", "quarter_in_year"). Pass None
            for horizons where the "period" column is itself the lead (the
            single irrigation season): the target unit is then just
            (code, model, target_year) and distinct leads separate the
            rows.
        lead_output_cols: Columns overwritten with the derived lead on
            selected rows. Default ("horizon_value",); seasonal callers
            add "season_in_year".
        date_col: Issue-date column name. Default "date".
        valid_from_col: Target-period start column name. Default
            "valid_from".
        code_col: Station code column name. Default "code".
        model_col: Model identifier column name. Default "model_short".

    Returns:
        DataFrame of the selected raw-model rows only (baseline/ensemble
        rows removed), at most one per (unit, lead). Empty input is
        returned unchanged; input missing a required column is returned
        unchanged with a warning; a valid input yielding no operational
        candidate returns an empty frame with the input's columns.
    """
    if df.empty:
        return df

    required_cols = {date_col, valid_from_col, code_col, model_col, target_year_col}
    if target_period_col is not None:
        required_cols.add(target_period_col)
    missing = required_cols - set(df.columns)
    if missing:
        logger.warning(
            "select_operational_issuances: input missing required column(s) %s; "
            "returning input unchanged",
            sorted(missing),
        )
        return df

    empty_result = df.iloc[0:0].copy()

    canonical = canonical_model_short_series(df[model_col])
    baseline_mask = canonical.isin(AGGREGATED_ENSEMBLE_MODELS)
    # Baseline/ensemble rows are dropped entirely (recomputed downstream).
    candidates = df[~baseline_mask].copy()

    if candidates.empty:
        return empty_result

    candidates[date_col] = pd.to_datetime(candidates[date_col])
    candidates[valid_from_col] = pd.to_datetime(candidates[valid_from_col])

    # Target-unit grain (for drop/log reporting): does NOT include the lead.
    unit_cols = [code_col, model_col, target_year_col]
    if target_period_col is not None:
        unit_cols.append(target_period_col)
    all_units = set(
        map(tuple, candidates[unit_cols].drop_duplicates().itertuples(index=False, name=None))
    )

    derived_lead = (candidates[valid_from_col].dt.year - candidates[date_col].dt.year) * 12 + (
        candidates[valid_from_col].dt.month - candidates[date_col].dt.month
    )
    issue_day = candidates[date_col].dt.day

    allowed_schedules = {(s.lead_time, s.issue_day) for s in schedules.values()}
    is_candidate = [
        (lead, day) in allowed_schedules for lead, day in zip(derived_lead, issue_day, strict=True)
    ]
    candidates = candidates.assign(_pp1_derived_lead=derived_lead)[
        pd.Series(is_candidate, index=candidates.index)
    ]

    if candidates.empty:
        remaining_units: set[tuple] = set()
    else:
        remaining_units = set(
            map(
                tuple,
                candidates[unit_cols].drop_duplicates().itertuples(index=False, name=None),
            )
        )

    dropped_units = all_units - remaining_units
    if dropped_units:
        logger.info(
            "select_operational_issuances: dropped %d target unit(s) with no operational "
            "candidate matching a configured (lead, issue_day) schedule -- "
            "%s: %s",
            len(dropped_units),
            tuple(unit_cols),
            sorted(dropped_units, key=lambda g: tuple(str(x) for x in g)),
        )

    if candidates.empty:
        return empty_result

    # Selection key INCLUDES the derived lead so distinct configured leads
    # for one target period survive as separate rows (CRITICAL fix).
    selection_key_cols = [*unit_cols, "_pp1_derived_lead"]

    # Deterministic tie-break: stable sort by issue date ascending, then
    # keep the LAST row per (unit, lead) -- i.e. the latest-dated
    # candidate; identical-date ties keep the last-occurring input row.
    candidates = candidates.sort_values(by=date_col, kind="stable")
    candidates = candidates.drop_duplicates(subset=selection_key_cols, keep="last")

    lead_values = candidates["_pp1_derived_lead"].astype(int)
    for col in lead_output_cols:
        candidates[col] = lead_values
    candidates = candidates.drop(columns=["_pp1_derived_lead"])

    return candidates.reset_index(drop=True)


def read_skill_metrics(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated skill metrics from API (primary) or CSV (fallback).

    Args:
        horizon_type: 'pentad', 'decad', 'month', 'quarter', or 'season'
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [pentad_in_year|decad_in_year|
        month_in_year|quarter_in_year|season_in_year, code,
        model_short, sdivsigma, nse, delta, accuracy, mae, n_pairs]

    Raises:
        ValueError: If horizon_type is invalid.
    """
    valid = ("pentad", "decad", "month", "quarter", "season")
    if horizon_type not in valid:
        raise ValueError(f"horizon_type must be one of {valid}, got: {horizon_type}")

    if horizon_type == "month":
        return read_monthly_skill_metrics(codes)
    if horizon_type == "quarter":
        return read_quarterly_skill_metrics(codes)
    if horizon_type == "season":
        return read_seasonal_skill_metrics(codes)

    # API-first: try the authoritative source
    df = _read_skill_metrics_api(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from API (%s)",
            len(df),
            horizon_type,
        )
        return df

    # CSV fallback (deprecated): only used when API is unavailable
    logger.info(
        "API skill metrics unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_skill_metrics_csv(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from CSV (%s)",
            len(df),
            horizon_type,
        )
        return df

    logger.warning("No skill metrics available for %s", horizon_type)
    return pd.DataFrame()


def _read_skill_metrics_csv(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read skill metrics from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv("ieasyforecast_pentadal_skill_metrics_file", "")
    else:
        filename = os.getenv("ieasyforecast_decadal_skill_metrics_file", "")

    if not intermediate_path or not filename:
        logger.debug("Skill metrics env vars not set for %s", horizon_type)
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Skill metrics CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        # Ensure code is string
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin(codes)]
        return df
    except Exception as e:
        logger.error("Failed to read skill metrics CSV %s: %s", filepath, e)
        return None


def _read_skill_metrics_api(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read skill metrics from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        # Map internal horizon names to API horizon names
        # Internal uses 'decad', API expects 'decade'
        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_skill_metrics(
                        horizon=api_horizon,
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            # Read all skill metrics for this horizon; paginate if needed
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_skill_metrics(
                    horizon=api_horizon, skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_api_skill_metrics(df, horizon_type)

    except Exception as e:
        logger.error("Failed to read skill metrics from API: %s", e)
        return None


def _normalize_api_skill_metrics(df: pd.DataFrame, horizon_type: str) -> pd.DataFrame:
    """Convert API column names to CSV-compatible column names.

    API returns: horizon_in_year, model_type, code, sdivsigma, nse,
                 delta, accuracy, mae, n_pairs, crps, pbias, kgelf,
                 nse_log
    CSV expects: pentad_in_year|decad_in_year, model_short,
                 code, sdivsigma, nse, delta, accuracy, mae, n_pairs,
                 pbias, kgelf, nse_log
    """
    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"

    # Rename API columns
    rename_map = {
        "horizon_in_year": period_col,
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    # Ensure code is string
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    return df


# ===================================================================
# Monthly skill metrics
# ===================================================================


def read_monthly_skill_metrics(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated monthly skill metrics from API or CSV.

    Tombstone rows (n_pairs == 0) produced by the stale-key write-side
    are silently dropped before the result is returned.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [month_in_year, code, model_short,
        horizon_value, sdivsigma, nse, delta, accuracy, mae, n_pairs].
        horizon_value is the forecast lead (0–3 for real models; sentinel 0
        for baselines and pre-PP-038 legacy rows).
    """
    # API-first: try the authoritative source
    df = _read_monthly_skill_metrics_api(codes)
    if df is not None and not df.empty:
        df = _drop_tombstone_rows(df)
        logger.info("Read %d monthly skill metric rows from API", len(df))
        return df

    # CSV fallback (deprecated)
    logger.info("API monthly skill metrics unavailable, falling back to CSV")
    df = _read_monthly_skill_metrics_csv(codes)
    if df is not None and not df.empty:
        df = _drop_tombstone_rows(df)
        logger.info("Read %d monthly skill metric rows from CSV", len(df))
        return df

    logger.warning("No monthly skill metrics available")
    return pd.DataFrame()


def _read_monthly_skill_metrics_csv(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly skill metrics from CSV file.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")
    filename = os.getenv("ieasyforecast_monthly_skill_metrics_file", "")

    if not intermediate_path or not filename:
        logger.debug("Monthly skill metrics env vars not set")
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Monthly skill metrics CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin(codes)]
        return df
    except Exception as e:
        logger.error(
            "Failed to read monthly skill metrics CSV %s: %s",
            filepath,
            e,
        )
        return None


def _read_monthly_skill_metrics_api(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly skill metrics from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_skill_metrics(
                        horizon="month",
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_skill_metrics(horizon="month", skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_api_monthly_skill_metrics(df)

    except Exception as e:
        logger.error("Failed to read monthly skill metrics from API: %s", e)
        return None


def _normalize_api_monthly_skill_metrics(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Convert API column names to CSV-compatible names for monthly.

    API returns: horizon_in_year, model_type, code, horizon_value,
                 sdivsigma, nse, delta, accuracy, mae, n_pairs, crps,
                 pbias, kgelf, nse_log
    CSV expects: month_in_year, model_short, code, horizon_value,
                 sdivsigma, nse, delta, accuracy, mae, n_pairs, crps,
                 pbias, kgelf, nse_log

    horizon_value is passed through unchanged (it is NOT renamed).
    Legacy rows with horizon_value=NULL are coerced to sentinel 0.
    """
    rename_map = {
        "horizon_in_year": "month_in_year",
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Coerce NaN horizon_value (legacy rows with NULL from pre-PP-038 DB) to
    # sentinel 0 so callers can safely group or filter on the column.
    if "horizon_value" in df.columns:
        df["horizon_value"] = df["horizon_value"].fillna(0).astype(int)

    return df


# ===================================================================
# Short-term combined forecasts (pentad / decad)
# ===================================================================


def read_combined_forecasts(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read combined forecasts from API (primary) or CSV (fallback).

    Used by the maintenance entry point for gap detection and
    merge-back after filling missing ensembles.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with combined forecasts (all models + ensembles),
        or empty DataFrame if no data available.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    # API-first: try the authoritative source
    df = _read_combined_forecasts_api(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d combined forecast rows from API (%s)",
            len(df),
            horizon_type,
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API combined forecasts unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_combined_forecasts_csv(horizon_type, codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d combined forecast rows from CSV (%s)",
            len(df),
            horizon_type,
        )
        return df

    logger.warning("No combined forecasts available for %s", horizon_type)
    return pd.DataFrame()


def _read_combined_forecasts_api(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read combined forecasts from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        # Map internal horizon names to API horizon names
        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_short_term_forecasts(
                        horizon=api_horizon,
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_short_term_forecasts(
                    horizon=api_horizon, skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_api_combined_forecasts(df, horizon_type)

    except Exception as e:
        logger.error("Failed to read combined forecasts from API: %s", e)
        return None


def _normalize_api_combined_forecasts(df: pd.DataFrame, horizon_type: str) -> pd.DataFrame:
    """Convert API response columns to internal column names.

    API returns: id, horizon_type, code, model_type,
        model_type_description, date, target, flag,
        horizon_value, horizon_in_year, composition,
        q05, q25, q50, q75, q95, forecasted_discharge

    Internal expects: code, model_short, date, target, flag,
        pentad_in_year|decad_in_year, pentad_in_month|decad_in_month,
        composition, q05-q95, forecasted_discharge
    """
    df = df.copy()

    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

    rename_map = {
        "model_type": "model_short",
        "horizon_in_year": period_col,
        "horizon_value": period_in_month_col,
    }
    df = df.rename(columns=rename_map)

    # Ensure date is datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Ensure code is string without trailing .0
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Drop API-only columns not needed internally
    drop_cols = ["id", "horizon_type", "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _read_combined_forecasts_csv(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read combined forecasts from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv("ieasyforecast_combined_forecast_pentad_file", "")
    else:
        filename = os.getenv("ieasyforecast_combined_forecast_decad_file", "")

    if not intermediate_path or not filename:
        logger.debug(
            "Combined forecast env vars not set for %s",
            horizon_type,
        )
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Combined forecasts CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin([str(c) for c in codes])]
        return df
    except Exception as e:
        logger.error(
            "Failed to read combined forecasts CSV %s: %s",
            filepath,
            e,
        )
        return None


# ===================================================================
# Daily observations and forecasts (for Tier 2 skill metrics)
# ===================================================================


def read_daily_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read daily runoff observations from preprocessing API.

    Thin wrapper around _read_daily_runoff_api() — no aggregation,
    returns raw daily data for Tier 2 skill metric calculations.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, date, discharge_avg].
        Empty DataFrame if no data available.
    """
    empty = pd.DataFrame(columns=["code", "date", "discharge_avg"])

    try:
        daily = _read_daily_runoff_api(codes, start_year, end_year)
    except Exception as e:
        logger.error("Failed to read daily observations: %s", e)
        return empty

    if daily is None or daily.empty:
        logger.warning("No daily observation data available")
        return empty

    # Normalize columns
    df = daily.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Keep only needed columns
    cols = ["code", "date", "discharge_avg"]
    available = [c for c in cols if c in df.columns]
    return df[available]


def read_daily_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read ML forecasts with horizon_type='day' from postprocessing API.

    Deduplicates: keeps the latest forecast_date per
    (code, target date, model_short).

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, date, model_short,
        forecasted_discharge]. Empty DataFrame if no data.
    """
    empty = pd.DataFrame(columns=["code", "date", "model_short", "forecasted_discharge"])

    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping")
        return empty

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping")
        return empty

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return empty

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                df_batch = client.read_forecasts(
                    horizon="day",
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    skip=skip,
                    limit=batch_size,
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        all_records = [df for df in all_records if not df.empty]
        if not all_records:
            return empty

        df = pd.concat(all_records, ignore_index=True)
        return _normalize_daily_forecasts(df)

    except Exception as e:
        logger.error("Failed to read daily forecasts from API: %s", e)
        return empty


def _normalize_daily_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API daily forecast response and deduplicate.

    Keeps latest forecast_date per (code, target, model).

    Returns DataFrame with: [code, date, model_short,
    forecasted_discharge].
    """
    df = df.copy()

    # Rename API columns
    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})
    # API returns 'date' (issue date) and 'target' (target date).
    # Rename 'date' → 'forecast_date' first to avoid collision when
    # renaming 'target' → 'date'.
    if "target" in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "forecast_date", "target": "date"})
    elif "target" in df.columns:
        df = df.rename(columns={"target": "date"})

    # Ensure types
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Deduplicate: keep latest forecast_date per (code, date, model)
    if "forecast_date" in df.columns:
        df["forecast_date"] = pd.to_datetime(df["forecast_date"])
        df = df.sort_values("forecast_date", ascending=False)
        df = df.drop_duplicates(subset=["code", "date", "model_short"], keep="first")

    # Keep only needed columns
    cols = ["code", "date", "model_short", "forecasted_discharge"]
    available = [c for c in cols if c in df.columns]
    return df[available].reset_index(drop=True)


# ===================================================================
# Monthly observations (daily runoff → monthly mean)
# ===================================================================


def read_monthly_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Aggregate daily runoff to monthly mean discharge.

    Reads daily runoff via preprocessing API. Requires >= 50%
    non-missing days per month.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, month, month_in_year,
        discharge_avg, delta]. Empty DataFrame if no data available.
    """
    empty = pd.DataFrame(
        columns=["code", "year", "month", "month_in_year", "discharge_avg", "delta"]
    )

    try:
        daily = _read_daily_runoff_api(codes, start_year, end_year)
    except Exception as e:
        logger.error("Failed to read daily runoff: %s", e)
        return empty

    if daily is None or daily.empty:
        logger.warning("No daily runoff data available")
        return empty

    return _aggregate_daily_to_monthly(daily)


def _read_daily_runoff_api(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read daily runoff from preprocessing API with pagination.

    Returns combined DataFrame or empty DataFrame if unavailable.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping")
        return pd.DataFrame()

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping")
        return pd.DataFrame()

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePreprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Preprocessing API not ready at %s", api_url)
            return pd.DataFrame()

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                df_batch = client.read_runoff(
                    horizon="day",
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    skip=skip,
                    limit=batch_size,
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        all_records = [df.dropna(axis=1, how="all") for df in all_records if not df.empty]
        if not all_records:
            return pd.DataFrame()

        df = pd.concat(all_records, ignore_index=True)
        # API returns 'discharge'; internal convention is 'discharge_avg'
        if "discharge" in df.columns and "discharge_avg" not in df.columns:
            df = df.rename(columns={"discharge": "discharge_avg"})
        return df

    except Exception as e:
        logger.error("Failed to read daily runoff from API: %s", e)
        return pd.DataFrame()


def _aggregate_daily_to_monthly(daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily runoff to monthly means with 50% coverage filter.

    Args:
        daily: DataFrame with columns [code, date, discharge_avg].

    Returns:
        DataFrame with columns [code, year, month, month_in_year,
        discharge_avg, delta].
    """
    df = daily.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["days_in_month"] = df["date"].dt.days_in_month

    # Aggregate to monthly means per (code, year, month)
    monthly = (
        df.groupby(["code", "year", "month"])
        .agg(
            discharge_avg=("discharge_avg", "mean"),
            non_missing_days=("discharge_avg", "count"),
            days_in_month=("days_in_month", "first"),
        )
        .reset_index()
    )

    # Filter: require >= 50% non-missing days
    monthly = monthly[monthly["non_missing_days"] >= monthly["days_in_month"] * 0.5].copy()

    if monthly.empty:
        return pd.DataFrame(
            columns=["code", "year", "month", "month_in_year", "discharge_avg", "delta"]
        )

    monthly["month_in_year"] = monthly["month"]

    # Compute delta per (code, month_in_year): 0.674 * std across years
    delta_df = (
        monthly.groupby(["code", "month_in_year"])
        .agg(
            std_discharge=("discharge_avg", "std"),
        )
        .reset_index()
    )
    # Single year -> std is NaN -> delta = 0
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)

    monthly = monthly.merge(
        delta_df[["code", "month_in_year", "delta"]],
        on=["code", "month_in_year"],
        how="left",
    )

    # Drop intermediate columns
    monthly = monthly.drop(columns=["non_missing_days", "days_in_month"], errors="ignore")

    return monthly


# ===================================================================
# Monthly forecasts (from long_forecasts table)
# ===================================================================


def read_monthly_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read monthly long-term forecasts from postprocessing API.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, month, model_short,
        q50, q05, q10, q25, q75, q90, q95, valid_from, valid_to,
        date, flag]. Empty DataFrame if no data available.

    Under ``SAPPHIRE_SKILL_LEAD_AWARE`` (default OFF), raw model rows are
    additionally reduced to one operational-issuance row per (code,
    model, target year, target month) via `select_operational_issuances`
    -- the API issue-date read window is expanded backward by the
    deployment's max configured monthly lead to capture the earliest
    possible operational issuance, then selected rows are trimmed back
    to [start_year, end_year] by `valid_from`. Flag OFF is byte-identical
    to the pre-existing (unfiltered horizon_type="month") read.
    """
    empty = pd.DataFrame()

    lead_aware = skill_lead_aware_enabled()
    month_schedules: dict[str, OperationalSchedule] | None = None
    read_start_year = start_year
    if lead_aware:
        # Fail LOUD under flag-ON: a config-resolution error (e.g. a
        # month_N mode missing operational_issue_day) must NOT silently
        # fall back to an unfiltered read that retains backfill rows.
        month_schedules = _operational_schedules_for_horizon_type("month")
        max_lead = max((s.lead_time for s in month_schedules.values()), default=0)
        read_start_year = start_year - _read_window_expansion_years(max_lead)

    try:
        raw = _read_long_forecasts_api(codes, read_start_year, end_year)
    except Exception as e:
        logger.error("Failed to read monthly forecasts: %s", e)
        return empty

    if raw is None or raw.empty:
        logger.warning("No monthly forecast data available")
        return empty

    df = _normalize_monthly_forecasts(raw)

    if lead_aware and month_schedules:
        df = select_operational_issuances(
            df, month_schedules, target_year_col="year", target_period_col="month"
        )
        df = _trim_to_target_year_range(df, "year", start_year, end_year)

    return df


def _read_long_forecasts_api(
    codes: list[str],
    start_year: int,
    end_year: int,
    horizon_type: str = "month",
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read long-term forecasts from postprocessing API with pagination.

    Args:
        codes: List of station codes to query.
        start_year: First year of the date range (inclusive).
        end_year: Last year of the date range (inclusive).
        horizon_type: Horizon type filter passed to the API (e.g. ``"month"``
            or ``"season"``). Defaults to ``"month"`` to preserve existing
            behaviour for all current callers.
        horizon_value: Optional lead/horizon-value filter. When omitted, the
            request is unchanged.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping")
        return pd.DataFrame()

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping")
        return pd.DataFrame()

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return pd.DataFrame()

        all_records = []
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        for code in codes:
            skip = 0
            batch_size = 1000
            while True:
                kwargs = {
                    "horizon_type": horizon_type,
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "skip": skip,
                    "limit": batch_size,
                }
                if horizon_value is not None:
                    kwargs["horizon_value"] = horizon_value
                df_batch = client.read_long_term_forecasts(**kwargs)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        all_records = [df.dropna(axis=1, how="all") for df in all_records if not df.empty]
        if not all_records:
            return pd.DataFrame()

        return pd.concat(all_records, ignore_index=True)

    except Exception as e:
        logger.error("Failed to read long-term forecasts from API: %s", e)
        return pd.DataFrame()


def _normalize_monthly_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API response to expected column format.

    Extracts year and month from valid_from, renames model_type
    to model_short.
    """
    df = df.copy()

    # Extract year and month from valid_from
    df["valid_from"] = pd.to_datetime(df["valid_from"])
    df["year"] = df["valid_from"].dt.year
    df["month"] = df["valid_from"].dt.month

    # Rename model_type -> model_short
    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})

    # Ensure code is string
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Normalize horizon_value: coerce NaN (legacy / NULL rows from API) to
    # sentinel 0 so subsequent groupby operations do not silently drop rows.
    if "horizon_value" in df.columns:
        df["horizon_value"] = df["horizon_value"].fillna(0).astype(int)

    return df


# ===================================================================
# Operational/maintenance monthly forecast readers
# ===================================================================


def read_latest_monthly_forecasts(
    codes: list[str],
    forecast_date: dt.date | None = None,
) -> pd.DataFrame:
    """Read the most recent month's long-term forecasts from API.

    Reads forecasts with issue dates in the last 60 days,
    then filters to the single most recent target (year, month).

    Args:
        codes: Station codes to read.
        forecast_date: Reference date for lookback window.
            Defaults to today if not provided.

    Returns:
        DataFrame with columns: code, year, month, month_in_year,
        model_short, forecasted_discharge (=q50), q05-q95,
        valid_from, valid_to, date, flag.
        Empty DataFrame if no data.
    """
    today = forecast_date if forecast_date is not None else dt.date.today()
    start_date = today - dt.timedelta(days=60)
    start_year = start_date.year
    end_year = today.year

    raw = _read_long_forecasts_api(codes, start_year, end_year)
    if raw is None or raw.empty:
        logger.warning("No recent monthly forecast data available")
        return pd.DataFrame()

    df = _normalize_monthly_forecasts(raw)
    if df.empty:
        return df

    # Add month_in_year
    if "month_in_year" not in df.columns and "month" in df.columns:
        df["month_in_year"] = df["month"]

    # Add forecasted_discharge from q50 if missing
    if "forecasted_discharge" not in df.columns and "q50" in df.columns:
        df["forecasted_discharge"] = df["q50"].astype(float)

    # Filter to the latest (year, month) based on valid_from
    vf = pd.to_datetime(df["valid_from"], errors="coerce")
    if vf.notna().any():
        latest_vf = vf.max()
        latest_year = latest_vf.year
        latest_month = latest_vf.month
    else:
        latest_year = int(df["year"].max())
        latest_month = int(df[df["year"] == latest_year]["month"].max())

    df = df[(df["year"] == latest_year) & (df["month"] == latest_month)].copy()

    logger.info(
        "Read %d latest monthly forecasts for %d-%02d",
        len(df),
        latest_year,
        latest_month,
    )
    return df


def read_monthly_combined_forecasts(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read monthly combined forecasts from API (primary) or CSV
    (fallback).

    Used by the maintenance entry point for gap detection and
    merge-back after filling missing ensembles.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with combined forecasts (all models + ensembles),
        or empty DataFrame if no data available.
    """
    # API-first: try the authoritative source
    df = _read_monthly_combined_forecasts_api(codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly combined forecast rows from API",
            len(df),
        )
        return df

    # CSV fallback (deprecated)
    logger.info("API monthly combined forecasts unavailable, falling back to CSV")
    df = _read_monthly_combined_forecasts_csv(codes)
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly combined forecast rows from CSV",
            len(df),
        )
        return df

    logger.warning("No monthly combined forecasts available")
    return pd.DataFrame()


def _read_monthly_combined_forecasts_api(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly combined forecasts from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_long_term_forecasts(
                        horizon_type="month",
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            frames = [df.dropna(axis=1, how="all") for df in frames if not df.empty]
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_long_term_forecasts(
                    horizon_type="month", skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            all_records = [df.dropna(axis=1, how="all") for df in all_records if not df.empty]
            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_monthly_combined_forecasts(df)

    except Exception as e:
        logger.error(
            "Failed to read monthly combined forecasts from API: %s",
            e,
        )
        return None


def _normalize_monthly_combined_forecasts(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Normalize API monthly forecast response for gap detection.

    Delegates to _normalize_monthly_forecasts() for base
    normalization, then adds month_in_year and
    forecasted_discharge if absent.
    """
    df = _normalize_monthly_forecasts(df)

    # Add month_in_year (needed by gap detector)
    if "month_in_year" not in df.columns and "month" in df.columns:
        df["month_in_year"] = df["month"]

    # Add forecasted_discharge from q50 (needed for merge-back)
    if "forecasted_discharge" not in df.columns and "q50" in df.columns:
        df["forecasted_discharge"] = df["q50"].astype(float)

    # Drop API-only columns not needed internally. Preserve
    # horizon_value so the lead carries through merge-back and skill
    # recalc (the base normalizer already fillna(0).astype(int)s it).
    drop_cols = ["id", "horizon_type", "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _read_monthly_combined_forecasts_csv(
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read monthly combined forecasts from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")
    filename = os.getenv("ieasyforecast_monthly_combined_forecast_file", "")

    if not intermediate_path or not filename:
        logger.debug("Monthly combined forecast env vars not set")
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Monthly combined forecasts CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
        if codes is not None and not df.empty and "code" in df.columns:
            df = df[df["code"].astype(str).isin([str(c) for c in codes])]
        return df
    except Exception as e:
        logger.error(
            "Failed to read monthly combined forecasts CSV %s: %s",
            filepath,
            e,
        )
        return None


# ===================================================================
# Short-term (pentad/decad) observations and individual forecasts
# ===================================================================

# tag_library is needed for period column computation
# (pentad_in_month, pentad_in_year, etc.)
try:
    import tag_library as tl

    TAG_LIBRARY_AVAILABLE = True
except ImportError:
    TAG_LIBRARY_AVAILABLE = False
    logger.warning("tag_library not available; short-term period columns cannot be computed")


def _is_pentad_boundary(d) -> bool:
    """Return True if *d* is a pentad issue day (5/10/15/20/25/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (5, 10, 15, 20, 25, last_day)


def _is_decad_boundary(d) -> bool:
    """Return True if *d* is a decad issue day (10/20/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (10, 20, last_day)


def _clean_code_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure code column is string without trailing .0."""
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
    return df


# -------------------------------------------------------------------
# Private API reader functions
# -------------------------------------------------------------------


def _read_short_term_runoff_api(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame | None:
    """Read pentad or decad runoff observations from preprocessing API.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Raw DataFrame from API, or None if unavailable.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePreprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Preprocessing API not ready at %s", api_url)
            return None

        # Map internal horizon names to API horizon names
        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        start_date = f"{start_year}-01-01" if start_year is not None else None
        end_date = f"{end_year}-12-31" if end_year is not None else None

        all_records = []
        batch_size = 1000

        if codes is not None:
            for code in codes:
                skip = 0
                kwargs = {"horizon": api_horizon, "code": code}
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                while True:
                    df_batch = client.read_runoff(**kwargs, skip=skip, limit=batch_size)
                    if df_batch is None or df_batch.empty:
                        break
                    all_records.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
        else:
            skip = 0
            kwargs = {"horizon": api_horizon}
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date
            while True:
                df_batch = client.read_runoff(**kwargs, skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        if not all_records:
            return None

        return pd.concat(all_records, ignore_index=True)

    except Exception as e:
        logger.error("Failed to read short-term runoff from API: %s", e)
        return None


def _read_lr_forecasts_pp_api(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame | None:
    """Read LR forecasts from postprocessing API.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Raw DataFrame from API, or None if unavailable.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        start_date = f"{start_year}-01-01" if start_year is not None else None
        end_date = f"{end_year}-12-31" if end_year is not None else None

        all_records = []
        batch_size = 1000

        if codes is not None:
            for code in codes:
                skip = 0
                kwargs = {"horizon": api_horizon, "code": code}
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                while True:
                    df_batch = client.read_lr_forecasts(**kwargs, skip=skip, limit=batch_size)
                    if df_batch is None or df_batch.empty:
                        break
                    all_records.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
        else:
            skip = 0
            kwargs = {"horizon": api_horizon}
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date
            while True:
                df_batch = client.read_lr_forecasts(**kwargs, skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        if not all_records:
            return None

        return pd.concat(all_records, ignore_index=True)

    except Exception as e:
        logger.error("Failed to read LR forecasts from API: %s", e)
        return None


def _read_ml_forecasts_pp_api(
    model: str,
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame | None:
    """Read ML forecasts from postprocessing API.

    Reads both horizon='day' (current pipeline writes daily targets) and
    horizon=horizon_type (migrated period archive), then keeps period rows
    only before each station/model's first DAY issue date.

    Args:
        model: Model short name (e.g. 'TFT', 'TiDE').
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Raw DataFrame from API, or None if unavailable.
    """

    def _fetch_archive(try_horizon: str) -> pd.DataFrame | None:
        all_records = []
        batch_size = 1000

        if codes is not None:
            for code in codes:
                skip = 0
                kwargs = {
                    "horizon": try_horizon,
                    "model": model,
                    "code": code,
                }
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                while True:
                    df_batch = client.read_short_term_forecasts(
                        **kwargs, skip=skip, limit=batch_size
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    all_records.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
        else:
            skip = 0
            kwargs = {
                "horizon": try_horizon,
                "model": model,
            }
            if start_date:
                kwargs["start_date"] = start_date
            if end_date:
                kwargs["end_date"] = end_date
            while True:
                df_batch = client.read_short_term_forecasts(**kwargs, skip=skip, limit=batch_size)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

        if not all_records:
            return None

        return pd.concat(all_records, ignore_index=True)

    def _working_archive(df: pd.DataFrame) -> pd.DataFrame:
        work = _clean_code_column(df.copy())
        if "date" in work.columns:
            work["date"] = pd.to_datetime(work["date"])
        if "model_type" in work.columns:
            work["_pp036_model_type_key"] = work["model_type"].astype(str)
        else:
            work["_pp036_model_type_key"] = model
        return work

    def _merge_archives_by_day_cutover(
        day_df: pd.DataFrame | None,
        period_df: pd.DataFrame | None,
    ) -> pd.DataFrame | None:
        day_rows = 0 if day_df is None else len(day_df)
        period_rows = 0 if period_df is None else len(period_df)

        if (day_df is None or day_df.empty) and (period_df is None or period_df.empty):
            logger.debug(
                "Read ML forecasts for %s (%s): day_rows=0, period_rows=0, "
                "retained_period_rows=0, final_rows=0",
                model,
                horizon_type,
            )
            return None

        if day_df is None or day_df.empty:
            logger.debug(
                "Read ML forecasts for %s (%s): day_rows=0, period_rows=%d, "
                "retained_period_rows=%d, final_rows=%d",
                model,
                horizon_type,
                period_rows,
                period_rows,
                period_rows,
            )
            return period_df

        if period_df is None or period_df.empty:
            logger.debug(
                "Read ML forecasts for %s (%s): day_rows=%d, period_rows=0, "
                "retained_period_rows=0, final_rows=%d",
                model,
                horizon_type,
                day_rows,
                day_rows,
            )
            return day_df

        day_work = _working_archive(day_df)
        period_work = _working_archive(period_df)
        pair_cols = ["code", "_pp036_model_type_key"]

        first_day = day_work.groupby(pair_cols)["date"].min()
        first_period = period_work.groupby(pair_cols)["date"].min()

        for pair, first_day_date in first_day.items():
            if pair not in first_period:
                continue
            first_period_date = first_period[pair]
            if first_day_date < first_period_date:
                logger.warning(
                    "DAY ML archive for %s code=%s model_type=%s starts at %s "
                    "before period archive starts at %s",
                    model,
                    pair[0],
                    pair[1],
                    first_day_date.date(),
                    first_period_date.date(),
                )

        period_with_cutover = period_work.merge(
            first_day.rename("_pp036_first_day_date"),
            left_on=pair_cols,
            right_index=True,
            how="left",
        )
        retain_period = period_with_cutover["_pp036_first_day_date"].isna() | (
            period_with_cutover["date"] < period_with_cutover["_pp036_first_day_date"]
        )
        retained_period = period_df.loc[retain_period.to_numpy()].copy()
        final = pd.concat([retained_period, day_df], ignore_index=True)

        logger.debug(
            "Read ML forecasts for %s (%s): day_rows=%d, period_rows=%d, "
            "retained_period_rows=%d, final_rows=%d",
            model,
            horizon_type,
            day_rows,
            period_rows,
            len(retained_period),
            len(final),
        )
        return final

    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        api_horizon = "decade" if horizon_type == "decad" else horizon_type

        start_date = f"{start_year}-01-01" if start_year is not None else None
        end_date = f"{end_year}-12-31" if end_year is not None else None

        day_records = _fetch_archive("day")
        period_records = _fetch_archive(api_horizon)
        return _merge_archives_by_day_cutover(day_records, period_records)

    except Exception as e:
        logger.error(
            "Failed to read ML forecasts for %s from API: %s",
            model,
            e,
        )
        return None


# -------------------------------------------------------------------
# Normalization functions
# -------------------------------------------------------------------


def _normalize_observed_runoff(df: pd.DataFrame, horizon_type: str) -> pd.DataFrame:
    """Normalize API runoff response to internal observed column format.

    Args:
        df: Raw DataFrame from preprocessing API.
        horizon_type: 'pentad' or 'decad'.

    Returns:
        DataFrame with columns: [code, date, discharge_avg,
        model_short, pentad_in_year, pentad_in_month] (or decad
        equivalents).
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

    # Rename API columns
    rename_map = {
        "discharge": "discharge_avg",
        "horizon_in_year": period_col,
        "horizon_value": period_in_month_col,
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Add model_short = "Obs"
    df["model_short"] = "Obs"

    # Clean code column
    df = _clean_code_column(df)

    # Parse dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Drop API-only columns
    drop_cols = ["id", "horizon_type", "model_type_description"]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


def _normalize_lr_forecasts(
    df: pd.DataFrame, horizon_type: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize API LR forecast response and split forecasts + stats.

    Args:
        df: Raw DataFrame from postprocessing API.
        horizon_type: 'pentad' or 'decad'.

    Returns:
        Tuple of (forecasts_df, stats_df).
        - forecasts_df: [code, date, forecasted_discharge, predictor,
          slope, intercept, rsquared, model_short, pentad_in_month,
          pentad_in_year] (or decad equivalents)
        - stats_df: [date, code, q_mean, q_std_sigma, delta]
    """
    empty_fc = pd.DataFrame()
    empty_stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])

    if df is None or df.empty:
        return empty_fc, empty_stats

    df = df.copy()

    # Clean code column and parse dates
    df = _clean_code_column(df)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Rename model_type -> model_short, or set it explicitly.
    # The lr-forecast API endpoint does not return a model_type column,
    # so we must assign model_short = "LR" when it's absent.
    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})
    if "model_short" not in df.columns:
        df["model_short"] = "LR"

    # Extract stats columns before dropping them from forecasts
    stats_cols = ["date", "code", "q_mean", "q_std_sigma", "delta"]
    stats_present = [c for c in stats_cols if c in df.columns]
    if len(stats_present) >= 3:  # At least date, code, and one stat
        stats = df[stats_present].drop_duplicates().copy()
    else:
        stats = empty_stats

    # Build forecasts: drop stats-only columns and discharge_avg
    drop_from_fc = [
        "q_mean",
        "q_std_sigma",
        "delta",
        "discharge_avg",
    ]
    forecasts = df.drop(
        columns=[c for c in drop_from_fc if c in df.columns],
        errors="ignore",
    )

    # Compute period columns using tag_library
    if TAG_LIBRARY_AVAILABLE and "date" in forecasts.columns:
        period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
        period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

        if horizon_type == "pentad":
            get_period = tl.get_pentad
            get_period_in_year = tl.get_pentad_in_year
        else:
            get_period = tl.get_decad_in_month
            get_period_in_year = tl.get_decad_in_year

        # +1 day offset: the forecast date is the last day of the
        # previous period, so +1 day gives the first day of the
        # forecasted period.
        offset_dates = forecasts["date"] + pd.Timedelta(days=1)
        forecasts[period_in_month_col] = offset_dates.apply(get_period)
        forecasts[period_col] = offset_dates.apply(get_period_in_year)

    # Deduplicate on [date, code], keep last
    if "date" in forecasts.columns and "code" in forecasts.columns:
        forecasts = forecasts.drop_duplicates(subset=["date", "code"], keep="last")

    # Drop API-only columns
    drop_cols = [
        "id",
        "horizon_type",
        "horizon_value",
        "horizon_in_year",
        "model_type_description",
    ]
    forecasts = forecasts.drop(
        columns=[c for c in drop_cols if c in forecasts.columns],
        errors="ignore",
    )

    return forecasts, stats


def _normalize_ml_forecasts(
    df: pd.DataFrame,
    model: str,
    horizon_type: str,
) -> pd.DataFrame:
    """Normalize API ML forecast response: aggregate daily->pentad/decad.

    Groups daily targets by (code, date) and computes:
    - mean for forecasted_discharge, q05, q25, q75, q95
    - max for flag
    - first for horizon_value, horizon_in_year

    Args:
        df: Raw DataFrame from postprocessing API.
        model: Model short name from API (e.g. 'TFT', 'TIDE').
        horizon_type: 'pentad' or 'decad'.

    Returns:
        DataFrame with aggregated forecasts and period columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Clean code column and parse dates
    df = _clean_code_column(df)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # PP-031: Drop rows where date is not a boundary day for this horizon.
    if "date" in df.columns:
        if horizon_type == "pentad":
            boundary_mask = df["date"].apply(_is_pentad_boundary)
        else:
            boundary_mask = df["date"].apply(_is_decad_boundary)

        n_non_boundary = (~boundary_mask).sum()
        if n_non_boundary > 0:
            logger.info(
                "Dropped %d/%d rows on non-%s-boundary dates for %s",
                n_non_boundary,
                len(df),
                horizon_type,
                model,
            )
        df = df[boundary_mask].copy()

        if df.empty:
            return pd.DataFrame()

    # Filter daily targets to the forecast period boundary.
    # The forecast date is the last day of the previous period;
    # date+1 is the first day of the target period.
    if TAG_LIBRARY_AVAILABLE and "target" in df.columns and "date" in df.columns:
        df["target"] = pd.to_datetime(df["target"])

        if horizon_type == "pentad":
            period_func = tl.get_pentad_in_year
        else:
            period_func = tl.get_decad_in_year

        expected_period = (df["date"] + pd.Timedelta(days=1)).apply(period_func)
        target_period = df["target"].apply(period_func)

        in_period = target_period == expected_period
        n_dropped = (~in_period).sum()
        if n_dropped > 0:
            logger.info(
                "Filtered %d/%d daily targets outside %s boundary for %s",
                n_dropped,
                len(df),
                horizon_type,
                model,
            )
        df = df[in_period].copy()

        if df.empty:
            logger.warning(
                "No %s targets within period for model %s after filtering",
                horizon_type,
                model,
            )
            return pd.DataFrame()

    # Aggregate daily targets -> pentad/decad level
    numeric_cols = [
        "q05",
        "q25",
        "q75",
        "q95",
        "forecasted_discharge",
    ]
    agg_dict = {}
    for col in numeric_cols:
        if col in df.columns:
            agg_dict[col] = "mean"

    if "flag" in df.columns:
        agg_dict["flag"] = "max"

    for col in ["horizon_value", "horizon_in_year"]:
        if col in df.columns:
            agg_dict[col] = "first"

    if agg_dict and "code" in df.columns and "date" in df.columns:
        df = df.groupby(["code", "date"], as_index=False).agg(agg_dict)
        count_quantile_crossings(df, ["q05", "q25", "q75", "q95"], label="daily→pentad/decad")

    # Model name mapping: API stores uppercase, need display names
    model_name_map = {
        "TFT": "TFT",
        "TIDE": "TiDE",
        "TSMIXER": "TSMixer",
        "ARIMA": "ARIMA",
    }
    df["model_short"] = model_name_map.get(model.upper(), model)

    # Compute period columns using tag_library
    if TAG_LIBRARY_AVAILABLE and "date" in df.columns:
        period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
        period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"

        if horizon_type == "pentad":
            get_period = tl.get_pentad
            get_period_in_year = tl.get_pentad_in_year
        else:
            get_period = tl.get_decad_in_month
            get_period_in_year = tl.get_decad_in_year

        offset_dates = df["date"] + pd.Timedelta(days=1)
        df[period_in_month_col] = offset_dates.apply(get_period)
        df[period_col] = offset_dates.apply(get_period_in_year)

    # Drop API-only columns
    drop_cols = [
        "id",
        "horizon_type",
        "horizon_value",
        "horizon_in_year",
        "model_type",
        "model_type_description",
    ]
    df = df.drop(
        columns=[c for c in drop_cols if c in df.columns],
        errors="ignore",
    )

    return df


# -------------------------------------------------------------------
# Public orchestrator functions
# -------------------------------------------------------------------


def read_short_term_observations(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame:
    """Read pentad or decad runoff observations from API or CSV.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, date, discharge_avg,
        model_short, pentad_in_year, pentad_in_month] (or decad
        equivalents). Empty DataFrame if no data available.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    # API-first
    raw = _read_short_term_runoff_api(horizon_type, codes, start_year, end_year)
    if raw is not None and not raw.empty:
        df = _normalize_observed_runoff(raw, horizon_type)
        logger.info(
            "Read %d short-term observations from API (%s)",
            len(df),
            horizon_type,
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API short-term observations unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_short_term_observations_csv(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d short-term observations from CSV (%s)",
            len(df),
            horizon_type,
        )
        return df

    logger.warning("No short-term observations available for %s", horizon_type)
    return pd.DataFrame()


def _read_short_term_observations_csv(
    horizon_type: str,
) -> pd.DataFrame | None:
    """Read pentad/decad observations from CSV (deprecated fallback).

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv("ieasyforecast_pentadal_discharge_file", "")
    else:
        filename = os.getenv("ieasyforecast_decadal_discharge_file", "")

    if not intermediate_path or not filename:
        logger.debug("Discharge CSV env vars not set for %s", horizon_type)
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Discharge CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        df = _clean_code_column(df)
        if "model_short" not in df.columns:
            df["model_short"] = "Obs"
        return df
    except Exception as e:
        logger.error("Failed to read discharge CSV %s: %s", filepath, e)
        return None


def read_individual_model_forecasts(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read all individual model forecasts (LR + ML) for a horizon.

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Tuple of (forecasts_df, stats_df).
        - forecasts_df: Concatenation of all model forecasts.
        - stats_df: Statistics from LR (q_mean, q_std_sigma, delta).

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    all_forecasts = []
    stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])

    # 1. Read LR forecasts
    lr_raw = _read_lr_forecasts_pp_api(horizon_type, codes, start_year, end_year)
    if lr_raw is not None and not lr_raw.empty:
        lr_fc, lr_stats = _normalize_lr_forecasts(lr_raw, horizon_type)
        if not lr_fc.empty:
            all_forecasts.append(lr_fc)
            logger.info(
                "Read %d LR forecast rows from API (%s)",
                len(lr_fc),
                horizon_type,
            )
        if not lr_stats.empty:
            stats = lr_stats
    else:
        logger.info("No LR forecasts from API for %s", horizon_type)

    # 2. Read ML models (env-gated)
    run_ml = os.getenv("ieasyhydroforecast_run_ML_models", "false").lower()
    if run_ml == "true":
        available_models_str = os.getenv("ieasyhydroforecast_available_ML_models", "")
        # Env var uses uppercase (TIDE, TSMIXER); API expects camelCase.
        _ml_name_map = {"TIDE": "TiDE", "TSMIXER": "TSMixer", "TFT": "TFT"}
        if available_models_str:
            available_models = [
                _ml_name_map.get(m.strip().upper(), m.strip())
                for m in available_models_str.split(",")
                if m.strip()
            ]
        else:
            available_models = []

        for model in available_models:
            ml_raw = _read_ml_forecasts_pp_api(model, horizon_type, codes, start_year, end_year)
            if ml_raw is not None and not ml_raw.empty:
                ml_fc = _normalize_ml_forecasts(ml_raw, model, horizon_type)
                if not ml_fc.empty:
                    all_forecasts.append(ml_fc)
                    logger.info(
                        "Read %d %s forecast rows from API (%s)",
                        len(ml_fc),
                        model,
                        horizon_type,
                    )
            else:
                logger.info(
                    "No %s forecasts from API for %s",
                    model,
                    horizon_type,
                )

    if all_forecasts:
        forecasts = pd.concat(all_forecasts, ignore_index=True)
    else:
        forecasts = pd.DataFrame()

    return forecasts, stats


def read_individual_model_forecasts_for_dates(
    horizon_type: str,
    dates: list,
    codes: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read LR + ML forecasts scoped to a specific set of dates.

    More efficient than ``read_individual_model_forecasts()`` when only a
    small number of gap or stale dates need to be filled. Calls the full
    reader with year bounds derived from ``dates``, then filters in-memory
    to exact dates.

    Args:
        horizon_type: 'pentad' or 'decad'.
        dates: Boundary dates to fetch data for (Timestamp, date, or str).
        codes: Station codes to filter. None reads all.

    Returns:
        Same tuple as ``read_individual_model_forecasts()``:
        (forecasts_df, stats_df).

    Raises:
        ValueError: If horizon_type is invalid.
    """
    empty_stats = pd.DataFrame(columns=["date", "code", "q_mean", "q_std_sigma", "delta"])
    if not dates:
        return pd.DataFrame(), empty_stats

    dates_ts = pd.to_datetime(list(dates))
    min_year = int(dates_ts.year.min())
    max_year = int(dates_ts.year.max())

    forecasts, stats = read_individual_model_forecasts(
        horizon_type,
        codes=codes,
        start_year=min_year,
        end_year=max_year,
    )

    if forecasts.empty:
        return forecasts, stats

    if not pd.api.types.is_datetime64_any_dtype(forecasts["date"]):
        forecasts = forecasts.copy()
        forecasts["date"] = pd.to_datetime(forecasts["date"])

    date_set = set(dates_ts)
    forecasts = forecasts[forecasts["date"].isin(date_set)].copy()

    logger.info(
        "read_individual_model_forecasts_for_dates (%s): %d dates requested, %d rows returned",
        horizon_type,
        len(date_set),
        len(forecasts),
    )
    return forecasts, stats


def read_observed_and_modelled_data(
    horizon_type: str,
    codes: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read observed and modelled data for pentad or decad horizon.

    API-first reader that replaces
    setup_library.read_observed_and_modelled_data_pentade() and
    setup_library.read_observed_and_modelled_data_decade().

    Does NOT include NE or virtual station calculations -- those must
    be called separately from the entry point via
    sl.calculate_virtual_stations_data() and
    sl.calculate_neural_ensemble_forecast() /
    sl.calculate_neural_ensemble_forecast_decade().

    Args:
        horizon_type: 'pentad' or 'decad'.
        codes: Station codes to filter. None reads all.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Tuple of (observed_df, modelled_df).
        - observed_df includes stats (q_mean, q_std_sigma, delta)
          merged from LR.
        - modelled_df contains all individual model forecasts.

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}")

    # Read observations
    observed = read_short_term_observations(horizon_type, codes, start_year, end_year)

    # Read individual model forecasts
    forecasts, stats = read_individual_model_forecasts(horizon_type, codes, start_year, end_year)

    # Merge stats into observed
    if (
        not stats.empty
        and not observed.empty
        and "date" in observed.columns
        and "code" in observed.columns
    ):
        merge_cols = ["date", "code"]
        stats_to_merge = stats.copy()
        if "date" in stats_to_merge.columns:
            stats_to_merge["date"] = pd.to_datetime(stats_to_merge["date"])
        observed = pd.merge(
            observed,
            stats_to_merge,
            on=merge_cols,
            how="left",
        )

    return observed, forecasts


# ===================================================================
# Quarterly skill metrics, observations, and forecasts
# ===================================================================


def read_quarterly_skill_metrics(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated quarterly skill metrics from API.

    API-only (no CSV fallback for new horizons).
    Tombstone rows (n_pairs == 0) are silently dropped before returning.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [quarter_in_year, code, model_short,
        sdivsigma, nse, delta, accuracy, mae, n_pairs, ...]
    """
    df = _read_horizon_skill_metrics_api("quarter", codes)
    if df is not None and not df.empty:
        df = _drop_tombstone_rows(df)
        logger.info("Read %d quarterly skill metric rows from API", len(df))
        return df
    logger.warning("No quarterly skill metrics available")
    return pd.DataFrame()


def read_seasonal_skill_metrics(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read pre-calculated seasonal skill metrics from API.

    API-only (no CSV fallback for new horizons).
    Tombstone rows (n_pairs == 0) are silently dropped before returning.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only skill metrics for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with columns: [season_in_year, code, model_short,
        sdivsigma, nse, delta, accuracy, mae, n_pairs, ...]
    """
    df = _read_horizon_skill_metrics_api("season", codes)
    if df is not None and not df.empty:
        df = _drop_tombstone_rows(df)
        logger.info("Read %d seasonal skill metric rows from API", len(df))
        return df
    logger.warning("No seasonal skill metrics available")
    return pd.DataFrame()


def _read_horizon_skill_metrics_api(
    horizon_type: str,
    codes: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read skill metrics from API for an arbitrary horizon type.

    Shared implementation for quarter/season (and potentially others).
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    df_batch = client.read_skill_metrics(
                        horizon=horizon_type,
                        code=code,
                        skip=skip,
                        limit=batch_size,
                    )
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                df_batch = client.read_skill_metrics(
                    horizon=horizon_type, skip=skip, limit=batch_size
                )
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_horizon_skill_metrics(df, horizon_type)

    except Exception as e:
        logger.error(
            "Failed to read %s skill metrics from API: %s",
            horizon_type,
            e,
        )
        return None


def _normalize_horizon_skill_metrics(
    df: pd.DataFrame,
    horizon_type: str,
) -> pd.DataFrame:
    """Normalize API skill metrics response for quarter/season horizons.

    Maps horizon_in_year → quarter_in_year or season_in_year,
    model_type → model_short.
    """
    period_col_map = {
        "quarter": "quarter_in_year",
        "season": "season_in_year",
    }
    period_col = period_col_map.get(horizon_type, f"{horizon_type}_in_year")

    rename_map = {
        "horizon_in_year": period_col,
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    return df


# -------------------------------------------------------------------
# Quarterly/seasonal observations — delegate to monthly + aggregate
# -------------------------------------------------------------------


def read_quarterly_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read quarterly observations by aggregating monthly observations.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, quarter_in_year,
        discharge_avg, delta].
    """
    from src.aggregation import aggregate_monthly_obs_to_quarterly

    monthly = read_monthly_observations(codes, start_year, end_year)
    if monthly.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "year",
                "quarter_in_year",
                "discharge_avg",
                "delta",
            ]
        )
    return aggregate_monthly_obs_to_quarterly(monthly)


def read_seasonal_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read seasonal observations by aggregating monthly observations.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, season_year, season_in_year,
        discharge_avg, delta].
    """
    from src.aggregation import aggregate_monthly_obs_to_seasonal

    monthly = read_monthly_observations(codes, start_year, end_year)
    if monthly.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "season_year",
                "season_in_year",
                "discharge_avg",
                "delta",
            ]
        )
    return aggregate_monthly_obs_to_seasonal(monthly)


# -------------------------------------------------------------------
# Quarterly/seasonal forecasts — delegate to monthly + aggregate
# -------------------------------------------------------------------


def read_quarterly_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read quarterly forecasts from aggregated monthly and direct API sources.

    Combines two sources:
    1. Monthly forecasts aggregated to quarterly via
       ``aggregate_monthly_fc_to_quarterly``.
    2. Direct quarterly forecasts read from the API
       (``horizon_type="quarter"``).

    When a model appears in both sources for the same quarter, the
    direct quarterly forecast takes precedence.  Raw model rows are
    restricted to the supported two-model set (LR_Base, LR_SM) after
    combining monthly-aggregated and direct quarterly sources.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, quarter_in_year,
        model_short, q05-q95, forecasted_discharge, valid_from,
        valid_to].
    """
    from src.aggregation import aggregate_monthly_fc_to_quarterly

    empty_cols = [
        "code",
        "year",
        "quarter_in_year",
        "model_short",
    ]

    # Source 1: aggregate monthly forecasts to quarterly
    monthly = read_monthly_forecasts(codes, start_year, end_year)
    if not monthly.empty:
        aggregated = aggregate_monthly_fc_to_quarterly(monthly)
    else:
        aggregated = pd.DataFrame()

    # Source 2: direct quarterly forecasts from API.
    #
    # Under SAPPHIRE_SKILL_LEAD_AWARE (default OFF), read WITHOUT the
    # horizon_value filter (read-then-derive-then-filter), expand the
    # read window backward by the configured quarter lead, and reduce
    # to one operational-issuance row per (code, model, target year,
    # target quarter). Flag OFF keeps the pre-existing single-lead API
    # filter unchanged (quarter_horizon_value()).
    lead_aware = skill_lead_aware_enabled()
    quarter_schedules: dict[str, OperationalSchedule] | None = None
    q_start_year = start_year
    if lead_aware:
        # Fail LOUD under flag-ON (no silent fallback to an unfiltered read).
        quarter_schedules = _operational_schedules_for_horizon_type("quarter")
        max_lead = max((s.lead_time for s in quarter_schedules.values()), default=0)
        q_start_year = start_year - _read_window_expansion_years(max_lead)

    if lead_aware and quarter_schedules:
        raw_q = _read_long_forecasts_api(
            codes,
            q_start_year,
            end_year,
            horizon_type="quarter",
        )
    else:
        raw_q = _read_long_forecasts_api(
            codes,
            start_year,
            end_year,
            horizon_type="quarter",
            horizon_value=quarter_horizon_value(),
        )
    if raw_q is not None and not raw_q.empty:
        direct = _normalize_combined_forecasts(raw_q, "quarter")
        if lead_aware and quarter_schedules and not direct.empty:
            direct = select_operational_issuances(
                direct,
                quarter_schedules,
                target_year_col="year",
                target_period_col="quarter_in_year",
            )
            direct = _trim_to_target_year_range(direct, "year", start_year, end_year)
    else:
        direct = pd.DataFrame()

    # Combine sources
    if aggregated.empty and direct.empty:
        return pd.DataFrame(columns=empty_cols)

    if aggregated.empty:
        combined = direct
    elif direct.empty:
        combined = aggregated
    else:
        # Concat: aggregated first, direct second.
        # drop_duplicates(keep="last") prefers direct.
        combined = pd.concat([aggregated, direct], ignore_index=True)
        dedup_cols = ["code", "year", "quarter_in_year", "model_short"]
        available = [c for c in dedup_cols if c in combined.columns]
        combined = combined.drop_duplicates(subset=available, keep="last")

    if combined.empty:
        return pd.DataFrame(columns=empty_cols)

    combined = _filter_supported_aggregated_forecast_models(combined)
    if combined.empty:
        return pd.DataFrame(columns=empty_cols)

    # Select canonical output columns
    combined = combined[[c for c in _QUARTERLY_FC_COLS if c in combined.columns]]

    # Normalize valid_from/valid_to to strings for consistency
    for col in ("valid_from", "valid_to"):
        if col in combined.columns:
            combined[col] = combined[col].astype(str)

    return combined


def read_seasonal_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read seasonal forecasts directly from the API.

    Reads forecasts stored with horizon_type="season" in the
    postprocessing API. Raw model rows are restricted to the supported
    two-model set (LR_Base, LR_SM); existing ensemble rows are kept.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).
        horizon_value: Optional seasonal issue lead to read.

    Returns:
        DataFrame with columns: [code, season_year, season_in_year,
        horizon_value, date, model_short, q05-q95,
        forecasted_discharge, valid_from, valid_to].

    Under ``SAPPHIRE_SKILL_LEAD_AWARE`` (default OFF), raw model rows are
    additionally reduced to one operational-issuance row per (code,
    model, target season_year, target season_in_year) via
    `select_operational_issuances` -- read WITHOUT the `horizon_value`
    API filter (read-then-derive-then-filter), with the read window
    expanded backward by the configured seasonal lead(s). When `horizon_value`
    is given, selection is restricted to the schedule(s) matching that
    lead so the caller's per-lead loop (see `recalculate_skill_metrics.py`)
    still yields one frame per configured seasonal lead. Flag OFF is
    byte-identical to the pre-existing single-`horizon_value`-filtered read.
    """
    empty = pd.DataFrame(
        columns=[
            "code",
            "season_year",
            "season_in_year",
            "model_short",
        ]
    )

    lead_aware = skill_lead_aware_enabled()
    season_schedules: dict[str, OperationalSchedule] | None = None
    read_start_year = start_year
    read_horizon_value = horizon_value
    if lead_aware:
        # Fail LOUD under flag-ON (no silent fallback to an unfiltered read).
        all_season_schedules = _operational_schedules_for_horizon_type("season")

        if horizon_value is not None:
            candidate_schedules = {
                mode: sched
                for mode, sched in all_season_schedules.items()
                if sched.lead_time == horizon_value
            }
        else:
            candidate_schedules = all_season_schedules

        if candidate_schedules:
            season_schedules = candidate_schedules
            max_lead = max(s.lead_time for s in season_schedules.values())
            read_start_year = start_year - _read_window_expansion_years(max_lead)
            read_horizon_value = None

    raw = _read_long_forecasts_api(
        codes,
        read_start_year,
        end_year,
        horizon_type="season",
        horizon_value=read_horizon_value,
    )
    if raw is None or raw.empty:
        logger.info("No seasonal forecast data from API for %d-%d", start_year, end_year)
        return empty

    df = _normalize_combined_forecasts(raw, "season")
    if df.empty:
        return empty

    if lead_aware and season_schedules:
        # season_in_year IS the lead key (one irrigation season/year), so
        # it is NOT an independent target period: target unit is
        # (code, model, season_year) and the derived lead is written into
        # BOTH horizon_value AND season_in_year so downstream seasonal
        # skill keys on the correct lead (not the stored sentinel 0).
        df = select_operational_issuances(
            df,
            season_schedules,
            target_year_col="season_year",
            target_period_col=None,
            lead_output_cols=("horizon_value", "season_in_year"),
        )
        df = _trim_to_target_year_range(df, "season_year", start_year, end_year)
        if df.empty:
            return empty

    df = _filter_supported_aggregated_forecast_models(df)
    if df.empty:
        return empty

    # Select canonical output columns
    df = df[[c for c in _SEASONAL_FC_COLS if c in df.columns]]
    df = _deduplicate_seasonal_forecasts(df)

    # Normalize valid_from/valid_to to strings for consistency
    for col in ("valid_from", "valid_to", "date"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df


# -------------------------------------------------------------------
# Latest quarterly/seasonal forecasts (for operational entry point)
# -------------------------------------------------------------------


def read_latest_quarterly_forecasts(
    codes: list[str],
    forecast_date: dt.date | None = None,
) -> pd.DataFrame:
    """Read latest quarterly forecasts from aggregated monthly and direct API.

    Combines two sources:
    1. Monthly forecasts (120-day lookback) aggregated to quarterly.
    2. Direct quarterly forecasts from the API.

    When a model appears in both sources, the direct forecast wins.
    Raw model rows are restricted to LR_Base and LR_SM after combining
    the two sources; existing ensemble rows are kept.

    Args:
        codes: Station codes to read.
        forecast_date: Reference date for lookback window.

    Returns:
        DataFrame with quarterly forecasts for the most recent
        quarter. Empty DataFrame if no data.
    """
    from src.aggregation import (
        aggregate_monthly_fc_to_quarterly,
    )

    today = forecast_date if forecast_date is not None else dt.date.today()
    start_date = today - dt.timedelta(days=120)
    start_year = start_date.year
    end_year = today.year

    # Source 1: aggregate monthly forecasts to quarterly
    raw_m = _read_long_forecasts_api(codes, start_year, end_year)
    if raw_m is not None and not raw_m.empty:
        df_m = _normalize_monthly_forecasts(raw_m)
        if "forecasted_discharge" not in df_m.columns and "q50" in df_m.columns:
            df_m["forecasted_discharge"] = df_m["q50"].astype(float)
        aggregated = aggregate_monthly_fc_to_quarterly(df_m)
    else:
        aggregated = pd.DataFrame()

    # Source 2: direct quarterly forecasts from API
    raw_q = _read_long_forecasts_api(
        codes,
        start_year,
        end_year,
        horizon_type="quarter",
        horizon_value=quarter_horizon_value(),
    )
    if raw_q is not None and not raw_q.empty:
        direct = _normalize_combined_forecasts(raw_q, "quarter")
    else:
        direct = pd.DataFrame()

    # Combine sources
    if aggregated.empty and direct.empty:
        logger.warning("No quarterly forecast data available")
        return pd.DataFrame(columns=_QUARTERLY_FC_COLS)

    if aggregated.empty:
        combined = direct
    elif direct.empty:
        combined = aggregated
    else:
        combined = pd.concat([aggregated, direct], ignore_index=True)
        dedup_cols = ["code", "year", "quarter_in_year", "model_short"]
        available = [c for c in dedup_cols if c in combined.columns]
        combined = combined.drop_duplicates(subset=available, keep="last")

    if combined.empty:
        return pd.DataFrame(columns=_QUARTERLY_FC_COLS)

    combined = _filter_supported_aggregated_forecast_models(combined)
    if combined.empty:
        return pd.DataFrame(columns=_QUARTERLY_FC_COLS)

    # Select canonical output columns
    combined = combined[[c for c in _QUARTERLY_FC_COLS if c in combined.columns]]

    # Normalize valid_from/valid_to to strings
    for col in ("valid_from", "valid_to"):
        if col in combined.columns:
            combined[col] = combined[col].astype(str)

    # Filter to the most recent quarter
    max_year = int(combined["year"].max())
    max_q = int(combined[combined["year"] == max_year]["quarter_in_year"].max())
    combined = combined[
        (combined["year"] == max_year) & (combined["quarter_in_year"] == max_q)
    ].copy()

    logger.info(
        "Read %d latest quarterly forecasts for Q%d-%d",
        len(combined),
        max_q,
        max_year,
    )
    return combined


def read_latest_seasonal_forecasts(
    codes: list[str],
    forecast_date: dt.date | None = None,
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read the most recent seasonal forecasts directly from the API.

    Uses a wide lookback (~200 days) to capture cross-year seasons.
    Raw model rows are restricted to LR_Base and LR_SM; existing
    ensemble rows are kept.

    Args:
        codes: Station codes to read.
        forecast_date: Reference date for lookback window.
        horizon_value: Optional seasonal issue lead to read.

    Returns:
        DataFrame with seasonal forecasts for the most recent season.
        Empty DataFrame if no data.
    """
    today = forecast_date if forecast_date is not None else dt.date.today()
    start_date = today - dt.timedelta(days=200)
    start_year = start_date.year
    end_year = today.year

    raw = _read_long_forecasts_api(
        codes,
        start_year,
        end_year,
        horizon_type="season",
        horizon_value=horizon_value,
    )
    if raw is None or raw.empty:
        logger.warning("No recent seasonal forecast data from API")
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    df = _normalize_combined_forecasts(raw, "season")
    if df.empty:
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    df = _filter_supported_aggregated_forecast_models(df)
    if df.empty:
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    # Select canonical output columns
    df = df[[c for c in _SEASONAL_FC_COLS if c in df.columns]]
    df = _deduplicate_seasonal_forecasts(df)

    # Normalize valid_from/valid_to to strings
    for col in ("valid_from", "valid_to", "date"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Filter to the most recent season_year
    max_sy = int(df["season_year"].max())
    df = df[df["season_year"] == max_sy].copy()

    logger.info(
        "Read %d latest seasonal forecasts for season_year %d",
        len(df),
        max_sy,
    )
    return df


# -------------------------------------------------------------------
# Quarterly/seasonal combined forecasts (from API)
# -------------------------------------------------------------------


def read_quarterly_combined_forecasts(
    codes: list[str] | None = None,
) -> pd.DataFrame:
    """Read quarterly combined forecasts from API.

    API-only — no CSV fallback for new horizons.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with combined quarterly forecasts, or empty DataFrame.
    """
    df = _read_long_combined_forecasts_api(
        "quarter",
        codes,
        horizon_value=quarter_horizon_value(),
    )
    if df is not None and not df.empty:
        logger.info("Read %d quarterly combined forecast rows from API", len(df))
        return df
    logger.warning("No quarterly combined forecasts available")
    return pd.DataFrame()


def read_seasonal_combined_forecasts(
    codes: list[str] | None = None,
    horizon_value: int | None = None,
) -> pd.DataFrame:
    """Read seasonal combined forecasts from API.

    API-only — no CSV fallback for new horizons.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.
        horizon_value: Optional seasonal issue lead to read.

    Returns:
        DataFrame with combined seasonal forecasts, or empty DataFrame.
    """
    df = _read_long_combined_forecasts_api("season", codes, horizon_value=horizon_value)
    if df is not None and not df.empty:
        logger.info("Read %d seasonal combined forecast rows from API", len(df))
        return df
    logger.warning("No seasonal combined forecasts available")
    return pd.DataFrame()


def _read_long_combined_forecasts_api(
    horizon_type: str,
    codes: list[str] | None = None,
    horizon_value: int | None = None,
) -> pd.DataFrame | None:
    """Read long-term combined forecasts from API for a given horizon type.

    Shared implementation for quarter/season.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.readiness_check():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        batch_size = 1000
        if codes is not None:
            # Per-code loop: API supports code= but not batch code__in
            frames = []
            for code in codes:
                skip = 0
                while True:
                    kwargs = {
                        "horizon_type": horizon_type,
                        "code": code,
                        "skip": skip,
                        "limit": batch_size,
                    }
                    if horizon_value is not None:
                        kwargs["horizon_value"] = horizon_value
                    df_batch = client.read_long_term_forecasts(**kwargs)
                    if df_batch is None or df_batch.empty:
                        break
                    frames.append(df_batch)
                    if len(df_batch) < batch_size:
                        break
                    skip += batch_size
            if not frames:
                return None
            df = pd.concat(frames, ignore_index=True)
        else:
            all_records = []
            skip = 0
            while True:
                kwargs = {
                    "horizon_type": horizon_type,
                    "skip": skip,
                    "limit": batch_size,
                }
                if horizon_value is not None:
                    kwargs["horizon_value"] = horizon_value
                df_batch = client.read_long_term_forecasts(**kwargs)
                if df_batch is None or df_batch.empty:
                    break
                all_records.append(df_batch)
                if len(df_batch) < batch_size:
                    break
                skip += batch_size

            if not all_records:
                return None

            df = pd.concat(all_records, ignore_index=True)

        return _normalize_combined_forecasts(df, horizon_type)

    except Exception as e:
        logger.error(
            "Failed to read %s combined forecasts from API: %s",
            horizon_type,
            e,
        )
        return None


def _normalize_combined_forecasts(
    df: pd.DataFrame,
    horizon_type: str,
) -> pd.DataFrame:
    """Normalize API combined forecast response for quarter/season.

    Extracts year/quarter/season from valid_from, renames model_type
    to model_short, adds derived columns.
    """
    from src.aggregation import MONTH_TO_QUARTER, get_season_year

    df = df.copy()

    # Parse valid_from for year extraction
    df["valid_from"] = pd.to_datetime(df["valid_from"])

    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)

    if horizon_type == "quarter":
        df["year"] = df["valid_from"].dt.year
        month = df["valid_from"].dt.month
        df["quarter_in_year"] = month.map(MONTH_TO_QUARTER)
    elif horizon_type == "season":
        df["season_year"] = df.apply(
            lambda r: get_season_year(r["valid_from"].year, r["valid_from"].month),
            axis=1,
        )
        if "horizon_value" in df.columns:
            lead = pd.to_numeric(df["horizon_value"], errors="coerce")
            df["season_in_year"] = lead.astype("Int64") if lead.isna().any() else lead.astype(int)
        else:
            df["season_in_year"] = 1
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Add forecasted_discharge from q/q50
    if "forecasted_discharge" not in df.columns:
        if "q" in df.columns:
            df["forecasted_discharge"] = pd.to_numeric(df["q"], errors="coerce")
        elif "q50" in df.columns:
            df["forecasted_discharge"] = df["q50"].astype(float)

    # Drop API-only columns
    drop_cols = [
        "id",
        "horizon_type",
        "model_type_description",
    ]
    if horizon_type != "season":
        drop_cols.append("horizon_value")
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    return df


def _deduplicate_seasonal_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate seasonal issue/model rows without folding leads."""
    if df.empty:
        return df

    dedup_cols = ["code", "season_year", "season_in_year", "date", "model_short"]
    available = [c for c in dedup_cols if c in df.columns]
    if len(available) < 4:
        return df
    return df.drop_duplicates(subset=available, keep="last")
