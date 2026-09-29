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
    LongTermHorizonResolverError,
    OperationalSchedule,
    operational_schedule_for_mode,
    quarter_horizon_value,
    supported_long_term_modes,
)
from skill_lead_aware_flag import skill_lead_aware_enabled
from src.model_names import (
    AGGREGATED_ENSEMBLE_MODELS,
    AGGREGATED_SUPPORTED_MODELS,
    QUARTER_NATIVE_RAW_MODELS,
    QUARTER_SUPPORTED_MODELS,
    QUARTERLY_DERIVED_MODELS,
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


def _quarterly_fc_output_cols() -> list[str]:
    """Return the canonical quarterly forecast output columns.

    Extends the base column list with "horizon_value" and "date" under
    BOTH SAPPHIRE_SKILL_LEAD_AWARE states (PP-065 P1b): flag ON needs
    them for the per-lead selection made by select_operational_issuances()
    to survive into the final output; flag OFF needs them so the PP-065
    derived rows' own `date`/`horizon_value` survive too (columns not
    present in the frame are filtered out by callers anyway, and
    `horizon_value` may be null on flag-OFF direct rows -- no flag-OFF
    consumer keys on it).
    """
    return _QUARTERLY_FC_COLS + ["horizon_value", "date"]


def _filter_supported_aggregated_forecast_models(df: pd.DataFrame) -> pd.DataFrame:
    """Keep supported quarter/season raw models plus existing ensemble rows."""
    if df.empty or "model_short" not in df.columns:
        return df

    model_keys = canonical_model_short_series(df["model_short"])
    return df[model_keys.isin(AGGREGATED_SUPPORTED_MODELS)].copy()


def _filter_supported_quarter_models(df: pd.DataFrame) -> pd.DataFrame:
    """Keep supported QUARTER models: the two native LR raw models, the

    seven PP-065 derived models, and existing ensemble aggregate rows.

    Quarter-only counterpart to `_filter_supported_aggregated_forecast_models`
    (used post-combine by `read_quarterly_forecasts` and
    `read_latest_quarterly_forecasts` ONLY) -- the season readers
    (`read_seasonal_forecasts`, `read_latest_seasonal_forecasts`) keep
    calling `_filter_supported_aggregated_forecast_models` with
    AGGREGATED_SUPPORTED_MODELS, unchanged.
    """
    if df.empty or "model_short" not in df.columns:
        return df

    model_keys = canonical_model_short_series(df["model_short"])
    return df[model_keys.isin(QUARTER_SUPPORTED_MODELS)].copy()


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
        if lead_aware and not month_schedules:
            logger.warning(
                "SAPPHIRE_SKILL_LEAD_AWARE is enabled but no operational month "
                "schedules are configured (check "
                "ieasyhydroforecast_ml_long_term_supported_modes); returning no "
                "operational forecasts."
            )
            return empty
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

    # Under SAPPHIRE_SKILL_LEAD_AWARE (default OFF), reduce raw model rows
    # to one operational-issuance row per (code, model, target year, target
    # month) BEFORE the latest-(year, month) filter -- read WITHOUT the
    # horizon_value filter (read-then-derive-then-filter), with the issue-
    # date read window expanded backward by the max configured monthly lead
    # so a latest-target month whose operational issuance was made in a
    # prior calendar year is not missed, then trim the SELECTED rows back to
    # [start_year, end_year]. Mirrors read_monthly_forecasts /
    # read_latest_quarterly_forecasts. Flag OFF keeps the pre-existing
    # unfiltered read + no selection unchanged.
    lead_aware = skill_lead_aware_enabled()
    month_schedules: dict[str, OperationalSchedule] | None = None
    read_start_year = start_year
    if lead_aware:
        # Fail LOUD under flag-ON (no silent fallback to an unfiltered read).
        month_schedules = _operational_schedules_for_horizon_type("month")
        if lead_aware and not month_schedules:
            logger.warning(
                "SAPPHIRE_SKILL_LEAD_AWARE is enabled but no operational month "
                "schedules are configured (check "
                "ieasyhydroforecast_ml_long_term_supported_modes); returning no "
                "operational forecasts."
            )
            return pd.DataFrame()
        max_lead = max((s.lead_time for s in month_schedules.values()), default=0)
        read_start_year = start_year - _read_window_expansion_years(max_lead)

    raw = _read_long_forecasts_api(codes, read_start_year, end_year)
    if raw is None or raw.empty:
        logger.warning("No recent monthly forecast data available")
        return pd.DataFrame()

    df = _normalize_monthly_forecasts(raw)
    if df.empty:
        return df

    if lead_aware and month_schedules:
        df = select_operational_issuances(
            df, month_schedules, target_year_col="year", target_period_col="month"
        )
        df = _trim_to_target_year_range(df, "year", start_year, end_year)
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


def _quarter_native_q1_issue_date(
    start_year: int,
    *,
    schedule: OperationalSchedule | None = None,
) -> pd.Timestamp | None:
    """Return the schedule-dated Q1-of-`start_year` issue date, or None.

    Used by `read_quarterly_forecasts`' flag-OFF exception (Problem 7) to
    restrict the "December-issued Q1 of start_year" admit to the ONE
    native, schedule-dated issuance -- never a persisted monthly-derived
    Q1 row that also happens to fall in the pre-start_year widened read
    window (see the dev-DB regression this guards: a Dec-1
    monthly-derived Q1 row winning over the genuine Dec-25 issuance in
    the later drop_duplicates(keep="last") combine).

    The schedule issue date is `valid_from` (Jan 1 of `start_year`)
    minus the quarter mode's configured `lead_time` months, on
    `issue_day`, clamped to the length of that month -- the same rule
    the producer (`long_term_forecasting/lt_utils.py`'s
    `nearest_scheduled_issue_date`) and the dashboard use.

    Args:
        start_year: The requested read window's first year.
        schedule: PP-065 P1b -- an already-resolved quarter operational
            schedule to use instead of resolving one internally (shared
            with the native-row rule and the quarterly derivation calls,
            so a degraded/invalid schedule logs exactly one
            schedule-resolution WARNING per reader call, not one per call
            site). When omitted (the default), behaviour is unchanged:
            this function resolves and validates the schedule itself, as
            it always has.

    Returns:
        A normalized (midnight) `pd.Timestamp` for the native Q1 issue
        date, or None if the quarter operational schedule cannot be
        resolved (config missing/invalid) or has an invalid
        `issue_day` (< 1) -- in which case a single WARNING is logged
        and callers must treat the Problem-7 exception as unavailable.
        When `schedule` is passed explicitly, it is trusted as already
        resolved and validated -- no warning is logged from here.
    """
    if schedule is None:
        try:
            schedule = operational_schedule_for_mode("quarter")
        except (LongTermHorizonResolverError, FileNotFoundError) as exc:
            logger.warning(
                "Could not resolve the quarter operational schedule needed for "
                "the flag-OFF December-issued-Q1-of-start_year exception (%s); "
                "every quarterly direct row issued before the requested year "
                "range will be dropped for start_year=%d.",
                exc,
                start_year,
            )
            return None

        if schedule.issue_day < 1:
            logger.warning(
                "Quarter operational schedule has an invalid issue_day=%d; the "
                "flag-OFF December-issued-Q1-of-start_year exception is "
                "disabled for start_year=%d.",
                schedule.issue_day,
                start_year,
            )
            return None

    # 0-based month index (Jan of year Y == Y*12) for valid_from (Jan 1 of
    # start_year) minus lead_time whole months.
    total_month_index = start_year * 12 - schedule.lead_time
    issue_year = total_month_index // 12
    issue_month = total_month_index % 12 + 1
    max_day = calendar.monthrange(issue_year, issue_month)[1]
    issue_day = min(schedule.issue_day, max_day)
    return pd.Timestamp(issue_year, issue_month, issue_day)


def _resolve_quarter_native_schedule(
    *,
    lead_aware: bool,
    quarter_schedules: dict[str, OperationalSchedule] | None,
) -> OperationalSchedule | None:
    """Resolve the SINGLE quarter operational schedule shared by the

    native-row rule, the quarterly derivation calls, and (flag OFF)
    `_quarter_native_q1_issue_date`'s own Problem-7 exception (PP-065
    P1b "Schedule resolution, flag OFF" -- exactly one
    schedule-resolution WARNING per reader call, on top of, and
    independent from, PP-064's own missing-column "filter skipped"
    guard).

    Flag ON: the schedule is already resolved via `quarter_schedules`
    (`_operational_schedules_for_horizon_type("quarter")`, guaranteed to
    contain the key "quarter" whenever `quarter_schedules` is
    non-empty) -- reused as-is. No new resolution attempt, no new
    warning path: flag-ON behaviour is unchanged by this function.

    Flag OFF: resolves `operational_schedule_for_mode("quarter")` once.
    `LongTermHorizonResolverError` (a superclass of
    `UnsupportedLongTermModeError`) and a resolved schedule with
    `issue_day < 1` are both degraded-mode triggers here; each logs
    exactly one WARNING, with a distinct substring
    ("quarter operational schedule" / "invalid issue_day"), and returns
    None -- callers must then skip the native-row filter AND the
    quarterly derivation entirely (mirrors FD-029's degraded mode), and
    must NOT call `_quarter_native_q1_issue_date` at all (passing
    `schedule=None` back to it would re-resolve and log a second
    warning). `FileNotFoundError` (a missing config FILE) is
    deliberately NOT caught here -- it propagates, per the "Missing
    quarter config FILE" rule: a missing file is a misconfiguration that
    fails the run, unlike a resolvable-but-incomplete config.

    Args:
        lead_aware: `skill_lead_aware_enabled()`, decides which branch
            runs.
        quarter_schedules: The reader's already-resolved flag-ON
            schedule dict (`_operational_schedules_for_horizon_type
            ("quarter")`), or None under flag OFF.

    Returns:
        The resolved schedule, or None if it could not be resolved
        (degraded mode).
    """
    if lead_aware:
        return quarter_schedules["quarter"] if quarter_schedules else None

    try:
        schedule = operational_schedule_for_mode("quarter")
    except LongTermHorizonResolverError as exc:
        logger.warning(
            "Could not resolve the quarter operational schedule for "
            "native-row selection and quarterly derivation (%s); "
            "quarterly direct LR rows are returned unfiltered and no "
            "derived quarterly rows are produced.",
            exc,
        )
        return None

    if schedule.issue_day < 1:
        logger.warning(
            "Quarter operational schedule has an invalid issue_day=%d; "
            "quarterly direct LR rows are returned unfiltered and no "
            "derived quarterly rows are produced.",
            schedule.issue_day,
        )
        return None

    return schedule


def _rename_monthly_raw_for_quarter_derivation(df: pd.DataFrame) -> pd.DataFrame:
    """Rename/normalize raw monthly API rows for

    `derive_quarterly_from_monthly_same_issue` (PP-065 P1b), WITHOUT the
    side effects of `_normalize_monthly_forecasts` (which fills a null
    `horizon_value` with 0 and parses `valid_from` without
    `format="mixed"`).

    Renames `model_type` -> `model_short` and normalizes `code` exactly
    as the other readers do (see `_normalize_monthly_forecasts`,
    `_normalize_combined_forecasts`); does nothing else. `date`,
    `valid_from`, `horizon_value`, `q`, `q50` are passed through
    unparsed/untouched -- the derivation helper parses/validates them
    itself.
    """
    df = df.copy()
    if "model_type" in df.columns:
        df = df.rename(columns={"model_type": "model_short"})
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True)
    return df


def _select_native_quarter_lr_rows(
    direct: pd.DataFrame,
    schedule: OperationalSchedule | None,
) -> pd.DataFrame:
    """PP-065 P1b native-row selection, shared by both quarter readers

    under both `SAPPHIRE_SKILL_LEAD_AWARE` states (decision
    R4-native-lr-precedence: the native rule wins over PP-064's broader
    flag-OFF "trunk set" for LR direct rows).

    Applies PP-064's Contract rule -- `date.day` == the quarter
    schedule's `issue_day` (clamped to the length of the issue month via
    `clamp_issue_days`, the same clamp the producer applies) AND
    year-aware lead (`valid_from` minus `date`, in months) ==
    `schedule.lead_time` -- to LR rows ONLY (`QUARTER_NATIVE_RAW_MODELS`).
    Non-LR rows (the seven derived models, EM/Naive/Skilled Mean) pass
    through unaffected here; the seven models are dropped by a separate,
    unconditional step elsewhere.

    A non-native LR row (a rewrite, a persisted monthly-derived row, a
    backfill) is dropped and counted (INFO). An LR row with a null or
    unparseable `date`, or a `direct` frame with no `date` column at
    all, is dropped and counted the same way -- never raises.

    Degraded mode (`schedule is None`, e.g. the quarter config has no
    `operational_issue_day`): the native-row filter is disabled
    entirely -- `direct` is returned UNCHANGED, mirroring FD-029's
    degraded-mode precedent.

    Args:
        direct: Normalized direct quarterly forecast rows (post
            `_normalize_combined_forecasts`), `date` unparsed.
        schedule: The reader's shared, already-resolved quarter
            operational schedule (`_resolve_quarter_native_schedule`),
            or None if it could not be resolved (degraded mode).

    Returns:
        `direct` with non-native LR rows removed; unchanged if
        `schedule` is None, `direct` is empty, or `direct` has no
        `model_short` column.
    """
    if schedule is None or direct.empty or "model_short" not in direct.columns:
        return direct

    from src.aggregation import clamp_issue_days, local_calendar_date

    canon_model = canonical_model_short_series(direct["model_short"])
    is_lr = canon_model.isin(QUARTER_NATIVE_RAW_MODELS)
    if not is_lr.any():
        return direct

    if "date" in direct.columns:
        issue_date = local_calendar_date(direct["date"])
    else:
        issue_date = pd.Series(pd.NaT, index=direct.index, dtype="datetime64[ns]")

    if "valid_from" in direct.columns:
        valid_from = local_calendar_date(direct["valid_from"])
    else:
        valid_from = pd.Series(pd.NaT, index=direct.index, dtype="datetime64[ns]")

    lead_months = (valid_from.dt.year - issue_date.dt.year) * 12 + (
        valid_from.dt.month - issue_date.dt.month
    )
    expected_day = clamp_issue_days(issue_date, schedule.issue_day)
    is_native = (
        issue_date.notna()
        & valid_from.notna()
        & (issue_date.dt.day == expected_day)
        & (lead_months == schedule.lead_time)
    )

    drop_mask = is_lr & ~is_native
    n_dropped = int(drop_mask.sum())
    if n_dropped:
        logger.info(
            "Dropped %d quarterly direct LR forecast row(s) that are not "
            "a native scheduled issuance (rewrite, persisted "
            "monthly-derived, or backfill row)",
            n_dropped,
        )
    return direct[~drop_mask].copy()


def _drop_stored_lead_mismatches(direct: pd.DataFrame) -> pd.DataFrame:
    """PP-065 P1b "Stored leads (flag ON)": drop and count direct rows

    whose stored `horizon_value` differs from the lead derived from
    (`valid_from` - `date`), BEFORE `select_operational_issuances` runs.
    Call `select_operational_issuances` with `lead_output_cols=()`
    afterward so it does not need to (re)write `horizon_value` -- every
    row reaching it here already carries a self-consistent stored lead.

    A row with a missing/unparseable `date`/`valid_from`, or a missing
    `horizon_value` column, is never counted as a mismatch (nothing to
    compare); `select_operational_issuances` and the native-row rule
    handle those cases separately.
    """
    if direct.empty or "horizon_value" not in direct.columns:
        return direct

    # A `date` or `valid_from` column entirely absent (e.g. an all-null
    # column already dropped upstream by _read_long_forecasts_api) must
    # never raise here (out-of-loop review finding:
    # pd.to_datetime(None, errors="coerce") returns a bare `None`, not an
    # empty/NaT Series, and `.dt` on that raises AttributeError) -- treat
    # it the same as a column full of unparseable/null values.
    if "date" in direct.columns:
        date_parsed = pd.to_datetime(direct["date"], errors="coerce")
    else:
        date_parsed = pd.Series(pd.NaT, index=direct.index)
    if "valid_from" in direct.columns:
        valid_from_parsed = pd.to_datetime(direct["valid_from"], errors="coerce")
    else:
        valid_from_parsed = pd.Series(pd.NaT, index=direct.index)
    derived_lead = (valid_from_parsed.dt.year - date_parsed.dt.year) * 12 + (
        valid_from_parsed.dt.month - date_parsed.dt.month
    )
    stored_hv = pd.to_numeric(direct["horizon_value"], errors="coerce")
    mismatch = stored_hv.notna() & derived_lead.notna() & (stored_hv != derived_lead)
    n_mismatch = int(mismatch.sum())
    if n_mismatch:
        logger.info(
            "Dropped %d quarterly direct forecast row(s) whose stored "
            "horizon_value does not match the lead derived from "
            "(valid_from - date)",
            n_mismatch,
        )
    return direct[~mismatch].copy()


def _drop_direct_quarterly_derived_model_rows(direct: pd.DataFrame) -> pd.DataFrame:
    """PP-065 P1b: unconditionally drop direct rows of the seven

    `QUARTERLY_DERIVED_MODELS` before the direct and derived sources are
    combined -- after this, the readers' `keep="last"` dedup has no LR
    or derived-model collision left to resolve. Applies regardless of
    native-ness or `SAPPHIRE_SKILL_LEAD_AWARE`; also drops legacy
    "Dataset B" rows (persisted QUARTER rows of the seven models at hv
    1-4 with `date == valid_from`), which predate this derivation
    mechanism.
    """
    if direct.empty or "model_short" not in direct.columns:
        return direct
    canon_model = canonical_model_short_series(direct["model_short"])
    return direct[~canon_model.isin(QUARTERLY_DERIVED_MODELS)].copy()


def _suppress_lr_fallback_covered_by_direct(
    derived_lr: pd.DataFrame,
    direct: pd.DataFrame,
) -> pd.DataFrame:
    """PP-065 P1b decision G: drop a decision-G LR fallback row for any

    (code, model, year, quarter) key already covered by a selected
    direct LR row -- native beats fallback (decision
    R4-native-lr-precedence).

    `direct` here is the reader's OWN final, fully-processed direct
    frame (post native-row selection and, under flag ON, post
    `select_operational_issuances`) -- the same rows about to be
    combined with the derived sources.
    """
    if derived_lr.empty or direct.empty:
        return derived_lr
    required = {"code", "model_short", "year", "quarter_in_year"}
    if not required.issubset(direct.columns):
        return derived_lr

    direct_canon = canonical_model_short_series(direct["model_short"])
    direct_lr_mask = direct_canon.isin(QUARTER_NATIVE_RAW_MODELS)
    if not direct_lr_mask.any():
        return derived_lr

    direct_lr = direct.loc[direct_lr_mask]
    covered_keys = set(
        zip(
            direct_lr["code"].astype(str),
            direct_canon.loc[direct_lr_mask],
            pd.to_numeric(direct_lr["year"], errors="coerce"),
            pd.to_numeric(direct_lr["quarter_in_year"], errors="coerce"),
            strict=True,
        )
    )
    if not covered_keys:
        return derived_lr

    derived_canon = canonical_model_short_series(derived_lr["model_short"])
    derived_keys = list(
        zip(
            derived_lr["code"].astype(str),
            derived_canon,
            pd.to_numeric(derived_lr["year"], errors="coerce"),
            pd.to_numeric(derived_lr["quarter_in_year"], errors="coerce"),
            strict=True,
        )
    )
    keep_mask = pd.Series([key not in covered_keys for key in derived_keys], index=derived_lr.index)
    return derived_lr.loc[keep_mask].copy()


def _derive_quarterly_rows(
    codes: list[str],
    issue_start_year: int,
    issue_end_year: int,
    schedule: OperationalSchedule,
    *,
    forecast_date: dt.date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """PP-065 P1b "Derived rows": read raw monthly rows and derive

    quarterly forecasts for the seven `QUARTERLY_DERIVED_MODELS`
    (unconditional) and, as the decision-G fallback source, for
    `QUARTER_NATIVE_RAW_MODELS` (LR) -- the caller suppresses the
    fallback for any key already covered by a native direct row
    (`_suppress_lr_fallback_covered_by_direct`).

    Reads via `_read_long_forecasts_api(..., horizon_type="month")` --
    NOT `read_monthly_forecasts` -- for issue years
    `[issue_start_year, issue_end_year]`. In the latest reader,
    `forecast_date` additionally bounds the monthly rows fed into
    derivation (a null/unparseable issue date is kept, unaffected by the
    bound, mirroring the direct-source Problem-6 bound).

    `derive_quarterly_from_monthly_same_issue` logs its own exclusion
    counts internally -- this function does not re-log them.

    Returns:
        (derived rows for the seven models, derived LR fallback rows) --
        both untrimmed by target year; the caller trims.
    """
    from src.aggregation import derive_quarterly_from_monthly_same_issue, local_calendar_date

    empty = pd.DataFrame()
    raw_monthly = _read_long_forecasts_api(
        codes, issue_start_year, issue_end_year, horizon_type="month"
    )
    if raw_monthly is None or raw_monthly.empty:
        return empty, empty

    renamed = _rename_monthly_raw_for_quarter_derivation(raw_monthly)

    if forecast_date is not None and "date" in renamed.columns:
        issue_date = local_calendar_date(renamed["date"])
        keep_mask = issue_date.isna() | (issue_date.dt.normalize() <= pd.Timestamp(forecast_date))
        dropped = int((~keep_mask).sum())
        renamed = renamed[keep_mask].copy()
        if dropped:
            logger.info(
                "Dropped %d monthly forecast row(s) dated after "
                "forecast_date before quarterly derivation",
                dropped,
            )

    if renamed.empty:
        return empty, empty

    derived_seven, _ = derive_quarterly_from_monthly_same_issue(
        renamed, schedule.lead_time, schedule.issue_day, QUARTERLY_DERIVED_MODELS
    )
    derived_lr, _ = derive_quarterly_from_monthly_same_issue(
        renamed, schedule.lead_time, schedule.issue_day, QUARTER_NATIVE_RAW_MODELS
    )
    return derived_seven, derived_lr


def read_quarterly_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read quarterly forecasts from derived monthly and direct API sources.

    Combines three sources (PP-065 P1b):
    1. The seven `QUARTERLY_DERIVED_MODELS`, derived from same-issue
       monthly triplets (`derive_quarterly_from_monthly_same_issue`).
    2. A decision-G LR (`QUARTER_NATIVE_RAW_MODELS`) fallback, derived
       the same way, for any (code, model, year, quarter) key with no
       selected native direct LR row.
    3. Direct quarterly forecasts read from the API
       (``horizon_type="quarter"``), restricted to native scheduled
       issuances for LR rows (decision R4-native-lr-precedence) and with
       the seven derived models' own direct rows dropped unconditionally.

    A native direct LR row always wins over the decision-G fallback for
    its own key.

    Args:
        codes: Station codes to read.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        DataFrame with columns: [code, year, quarter_in_year,
        model_short, q05-q95, forecasted_discharge, valid_from,
        valid_to, horizon_value, date].
    """
    from src.aggregation import local_calendar_date

    # PP-065 P1b: every early-return empty frame carries the full output
    # schema (including horizon_value/date, out-of-loop review finding),
    # not just the four base columns -- so a caller never sees the output
    # schema change shape depending on whether any row happened to survive.
    empty_cols = _quarterly_fc_output_cols()

    # Direct quarterly forecasts from API.
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
        if lead_aware and not quarter_schedules:
            logger.warning(
                "SAPPHIRE_SKILL_LEAD_AWARE is enabled but no operational quarter "
                "schedules are configured (check "
                "ieasyhydroforecast_ml_long_term_supported_modes); returning no "
                "operational forecasts."
            )
            return pd.DataFrame(columns=empty_cols)
        max_lead = max((s.lead_time for s in quarter_schedules.values()), default=0)
        q_start_year = start_year - _read_window_expansion_years(max_lead)

    # PP-065 P1b: resolve the SINGLE shared quarter schedule once, used by
    # the native-row rule, the derivation calls below, and (flag OFF)
    # _quarter_native_q1_issue_date's own Problem-7 exception. None means
    # degraded mode: no native filter, no derivation.
    single_schedule = _resolve_quarter_native_schedule(
        lead_aware=lead_aware, quarter_schedules=quarter_schedules
    )

    if lead_aware and quarter_schedules:
        raw_q = _read_long_forecasts_api(
            codes,
            q_start_year,
            end_year,
            horizon_type="quarter",
        )
    else:
        # Problem 7: read issue years from start_year - 1 (rather than
        # start_year) so a December-issued Q1 of the first requested
        # year is read; the horizon_value filter is unchanged. The extra
        # rows this widening admits are filtered below, not by a
        # two-sided target-year trim: the actual invariant is trunk's
        # set (every row issued in [start_year, end_year], any target
        # year) PLUS ONLY the December-issued Q1 of start_year -- see
        # TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim,
        # TestRegressionDirectPrecedenceSurvivesLowerBoundWidening and
        # TestRegressionIssueYearMaskTooPermissive in
        # tests/test_quarter_calendar_window.py.
        raw_q = _read_long_forecasts_api(
            codes,
            start_year - 1,
            end_year,
            horizon_type="quarter",
            horizon_value=quarter_horizon_value(),
        )
    # PP-065 P1b: the LR rows that PASSED native-row selection, captured
    # BEFORE any later step (stored-leads pre-filter, select_operational_
    # issuances) can drop one for a reason unrelated to nativity itself.
    # Used ONLY to decide which decision-G fallback keys to suppress below
    # -- out-of-loop review finding, round 2: select_operational_issuances
    # still matches the UNCLAMPED issue day (PP-066's own gap, not
    # modified here), so a clamped-day-valid native row can be dropped
    # from the FINAL `direct` under flag ON for a reason that has nothing
    # to do with whether it is native. Basing suppression on the final
    # `direct` in that case would let the fallback silently substitute a
    # different, wrong value in place of what should simply be "no row"
    # for that key (matching this same gap's existing behaviour with the
    # fallback mechanism absent) -- never a value native-row selection
    # itself disagrees with.
    native_direct_for_suppression = pd.DataFrame()
    if raw_q is not None and not raw_q.empty:
        direct = _normalize_combined_forecasts(raw_q, "quarter")
        # PP-065 P1b: drop direct rows of the seven derived-eligible
        # models unconditionally, before anything else -- this also
        # drops legacy "Dataset B" rows (see
        # _drop_direct_quarterly_derived_model_rows).
        direct = _drop_direct_quarterly_derived_model_rows(direct)
        if lead_aware and quarter_schedules and not direct.empty:
            # Order (flag ON): native-row helper -> stored-leads
            # pre-filter -> select_operational_issuances.
            direct = _select_native_quarter_lr_rows(direct, single_schedule)
            native_direct_for_suppression = direct
            direct = _drop_stored_lead_mismatches(direct)
            if not direct.empty:
                direct = select_operational_issuances(
                    direct,
                    quarter_schedules,
                    target_year_col="year",
                    target_period_col="quarter_in_year",
                    lead_output_cols=(),
                )
                direct = _trim_to_target_year_range(direct, "year", start_year, end_year)
        elif (
            not lead_aware
            and not direct.empty
            and "year" in direct.columns
            and "quarter_in_year" in direct.columns
            and "date" in direct.columns
        ):
            # Order (flag OFF): PP-064's Problem-7 issue-year mask runs
            # FIRST, then this item's native-row helper (decision
            # R4-native-lr-precedence).
            #
            # Invariant: the flag-OFF direct set = trunk's set (every row
            # with issue year in [start_year, end_year], ANY target year)
            # PLUS ONLY the NATIVE, schedule-dated December-issued Q1 of
            # start_year (Problem 7). Nothing else is added, nothing else
            # is removed. A row issued before start_year is dropped UNLESS
            # it is that exact Q1-of-start_year issuance -- checking target
            # year alone (round-2 fix) was still too permissive: it also
            # kept an out-of-window row targeting some OTHER calendar
            # quarter of start_year (e.g. issued 2024-12-25 targeting Q2
            # 2025), which could then beat a same-target monthly-derived
            # row, or even an in-window direct row, in the
            # drop_duplicates(keep="last") combine below depending on API
            # order (round-3 out-of-loop review). Checking target
            # year+quarter alone (Problem 7's original fix) was ALSO too
            # permissive: a dev-DB read showed it also admits a PERSISTED
            # MONTHLY-DERIVED Q1 row backdated to Dec 1 (valid_from minus
            # horizon_value months) -- not the genuine Dec-25 issuance --
            # which shares the (code, model, year, quarter) dedup key and,
            # carrying a higher API id, can win keep="last" over the real
            # issuance (owner decision: the exception admits ONLY the
            # native issuance, identified by matching the configured
            # quarter operational schedule's issue date exactly). Everything
            # with issue year >= start_year is kept UNCONDITIONALLY
            # regardless of target year (trunk's own set, including
            # backfills like a Q4 start_year-1 row issued in start_year,
            # #521-style). A null/unparseable issue date is kept: trunk's
            # API-side year filter could not have excluded it by year
            # either. A target year > end_year (e.g. a Dec-end_year issue's
            # next-year Q1) also survives unconditionally -- see the
            # next-year-precedence regression test.
            #
            # Once past this mask, decision R4-native-lr-precedence's
            # native-row rule below still governs which LR rows survive
            # at all -- this mask alone no longer describes the final
            # outcome for LR rows.
            target_years = pd.to_numeric(direct["year"], errors="coerce")
            quarters = pd.to_numeric(direct["quarter_in_year"], errors="coerce")
            issue_dates = local_calendar_date(direct["date"])
            issue_years = issue_dates.dt.year
            native_q1_issue_date = (
                _quarter_native_q1_issue_date(start_year, schedule=single_schedule)
                if single_schedule is not None
                else None
            )
            if native_q1_issue_date is None:
                # Schedule unresolvable: no exception -- trunk's set only.
                is_december_q1_of_start_year = pd.Series(False, index=direct.index)
            else:
                is_december_q1_of_start_year = (
                    (target_years == start_year)
                    & (quarters == 1)
                    & (issue_dates == native_q1_issue_date)
                )
            drop_mask = (
                issue_years.notna() & (issue_years < start_year) & ~is_december_q1_of_start_year
            )
            dropped_issue_year_rows = int(drop_mask.sum())
            direct = direct[~drop_mask].copy()
            if dropped_issue_year_rows:
                logger.info(
                    "Dropped %d quarterly direct forecast row(s) issued "
                    "before the requested year range",
                    dropped_issue_year_rows,
                )
            direct = _select_native_quarter_lr_rows(direct, single_schedule)
            native_direct_for_suppression = direct
        elif not lead_aware and not direct.empty:
            # The mask above needs quarter_in_year and date to tell a
            # genuine December-issued Q1 of start_year apart from any
            # other out-of-window row; without them it cannot run at
            # all (year alone was already shown insufficient -- see
            # TestRegressionIssueYearMaskTooPermissive). Surface that
            # rather than silently skipping it. The row(s) still reach
            # the native-row helper below (a missing `date` column is
            # itself one of the reasons that helper drops an LR row).
            missing_cols = sorted({"quarter_in_year", "date"} - set(direct.columns))
            if missing_cols:
                logger.warning(
                    "Flag-OFF quarterly issue-year filter skipped: direct "
                    "rows missing column(s) %s",
                    missing_cols,
                )
            direct = _select_native_quarter_lr_rows(direct, single_schedule)
            native_direct_for_suppression = direct
    else:
        direct = pd.DataFrame()

    # PP-065 P1b: derived rows (both flags), only when the shared schedule
    # resolved. The derivation's own issue-year read window must match
    # THIS reader's own direct-read window for the SAME flag -- q_start_year
    # under flag ON (already widened by the configured lead), start_year - 1
    # under flag OFF (Problem 7's own widening) -- so a native direct row
    # outside the derivation's window can never be mistaken for "absent"
    # by _suppress_lr_fallback_covered_by_direct below (out-of-loop review
    # finding: a mismatched window let an unread-but-present native row's
    # fallback go unsuppressed).
    derivation_start_year = q_start_year if (lead_aware and quarter_schedules) else start_year - 1
    # Skip the derivation entirely (rather than let it run and immediately
    # hit its own internal invalid_config guard, src/aggregation.py) when
    # the schedule is invalid -- out-of-loop review finding: calling it
    # twice (once per model set) with a bad schedule logged the SAME
    # invalid_config WARNING twice, under flag ON, where nothing upstream
    # already validated issue_day/lead_time (flag OFF's shared resolution
    # already rejects both before single_schedule is ever set).
    schedule_valid = (
        single_schedule is not None
        and single_schedule.issue_day >= 1
        and single_schedule.lead_time >= 0
    )
    derived_seven = pd.DataFrame()
    derived_lr_fallback = pd.DataFrame()
    if schedule_valid:
        derived_seven, derived_lr = _derive_quarterly_rows(
            codes, derivation_start_year, end_year, single_schedule
        )
        derived_lr_fallback = _suppress_lr_fallback_covered_by_direct(
            derived_lr, native_direct_for_suppression
        )
        derived_seven = _trim_to_target_year_range(derived_seven, "year", start_year, end_year)
        derived_lr_fallback = _trim_to_target_year_range(
            derived_lr_fallback, "year", start_year, end_year
        )

    # Combine sources: derived rows first, direct last --
    # drop_duplicates(keep="last") prefers direct (though by this point
    # there is no remaining LR/derived-model collision to resolve; see
    # _drop_direct_quarterly_derived_model_rows and
    # _suppress_lr_fallback_covered_by_direct).
    frames = [f for f in (derived_seven, derived_lr_fallback, direct) if not f.empty]
    if not frames:
        return pd.DataFrame(columns=empty_cols)

    if len(frames) == 1:
        combined = frames[0].copy()
    else:
        combined = pd.concat(frames, ignore_index=True)
        dedup_cols = ["code", "year", "quarter_in_year", "model_short"]
        if lead_aware and "horizon_value" in combined.columns:
            dedup_cols = [*dedup_cols, "horizon_value"]
        available = [c for c in dedup_cols if c in combined.columns]
        combined = combined.drop_duplicates(subset=available, keep="last")

    if combined.empty:
        return pd.DataFrame(columns=empty_cols)

    combined = _filter_supported_quarter_models(combined)
    if combined.empty:
        return pd.DataFrame(columns=empty_cols)

    # Select canonical output columns. reindex (not a plain column filter)
    # so horizon_value/date (and any other canonical column) is always
    # PRESENT -- as an all-null column when nothing in this call's result
    # happened to carry it -- rather than silently absent (out-of-loop
    # review finding: a caller keying on result["horizon_value"] must
    # never see the schema change shape depending on the data).
    combined = combined.reindex(columns=_quarterly_fc_output_cols())

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

        if not candidate_schedules:
            logger.warning(
                "SAPPHIRE_SKILL_LEAD_AWARE is enabled but no operational season "
                "schedules are configured for this read (no seasonal modes "
                "configured, or no seasonal mode at the requested lead; check "
                "ieasyhydroforecast_ml_long_term_supported_modes); returning no "
                "operational forecasts."
            )
            return empty

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
    """Read latest quarterly forecasts from derived monthly and direct API.

    Combines three sources (PP-065 P1b) -- see `read_quarterly_forecasts`
    for the full contract, which this mirrors:
    1. The seven `QUARTERLY_DERIVED_MODELS`, derived from same-issue
       monthly triplets issued on or before `forecast_date`.
    2. A decision-G LR fallback, derived the same way, for any (code,
       model, year, quarter) key with no selected native direct LR row.
    3. Direct quarterly forecasts from the API, restricted to native
       scheduled issuances for LR rows, with the seven derived models'
       own direct rows dropped unconditionally.

    A native direct LR row always wins over the decision-G fallback for
    its own key.

    Args:
        codes: Station codes to read.
        forecast_date: Reference date for lookback window.

    Returns:
        DataFrame with quarterly forecasts for the most recent
        quarter. Empty DataFrame if no data.
    """
    from src.aggregation import local_calendar_date

    today = forecast_date if forecast_date is not None else dt.date.today()
    start_date = today - dt.timedelta(days=120)
    start_year = start_date.year
    end_year = today.year

    # Direct quarterly forecasts from API.
    #
    # Under SAPPHIRE_SKILL_LEAD_AWARE (default OFF), read WITHOUT the
    # horizon_value filter (read-then-derive-then-filter), expand the
    # read window backward by the configured quarter lead, and reduce to
    # one operational-issuance row per (code, model, target year, target
    # quarter) -- mirroring read_quarterly_forecasts' direct branch. Flag
    # OFF keeps the pre-existing single-lead API filter unchanged
    # (quarter_horizon_value()).
    lead_aware = skill_lead_aware_enabled()
    quarter_schedules: dict[str, OperationalSchedule] | None = None
    q_start_year = start_year
    if lead_aware:
        # Fail LOUD under flag-ON (no silent fallback to an unfiltered read).
        quarter_schedules = _operational_schedules_for_horizon_type("quarter")
        if lead_aware and not quarter_schedules:
            logger.warning(
                "SAPPHIRE_SKILL_LEAD_AWARE is enabled but no operational quarter "
                "schedules are configured (check "
                "ieasyhydroforecast_ml_long_term_supported_modes); returning no "
                "operational forecasts."
            )
            return pd.DataFrame(columns=_quarterly_fc_output_cols())
        max_lead = max((s.lead_time for s in quarter_schedules.values()), default=0)
        q_start_year = start_year - _read_window_expansion_years(max_lead)

    # PP-065 P1b: resolve the SINGLE shared quarter schedule once, used by
    # the native-row rule and the derivation calls below (this reader has
    # no Problem-7 exception to share it with). None means degraded mode:
    # no native filter, no derivation.
    single_schedule = _resolve_quarter_native_schedule(
        lead_aware=lead_aware, quarter_schedules=quarter_schedules
    )

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
        # Problem 6: under both flags, a direct row issued after
        # forecast_date cannot be an operational issuance for this run
        # (guards the widened target-year trim below against a
        # back-dated run picking a later issue). Rows with a null or
        # unparseable date are kept, unaffected by the bound.
        if not direct.empty and "date" in direct.columns:
            issue_date = local_calendar_date(direct["date"])
            keep_mask = issue_date.isna() | (issue_date.dt.normalize() <= pd.Timestamp(today))
            dropped_future_issue_rows = int((~keep_mask).sum())
            direct = direct[keep_mask].copy()
            if dropped_future_issue_rows:
                logger.info(
                    "Dropped %d quarterly direct forecast row(s) dated after "
                    "forecast_date (back-dated run, or flag-OFF rows dated at "
                    "the quarter start)",
                    dropped_future_issue_rows,
                )
        # PP-065 P1b: drop direct rows of the seven derived-eligible
        # models unconditionally (also drops legacy "Dataset B" rows).
        direct = _drop_direct_quarterly_derived_model_rows(direct)
        # This reader has no Problem-7 mask to order against -- the
        # native-row helper runs right after the forecast_date bound
        # above, under both flags.
        direct = _select_native_quarter_lr_rows(direct, single_schedule)
        # Captured HERE, before select_operational_issuances gets a chance
        # to drop a clamped-day-valid native row for an unrelated reason
        # (out-of-loop review finding, round 2 -- see
        # read_quarterly_forecasts' identical comment for the full
        # rationale: PP-066's own gap must not surface as a wrong
        # fallback value in place of a correctly-classified native row).
        native_direct_for_suppression = direct
        if lead_aware and quarter_schedules and not direct.empty:
            direct = _drop_stored_lead_mismatches(direct)
            if not direct.empty:
                direct = select_operational_issuances(
                    direct,
                    quarter_schedules,
                    target_year_col="year",
                    target_period_col="quarter_in_year",
                    lead_output_cols=(),
                )
                # Problem 6: admit end_year + 1 so a 25 Dec issue's next-year
                # Q1 survives (the date bound above prevents a back-dated
                # run from picking a later issue through this wider bound).
                direct = _trim_to_target_year_range(direct, "year", start_year, end_year + 1)
    else:
        direct = pd.DataFrame()
        native_direct_for_suppression = pd.DataFrame()

    # PP-065 P1b: derived rows (both flags), only when the shared schedule
    # resolved. The derivation's own issue-year read window must match
    # THIS reader's own direct-read window for the SAME flag -- q_start_year
    # under flag ON (already widened by the configured lead), start_year
    # under flag OFF (this reader's direct branch has no Problem-7-style
    # widening at all) -- so a native direct row outside the derivation's
    # window can never be mistaken for "absent" by
    # _suppress_lr_fallback_covered_by_direct below (out-of-loop review
    # finding: a mismatched window let an unread-but-present native row's
    # fallback go unsuppressed, changing output solely as forecast_date
    # crossed a year boundary). Bounded by forecast_date (Problem 6, for
    # free via _derive_quarterly_rows).
    derivation_start_year = q_start_year if (lead_aware and quarter_schedules) else start_year
    # See read_quarterly_forecasts' identical guard for why lead_time is
    # checked here too (out-of-loop review finding, round 2): a negative
    # lead_time hits the same double-WARNING gap as an invalid issue_day.
    schedule_valid = (
        single_schedule is not None
        and single_schedule.issue_day >= 1
        and single_schedule.lead_time >= 0
    )
    derived_seven = pd.DataFrame()
    derived_lr_fallback = pd.DataFrame()
    if schedule_valid:
        derived_seven, derived_lr = _derive_quarterly_rows(
            codes, derivation_start_year, end_year, single_schedule, forecast_date=today
        )
        derived_lr_fallback = _suppress_lr_fallback_covered_by_direct(
            derived_lr, native_direct_for_suppression
        )
        derived_seven = _trim_to_target_year_range(derived_seven, "year", start_year, end_year + 1)
        derived_lr_fallback = _trim_to_target_year_range(
            derived_lr_fallback, "year", start_year, end_year + 1
        )

    # Combine sources: derived rows first, direct last (see
    # read_quarterly_forecasts for why no collision remains by this point).
    frames = [f for f in (derived_seven, derived_lr_fallback, direct) if not f.empty]
    if not frames:
        logger.warning("No quarterly forecast data available")
        return pd.DataFrame(columns=_quarterly_fc_output_cols())

    if len(frames) == 1:
        combined = frames[0].copy()
    else:
        combined = pd.concat(frames, ignore_index=True)
        dedup_cols = ["code", "year", "quarter_in_year", "model_short"]
        if lead_aware and "horizon_value" in combined.columns:
            dedup_cols = [*dedup_cols, "horizon_value"]
        available = [c for c in dedup_cols if c in combined.columns]
        combined = combined.drop_duplicates(subset=available, keep="last")

    if combined.empty:
        return pd.DataFrame(columns=_quarterly_fc_output_cols())

    combined = _filter_supported_quarter_models(combined)
    if combined.empty:
        return pd.DataFrame(columns=_quarterly_fc_output_cols())

    # Select canonical output columns. reindex (not a plain column filter)
    # so horizon_value/date (and any other canonical column) is always
    # PRESENT -- as an all-null column when nothing in this call's result
    # happened to carry it -- rather than silently absent (out-of-loop
    # review finding: a caller keying on result["horizon_value"] must
    # never see the schema change shape depending on the data).
    combined = combined.reindex(columns=_quarterly_fc_output_cols())

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

    # Under SAPPHIRE_SKILL_LEAD_AWARE (default OFF), reduce raw model rows
    # to one operational-issuance row per (code, model, target season_year)
    # BEFORE the latest-season filter -- read WITHOUT the horizon_value
    # filter (read-then-derive-then-filter), with the issue-date read
    # window expanded backward by the configured seasonal lead(s). When
    # horizon_value is given, selection is restricted to the schedule(s)
    # matching that lead so a caller's per-lead loop still yields one frame
    # per configured seasonal lead. Mirrors read_seasonal_forecasts. Flag
    # OFF is byte-identical to the pre-existing single-horizon_value read.
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

        if not candidate_schedules:
            logger.warning(
                "SAPPHIRE_SKILL_LEAD_AWARE is enabled but no operational season "
                "schedules are configured for this read (no seasonal modes "
                "configured, or no seasonal mode at the requested lead; check "
                "ieasyhydroforecast_ml_long_term_supported_modes); returning no "
                "operational forecasts."
            )
            return pd.DataFrame(columns=_SEASONAL_FC_COLS)

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
        logger.warning("No recent seasonal forecast data from API")
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    df = _normalize_combined_forecasts(raw, "season")
    if df.empty:
        return pd.DataFrame(columns=_SEASONAL_FC_COLS)

    if lead_aware and season_schedules:
        # season_in_year IS the lead key (one irrigation season/year), so
        # target unit is (code, model, season_year) and the derived lead is
        # written into BOTH horizon_value AND season_in_year.
        df = select_operational_issuances(
            df,
            season_schedules,
            target_year_col="season_year",
            target_period_col=None,
            lead_output_cols=("horizon_value", "season_in_year"),
        )
        df = _trim_to_target_year_range(df, "season_year", start_year, end_year)
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

    PP-065 P1b: drops rows of the seven `QUARTERLY_DERIVED_MODELS` (this
    reader is filter-only — no monthly read, unlike
    `read_quarterly_forecasts`/`read_latest_quarterly_forecasts`, which
    additionally derive replacement rows for those models). This also
    drops legacy "Dataset B" rows (persisted QUARTER rows of the seven
    models at hv 1-4 with `date == valid_from`), which predate the
    derivation mechanism.

    Args:
        codes: Optional list of station codes to filter. When provided,
            only forecasts for those codes are returned. When None,
            all codes are returned.

    Returns:
        DataFrame with combined quarterly forecasts, or empty DataFrame.
    """
    # Under SAPPHIRE_SKILL_LEAD_AWARE, read ALL leads written for quarter
    # (omit the single-lead deployment-config filter) so per-lead gap
    # detection / gap-fill downstream sees every lead. Flag OFF: unchanged
    # single-lead filter.
    lead_aware = skill_lead_aware_enabled()
    df = _read_long_combined_forecasts_api(
        "quarter",
        codes,
        horizon_value=None if lead_aware else quarter_horizon_value(),
    )
    if df is not None and not df.empty:
        logger.info("Read %d quarterly combined forecast rows from API", len(df))
        df = _drop_direct_quarterly_derived_model_rows(df)
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
    from src.aggregation import MONTH_TO_QUARTER, filter_calendar_quarter_windows, get_season_year

    df = df.copy()

    # Calendar-window validation (PP-064 Chunk A): a non-calendar quarter
    # window is excluded here, at the single choke point for every direct
    # quarter read, rather than relabelled. Season is unaffected. This also
    # writes a normalized valid_from back into df, so the parse below
    # cannot raise on mixed date-only / timestamp strings.
    if horizon_type == "quarter":
        df, dropped_calendar_rows = filter_calendar_quarter_windows(df)
        if dropped_calendar_rows:
            logger.info(
                "Dropped %d non-calendar %s forecast window row(s)",
                dropped_calendar_rows,
                horizon_type,
            )
        if df.empty or "valid_from" not in df.columns:
            # The helper can leave zero rows with the valid_from column
            # absent entirely: e.g. valid_from was null for every row of
            # a batch, and _read_long_forecasts_api's upstream
            # dropna(axis=1, how="all") already dropped the all-null
            # column before this function ever saw it (only valid_to
            # present). The parse below would then raise KeyError on a
            # column that no longer exists, aborting callers that call
            # this function directly with no try/except (e.g.
            # read_quarterly_forecasts, unlike
            # _read_long_combined_forecasts_api's try/except). Return
            # the (empty) frame early with the columns downstream
            # expects instead.
            if not df.empty and dropped_calendar_rows == 0:
                # Neither valid_from nor valid_to was present at all --
                # the helper returns such a frame UNCHANGED (0 dropped),
                # so nothing has been logged yet, and every one of these
                # rows is about to be discarded silently otherwise.
                logger.warning(
                    "Dropped %d %s forecast row(s) with neither valid_from nor valid_to present",
                    len(df),
                    horizon_type,
                )
            return pd.DataFrame(
                columns=[
                    *df.columns,
                    *[c for c in ("year", "quarter_in_year") if c not in df.columns],
                ]
            )

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
    # Quarter drops the raw horizon_value (single-lead deployment config
    # historically made it redundant); season always keeps it. Under
    # SAPPHIRE_SKILL_LEAD_AWARE, quarter keeps it too so the per-lead
    # selection made by select_operational_issuances() survives.
    if horizon_type != "season" and not skill_lead_aware_enabled():
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
