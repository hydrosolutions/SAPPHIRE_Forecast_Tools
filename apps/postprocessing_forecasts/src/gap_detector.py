"""Detect missing ensemble forecasts for gap-fill maintenance.

Used by postprocessing_maintenance.py to find (date, code) pairs where
individual model forecasts exist but the ensemble (model_short='EM') is
missing within a lookback window.
"""

import calendar as _calendar
import datetime as _dt
import logging
import math
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from skill_lead_aware_flag import skill_lead_aware_enabled

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

    # Under SAPPHIRE_SKILL_LEAD_AWARE, detect gaps per-lead -- mirrors the
    # quarterly detector's horizon_value key -- instead of conflating
    # distinct horizon_value leads into one (year, month, code) grain, so a
    # missing ensemble at a specific lead is detected even when another lead
    # is present for the same target month. Flag OFF, or horizon_value absent
    # from the input, is unchanged.
    lead_aware = skill_lead_aware_enabled() and (
        not combined_forecasts.empty and "horizon_value" in combined_forecasts.columns
    )

    key_cols = ["year", "month", "code"]
    if lead_aware:
        key_cols = [*key_cols, "horizon_value"]
    cols = [*key_cols, "model_short"]
    empty = pd.DataFrame(columns=cols)

    if combined_forecasts.empty:
        return empty

    required = set(key_cols) | {"model_short"}
    if not required.issubset(combined_forecasts.columns):
        logger.warning(
            "Monthly combined forecasts missing required columns: %s",
            required - set(combined_forecasts.columns),
        )
        return empty

    df = combined_forecasts.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    numeric_cols = ["year", "month"]
    if lead_aware:
        # Legacy rows with a NULL/non-numeric horizon_value cannot be keyed
        # to a real lead. Manufacturing an "unknown-lead" sentinel gap only
        # yields a PHANTOM gap that downstream maintenance -- which
        # regenerates ensembles at REAL leads (0/1/...) -- can never fill,
        # so it would recur every run. Instead, EXCLUDE such rows from the
        # lead-aware gap universe, but emit a WARNING naming the count so
        # they are surfaced (not silently dropped). (Monthly rows normally
        # arrive with horizon_value already coerced to a clean int upstream
        # by _normalize_monthly_forecasts; this mirrors the quarterly
        # detector for parity/robustness.)
        df["horizon_value"] = pd.to_numeric(df["horizon_value"], errors="coerce")
        null_lead_count = int(df["horizon_value"].isna().sum())
        if null_lead_count:
            logger.warning(
                "%d legacy NULL-lead month rows skipped from lead-aware "
                "gap detection -- need migration",
                null_lead_count,
            )
        numeric_cols = [*numeric_cols, "horizon_value"]
    df = df.dropna(subset=numeric_cols)
    for col in numeric_cols:
        df[col] = df[col].astype(int)

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

    # Find all key-grain pairs with any forecasts
    all_pairs = recent[key_cols].drop_duplicates()

    # Check each ensemble model
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
    )[cols]


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

    # Under SAPPHIRE_SKILL_LEAD_AWARE, detect gaps per-lead -- mirrors
    # detect_missing_seasonal_ensembles' unconditional season_in_year key
    # -- instead of conflating distinct horizon_value leads into one
    # (year, quarter, code) grain. Flag OFF, or horizon_value absent from
    # the input, is unchanged.
    lead_aware = skill_lead_aware_enabled() and (
        not combined_forecasts.empty and "horizon_value" in combined_forecasts.columns
    )

    key_cols = ["year", "quarter_in_year", "code"]
    if lead_aware:
        key_cols = [*key_cols, "horizon_value"]
    cols = [*key_cols, "model_short"]
    empty = pd.DataFrame(columns=cols)

    if combined_forecasts.empty:
        return empty

    required = set(key_cols) | {"model_short"}
    if not required.issubset(combined_forecasts.columns):
        logger.warning(
            "Quarterly combined forecasts missing columns: %s",
            required - set(combined_forecasts.columns),
        )
        return empty

    df = combined_forecasts.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter_in_year"] = pd.to_numeric(df["quarter_in_year"], errors="coerce")
    numeric_cols = ["year", "quarter_in_year"]
    if lead_aware:
        # Legacy rows with a NULL/non-numeric horizon_value cannot be keyed
        # to a real lead. Manufacturing an "unknown-lead" sentinel gap only
        # yields a PHANTOM gap that downstream maintenance -- which
        # regenerates ensembles at REAL leads (0/1/...) -- can never fill,
        # so it would recur every run. Instead, EXCLUDE such rows from the
        # lead-aware gap universe, but emit a WARNING naming the count so
        # they are surfaced (not silently dropped): they need migration
        # (FIX 5, revised).
        df["horizon_value"] = pd.to_numeric(df["horizon_value"], errors="coerce")
        null_lead_count = int(df["horizon_value"].isna().sum())
        if null_lead_count:
            logger.warning(
                "%d legacy NULL-lead quarter rows skipped from lead-aware "
                "gap detection -- need migration",
                null_lead_count,
            )
        numeric_cols = [*numeric_cols, "horizon_value"]
    df = df.dropna(subset=numeric_cols)
    for col in numeric_cols:
        df[col] = df[col].astype(int)

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


# ===================================================================
# P1 Recovery: pure period-refresh planner (no API calls, no writes)
# ===================================================================

_PLAN_ALL5_FIELDS: tuple[str, ...] = (
    "forecasted_discharge",
    "q05",
    "q25",
    "q75",
    "q95",
)
_PLAN_WRITABLE_FIELDS: tuple[str, ...] = (
    "forecasted_discharge",
    "q05",
    "q25",
    "q75",
    "q95",
    "flag",
    "composition",
    "horizon_value",
    "horizon_in_year",
)
# LR records are independent source inputs, not PP period recovery outputs.
_PLAN_EXCLUDED_MODEL_TYPES: frozenset[str] = frozenset({"LR"})

# Explicit horizon_type mappings only; no strip/case rescue.
# "decade" is the API surface form; "decad" is the internal canonical form.
_HT_EXPLICIT: dict[str, str] = {"pentad": "pentad", "decad": "decad", "decade": "decad"}

# Explicit supported short-term model alias map.  Keys are known API enum
# surface values (exact case from service schema) or established internal
# canonical forms.  Anything not in this map is rejected without case rescue.
# LR/LINEAR_REGRESSION map to "LR" so _PLAN_EXCLUDED_MODEL_TYPES catches both.
_PLAN_SUPPORTED_MODEL_MAP: dict[str, str] = {
    "TFT": "TFT",
    "TIDE": "TIDE",
    "TiDE": "TIDE",  # API enum surface value (exact case)
    "TSMIXER": "TSMIXER",
    "TSMixer": "TSMIXER",  # API enum surface value (exact case)
    "NE": "NE",
    "NEURAL_ENSEMBLE": "NE",
    "EM": "ENSEMBLE_MEAN",
    "ENSEMBLE_MEAN": "ENSEMBLE_MEAN",
    "LR": "LR",  # accepted for key extraction; excluded as output
    "LINEAR_REGRESSION": "LR",  # canonical "LR"; caught by _PLAN_EXCLUDED_MODEL_TYPES
}

# Field-type categories for type-aware writable comparison.
_PLAN_INT_META_FIELDS: frozenset[str] = frozenset({"flag", "horizon_value", "horizon_in_year"})
_PLAN_STR_META_FIELDS: frozenset[str] = frozenset({"composition"})


def _plan_is_valid_boundary(d: _dt.date, horizon_type: str) -> bool:
    last_day = _calendar.monthrange(d.year, d.month)[1]
    if horizon_type == "pentad":
        return d.day in (5, 10, 15, 20, 25, last_day)
    if horizon_type == "decad":
        return d.day in (10, 20, last_day)
    return False


def _plan_key(payload: dict[str, Any]) -> tuple | None:
    """Extract the normalized 5-part identity key from a payload dict.

    Strict: rejects any input not in the explicit _HT_EXPLICIT or
    _PLAN_SUPPORTED_MODEL_MAP tables.  Returns None if any key field is
    missing, wrong type, has surrounding whitespace, requires case rescue,
    or contains a non-canonical date value.
    """
    try:
        ht_raw = payload["horizon_type"]
        code_raw = payload["code"]
        model_raw = payload["model_type"]
        issue_raw = payload["date"]
        target_raw = payload["target"]
    except (KeyError, TypeError):
        return None

    # horizon_type: exact lookup only; no strip/lower rescue
    if not isinstance(ht_raw, str):
        return None
    ht = _HT_EXPLICIT.get(ht_raw)
    if ht is None:
        return None

    # code: must be a non-empty str with no surrounding whitespace (exact)
    if not isinstance(code_raw, str) or not code_raw or code_raw != code_raw.strip():
        return None
    code = code_raw

    # model_type: must be in the explicit supported model alias map; no general
    # strip/case rescue applied.  TiDE and TSMixer (API surface forms) are
    # accepted alongside the internal TIDE/TSMIXER/NE/EM canonical keys.
    if not isinstance(model_raw, str):
        return None
    model_type = _PLAN_SUPPORTED_MODEL_MAP.get(model_raw)
    if model_type is None:
        return None

    def _to_date(v: Any) -> _dt.date | None:
        # pd.Timestamp is a subclass of datetime; check it first.
        if isinstance(v, pd.Timestamp):
            if v.tzinfo is not None:
                return None
            if v.hour != 0 or v.minute != 0 or v.second != 0 or v.microsecond != 0:
                return None
            if v.nanosecond != 0:
                return None
            return v.date()
        if isinstance(v, _dt.datetime):
            if v.tzinfo is not None:
                return None
            if v.hour != 0 or v.minute != 0 or v.second != 0 or v.microsecond != 0:
                return None
            return v.date()
        if isinstance(v, _dt.date):
            return v
        if isinstance(v, str):
            # Must be exactly 10 chars YYYY-MM-DD; no truncation of longer strings.
            # Roundtrip guard rejects ISO week-dates like '2026-W41-1' which are
            # also 10 chars and accepted by fromisoformat in Python 3.11+ but
            # whose .isoformat() returns the calendar form, not the original string.
            if len(v) != 10:
                return None
            try:
                parsed = _dt.date.fromisoformat(v)
            except ValueError:
                return None
            if parsed.isoformat() != v:
                return None
            return parsed
        return None

    issue_date = _to_date(issue_raw)
    target_date = _to_date(target_raw)
    if issue_date is None or target_date is None:
        return None
    return (ht, code, model_type, issue_date, target_date)


def _plan_all5_finite(payload: dict[str, Any]) -> bool:
    """Return True iff all 5 required numeric fields are present, real-numeric, and finite.

    Booleans and non-numeric types (str, etc.) are rejected; no coercion applied.
    """
    for f in _PLAN_ALL5_FIELDS:
        v = payload.get(f)
        if v is None:
            return False
        if isinstance(v, bool):
            return False
        if not isinstance(v, (int, float)):
            return False
        if not math.isfinite(v):
            return False
    return True


def _plan_existing_all5_valid(payload: dict[str, Any]) -> bool:
    """Return True iff ALL5 fields are each real numeric or None in an existing record.

    None is permitted (existing rows may predate quantile propagation).
    Bool values, non-numeric types, and non-finite values are rejected.
    """
    for f in _PLAN_ALL5_FIELDS:
        v = payload.get(f)
        if v is None:
            continue
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return False
        if not math.isfinite(v):
            return False
    return True


def _plan_valid_meta_fields(payload: dict[str, Any], key: tuple) -> bool:
    """Return True iff writable metadata fields pass type and calendar checks.

    All _PLAN_WRITABLE_FIELDS keys must be present in payload (value may be
    None for composition/flag/ALL5; horizon_value and horizon_in_year must be
    non-null).  Missing keys are rejected regardless of caller context.
    composition: None or str (no bool/numeric).
    flag: None or non-bool int.
    horizon_value: non-null non-bool int, matching period position within its month.
    horizon_in_year: non-null non-bool int, matching cumulative period number.
    issue_date (from key): must be a valid horizon boundary.
    target_date (from key): must equal issue_date + 1 day.
    """
    for f in _PLAN_WRITABLE_FIELDS:
        if f not in payload:
            return False

    comp = payload.get("composition")
    if comp is not None and (isinstance(comp, bool) or not isinstance(comp, str)):
        return False

    flag = payload.get("flag")
    if flag is not None and (isinstance(flag, bool) or not isinstance(flag, int)):
        return False

    hv = payload.get("horizon_value")
    if hv is None or isinstance(hv, bool) or not isinstance(hv, int):
        return False

    hiy = payload.get("horizon_in_year")
    if hiy is None or isinstance(hiy, bool) or not isinstance(hiy, int):
        return False

    ht, _, _, issue_date, target_date = key

    if not _plan_is_valid_boundary(issue_date, ht):
        return False
    if target_date != issue_date + _dt.timedelta(days=1):
        return False

    day = target_date.day
    if ht == "pentad":
        if day <= 5:
            exp_hv = 1
        elif day <= 10:
            exp_hv = 2
        elif day <= 15:
            exp_hv = 3
        elif day <= 20:
            exp_hv = 4
        elif day <= 25:
            exp_hv = 5
        else:
            exp_hv = 6
        exp_hiy = (target_date.month - 1) * 6 + exp_hv
    elif ht == "decad":
        if day <= 10:
            exp_hv = 1
        elif day <= 20:
            exp_hv = 2
        else:
            exp_hv = 3
        exp_hiy = (target_date.month - 1) * 3 + exp_hv
    else:
        return False

    return hv == exp_hv and hiy == exp_hiy


def _plan_records_identical(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Compare two payload dicts on all writable fields by validated types.

    composition: exact string equality ("01" != "1").
    flag/horizon_value/horizon_in_year: exact type-and-value equality
        (bool is not equal to its int equivalent).
    ALL5 numeric fields: float equality; booleans are rejected as invalid.
    """
    for f in _PLAN_WRITABLE_FIELDS:
        va, vb = a.get(f), b.get(f)
        if va is None and vb is None:
            continue
        if va is None or vb is None:
            return False
        if f in _PLAN_STR_META_FIELDS:
            if va != vb:
                return False
        elif f in _PLAN_INT_META_FIELDS:
            # bool is a subclass of int in Python, so type(True) is bool != int;
            # flag=True must not collapse with flag=1.
            if type(va) is not type(vb) or va != vb:
                return False
        else:
            # ALL5 numeric: direct equality; booleans are invalid values.
            # float() conversion is intentionally avoided: it loses precision
            # for large integers (e.g. 2**53 == 2**53+1 after float cast).
            if isinstance(va, bool) or isinstance(vb, bool):
                return False
            if va != vb:
                return False
    return True


@dataclass
class PeriodRefreshPlan:
    """Disjoint disposition sets produced by plan_period_refresh.

    Key type: (horizon_type, code, model_type, issue_date, target_date).
    horizon_type is normalized via _HT_EXPLICIT; model_type is canonical
    via _PLAN_SUPPORTED_MODEL_MAP.

    ``plan_error`` is non-None when a scope-level failure prevents safe planning;
    callers must treat this as an abort signal and not act on any other field.
    """

    missing: set = field(default_factory=set)
    identical: set = field(default_factory=set)
    authorized_refresh: list = field(default_factory=list)  # [(key, merged_payload), ...]
    protected: set = field(default_factory=set)
    excluded: dict = field(default_factory=dict)  # key -> reason str
    blocked: dict = field(default_factory=dict)  # key -> reason str
    plan_error: str | None = None  # scope-level failure; set → caller must abort


def plan_period_refresh(
    existing_payloads: list[dict[str, Any]],
    candidate_payloads: list[dict[str, Any]],
    authorized_keys: set[tuple],
    explicit_refresh_keys: set[tuple],
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    forecast_date: _dt.date,
    existing_read_status: str = "",
) -> PeriodRefreshPlan:
    """Plan which period forecast records to write for a scoped recovery run.

    Pure function: no API calls, no writes, no side effects.  Returns a
    PeriodRefreshPlan whose sets are disjoint — every candidate key lands in
    exactly one of: missing, identical, authorized_refresh (list of payloads),
    protected, excluded, or blocked.

    ``existing_read_status`` MUST be passed explicitly as ``"COMPLETE"`` or
    ``"CONFIRMED_EMPTY"`` by the caller, proving the read succeeded.  Any
    other value (including the empty-string default) causes an immediate
    ``plan_error`` return so the planner never infers absence from an
    unverified read.

    ``authorized_keys`` is the output allowlist: only keys in this set may
    land in ``missing`` or ``authorized_refresh``.  Candidates not present in
    ``authorized_keys`` are classified as ``protected`` even when absent from
    the existing DB state.  ``explicit_refresh_keys`` must be a strict subset
    of ``authorized_keys``; it further gates which existing-differs candidates
    may be overwritten.

    Args:
        existing_payloads: Complete current DB records for the queried scope.
        candidate_payloads: Normalized input-derived candidates.  Each must
            carry all 5-part key fields and ALL5 numeric fields.
        authorized_keys: Output allowlist; gates missing and authorized_refresh.
        explicit_refresh_keys: Subset of authorized_keys; gates overwrite of
            existing records that differ from the candidate.
        start_date: Inclusive lower bound for issue_date.
        end_date: Inclusive upper bound for issue_date.
        forecast_date: Captured run date; issue_date must be <= this.
        existing_read_status: Must be "COMPLETE" or "CONFIRMED_EMPTY".
    """
    plan = PeriodRefreshPlan()

    # --- Validate existing_read_status before any inference ---
    if existing_read_status not in ("COMPLETE", "CONFIRMED_EMPTY"):
        plan.plan_error = (
            f"existing_read_status must be 'COMPLETE' or 'CONFIRMED_EMPTY'; "
            f"got {existing_read_status!r}"
        )
        return plan

    # Contradictory: CONFIRMED_EMPTY but rows present
    if existing_read_status == "CONFIRMED_EMPTY" and existing_payloads:
        plan.plan_error = (
            "existing_read_status is CONFIRMED_EMPTY but existing_payloads is non-empty"
        )
        return plan

    # --- Validate bounds ---
    if start_date > end_date:
        plan.plan_error = f"start_date {start_date} > end_date {end_date}"
        return plan
    if end_date > forecast_date:
        plan.plan_error = f"end_date {end_date} > forecast_date {forecast_date}"
        return plan

    # --- Validate explicit_refresh_keys ⊆ authorized_keys ---
    extra_refresh = explicit_refresh_keys - authorized_keys
    if extra_refresh:
        plan.plan_error = (
            f"explicit_refresh_keys has {len(extra_refresh)} key(s) not in "
            "authorized_keys; explicit_refresh_keys must be a subset"
        )
        return plan

    # --- Index existing payloads by key; detect conflicting duplicates ---
    existing_by_key: dict[tuple, dict[str, Any]] = {}
    existing_conflicted: set[tuple] = set()
    has_malformed_existing = False

    for p in existing_payloads:
        key = _plan_key(p)
        if key is None:
            has_malformed_existing = True
            logger.warning("plan_period_refresh: malformed existing payload (unparseable key)")
            continue
        # Validate ALL5 and writable metadata for existing records; fail closed.
        if not _plan_existing_all5_valid(p) or not _plan_valid_meta_fields(p, key):
            has_malformed_existing = True
            logger.warning(
                "plan_period_refresh: malformed existing payload (invalid writable fields)"
            )
            continue
        if key in existing_conflicted:
            continue
        if key in existing_by_key:
            if _plan_records_identical(existing_by_key[key], p):
                pass  # exact duplicate: collapse to first occurrence
            else:
                existing_conflicted.add(key)
                del existing_by_key[key]
                logger.warning("plan_period_refresh: conflicting existing duplicate key")
        else:
            existing_by_key[key] = p

    # Fail closed: a malformed existing record means absence cannot be safely inferred
    if has_malformed_existing:
        plan.plan_error = (
            "existing_payloads contains malformed records with unparseable keys; "
            "cannot safely infer absence — aborting"
        )
        return plan

    # --- Validate, dedup, and index candidate payloads ---
    candidate_by_key: dict[tuple, dict[str, Any]] = {}
    candidate_conflicted: dict[tuple, str] = {}
    candidate_excluded: dict[tuple, str] = {}

    for p in candidate_payloads:
        key = _plan_key(p)
        if key is None:
            # Finding 6: a candidate whose key cannot be trusted means absence
            # cannot be safely inferred — fail closed.
            plan.plan_error = (
                "candidate_payloads contains a record with an unparseable key; "
                "cannot safely plan — aborting"
            )
            return plan

        ht, code, model_type, issue_date, target_date = key

        if ht not in ("pentad", "decad"):
            candidate_excluded[key] = f"unsupported horizon_type {ht!r}"
            continue

        if model_type in _PLAN_EXCLUDED_MODEL_TYPES:
            candidate_excluded[key] = f"model_type {model_type!r} is an independent source"
            continue

        if issue_date < start_date or issue_date > end_date:
            candidate_excluded[key] = f"issue_date {issue_date} outside [{start_date}, {end_date}]"
            continue

        if issue_date > forecast_date:
            candidate_excluded[key] = f"issue_date {issue_date} after forecast_date {forecast_date}"
            continue

        if not _plan_is_valid_boundary(issue_date, ht):
            candidate_excluded[key] = f"issue_date {issue_date} not a {ht} boundary"
            continue

        expected_target = issue_date + _dt.timedelta(days=1)
        if target_date != expected_target:
            candidate_excluded[key] = f"target {target_date} != issue+1 day ({expected_target})"
            continue

        if not _plan_all5_finite(p):
            # Finding 7: if a valid variant is already indexed for this key,
            # both are in conflict — block the key rather than allowing the
            # valid variant to proceed while the excluded one is ignored.
            if key in candidate_by_key:
                del candidate_by_key[key]
                candidate_conflicted[key] = "valid and excluded variant for same key"
            elif key not in candidate_conflicted:
                candidate_excluded[key] = "incomplete: not all 5 numeric fields are finite"
            continue

        # Validate metadata fields (composition, flag, horizon_value, horizon_in_year).
        # Finding 7 semantics: a meta-invalid variant conflicts with any valid variant.
        if not _plan_valid_meta_fields(p, key):
            if key in candidate_by_key:
                del candidate_by_key[key]
                candidate_conflicted[key] = "valid and excluded variant for same key"
            elif key not in candidate_conflicted:
                candidate_excluded[key] = "invalid writable metadata fields"
            continue

        if key in candidate_conflicted:
            continue  # already blocked

        # Finding 7: if an excluded variant is already indexed for this key,
        # the valid+excluded conflict must block the key.
        if key in candidate_excluded:
            del candidate_excluded[key]
            candidate_conflicted[key] = "valid and excluded variant for same key"
            continue

        if key in candidate_by_key:
            if _plan_records_identical(candidate_by_key[key], p):
                continue  # exact duplicate: collapse without heuristic
            else:
                del candidate_by_key[key]
                candidate_conflicted[key] = "conflicting duplicate candidate keys"
        else:
            candidate_by_key[key] = p

    # --- Classify each surviving candidate ---
    for key, candidate in candidate_by_key.items():
        if key in existing_conflicted:
            plan.blocked[key] = "conflicting duplicate existing records"
            continue

        # authorized_keys is the output allowlist: only these may be written
        if key not in authorized_keys:
            plan.protected.add(key)
            continue

        existing = existing_by_key.get(key)

        if existing is None:
            plan.missing.add(key)
        elif _plan_records_identical(existing, candidate):
            plan.identical.add(key)
        elif key in explicit_refresh_keys:
            # Preserve non-derived fields from existing; override ALL5 + composition
            merged: dict[str, Any] = dict(existing)
            for f in _PLAN_ALL5_FIELDS:
                merged[f] = candidate.get(f)
            cand_comp = candidate.get("composition")
            if cand_comp is not None:
                merged["composition"] = cand_comp
            # If merged equals existing, candidate ALL5 matches → no-op write
            if _plan_records_identical(merged, existing):
                plan.identical.add(key)
            else:
                plan.authorized_refresh.append((key, merged))
        else:
            plan.protected.add(key)

    for key, reason in candidate_conflicted.items():
        plan.blocked[key] = reason

    for key, reason in candidate_excluded.items():
        plan.excluded[key] = reason

    # --- Existing healthy keys with no surviving candidate → protected ---
    # Priority: blocked > excluded > protected — a key must appear in at most one set.
    for key in existing_by_key:
        if key not in candidate_by_key and key not in plan.blocked and key not in plan.excluded:
            plan.protected.add(key)

    # Conflicting existing keys with no matching candidate → blocked
    # Only if not already assigned a higher-priority disposition.
    for key in existing_conflicted:
        if key not in plan.blocked:
            plan.excluded.pop(key, None)  # blocked takes priority over excluded
            plan.blocked[key] = "conflicting duplicate existing records (no candidate)"

    return plan
