"""Shared fixtures + canonicalization for the M1 P0 flag-OFF golden baseline.

Not a test module itself (no ``test_*`` functions) — imported by
``test_skill_lead_aware_golden_baseline.py`` and by
``generate_skill_lead_aware_golden.py`` (the one-off script that (re)writes
``golden/skill_lead_aware_flag_off_baseline.json``). Keeping the
fixture-building + canonicalization logic in one place means the generator
and the assertion test can never silently drift apart.

Station code is the synthetic "19999" placeholder (see
doc/plans/working feedback_no_real_station_codes.md convention) — no real
station codes or discharge values.
"""

import math

import pandas as pd

STATION = "19999"
MODELS = ["LR", "TFT"]

# The aggregated (quarter/season) EM path only forms an Ensemble Mean from
# the canonical model pair AGGREGATED_EM_RAW_MODELS = {"LR_BASE", "LR_SM"}
# (src/model_names.py) — an arbitrary two-model pool (e.g. LR/TFT, used for
# the monthly fixture below) never produces an aggregated EM row. Use the
# restricted pair for quarter/season so the golden snapshot actually
# exercises the aggregated EM code path instead of silently skipping it.
AGGREGATED_MODELS = ["LR_BASE", "LR_SM"]

# Threshold/gate env vars pinned explicitly so the golden snapshot is
# insulated from unrelated future changes to METRIC_REGISTRY defaults.
THRESHOLD_ENV = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",
    "ieasyhydroforecast_nse_threshold": "0.8",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
    "ieasyhydroforecast_min_pairs_long_term": "4",
    "ieasyhydroforecast_min_pairs_long_term_quarter": "5",
    "ieasyhydroforecast_min_pairs_long_term_season": "5",
}

# Synthetic observed discharge by year — enough year-to-year variance for
# NSE/sdivsigma/accuracy to be well-defined and non-degenerate.
_OBS_BY_YEAR = {
    2015: 100.0,
    2016: 130.0,
    2017: 90.0,
    2018: 150.0,
    2019: 115.0,
}

# Per-model relative bias applied to the synthetic observed discharge to
# build "forecasts". Small enough that both models clear the default
# skill thresholds (so EM/Skilled Mean/Naive Mean all materialize).
_MODEL_BIAS = {"LR": 1.03, "TFT": 0.96, "LR_BASE": 1.03, "LR_SM": 0.96}

_QUANTILE_SPREAD = {
    "q05": 0.85,
    "q10": 0.90,
    "q25": 0.95,
    "q50": 1.00,
    "q75": 1.05,
    "q90": 1.10,
    "q95": 1.15,
}


def _quantiles_for(center: float) -> dict:
    return {qcol: round(center * mult, 4) for qcol, mult in _QUANTILE_SPREAD.items()}


# ---------------------------------------------------------------------------
# Monthly fixtures — month_in_year=6, two leads (horizon_value 0 and 1) to
# exercise the already-shipped PP-038 per-lead stratification.
# ---------------------------------------------------------------------------

MONTH_IN_YEAR = 6
_MONTH_YEARS_BY_LEAD = {
    0: [2015, 2016, 2017, 2018, 2019],  # 5 years — above the K=4 floor
    1: [2015, 2016, 2017, 2018],  # 4 years — exactly at the K=4 floor
}


def build_monthly_observations() -> pd.DataFrame:
    years = sorted({y for ys in _MONTH_YEARS_BY_LEAD.values() for y in ys})
    rows = [
        {
            "code": STATION,
            "year": year,
            "month": MONTH_IN_YEAR,
            "discharge_avg": _OBS_BY_YEAR[year],
        }
        for year in years
    ]
    df = pd.DataFrame(rows)
    df["month_in_year"] = df["month"]
    delta_df = (
        df.groupby(["code", "month_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "month_in_year", "delta"]], on=["code", "month_in_year"])


def build_monthly_forecasts() -> pd.DataFrame:
    rows = []
    for horizon_value, years in _MONTH_YEARS_BY_LEAD.items():
        for year in years:
            obs = _OBS_BY_YEAR[year]
            for model in MODELS:
                center = obs * _MODEL_BIAS[model]
                row = {
                    "code": STATION,
                    "year": year,
                    "month": MONTH_IN_YEAR,
                    "model_short": model,
                    "horizon_value": horizon_value,
                    "valid_from": pd.Timestamp(year=year, month=MONTH_IN_YEAR, day=1),
                    "valid_to": pd.Timestamp(year=year, month=MONTH_IN_YEAR, day=28),
                    "date": pd.Timestamp(year=year, month=MONTH_IN_YEAR, day=1)
                    - pd.DateOffset(months=horizon_value),
                }
                row.update(_quantiles_for(center))
                rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Quarterly fixtures — quarter_in_year=2, single lead, 5 years (== K=5 floor).
# ---------------------------------------------------------------------------

QUARTER_IN_YEAR = 2
_QUARTER_YEARS = [2015, 2016, 2017, 2018, 2019]


def build_quarterly_observations() -> pd.DataFrame:
    rows = [
        {
            "code": STATION,
            "year": year,
            "quarter_in_year": QUARTER_IN_YEAR,
            "discharge_avg": _OBS_BY_YEAR[year],
        }
        for year in _QUARTER_YEARS
    ]
    df = pd.DataFrame(rows)
    delta_df = (
        df.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "quarter_in_year", "delta"]], on=["code", "quarter_in_year"])


def build_quarterly_forecasts() -> pd.DataFrame:
    rows = []
    for year in _QUARTER_YEARS:
        obs = _OBS_BY_YEAR[year]
        for model in AGGREGATED_MODELS:
            center = obs * _MODEL_BIAS[model]
            row = {
                "code": STATION,
                "year": year,
                "quarter_in_year": QUARTER_IN_YEAR,
                "model_short": model,
            }
            row.update(_quantiles_for(center))
            rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Seasonal fixtures — season_in_year=1 (constant), 5 years (== K=5 floor),
# one issue date per year so the per-date EM grouping is exercised without
# stressing the (separately-scoped) multi-issue-per-year re-issue case.
# ---------------------------------------------------------------------------

_SEASON_YEARS = [2015, 2016, 2017, 2018, 2019]


def build_seasonal_observations() -> pd.DataFrame:
    rows = [
        {"code": STATION, "season_year": year, "discharge_avg": _OBS_BY_YEAR[year]}
        for year in _SEASON_YEARS
    ]
    df = pd.DataFrame(rows)
    df["season_in_year"] = 1
    delta_df = df.groupby(["code"]).agg(std_discharge=("discharge_avg", "std")).reset_index()
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "delta"]], on=["code"])


def build_seasonal_forecasts() -> pd.DataFrame:
    rows = []
    for year in _SEASON_YEARS:
        obs = _OBS_BY_YEAR[year]
        issue_date = pd.Timestamp(year=year, month=4, day=1)
        for model in AGGREGATED_MODELS:
            center = obs * _MODEL_BIAS[model]
            row = {
                "code": STATION,
                "season_year": year,
                "model_short": model,
                "date": issue_date,
            }
            row.update(_quantiles_for(center))
            rows.append(row)
    df = pd.DataFrame(rows)
    df["season_in_year"] = 1
    return df


# ---------------------------------------------------------------------------
# Canonicalization — deterministic, JSON-safe representation of a DataFrame
# for snapshot comparison.
# ---------------------------------------------------------------------------


def _json_safe_scalar(value):
    if hasattr(value, "item"):  # numpy scalar types (int64, float64, ...)
        value = value.item()
    try:
        is_na = pd.isna(value)
    except (TypeError, ValueError):
        is_na = False
    if is_na is True:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return round(value, 6)
    return value


def canonicalize(df: pd.DataFrame) -> list:
    """Convert a DataFrame into a deterministic, JSON-serializable snapshot.

    Columns are sorted alphabetically; rows are sorted by their
    (stringified) full-row tuple for a stable order independent of the
    upstream groupby/concat order; floats are rounded to 6 decimals; NaN
    becomes ``None``; numpy/pandas scalar types are converted to native
    Python types.
    """
    if df is None or df.empty:
        return []
    ordered_cols = sorted(df.columns)
    records = []
    for _, row in df[ordered_cols].iterrows():
        record = {col: _json_safe_scalar(row[col]) for col in ordered_cols}
        records.append(record)
    records.sort(key=lambda r: [str(r[c]) for c in ordered_cols])
    return records
