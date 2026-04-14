"""Tests for _add_climatological_quantile_bounds and Q25/Q75 column survival."""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from lt_utils import infer_q_columns, prepare_long_forecast_records
from run_forecast import _add_climatological_quantile_bounds

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_CODES = ["12345", "67890"]
_FORECAST_VALID_FROM = pd.Timestamp("2024-07-01")
_FORECAST_VALID_TO = pd.Timestamp("2024-07-31")
_FORECAST_DATE = pd.Timestamp("2024-06-15")


def _make_temporal_data(
    start: str,
    end: str,
    codes: list[str] = None,
    discharge_by_code: dict[str, float] | None = None,
    discharge_july_override: dict[str, float] | None = None,
    missing_fraction_code_month: dict[tuple, float] | None = None,
) -> pd.DataFrame:
    """Build a deterministic daily temporal_data DataFrame.

    Args:
        start: Start date string (e.g. "2018-01-01").
        end: End date string (e.g. "2023-12-31").
        codes: Station codes to include (default: _CODES).
        discharge_by_code: Base annual discharge value per code. Defaults to
            {"12345": 100.0, "67890": 50.0}.
        discharge_july_override: Optional override for July discharge per code.
            Used by leave-one-out tests that want July of a specific year to be
            different. Keys are (code, year) tuples.
        missing_fraction_code_month: Map of (code, year, month) -> fraction of
            days to set as NaN (to simulate sparse months).

    Returns:
        DataFrame with columns date, code, discharge.
    """
    if codes is None:
        codes = _CODES
    if discharge_by_code is None:
        discharge_by_code = {"12345": 100.0, "67890": 50.0}

    dates = pd.date_range(start, end, freq="D")
    rows = []
    rng = np.random.default_rng(seed=42)
    for code in codes:
        base_q = discharge_by_code.get(code, 100.0)
        for dt in dates:
            q = base_q + rng.normal(0, 5)

            # Apply per-(code, year) July override
            if discharge_july_override and dt.month == 7:
                key = (code, dt.year)
                if key in discharge_july_override:
                    q = discharge_july_override[key] + rng.normal(0, 1)

            rows.append({"date": dt, "code": code, "discharge": q})

    df = pd.DataFrame(rows)

    # Apply missing-data fraction per (code, year, month)
    if missing_fraction_code_month:
        for (code, year, month), frac in missing_fraction_code_month.items():
            mask = (
                (df["code"] == code) & (df["date"].dt.year == year) & (df["date"].dt.month == month)
            )
            idx = df[mask].index
            n_missing = int(len(idx) * frac)
            df.loc[idx[:n_missing], "discharge"] = np.nan

    return df


def _make_forecast(codes: list[str] = None, q_values: dict[str, float] = None) -> pd.DataFrame:
    """Build a minimal forecast DataFrame.

    Args:
        codes: Station codes. Defaults to _CODES.
        q_values: Q_GBT value per code. Defaults to {"12345": 100.0, "67890": 50.0}.

    Returns:
        DataFrame with columns date, code, valid_from, valid_to, Q_GBT, flag.
    """
    if codes is None:
        codes = _CODES
    if q_values is None:
        q_values = {"12345": 100.0, "67890": 50.0}

    rows = [
        {
            "date": _FORECAST_DATE,
            "code": code,
            "valid_from": _FORECAST_VALID_FROM,
            "valid_to": _FORECAST_VALID_TO,
            "Q_GBT": q_values.get(code, 100.0),
            "flag": 0,
        }
        for code in codes
    ]
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Fixture
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def sample_data() -> dict:
    """Return a dict with forecast, temporal_data, model_name, and today.

    Six years of daily discharge (2018-2023). Excluding the forecast year
    (2024) leaves exactly 6 years, satisfying the n >= 3 threshold.
    """
    return {
        "model_name": "GBT",
        "today": pd.Timestamp("2024-06-15"),
        "forecast": _make_forecast(),
        "temporal_data": _make_temporal_data("2018-01-01", "2023-12-31"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Tests: _add_climatological_quantile_bounds
# ──────────────────────────────────────────────────────────────────────────────


def test_gbt_forecast_gets_q25_q75(sample_data):
    """Q25 and Q75 columns exist in the result after calling the function."""
    result = _add_climatological_quantile_bounds(
        forecast=sample_data["forecast"],
        temporal_data=sample_data["temporal_data"],
        model_name=sample_data["model_name"],
        today=sample_data["today"],
    )

    assert "Q25" in result.columns, "Q25 column missing from result"
    assert "Q75" in result.columns, "Q75 column missing from result"


def test_quantile_ordering(sample_data):
    """Q25 <= Q_GBT <= Q75 for all non-NaN rows."""
    result = _add_climatological_quantile_bounds(
        forecast=sample_data["forecast"],
        temporal_data=sample_data["temporal_data"],
        model_name=sample_data["model_name"],
        today=sample_data["today"],
    )

    valid = result.dropna(subset=["Q25", "Q75", "Q_GBT"])
    assert len(valid) > 0, "No valid (non-NaN) rows to assert ordering on"
    assert (valid["Q25"] <= valid["Q_GBT"]).all(), "Q25 > Q_GBT in at least one row"
    assert (valid["Q_GBT"] <= valid["Q75"]).all(), "Q_GBT > Q75 in at least one row"


def test_non_negativity(sample_data):
    """All Q25 and Q75 values are >= 0."""
    result = _add_climatological_quantile_bounds(
        forecast=sample_data["forecast"],
        temporal_data=sample_data["temporal_data"],
        model_name=sample_data["model_name"],
        today=sample_data["today"],
    )

    valid_q25 = result["Q25"].dropna()
    valid_q75 = result["Q75"].dropna()
    assert (valid_q25 >= 0).all(), "Negative Q25 values found"
    assert (valid_q75 >= 0).all(), "Negative Q75 values found"


def test_empty_forecast_returns_unchanged():
    """An empty forecast DataFrame is returned unchanged."""
    empty_forecast = pd.DataFrame(
        columns=["date", "code", "valid_from", "valid_to", "Q_GBT", "flag"]
    )
    temporal_data = _make_temporal_data("2018-01-01", "2023-12-31")

    result = _add_climatological_quantile_bounds(
        forecast=empty_forecast,
        temporal_data=temporal_data,
        model_name="GBT",
        today=pd.Timestamp("2024-06-15"),
    )

    assert len(result) == 0


def test_insufficient_data_fallback():
    """Q25 and Q75 are NaN when fewer than 3 years of data are available."""
    # Only 2 years of temporal data — below the n >= 3 threshold
    temporal_data = _make_temporal_data("2022-01-01", "2023-12-31")
    forecast = _make_forecast()

    result = _add_climatological_quantile_bounds(
        forecast=forecast,
        temporal_data=temporal_data,
        model_name="GBT",
        today=pd.Timestamp("2024-06-15"),
    )

    # Either the columns are absent or all their values are NaN
    if "Q25" in result.columns:
        assert result["Q25"].isna().all(), "Expected Q25 to be NaN with insufficient data"
    if "Q75" in result.columns:
        assert result["Q75"].isna().all(), "Expected Q75 to be NaN with insufficient data"


def test_leave_one_out():
    """2024 data is excluded from std computation (leave-one-out on forecast year)."""
    # Years 2020-2023: normal discharge (~100 m3/s for July, code 12345)
    # Year 2024: extreme discharge (1000 m3/s for July) — must be excluded
    discharge_july_overrides = {
        ("12345", 2024): 1000.0,
        ("67890", 2024): 1000.0,
    }
    temporal_data = _make_temporal_data(
        "2020-01-01",
        "2024-06-14",  # up to day before today
        discharge_july_override=discharge_july_overrides,
    )
    forecast = _make_forecast(codes=["12345"], q_values={"12345": 100.0})
    today = pd.Timestamp("2024-06-15")

    result = _add_climatological_quantile_bounds(
        forecast=forecast,
        temporal_data=temporal_data,
        model_name="GBT",
        today=today,
    )

    # Compute the expected std from years 2020-2023 only (July, code 12345)
    td_filtered = temporal_data[
        (temporal_data["code"] == "12345")
        & (temporal_data["date"].dt.month == 7)
        & (temporal_data["date"].dt.year != 2024)
    ].copy()
    td_filtered["year"] = td_filtered["date"].dt.year
    yearly_means = (
        td_filtered.groupby("year")["discharge"]
        .apply(lambda s: s.mean() if s.notna().mean() >= 0.5 else np.nan)
        .dropna()
    )
    if len(yearly_means) >= 3:
        expected_std = yearly_means.std(ddof=1)
        expected_q25 = 100.0 - 0.674 * expected_std
        expected_q75 = 100.0 + 0.674 * expected_std
        expected_q25 = max(expected_q25, 0.0)
        expected_q75 = max(expected_q75, 0.0)

        row = result[result["code"] == "12345"].iloc[0]
        np.testing.assert_allclose(row["Q25"], expected_q25, rtol=0.05)
        np.testing.assert_allclose(row["Q75"], expected_q75, rtol=0.05)
    else:
        pytest.skip("Fewer than 3 valid years after filtering — adjust test data range")


def test_50_percent_coverage_filter():
    """Months with < 50% non-missing days are excluded from std computation."""
    # Create temporal data where July of 2021 has only ~30% coverage for code 12345
    # This means that year should be dropped from std calculation
    temporal_data = _make_temporal_data(
        "2018-01-01",
        "2023-12-31",
        missing_fraction_code_month={
            ("12345", 2021, 7): 0.75,  # 75% missing => only 25% coverage
        },
    )
    forecast = _make_forecast(codes=["12345"], q_values={"12345": 100.0})

    # Compute expected std using only years with >= 50% coverage for July, code 12345
    td_12345_july = temporal_data[
        (temporal_data["code"] == "12345") & (temporal_data["date"].dt.month == 7)
    ].copy()
    td_12345_july["year"] = td_12345_july["date"].dt.year
    yearly_means = (
        td_12345_july.groupby("year")["discharge"]
        .apply(lambda s: s.mean() if s.notna().mean() >= 0.5 else np.nan)
        .dropna()
    )
    # 2021 should be excluded; remaining years should produce a valid std
    assert 2021 not in yearly_means.index, "2021 should be excluded due to low coverage"
    assert len(yearly_means) >= 3, "Need at least 3 valid years for this test to be meaningful"

    result = _add_climatological_quantile_bounds(
        forecast=forecast,
        temporal_data=temporal_data,
        model_name="GBT",
        today=pd.Timestamp("2024-06-15"),
    )

    # Result should still have valid Q25/Q75 (from the remaining years)
    row = result[result["code"] == "12345"].iloc[0]
    assert pd.notna(row["Q25"]), "Q25 should be non-NaN when enough valid years remain"
    assert pd.notna(row["Q75"]), "Q75 should be non-NaN when enough valid years remain"


def test_empty_temporal_data():
    """Forecast is returned with NaN (or absent) Q25/Q75 when all discharge is NaN."""
    temporal_data = _make_temporal_data("2018-01-01", "2023-12-31")
    temporal_data["discharge"] = np.nan  # wipe all discharge values
    forecast = _make_forecast()

    result = _add_climatological_quantile_bounds(
        forecast=forecast,
        temporal_data=temporal_data,
        model_name="GBT",
        today=pd.Timestamp("2024-06-15"),
    )

    if "Q25" in result.columns:
        assert result["Q25"].isna().all(), "Expected all Q25 to be NaN with empty temporal data"
    if "Q75" in result.columns:
        assert result["Q75"].isna().all(), "Expected all Q75 to be NaN with empty temporal data"


# ──────────────────────────────────────────────────────────────────────────────
# Tests: column survival through infer_q_columns and prepare_long_forecast_records
# ──────────────────────────────────────────────────────────────────────────────


def test_column_survival_through_infer_q_columns():
    """infer_q_columns detects Q25 and Q75 alongside Q_GBT."""
    df = pd.DataFrame(
        {
            "Q_GBT": [100.0],
            "Q25": [73.26],
            "Q75": [126.74],
            "flag": [0],
        }
    )

    detected = infer_q_columns(df)

    assert "Q25" in detected, "Q25 not detected by infer_q_columns"
    assert "Q75" in detected, "Q75 not detected by infer_q_columns"
    assert "Q_GBT" in detected, "Q_GBT not detected by infer_q_columns"


def test_column_survival_through_prepare_long_forecast_records():
    """prepare_long_forecast_records maps Q25/Q75 to q25/q75 in output records."""
    forecast_df = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-06-15"),
                "code": "12345",
                "valid_from": pd.Timestamp("2024-07-01"),
                "valid_to": pd.Timestamp("2024-07-31"),
                "Q_GBT": 100.0,
                "Q25": 73.26,
                "Q75": 126.74,
                "flag": 0,
            }
        ]
    )

    records = prepare_long_forecast_records(forecast_df, model_name="GBT")

    assert len(records) == 1, "Expected exactly one record"
    record = records[0]
    assert "q25" in record, "q25 key missing from output record"
    assert "q75" in record, "q75 key missing from output record"
    np.testing.assert_allclose(record["q25"], 73.26, rtol=1e-6)
    np.testing.assert_allclose(record["q75"], 126.74, rtol=1e-6)
