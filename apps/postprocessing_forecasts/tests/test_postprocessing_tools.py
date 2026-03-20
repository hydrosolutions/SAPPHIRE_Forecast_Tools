import datetime as dt
import os
import sys

import pandas as pd
import pytest

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from conftest import DECAD, PENTAD

# Import the functions to test
from src.postprocessing_tools import (
    forecast_target_date,
    log_most_recent_forecasts,
    log_most_recent_forecasts_monthly,
)


# Fixture to create sample test data
@pytest.fixture
def sample_data():
    """Create sample forecast data for testing."""
    sample_date = dt.datetime(2023, 5, 25)
    data = pd.DataFrame(
        {
            "code": [
                "15102",
                "15124",
                "15136",
                "15102",
                "15124",
                "15136",
                "15102",
                "15124",
                "15136",
                "15102",
                "15124",
                "15136",
            ],
            "date": [sample_date] * 12,
            "pentad_in_month": [5] * 12,
            "pentad_in_year": [30] * 12,
            "forecasted_discharge": [
                125.4,
                45.7,
                67.8,
                130.2,
                47.2,
                65.1,
                128.7,
                46.9,
                66.5,
                129.1,
                47.0,
                66.3,
            ],
            "model_short": [
                "LR",
                "LR",
                "LR",
                "TFT",
                "TFT",
                "TFT",
                "TIDE",
                "TIDE",
                "TIDE",
                "EM",
                "EM",
                "EM",
            ],
        }
    )
    return data


@pytest.fixture
def expected_pivot_data(sample_data):
    """Create expected pivot table output based on sample data."""
    sample_date = dt.datetime(2023, 5, 25)
    return pd.DataFrame(
        {
            "code": ["15102", "15124", "15136"],
            "date": [sample_date] * 3,
            "pentad_in_month": [5] * 3,
            "LR": [125.4, 45.7, 67.8],
            "TFT": [130.2, 47.2, 65.1],
            "TIDE": [128.7, 46.9, 66.5],
            "EM": [129.1, 47.0, 66.3],
        }
    )


def test_log_most_recent_forecasts_pentad(tmp_path, monkeypatch, sample_data, expected_pivot_data):
    """Test log_most_recent_forecasts with pentad config and sample data."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))

    result = log_most_recent_forecasts(PENTAD, sample_data)

    # Verify directory was created
    forecast_dir = tmp_path / "forecast_logs"
    assert forecast_dir.exists()

    # Verify CSV was written
    csv_files = list(forecast_dir.glob("recent_model_forecasts_*.csv"))
    assert len(csv_files) == 1
    assert "20230525" in csv_files[0].name

    # Verify written CSV content matches returned DataFrame
    written = pd.read_csv(csv_files[0])
    assert len(written) == len(result)

    # Check the structure of the result
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(expected_pivot_data)

    result_names = sorted(result.columns.tolist())
    expected_names = sorted(expected_pivot_data.columns.tolist())
    assert result_names == expected_names

    # Compare with expected values
    for idx, row in result.iterrows():
        expected_row = expected_pivot_data.iloc[idx]
        assert row["code"] == expected_row["code"]
        assert row["date"] == expected_row["date"]
        for model in ["LR", "TFT", "TIDE", "EM"]:
            assert row[model] == pytest.approx(expected_row[model])


def test_log_most_recent_forecasts_pentad_empty_data(tmp_path, monkeypatch, sample_data):
    """Test the function with empty data."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    empty_data = pd.DataFrame(columns=sample_data.columns)

    result = log_most_recent_forecasts(PENTAD, empty_data)

    # No CSV should be written
    forecast_dir = tmp_path / "forecast_logs"
    csv_files = list(forecast_dir.glob("*.csv")) if forecast_dir.exists() else []
    assert len(csv_files) == 0
    assert result.empty


def test_log_most_recent_forecasts_pentad_multiple_dates(tmp_path, monkeypatch, sample_data):
    """Test with multiple dates - should only use the most recent date."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))

    multi_date_data = sample_data.copy()
    older_data = sample_data.copy()
    older_data["date"] = dt.datetime(2023, 5, 20)
    multi_date_data = pd.concat([multi_date_data, older_data])

    result = log_most_recent_forecasts(PENTAD, multi_date_data)

    assert len(result) == 3  # 3 stations
    sample_date = dt.datetime(2023, 5, 25)
    assert all(date == sample_date for date in result["date"])


# ============================================================================
# Bug 3 Fix Tests: Unsafe .iloc[0] access
# ============================================================================


def test_log_most_recent_forecasts_pentad_nat_dates(tmp_path, monkeypatch):
    """Test that the function handles NaT dates gracefully."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    sample_date = dt.datetime(2023, 5, 25)

    valid_data = pd.DataFrame(
        {
            "code": ["15102"] * 4,
            "date": [sample_date] * 4,
            "pentad_in_month": [5] * 4,
            "pentad_in_year": [30] * 4,
            "forecasted_discharge": [125.4, 130.2, 128.7, 129.1],
            "model_short": ["LR", "TFT", "TIDE", "EM"],
        }
    )

    nat_data = pd.DataFrame(
        {
            "code": ["15999"] * 4,
            "date": [pd.NaT] * 4,
            "pentad_in_month": [5] * 4,
            "pentad_in_year": [30] * 4,
            "forecasted_discharge": [100.0, 110.0, 105.0, 107.5],
            "model_short": ["LR", "TFT", "TIDE", "EM"],
        }
    )

    combined_data = pd.concat([valid_data, nat_data], ignore_index=True)

    # Should NOT raise IndexError
    result = log_most_recent_forecasts(PENTAD, combined_data)

    assert len(result) == 1, (
        f"Only station 15102 has valid dates, expected 1 row, got {len(result)}"
    )
    assert result.iloc[0]["code"] == "15102"
    assert result.iloc[0]["LR"] == pytest.approx(125.4)
    assert result.iloc[0]["TFT"] == pytest.approx(130.2)
    assert result.iloc[0]["EM"] == pytest.approx(129.1)


def test_log_most_recent_forecasts_pentad_missing_code_no_matching_date(tmp_path, monkeypatch):
    """Test handling of missing codes where date filter returns no matches."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    sample_date = dt.datetime(2023, 5, 25)
    older_date = dt.datetime(2023, 5, 20)

    recent_data = pd.DataFrame(
        {
            "code": ["15102"] * 4,
            "date": [sample_date] * 4,
            "pentad_in_month": [5] * 4,
            "pentad_in_year": [30] * 4,
            "forecasted_discharge": [125.4, 130.2, 128.7, 129.1],
            "model_short": ["LR", "TFT", "TIDE", "EM"],
        }
    )

    old_data = pd.DataFrame(
        {
            "code": ["15999"] * 4,
            "date": [older_date] * 4,
            "pentad_in_month": [4] * 4,
            "pentad_in_year": [29] * 4,
            "forecasted_discharge": [100.0, 110.0, 105.0, 107.5],
            "model_short": ["LR", "TFT", "TIDE", "EM"],
        }
    )

    combined_data = pd.concat([recent_data, old_data], ignore_index=True)

    result = log_most_recent_forecasts(PENTAD, combined_data)

    assert len(result) == 2, f"Expected 2 rows (1 active + 1 missing-code), got {len(result)}"
    assert "15102" in result["code"].values
    assert "15999" in result["code"].values
    row_15102 = result[result["code"] == "15102"].iloc[0]
    assert row_15102["LR"] == pytest.approx(125.4)
    assert row_15102["TFT"] == pytest.approx(130.2)
    row_15999 = result[result["code"] == "15999"].iloc[0]
    assert pd.isna(row_15999["LR"])
    assert pd.isna(row_15999["TFT"])


# ============================================================================
# Decade Function Tests
# ============================================================================


@pytest.fixture
def sample_decade_data():
    """Create sample decade forecast data for testing."""
    sample_date = dt.datetime(2023, 5, 25)
    data = pd.DataFrame(
        {
            "code": [
                "15102",
                "15124",
                "15136",
                "15102",
                "15124",
                "15136",
                "15102",
                "15124",
                "15136",
                "15102",
                "15124",
                "15136",
            ],
            "date": [sample_date] * 12,
            "decad_in_month": [3] * 12,
            "decad_in_year": [15] * 12,
            "forecasted_discharge": [
                125.4,
                45.7,
                67.8,
                130.2,
                47.2,
                65.1,
                128.7,
                46.9,
                66.5,
                129.1,
                47.0,
                66.3,
            ],
            "model_short": [
                "LR",
                "LR",
                "LR",
                "TFT",
                "TFT",
                "TFT",
                "TIDE",
                "TIDE",
                "TIDE",
                "EM",
                "EM",
                "EM",
            ],
        }
    )
    return data


def test_log_most_recent_forecasts_decade(tmp_path, monkeypatch, sample_decade_data):
    """Test log_most_recent_forecasts with decad config and sample data."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))

    result = log_most_recent_forecasts(DECAD, sample_decade_data)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3

    assert "code" in result.columns
    assert "date" in result.columns
    assert "decad_in_month" in result.columns

    for model in ["LR", "TFT", "TIDE", "EM"]:
        assert model in result.columns, f"Model column {model} missing"
    row_15102 = result[result["code"] == "15102"].iloc[0]
    assert row_15102["LR"] == pytest.approx(125.4)
    assert row_15102["TFT"] == pytest.approx(130.2)
    assert row_15102["EM"] == pytest.approx(129.1)
    sample_date = dt.datetime(2023, 5, 25)
    assert all(d == sample_date for d in result["date"])

    # Verify CSV was written
    forecast_dir = tmp_path / "forecast_logs"
    csv_files = list(forecast_dir.glob("recent_model_forecasts_decad_*.csv"))
    assert len(csv_files) == 1


def test_log_most_recent_forecasts_decade_empty_data(tmp_path, monkeypatch, sample_decade_data):
    """Test log_most_recent_forecasts with decad config and empty data."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    empty_data = pd.DataFrame(columns=sample_decade_data.columns)

    result = log_most_recent_forecasts(DECAD, empty_data)

    forecast_dir = tmp_path / "forecast_logs"
    csv_files = list(forecast_dir.glob("*.csv")) if forecast_dir.exists() else []
    assert len(csv_files) == 0
    assert result.empty


def test_log_most_recent_forecasts_decade_nat_dates(tmp_path, monkeypatch):
    """Test that log_most_recent_forecasts with decad config handles NaT dates gracefully."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    sample_date = dt.datetime(2023, 5, 25)

    valid_data = pd.DataFrame(
        {
            "code": ["15102"] * 4,
            "date": [sample_date] * 4,
            "decad_in_month": [3] * 4,
            "decad_in_year": [15] * 4,
            "forecasted_discharge": [125.4, 130.2, 128.7, 129.1],
            "model_short": ["LR", "TFT", "TIDE", "EM"],
        }
    )

    nat_data = pd.DataFrame(
        {
            "code": ["15999"] * 4,
            "date": [pd.NaT] * 4,
            "decad_in_month": [3] * 4,
            "decad_in_year": [15] * 4,
            "forecasted_discharge": [100.0, 110.0, 105.0, 107.5],
            "model_short": ["LR", "TFT", "TIDE", "EM"],
        }
    )

    combined_data = pd.concat([valid_data, nat_data], ignore_index=True)

    result = log_most_recent_forecasts(DECAD, combined_data)

    assert len(result) == 1, (
        f"Only station 15102 has valid dates, expected 1 row, got {len(result)}"
    )
    assert result.iloc[0]["code"] == "15102"
    assert result.iloc[0]["LR"] == pytest.approx(125.4)
    assert result.iloc[0]["TFT"] == pytest.approx(130.2)
    assert result.iloc[0]["EM"] == pytest.approx(129.1)


# ===================================================================
# forecast_target_date tests
# ===================================================================


class TestForecastTargetDate:
    """Tests for forecast_target_date helper."""

    def test_scalar_date(self):
        """Single date: Jan 5 -> Jan 6."""
        result = forecast_target_date(dt.date(2024, 1, 5))
        assert result == dt.date(2024, 1, 6)

    def test_series_dates(self):
        """pandas Series input produces +1 day output."""
        dates = pd.Series(pd.to_datetime(["2024-01-05", "2024-01-10"]))
        result = forecast_target_date(dates)
        expected = pd.Series(pd.to_datetime(["2024-01-06", "2024-01-11"]))
        pd.testing.assert_series_equal(result, expected)

    def test_year_boundary(self):
        """Dec 31 -> Jan 1 next year."""
        result = forecast_target_date(dt.date(2024, 12, 31))
        assert result == dt.date(2025, 1, 1)

    def test_leap_year_feb28(self):
        """Feb 28 2024 (leap year) -> Feb 29 2024."""
        result = forecast_target_date(dt.date(2024, 2, 28))
        assert result == dt.date(2024, 2, 29)

    def test_non_leap_year_feb28(self):
        """Feb 28 2023 (not leap year) -> Mar 1 2023."""
        result = forecast_target_date(dt.date(2023, 2, 28))
        assert result == dt.date(2023, 3, 1)


# ===================================================================
# Monthly logging tests
# ===================================================================


class TestLogMostRecentForecastsMonthly:
    """Tests for log_most_recent_forecasts_monthly()."""

    @pytest.fixture
    def sample_monthly_data(self):
        """Monthly joint forecast data with 2 stations, 2 models."""
        return pd.DataFrame(
            {
                "code": ["15013", "15013", "15014", "15014", "15013", "15013", "15014", "15014"],
                "year": [2023, 2023, 2023, 2023, 2024, 2024, 2024, 2024],
                "month_in_year": [6, 6, 6, 6, 6, 6, 6, 6],
                "forecasted_discharge": [100.0, 105.0, 200.0, 210.0, 102.0, 107.0, 202.0, 212.0],
                "model_short": ["GBT", "EM", "GBT", "EM", "GBT", "EM", "GBT", "EM"],
            }
        )

    def test_happy_path(self, tmp_path, monkeypatch, sample_monthly_data):
        """Returns pivoted DataFrame with correct shape and values."""
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))

        result = log_most_recent_forecasts_monthly(sample_monthly_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # 2 stations
        assert "GBT" in result.columns
        assert "EM" in result.columns

        # Verify CSV was written
        forecast_dir = tmp_path / "forecast_logs"
        csv_files = list(forecast_dir.glob("recent_model_forecasts_monthly_*.csv"))
        assert len(csv_files) == 1

        # Spot-check discharge values for 2024 (most recent year)
        row_15013 = result[result["code"] == "15013"].iloc[0]
        assert row_15013["GBT"] == pytest.approx(102.0)
        assert row_15013["EM"] == pytest.approx(107.0)
        row_15014 = result[result["code"] == "15014"].iloc[0]
        assert row_15014["GBT"] == pytest.approx(202.0)
        assert row_15014["EM"] == pytest.approx(212.0)

    def test_uses_most_recent_year_month(
        self,
        tmp_path,
        monkeypatch,
        sample_monthly_data,
    ):
        """Only the most recent (year, month) combination is used."""
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))

        result = log_most_recent_forecasts_monthly(sample_monthly_data)

        assert all(result["year"] == 2024)
        assert all(result["month_in_year"] == 6)

    def test_empty_data(self):
        """Empty DataFrame returns empty DataFrame."""
        result = log_most_recent_forecasts_monthly(pd.DataFrame())
        assert result.empty

    def test_none_data(self):
        """None input returns empty DataFrame."""
        result = log_most_recent_forecasts_monthly(None)
        assert result.empty

    def test_single_model(self, tmp_path, monkeypatch):
        """Single model still produces valid pivot."""
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))

        data = pd.DataFrame(
            {
                "code": ["15013"],
                "year": [2024],
                "month_in_year": [3],
                "forecasted_discharge": [100.0],
                "model_short": ["GBT"],
            }
        )
        result = log_most_recent_forecasts_monthly(data)

        assert len(result) == 1
        assert result.iloc[0]["GBT"] == pytest.approx(100.0)
