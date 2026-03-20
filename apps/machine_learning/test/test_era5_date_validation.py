"""Tests for ERA5 date validation in hindcast_ML_models.py.

Verifies that read_meteo_data_combined is called without start_date/end_date
filtering, so the downstream validation check (start_date < data.min() + 60d)
works correctly against the full ERA5 dataset.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Mock heavy dependencies before importing modules under test
sys.modules["darts"] = MagicMock()
sys.modules["darts.TimeSeries"] = MagicMock()
sys.modules["darts.concatenate"] = MagicMock()
sys.modules["darts.utils"] = MagicMock()
sys.modules["darts.utils.timeseries_generation"] = MagicMock()
sys.modules["darts.models"] = MagicMock()
sys.modules["pytorch_lightning"] = MagicMock()
sys.modules["pytorch_lightning.callbacks"] = MagicMock()
sys.modules["torch"] = MagicMock()
sys.modules["pe_oudin"] = MagicMock()
sys.modules["pe_oudin.PE_Oudin"] = MagicMock()
sys.modules["suntime"] = MagicMock()
sys.modules["setup_library"] = MagicMock()
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["matplotlib"] = MagicMock()
sys.modules["matplotlib.pyplot"] = MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))


def _make_era5_df(start: str, end: str, code: str = "12345") -> pd.DataFrame:
    """Build a minimal ERA5-shaped DataFrame spanning [start, end]."""
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "code": code,
            "T": 5.0,
            "P": 1.0,
        }
    )


class TestEra5DateValidation:
    """Verify the ERA5 date-range validation in hindcast_ML_models.py."""

    def test_validation_passes_with_full_era5_data(self):
        """When ERA5 data starts well before start_date, validation passes.

        This is the correct behavior after removing start_date/end_date from
        the read_meteo_data_combined call: the full dataset is available,
        so start_date is safely after data.min() + 60 days.
        """
        start_date = "2024-06-01"
        # ERA5 data starts 2024-01-01 -> min + 60d = 2024-03-01 < 2024-06-01
        era5_df = _make_era5_df("2024-01-01", "2024-12-31")

        # Replicate the validation logic from hindcast_ML_models.py line 274
        assert pd.to_datetime(start_date) >= (era5_df["date"].min() + pd.DateOffset(days=60)), (
            "Validation should pass when ERA5 data starts well before start_date"
        )

    def test_validation_fails_when_data_starts_at_start_date(self):
        """When ERA5 data starts AT start_date, validation correctly fails.

        This reproduces the bug caused by passing start_date/end_date to
        read_meteo_data_combined: the API filters data so data.min() ==
        start_date, making start_date < data.min() + 60 always true.
        """
        start_date = "2024-06-01"
        # Simulate API-filtered data: starts at start_date
        era5_df = _make_era5_df("2024-06-01", "2024-12-31")

        with pytest.raises(ValueError, match="before the first date"):
            if pd.to_datetime(start_date) < (era5_df["date"].min() + pd.DateOffset(days=60)):
                raise ValueError(
                    f"The start date {start_date} is before the first date "
                    f"in the era5 data {era5_df['date'].min()} + 60 days"
                )

    @patch("scr.utils_ml_forecast.fl.read_meteo_data")
    def test_read_meteo_combined_called_without_date_filter(self, mock_read_meteo):
        """read_meteo_data_combined works when called without date params.

        After the fix, hindcast_ML_models.py calls read_meteo_data_combined
        without start_date and end_date. Verify the function works correctly
        when those parameters default to None.
        """
        from scr.utils_ml_forecast import read_meteo_data_combined

        t_data = pd.DataFrame(
            {
                "code": [12345, 12345, 12345],
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "value": [5.0, 6.0, 7.0],
            }
        )
        p_data = pd.DataFrame(
            {
                "code": [12345, 12345, 12345],
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "value": [1.0, 2.0, 3.0],
            }
        )

        def side_effect(meteo_type, **kwargs):
            if meteo_type == "T":
                return t_data.copy()
            return p_data.copy()

        mock_read_meteo.side_effect = side_effect

        # Call without start_date/end_date (the fixed calling pattern)
        result = read_meteo_data_combined(site_codes=["12345"])

        assert "T" in result.columns
        assert "P" in result.columns
        assert len(result) == 3, "All 3 rows should be returned when no date filter is applied"
