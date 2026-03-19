"""Tests for write_pentad_forecast / write_decad_forecast in make_forecast.py.

Verifies:
1. Mixed date formats (CSV string dates + pandas Timestamps) are handled.
2. API write executes before CSV write (API is primary path).
3. CSV failure does not crash the process.
4. API failure does not prevent CSV write.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

# ---------------------------------------------------------------------------
# Mock heavy dependencies before importing make_forecast
# ---------------------------------------------------------------------------
sys.modules["darts"] = MagicMock()
sys.modules["darts.TimeSeries"] = MagicMock()
sys.modules["darts.concatenate"] = MagicMock()
sys.modules["darts.utils"] = MagicMock()
sys.modules["darts.utils.timeseries_generation"] = MagicMock()
sys.modules["darts.utils.likelihood_models"] = MagicMock()
sys.modules["darts.utils.likelihood_models.base"] = MagicMock()
sys.modules["darts.models"] = MagicMock()
sys.modules["pytorch_lightning"] = MagicMock()
sys.modules["pytorch_lightning.callbacks"] = MagicMock()
sys.modules["torch"] = MagicMock()
sys.modules["torch.optim"] = MagicMock()
sys.modules["torch.optim.lr_scheduler"] = MagicMock()
sys.modules["torch.nn"] = MagicMock()
sys.modules["torch.nn.modules"] = MagicMock()
sys.modules["torch.nn.modules.loss"] = MagicMock()
sys.modules["torch.serialization"] = MagicMock()
sys.modules["torchmetrics"] = MagicMock()
sys.modules["torchmetrics.collections"] = MagicMock()
sys.modules["pe_oudin"] = MagicMock()
sys.modules["pe_oudin.PE_Oudin"] = MagicMock()
sys.modules["suntime"] = MagicMock()
sys.modules["matplotlib"] = MagicMock()
sys.modules["matplotlib.pyplot"] = MagicMock()

_mock_sl = MagicMock()
_mock_sl.load_environment = MagicMock()
sys.modules["setup_library"] = _mock_sl
sys.modules["forecast_library"] = MagicMock()

# Add paths so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"),
)

import make_forecast  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _new_forecast_df():
    """Build a forecast DataFrame with pandas Timestamps (new data)."""
    return pd.DataFrame(
        {
            "code": [12345, 12345, 12345],
            "forecast_date": pd.to_datetime(["2024-06-01", "2024-06-01", "2024-06-01"]),
            "date": pd.to_datetime(["2024-06-02", "2024-06-03", "2024-06-04"]),
            "Q5": [10.0, 11.0, 12.0],
            "Q25": [20.0, 21.0, 22.0],
            "Q50": [30.0, 31.0, 32.0],
            "Q75": [40.0, 41.0, 42.0],
            "Q95": [50.0, 51.0, 52.0],
            "flag": [0, 0, 0],
        }
    )


def _old_csv_content():
    """CSV text with plain string dates (simulating old CSV on disk)."""
    return (
        "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag\n"
        "12345,2024-03-19,2024-03-20,5.0,15.0,25.0,35.0,45.0,0\n"
        "12345,2024-03-19,2024-03-21,6.0,16.0,26.0,36.0,46.0,0\n"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWriteDecadForecast:
    """Tests for write_decad_forecast."""

    def test_mixed_date_formats_handled(self, tmp_path):
        """Old CSV with string dates + new Timestamps must not crash."""
        out_dir = str(tmp_path)
        csv_path = os.path.join(out_dir, "decad_TFT_forecast.csv")
        with open(csv_path, "w") as f:
            f.write(_old_csv_content())

        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        # Old (2 rows) + new (3 rows) = 5 rows (no duplicate keys)
        assert len(result) == 5

    def test_api_write_attempted_before_csv(self, tmp_path):
        """API write must be attempted even if CSV would fail."""
        out_dir = str(tmp_path)
        mock_api_write = MagicMock()
        mock_consistency = MagicMock()

        new_data = _new_forecast_df()

        # Make CSV read raise an unexpected error to verify API
        # still gets called (no CSV file exists, so it falls back to
        # empty DataFrame — instead, we patch read_csv to explode)
        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                mock_api_write,
            ),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                mock_consistency,
            ),
            patch("pandas.read_csv", side_effect=RuntimeError("disk fail")),
        ):
            # Should not raise — both API and CSV errors are caught
            make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        # API write was called (primary path succeeded)
        mock_api_write.assert_called_once_with(new_data, "decade", "TFT")

    def test_api_failure_does_not_block_csv(self, tmp_path):
        """If API write fails, CSV should still be written."""
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                side_effect=RuntimeError("API down"),
            ),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                MagicMock(),
            ),
        ):
            make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        csv_path = os.path.join(out_dir, "decad_TFT_forecast.csv")
        assert os.path.exists(csv_path)
        result = pd.read_csv(csv_path)
        assert len(result) == 3

    def test_no_existing_csv(self, tmp_path):
        """Works when no prior CSV exists on disk."""
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        csv_path = os.path.join(out_dir, "decad_TFT_forecast.csv")
        result = pd.read_csv(csv_path)
        assert len(result) == 3


class TestWritePentadForecast:
    """Tests for write_pentad_forecast."""

    def test_mixed_date_formats_handled(self, tmp_path):
        """Old CSV with string dates + new Timestamps must not crash."""
        out_dir = str(tmp_path)
        csv_path = os.path.join(out_dir, "pentad_TFT_forecast.csv")
        with open(csv_path, "w") as f:
            f.write(_old_csv_content())

        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        assert len(result) == 5

    def test_api_write_attempted_before_csv(self, tmp_path):
        """API write must be attempted even if CSV would fail."""
        out_dir = str(tmp_path)
        mock_api_write = MagicMock()
        mock_consistency = MagicMock()
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                mock_api_write,
            ),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                mock_consistency,
            ),
            patch("pandas.read_csv", side_effect=RuntimeError("disk fail")),
        ):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        mock_api_write.assert_called_once_with(new_data, "pentad", "TFT")

    def test_csv_output_has_only_canonical_columns(self, tmp_path):
        """When the old CSV contains API-only columns, the output must strip them.

        If a corrupted/legacy CSV on disk carries extra columns like
        ``horizon_type`` or ``model_type``, those must not propagate into
        the combined CSV that write_pentad_forecast() writes.
        """
        import os as _os
        import sys

        sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), "..", "scr"))
        from utils_ml_forecast import ML_CANONICAL_CSV_COLUMNS

        out_dir = str(tmp_path)
        csv_path = _os.path.join(out_dir, "pentad_TFT_forecast.csv")

        # Old CSV has API-only extra columns (simulating a corrupted archive)
        corrupted_csv = (
            "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag,horizon_type,model_type,id\n"
            "12345,2024-03-19,2024-03-20,5.0,15.0,25.0,35.0,45.0,0,day,TFT,1\n"
            "12345,2024-03-19,2024-03-21,6.0,16.0,26.0,36.0,46.0,0,day,TFT,2\n"
        )
        with open(csv_path, "w") as f:
            f.write(corrupted_csv)

        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        api_only = {
            "horizon_type",
            "model_type",
            "id",
            "model_type_description",
            "composition",
            "horizon_value",
            "horizon_in_year",
        }
        leaked = api_only & set(result.columns)
        assert not leaked, f"API-only columns leaked into output CSV: {leaked}"
        non_canonical = set(result.columns) - set(ML_CANONICAL_CSV_COLUMNS)
        assert not non_canonical, f"Non-canonical columns in output CSV: {non_canonical}"

    def test_deduplication_keeps_latest(self, tmp_path):
        """When old and new data share keys, the latest value wins."""
        out_dir = str(tmp_path)
        csv_path = os.path.join(out_dir, "pentad_TFT_forecast.csv")

        # Old CSV has Q50=25.0 for 2024-03-20
        old_csv = (
            "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag\n"
            "12345,2024-06-01,2024-06-02,5.0,15.0,25.0,35.0,45.0,0\n"
        )
        with open(csv_path, "w") as f:
            f.write(old_csv)

        new_data = _new_forecast_df()
        # new_data has Q50=30.0 for the same (forecast_date, date, code) key

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        # The duplicate row should keep the new value (30.0)
        row = result[
            (result["code"] == 12345) & (result["date"].astype(str).str.startswith("2024-06-02"))
        ]
        assert len(row) == 1
        assert row.iloc[0]["Q50"] == 30.0
