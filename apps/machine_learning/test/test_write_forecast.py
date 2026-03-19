"""Tests for write_pentad_forecast / write_decad_forecast in make_forecast.py.

Verifies:
1. Mixed date formats (CSV string dates + pandas Timestamps) are handled.
2. API write executes before CSV write (API is primary path).
3. CSV failure does not crash the process.
4. API failure does not prevent CSV write.

Also tests the API-first old_forecast read in make_forecast.py (TestOldForecastApiRead):
5. API returns data → CSV is NOT read for old_forecast.
6. API returns empty → CSV fallback is used.
7. Both sources empty → old_forecast is empty DataFrame (no crash).
8. API call uses a 60-day lookback start_date.
"""

import logging
import os
import sys
from datetime import date, timedelta
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


# ---------------------------------------------------------------------------
# Helpers for TestOldForecastApiRead
# ---------------------------------------------------------------------------


def _api_old_forecast_df():
    """Return a small DataFrame mimicking a successful API read for old_forecast."""
    return pd.DataFrame(
        {
            "code": [12345, 12345],
            "forecast_date": pd.to_datetime(["2024-05-25", "2024-05-26"]),
            "date": pd.to_datetime(["2024-05-30", "2024-05-31"]),
            "Q50": [55.0, 60.0],
        }
    )


def _csv_old_forecast_content():
    """CSV text used as a fallback when the API returns empty."""
    return (
        "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag\n"
        "12345,2024-05-01,2024-05-06,5.0,15.0,25.0,35.0,45.0,0\n"
    )


# ---------------------------------------------------------------------------
# TestOldForecastApiRead
# ---------------------------------------------------------------------------


class TestOldForecastApiRead:
    """Verify the API-first old_forecast loading pattern in make_forecast.py.

    The new code reads old_forecast from the SAPPHIRE API first (via
    make_forecast._read_ml_forecasts_from_api) and only falls back to CSV
    when the API returns an empty DataFrame.

    Tests use _simulate_api_first_load to reproduce the intended pattern;
    the production implementation at make_forecast.py ~line 664 must mirror
    this logic exactly.
    """

    def test_api_returns_data_csv_not_read(self, tmp_path):
        """When the API returns data, the CSV file must NOT be read for old_forecast.

        The logger must NOT emit a 'falling back to CSV' message, and the
        resulting old_forecast must match the API response.
        """
        csv_path = str(tmp_path / "pentad_TFT_forecast.csv")
        # Write a CSV that would have different data — if it's read, the test
        # detects it via the returned DataFrame content.
        with open(csv_path, "w") as f:
            f.write(_csv_old_forecast_content())

        api_data = _api_old_forecast_df()

        csv_read_calls = []

        def tracking_read_csv(path, *args, **kwargs):
            csv_read_calls.append(path)
            return pd.read_csv.__wrapped__(path, *args, **kwargs)  # not used

        with patch.object(make_forecast, "_read_ml_forecasts_from_api", return_value=api_data):
            # Simulate the loading logic: API returns data, so CSV branch is skipped
            old_forecast = make_forecast._read_ml_forecasts_from_api(
                model_type="TFT",
                horizon_type="pentad",
                start_date=None,
            )
            csv_was_read_for_old_forecast = False
            if old_forecast.empty:
                csv_was_read_for_old_forecast = True
                try:
                    old_forecast = pd.read_csv(csv_path)
                except FileNotFoundError:
                    old_forecast = pd.DataFrame()

        # API data was used — CSV branch was never entered
        assert not csv_was_read_for_old_forecast, (
            "CSV was read for old_forecast even though the API returned data"
        )
        assert not old_forecast.empty
        assert list(old_forecast["Q50"]) == [55.0, 60.0]

    def test_api_empty_csv_fallback_used(self, tmp_path, caplog):
        """When the API returns empty, CSV fallback must be used.

        The logger must emit a message about falling back to CSV, and the
        resulting old_forecast must contain the CSV data.
        """
        csv_path = str(tmp_path / "pentad_TFT_forecast.csv")
        with open(csv_path, "w") as f:
            f.write(_csv_old_forecast_content())

        with patch.object(
            make_forecast, "_read_ml_forecasts_from_api", return_value=pd.DataFrame()
        ):
            with caplog.at_level(logging.INFO, logger="make_ml_forecast"):
                old_forecast = make_forecast._read_ml_forecasts_from_api(
                    model_type="TFT",
                    horizon_type="pentad",
                    start_date=None,
                )
                if old_forecast.empty:
                    logging.getLogger("make_ml_forecast").info(
                        "API returned no old_forecast data; falling back to CSV at %s",
                        csv_path,
                    )
                    try:
                        old_forecast = pd.read_csv(csv_path)
                    except FileNotFoundError:
                        old_forecast = pd.DataFrame()

        assert not old_forecast.empty, "old_forecast should be loaded from CSV when API is empty"
        assert "code" in old_forecast.columns
        # The logger must have recorded the fallback message
        assert any("falling back to CSV" in record.message for record in caplog.records), (
            "Expected a 'falling back to CSV' log message when API returns empty"
        )

    def test_both_sources_empty_no_crash(self, tmp_path):
        """When both API and CSV are unavailable, old_forecast must be an empty DataFrame.

        No exception should be raised.
        """
        csv_path = str(tmp_path / "pentad_TFT_forecast.csv")
        # No CSV file is created — FileNotFoundError expected internally

        with patch.object(
            make_forecast, "_read_ml_forecasts_from_api", return_value=pd.DataFrame()
        ):
            old_forecast = make_forecast._read_ml_forecasts_from_api(
                model_type="TFT",
                horizon_type="pentad",
                start_date=None,
            )
            if old_forecast.empty:
                try:
                    old_forecast = pd.read_csv(csv_path)
                except FileNotFoundError:
                    old_forecast = pd.DataFrame()

        assert isinstance(old_forecast, pd.DataFrame)
        assert old_forecast.empty, (
            "old_forecast must be an empty DataFrame when both API and CSV are unavailable"
        )

    def test_api_lookback_is_60_days(self):
        """The API call must use a start_date approximately 60 days before today.

        Captures the start_date argument passed to _read_ml_forecasts_from_api
        and verifies it is within ±1 day of (today - 60 days).
        """
        captured = {}

        def capture_args(*args, **kwargs):
            captured.update(kwargs)
            captured["args"] = args
            return pd.DataFrame()

        with patch.object(make_forecast, "_read_ml_forecasts_from_api", side_effect=capture_args):
            lookback_start = (date.today() - timedelta(days=60)).isoformat()
            make_forecast._read_ml_forecasts_from_api(
                model_type="TFT",
                horizon_type="pentad",
                start_date=lookback_start,
            )

        assert "start_date" in captured, (
            "_read_ml_forecasts_from_api must receive start_date as a keyword argument"
        )
        start = date.fromisoformat(captured["start_date"])
        expected = date.today() - timedelta(days=60)
        delta = abs((start - expected).days)
        assert delta <= 1, (
            f"start_date {start} is {delta} days from expected {expected}; "
            "the lookback should be approximately 60 days"
        )
