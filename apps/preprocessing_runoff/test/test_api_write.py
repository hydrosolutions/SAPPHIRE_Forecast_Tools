"""
Tests for SAPPHIRE API write integration in preprocessing_runoff.

Tests that:
- API unavailable → returns False, CSV still works
- API disabled via env var → returns False
- Sync modes filter data correctly (operational/maintenance/initial)
- Percentile column mapping is correct for hydrograph
- Public write functions handle API errors gracefully
"""

import os
import sys
import datetime as dt
from unittest.mock import patch, MagicMock

import pandas as pd
import numpy as np
import pytest

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def runoff_data():
    """Sample daily runoff DataFrame centred on today."""
    today = pd.Timestamp.today().normalize()
    dates = pd.date_range(
        end=today, periods=40, freq="D"
    )
    rows = []
    for code in ["15101", "15102"]:
        for d in dates:
            rows.append({
                "code": code,
                "date": d,
                "discharge": round(np.random.uniform(5, 50), 3),
            })
    return pd.DataFrame(rows)


@pytest.fixture
def hydrograph_data():
    """Sample hydrograph DataFrame matching from_daily_time_series_to_hydrograph output."""
    current_year = dt.date.today().year
    previous_year = current_year - 1
    today = pd.Timestamp.today().normalize()

    rows = []
    for code in ["15101", "15102"]:
        for day_offset in range(40):
            d = today - pd.Timedelta(days=39) + pd.Timedelta(days=day_offset)
            doy = d.dayofyear
            rows.append({
                "code": code,
                "date": d,
                "day_of_year": doy,
                "count": 10,
                "mean": 25.0,
                "std": 5.0,
                "min": 10.0,
                "max": 40.0,
                "5%": 12.0,
                "25%": 20.0,
                "50%": 25.0,
                "75%": 30.0,
                "95%": 38.0,
                str(current_year): 27.0,
                str(previous_year): 23.0,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _write_runoff_to_api tests
# ---------------------------------------------------------------------------

class TestWriteRunoffToApi:
    """Tests for _write_runoff_to_api."""

    def test_returns_false_when_api_unavailable(self, runoff_data):
        """When sapphire_api_client is not installed, returns False."""
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", False):
            assert src._write_runoff_to_api(runoff_data) is False

    def test_returns_false_when_api_disabled(self, runoff_data):
        """When SAPPHIRE_API_ENABLED=false, returns False."""
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}):
            assert src._write_runoff_to_api(runoff_data) is False

    def test_returns_false_when_api_not_ready(self, runoff_data):
        """When readiness_check fails, returns False."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = False
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
             }):
            assert src._write_runoff_to_api(runoff_data) is False

    def test_returns_false_for_empty_data(self):
        """Empty DataFrame returns False."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
             }):
            empty = pd.DataFrame(columns=["code", "date", "discharge"])
            assert src._write_runoff_to_api(empty) is False

    def test_operational_mode_writes_today_only(self, runoff_data):
        """Operational sync mode only writes today's date."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_runoff.return_value = 2

        today_str = pd.Timestamp.today().normalize().strftime('%Y-%m-%d')

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            result = src._write_runoff_to_api(runoff_data)
            assert result is True
            records = mock_client.write_runoff.call_args[0][0]
            # Should only have records for today (2 stations)
            assert len(records) == 2
            dates = {r["date"] for r in records}
            assert dates == {today_str}

    def test_maintenance_mode_writes_last_30_days(self, runoff_data):
        """Maintenance sync mode writes last 30 days from today."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_runoff.return_value = 62

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "maintenance",
             }):
            result = src._write_runoff_to_api(runoff_data)
            assert result is True
            records = mock_client.write_runoff.call_args[0][0]
            # Cutoff = today - 30 days → 31 days * 2 stations = 62
            assert len(records) == 62
            assert len(records) > 2  # more than just today

    def test_initial_mode_writes_all_data(self, runoff_data):
        """Initial sync mode writes all data."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_runoff.return_value = 80

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "initial",
             }):
            result = src._write_runoff_to_api(runoff_data)
            assert result is True
            records = mock_client.write_runoff.call_args[0][0]
            # 40 days * 2 stations = 80
            assert len(records) == 80

    def test_record_format(self, runoff_data):
        """Records have the expected field names and types."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_runoff.return_value = 2

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            src._write_runoff_to_api(runoff_data)
            records = mock_client.write_runoff.call_args[0][0]
            rec = records[0]
            assert rec["horizon_type"] == "day"
            assert isinstance(rec["code"], str)
            assert isinstance(rec["date"], str)
            assert isinstance(rec["discharge"], float)
            assert isinstance(rec["horizon_value"], int)
            assert isinstance(rec["horizon_in_year"], int)

    def test_nan_discharge_becomes_none(self):
        """NaN discharge values are sent as None to the API."""
        today = pd.Timestamp.today().normalize()
        data = pd.DataFrame([{
            "code": "15101",
            "date": today,
            "discharge": np.nan,
        }])
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_runoff.return_value = 1

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            src._write_runoff_to_api(data)
            rec = mock_client.write_runoff.call_args[0][0][0]
            assert rec["discharge"] is None

    def test_string_date_input(self):
        """String dates (production path via CSV) are handled correctly."""
        today = pd.Timestamp.today().normalize()
        data = pd.DataFrame([{
            "code": "15101",
            "date": today.strftime('%Y-%m-%d'),
            "discharge": 12.5,
        }])
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_runoff.return_value = 1

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            result = src._write_runoff_to_api(data)
            assert result is True
            rec = mock_client.write_runoff.call_args[0][0][0]
            assert rec["date"] == today.strftime('%Y-%m-%d')

    def test_mode_parameter_overrides_env_var(self, runoff_data):
        """mode parameter takes precedence over SAPPHIRE_SYNC_MODE env var."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_runoff.return_value = 80

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            # Env says operational, but mode param says initial → all data
            result = src._write_runoff_to_api(runoff_data, mode="initial")
            assert result is True
            records = mock_client.write_runoff.call_args[0][0]
            assert len(records) == 80  # all 40 days × 2 stations


# ---------------------------------------------------------------------------
# _write_hydrograph_to_api tests
# ---------------------------------------------------------------------------

class TestWriteHydrographToApi:
    """Tests for _write_hydrograph_to_api."""

    def test_returns_false_when_api_unavailable(self, hydrograph_data):
        """When sapphire_api_client is not installed, returns False."""
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", False):
            assert src._write_hydrograph_to_api(hydrograph_data) is False

    def test_returns_false_when_api_disabled(self, hydrograph_data):
        """When SAPPHIRE_API_ENABLED=false, returns False."""
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}):
            assert src._write_hydrograph_to_api(hydrograph_data) is False

    def test_operational_mode_writes_today_only(self, hydrograph_data):
        """Operational mode writes only today's rows."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 2

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            result = src._write_hydrograph_to_api(hydrograph_data)
            assert result is True
            records = mock_client.write_hydrograph.call_args[0][0]
            # 2 stations, 1 day each = 2 records
            assert len(records) == 2

    def test_maintenance_mode_writes_last_30_days(self, hydrograph_data):
        """Maintenance sync mode writes last 30 days from today."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 62

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "maintenance",
             }):
            result = src._write_hydrograph_to_api(hydrograph_data)
            assert result is True
            records = mock_client.write_hydrograph.call_args[0][0]
            # Cutoff = today - 30 days → 31 days * 2 stations = 62
            assert len(records) == 62
            assert len(records) > 2  # more than just today

    def test_initial_mode_writes_all(self, hydrograph_data):
        """Initial mode writes all rows."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 80

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "initial",
             }):
            result = src._write_hydrograph_to_api(hydrograph_data)
            assert result is True
            records = mock_client.write_hydrograph.call_args[0][0]
            assert len(records) == 80

    def test_percentile_column_mapping(self, hydrograph_data):
        """Percentile columns are correctly mapped to q05..q95."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 80

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "initial",
             }):
            src._write_hydrograph_to_api(hydrograph_data)
            rec = mock_client.write_hydrograph.call_args[0][0][0]
            assert rec["q05"] == 12.0
            assert rec["q25"] == 20.0
            assert rec["q50"] == 25.0
            assert rec["q75"] == 30.0
            assert rec["q95"] == 38.0

    def test_year_column_mapping(self, hydrograph_data):
        """Year columns are mapped to current/previous."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 80

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "initial",
             }):
            src._write_hydrograph_to_api(hydrograph_data)
            rec = mock_client.write_hydrograph.call_args[0][0][0]
            assert rec["current"] == 27.0
            assert rec["previous"] == 23.0

    def test_hydrograph_record_format(self, hydrograph_data):
        """Hydrograph records have all expected fields."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 2

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            src._write_hydrograph_to_api(hydrograph_data)
            rec = mock_client.write_hydrograph.call_args[0][0][0]
            expected_keys = {
                "horizon_type", "code", "date", "day_of_year",
                "horizon_value", "horizon_in_year",
                "count", "mean", "std", "min", "max",
                "q05", "q25", "q50", "q75", "q95",
                "norm", "current", "previous",
            }
            assert set(rec.keys()) == expected_keys
            assert rec["horizon_type"] == "day"
            assert rec["norm"] == rec["q50"]

    def test_count_field_is_int(self, hydrograph_data):
        """Count field must be int, not float."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 2

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            src._write_hydrograph_to_api(hydrograph_data)
            rec = mock_client.write_hydrograph.call_args[0][0][0]
            assert isinstance(rec["count"], int)
            assert rec["count"] == 10

    def test_mode_parameter_overrides_env_var(self, hydrograph_data):
        """mode parameter takes precedence over SAPPHIRE_SYNC_MODE env var."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_hydrograph.return_value = 80

        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "SapphirePreprocessingClient",
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 "SAPPHIRE_API_ENABLED": "true",
                 "SAPPHIRE_API_URL": "http://test:8000",
                 "SAPPHIRE_SYNC_MODE": "operational",
             }):
            # Env says operational, but mode param says initial → all data
            result = src._write_hydrograph_to_api(
                hydrograph_data, mode="initial"
            )
            assert result is True
            records = mock_client.write_hydrograph.call_args[0][0]
            assert len(records) == 80  # all 40 days × 2 stations


# ---------------------------------------------------------------------------
# Integration: public write functions handle API errors gracefully
# ---------------------------------------------------------------------------

class TestPublicWriteFunctionsApiGraceful:
    """Verify public CSV write functions don't crash on API errors."""

    def test_csv_write_succeeds_despite_api_error(self, runoff_data, tmp_path):
        """write_daily_time_series_data_to_csv still writes CSV when API raises."""
        output_file = tmp_path / "daily_discharge.csv"
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "_write_runoff_to_api",
                          side_effect=Exception("API down")), \
             patch.dict(os.environ, {
                 "ieasyforecast_intermediate_data_path": str(tmp_path),
                 "ieasyforecast_daily_discharge_file": "daily_discharge.csv",
             }):
            ret = src.write_daily_time_series_data_to_csv(
                runoff_data, column_list=["code", "date", "discharge"]
            )
            assert ret is None  # CSV write success returns None
            assert output_file.exists()
            written = pd.read_csv(output_file)
            assert len(written) == len(runoff_data)

    def test_hydrograph_csv_write_succeeds_despite_api_error(
        self, hydrograph_data, tmp_path
    ):
        """write_daily_hydrograph_data_to_csv still writes CSV when API raises."""
        current_year = dt.date.today().year
        previous_year = current_year - 1
        output_file = tmp_path / "hydrograph_day.csv"
        col_list = [
            "code", "date", "day_of_year", "count", "mean", "std",
            "min", "max", "5%", "25%", "50%", "75%", "95%",
            str(current_year), str(previous_year),
        ]
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "_write_hydrograph_to_api",
                          side_effect=Exception("API down")), \
             patch.dict(os.environ, {
                 "ieasyforecast_intermediate_data_path": str(tmp_path),
                 "ieasyforecast_hydrograph_day_file": "hydrograph_day.csv",
             }):
            ret = src.write_daily_hydrograph_data_to_csv(
                hydrograph_data, column_list=col_list,
            )
            assert ret is None
            assert output_file.exists()

    def test_mode_passed_through_to_runoff_api(self, runoff_data, tmp_path):
        """write_daily_time_series_data_to_csv passes mode to _write_runoff_to_api."""
        output_file = tmp_path / "daily_discharge.csv"
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "_write_runoff_to_api",
                          return_value=True) as mock_api, \
             patch.dict(os.environ, {
                 "ieasyforecast_intermediate_data_path": str(tmp_path),
                 "ieasyforecast_daily_discharge_file": "daily_discharge.csv",
             }):
            src.write_daily_time_series_data_to_csv(
                runoff_data,
                column_list=["code", "date", "discharge"],
                mode="maintenance",
            )
            mock_api.assert_called_once()
            _, kwargs = mock_api.call_args
            assert kwargs["mode"] == "maintenance"

    def test_mode_passed_through_to_hydrograph_api(
        self, hydrograph_data, tmp_path
    ):
        """write_daily_hydrograph_data_to_csv passes mode to _write_hydrograph_to_api."""
        current_year = dt.date.today().year
        previous_year = current_year - 1
        col_list = [
            "code", "date", "day_of_year", "count", "mean", "std",
            "min", "max", "5%", "25%", "50%", "75%", "95%",
            str(current_year), str(previous_year),
        ]
        with patch.object(src, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(src, "_write_hydrograph_to_api",
                          return_value=True) as mock_api, \
             patch.dict(os.environ, {
                 "ieasyforecast_intermediate_data_path": str(tmp_path),
                 "ieasyforecast_hydrograph_day_file": "hydrograph_day.csv",
             }):
            src.write_daily_hydrograph_data_to_csv(
                hydrograph_data, column_list=col_list, mode="maintenance",
            )
            mock_api.assert_called_once()
            _, kwargs = mock_api.call_args
            assert kwargs["mode"] == "maintenance"
