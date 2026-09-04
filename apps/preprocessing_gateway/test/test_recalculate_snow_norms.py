"""
Tests for recalculate_snow_norms.py — yearly snow norm computation
and API write.

The script:
1. Checks API availability and creates client
2. Calls dg_utils.calculate_snow_norms_from_api() to compute norms from API
3. Writes full-year norms to the API, preserving existing values
"""

import math
import os
import sys
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client module before importing
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

import dg_utils
import recalculate_snow_norms as rsn


class TestRecalculateSnowNorms:
    """End-to-end tests for recalculate_snow_norms.py."""

    def test_parse_snow_vars_normalizes_case_and_dedupes(self):
        """SNOW_VARS parsing canonicalizes case, trims, and de-dupes."""
        assert rsn._parse_snow_vars("SWE,HS,RoF") == ["SWE", "HS", "ROF"]
        assert rsn._parse_snow_vars("swe,hs,rof") == ["SWE", "HS", "ROF"]
        assert rsn._parse_snow_vars("SWE,SWE,HS") == ["SWE", "HS"]
        assert rsn._parse_snow_vars("") == []
        assert rsn._parse_snow_vars(None) == []
        assert rsn._parse_snow_vars("  SWE  ,  HS  ") == ["SWE", "HS"]

    def test_json_safe_helper_handles_nan_inf_none(self):
        """_json_safe returns None for non-finite/non-numeric values."""
        assert rsn._json_safe(None) is None
        assert rsn._json_safe(math.nan) is None
        assert rsn._json_safe(float("nan")) is None
        assert rsn._json_safe(float("inf")) is None
        assert rsn._json_safe(float("-inf")) is None
        assert rsn._json_safe(0.0) == 0.0
        assert rsn._json_safe(1.5) == 1.5
        assert rsn._json_safe(42) == 42
        assert rsn._json_safe("HS") is None

    @staticmethod
    def _make_norms_df(variables, code="19999", n_days=365):
        """Build a norms DataFrame matching calculate_snow_norms_from_api output.

        Uses fake station code "19999" (real codes must never appear
        in tests).
        """
        frames = []
        for var in variables:
            frames.append(
                pd.DataFrame(
                    {
                        "snow_type": [var] * n_days,
                        "code": [code] * n_days,
                        "dayofyear": range(1, n_days + 1),
                        "norm": [50.0 + i * 0.1 for i in range(n_days)],
                    }
                )
            )
        return pd.concat(frames, ignore_index=True)

    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_happy_path_norms_written_to_api(self, mock_client_class, mock_calc_norms, tmp_path):
        """Full flow: API data available, norms computed, written to API."""

        snow_path = str(tmp_path / "snow")
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=366)

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()
        mock_client.write_snow.return_value = 366
        mock_client_class.return_value = mock_client

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_API_URL": "http://localhost:8000",
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,
            env_overrides=env,
        )

        assert result is True
        mock_client.write_snow.assert_called()
        records = mock_client.write_snow.call_args[0][0]
        assert len(records) > 0

        # Every record should have a non-None norm
        for r in records:
            assert r["norm"] is not None
            assert r["snow_type"] == "SWE"

        # Dates should span Jan 1 to Dec 31 of the target year
        dates = sorted(r["date"] for r in records)
        assert dates[0] == "2024-01-01"
        assert dates[-1] == "2024-12-31"

    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_preserves_existing_values_from_api(self, mock_client_class, mock_calc_norms, tmp_path):
        """Existing API values are preserved; only norm is added."""

        snow_path = str(tmp_path / "snow")
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=366)

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        # API returns existing record with value but no norm
        mock_client.read_snow.return_value = pd.DataFrame(
            {
                "id": [1],
                "date": pd.to_datetime(["2024-01-15"]),
                "code": ["19999"],
                "snow_type": ["SWE"],
                "value": [88.8],
                "norm": [np.nan],
            }
        )
        mock_client.write_snow.return_value = 365
        mock_client_class.return_value = mock_client

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_API_URL": "http://localhost:8000",
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,
            env_overrides=env,
        )

        assert result is True
        records = mock_client.write_snow.call_args[0][0]
        # Find the Jan 15 record
        jan15 = [r for r in records if r["date"] == "2024-01-15"]
        assert len(jan15) == 1
        # Value should be preserved from API
        assert jan15[0]["value"] == 88.8
        # Norm should be computed (not None)
        assert jan15[0]["norm"] is not None

    def test_api_unavailable_returns_false(self, tmp_path):
        """When API is unavailable, returns False gracefully."""
        snow_path = str(tmp_path / "snow")

        env = {
            "SAPPHIRE_API_ENABLED": "false",
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,
            env_overrides=env,
        )

        assert result is False

    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_empty_api_data_returns_false(self, mock_client_class, mock_calc_norms, tmp_path):
        """When API has no historical data, returns False."""
        mock_calc_norms.return_value = pd.DataFrame(
            columns=["snow_type", "code", "dayofyear", "norm"]
        )
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client_class.return_value = mock_client

        result = rsn.recalculate_norms(
            snow_path=str(tmp_path),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,
            env_overrides={
                "SAPPHIRE_API_ENABLED": "true",
                "SAPPHIRE_API_URL": "http://localhost:8000",
            },
        )

        assert result is False
        mock_client.write_snow.assert_not_called()

    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_leap_year_includes_day_366(self, mock_client_class, mock_calc_norms, tmp_path):
        """Leap year norms include day 366 (Dec 31)."""

        snow_path = str(tmp_path / "snow")
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=366)

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()
        mock_client.write_snow.return_value = 366
        mock_client_class.return_value = mock_client

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_API_URL": "http://localhost:8000",
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,  # leap year
            env_overrides=env,
        )

        assert result is True
        records = mock_client.write_snow.call_args[0][0]
        dates = sorted(r["date"] for r in records)
        assert dates[-1] == "2024-12-31"
        # 2024 is a leap year, so 366 days
        assert len(dates) == 366

    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_multiple_variables_and_codes(self, mock_client_class, mock_calc_norms, tmp_path):
        """Multiple variables and HRU codes produce norms for each."""

        snow_path = str(tmp_path / "snow")
        mock_calc_norms.return_value = self._make_norms_df(["SWE", "HS"])

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()
        mock_client.write_snow.return_value = 365
        mock_client_class.return_value = mock_client

        env = {
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_API_URL": "http://localhost:8000",
        }

        result = rsn.recalculate_norms(
            snow_path=snow_path,
            variables=["SWE", "HS"],
            hru_codes=["HRU01"],
            year=2023,
            env_overrides=env,
        )

        assert result is True
        # write_snow should be called at least twice (once per variable)
        assert mock_client.write_snow.call_count >= 2

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    @patch("setup_library.load_environment")
    def test_year_override_via_env_var(
        self,
        mock_load_environment,
        mock_client_class,
        mock_calc_norms,
        mock_calc_stats,
        monkeypatch,
    ):
        """main() honors ieasyhydroforecast_SNOW_RECALC_YEAR without live API calls."""
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)
        mock_calc_stats.return_value = pd.DataFrame(
            columns=[
                "snow_type",
                "code",
                "dayofyear",
                "count",
                "mean",
                "std",
                "min",
                "max",
                "q05",
                "q25",
                "q50",
                "q75",
                "q95",
            ]
        )

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()
        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        monkeypatch.setenv("ieasyhydroforecast_SNOW_RECALC_YEAR", "2019")
        monkeypatch.setenv("ieasyhydroforecast_HRU_SNOW_DATA", "HRU01")
        monkeypatch.setenv("ieasyhydroforecast_SNOW_VARS", "SWE")
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        monkeypatch.setenv("SAPPHIRE_API_URL", "http://localhost:8000")

        rsn.main()

        assert captured
        assert all(record["date"].startswith("2019-") for record in captured)

        monkeypatch.setenv("ieasyhydroforecast_SNOW_RECALC_YEAR", "not-a-year")
        with pytest.raises(SystemExit) as excinfo:
            rsn.main()

        assert "ieasyhydroforecast_SNOW_RECALC_YEAR must be an integer" in str(excinfo.value)


class TestRecalculateSnowStats:
    """Tests for stat-record writing in recalculate_snow_norms.py (P2)."""

    ENV = {
        "SAPPHIRE_API_ENABLED": "true",
        "SAPPHIRE_API_URL": "http://localhost:8000",
    }

    @staticmethod
    def _make_norms_df(variables, code="19999", n_days=365):
        """Build a norms DataFrame matching calculate_snow_norms_from_api output."""
        frames = []
        for var in variables:
            frames.append(
                pd.DataFrame(
                    {
                        "snow_type": [var] * n_days,
                        "code": [code] * n_days,
                        "dayofyear": range(1, n_days + 1),
                        "norm": [50.0 + i * 0.1 for i in range(n_days)],
                    }
                )
            )
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _make_stats_df(variables, code="19999", n_days=365):
        """Build a stats DataFrame matching calculate_snow_stats_from_api output."""
        frames = []
        for var in variables:
            frames.append(
                pd.DataFrame(
                    {
                        "snow_type": [var] * n_days,
                        "code": [code] * n_days,
                        "dayofyear": range(1, n_days + 1),
                        "count": [10] * n_days,
                        "mean": [40.0 + i * 0.1 for i in range(n_days)],
                        "std": [5.0] * n_days,
                        "min": [20.0] * n_days,
                        "max": [80.0] * n_days,
                        "q05": [22.0] * n_days,
                        "q25": [35.0] * n_days,
                        "q50": [40.0] * n_days,
                        "q75": [55.0] * n_days,
                        "q95": [70.0] * n_days,
                    }
                )
            )
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _make_target_year_df(snow_type, code, year, n_days=365):
        """Build a target-year snow DataFrame as read_snow would return."""
        dates = pd.date_range(f"{year}-01-01", periods=n_days, freq="D")
        return pd.DataFrame(
            {
                "id": range(1, n_days + 1),
                "snow_type": [snow_type] * n_days,
                "code": [code] * n_days,
                "date": dates,
                "value": [float(30 + i * 0.1) for i in range(n_days)],
                "norm": [float(50 + i * 0.1) for i in range(n_days)],
            }
        )

    @staticmethod
    def _make_prior_year_df(snow_type, code, year, n_days=365):
        """Build a prior-year snow DataFrame as read_snow would return."""
        dates = pd.date_range(f"{year}-01-01", periods=n_days, freq="D")
        return pd.DataFrame(
            {
                "id": range(1, n_days + 1),
                "snow_type": [snow_type] * n_days,
                "code": [code] * n_days,
                "date": dates,
                "value": [float(25 + i * 0.1) for i in range(n_days)],
                "norm": [float(50 + i * 0.1) for i in range(n_days)],
            }
        )

    def _invoke_recalc(self, mock_client, mock_calc_norms, mock_calc_stats, year=2023):
        """Helper to invoke recalculate_norms with the standard mock setup."""
        return rsn.recalculate_norms(
            snow_path="/tmp/snow",
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=year,
            env_overrides=self.ENV,
        )

    # -----------------------------------------------------------------------
    # Test 1: stat columns present in written records
    # -----------------------------------------------------------------------

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_record_builder_no_nan_in_payload(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """Written records contain no non-finite floats."""
        target_dates = pd.date_range("2024-01-01", periods=366, freq="D")
        target_values = [float(10 + i) for i in range(366)]
        target_values[0] = np.nan
        target_values[1] = float("inf")
        target_df = pd.DataFrame(
            {
                "id": range(1, 367),
                "snow_type": ["SWE"] * 366,
                "code": ["19999"] * 366,
                "date": target_dates,
                "value": target_values,
                "norm": [50.0] * 366,
                "value1": [float("-inf")] + [None] * 365,
            }
        )

        prior_dates = pd.date_range("2023-01-01", periods=365, freq="D")
        prior_df = pd.DataFrame(
            {
                "id": range(1, 366),
                "snow_type": ["SWE"] * 365,
                "code": ["19999"] * 365,
                "date": prior_dates,
                "value": [float(5 + i) for i in range(365)],
                "norm": [50.0] * 365,
            }
        )

        norms_df = self._make_norms_df(["SWE"], n_days=366)
        stats_df = self._make_stats_df(["SWE"], n_days=366)
        stats_df.loc[0, ["mean", "std", "min", "max", "q05", "q25", "q50", "q75", "q95"]] = np.nan

        mock_calc_norms.return_value = norms_df
        mock_calc_stats.return_value = stats_df

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            if start.startswith("2024"):
                return target_df
            if start.startswith("2023"):
                return prior_df
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        result = rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,
            env_overrides=self.ENV,
        )

        assert result is True
        assert len(captured) > 0
        for record in captured:
            for key, value in record.items():
                if isinstance(value, float):
                    assert math.isfinite(value), f"Record {record} key {key} is non-finite: {value}"

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_record_builder_includes_snow_stat_columns(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """Written records include all 12 new stat fields with correct values."""
        target_df = self._make_target_year_df("SWE", "19999", 2023)
        prior_df = self._make_prior_year_df("SWE", "19999", 2022)
        stats_df = self._make_stats_df(["SWE"])
        norms_df = self._make_norms_df(["SWE"])

        mock_calc_norms.return_value = norms_df
        mock_calc_stats.return_value = stats_df

        captured = []

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            if start.startswith("2023"):
                return target_df
            elif start.startswith("2022"):
                return prior_df
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        result = rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2023,
            env_overrides=self.ENV,
        )

        assert result is True
        assert len(captured) > 0

        stat_keys = {
            "count",
            "mean",
            "std",
            "min",
            "max",
            "q05",
            "q25",
            "q50",
            "q75",
            "q95",
            "previous",
            "current",
        }
        for record in captured:
            for key in stat_keys:
                assert key in record, f"Record missing key '{key}': {record.keys()}"

        # Spot-check: mean value from stats_df matches what was written (DOY 1 → index 0)
        jan1_records = [r for r in captured if r["date"] == "2023-01-01"]
        assert len(jan1_records) == 1
        jan1 = jan1_records[0]
        assert abs(jan1["mean"] - 40.0) < 1e-6

    # -----------------------------------------------------------------------
    # Test 2: previous uses calendar-date alignment
    # -----------------------------------------------------------------------

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_record_builder_previous_uses_calendar_date_alignment(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """previous is calendar-date aligned; 2024-02-29 has None previous (no 2023-02-29)."""
        # 2024 is a leap year (366 days); 2023 is not (365 days)
        target_dates = pd.date_range("2024-01-01", periods=366, freq="D")
        target_df = pd.DataFrame(
            {
                "id": range(1, 367),
                "snow_type": ["SWE"] * 366,
                "code": ["19999"] * 366,
                "date": target_dates,
                "value": [float(10 + i) for i in range(366)],
                "norm": [50.0] * 366,
            }
        )

        prior_dates = pd.date_range("2023-01-01", periods=365, freq="D")
        prior_df = pd.DataFrame(
            {
                "id": range(1, 366),
                "snow_type": ["SWE"] * 365,
                "code": ["19999"] * 365,
                "date": prior_dates,
                "value": [float(5 + i) for i in range(365)],
                "norm": [50.0] * 365,
            }
        )

        norms_df = self._make_norms_df(["SWE"], n_days=366)
        stats_df = self._make_stats_df(["SWE"], n_days=366)

        mock_calc_norms.return_value = norms_df
        mock_calc_stats.return_value = stats_df

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            if start.startswith("2024"):
                return target_df
            elif start.startswith("2023"):
                return prior_df
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        result = rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,
            env_overrides=self.ENV,
        )

        assert result is True
        assert len(captured) > 0

        # 2024-03-01 → prior date is 2023-03-01 (valid, index 59 in prior_df)
        mar1_records = [r for r in captured if r["date"] == "2024-03-01"]
        assert len(mar1_records) == 1
        # prior_df value for 2023-03-01 (day index 59, 0-based): 5 + 59 = 64.0
        assert abs(mar1_records[0]["previous"] - 64.0) < 1e-6, (
            f"Expected previous=64.0, got {mar1_records[0]['previous']}"
        )

        # 2024-02-29 → prior date 2023-02-29 is INVALID (2023 is not a leap year)
        feb29_records = [r for r in captured if r["date"] == "2024-02-29"]
        assert len(feb29_records) == 1
        assert feb29_records[0]["previous"] is None

    # -----------------------------------------------------------------------
    # Test 3: current equals target-year value
    # -----------------------------------------------------------------------

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_record_builder_writes_current_from_target_year_value(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """current equals the target-year row's own value field."""
        dates = pd.date_range("2024-01-01", periods=366, freq="D")
        target_df = pd.DataFrame(
            {
                "id": range(1, 367),
                "snow_type": ["SWE"] * 366,
                "code": ["19999"] * 366,
                "date": dates,
                "value": [float(i) for i in range(366)],
                "norm": [50.0] * 366,
            }
        )
        # June 15 is day index 166 (0-based) in 2024 = value 166.0
        june15_value = 166.0

        norms_df = self._make_norms_df(["SWE"], n_days=366)
        stats_df = self._make_stats_df(["SWE"], n_days=366)

        mock_calc_norms.return_value = norms_df
        mock_calc_stats.return_value = stats_df

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            if start.startswith("2024"):
                return target_df
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2024,
            env_overrides=self.ENV,
        )

        june15_records = [r for r in captured if r["date"] == "2024-06-15"]
        assert len(june15_records) == 1
        assert abs(june15_records[0]["current"] - june15_value) < 1e-6, (
            f"Expected current={june15_value}, got {june15_records[0]['current']}"
        )

    # -----------------------------------------------------------------------
    # Test 4: idempotency — two runs produce identical record sequences
    # -----------------------------------------------------------------------

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_recalculate_stats_is_idempotent_on_rerun(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """Running recalc twice produces identical record dicts both times."""
        target_df = self._make_target_year_df("SWE", "19999", 2023)
        prior_df = self._make_prior_year_df("SWE", "19999", 2022)
        norms_df = self._make_norms_df(["SWE"])
        stats_df = self._make_stats_df(["SWE"])

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            if start.startswith("2023"):
                return target_df.copy()
            elif start.startswith("2022"):
                return prior_df.copy()
            return pd.DataFrame()

        def make_client():
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_snow.side_effect = read_snow_side_effect
            return mock_client

        mock_calc_norms.return_value = norms_df
        mock_calc_stats.return_value = stats_df

        run1_captured = []
        run2_captured = []

        # Run 1
        client1 = make_client()
        client1.write_snow.side_effect = lambda records: (
            run1_captured.extend(records) or len(records)
        )
        mock_client_class.return_value = client1

        rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2023,
            env_overrides=self.ENV,
        )

        # Run 2
        client2 = make_client()
        client2.write_snow.side_effect = lambda records: (
            run2_captured.extend(records) or len(records)
        )
        mock_client_class.return_value = client2

        rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2023,
            env_overrides=self.ENV,
        )

        assert len(run1_captured) > 0
        assert len(run1_captured) == len(run2_captured), (
            f"Run 1 wrote {len(run1_captured)} records, run 2 wrote {len(run2_captured)}"
        )

        sorted1 = sorted(run1_captured, key=lambda r: (r["snow_type"], r["code"], r["date"]))
        sorted2 = sorted(run2_captured, key=lambda r: (r["snow_type"], r["code"], r["date"]))

        for i, (r1, r2) in enumerate(zip(sorted1, sorted2, strict=True)):
            assert r1.keys() == r2.keys(), f"Record {i} key mismatch: {r1.keys()} vs {r2.keys()}"
            for key in r1:
                v1, v2 = r1[key], r2[key]
                if isinstance(v1, float) and isinstance(v2, float):
                    if v1 != v1 and v2 != v2:  # both NaN
                        continue
                    assert abs(v1 - v2) < 1e-9, f"Record {i}, key '{key}': {v1} != {v2}"
                else:
                    assert v1 == v2, f"Record {i}, key '{key}': {v1} != {v2}"

    # -----------------------------------------------------------------------
    # Test 5: preserve existing value and band fields
    # -----------------------------------------------------------------------

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_preserves_existing_value_and_band_fields_when_writing_stats(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """Existing value and value1..value14 are preserved alongside new stat fields."""
        # Build target-year df with band values on one row
        dates = pd.date_range("2023-01-01", periods=365, freq="D")
        base_df = pd.DataFrame(
            {
                "id": range(1, 366),
                "snow_type": ["SWE"] * 365,
                "code": ["19999"] * 365,
                "date": dates,
                "value": [float(30 + i) for i in range(365)],
                "norm": [50.0] * 365,
            }
        )
        # Add band columns value1..value14 for the first row
        for i in range(1, 15):
            base_df[f"value{i}"] = [float(i * 10) if j == 0 else None for j in range(365)]

        norms_df = self._make_norms_df(["SWE"])
        stats_df = self._make_stats_df(["SWE"])

        mock_calc_norms.return_value = norms_df
        mock_calc_stats.return_value = stats_df

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            if start.startswith("2023"):
                return base_df.copy()
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2023,
            env_overrides=self.ENV,
        )

        assert len(captured) > 0

        # The first record (2023-01-01) should have value=30.0 and all bands preserved
        jan1 = [r for r in captured if r["date"] == "2023-01-01"]
        assert len(jan1) == 1
        r = jan1[0]
        assert abs(r["value"] - 30.0) < 1e-6, f"value changed: {r['value']}"
        for i in range(1, 15):
            expected_band = float(i * 10)
            assert f"value{i}" in r, f"Missing band key 'value{i}'"
            assert abs(r[f"value{i}"] - expected_band) < 1e-6, (
                f"value{i} changed: {r[f'value{i}']} != {expected_band}"
            )

        # Stat fields must also be present
        stat_keys = {
            "count",
            "mean",
            "std",
            "min",
            "max",
            "q05",
            "q25",
            "q50",
            "q75",
            "q95",
            "previous",
            "current",
        }
        for key in stat_keys:
            assert key in r, f"Stat key '{key}' missing from record"

    # -----------------------------------------------------------------------
    # Test 6: per-station error isolation
    # -----------------------------------------------------------------------

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_station_write_error_does_not_abort_other_stations(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path, caplog
    ):
        """A write failure for one station is isolated; other stations still get written."""
        import logging

        # Three stations: "19999", "19998", "19997"
        codes = ["19999", "19998", "19997"]
        n_days = 10  # small to keep test fast

        norms_rows = []
        stats_rows = []
        for code in codes:
            for doy in range(1, n_days + 1):
                norms_rows.append(
                    {"snow_type": "SWE", "code": code, "dayofyear": doy, "norm": 50.0}
                )
                stats_rows.append(
                    {
                        "snow_type": "SWE",
                        "code": code,
                        "dayofyear": doy,
                        "count": 10,
                        "mean": 40.0,
                        "std": 5.0,
                        "min": 20.0,
                        "max": 80.0,
                        "q05": 22.0,
                        "q25": 35.0,
                        "q50": 40.0,
                        "q75": 55.0,
                        "q95": 70.0,
                    }
                )

        mock_calc_norms.return_value = pd.DataFrame(norms_rows)
        mock_calc_stats.return_value = pd.DataFrame(stats_rows)

        def read_snow_side_effect(**kwargs):
            code_filter = kwargs.get("code")
            start = kwargs.get("start_date", "")
            if code_filter is None:
                return pd.DataFrame()
            year = int(start[:4]) if start else 2023
            dates = pd.date_range(f"{year}-01-01", periods=n_days, freq="D")
            return pd.DataFrame(
                {
                    "id": range(1, n_days + 1),
                    "snow_type": ["SWE"] * n_days,
                    "code": [code_filter] * n_days,
                    "date": dates,
                    "value": [float(i) for i in range(n_days)],
                    "norm": [50.0] * n_days,
                }
            )

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        written_codes = []

        def write_snow_side_effect(records):
            code_in_records = records[0]["code"] if records else None
            if code_in_records == "19998":
                raise RuntimeError("simulated write failure")
            written_codes.append(code_in_records)
            return len(records)

        mock_client.write_snow.side_effect = write_snow_side_effect
        mock_client_class.return_value = mock_client

        with caplog.at_level(logging.WARNING):
            rsn.recalculate_norms(
                snow_path=str(tmp_path / "snow"),
                variables=["SWE"],
                hru_codes=["HRU01"],
                year=2023,
                env_overrides=self.ENV,
            )

        # The run must not propagate the exception
        # (result may be True or False, but no exception raised)

        # "19999" and "19997" should have been written
        assert "19999" in written_codes, f"Expected '19999' in {written_codes}"
        assert "19997" in written_codes, f"Expected '19997' in {written_codes}"

        # A warning must have been logged for the failed station
        assert any("19998" in record.message for record in caplog.records), (
            f"No warning mentioning '19998' found in logs: {[r.message for r in caplog.records]}"
        )


class TestRecalculateSnowNormsPreservationReadFailures:
    """PREPG-020: each of the three preservation reads in
    recalculate_snow_norms.py — target-year, prior-year, and
    statistics history — must abort the code/type it guards rather
    than let the write proceed with the fields it would have
    preserved nulled. See
    doc/plans/issues/archive/high_prio_gi_draft_prepg_snow_preservation_read_fails_open.md.

    This is distinct from ``test_station_write_error_does_not_abort_other_stations``
    above, which is about a *write* failure (isolated per station, run
    continues) — these tests are about *read* failures (abort, no
    write for the affected code/type at all).
    """

    ENV = {
        "SAPPHIRE_API_ENABLED": "true",
        "SAPPHIRE_API_URL": "http://localhost:8000",
    }

    @staticmethod
    def _make_norms_df(variables, code="19999", n_days=365):
        frames = []
        for var in variables:
            frames.append(
                pd.DataFrame(
                    {
                        "snow_type": [var] * n_days,
                        "code": [code] * n_days,
                        "dayofyear": range(1, n_days + 1),
                        "norm": [50.0] * n_days,
                    }
                )
            )
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _empty_stats_df():
        return pd.DataFrame(
            columns=[
                "snow_type",
                "code",
                "dayofyear",
                "count",
                "mean",
                "std",
                "min",
                "max",
                "q05",
                "q25",
                "q50",
                "q75",
                "q95",
            ]
        )

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_target_year_read_failure_aborts_without_write(
        self, mock_client_class, mock_calc_norms, mock_calc_stats
    ):
        """Target-year read (call site ~:182) raising must propagate
        and skip the write, not null value/current/bands for the
        whole year."""
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)
        mock_calc_stats.return_value = self._empty_stats_df()

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            if start.startswith("2023"):
                raise Exception("API read error (target year)")
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect
        mock_client_class.return_value = mock_client

        with pytest.raises(dg_utils.SnowPreservationReadError):
            rsn.recalculate_norms(
                snow_path="/tmp/snow",
                variables=["SWE"],
                hru_codes=["HRU01"],
                year=2023,
                env_overrides=self.ENV,
            )

        mock_client.write_snow.assert_not_called()

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_prior_year_read_failure_aborts_without_write(
        self, mock_client_class, mock_calc_norms, mock_calc_stats
    ):
        """Prior-year read (call site ~:207) raising must propagate
        and skip the write, not null 'previous' for the whole year."""
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)
        mock_calc_stats.return_value = self._empty_stats_df()

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            # Target-year (2023) read succeeds (empty); prior-year
            # (2022) read raises.
            if start.startswith("2022"):
                raise Exception("API read error (prior year)")
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect
        mock_client_class.return_value = mock_client

        with pytest.raises(dg_utils.SnowPreservationReadError):
            rsn.recalculate_norms(
                snow_path="/tmp/snow",
                variables=["SWE"],
                hru_codes=["HRU01"],
                year=2023,
                env_overrides=self.ENV,
            )

        mock_client.write_snow.assert_not_called()

    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_stats_read_failure_aborts_without_write(self, mock_client_class, mock_calc_norms):
        """Statistics-history read (dg_utils.py call site ~:737)
        raising must propagate and skip the write, not fall back to
        writing null count/mean/std/min/max/q* across the whole year.

        Drives the REAL ``dg_utils.calculate_snow_stats_from_api`` —
        it is deliberately not mocked here — via a raising
        ``client.read_snow``, so this test exercises the production
        except-clause at dg_utils.py directly. A prior version of this
        test mocked ``calculate_snow_stats_from_api`` itself and
        injected an already-wrapped ``SnowPreservationReadError``,
        which stayed green even if the production handler were
        reverted to "log a warning and continue" — see PREPG-020
        review finding 1.

        The ``read_snow`` failure is scoped to the stats-history
        pagination call specifically (identified by the *absence* of
        a ``code`` kwarg — only ``calculate_snow_stats_from_api``
        calls ``read_snow`` without one; the target-year/prior-year
        reads inside ``_recalculate_norms_impl`` always pass one).
        Making *every* ``read_snow`` call raise would still pass this
        test even with the stats handler reverted, because the
        target-year read (fixed separately, in
        recalculate_snow_norms.py) would raise first — the very "test
        that cannot fail" shape this fix is for. Confirmed by
        reverting the dg_utils.py stats handler locally with this
        exact scoping and observing this test fail (no exception
        raised, stats silently degraded to NaN and the write went
        through); restored immediately after.
        """
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)

        def read_snow_side_effect(**kwargs):
            if "code" not in kwargs:
                # calculate_snow_stats_from_api's pagination read —
                # the one this test targets.
                raise Exception("API read error (statistics)")
            # Target-year / prior-year reads (always pass `code`)
            # succeed with nothing stored.
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect
        mock_client_class.return_value = mock_client

        with pytest.raises(dg_utils.SnowPreservationReadError):
            rsn.recalculate_norms(
                snow_path="/tmp/snow",
                variables=["SWE"],
                hru_codes=["HRU01"],
                year=2023,
                env_overrides=self.ENV,
            )

        mock_client.write_snow.assert_not_called()

    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    @patch("setup_library.load_environment")
    def test_read_failure_propagates_uncaught_through_main(
        self,
        mock_load_environment,
        mock_client_class,
        mock_calc_norms,
        monkeypatch,
    ):
        """End-to-end: main() does not catch the abort either.

        recalculate_norms() only wraps the impl in try/finally (env
        restoration), never except — so an uncaught
        SnowPreservationReadError reaches main() and, from there, the
        top of the process. That is what gives the script (and the
        Docker container running it, and — after the shell wrapper
        fix — bin/yearly_snow_norm_recalculation.sh) a non-zero exit
        status instead of the old "logged warning, exit 0" behaviour.

        As in ``test_stats_read_failure_aborts_without_write`` above,
        ``calculate_snow_stats_from_api`` is not mocked — the failure
        is driven through the real function via a raising
        ``client.read_snow``, scoped to the ``code``-less pagination
        call so a reverted stats handler is not masked by the
        separately-fixed target-year read.
        """
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)

        def read_snow_side_effect(**kwargs):
            if "code" not in kwargs:
                raise Exception("API read error (statistics)")
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect
        mock_client_class.return_value = mock_client

        monkeypatch.setenv("ieasyhydroforecast_SNOW_RECALC_YEAR", "2023")
        monkeypatch.setenv("ieasyhydroforecast_HRU_SNOW_DATA", "HRU01")
        monkeypatch.setenv("ieasyhydroforecast_SNOW_VARS", "SWE")
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        monkeypatch.setenv("SAPPHIRE_API_URL", "http://localhost:8000")

        with pytest.raises(dg_utils.SnowPreservationReadError):
            rsn.main()

        mock_client.write_snow.assert_not_called()

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_widened_prior_range_read_failure_aborts_without_write(
        self, mock_client_class, mock_calc_norms, mock_calc_stats
    ):
        """PREPG-022: with an extended (year+1) display window, a
        failure on the widened prior-range read (now covering
        {range_start.year-1}-01-01 .. {range_end.year-1}-12-31, not
        just a single fixed prior year) must still raise
        SnowPreservationReadError and write nothing for that
        code/type."""
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)
        mock_calc_stats.return_value = self._empty_stats_df()

        def read_snow_side_effect(**kwargs):
            start = kwargs.get("start_date", "")
            # Target-range (2026-01-01..2027-08-31) read succeeds
            # (empty); the widened prior-range read
            # (2025-01-01..2026-12-31) raises.
            if start == "2025-01-01":
                raise Exception("API read error (widened prior range)")
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect
        mock_client_class.return_value = mock_client

        with pytest.raises(dg_utils.SnowPreservationReadError):
            rsn.recalculate_norms(
                snow_path="/tmp/snow",
                variables=["SWE"],
                hru_codes=["HRU01"],
                year=2026,
                env_overrides=self.ENV,
                display_start_month=9,
                display_start_day=1,
            )

        mock_client.write_snow.assert_not_called()


class TestSnowRecordRange:
    """Unit tests for ``_snow_record_range`` (PREPG-022).

    Expected day counts are computed independently via
    ``datetime.date`` subtraction, not re-derived from the
    implementation under test.
    """

    def test_default_start_gives_plain_calendar_year(self):
        """(1, 1) is the byte-identical-to-before calendar-year case."""
        start, end = rsn._snow_record_range(2026, 1, 1)
        assert start == pd.Timestamp("2026-01-01")
        assert end == pd.Timestamp("2026-12-31")
        assert (end - start).days + 1 == 365

    def test_sep1_start_gives_extended_range_into_next_year(self):
        """(9, 1) -> N-01-01 .. N+1-08-31 (608 days for a non-leap N)."""
        start, end = rsn._snow_record_range(2026, 9, 1)
        assert start == pd.Timestamp("2026-01-01")
        assert end == pd.Timestamp("2027-08-31")
        assert (end - start).days + 1 == 608

    def test_oct1_start_gives_extended_range_into_next_year(self):
        """(10, 1) -> N-01-01 .. N+1-09-30 (638 days for a non-leap N)."""
        start, end = rsn._snow_record_range(2026, 10, 1)
        assert start == pd.Timestamp("2026-01-01")
        assert end == pd.Timestamp("2027-09-30")
        assert (end - start).days + 1 == 638

    def test_leap_target_year_calendar_case_has_366_days(self):
        """Leap ``year`` with (1, 1) still yields the calendar-year 366."""
        start, end = rsn._snow_record_range(2024, 1, 1)
        assert start == pd.Timestamp("2024-01-01")
        assert end == pd.Timestamp("2024-12-31")
        assert (end - start).days + 1 == 366

    def test_leap_target_year_extended_range_counts_its_own_feb29(self):
        """Leap ``year`` (2024) with (9, 1): the Feb 29 inside ``year``
        itself is counted once (609 days: 366 in 2024 + 243 in 2025
        Jan-Aug)."""
        start, end = rsn._snow_record_range(2024, 9, 1)
        assert start == pd.Timestamp("2024-01-01")
        assert end == pd.Timestamp("2025-08-31")
        assert (end - start).days + 1 == 609

    def test_leap_year_plus_one_extended_range_counts_its_feb29(self):
        """Non-leap ``year`` (2027) with (9, 1): the Feb 29 inside
        ``year + 1`` (2028, leap) is counted once (609 days: 365 in
        2027 + 244 in 2028 Jan-Aug)."""
        start, end = rsn._snow_record_range(2027, 9, 1)
        assert start == pd.Timestamp("2027-01-01")
        assert end == pd.Timestamp("2028-08-31")
        assert (end - start).days + 1 == 609


class TestSnowDisplayWindowEnvParsing:
    """``main()`` parses ``ieasyhydroforecast_SNOW_DISPLAY_START_MMDD``
    with the same tolerant "MM-DD" semantics as
    ``apps/forecast_dashboard/dashboard/config.py`` (PREPG-022):
    absent, invalid/unparseable, or 02-29 all fall back to (1, 1).
    """

    def _run_main_and_capture_display_start(self, mock_recalc, monkeypatch, mmdd):
        monkeypatch.setenv("ieasyhydroforecast_HRU_SNOW_DATA", "HRU01")
        monkeypatch.setenv("ieasyhydroforecast_SNOW_VARS", "SWE")
        if mmdd is None:
            monkeypatch.delenv("ieasyhydroforecast_SNOW_DISPLAY_START_MMDD", raising=False)
        else:
            monkeypatch.setenv("ieasyhydroforecast_SNOW_DISPLAY_START_MMDD", mmdd)
        mock_recalc.return_value = True

        rsn.main()

        _, kwargs = mock_recalc.call_args
        return kwargs["display_start_month"], kwargs["display_start_day"]

    @patch("recalculate_snow_norms.recalculate_norms")
    @patch("setup_library.load_environment")
    def test_absent_env_falls_back_to_jan1(self, mock_load_env, mock_recalc, monkeypatch):
        assert self._run_main_and_capture_display_start(mock_recalc, monkeypatch, None) == (1, 1)

    @patch("recalculate_snow_norms.recalculate_norms")
    @patch("setup_library.load_environment")
    def test_invalid_env_falls_back_to_jan1(self, mock_load_env, mock_recalc, monkeypatch):
        result = self._run_main_and_capture_display_start(mock_recalc, monkeypatch, "not-a-date")
        assert result == (1, 1)

    @patch("recalculate_snow_norms.recalculate_norms")
    @patch("setup_library.load_environment")
    def test_out_of_range_env_falls_back_to_jan1(self, mock_load_env, mock_recalc, monkeypatch):
        """month=13 is out of range -> date() raises ValueError -> (1, 1)."""
        result = self._run_main_and_capture_display_start(mock_recalc, monkeypatch, "13-40")
        assert result == (1, 1)

    @patch("recalculate_snow_norms.recalculate_norms")
    @patch("setup_library.load_environment")
    def test_feb29_env_falls_back_to_jan1(self, mock_load_env, mock_recalc, monkeypatch):
        result = self._run_main_and_capture_display_start(mock_recalc, monkeypatch, "02-29")
        assert result == (1, 1)

    @patch("recalculate_snow_norms.recalculate_norms")
    @patch("setup_library.load_environment")
    def test_sep1_env_parses_correctly(self, mock_load_env, mock_recalc, monkeypatch):
        result = self._run_main_and_capture_display_start(mock_recalc, monkeypatch, "09-01")
        assert result == (9, 1)


class TestSnowNormsExtendedDisplayWindow:
    """PREPG-022: the hydrological-window write range, the widened
    target-range preservation read, and the per-date ``previous``
    alignment across the extended range.
    """

    ENV = {
        "SAPPHIRE_API_ENABLED": "true",
        "SAPPHIRE_API_URL": "http://localhost:8000",
    }

    @staticmethod
    def _make_norms_df(variables, code="19999", n_days=365):
        frames = []
        for var in variables:
            frames.append(
                pd.DataFrame(
                    {
                        "snow_type": [var] * n_days,
                        "code": [code] * n_days,
                        "dayofyear": range(1, n_days + 1),
                        "norm": [50.0] * n_days,
                    }
                )
            )
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _empty_stats_df():
        return pd.DataFrame(
            columns=[
                "snow_type",
                "code",
                "dayofyear",
                "count",
                "mean",
                "std",
                "min",
                "max",
                "q05",
                "q25",
                "q50",
                "q75",
                "q95",
            ]
        )

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_preservation_read_covers_dates_past_31_december(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """The important regression: with a (9, 1) display window and
        year=2026, the write range extends to 2027-08-31. The
        target-range preservation read must cover that whole span, not
        stop at 2026-12-31 -- otherwise every date past 31 December
        gets its value/current/band fields nulled on write. Stored
        value/current/value3 for 2027-03-15 must be preserved.

        Mutation check performed: temporarily reverted the read's
        ``end_str`` back to the old hardcoded ``f"{year}-12-31"``
        (instead of ``range_end.strftime(...)``) and re-ran this test
        -- it failed (the mock's ``read_snow_side_effect`` below
        requires ``end_date == "2027-08-31"`` exactly, so the
        mutated, narrower ``end_date`` no longer matched and the mock
        returned an empty DataFrame instead of ``target_row``; the
        written record for 2027-03-15 came back with value=None,
        current=None, and no value3 key). Reverted immediately after
        confirming the failure. (An earlier version of this test only
        matched on ``start_date`` and did not actually exercise
        ``end_str`` -- it stayed green under the same mutation. This
        is the corrected, end-date-gated version.)
        """
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)
        mock_calc_stats.return_value = self._empty_stats_df()

        target_row = pd.DataFrame(
            {
                "id": [1],
                "snow_type": ["SWE"],
                "code": ["19999"],
                "date": pd.to_datetime(["2027-03-15"]),
                "value": [77.7],
                "current": [77.7],
                "value3": [33.3],
            }
        )

        def read_snow_side_effect(**kwargs):
            # Gate on BOTH bounds: a mutation that narrows end_date
            # back to "{year}-12-31" (missing the 2027-03-15 row
            # entirely) must not still match here.
            start = kwargs.get("start_date", "")
            end = kwargs.get("end_date", "")
            if start == "2026-01-01" and end == "2027-08-31":
                return target_row
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        result = rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2026,
            env_overrides=self.ENV,
            display_start_month=9,
            display_start_day=1,
        )

        assert result is True
        mar15 = [r for r in captured if r["date"] == "2027-03-15"]
        assert len(mar15) == 1
        assert mar15[0]["value"] == 77.7
        assert mar15[0]["current"] == 77.7
        assert mar15[0]["value3"] == 33.3

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_previous_for_year_plus_one_date_resolves_against_year(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """For a date in year+1 (2027-03-15, within the extended
        window for year=2026), `previous` must resolve against
        `year`'s (2026) same-calendar-date value -- not `year - 1`'s
        (2025) -- confirming the lookup is keyed off dt.year - 1.

        Mutation check performed: temporarily reverted
        ``prior_end_str`` back to the old hardcoded
        ``f"{year - 1}-12-31"`` (instead of
        ``f"{range_end.year - 1}-12-31"``) and re-ran this test -- it
        failed (``read_snow_side_effect`` below requires
        ``end_date == "2026-12-31"`` exactly, so the mutated, narrower
        ``end_date`` of ``"2025-12-31"`` no longer matched and the
        mock returned an empty DataFrame instead of
        ``prior_range_df``; ``previous`` for 2027-03-15 came back
        ``None`` instead of ``55.5``). Reverted immediately after
        confirming the failure. (An earlier version of this test only
        matched on ``start_date`` and did not actually exercise
        ``prior_end_str`` -- it stayed green under the same mutation.
        This is the corrected, end-date-gated version.)
        """
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)
        mock_calc_stats.return_value = self._empty_stats_df()

        # Single prior-range read (2025-01-01..2026-12-31) returns
        # both years' data at the same calendar date, 2025-03-15 and
        # 2026-03-15, with distinguishable values.
        prior_range_df = pd.DataFrame(
            {
                "id": [1, 2],
                "snow_type": ["SWE", "SWE"],
                "code": ["19999", "19999"],
                "date": pd.to_datetime(["2025-03-15", "2026-03-15"]),
                "value": [11.1, 55.5],
            }
        )

        def read_snow_side_effect(**kwargs):
            # Gate on BOTH bounds: a mutation that narrows end_date
            # back to "{year - 1}-12-31" (missing the 2026-03-15 row
            # entirely) must not still match here.
            start = kwargs.get("start_date", "")
            end = kwargs.get("end_date", "")
            if start == "2025-01-01" and end == "2026-12-31":
                return prior_range_df
            return pd.DataFrame()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.side_effect = read_snow_side_effect

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2026,
            env_overrides=self.ENV,
            display_start_month=9,
            display_start_day=1,
        )

        mar15_2027 = [r for r in captured if r["date"] == "2027-03-15"]
        assert len(mar15_2027) == 1
        assert abs(mar15_2027[0]["previous"] - 55.5) < 1e-6

        # Explicitly assert the prior-range read used the widened
        # bounds (start pinned to range_start.year - 1, end widened
        # to range_end.year - 1), not the old fixed year - 1 range.
        prior_calls = [
            call
            for call in mock_client.read_snow.call_args_list
            if call.kwargs.get("start_date") == "2025-01-01"
        ]
        assert len(prior_calls) >= 1
        assert all(call.kwargs.get("end_date") == "2026-12-31" for call in prior_calls)

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_previous_none_for_feb29_shaped_miss_in_extended_range(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """previous is None, not an exception, when dt.year - 1 has no
        Feb 29 -- including for a date in the extended (year+1)
        portion of the range. year=2027 (not leap); the (9, 1) window
        extends into 2028 (leap), so 2028-02-29 is written, but
        2027-02-29 does not exist."""
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=366)
        mock_calc_stats.return_value = self._empty_stats_df()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2027,
            env_overrides=self.ENV,
            display_start_month=9,
            display_start_day=1,
        )

        feb29_records = [r for r in captured if r["date"] == "2028-02-29"]
        assert len(feb29_records) == 1
        assert feb29_records[0]["previous"] is None

    @patch("dg_utils.calculate_snow_stats_from_api")
    @patch("dg_utils.calculate_snow_norms_from_api")
    @patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
    @patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
    def test_written_range_ends_at_season_boundary(
        self, mock_client_class, mock_calc_norms, mock_calc_stats, tmp_path
    ):
        """For (9, 1)/year=2026 the write range must stop at
        2027-08-31 -- no record exists for 2027-09-01 or later
        (correctness constraint 3: do not pre-write into next
        season)."""
        mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=365)
        mock_calc_stats.return_value = self._empty_stats_df()

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.read_snow.return_value = pd.DataFrame()

        captured = []

        def capture_write(records):
            captured.extend(records)
            return len(records)

        mock_client.write_snow.side_effect = capture_write
        mock_client_class.return_value = mock_client

        rsn.recalculate_norms(
            snow_path=str(tmp_path / "snow"),
            variables=["SWE"],
            hru_codes=["HRU01"],
            year=2026,
            env_overrides=self.ENV,
            display_start_month=9,
            display_start_day=1,
        )

        dates = sorted(r["date"] for r in captured)
        assert dates[0] == "2026-01-01"
        assert dates[-1] == "2027-08-31"
        assert "2027-09-01" not in dates
        assert all(not d.startswith("2027-09") for d in dates)
        assert len(dates) == 608
