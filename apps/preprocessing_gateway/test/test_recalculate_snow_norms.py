"""
Tests for recalculate_snow_norms.py — yearly snow norm computation
and API write.

The script:
1. Checks API availability and creates client
2. Calls dg_utils.calculate_snow_norms_from_api() to compute norms from API
3. Writes full-year norms to the API, preserving existing values
"""

import os
import sys
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd

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
        """previous is calendar-date aligned; 2024-02-29 has NaN previous (no 2023-02-29)."""
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
        assert feb29_records[0]["previous"] is None or (
            isinstance(feb29_records[0]["previous"], float)
            and feb29_records[0]["previous"] != feb29_records[0]["previous"]  # NaN check
        ), f"Expected NaN/None for previous, got {feb29_records[0]['previous']}"

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
