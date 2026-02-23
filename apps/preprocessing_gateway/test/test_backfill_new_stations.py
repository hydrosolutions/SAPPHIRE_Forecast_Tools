"""
Tests for backfill_new_stations.py.

Tests gap detection logic, CSV extraction, and backfill writers
with mocked API responses.
"""

import os
import sys
import tempfile
from datetime import date, timedelta
from unittest.mock import Mock, patch, MagicMock

import pandas as pd
import pytest

# Add preprocessing_gateway to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

# Mock the sapphire_dg_client module before importing
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()

import backfill_new_stations as bns


# =====================================================================
# Gap detection tests
# =====================================================================

class TestDetectMeteoGaps:
    """Tests for detect_meteo_gaps()."""

    def test_new_station_not_in_api(self):
        """Code present in CSV but not in API is detected as new."""
        csv_codes = {"ST001", "ST002"}
        recent = date.today() - timedelta(days=1)
        api_coverage = {("T", "ST001"): recent}

        new, stale = bns.detect_meteo_gaps(
            csv_codes, api_coverage, "T"
        )
        assert new == {"ST002"}
        assert stale == set()

    def test_stale_station(self):
        """Code in API with old max_date is detected as stale."""
        csv_codes = {"ST001"}
        old_date = date.today() - timedelta(days=30)
        api_coverage = {("T", "ST001"): old_date}

        new, stale = bns.detect_meteo_gaps(
            csv_codes, api_coverage, "T", staleness_days=7
        )
        assert new == set()
        assert stale == {"ST001"}

    def test_up_to_date_station_skipped(self):
        """Code with recent max_date is neither new nor stale."""
        csv_codes = {"ST001"}
        recent = date.today() - timedelta(days=2)
        api_coverage = {("T", "ST001"): recent}

        new, stale = bns.detect_meteo_gaps(
            csv_codes, api_coverage, "T", staleness_days=7
        )
        assert new == set()
        assert stale == set()

    def test_empty_csv_codes(self):
        """Empty CSV codes produce no gaps."""
        new, stale = bns.detect_meteo_gaps(set(), {}, "P")
        assert new == set()
        assert stale == set()

    def test_multiple_new_and_stale(self):
        """Correctly separates multiple new and stale codes."""
        csv_codes = {"A", "B", "C", "D"}
        old = date.today() - timedelta(days=20)
        recent = date.today() - timedelta(days=1)
        api_coverage = {
            ("P", "A"): recent,  # up to date
            ("P", "B"): old,     # stale
            # C missing → new
            ("P", "D"): old,     # stale
        }

        new, stale = bns.detect_meteo_gaps(
            csv_codes, api_coverage, "P", staleness_days=7
        )
        assert new == {"C"}
        assert stale == {"B", "D"}


class TestDetectSnowGaps:
    """Tests for detect_snow_gaps()."""

    def test_new_snow_station(self):
        csv_codes = {"HRU01"}
        api_coverage = {}

        new, stale = bns.detect_snow_gaps(
            csv_codes, api_coverage, "SWE"
        )
        assert new == {"HRU01"}
        assert stale == set()

    def test_stale_snow_station(self):
        csv_codes = {"HRU01"}
        old = date.today() - timedelta(days=30)
        api_coverage = {("SWE", "HRU01"): old}

        new, stale = bns.detect_snow_gaps(
            csv_codes, api_coverage, "SWE", staleness_days=7
        )
        assert stale == {"HRU01"}

    def test_snow_type_case_handling(self):
        """snow_type is uppercased for API lookup."""
        csv_codes = {"HRU01"}
        old = date.today() - timedelta(days=30)
        api_coverage = {("HS", "HRU01"): old}

        new, stale = bns.detect_snow_gaps(
            csv_codes, api_coverage, "hs", staleness_days=7
        )
        assert stale == {"HRU01"}


# =====================================================================
# CSV extraction tests
# =====================================================================

class TestExtractMeteoCodes:
    """Tests for extract_meteo_codes_from_csv()."""

    def test_reads_codes_from_csv(self, tmp_path):
        csv_file = tmp_path / "test_P_reanalysis.csv"
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
            "code": [12345, 67890, 12345],
            "P": [1.0, 2.0, 3.0],
        })
        df.to_csv(csv_file, index=False)

        codes = bns.extract_meteo_codes_from_csv(str(csv_file))
        assert codes == {"12345", "67890"}

    def test_missing_file_returns_empty(self):
        codes = bns.extract_meteo_codes_from_csv("/nonexistent/path.csv")
        assert codes == set()


class TestExtractSnowCodes:
    """Tests for extract_snow_codes_from_csv()."""

    def test_reads_codes_from_csv(self, tmp_path):
        csv_file = tmp_path / "test_SWE.csv"
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "code": ["HRU01"],
            "SWE": [100.0],
        })
        df.to_csv(csv_file, index=False)

        codes = bns.extract_snow_codes_from_csv(str(csv_file))
        assert codes == {"HRU01"}


# =====================================================================
# Coverage query tests
# =====================================================================

class TestGetMeteoCoverage:
    """Tests for get_meteo_coverage()."""

    @patch("backfill_new_stations.requests.get")
    def test_parses_api_response(self, mock_get):
        mock_get.return_value = Mock(
            status_code=200,
            json=lambda: [
                {
                    "meteo_type": "T",
                    "code": "ST001",
                    "min_date": "2024-01-01",
                    "max_date": "2024-06-15",
                    "record_count": 166,
                },
            ],
        )
        mock_get.return_value.raise_for_status = Mock()

        result = bns.get_meteo_coverage("http://test:8000")
        assert result == {("T", "ST001"): date(2024, 6, 15)}

    @patch("backfill_new_stations.requests.get")
    def test_returns_empty_on_error(self, mock_get):
        mock_get.side_effect = ConnectionError("refused")

        result = bns.get_meteo_coverage("http://test:8000")
        assert result == {}


class TestGetSnowCoverage:
    """Tests for get_snow_coverage()."""

    @patch("backfill_new_stations.requests.get")
    def test_parses_api_response(self, mock_get):
        mock_get.return_value = Mock(
            status_code=200,
            json=lambda: [
                {
                    "snow_type": "SWE",
                    "code": "HRU01",
                    "min_date": "2024-01-10",
                    "max_date": "2024-02-20",
                    "record_count": 42,
                },
            ],
        )
        mock_get.return_value.raise_for_status = Mock()

        result = bns.get_snow_coverage("http://test:8000")
        assert result == {("SWE", "HRU01"): date(2024, 2, 20)}


# =====================================================================
# Backfill writer tests
# =====================================================================

class TestBackfillMeteoFromCsv:
    """Tests for backfill_meteo_from_csv()."""

    def test_backfills_new_station_full_history(self, tmp_path):
        """New station (not in API) gets all CSV data written."""
        csv_file = tmp_path / "test_T_reanalysis.csv"
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "code": ["ST001", "ST001", "ST001"],
            "T": [10.0, 11.0, 12.0],
        })
        df.to_csv(csv_file, index=False)

        mock_client = Mock()
        mock_client.write_meteo.return_value = 3

        count = bns.backfill_meteo_from_csv(
            str(csv_file), "T", {"ST001"}, mock_client,
            max_date_by_code={"ST001": None},
        )
        assert count == 3
        records = mock_client.write_meteo.call_args[0][0]
        assert len(records) == 3
        assert records[0]["meteo_type"] == "T"
        assert records[0]["code"] == "ST001"
        assert records[0]["value"] == 10.0

    def test_backfills_stale_station_incrementally(self, tmp_path):
        """Stale station gets only data after its API max_date."""
        csv_file = tmp_path / "test_P_reanalysis.csv"
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "code": ["ST001", "ST001", "ST001"],
            "P": [5.0, 6.0, 7.0],
        })
        df.to_csv(csv_file, index=False)

        mock_client = Mock()
        mock_client.write_meteo.return_value = 1

        count = bns.backfill_meteo_from_csv(
            str(csv_file), "P", {"ST001"}, mock_client,
            max_date_by_code={"ST001": date(2024, 1, 2)},
        )
        assert count == 1
        records = mock_client.write_meteo.call_args[0][0]
        assert len(records) == 1
        assert records[0]["date"] == "2024-01-03"

    def test_missing_csv_returns_zero(self):
        mock_client = Mock()
        count = bns.backfill_meteo_from_csv(
            "/nonexistent.csv", "T", {"ST001"}, mock_client,
            max_date_by_code={"ST001": None},
        )
        assert count == 0
        mock_client.write_meteo.assert_not_called()

    def test_no_matching_codes_returns_zero(self, tmp_path):
        csv_file = tmp_path / "test_T_reanalysis.csv"
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "code": ["ST001"],
            "T": [10.0],
        })
        df.to_csv(csv_file, index=False)

        mock_client = Mock()
        count = bns.backfill_meteo_from_csv(
            str(csv_file), "T", {"XXXX"}, mock_client,
            max_date_by_code={"XXXX": None},
        )
        assert count == 0


class TestBackfillSnowFromCsv:
    """Tests for backfill_snow_from_csv()."""

    def test_backfills_with_elevation_bands(self, tmp_path):
        """Snow data with elevation bands is correctly mapped."""
        csv_file = tmp_path / "test_SWE.csv"
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "code": ["HRU01", "HRU01"],
            "SWE": [100.0, 120.0],
            "SWE_1": [80.0, 90.0],
            "SWE_2": [110.0, 130.0],
        })
        df.to_csv(csv_file, index=False)

        mock_client = Mock()
        mock_client.write_snow.return_value = 2

        count = bns.backfill_snow_from_csv(
            str(csv_file), "SWE", {"HRU01"}, mock_client,
            max_date_by_code={"HRU01": None},
        )
        assert count == 2
        records = mock_client.write_snow.call_args[0][0]
        assert records[0]["value"] == 100.0
        assert records[0]["value1"] == 80.0
        assert records[0]["value2"] == 110.0

    def test_incremental_backfill(self, tmp_path):
        """Only writes data after the API's max_date."""
        csv_file = tmp_path / "test_HS.csv"
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "code": ["H01", "H01", "H01"],
            "HS": [50.0, 60.0, 70.0],
        })
        df.to_csv(csv_file, index=False)

        mock_client = Mock()
        mock_client.write_snow.return_value = 2

        count = bns.backfill_snow_from_csv(
            str(csv_file), "HS", {"H01"}, mock_client,
            max_date_by_code={"H01": date(2024, 1, 1)},
        )
        assert count == 2
        records = mock_client.write_snow.call_args[0][0]
        assert len(records) == 2
        assert records[0]["date"] == "2024-01-02"
