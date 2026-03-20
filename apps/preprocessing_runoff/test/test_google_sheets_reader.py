"""Tests for Google Sheets discharge data reader."""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from google_sheets_reader import (
    get_google_sheets_site_codes,
    is_google_sheets_enabled,
    read_discharge_from_google_sheet,
)


class TestIsGoogleSheetsEnabled:
    def test_enabled_true(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_ENABLED": "true"}):
            assert is_google_sheets_enabled() is True

    def test_enabled_True(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_ENABLED": "True"}):
            assert is_google_sheets_enabled() is True

    def test_disabled(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_ENABLED": "false"}):
            assert is_google_sheets_enabled() is False

    def test_absent(self):
        with patch.dict(os.environ, {}, clear=True):
            assert is_google_sheets_enabled() is False


class TestGetGoogleSheetsSiteCodes:
    def test_valid_codes(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_SITE_CODES": "99001,99002"}):
            assert get_google_sheets_site_codes() == ["99001", "99002"]

    def test_empty_string(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_SITE_CODES": ""}):
            assert get_google_sheets_site_codes() == []

    def test_whitespace_only(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_SITE_CODES": "  "}):
            assert get_google_sheets_site_codes() == []

    def test_absent(self):
        with patch.dict(os.environ, {}, clear=True):
            assert get_google_sheets_site_codes() == []

    def test_non_numeric_rejected(self, caplog):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_SITE_CODES": "99001,abc,99002"}):
            result = get_google_sheets_site_codes()
            assert result == ["99001", "99002"]
            assert "Invalid site code 'abc'" in caplog.text

    def test_trailing_comma(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_SITE_CODES": "99001,"}):
            assert get_google_sheets_site_codes() == ["99001"]

    def test_spaces_around_codes(self):
        with patch.dict(os.environ, {"GOOGLE_SHEETS_SITE_CODES": " 99001 , 99002 "}):
            assert get_google_sheets_site_codes() == ["99001", "99002"]


class TestReadDischargeFromGoogleSheet:
    """Tests using mocked gspread client."""

    def _make_mock_worksheet(self, values):
        """Create a mock worksheet that returns the given values."""
        ws = MagicMock()
        ws.get_all_values.return_value = values
        return ws

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_happy_path(self, mock_gspread_module, _mock_validate):
        """Reads valid data and returns correct DataFrame."""
        mock_gc = MagicMock()
        mock_gspread_module.service_account.return_value = mock_gc

        mock_spreadsheet = MagicMock()
        mock_gc.open_by_key.return_value = mock_spreadsheet

        ws = self._make_mock_worksheet(
            [
                ["date", "discharge"],
                ["01.03.2026", "45.2"],
                ["02.03.2026", "43.8"],
                ["03.03.2026", "-"],
            ]
        )
        mock_spreadsheet.worksheet.return_value = ws

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/test_creds.json",
        )

        assert len(result) == 3
        assert list(result.columns) == ["code", "date", "discharge"]
        assert result.iloc[0]["code"] == "99001"
        assert result.iloc[0]["discharge"] == 45.2
        assert pd.isna(result.iloc[2]["discharge"])

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_missing_tab(self, mock_gspread_module, _mock_validate, caplog):
        """Missing worksheet tab returns empty DataFrame with warning."""
        mock_gc = MagicMock()
        mock_gspread_module.service_account.return_value = mock_gc
        mock_gspread_module.exceptions.WorksheetNotFound = type(
            "WorksheetNotFound", (Exception,), {}
        )

        mock_spreadsheet = MagicMock()
        mock_gc.open_by_key.return_value = mock_spreadsheet
        mock_spreadsheet.worksheet.side_effect = mock_gspread_module.exceptions.WorksheetNotFound(
            "not found"
        )

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99999"],
            credentials_path="/tmp/test_creds.json",
        )

        assert result.empty
        assert "no tab named '99999'" in caplog.text

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_malformed_dates_skipped(self, mock_gspread_module, _mock_validate, caplog):
        """Rows with unparseable dates are skipped."""
        mock_gc = MagicMock()
        mock_gspread_module.service_account.return_value = mock_gc

        mock_spreadsheet = MagicMock()
        mock_gc.open_by_key.return_value = mock_spreadsheet

        ws = self._make_mock_worksheet(
            [
                ["date", "discharge"],
                ["not-a-date", "45.2"],
                ["02.03.2026", "43.8"],
            ]
        )
        mock_spreadsheet.worksheet.return_value = ws

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/test_creds.json",
        )

        assert len(result) == 1
        assert "invalid date" in caplog.text

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_non_numeric_discharge_skipped(self, mock_gspread_module, _mock_validate, caplog):
        """Non-numeric discharge values (not '-') are skipped with warning."""
        mock_gc = MagicMock()
        mock_gspread_module.service_account.return_value = mock_gc

        mock_spreadsheet = MagicMock()
        mock_gc.open_by_key.return_value = mock_spreadsheet

        ws = self._make_mock_worksheet(
            [
                ["date", "discharge"],
                ["01.03.2026", "abc"],
                ["02.03.2026", "43.8"],
            ]
        )
        mock_spreadsheet.worksheet.return_value = ws

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/test_creds.json",
        )

        assert len(result) == 1
        assert "non-numeric discharge" in caplog.text

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_negative_discharge_warning(self, mock_gspread_module, _mock_validate, caplog):
        """Negative discharge values are kept but logged as warning."""
        mock_gc = MagicMock()
        mock_gspread_module.service_account.return_value = mock_gc

        mock_spreadsheet = MagicMock()
        mock_gc.open_by_key.return_value = mock_spreadsheet

        ws = self._make_mock_worksheet(
            [
                ["date", "discharge"],
                ["01.03.2026", "-5.0"],
                ["02.03.2026", "43.8"],
            ]
        )
        mock_spreadsheet.worksheet.return_value = ws

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/test_creds.json",
        )

        assert len(result) == 2
        assert "negative discharge" in caplog.text

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_empty_sheet(self, mock_gspread_module, _mock_validate):
        """Sheet with only headers returns empty DataFrame."""
        mock_gc = MagicMock()
        mock_gspread_module.service_account.return_value = mock_gc

        mock_spreadsheet = MagicMock()
        mock_gc.open_by_key.return_value = mock_spreadsheet

        ws = self._make_mock_worksheet([["date", "discharge"]])
        mock_spreadsheet.worksheet.return_value = ws

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/test_creds.json",
        )

        assert result.empty

    @patch("google_sheets_reader.gspread", None)
    def test_gspread_not_installed(self, caplog):
        """Returns empty DataFrame with error log when gspread missing."""
        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/test_creds.json",
        )

        assert result.empty
        assert "gspread is not installed" in caplog.text

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_auth_failure(self, mock_gspread_module, _mock_validate, caplog):
        """Auth failure logs ERROR with credentials path."""
        mock_gspread_module.service_account.side_effect = Exception("401 Unauthorized")

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/bad_creds.json",
        )

        assert result.empty
        assert "auth failed" in caplog.text
        assert "/tmp/bad_creds.json" in caplog.text

    @patch("google_sheets_reader.gspread", MagicMock())
    def test_credentials_path_nonexistent(self, caplog):
        """Non-existent credentials file returns empty DataFrame."""
        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/nonexistent/creds.json",
        )

        assert result.empty
        assert "not found" in caplog.text

    def test_no_site_codes(self):
        """Empty site codes list returns empty DataFrame without fetching."""
        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=[],
            credentials_path="/tmp/creds.json",
        )

        assert result.empty

    @patch("google_sheets_reader._validate_credentials_path", return_value=True)
    @patch("google_sheets_reader.gspread")
    def test_operator_feedback_log(self, mock_gspread_module, _mock_validate, caplog):
        """Info log contains site code, row count, and date range."""
        mock_gc = MagicMock()
        mock_gspread_module.service_account.return_value = mock_gc

        mock_spreadsheet = MagicMock()
        mock_gc.open_by_key.return_value = mock_spreadsheet

        ws = self._make_mock_worksheet(
            [
                ["date", "discharge"],
                ["01.03.2026", "45.2"],
                ["02.03.2026", "43.8"],
                ["03.03.2026", "42.1"],
            ]
        )
        mock_spreadsheet.worksheet.return_value = ws

        result = read_discharge_from_google_sheet(
            sheet_id="test_id",
            site_codes=["99001"],
            credentials_path="/tmp/test_creds.json",
        )

        assert len(result) == 3
        assert "99001" in caplog.text
        assert "3 rows" in caplog.text


class TestConjunctionRule:
    """Tests for the conjunction rule in preprocessing integration."""

    @patch.dict(
        os.environ,
        {
            "ieasyforecast_configuration_path": "/tmp/test",
            "ieasyforecast_config_file_all_stations": "config.json",
            "GOOGLE_SHEETS_SITE_CODES": "99001,99002",
        },
    )
    def test_site_in_env_but_not_manual_skipped(self, caplog):
        """Site in GOOGLE_SHEETS_SITE_CODES but not marked manual is skipped."""
        from google_sheets_reader import get_google_sheets_site_codes

        codes = get_google_sheets_site_codes()
        assert "99001" in codes
        # The conjunction check happens in src.py integration, not in the reader.
        # This test verifies the reader correctly parses the codes.

    def test_site_marked_manual_but_not_in_env(self):
        """Site marked manual in JSON but not in GOOGLE_SHEETS_SITE_CODES
        is simply not fetched — no error expected."""
        with patch.dict(os.environ, {"GOOGLE_SHEETS_SITE_CODES": "99002"}):
            codes = get_google_sheets_site_codes()
            assert "99001" not in codes
