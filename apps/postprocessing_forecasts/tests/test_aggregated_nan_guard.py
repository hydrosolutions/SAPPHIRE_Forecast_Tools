"""Tests for NaN guard in _write_aggregated_forecasts_to_api().

Verifies that rows with NaN values in period-identifier columns
(year/quarter_in_year for quarterly, season_year for seasonal) are
dropped before writing to the API, with a WARNING logged for the
dropped count and sample codes.
"""

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.api_writer import (
    _write_quarterly_ensemble_to_api,
    _write_seasonal_ensemble_to_api,
)

# ===================================================================
# Quarterly NaN guard
# ===================================================================


class TestAggregatedNanGuardQuarterly:
    """NaN guard tests for quarterly ensemble API writer."""

    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_long_forecasts.return_value = 2

    def _call_with_mock(self, func, data):
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch(
                "src.api_writer._get_postprocessing_client",
                return_value=self.mock_client,
            ),
        ):
            return func(data)

    def _make_quarterly_data(self):
        """Return a baseline 3-row quarterly DataFrame."""
        return pd.DataFrame(
            {
                "code": ["12345", "NAN_STATION", "67890"],
                "year": [2025, 2025, 2025],
                "quarter_in_year": [2, 2, 2],
                "model_short": ["EM", "EM", "EM"],
                "forecasted_discharge": [100.0, 200.0, 300.0],
                "composition": ["GBT,LR", "GBT,LR", "GBT,LR"],
            }
        )

    def test_nan_year_only(self, caplog):
        """Row with NaN year is dropped; remaining 2 rows are written."""
        data = self._make_quarterly_data()
        data.loc[1, "year"] = np.nan  # NAN_STATION row

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_quarterly_ensemble_to_api, data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 2
        codes_written = {r["code"] for r in records}
        assert "NAN_STATION" not in codes_written

    def test_nan_quarter_only(self, caplog):
        """Row with NaN quarter_in_year is dropped; WARNING logged."""
        data = self._make_quarterly_data()
        data.loc[1, "quarter_in_year"] = np.nan  # NAN_STATION row

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_quarterly_ensemble_to_api, data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 2
        assert "Dropped" in caplog.text
        assert "1" in caplog.text

    def test_all_valid_no_warning(self, caplog):
        """All valid rows — no WARNING emitted and all 3 rows written."""
        data = pd.DataFrame(
            {
                "code": ["12345", "12345", "67890"],
                "year": [2025, 2025, 2025],
                "quarter_in_year": [2, 2, 2],
                "model_short": ["EM", "EM", "EM"],
                "forecasted_discharge": [100.0, 200.0, 300.0],
                "composition": ["GBT,LR", "GBT,LR", "GBT,LR"],
            }
        )
        self.mock_client.write_long_forecasts.return_value = 3

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_quarterly_ensemble_to_api, data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 3
        assert "Dropped" not in caplog.text

    def test_all_nan_returns_false(self, caplog):
        """All rows have NaN year — function returns False and skips write."""
        data = pd.DataFrame(
            {
                "code": ["12345", "67890"],
                "year": [np.nan, np.nan],
                "quarter_in_year": [2, 2],
                "model_short": ["EM", "EM"],
                "forecasted_discharge": [100.0, 200.0],
                "composition": ["GBT,LR", "GBT,LR"],
            }
        )

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_quarterly_ensemble_to_api, data)

        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    def test_nan_forecasted_discharge_not_dropped(self, caplog):
        """NaN forecasted_discharge does NOT trigger the guard — row is kept."""
        data = pd.DataFrame(
            {
                "code": ["12345", "NAN_STATION", "67890"],
                "year": [2025, 2025, 2025],
                "quarter_in_year": [2, 2, 2],
                "model_short": ["EM", "EM", "EM"],
                "forecasted_discharge": [100.0, np.nan, 300.0],
                "composition": ["GBT,LR", "GBT,LR", "GBT,LR"],
            }
        )
        self.mock_client.write_long_forecasts.return_value = 3

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_quarterly_ensemble_to_api, data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 3
        # The NaN-discharge row is kept but its q field is None
        nan_record = next(r for r in records if r["code"] == "NAN_STATION")
        assert nan_record["q"] is None
        assert "Dropped" not in caplog.text


# ===================================================================
# Seasonal NaN guard
# ===================================================================


class TestAggregatedNanGuardSeasonal:
    """NaN guard tests for seasonal ensemble API writer."""

    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_long_forecasts.return_value = 2

    def _call_with_mock(self, func, data):
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch(
                "src.api_writer._get_postprocessing_client",
                return_value=self.mock_client,
            ),
        ):
            return func(data)

    def _make_seasonal_data(self):
        """Return a baseline 3-row seasonal DataFrame."""
        return pd.DataFrame(
            {
                "code": ["12345", "NAN_STATION", "67890"],
                "season_year": [2025, 2025, 2025],
                "season_in_year": [1, 1, 1],
                "model_short": ["EM", "EM", "EM"],
                "forecasted_discharge": [100.0, 200.0, 300.0],
                "composition": ["GBT,LR", "GBT,LR", "GBT,LR"],
            }
        )

    def test_nan_season_year(self, caplog):
        """Row with NaN season_year is dropped; WARNING logged."""
        data = self._make_seasonal_data()
        data.loc[1, "season_year"] = np.nan  # NAN_STATION row

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_seasonal_ensemble_to_api, data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 2
        assert "Dropped" in caplog.text

    def test_all_valid_no_warning(self, caplog):
        """All valid rows — no WARNING emitted and all 3 rows written."""
        data = pd.DataFrame(
            {
                "code": ["12345", "12345", "67890"],
                "season_year": [2025, 2025, 2025],
                "season_in_year": [1, 1, 1],
                "model_short": ["EM", "EM", "EM"],
                "forecasted_discharge": [100.0, 200.0, 300.0],
                "composition": ["GBT,LR", "GBT,LR", "GBT,LR"],
            }
        )
        self.mock_client.write_long_forecasts.return_value = 3

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_seasonal_ensemble_to_api, data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 3
        assert "Dropped" not in caplog.text

    def test_missing_season_year_column_uses_year(self, caplog):
        """DataFrame with no season_year column falls back to year for the guard."""
        data = pd.DataFrame(
            {
                "code": ["12345", "NAN_STATION", "67890"],
                "year": [2025, np.nan, 2025],
                "season_in_year": [1, 1, 1],
                "model_short": ["EM", "EM", "EM"],
                "forecasted_discharge": [100.0, 200.0, 300.0],
                "composition": ["GBT,LR", "GBT,LR", "GBT,LR"],
            }
        )

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            result = self._call_with_mock(_write_seasonal_ensemble_to_api, data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 2
        codes_written = {r["code"] for r in records}
        assert "NAN_STATION" not in codes_written
        assert "Dropped" in caplog.text
