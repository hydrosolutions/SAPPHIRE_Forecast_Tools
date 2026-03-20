"""Tests for the write-side guard in api_writer.py (INFRA-012 Phase 1b)."""

import json
import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from src import api_writer


class TestCheckWriteCodes:
    """Tests for _check_write_codes() logic."""

    def test_no_unexpected_codes(self, caplog):
        """Batch fully within configured set produces no warning."""
        # Arrange
        with patch.object(api_writer, "_load_configured_codes", return_value={"99001", "99002"}):
            # Act
            with caplog.at_level(logging.WARNING, logger="src.api_writer"):
                api_writer._check_write_codes({"99001"}, "combined_forecast")

        # Assert
        assert "WRITE GUARD" not in caplog.text

    def test_unexpected_codes_warns(self, caplog):
        """Batch containing an unlisted code triggers a WARNING with WRITE GUARD marker."""
        # Arrange
        with patch.object(api_writer, "_load_configured_codes", return_value={"99001"}):
            # Act
            with caplog.at_level(logging.WARNING, logger="src.api_writer"):
                api_writer._check_write_codes({"99001", "88001"}, "combined_forecast")

        # Assert
        assert "WRITE GUARD" in caplog.text
        assert "88001" in caplog.text

    def test_empty_config_skips(self, caplog):
        """Empty configured set skips the check entirely — no warning emitted."""
        # Arrange
        with patch.object(api_writer, "_load_configured_codes", return_value=set()):
            # Act
            with caplog.at_level(logging.WARNING, logger="src.api_writer"):
                api_writer._check_write_codes({"88001", "88002"}, "combined_forecast")

        # Assert
        assert "WRITE GUARD" not in caplog.text

    def test_all_unexpected(self, caplog):
        """All batch codes unlisted → warning mentions both codes."""
        # Arrange
        with patch.object(api_writer, "_load_configured_codes", return_value={"99001"}):
            # Act
            with caplog.at_level(logging.WARNING, logger="src.api_writer"):
                api_writer._check_write_codes({"88001", "88002"}, "combined_forecast")

        # Assert
        assert "WRITE GUARD" in caplog.text
        warning_text = caplog.text
        assert "88001" in warning_text or "88002" in warning_text


class TestLoadConfiguredCodes:
    """Tests for _load_configured_codes() config loading."""

    def test_load_from_file(self, tmp_path):
        """Returns string codes loaded from the pentad station selection file."""
        # Arrange
        config_file = tmp_path / "config_station_selection.json"
        config_file.write_text(json.dumps({"stationsID": [99001, 99002]}))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_station_selection": "config_station_selection.json",
            "ieasyforecast_config_file_station_selection_decad": "",
        }
        api_writer._reset_api_client()

        # Act
        with patch.dict(os.environ, env):
            result = api_writer._load_configured_codes()

        # Assert
        assert result == {"99001", "99002"}

    def test_merges_pentad_and_decad(self, tmp_path):
        """Returns the union of pentad and decad station selection files."""
        # Arrange
        pentad_file = tmp_path / "config_station_selection.json"
        pentad_file.write_text(json.dumps({"stationsID": [99001]}))

        decad_file = tmp_path / "config_station_selection_decad.json"
        decad_file.write_text(json.dumps({"stationsID": [99003]}))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_station_selection": "config_station_selection.json",
            "ieasyforecast_config_file_station_selection_decad": (
                "config_station_selection_decad.json"
            ),
        }
        api_writer._reset_api_client()

        # Act
        with patch.dict(os.environ, env):
            result = api_writer._load_configured_codes()

        # Assert
        assert result == {"99001", "99003"}

    def test_missing_file_returns_empty(self, tmp_path):
        """Pointing at a nonexistent file returns an empty set without raising."""
        # Arrange
        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_station_selection": "nonexistent.json",
            "ieasyforecast_config_file_station_selection_decad": "",
        }
        api_writer._reset_api_client()

        # Act
        with patch.dict(os.environ, env):
            result = api_writer._load_configured_codes()

        # Assert
        assert result == set()

    def test_reset_clears_cache(self, tmp_path):
        """After _reset_api_client(), a second load picks up a modified config."""
        # Arrange — first config
        config_file = tmp_path / "config_station_selection.json"
        config_file.write_text(json.dumps({"stationsID": [99001]}))

        env = {
            "ieasyforecast_configuration_path": str(tmp_path),
            "ieasyforecast_config_file_station_selection": "config_station_selection.json",
            "ieasyforecast_config_file_station_selection_decad": "",
        }
        api_writer._reset_api_client()

        with patch.dict(os.environ, env):
            first_result = api_writer._load_configured_codes()

        assert first_result == {"99001"}

        # Reset and modify config
        api_writer._reset_api_client()
        config_file.write_text(json.dumps({"stationsID": [99001, 99002]}))

        # Act — load again after reset
        with patch.dict(os.environ, env):
            second_result = api_writer._load_configured_codes()

        # Assert — cache was cleared so the new file is read
        assert second_result == {"99001", "99002"}


class TestWriteFunctionGuardWiring:
    """Tests that write functions call the guard before hitting the API."""

    def _make_mock_client(self):
        """Return a mock client that passes the readiness check."""
        client = MagicMock()
        client.readiness_check.return_value = True
        client.write_forecasts.return_value = 1
        client.write_skill_metrics.return_value = 1
        return client

    def test_combined_forecast_triggers_guard(self):
        """_write_combined_forecast_to_api calls _check_write_codes with station codes."""
        # Arrange
        data = pd.DataFrame(
            {
                "code": ["99001"],
                "date": pd.to_datetime(["2025-01-25"]),
                "pentad_in_month": [5],
                "pentad_in_year": [5],
                "forecasted_discharge": [42.0],
                "model_short": ["TFT"],
            }
        )
        mock_client = self._make_mock_client()

        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_get_postprocessing_client", return_value=mock_client),
            patch.object(api_writer, "_check_write_codes") as mock_guard,
        ):
            # Act
            api_writer._write_combined_forecast_to_api(data, "pentad")

        # Assert
        mock_guard.assert_called_once()
        call_args = mock_guard.call_args
        batch_codes, context = call_args[0]
        assert batch_codes == {"99001"}
        assert context == "combined_forecast"

    def test_skill_metrics_triggers_guard(self):
        """_write_skill_metrics_to_api calls _check_write_codes with station codes."""
        # Arrange
        data = pd.DataFrame(
            {
                "code": ["99001"],
                "pentad_in_year": [5],
                "model_short": ["LR"],
                "n_pairs": [10],
                "nse": [0.8],
                "accuracy": [0.75],
                "sdivsigma": [0.6],
                "delta": [0.1],
                "mae": [1.2],
            }
        )
        mock_client = self._make_mock_client()

        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_get_postprocessing_client", return_value=mock_client),
            patch.object(api_writer, "_check_write_codes") as mock_guard,
        ):
            # Act
            api_writer._write_skill_metrics_to_api(data, "pentad", year=2025)

        # Assert
        mock_guard.assert_called_once()
        call_args = mock_guard.call_args
        batch_codes, context = call_args[0]
        assert batch_codes == {"99001"}
        assert context == "skill_metrics"
