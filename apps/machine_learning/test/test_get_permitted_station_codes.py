"""Tests for get_permitted_station_codes() in utils_ml_forecast.py.

Verifies:
1. Valid pentad config returns correct set of station code strings.
2. Union of pentad and decad configs is returned when both exist.
3. Pentad-only config (no decad env var) returns just pentad codes.
4. Missing config file returns None.
5. Malformed JSON returns None.
6. Empty stationsID list returns empty set (not None).
7. Integer codes in config are converted to strings.
8. Missing env vars return None.
"""

import json
import os
import sys

# Mock heavy dependencies before importing from scr
from unittest.mock import MagicMock

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

# Add module root and scr to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"),
)

from scr.utils_ml_forecast import get_permitted_station_codes  # noqa: E402

# ---------------------------------------------------------------------------
# ENV VAR NAMES (must match the implementation)
# ---------------------------------------------------------------------------
_ENV_CONFIG_PATH = "ieasyforecast_configuration_path"
_ENV_PENTAD_FILE = "ieasyforecast_config_file_station_selection"
_ENV_DECAD_FILE = "ieasyforecast_config_file_station_selection_decad"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetPermittedStationCodes:
    """Tests for get_permitted_station_codes()."""

    def test_valid_pentad_config(self, tmp_path, monkeypatch):
        """Valid pentad config returns correct set of station code strings."""
        config_file = tmp_path / "stations_pentad.json"
        config_file.write_text(json.dumps({"stationsID": ["15020", "15030", "16059"]}))

        monkeypatch.setenv(_ENV_CONFIG_PATH, str(tmp_path))
        monkeypatch.setenv(_ENV_PENTAD_FILE, "stations_pentad.json")
        monkeypatch.delenv(_ENV_DECAD_FILE, raising=False)

        result = get_permitted_station_codes()

        assert result == {"15020", "15030", "16059"}

    def test_valid_pentad_and_decad_config(self, tmp_path, monkeypatch):
        """Both pentad and decad configs return the union of their codes."""
        pentad_file = tmp_path / "stations_pentad.json"
        pentad_file.write_text(json.dumps({"stationsID": ["15020", "15030"]}))

        decad_file = tmp_path / "stations_decad.json"
        decad_file.write_text(json.dumps({"stationsID": ["16059", "17000"]}))

        monkeypatch.setenv(_ENV_CONFIG_PATH, str(tmp_path))
        monkeypatch.setenv(_ENV_PENTAD_FILE, "stations_pentad.json")
        monkeypatch.setenv(_ENV_DECAD_FILE, "stations_decad.json")

        result = get_permitted_station_codes()

        assert result == {"15020", "15030", "16059", "17000"}

    def test_pentad_only_no_decad_env(self, tmp_path, monkeypatch):
        """When decad env var is absent, only pentad codes are returned."""
        config_file = tmp_path / "stations_pentad.json"
        config_file.write_text(json.dumps({"stationsID": ["15020", "15030"]}))

        monkeypatch.setenv(_ENV_CONFIG_PATH, str(tmp_path))
        monkeypatch.setenv(_ENV_PENTAD_FILE, "stations_pentad.json")
        monkeypatch.delenv(_ENV_DECAD_FILE, raising=False)

        result = get_permitted_station_codes()

        assert result == {"15020", "15030"}

    def test_missing_config_file(self, tmp_path, monkeypatch):
        """Pointing env vars to a non-existent file returns None."""
        monkeypatch.setenv(_ENV_CONFIG_PATH, str(tmp_path))
        monkeypatch.setenv(_ENV_PENTAD_FILE, "nonexistent_stations.json")
        monkeypatch.delenv(_ENV_DECAD_FILE, raising=False)

        result = get_permitted_station_codes()

        assert result is None

    def test_malformed_json(self, tmp_path, monkeypatch):
        """A config file with invalid JSON content returns None."""
        config_file = tmp_path / "stations_pentad.json"
        config_file.write_text("this is not valid json {{{")

        monkeypatch.setenv(_ENV_CONFIG_PATH, str(tmp_path))
        monkeypatch.setenv(_ENV_PENTAD_FILE, "stations_pentad.json")
        monkeypatch.delenv(_ENV_DECAD_FILE, raising=False)

        result = get_permitted_station_codes()

        assert result is None

    def test_empty_stations_list(self, tmp_path, monkeypatch):
        """Config with an empty stationsID list returns an empty set, not None."""
        config_file = tmp_path / "stations_pentad.json"
        config_file.write_text(json.dumps({"stationsID": []}))

        monkeypatch.setenv(_ENV_CONFIG_PATH, str(tmp_path))
        monkeypatch.setenv(_ENV_PENTAD_FILE, "stations_pentad.json")
        monkeypatch.delenv(_ENV_DECAD_FILE, raising=False)

        result = get_permitted_station_codes()

        assert result == set()
        assert result is not None

    def test_numeric_codes_converted_to_strings(self, tmp_path, monkeypatch):
        """Integer station codes in config are cast to strings."""
        config_file = tmp_path / "stations_pentad.json"
        config_file.write_text(json.dumps({"stationsID": [15020, 15030]}))

        monkeypatch.setenv(_ENV_CONFIG_PATH, str(tmp_path))
        monkeypatch.setenv(_ENV_PENTAD_FILE, "stations_pentad.json")
        monkeypatch.delenv(_ENV_DECAD_FILE, raising=False)

        result = get_permitted_station_codes()

        assert result == {"15020", "15030"}

    def test_missing_env_vars(self, monkeypatch):
        """When no env vars are set, function returns None."""
        monkeypatch.delenv(_ENV_CONFIG_PATH, raising=False)
        monkeypatch.delenv(_ENV_PENTAD_FILE, raising=False)
        monkeypatch.delenv(_ENV_DECAD_FILE, raising=False)

        result = get_permitted_station_codes()

        assert result is None
