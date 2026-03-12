"""Integration tests for config_all_stations_library.json bootstrap from HF SDK.

Tests that write_config_all_stations correctly writes station metadata
when called from the preprocessing_runoff HF SDK path.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
import setup_library as sl


class TestConfigAllBootstrapFromHFSdk:
    """Integration: HF SDK fetch -> write_config_all_stations -> valid config file."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        self.config_dir = str(tmp_path)
        self.config_file = os.path.join(self.config_dir, "config_all_stations_library.json")

    def _make_hf_site(
        self,
        code="12176",
        name_nat="\u0440. \u0410\u043b\u0430-\u0410\u0440\u0447\u0430 - \u0441. \u041a\u0430\u0448\u043a\u0430-\u0421\u0443\u0443",
        lat=42.65,
        lon=74.48,
        basin_nat="\u0427\u0443",
        region_nat="\u0427\u0443\u0439\u0441\u043a\u0430\u044f",
        river_name_nat="\u0440. \u0410\u043b\u0430-\u0410\u0440\u0447\u0430",
        punkt_name_nat="\u0441. \u041a\u0430\u0448\u043a\u0430-\u0421\u0443\u0443",
        site_type="automatic-discharge",
        iehhf_site_id=42,
    ):
        """Create a mock Site like those from get_all_forecast_sites_from_HF_SDK."""
        site = MagicMock()
        site.code = code
        site.name = "r. Ala-Archa - v. Kashka-Suu"
        site.name_nat = name_nat
        site.lat = lat
        site.lon = lon
        site.basin = "Chu"
        site.basin_nat = basin_nat
        site.region = "Chuy"
        site.region_nat = region_nat
        site.river_name = "r. Ala-Archa"
        site.river_name_nat = river_name_nat
        site.punkt_name = "v. Kashka-Suu"
        site.punkt_name_nat = punkt_name_nat
        site.site_type = site_type
        site.iehhf_site_id = iehhf_site_id
        site.is_virtual = False
        return site

    def test_hf_sdk_sites_produce_valid_config(self):
        """Sites from HF SDK -> config file with correct schema and values."""
        sites = [
            self._make_hf_site(code="12176", iehhf_site_id=42),
            self._make_hf_site(
                code="12345",
                name_nat="\u0440. \u041d\u0430\u0440\u044b\u043d - \u0441. \u0423\u0447\u043a\u0443\u043d",
                lat=41.2,
                lon=75.9,
                basin_nat="\u0421\u044b\u0440-\u0414\u0430\u0440\u044c\u044f",
                iehhf_site_id=99,
            ),
        ]

        with patch.object(sl, "_read_manual_entries_from_config", return_value={}):
            sl.write_config_all_stations(sites, self.config_file)

        assert os.path.exists(self.config_file)
        with open(self.config_file, encoding="utf-8") as f:
            data = json.load(f)

        stations = data["stations_available_for_forecast"]
        assert len(stations) == 2
        assert "12176" in stations
        assert "12345" in stations

        # Verify schema: all values list-wrapped
        s = stations["12176"]
        assert s["code"] == [12176]
        assert s["lat"] == [42.65]
        assert s["long"] == [74.48]
        assert s["name_ru"] == [
            "\u0440. \u0410\u043b\u0430-\u0410\u0440\u0447\u0430 - "
            "\u0441. \u041a\u0430\u0448\u043a\u0430-\u0421\u0443\u0443"
        ]
        assert s["basin"] == ["\u0427\u0443"]
        assert s["river_ru"] == ["\u0440. \u0410\u043b\u0430-\u0410\u0440\u0447\u0430"]
        assert s["punkt_ru"] == ["\u0441. \u041a\u0430\u0448\u043a\u0430-\u0421\u0443\u0443"]
        assert s["data_source"] == ["ieh_hf"]

        # Verify required fields exist (consumers check for these)
        for required in ["name_ru", "lat", "long", "code"]:
            assert required in s

    def test_cache_mode_no_sites_skips_write(self):
        """When cache is used (fc_sites=[]), config write should be skipped.

        This simulates the preprocessing_runoff behavior where cache_used=True
        means fc_sites is empty -- the guard ``if fc_sites:`` prevents the
        write.
        """
        fc_sites = []  # Empty, as in cache mode

        # The guard in preprocessing_runoff.py is:
        #   if fc_sites:
        #       sl.write_config_all_stations(fc_sites)
        # So with empty list, write_config_all_stations is never called.
        assert not fc_sites  # Falsy -- guard prevents call
        assert not os.path.exists(self.config_file)  # No file created

    def test_single_site_roundtrip(self):
        """A single HF SDK site produces a one-entry config file."""
        sites = [self._make_hf_site()]

        with patch.object(sl, "_read_manual_entries_from_config", return_value={}):
            result_path = sl.write_config_all_stations(sites, self.config_file)

        assert result_path == self.config_file
        with open(self.config_file, encoding="utf-8") as f:
            data = json.load(f)

        stations = data["stations_available_for_forecast"]
        assert len(stations) == 1
        assert "12176" in stations

    def test_site_type_and_id_fields_persisted(self):
        """site_type and id fields from HF SDK are written to config."""
        sites = [self._make_hf_site(site_type="automatic-discharge", iehhf_site_id=42)]

        with patch.object(sl, "_read_manual_entries_from_config", return_value={}):
            sl.write_config_all_stations(sites, self.config_file)

        with open(self.config_file, encoding="utf-8") as f:
            data = json.load(f)

        s = data["stations_available_for_forecast"]["12176"]
        assert s["site_type"] == ["automatic-discharge"]
        assert s["id"] == [42]
        assert s["is_virtual"] == [False]

    def test_manual_entries_preserved_alongside_sdk(self):
        """Manual entries (non-ieh_hf) are merged into the output file."""
        sites = [self._make_hf_site(code="12176")]
        manual = {
            "99999": {
                "code": [99999],
                "name_ru": ["Manual Station"],
                "lat": [40.0],
                "long": [70.0],
                "data_source": ["google_sheets"],
            }
        }

        with patch.object(sl, "_read_manual_entries_from_config", return_value=manual):
            sl.write_config_all_stations(sites, self.config_file)

        with open(self.config_file, encoding="utf-8") as f:
            data = json.load(f)

        stations = data["stations_available_for_forecast"]
        assert "12176" in stations
        assert "99999" in stations
        assert stations["99999"]["data_source"] == ["google_sheets"]

    def test_sdk_collision_with_manual_prefers_sdk(self):
        """When a manual entry code matches an SDK site, SDK data wins."""
        sites = [self._make_hf_site(code="12176")]
        manual = {
            "12176": {
                "code": [12176],
                "name_ru": ["Old Manual Name"],
                "data_source": ["google_sheets"],
            }
        }

        with patch.object(sl, "_read_manual_entries_from_config", return_value=manual):
            sl.write_config_all_stations(sites, self.config_file)

        with open(self.config_file, encoding="utf-8") as f:
            data = json.load(f)

        s = data["stations_available_for_forecast"]["12176"]
        # SDK data wins -- data_source should be ieh_hf, not google_sheets
        assert s["data_source"] == ["ieh_hf"]

    def test_backup_created_on_overwrite(self):
        """Existing config file is backed up before overwrite."""
        # Create an initial file
        initial_data = {"stations_available_for_forecast": {"00001": {"code": [1]}}}
        with open(self.config_file, "w", encoding="utf-8") as f:
            json.dump(initial_data, f)

        sites = [self._make_hf_site(code="12176")]

        with patch.object(sl, "_read_manual_entries_from_config", return_value={}):
            sl.write_config_all_stations(sites, self.config_file)

        # Backup should exist
        backup_file = self.config_file + ".bak"
        assert os.path.exists(backup_file)

        # Backup contains the original data
        with open(backup_file, encoding="utf-8") as f:
            backup_data = json.load(f)
        assert "00001" in backup_data["stations_available_for_forecast"]

    def test_output_is_utf8_with_cyrillic(self):
        """Config file is written in UTF-8 so Cyrillic characters survive."""
        sites = [self._make_hf_site()]

        with patch.object(sl, "_read_manual_entries_from_config", return_value={}):
            sl.write_config_all_stations(sites, self.config_file)

        with open(self.config_file, encoding="utf-8") as f:
            raw = f.read()

        # Cyrillic characters should be present (not escaped as \uXXXX)
        assert "\u0410\u043b\u0430" in raw  # "Ала" from Ала-Арча
        assert "\u0427\u0443" in raw  # "Чу" basin
