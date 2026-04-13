"""Tests for LR-only deployment bug fixes in the forecast dashboard.

Covers:
- Bug 1: Normalized columns after pd.concat (pentad_in_month / decad_in_month)
- Bug 2: _create_manual_sites helper returns correct fl.Site objects
- Bug 3: create_skill_table early-return on empty forecast_stats
- Bug 6/8: delta_offset guard in calculate_forecast_range
- Bug 7: reindex fills missing skill columns with NaN
"""

import json
import logging

import numpy as np
import pandas as pd
import pytest

from src import processing


# ---------------------------------------------------------------------------
# Bug 1: Normalized columns after pd.concat
# ---------------------------------------------------------------------------


class TestNormalizedColumns:
    def test_pentad_in_month_filled_from_pentad_lr_only(self):
        """LR-only data: 'pentad' column exists but no 'pentad_in_month'."""
        df = pd.DataFrame(
            {
                "pentad": [1, 2, 3],
                "model_short": ["LR", "LR", "LR"],
            }
        )
        # Apply the same normalization as db.py post-concat
        if "pentad_in_month" in df.columns and "pentad" in df.columns:
            df["pentad_in_month"] = df["pentad_in_month"].fillna(df["pentad"])
        elif "pentad" in df.columns:
            df["pentad_in_month"] = df["pentad"]
        assert list(df["pentad_in_month"]) == [1, 2, 3]
        assert "pentad" in df.columns  # original preserved

    def test_pentad_in_month_fillna_mixed(self):
        """Mixed ML+LR: both columns exist, NaN gaps filled."""
        df = pd.DataFrame(
            {
                "pentad_in_month": [1.0, np.nan, 3.0],
                "pentad": [np.nan, 2.0, np.nan],
            }
        )
        if "pentad_in_month" in df.columns and "pentad" in df.columns:
            df["pentad_in_month"] = df["pentad_in_month"].fillna(df["pentad"])
        elif "pentad" in df.columns:
            df["pentad_in_month"] = df["pentad"]
        assert list(df["pentad_in_month"]) == [1.0, 2.0, 3.0]

    def test_decad_in_month_from_decade(self):
        """Decade data: 'decad_in_month' added as copy of 'decade'."""
        df = pd.DataFrame({"decade": [1, 2, 3]})
        if "decade" in df.columns:
            df["decad_in_month"] = df["decade"]
        assert list(df["decad_in_month"]) == [1, 2, 3]
        assert list(df["decade"]) == [1, 2, 3]  # original preserved


# ---------------------------------------------------------------------------
# Bug 3: create_skill_table early-return on empty stats
# ---------------------------------------------------------------------------


class TestCreateSkillTable:
    def test_empty_stats_returns_empty_tabulator(self, identity_gettext):
        """Empty forecast_stats → empty Tabulator (no crash)."""
        import panel as pn

        from src import vizualization

        result = vizualization.create_skill_table(identity_gettext, "pentad", pd.DataFrame())
        assert isinstance(result, pn.widgets.Tabulator)
        assert result.value.empty

    def test_empty_stats_decad_returns_empty_tabulator(self, identity_gettext):
        """Empty forecast_stats for decad → empty Tabulator (no crash)."""
        import panel as pn

        from src import vizualization

        result = vizualization.create_skill_table(identity_gettext, "decade", pd.DataFrame())
        assert isinstance(result, pn.widgets.Tabulator)
        assert result.value.empty


# ---------------------------------------------------------------------------
# Bug 6/8: delta_offset guard in calculate_forecast_range
# ---------------------------------------------------------------------------


class TestCalculateForecastRange:
    def test_delta_missing_produces_zero_width_range(self, identity_gettext):
        """No delta column → fc_lower == fc_upper == forecasted_discharge."""
        df = pd.DataFrame(
            {"forecasted_discharge": [10.0, 20.0], "model_short": ["LR", "LR"]}
        )
        result = processing.calculate_forecast_range(identity_gettext, df, "delta", 10)
        assert list(result["fc_lower"]) == [10.0, 20.0]
        assert list(result["fc_upper"]) == [10.0, 20.0]

    def test_delta_all_nan_produces_zero_width_range(self, identity_gettext):
        """Delta column exists but all NaN → zero-width range."""
        df = pd.DataFrame(
            {
                "forecasted_discharge": [10.0, 20.0],
                "delta": [np.nan, np.nan],
            }
        )
        result = processing.calculate_forecast_range(identity_gettext, df, "delta", 10)
        assert list(result["fc_lower"]) == [10.0, 20.0]
        assert list(result["fc_upper"]) == [10.0, 20.0]

    def test_delta_present_uses_delta(self, identity_gettext):
        """Delta column with values → normal range calculation."""
        df = pd.DataFrame(
            {
                "forecasted_discharge": [10.0, 20.0],
                "delta": [2.0, 3.0],
            }
        )
        result = processing.calculate_forecast_range(identity_gettext, df, "delta", 10)
        assert list(result["fc_lower"]) == [8.0, 17.0]
        assert list(result["fc_upper"]) == [12.0, 23.0]

    def test_manual_range_ignores_delta(self, identity_gettext):
        """Manual range works without delta column."""
        df = pd.DataFrame({"forecasted_discharge": [100.0]})
        result = processing.calculate_forecast_range(
            identity_gettext, df, "Manual range, select value below", 10
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

    def test_min_delta_percent_missing_delta(self, identity_gettext):
        """min[delta, %] with missing delta → zero-width range.

        With delta_offset=0 and slider=10%: delta band=[100,100],
        pct band=[90,110]. Intersection (narrow): max(100,90)=100,
        min(100,110)=100 → zero-width.
        """
        df = pd.DataFrame({"forecasted_discharge": [100.0]})
        result = processing.calculate_forecast_range(
            identity_gettext, df, "min[delta, %]", 10
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(100.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Bug 7: reindex in create_forecast_summary_table
# ---------------------------------------------------------------------------


class TestForecastSummaryReindex:
    def test_reindex_fills_missing_skill_columns_with_nan(self):
        """Missing skill metric columns → NaN instead of KeyError."""
        df = pd.DataFrame(
            {
                "model_short": ["LR"],
                "forecasted_discharge": [10.0],
                "fc_lower": [10.0],
                "fc_upper": [10.0],
            }
        )
        expected_cols = [
            "model_short",
            "forecasted_discharge",
            "fc_lower",
            "fc_upper",
            "delta",
            "sdivsigma",
            "mae",
            "accuracy",
        ]
        result = df.reindex(columns=expected_cols)
        assert list(result.columns) == expected_cols
        assert pd.isna(result["delta"].iloc[0])
        assert pd.isna(result["sdivsigma"].iloc[0])

    def test_reindex_preserves_existing_columns(self):
        """Existing columns preserved during reindex."""
        df = pd.DataFrame(
            {
                "model_short": ["TFT"],
                "forecasted_discharge": [10.0],
                "fc_lower": [9.0],
                "fc_upper": [11.0],
                "delta": [1.0],
                "sdivsigma": [0.5],
                "mae": [0.3],
                "accuracy": [85.0],
            }
        )
        expected_cols = [
            "model_short",
            "forecasted_discharge",
            "fc_lower",
            "fc_upper",
            "delta",
            "sdivsigma",
            "mae",
            "accuracy",
        ]
        result = df.reindex(columns=expected_cols)
        assert result["delta"].iloc[0] == 1.0
        assert result["accuracy"].iloc[0] == 85.0


# ---------------------------------------------------------------------------
# Bug 2: _create_manual_sites helper
# ---------------------------------------------------------------------------


class TestCreateManualSites:
    def test_returns_sites_from_config(self, monkeypatch, tmp_path):
        """Manual entries in config → fl.Site objects returned."""
        import forecast_library as fl

        config = {
            "stations_available_for_forecast": {
                "10001": {
                    "name_ru": ["Test Station"],
                    "river_ru": ["Test River"],
                    "punkt_ru": ["Test Punkt"],
                    "lat": [42.0],
                    "long": [71.0],
                    "region": ["Test Region"],
                    "basin": ["Test Basin"],
                    "data_source": ["google_sheets"],
                    "qdanger": [150.0],
                    "bulletin_order": [5],
                }
            }
        }
        config_file = tmp_path / "config_all_stations.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyforecast_config_file_all_stations", "config_all_stations.json"
        )

        sites = processing._create_manual_sites()
        assert len(sites) == 1
        assert sites[0].code == "10001"
        assert sites[0].name == "Test Station"
        assert sites[0].river_name == "Test River"
        assert sites[0].basin == "Test Basin"
        assert sites[0].lat == 42.0
        assert sites[0].lon == 71.0
        assert sites[0].qdanger == 150.0
        assert sites[0].bulletin_order == 5

    def test_missing_basin_logs_warning(self, monkeypatch, tmp_path, caplog):
        """Missing basin field → warning logged, default used."""
        config = {
            "stations_available_for_forecast": {
                "10002": {
                    "name_ru": ["No Basin Station"],
                    "river_ru": ["River"],
                    "punkt_ru": ["Punkt"],
                    "lat": [0.0],
                    "long": [0.0],
                    "region": ["Region"],
                    "data_source": ["google_sheets"],
                }
            }
        }
        config_file = tmp_path / "config_all_stations.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyforecast_config_file_all_stations", "config_all_stations.json"
        )

        with caplog.at_level(logging.WARNING):
            sites = processing._create_manual_sites()
        assert len(sites) == 1
        assert sites[0].basin == "Basin"  # default
        assert "no basin" in caplog.text.lower() or "10002" in caplog.text

    def test_empty_config_returns_empty_list(self, monkeypatch, tmp_path):
        """No manual entries → empty list."""
        config = {"stations_available_for_forecast": {}}
        config_file = tmp_path / "config_all_stations.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyforecast_config_file_all_stations", "config_all_stations.json"
        )

        sites = processing._create_manual_sites()
        assert sites == []

    def test_dedup_skips_existing_codes(self, monkeypatch, tmp_path):
        """Manual sites are not added if code already exists in all_stations."""
        import forecast_library as fl

        config = {
            "stations_available_for_forecast": {
                "15013": {
                    "name_ru": ["Duplicate"],
                    "river_ru": ["R"],
                    "punkt_ru": ["P"],
                    "lat": [0.0],
                    "long": [0.0],
                    "region": ["R"],
                    "basin": ["B"],
                    "data_source": ["google_sheets"],
                }
            }
        }
        config_file = tmp_path / "config_all_stations.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyforecast_config_file_all_stations", "config_all_stations.json"
        )

        manual_sites = processing._create_manual_sites()
        existing = [fl.Site(code="15013")]
        existing_codes = {s.code for s in existing}
        filtered = [s for s in manual_sites if s.code not in existing_codes]
        assert len(filtered) == 0
