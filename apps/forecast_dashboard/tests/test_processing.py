"""Unit tests for forecast_dashboard/src/processing.py.

Tests pure functions that don't require a running Panel server or live API.
"""

import numpy as np
import pandas as pd
import pytest
from src import processing


# ── internationalize_forecast_model_names ──────────────────────────────────


class TestInternationalizeForecastModelNames:
    def test_identity_translation_returns_same(self, identity_gettext):
        df = pd.DataFrame(
            {
                "model_long": ["Linear regression (LR)", "Neural Ensemble (NE)"],
                "model_short": ["LR", "NE"],
            }
        )
        result = processing.internationalize_forecast_model_names(identity_gettext, df)
        assert list(result["model_long"]) == [
            "Linear regression (LR)",
            "Neural Ensemble (NE)",
        ]

    def test_custom_translation_applies(self):
        translations = {
            "LR": "ЛР",
            "Linear regression (LR)": "Линейная регрессия (ЛР)",
        }

        def fake_gettext(msg):
            return translations.get(msg, msg)

        df = pd.DataFrame(
            {
                "model_long": ["Linear regression (LR)"],
                "model_short": ["LR"],
            }
        )
        result = processing.internationalize_forecast_model_names(fake_gettext, df)
        assert result["model_long"].iloc[0] == "Линейная регрессия (ЛР)"
        assert result["model_short"].iloc[0] == "ЛР"

    def test_does_not_mutate_input(self, identity_gettext):
        df = pd.DataFrame(
            {
                "model_long": ["Original"],
                "model_short": ["O"],
            }
        )
        processing.internationalize_forecast_model_names(identity_gettext, df)
        assert df["model_long"].iloc[0] == "Original"


# ── add_labels_to_hydrograph ───────────────────────────────────────────────


class TestAddLabelsToHydrograph:
    def test_adds_station_labels(self, sample_stations_df):
        hydrograph = pd.DataFrame(
            {
                "code": ["99001", "99002"],
                "value": [100.0, 200.0],
            }
        )
        result = processing.add_labels_to_hydrograph(hydrograph, sample_stations_df)
        assert "station_labels" in result.columns
        assert result["station_labels"].iloc[0] == "99001 - Test River A"
        assert result["station_labels"].iloc[1] == "99002 - Test River B"

    def test_missing_code_gets_nan_label(self, sample_stations_df):
        hydrograph = pd.DataFrame(
            {
                "code": ["00000"],
                "value": [100.0],
            }
        )
        result = processing.add_labels_to_hydrograph(hydrograph, sample_stations_df)
        assert pd.isna(result["station_labels"].iloc[0])


# ── calculate_forecast_range ───────────────────────────────────────────────


class TestCalculateForecastRange:
    @pytest.fixture
    def forecast_table(self):
        return pd.DataFrame(
            {
                "forecasted_discharge": [100.0, 200.0],
                "delta": [10.0, 20.0],
            }
        )

    def test_delta_mode(self, identity_gettext, forecast_table):
        result = processing.calculate_forecast_range(identity_gettext, forecast_table, "delta", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)
        assert result["fc_lower"].iloc[1] == pytest.approx(180.0)
        assert result["fc_upper"].iloc[1] == pytest.approx(220.0)

    def test_manual_range_scalar(self, identity_gettext, forecast_table):
        # 20% range
        result = processing.calculate_forecast_range(
            identity_gettext,
            forecast_table,
            "Manual range, select value below",
            20,
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(80.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(120.0)

    def test_manual_range_widget(self, identity_gettext, forecast_table):
        class FakeWidget:
            value = 10

        result = processing.calculate_forecast_range(
            identity_gettext,
            forecast_table,
            "Manual range, select value below",
            FakeWidget(),
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

    def test_max_delta_pct_scalar(self, identity_gettext, forecast_table):
        # max[delta, %] with 5% → pct = 5.0, delta = 10.0
        # For row 0: delta range = [90, 110], pct range = [95, 105]
        # fc_lower = min(90, 95) = 90; fc_upper = max(110, 105) = 110
        result = processing.calculate_forecast_range(
            identity_gettext, forecast_table, "max[delta, %]", 5
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

    def test_unknown_range_type_falls_back_to_delta(self, identity_gettext, forecast_table):
        result = processing.calculate_forecast_range(
            identity_gettext, forecast_table, "unknown_type", 0
        )
        # Fallback is delta mode
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)


# ── get_bulletin_header_info ───────────────────────────────────────────────


class TestGetBulletinHeaderInfo:
    def test_pentad_header(self):
        # tag_library functions expect date strings, not date objects
        d = "2026-03-05"
        result = processing.get_bulletin_header_info(d, "pentad")
        assert "pentad" in result.columns
        assert "month_number" in result.columns
        assert "year" in result.columns
        assert "day_start_pentad" in result.columns
        assert "day_end_pentad" in result.columns
        assert str(result["year"].iloc[0]) == "2026"

    def test_decad_header(self):
        d = "2026-03-15"
        result = processing.get_bulletin_header_info(d, "decad")
        assert "decad" in result.columns
        assert "month_number" in result.columns
        assert "year" in result.columns
        assert "day_start_decad" in result.columns
        assert "day_end_decad" in result.columns

    def test_pentad_single_row(self):
        d = "2026-01-01"
        result = processing.get_bulletin_header_info(d, "pentad")
        assert len(result) == 1
