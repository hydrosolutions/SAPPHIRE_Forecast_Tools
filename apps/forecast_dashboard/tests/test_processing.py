"""Unit tests for forecast_dashboard/src/processing.py.

Tests pure functions that don't require a running Panel server or live API.
"""

import numpy as np
import pandas as pd
import pytest
from src import processing

# ── filter_dataframe_for_selected_stations ──────────────────────────────────


class TestFilterDataframeForSelectedStations:
    def test_filters_matching_codes(self, sample_stations_df):
        result = processing.filter_dataframe_for_selected_stations(
            sample_stations_df, "code", ["99001", "99003"]
        )
        assert list(result["code"]) == ["99001", "99003"]

    def test_returns_empty_when_no_match(self, sample_stations_df):
        result = processing.filter_dataframe_for_selected_stations(
            sample_stations_df, "code", ["00000"]
        )
        assert len(result) == 0

    def test_returns_all_when_all_selected(self, sample_stations_df):
        result = processing.filter_dataframe_for_selected_stations(
            sample_stations_df, "code", ["99001", "99002", "99003"]
        )
        assert len(result) == 3

    def test_empty_selection_returns_empty(self, sample_stations_df):
        result = processing.filter_dataframe_for_selected_stations(sample_stations_df, "code", [])
        assert len(result) == 0


# ── parse_dates ─────────────────────────────────────────────────────────────


class TestParseDates:
    def test_iso_format(self):
        result = processing.parse_dates("2026-03-05")
        assert result == pd.Timestamp("2026-03-05")

    def test_dot_format(self):
        result = processing.parse_dates("05.03.2026")
        assert result == pd.Timestamp("2026-03-05")

    def test_invalid_returns_nat(self):
        result = processing.parse_dates("not-a-date")
        assert pd.isna(result)

    def test_empty_string_returns_nat(self):
        result = processing.parse_dates("")
        assert pd.isna(result)


# ── shift_date_by_n_days ───────────────────────────────────────────────────


class TestShiftDateByNDays:
    def test_shifts_date_column(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-01", "2026-03-02"]),
                "predictor": [1.0, 2.0],
                "discharge_avg": [10.0, 20.0],
            }
        )
        result = processing.shift_date_by_n_days(df, n=1)
        # date column is dropped after shift
        assert "date" not in result.columns
        assert len(result) == 2

    def test_does_not_mutate_original(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-01"]),
                "predictor": [1.0],
                "discharge_avg": [10.0],
            }
        )
        original_date = df["date"].iloc[0]
        processing.shift_date_by_n_days(df, n=5)
        assert df["date"].iloc[0] == original_date

    def test_drops_nan_predictor_rows(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-01", "2026-03-02"]),
                "predictor": [1.0, np.nan],
                "discharge_avg": [10.0, 20.0],
            }
        )
        result = processing.shift_date_by_n_days(df, n=1)
        assert len(result) == 1

    def test_drops_nan_discharge_avg_rows(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-01", "2026-03-02"]),
                "predictor": [1.0, 2.0],
                "discharge_avg": [np.nan, 20.0],
            }
        )
        result = processing.shift_date_by_n_days(df, n=1)
        assert len(result) == 1

    def test_updates_pentad_in_year_if_present(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-01"]),
                "pentad_in_year": [13],
                "predictor": [1.0],
                "discharge_avg": [10.0],
            }
        )
        result = processing.shift_date_by_n_days(df, n=1)
        # pentad_in_year should be recalculated for Mar 2 (still pentad 13)
        assert "pentad_in_year" in result.columns
        assert result["pentad_in_year"].dtype in (int, np.int64, np.int32)


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


# ── add_labels_to_forecast_pentad_df ───────────────────────────────────────


class TestAddLabelsToForecastPentadDf:
    def test_adds_labels_with_code_format(self, sample_stations_df):
        forecast = pd.DataFrame(
            {
                "code": ["99001", "99002"],
                "forecasted_discharge": [10.0, 20.0],
            }
        )
        result = processing.add_labels_to_forecast_pentad_df(forecast, sample_stations_df)
        assert result["station_labels"].iloc[0] == "99001 - Test River A"

    def test_strips_trailing_dot_zero(self, sample_stations_df):
        forecast = pd.DataFrame(
            {
                "code": ["99001.0"],
                "forecasted_discharge": [10.0],
            }
        )
        result = processing.add_labels_to_forecast_pentad_df(forecast, sample_stations_df)
        assert result["code"].iloc[0] == "99001"


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


# ── get_best_models_for_station_and_pentad ─────────────────────────────────


class TestGetBestModelsForStationAndPentad:
    def test_returns_lr_and_best_ml(self, sample_forecast_df):
        result = processing.get_best_models_for_station_and_pentad(
            sample_forecast_df,
            selected_station="99001 - Test River A",
            selected_pentad=13,
            selected_decad=None,
        )
        # Should include LR and the ML model with highest accuracy (NE=88)
        assert len(result) == 2
        assert "Linear regression (LR)" in result
        assert "Neural Ensemble (NE)" in result

    def test_no_ml_models_returns_lr_only(self):
        df = pd.DataFrame(
            {
                "station_labels": ["S1", "S1"],
                "pentad_in_year": [13, 13],
                "model_short": ["LR", "LR"],
                "model_long": ["Linear regression (LR)", "Linear regression (LR)"],
                "forecasted_discharge": [10.0, 10.0],
                "accuracy": [80.0, 80.0],
            }
        )
        result = processing.get_best_models_for_station_and_pentad(df, "S1", 13, None)
        assert result == ["Linear regression (LR)"]

    def test_no_forecasts_returns_empty(self):
        df = pd.DataFrame(
            {
                "station_labels": ["S1"],
                "pentad_in_year": [99],
                "model_short": ["TFT"],
                "model_long": ["Temporal Fusion Transformer (TFT)"],
                "forecasted_discharge": [10.0],
                "accuracy": [80.0],
            }
        )
        result = processing.get_best_models_for_station_and_pentad(df, "S1", 13, None)
        assert result == []


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
