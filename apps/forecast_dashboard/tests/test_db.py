"""Unit tests for forecast_dashboard/src/db.py.

Tests pure helpers directly, and API-calling functions with mocked HTTP.
"""

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from src import db

# ── _convert_na_to_nan ─────────────────────────────────────────────────────


class TestConvertNaToNan:
    def test_converts_pd_na_to_np_nan(self):
        df = pd.DataFrame({"a": pd.array([1, pd.NA, 3], dtype="Int64")})
        result = db._convert_na_to_nan(df)
        assert np.isnan(result["a"].iloc[1])
        # Non-NA values preserved
        assert result["a"].iloc[0] == 1
        assert result["a"].iloc[2] == 3

    def test_preserves_regular_values(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        result = db._convert_na_to_nan(df)
        assert list(result["x"]) == [1.0, 2.0, 3.0]

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"a": pd.array([1, pd.NA], dtype="Int64")})
        db._convert_na_to_nan(df)
        assert pd.isna(df["a"].iloc[1])  # original still has pd.NA

    def test_handles_string_columns(self):
        df = pd.DataFrame({"s": pd.array(["a", pd.NA, "c"], dtype="string")})
        result = db._convert_na_to_nan(df)
        assert result["s"].iloc[0] == "a"

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": pd.array([], dtype="Int64")})
        result = db._convert_na_to_nan(df)
        assert len(result) == 0


# ── _get_horizon ───────────────────────────────────────────────────────────


class TestGetHorizon:
    def test_default_is_pentad(self, monkeypatch):
        monkeypatch.delenv("sapphire_forecast_horizon", raising=False)
        assert db._get_horizon() == "pentad"

    def test_pentad_passthrough(self, monkeypatch):
        monkeypatch.setenv("sapphire_forecast_horizon", "pentad")
        assert db._get_horizon() == "pentad"

    def test_decad_becomes_decade(self, monkeypatch):
        monkeypatch.setenv("sapphire_forecast_horizon", "decad")
        assert db._get_horizon() == "decade"


# ── _horizon_in_year_col ──────────────────────────────────────────────────


class TestHorizonInYearCol:
    def test_pentad(self):
        assert db._horizon_in_year_col("pentad") == "pentad_in_year"

    def test_decade(self):
        assert db._horizon_in_year_col("decade") == "decad_in_year"


# ── _resolve_station ──────────────────────────────────────────────────────


class TestResolveStation:
    def test_string_passthrough(self):
        assert db._resolve_station("15102") == "15102"

    def test_widget_with_value(self):
        widget = MagicMock()
        widget.value = "15102 - River Name"
        assert db._resolve_station(widget) == "15102"

    def test_widget_extracts_first_token(self):
        widget = MagicMock()
        widget.value = "99001 Test Station Extra Words"
        assert db._resolve_station(widget) == "99001"


# ── get_ml_forecast (mocked HTTP) ─────────────────────────────────────────


class TestGetMlForecast:
    def test_renames_columns_and_computes_ne(
        self, monkeypatch, mock_api_response, forecast_response_json
    ):
        """ML forecast should rename cols and add Neural Ensemble rows."""
        monkeypatch.setenv("sapphire_forecast_horizon", "pentad")
        monkeypatch.setattr(
            "src.db.requests.get",
            lambda *a, **kw: mock_api_response(forecast_response_json),
        )

        result = db.get_ml_forecast("99001")

        # Column renames applied
        assert "model_short" in result.columns
        assert "model_long" in result.columns
        assert "E[Q]" in result.columns
        assert "Q5" in result.columns

        # NE rows should be added (one per station/date combo)
        ne_rows = result[result["model_short"] == "NE"]
        assert len(ne_rows) >= 1

        # NE E[Q] is mean of TFT(10), TiDE(11), TSMixer(12) = 11.0
        ne_eq = ne_rows["E[Q]"].iloc[0]
        assert ne_eq == pytest.approx(11.0)

    def test_ne_quantiles_are_means(self, monkeypatch, mock_api_response, forecast_response_json):
        """NE quantile columns should be means of base model quantiles."""
        monkeypatch.setenv("sapphire_forecast_horizon", "pentad")
        monkeypatch.setattr(
            "src.db.requests.get",
            lambda *a, **kw: mock_api_response(forecast_response_json),
        )

        result = db.get_ml_forecast("99001")
        ne = result[result["model_short"] == "NE"].iloc[0]

        # Q5: mean of (10-3, 11-3, 12-3) = mean(7, 8, 9) = 8.0
        assert ne["Q5"] == pytest.approx(8.0)
        # Q95: mean of (10+3, 11+3, 12+3) = mean(13, 14, 15) = 14.0
        assert ne["Q95"] == pytest.approx(14.0)


# ── get_forecasts_all (mocked HTTP) ───────────────────────────────────────


class TestGetForecastsAll:
    def test_combines_ml_and_lr(
        self,
        monkeypatch,
        mock_api_response,
        forecast_response_json,
        lr_forecast_response_json,
    ):
        monkeypatch.setenv("sapphire_forecast_horizon", "pentad")

        call_count = {"n": 0}

        def fake_get(*args, **kwargs):
            call_count["n"] += 1
            url = args[0] if args else kwargs.get("url", "")
            if "lr-forecast" in url:
                return mock_api_response(lr_forecast_response_json)
            return mock_api_response(forecast_response_json)

        monkeypatch.setattr("src.db.requests.get", fake_get)

        result = db.get_forecasts_all("99001")

        # Should have both ML and LR rows
        models = result["model_short"].unique()
        assert "LR" in models
        assert "TFT" in models or "TiDE" in models

        # LR rows should have model_long set
        lr_rows = result[result["model_short"] == "LR"]
        assert (lr_rows["model_long"] == "Linear regression (LR)").all()

        # Date column should be shifted by 1 day
        assert "Date" in result.columns

    def test_sorted_by_date(
        self,
        monkeypatch,
        mock_api_response,
        forecast_response_json,
        lr_forecast_response_json,
    ):
        monkeypatch.setenv("sapphire_forecast_horizon", "pentad")

        def fake_get(*args, **kwargs):
            url = args[0] if args else kwargs.get("url", "")
            if "lr-forecast" in url:
                return mock_api_response(lr_forecast_response_json)
            return mock_api_response(forecast_response_json)

        monkeypatch.setattr("src.db.requests.get", fake_get)

        result = db.get_forecasts_all("99001")
        dates = result["Date"].tolist()
        assert dates == sorted(dates)


# ── get_forecast_stats (mocked HTTP) ──────────────────────────────────────


class TestGetForecastStats:
    def test_renames_columns(self, monkeypatch, mock_api_response, skill_metric_response_json):
        monkeypatch.setenv("sapphire_forecast_horizon", "pentad")
        monkeypatch.setattr(
            "src.db.requests.get",
            lambda *a, **kw: mock_api_response(skill_metric_response_json),
        )

        result = db.get_forecast_stats("99001")

        assert "model_short" in result.columns
        assert "model_long" in result.columns
        assert "pentad_in_year" in result.columns
        # Original columns should be dropped
        assert "horizon_type" not in result.columns
        assert "id" not in result.columns

    def test_preserves_metric_values(
        self, monkeypatch, mock_api_response, skill_metric_response_json
    ):
        monkeypatch.setenv("sapphire_forecast_horizon", "pentad")
        monkeypatch.setattr(
            "src.db.requests.get",
            lambda *a, **kw: mock_api_response(skill_metric_response_json),
        )

        result = db.get_forecast_stats("99001")
        tft_row = result[result["model_short"] == "TFT"].iloc[0]
        assert tft_row["accuracy"] == pytest.approx(90.0)
        assert tft_row["sdivsigma"] == pytest.approx(0.50)
