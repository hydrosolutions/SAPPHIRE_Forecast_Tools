"""Unit tests for forecast_dashboard/src/db.py.

Tests pure helpers directly, and API-calling functions with mocked HTTP.
"""

from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import requests
from src import db
from src import vizualization

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


# ── _horizon_in_year_col ──────────────────────────────────────────────────


class TestHorizonInYearCol:
    def test_pentad(self):
        assert db._horizon_in_year_col("pentad") == "pentad_in_year"

    def test_decade(self):
        assert db._horizon_in_year_col("decade") == "decad_in_year"

    def test_month(self):
        assert db._horizon_in_year_col("month") == "month_in_year"

    def test_quarter(self):
        assert db._horizon_in_year_col("quarter") == "quarter_in_year"

    def test_season(self):
        assert db._horizon_in_year_col("season") == "season_in_year"


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


# ── _get_snow_single / get_snow_data ─────────────────────────────────────

_SNOW_CONTRACT_COLUMNS = [
    "code", "date", "HS", "norm", "mean", "min", "max",
    "5%", "25%", "50%", "75%", "95%", "last_year", "current_year",
]


def _snow_record(snow_type="HS", value=1.0):
    record = {
        "id": 1,
        "snow_type": snow_type,
        "code": "19999",
        "date": "2026-02-03",
        "value": value,
        "norm": 2.0,
        "mean": 3.0,
        "min": 4.0,
        "max": 5.0,
        "q05": 6.0,
        "q25": 7.0,
        "q50": 8.0,
        "q75": 9.0,
        "q95": 10.0,
        "previous": 11.0,
        "current": 12.0,
    }
    record.update({f"value{i}": float(i) for i in range(1, 15)})
    return record


class TestSnowData:
    def test_get_snow_single_uses_calendar_year_fetch_window_by_default(self, monkeypatch):
        seen_params = []

        def mock_get(url, **kwargs):
            seen_params.append(kwargs["params"])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        db._get_snow_single("19999", "HS", "HS", ref_date=date(2026, 6, 15))

        assert seen_params[0]["start_date"] == "2026-01-01"
        assert seen_params[0]["end_date"] == "2026-12-31"

    def test_get_snow_single_uses_hydrological_fetch_window(self, monkeypatch):
        seen_params = []

        def mock_get(url, **kwargs):
            seen_params.append(kwargs["params"])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        db._get_snow_single(
            "19999",
            "HS",
            "HS",
            display_start_month=9,
            display_start_day=1,
            ref_date=date(2026, 3, 15),
        )

        assert seen_params[0]["start_date"] == "2025-09-01"
        assert seen_params[0]["end_date"] == "2026-08-31"

    def test_get_snow_single_preserves_statistical_fields(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([_snow_record()])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db._get_snow_single("19999", "HS", "HS")

        assert list(result.columns) == _SNOW_CONTRACT_COLUMNS

    def test_get_snow_single_drops_only_service_and_elevation_band_fields(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([_snow_record()])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db._get_snow_single("19999", "HS", "HS")

        dropped_columns = {"snow_type", "id", *{f"value{i}" for i in range(1, 15)}}
        assert dropped_columns.isdisjoint(result.columns)

    def test_get_snow_single_renames_percentiles_to_hydrograph_names(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([_snow_record()])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db._get_snow_single("19999", "HS", "HS")

        assert {"5%", "25%", "50%", "75%", "95%"}.issubset(result.columns)
        assert {"q05", "q25", "q50", "q75", "q95"}.isdisjoint(result.columns)

    def test_get_snow_data_hs_converts_all_stat_columns_to_cm(self, monkeypatch):
        records_by_type = {
            "HS": [_snow_record(snow_type="HS", value=1.0)],
            "ROF": [_snow_record(snow_type="ROF", value=10.0)],
            "SWE": [_snow_record(snow_type="SWE", value=20.0)],
        }

        def mock_get(url, **kwargs):
            snow_type = kwargs["params"]["snow_type"]
            return _make_mock_response(records_by_type[snow_type])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_snow_data("19999")

        stat_columns = [
            "HS", "norm", "mean", "min", "max",
            "5%", "25%", "50%", "75%", "95%", "last_year", "current_year",
        ]
        original = _snow_record()
        original_by_column = {
            "HS": original["value"],
            "norm": original["norm"],
            "mean": original["mean"],
            "min": original["min"],
            "max": original["max"],
            "5%": original["q05"],
            "25%": original["q25"],
            "50%": original["q50"],
            "75%": original["q75"],
            "95%": original["q95"],
            "last_year": original["previous"],
            "current_year": original["current"],
        }
        for column in stat_columns:
            assert result["HS"][column].iloc[0] == original_by_column[column] * 100

        assert result["RoF"]["RoF"].iloc[0] == 10.0
        assert result["RoF"]["norm"].iloc[0] == 2.0
        assert result["RoF"]["mean"].iloc[0] == 3.0
        assert result["RoF"]["5%"].iloc[0] == 6.0
        assert result["SWE"]["SWE"].iloc[0] == 20.0
        assert result["SWE"]["norm"].iloc[0] == 2.0
        assert result["SWE"]["mean"].iloc[0] == 3.0
        assert result["SWE"]["5%"].iloc[0] == 6.0

    def test_get_snow_single_empty_response_has_expected_contract(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db._get_snow_single("19999", "HS", "HS")

        assert len(result) == 0
        assert list(result.columns) == _SNOW_CONTRACT_COLUMNS
        assert result["code"].dtype == object
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
        for column in _SNOW_CONTRACT_COLUMNS[2:]:
            assert result[column].dtype == "float64"


# ── get_long_forecasts ────────────────────────────────────────────────────

# Shared fixture data used across multiple tests.
_LONG_FORECAST_RECORD = {
    "id": 1,
    "horizon_type": "month",
    "horizon_value": 1,
    "code": "99001",
    "date": "2026-03-22",
    "model_type": "GBT",
    "model_type_description": "Gradient Boosted Trees (GBT)",
    "valid_from": "2026-04-01",
    "valid_to": "2026-04-30",
    "flag": 0,
    "composition": "",
    "q": 123.45,
    "q_obs": None,
    "q_xgb": None,
    "q_lgbm": None,
    "q_catboost": None,
    "q_loc": None,
    "q05": 100.0,
    "q10": 105.0,
    "q25": 110.0,
    "q50": 120.0,
    "q75": 130.0,
    "q90": 135.0,
    "q95": 140.0,
}

_SKILL_METRIC_RECORD = {
    "id": 1,
    "horizon_type": "month",
    "horizon_in_year": 4,
    "code": "99001",
    "model_type": "GBT",
    "model_type_description": "Gradient Boosted Trees (GBT)",
    "date": "2026-03-15",
    "sdivsigma": 0.5,
    "nse": 0.8,
    "delta": 1.0,
    "accuracy": 90.0,
    "mae": 1.0,
    "n_pairs": 12,
    "crps": None,
    "pbias": None,
    "kgelf": None,
    "nse_log": None,
    "fhv": None,
    "flv": None,
}

_QUARTER_FORECAST_RECORD_19999 = {
    "id": 20,
    "horizon_type": "quarter",
    "horizon_value": 1,
    "code": "19999",
    "date": "2026-03-22",
    "model_type": "LR_Base",
    "model_type_description": "Linear regression base",
    "valid_from": "2026-04-01",
    "valid_to": "2026-06-30",
    "flag": 0,
    "composition": "",
    "q": 200.0,
    "q_obs": None,
    "q_xgb": None,
    "q_lgbm": None,
    "q_catboost": None,
    "q_loc": None,
    "q05": 180.0,
    "q10": 185.0,
    "q25": 190.0,
    "q50": 200.0,
    "q75": 210.0,
    "q90": 215.0,
    "q95": 220.0,
}

_SEASON_FORECAST_RECORD_19999 = {
    **_QUARTER_FORECAST_RECORD_19999,
    "id": 30,
    "horizon_type": "season",
    "horizon_value": 1,
    "valid_to": "2026-09-30",
    "q": 300.0,
    "q05": 270.0,
    "q95": 330.0,
}


def _skill_metric_record_19999(horizon, horizon_in_year, model_type, delta):
    return {
        "id": 100 + int(delta * 10),
        "horizon_type": horizon,
        "horizon_in_year": horizon_in_year,
        "code": "19999",
        "model_type": model_type,
        "model_type_description": model_type,
        "date": "2026-03-15",
        "sdivsigma": 0.5 + delta,
        "nse": 0.8,
        "delta": delta,
        "accuracy": 90.0 + delta,
        "mae": 1.0 + delta,
        "n_pairs": 12,
        "crps": None,
        "pbias": None,
        "kgelf": None,
        "nse_log": None,
        "fhv": None,
        "flv": None,
    }


def _make_mock_response(json_data, status_code=200):
    """Return a lightweight fake requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


class TestGetLongForecasts:
    def test_month_in_year_computed_from_valid_from(self, monkeypatch):
        """month_in_year is derived from valid_from (April → 4), horizon_value dropped."""

        def mock_get(url, **kwargs):
            return _make_mock_response([_LONG_FORECAST_RECORD])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts(station="99001", horizon_value=1)

        assert "month_in_year" in result.columns
        assert result["month_in_year"].iloc[0] == 4
        assert "horizon_value" not in result.columns
        assert "forecasted_discharge" in result.columns

    def test_empty_api_response(self, monkeypatch):
        """Empty API payload returns an empty DataFrame that still declares key columns."""

        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts(station="99001", horizon_value=1)

        assert result.empty
        assert "month_in_year" in result.columns
        assert "valid_from" in result.columns


# ── get_forecast_stats ────────────────────────────────────────────────────


class TestGetForecastStats:
    def test_month_horizon_renames_to_month_in_year(self, monkeypatch):
        """horizon_in_year is renamed to month_in_year for month horizon."""

        def mock_get(url, **kwargs):
            return _make_mock_response([_SKILL_METRIC_RECORD])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "99001")

        assert "month_in_year" in result.columns
        assert result["month_in_year"].iloc[0] == 4
        assert "pentad_in_year" not in result.columns
        assert "delta" in result.columns
        assert "sdivsigma" in result.columns
        assert "mae" in result.columns
        assert "accuracy" in result.columns
        assert "date" not in result.columns

    def test_month_deduplicates_keeping_latest(self, monkeypatch):
        """When two rows share (code, month_in_year, model_type), only the later date survives."""
        records = [
            {**_SKILL_METRIC_RECORD, "id": 1, "date": "2026-03-01", "delta": 1.0},
            {**_SKILL_METRIC_RECORD, "id": 2, "date": "2026-03-15", "delta": 2.0},
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "99001")

        assert len(result) == 1
        assert result["delta"].iloc[0] == 2.0

    @pytest.mark.parametrize(
        ("horizon", "period_col", "period_value"),
        [
            ("quarter", "quarter_in_year", 2),
            ("season", "season_in_year", 1),
        ],
    )
    def test_long_horizons_rename_horizon_in_year(
        self, horizon, period_col, period_value, monkeypatch
    ):
        """Quarter and season stats use horizon-specific period keys."""
        records = [
            _skill_metric_record_19999(horizon, period_value, "LR_Base", 1.0)
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats(horizon, "19999")

        assert period_col in result.columns
        assert result[period_col].iloc[0] == period_value
        assert "pentad_in_year" not in result.columns
        assert "delta" in result.columns
        assert "sdivsigma" in result.columns
        assert "mae" in result.columns
        assert "accuracy" in result.columns

    @pytest.mark.parametrize(
        ("horizon", "period_col"),
        [
            ("quarter", "quarter_in_year"),
            ("season", "season_in_year"),
        ],
    )
    def test_long_horizons_empty_stats_keep_period_key(
        self, horizon, period_col, monkeypatch
    ):
        """Empty skill-metric responses still declare the right merge key."""

        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats(horizon, "19999")

        assert result.empty
        assert period_col in result.columns
        assert "pentad_in_year" not in result.columns


class TestGetForecastStatsAll:
    @pytest.mark.parametrize(
        ("horizon", "period_col", "period_value"),
        [
            ("quarter", "quarter_in_year", 2),
            ("season", "season_in_year", 1),
        ],
    )
    def test_long_horizons_page_and_rename(
        self, horizon, period_col, period_value, monkeypatch
    ):
        """All-station stats use the same horizon-specific period keys."""
        records = [
            _skill_metric_record_19999(horizon, period_value, "LR_Base", 1.0)
        ]

        def mock_get(url, **kwargs):
            limit = kwargs["params"]["limit"]
            assert limit == 1000
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats_all(horizon)

        assert period_col in result.columns
        assert result[period_col].iloc[0] == period_value
        assert "pentad_in_year" not in result.columns

    @pytest.mark.parametrize(
        ("horizon", "period_col"),
        [
            ("quarter", "quarter_in_year"),
            ("season", "season_in_year"),
        ],
    )
    def test_long_horizons_empty_all_stats_keep_period_key(
        self, horizon, period_col, monkeypatch
    ):
        """Empty all-station stats declare the right period key."""

        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats_all(horizon)

        assert result.empty
        assert period_col in result.columns
        assert "pentad_in_year" not in result.columns


# ── _get_data_monthly / get_data ──────────────────────────────────────────


class TestGetDataMonthly:
    """Integration tests for get_data("month", ...) — all HTTP mocked."""

    def _make_dispatch_mock(self, monkeypatch):
        """Patch requests.get to dispatch by URL segment."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_LONG_FORECAST_RECORD])
            if "/skill-metric/" in url:
                return _make_mock_response([_SKILL_METRIC_RECORD])
            # All other endpoints (hydrograph, meteo, snow, …) return empty.
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

    def _patch_processing(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.processing.add_labels_to_hydrograph",
            lambda df, stations: df,
        )
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def _all_stations_df(self):
        return pd.DataFrame(
            {"code": ["99001"], "station_labels": ["Test River A"]}
        )

    def _all_stations_19999_df(self):
        return pd.DataFrame(
            {"code": ["19999"], "station_labels": ["Test Reservoir B"]}
        )

    def _monthly_forecast_19999(self):
        return {
            **_LONG_FORECAST_RECORD,
            "id": 40,
            "code": "19999",
            "model_type": "LR_Base",
            "model_type_description": "Linear regression base",
            "q": 150.0,
        }

    def test_merges_quarter_skill_metrics_into_monthly_quarter_frame(self, monkeypatch):
        """Month tab data enriches long_forecasts_quarter without changing forecasts_all."""
        monthly_forecast = self._monthly_forecast_19999()
        monthly_skill = _skill_metric_record_19999("month", 4, "LR_Base", 3.0)
        quarter_skill = _skill_metric_record_19999("quarter", 2, "LR_Base", 4.0)

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url and params.get("horizon_type") == "month":
                return _make_mock_response([monthly_forecast])
            if "/long-forecast/" in url and params.get("horizon_type") == "quarter":
                return _make_mock_response([_QUARTER_FORECAST_RECORD_19999])
            if "/skill-metric/" in url and params.get("horizon") == "month":
                return _make_mock_response([monthly_skill])
            if "/skill-metric/" in url and params.get("horizon") == "quarter":
                return _make_mock_response([quarter_skill])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_stations_19999_df())

        fa = data["forecasts_all"]
        month_row = fa[(fa["code"] == "19999") & (fa["model_short"] == "LR_Base")]
        assert len(month_row) == 1
        assert month_row["month_in_year"].iloc[0] == 4
        assert month_row["delta"].iloc[0] == 3.0
        assert month_row["sdivsigma"].iloc[0] == 3.5
        assert month_row["mae"].iloc[0] == 4.0
        assert month_row["accuracy"].iloc[0] == 93.0

        quarter = data["long_forecasts_quarter"]
        quarter_row = quarter[
            (quarter["code"] == "19999") & (quarter["model_short"] == "LR_Base")
        ]
        assert len(quarter_row) == 1
        assert quarter_row["quarter_in_year"].iloc[0] == 2
        assert quarter_row["delta"].iloc[0] == 4.0
        assert quarter_row["sdivsigma"].iloc[0] == 4.5
        assert quarter_row["mae"].iloc[0] == 5.0
        assert quarter_row["accuracy"].iloc[0] == 94.0

    def test_monthly_quarter_frame_preserves_unmatched_rows_with_nan_metrics(
        self, monkeypatch
    ):
        """Unmatched quarter forecast models stay present with NaN skill metrics."""
        monthly_forecast = self._monthly_forecast_19999()
        monthly_skill = _skill_metric_record_19999("month", 4, "LR_Base", 1.0)
        quarter_forecasts = [
            _QUARTER_FORECAST_RECORD_19999,
            {
                **_QUARTER_FORECAST_RECORD_19999,
                "id": 41,
                "model_type": "LR_SM",
                "model_type_description": "Linear regression snowmelt",
                "q": 220.0,
            },
        ]
        quarter_skill = _skill_metric_record_19999("quarter", 2, "LR_Base", 2.0)

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url and params.get("horizon_type") == "month":
                return _make_mock_response([monthly_forecast])
            if "/long-forecast/" in url and params.get("horizon_type") == "quarter":
                return _make_mock_response(quarter_forecasts)
            if "/skill-metric/" in url and params.get("horizon") == "month":
                return _make_mock_response([monthly_skill])
            if "/skill-metric/" in url and params.get("horizon") == "quarter":
                return _make_mock_response([quarter_skill])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_stations_19999_df())

        quarter = data["long_forecasts_quarter"]
        assert set(quarter["model_short"]) == {"LR_Base", "LR_SM"}
        base = quarter[quarter["model_short"] == "LR_Base"].iloc[0]
        sm = quarter[quarter["model_short"] == "LR_SM"].iloc[0]
        assert base["delta"] == 2.0
        assert base["sdivsigma"] == 2.5
        assert pd.isna(sm["delta"])
        assert pd.isna(sm["sdivsigma"])
        assert pd.isna(sm["mae"])
        assert pd.isna(sm["accuracy"])

    def test_monthly_quarter_frame_no_matching_skill_rows_preserves_forecasts(
        self, monkeypatch
    ):
        """Quarter forecasts are not dropped when quarter skill rows do not match."""
        monthly_forecast = self._monthly_forecast_19999()
        monthly_skill = _skill_metric_record_19999("month", 4, "LR_Base", 1.0)
        unmatched_quarter_skill = _skill_metric_record_19999(
            "quarter", 3, "LR_Base", 2.0
        )

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url and params.get("horizon_type") == "month":
                return _make_mock_response([monthly_forecast])
            if "/long-forecast/" in url and params.get("horizon_type") == "quarter":
                return _make_mock_response([_QUARTER_FORECAST_RECORD_19999])
            if "/skill-metric/" in url and params.get("horizon") == "month":
                return _make_mock_response([monthly_skill])
            if "/skill-metric/" in url and params.get("horizon") == "quarter":
                return _make_mock_response([unmatched_quarter_skill])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_stations_19999_df())

        quarter = data["long_forecasts_quarter"]
        assert len(quarter) == 1
        assert quarter["forecasted_discharge"].iloc[0] == 200.0
        assert quarter["quarter_in_year"].iloc[0] == 2
        assert pd.isna(quarter["delta"].iloc[0])
        assert pd.isna(quarter["sdivsigma"].iloc[0])
        assert pd.isna(quarter["mae"].iloc[0])
        assert pd.isna(quarter["accuracy"].iloc[0])

    def test_empty_monthly_quarter_frame_does_not_synthesize_rows(self, monkeypatch):
        """Empty quarter long forecasts do not crash or create merged rows."""
        monthly_forecast = self._monthly_forecast_19999()
        monthly_skill = _skill_metric_record_19999("month", 4, "LR_Base", 1.0)
        quarter_skill = _skill_metric_record_19999("quarter", 2, "LR_Base", 2.0)

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url and params.get("horizon_type") == "month":
                return _make_mock_response([monthly_forecast])
            if "/long-forecast/" in url and params.get("horizon_type") == "quarter":
                return _make_mock_response([])
            if "/skill-metric/" in url and params.get("horizon") == "month":
                return _make_mock_response([monthly_skill])
            if "/skill-metric/" in url and params.get("horizon") == "quarter":
                return _make_mock_response([quarter_skill])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_stations_19999_df())

        assert data["long_forecasts_quarter"].empty
        assert "forecasted_discharge" in data["long_forecasts_quarter"].columns
        assert "quarter_in_year" in data["long_forecasts_quarter"].columns

    def test_merges_skill_metrics_into_forecasts(self, monkeypatch):
        """Skill metric columns (delta, sdivsigma, mae, accuracy) appear in forecasts_all."""
        self._make_dispatch_mock(monkeypatch)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "99001", self._all_stations_df())

        fa = data["forecasts_all"]
        assert "delta" in fa.columns
        assert "sdivsigma" in fa.columns
        assert "mae" in fa.columns
        assert "accuracy" in fa.columns
        # The GBT row should carry delta=1.0 from the skill-metric fixture.
        gbt_rows = fa[fa["model_short"] == "GBT"]
        assert len(gbt_rows) > 0
        assert gbt_rows["delta"].iloc[0] == 1.0

        assert not data["forecast_stats"].empty

    def test_no_skill_metrics_still_returns_forecasts(self, monkeypatch):
        """When skill-metric API returns nothing, forecasts_all still has forecast data."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_LONG_FORECAST_RECORD])
            # skill-metric and all other endpoints return empty.
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "99001", self._all_stations_df())

        fa = data["forecasts_all"]
        assert not fa.empty
        assert "forecasted_discharge" in fa.columns
        assert "delta" not in fa.columns
        assert data["forecast_stats"].empty

    def test_merges_skill_metrics_into_month0_forecasts(self, monkeypatch):
        """Skill metric columns appear in long_forecasts_m0 when month_0 is enabled."""
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "month_0,month_1"
        )
        self._make_dispatch_mock(monkeypatch)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "99001", self._all_stations_df())

        m0 = data["long_forecasts_m0"]
        assert not m0.empty
        assert "delta" in m0.columns
        assert "sdivsigma" in m0.columns
        assert "mae" in m0.columns
        assert "accuracy" in m0.columns
        # The GBT row should carry delta=1.0 from the skill-metric fixture.
        gbt_rows = m0[m0["model_short"] == "GBT"]
        assert len(gbt_rows) > 0
        assert gbt_rows["delta"].iloc[0] == 1.0

    def test_month0_without_skill_metrics_still_returns_forecasts(self, monkeypatch):
        """When skill-metric API returns nothing, month_0 forecasts are still present."""
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "month_0,month_1"
        )

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_LONG_FORECAST_RECORD])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "99001", self._all_stations_df())

        m0 = data["long_forecasts_m0"]
        assert not m0.empty
        assert "forecasted_discharge" in m0.columns
        assert "delta" not in m0.columns

    def test_month0_disabled_returns_empty_dataframe(self, monkeypatch):
        """When month_0 is not in supported modes, long_forecasts_m0 is empty."""
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "month_1"
        )
        self._make_dispatch_mock(monkeypatch)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "99001", self._all_stations_df())

        assert data["long_forecasts_m0"].empty


# ── get_long_forecasts_quarter / get_long_forecasts_season ────────────────

_QUARTER_FORECAST_RECORD = {
    "id": 2,
    "horizon_type": "quarter",
    "horizon_value": 1,
    "code": "99001",
    "date": "2026-03-22",
    "model_type": "GBT",
    "model_type_description": "Gradient Boosted Trees (GBT)",
    "valid_from": "2026-04-01",
    "valid_to": "2026-06-30",
    "flag": 0,
    "composition": "",
    "q": 200.0,
    "q_obs": None,
    "q_xgb": None,
    "q_lgbm": None,
    "q_catboost": None,
    "q_loc": None,
    "q05": 180.0,
    "q10": 185.0,
    "q25": 190.0,
    "q50": 200.0,
    "q75": 210.0,
    "q90": 215.0,
    "q95": 220.0,
}

_SEASON_FORECAST_RECORD = {
    **_QUARTER_FORECAST_RECORD,
    "id": 3,
    "horizon_type": "season",
    "horizon_value": 2,
    "q": 300.0,
    "q05": 270.0,
    "q95": 330.0,
}


class TestGetLongForecastsQuarter:
    def test_renames_and_latest_dedup(self, monkeypatch):
        """Two rows same (code, model_short) — only latest date survives."""
        older = {**_QUARTER_FORECAST_RECORD, "date": "2026-03-01"}
        newer = {**_QUARTER_FORECAST_RECORD, "date": "2026-03-22"}

        def mock_get(url, **kwargs):
            return _make_mock_response([older, newer])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="99001")

        assert "forecasted_discharge" in result.columns
        assert len(result) == 1
        assert str(result["date"].iloc[0].date()) == "2026-03-22"

    def test_empty_api_response(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="99001")

        assert result.empty
        assert "forecasted_discharge" in result.columns

    def test_quarter_in_year_computed_from_valid_from(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([_QUARTER_FORECAST_RECORD_19999])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999")

        assert "quarter_in_year" in result.columns
        assert result["quarter_in_year"].iloc[0] == 2

    def test_empty_api_response_declares_quarter_key(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999")

        assert result.empty
        assert "quarter_in_year" in result.columns


class TestGetLongForecastsSeason:
    def test_renames_and_latest_dedup(self, monkeypatch):
        """Two rows same (code, model_short) — only latest date survives."""
        older = {**_SEASON_FORECAST_RECORD, "date": "2026-02-15"}
        newer = {**_SEASON_FORECAST_RECORD, "date": "2026-03-22"}

        def mock_get(url, **kwargs):
            return _make_mock_response([older, newer])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_season(station="99001")

        assert "forecasted_discharge" in result.columns
        assert len(result) == 1
        assert str(result["date"].iloc[0].date()) == "2026-03-22"

    def test_empty_api_response(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_season(station="99001")

        assert result.empty
        assert "forecasted_discharge" in result.columns

    def test_season_in_year_is_single_bucket(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([_SEASON_FORECAST_RECORD_19999])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_season(station="19999")

        assert "season_in_year" in result.columns
        assert result["season_in_year"].iloc[0] == 1

    def test_empty_api_response_declares_season_key(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_season(station="19999")

        assert result.empty
        assert "season_in_year" in result.columns


# ── _get_data_quarter / _get_data_season ─────────────────────────────────


class TestGetDataQuarter:
    """Integration tests for get_data("quarter", ...) — all HTTP mocked."""

    def _patch_processing(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.processing.add_labels_to_hydrograph",
            lambda df, stations: df,
        )
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def _all_stations_df(self):
        return pd.DataFrame({"code": ["99001"], "station_labels": ["Test River A"]})

    def test_returns_required_keys(self, monkeypatch):
        """get_data('quarter') returns dict with all required keys."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_QUARTER_FORECAST_RECORD])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("quarter", "99001", self._all_stations_df())

        for key in ("hydrograph_day_all", "hydrograph_pentad_all", "rain", "temp",
                    "snow_data", "ml_forecast", "linreg_predictor",
                    "forecasts_all", "forecast_stats"):
            assert key in data, f"Missing key: {key}"

    def test_forecast_stats_populated_and_merged(self, monkeypatch):
        """Quarter skill metrics populate forecast_stats and merge into forecasts_all."""
        forecast = _QUARTER_FORECAST_RECORD_19999
        skill = _skill_metric_record_19999("quarter", 2, "LR_Base", 1.0)

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([forecast])
            if "/skill-metric/" in url:
                return _make_mock_response([skill])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "quarter",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        assert not data["forecast_stats"].empty
        fa = data["forecasts_all"]
        assert "delta" in fa.columns
        assert "sdivsigma" in fa.columns
        assert "mae" in fa.columns
        assert "accuracy" in fa.columns
        row = fa[(fa["code"] == "19999") & (fa["model_short"] == "LR_Base")]
        assert len(row) == 1
        assert row["quarter_in_year"].iloc[0] == 2
        assert row["delta"].iloc[0] == 1.0
        assert row["sdivsigma"].iloc[0] == 1.5
        assert row["mae"].iloc[0] == 2.0
        assert row["accuracy"].iloc[0] == 91.0

    def test_partial_skill_metrics_preserve_unmatched_forecast_row(self, monkeypatch):
        """LR_SM stays present with NaN metrics when only LR_Base has skill data."""
        forecasts = [
            _QUARTER_FORECAST_RECORD_19999,
            {
                **_QUARTER_FORECAST_RECORD_19999,
                "id": 21,
                "model_type": "LR_SM",
                "model_type_description": "Linear regression snowmelt",
                "q": 210.0,
            },
        ]
        skills = [_skill_metric_record_19999("quarter", 2, "LR_Base", 2.0)]

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response(forecasts)
            if "/skill-metric/" in url:
                return _make_mock_response(skills)
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "quarter",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        fa = data["forecasts_all"]
        assert set(fa["model_short"]) == {"LR_Base", "LR_SM"}
        base = fa[fa["model_short"] == "LR_Base"].iloc[0]
        sm = fa[fa["model_short"] == "LR_SM"].iloc[0]
        assert base["delta"] == 2.0
        assert base["sdivsigma"] == 2.5
        assert pd.isna(sm["delta"])
        assert pd.isna(sm["sdivsigma"])
        assert pd.isna(sm["mae"])
        assert pd.isna(sm["accuracy"])

    def test_no_skill_metrics_still_returns_forecasts(self, monkeypatch):
        """Empty quarter skill metrics do not block forecast rows."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_QUARTER_FORECAST_RECORD_19999])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "quarter",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        assert data["forecast_stats"].empty
        assert not data["forecasts_all"].empty
        assert "forecasted_discharge" in data["forecasts_all"].columns
        assert "delta" not in data["forecasts_all"].columns

    def test_no_m0_key(self, monkeypatch):
        """long_forecasts_m0 key must not be present for quarter horizon."""

        def mock_get(url, **kwargs):
            return _make_mock_response([_QUARTER_FORECAST_RECORD])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("quarter", "99001", self._all_stations_df())

        assert "long_forecasts_m0" not in data

    def test_forecasts_all_has_discharge(self, monkeypatch):
        """forecasts_all contains forecasted_discharge when API returns data."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_QUARTER_FORECAST_RECORD])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("quarter", "99001", self._all_stations_df())

        assert "forecasted_discharge" in data["forecasts_all"].columns


class TestGetDataSeason:
    """Integration tests for get_data("season", ...) — all HTTP mocked."""

    def _patch_processing(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.processing.add_labels_to_hydrograph",
            lambda df, stations: df,
        )
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def _all_stations_df(self):
        return pd.DataFrame({"code": ["99001"], "station_labels": ["Test River A"]})

    def test_returns_required_keys(self, monkeypatch):
        """get_data('season') returns dict with all required keys."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_SEASON_FORECAST_RECORD])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("season", "99001", self._all_stations_df())

        for key in ("hydrograph_day_all", "hydrograph_pentad_all", "rain", "temp",
                    "snow_data", "ml_forecast", "linreg_predictor",
                    "forecasts_all", "forecast_stats"):
            assert key in data, f"Missing key: {key}"

    def test_forecast_stats_populated_and_merged(self, monkeypatch):
        """Season skill metrics populate forecast_stats and merge into forecasts_all."""
        forecasts = [
            _SEASON_FORECAST_RECORD_19999,
            {
                **_SEASON_FORECAST_RECORD_19999,
                "id": 31,
                "model_type": "LR_SM",
                "model_type_description": "Linear regression snowmelt",
                "q": 310.0,
            },
        ]
        skills = [
            _skill_metric_record_19999("season", 1, "LR_Base", 1.0),
            _skill_metric_record_19999("season", 1, "LR_SM", 3.0),
        ]

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response(forecasts)
            if "/skill-metric/" in url:
                return _make_mock_response(skills)
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "season",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        assert not data["forecast_stats"].empty
        fa = data["forecasts_all"]
        assert "delta" in fa.columns
        assert "sdivsigma" in fa.columns
        assert "mae" in fa.columns
        assert "accuracy" in fa.columns
        assert set(fa["model_short"]) == {"LR_Base", "LR_SM"}
        base = fa[fa["model_short"] == "LR_Base"].iloc[0]
        sm = fa[fa["model_short"] == "LR_SM"].iloc[0]
        assert base["season_in_year"] == 1
        assert base["delta"] == 1.0
        assert sm["delta"] == 3.0
        assert sm["sdivsigma"] == 3.5

    def test_no_skill_metrics_still_returns_forecasts(self, monkeypatch):
        """Empty season skill metrics do not block forecast rows."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_SEASON_FORECAST_RECORD_19999])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "season",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        assert data["forecast_stats"].empty
        assert not data["forecasts_all"].empty
        assert "forecasted_discharge" in data["forecasts_all"].columns
        assert "delta" not in data["forecasts_all"].columns

    def test_no_m0_key(self, monkeypatch):
        """long_forecasts_m0 key must not be present for season horizon."""

        def mock_get(url, **kwargs):
            return _make_mock_response([_SEASON_FORECAST_RECORD])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("season", "99001", self._all_stations_df())

        assert "long_forecasts_m0" not in data

    def test_forecasts_all_has_discharge(self, monkeypatch):
        """forecasts_all contains forecasted_discharge when API returns data."""

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([_SEASON_FORECAST_RECORD])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("season", "99001", self._all_stations_df())

        assert "forecasted_discharge" in data["forecasts_all"].columns


class TestSeasonSummaryRendering:
    """Deterministic data-layer plus summary-table checks for season metrics."""

    def _patch_processing(self, monkeypatch):
        def add_labels(df, stations):
            if df.empty:
                return df
            return df.assign(station_labels="Test River B")

        monkeypatch.setattr("src.db.processing.add_labels_to_hydrograph", add_labels)
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def _model_selection(self):
        selection = MagicMock()
        selection.options = {
            "LR Base": "LR_Base",
            "LR SM": "LR_SM",
        }
        return selection

    def _summary_table(self, forecasts_all):
        return vizualization.create_forecast_summary_table(
            lambda value: value,
            "season",
            forecasts_all,
            "Test River B",
            "2026-03-22",
            self._model_selection(),
            "delta",
            0,
        )

    def test_season_summary_table_renders_skill_metrics(self, monkeypatch):
        forecasts = [
            _SEASON_FORECAST_RECORD_19999,
            {
                **_SEASON_FORECAST_RECORD_19999,
                "id": 31,
                "model_type": "LR_SM",
                "model_type_description": "Linear regression snowmelt",
                "q": 310.0,
            },
        ]
        skills = [
            _skill_metric_record_19999("season", 1, "LR_Base", 1.0),
            _skill_metric_record_19999("season", 1, "LR_SM", 3.0),
        ]

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response(forecasts)
            if "/skill-metric/" in url:
                return _make_mock_response(skills)
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "season",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )
        table = self._summary_table(data["forecasts_all"])

        assert set(table["Model"]) == {"LR_Base", "LR_SM"}
        base = table[table["Model"] == "LR_Base"].iloc[0]
        sm = table[table["Model"] == "LR_SM"].iloc[0]
        assert base["Accuracy"] == 91.0
        assert base["δ"] == 1.0
        assert base["s/σ"] == 1.5
        assert base["MAE"] == 2.0
        assert sm["Accuracy"] == 93.0
        assert sm["δ"] == 3.0
        assert sm["s/σ"] == 3.5
        assert sm["MAE"] == 4.0

    def test_season_summary_table_without_skill_metrics_does_not_crash(
        self, monkeypatch
    ):
        forecasts = [
            _SEASON_FORECAST_RECORD_19999,
            {
                **_SEASON_FORECAST_RECORD_19999,
                "id": 31,
                "model_type": "LR_SM",
                "model_type_description": "Linear regression snowmelt",
                "q": 310.0,
            },
        ]

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response(forecasts)
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "season",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )
        table = self._summary_table(data["forecasts_all"])

        assert set(table["Model"]) == {"LR_Base", "LR_SM"}
        for column in ("Accuracy", "δ", "s/σ", "MAE"):
            assert column in table.columns
            assert table[column].isna().all()
