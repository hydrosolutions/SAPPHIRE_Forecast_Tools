"""Unit tests for forecast_dashboard/src/db.py.

Tests pure helpers directly, and API-calling functions with mocked HTTP.
"""

import inspect
import json
from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import requests
from src import db, vizualization
from src.snow_window import snow_display_window


@pytest.fixture(autouse=True)
def _long_term_resolver_env(monkeypatch, tmp_path):
    config_dir = tmp_path / "long_term_configs"
    config_dir.mkdir()
    leads = {
        "quarter": 1,
        "seasonal_january": 3,
        "seasonal_february": 2,
        "seasonal_march": 1,
        "seasonal_april": 0,
    }
    for name, lead in leads.items():
        (config_dir / f"{name}.json").write_text(json.dumps({"operational_month_lead_time": lead}))

    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_configuration",
        "long_term_configs",
    )
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_supported_modes",
        ",".join(leads),
    )
    return config_dir


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
    "code",
    "date",
    "HS",
    "norm",
    "mean",
    "min",
    "max",
    "5%",
    "25%",
    "50%",
    "75%",
    "95%",
    "last_year",
    "current_year",
]


def _snow_record(snow_type="HS", value=1.0, **overrides):
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
    record.update(overrides)
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

    def test_get_snow_single_sorts_rows_by_date(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response(
                [
                    _snow_record(id=3, date="2026-02-03", value=3.0),
                    _snow_record(id=1, date="2026-02-01", value=1.0),
                    _snow_record(id=2, date="2026-02-02", value=2.0),
                ]
            )

        monkeypatch.setattr(requests, "get", mock_get)

        result = db._get_snow_single("19999", "HS", "HS")

        assert result["date"].tolist() == [
            pd.Timestamp("2026-02-01"),
            pd.Timestamp("2026-02-02"),
            pd.Timestamp("2026-02-03"),
        ]
        assert result["HS"].tolist() == [1.0, 2.0, 3.0]

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
            "HS",
            "norm",
            "mean",
            "min",
            "max",
            "5%",
            "25%",
            "50%",
            "75%",
            "95%",
            "last_year",
            "current_year",
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


class TestGetDataThreadsSnowRefDate:
    """get_data(..., snow_ref_date=...) must reach get_snow_data unchanged.

    Regression test for the two-reference-date snow bug: db.get_data is the
    only place the dashboard's data layer should decide the snow reference
    date, and it must hand that SAME date to get_snow_data rather than
    letting get_snow_data fall back to date.today() on its own.
    """

    def _all_stations_df(self):
        return pd.DataFrame({"code": ["19999"], "station_labels": ["Test River"]})

    def _stub_non_snow_endpoints(self, monkeypatch):
        empty = pd.DataFrame()
        monkeypatch.setattr(db, "get_hydrograph_day_all", lambda station: empty)
        monkeypatch.setattr(db, "get_hydrograph_pentad_all", lambda horizon, station: empty)
        monkeypatch.setattr(db, "get_rain", lambda station: empty)
        monkeypatch.setattr(db, "get_temp", lambda station: empty)
        monkeypatch.setattr(db, "get_ml_forecast", lambda horizon, station: empty)
        monkeypatch.setattr(db, "get_linreg_predictor", lambda horizon, station: empty)
        monkeypatch.setattr(db, "get_forecasts_all", lambda horizon, station: empty)
        monkeypatch.setattr(db, "get_forecast_stats", lambda horizon, station: empty)
        monkeypatch.setattr(
            "src.db.processing.add_labels_to_hydrograph", lambda df, stations: df
        )
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def _capture_snow_read_data(self, monkeypatch):
        """Stub the single HTTP-performing function `_read_data` instead of
        `get_snow_data`, so the assertion exercises the layer where the
        two-reference-date bug actually lived: `_get_snow_single` computing
        its own `date.today()` while accepting-and-ignoring `ref_date`. A
        regression that re-hardcodes `date.today()` there while still
        threading `snow_ref_date` through unused would keep a
        `get_snow_data`-stubbed test green but must fail this one.

        Returns the list `seen_params` that every `("preprocessing",
        "snow", params)` call appends its `params` dict to (all non-snow
        fetchers are stubbed directly and never reach `_read_data`).
        """
        seen_params = []

        def fake_read_data(service_type, data_type, params=None):
            if service_type == "preprocessing" and data_type == "snow":
                seen_params.append(params)
            return pd.DataFrame()

        monkeypatch.setattr(db, "_read_data", fake_read_data)
        return seen_params

    def test_get_data_passes_snow_ref_date_to_snow_query_window(self, monkeypatch):
        self._stub_non_snow_endpoints(monkeypatch)
        seen_params = self._capture_snow_read_data(monkeypatch)

        # Deliberately NOT today's date: a mutation that ignores snow_ref_date
        # and falls back to date.today() inside _get_snow_single must produce
        # a different (today-anchored) window and fail this assertion no
        # matter which real-world day the suite happens to run on.
        db.get_data(
            "pentad",
            "19999",
            self._all_stations_df(),
            snow_display_start_month=9,
            snow_display_start_day=1,
            snow_ref_date=date(2024, 9, 2),
        )

        # One /snow/ request per snow type (HS, RoF, SWE); all must carry
        # the same window derived from the threaded snow_ref_date.
        assert len(seen_params) == 3
        for params in seen_params:
            assert params["start_date"] == "2024-09-01"
            assert params["end_date"] == "2025-08-31"

    def test_get_data_defaults_snow_ref_date_to_todays_window(self, monkeypatch):
        """No caller passing a date must still work: the resulting query
        window must be exactly what `snow_display_window` returns for
        `date.today()` — not merely that the argument was `None`, which
        would stay green even if `snow_ref_date` were silently dropped
        somewhere in `get_data`'s call chain."""
        self._stub_non_snow_endpoints(monkeypatch)
        seen_params = self._capture_snow_read_data(monkeypatch)

        db.get_data(
            "pentad",
            "19999",
            self._all_stations_df(),
            snow_display_start_month=9,
            snow_display_start_day=1,
        )

        expected_begin, expected_end = snow_display_window(9, 1, date.today())
        assert len(seen_params) == 3
        for params in seen_params:
            assert params["start_date"] == expected_begin.strftime("%Y-%m-%d")
            assert params["end_date"] == expected_end.strftime("%Y-%m-%d")


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


# ── get_long_forecasts: M1 P3 lead-aware horizon_value preservation ───────


class TestGetLongForecastsLeadAware:
    """M1 P3: under SAPPHIRE_SKILL_LEAD_AWARE, horizon_value (lead) survives
    get_long_forecasts so callers can merge/dedup on the lead key instead of
    collapsing all leads together."""

    def test_horizon_value_preserved_when_flag_on(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        def mock_get(url, **kwargs):
            return _make_mock_response([_LONG_FORECAST_RECORD])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts(station="99001", horizon_value=1)

        assert "horizon_value" in result.columns
        assert result["horizon_value"].iloc[0] == 1

    def test_empty_schema_declares_horizon_value_when_flag_on(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts(station="99001", horizon_value=1)

        assert result.empty
        assert "horizon_value" in result.columns

    def test_horizon_value_still_dropped_when_flag_explicitly_off(self, monkeypatch):
        """An explicit falsey token behaves identically to unset (byte-identical golden)."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "false")

        def mock_get(url, **kwargs):
            return _make_mock_response([_LONG_FORECAST_RECORD])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts(station="99001", horizon_value=1)

        assert "horizon_value" not in result.columns


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
        records = [_skill_metric_record_19999(horizon, period_value, "LR_Base", 1.0)]

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
    def test_long_horizons_empty_stats_keep_period_key(self, horizon, period_col, monkeypatch):
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
    def test_long_horizons_page_and_rename(self, horizon, period_col, period_value, monkeypatch):
        """All-station stats use the same horizon-specific period keys."""
        records = [_skill_metric_record_19999(horizon, period_value, "LR_Base", 1.0)]

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
    def test_long_horizons_empty_all_stats_keep_period_key(self, horizon, period_col, monkeypatch):
        """Empty all-station stats declare the right period key."""

        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats_all(horizon)

        assert result.empty
        assert period_col in result.columns
        assert "pentad_in_year" not in result.columns


# ── PP-038: get_forecast_stats month per-lead dedup ───────────────────────


def _skill_metric_record_with_lead(horizon_in_year, model_type, horizon_value, delta=1.0):
    """Skill metric record that includes horizon_value (post-PP-038 API response)."""
    return {
        "id": 200 + int(delta * 10) + horizon_value,
        "horizon_type": "month",
        "horizon_in_year": horizon_in_year,
        "code": "19999",
        "model_type": model_type,
        "model_type_description": model_type,
        "date": "2026-03-15",
        "horizon_value": horizon_value,
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


class TestGetForecastStatsPP038:
    """PP-038: get_forecast_stats preserves per-lead rows when horizon_value is present.

    After PP-038, the API returns multiple rows per (month_in_year, code, model_short)
    — one per lead (horizon_value 0, 1, 2, 3).  get_forecast_stats must NOT collapse
    them to one row.
    """

    def test_month_per_lead_rows_preserved(self, monkeypatch):
        """Two rows differing only in horizon_value survive the dedup.

        This guards the regression where drop_duplicates on
        (code, month_in_year, model_short) collapses 4 leads to 1.
        """
        records = [
            _skill_metric_record_with_lead(3, "GBT", 0, delta=1.0),
            _skill_metric_record_with_lead(3, "GBT", 1, delta=1.5),
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "19999")

        # Both leads must survive — not collapsed to one
        assert len(result) == 2, (
            f"Expected 2 rows (one per lead), got {len(result)}: {result[['month_in_year', 'model_short', 'horizon_value']].to_dict('records') if not result.empty else '(empty)'}"
        )
        assert "horizon_value" in result.columns
        assert set(result["horizon_value"].unique()) == {0, 1}

    def test_month_same_lead_dedup_keeps_latest_date(self, monkeypatch):
        """Two rows with the same lead but different recalc dates → keep the later date."""
        records = [
            {**_skill_metric_record_with_lead(4, "GBT", 0, delta=1.0), "date": "2026-03-01"},
            {**_skill_metric_record_with_lead(4, "GBT", 0, delta=2.0), "date": "2026-03-15"},
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "19999")

        assert len(result) == 1
        assert result["delta"].iloc[0] == 2.0

    def test_non_month_horizons_unchanged(self, monkeypatch):
        """Quarter and season records without horizon_value field still deduplicate correctly."""
        records = [
            _skill_metric_record_19999("quarter", 2, "LR_Base", 1.0),
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("quarter", "19999")

        assert len(result) == 1
        assert "quarter_in_year" in result.columns


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
        return pd.DataFrame({"code": ["99001"], "station_labels": ["Test River A"]})

    def _all_stations_19999_df(self):
        return pd.DataFrame({"code": ["19999"], "station_labels": ["Test Reservoir B"]})

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
        quarter_row = quarter[(quarter["code"] == "19999") & (quarter["model_short"] == "LR_Base")]
        assert len(quarter_row) == 1
        assert quarter_row["quarter_in_year"].iloc[0] == 2
        assert quarter_row["delta"].iloc[0] == 4.0
        assert quarter_row["sdivsigma"].iloc[0] == 4.5
        assert quarter_row["mae"].iloc[0] == 5.0
        assert quarter_row["accuracy"].iloc[0] == 94.0

    def test_monthly_quarter_frame_preserves_unmatched_rows_with_nan_metrics(self, monkeypatch):
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

    def test_monthly_quarter_frame_no_matching_skill_rows_preserves_forecasts(self, monkeypatch):
        """Quarter forecasts are not dropped when quarter skill rows do not match."""
        monthly_forecast = self._monthly_forecast_19999()
        monthly_skill = _skill_metric_record_19999("month", 4, "LR_Base", 1.0)
        unmatched_quarter_skill = _skill_metric_record_19999("quarter", 3, "LR_Base", 2.0)

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
        # Legacy (flag-off) contract: the m0 card is annotated from the lead-1
        # stats frame; asserted here without providing a monthly config.
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "false")
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_0,month_1")
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
        # Legacy (flag-off) contract: asserted without a monthly config present.
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "false")
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_0,month_1")

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
        # Legacy (flag-off) contract: asserted without a monthly config present.
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "false")
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1")
        self._make_dispatch_mock(monkeypatch)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "99001", self._all_stations_df())

        assert data["long_forecasts_m0"].empty


# ── _get_data_monthly: M1 P3 config-driven per-lead monthly display ───────


class TestGetDataMonthlyLeadAware:
    """M1 P3: monthly display becomes config-driven over ALL supported
    month_N leads under the flag, instead of hardcoding horizon_value=1/0.

    Critical org concern: taj month_1 == LEAD 0 (kyg month_1 == lead 1).
    Hardcoding horizon_value=1 for the primary monthly tile silently hides
    taj's flagship monthly forecast.
    """

    def _patch_processing(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.processing.add_labels_to_hydrograph",
            lambda df, stations: df,
        )
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def _write_month_config(self, config_dir, mode, lead, issue_day=25):
        (config_dir / f"{mode}.json").write_text(
            json.dumps({"operational_month_lead_time": lead, "operational_issue_day": issue_day})
        )

    def test_taj_style_month1_lead0_forecast_visible_when_flag_on(
        self, monkeypatch, _long_term_resolver_env
    ):
        """taj: month_1's TRUE operational lead is 0. The flag must resolve
        the lead from config instead of hardcoding horizon_value=1."""
        config_dir = _long_term_resolver_env
        self._write_month_config(config_dir, "month_1", lead=0, issue_day=1)
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        taj_forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 0}
        taj_skill = _skill_metric_record_with_lead(4, "GBT", 0, delta=1.0)

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                if params.get("horizon_value") == 0:
                    return _make_mock_response([taj_forecast])
                return _make_mock_response([])
            if "/skill-metric/" in url:
                return _make_mock_response([taj_skill])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "month", "19999", pd.DataFrame({"code": ["19999"], "station_labels": ["x"]})
        )

        fa = data["forecasts_all"]
        assert not fa.empty, "taj lead-0 monthly forecast must be visible, not filtered out"
        assert (fa["horizon_value"] == 0).all()
        assert fa["delta"].iloc[0] == 1.0  # skill metrics correctly attached to lead 0

    def test_taj_style_month1_lead0_forecast_hidden_when_flag_off(
        self, monkeypatch, _long_term_resolver_env
    ):
        """Documents the pre-M1 bug: flag OFF hardcodes horizon_value=1, so
        taj's lead-0 forecast (only queryable at horizon_value=0) comes back
        empty. This is the flag-OFF golden — it must NOT change."""
        config_dir = _long_term_resolver_env
        self._write_month_config(config_dir, "month_1", lead=0, issue_day=1)
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1")
        # SAPPHIRE_SKILL_LEAD_AWARE intentionally left unset (default OFF).

        taj_forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 0}

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                if params.get("horizon_value") == 0:
                    return _make_mock_response([taj_forecast])
                return _make_mock_response([])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "month", "19999", pd.DataFrame({"code": ["19999"], "station_labels": ["x"]})
        )

        assert data["forecasts_all"].empty

    def test_month2_and_month3_leads_available_via_by_mode_dict_when_flag_on(
        self, monkeypatch, _long_term_resolver_env
    ):
        """kyg-style: month_1/2/3 == lead 1/2/3. hv2/hv3 must be reachable
        and each carry their own lead's forecasts + skill metrics."""
        config_dir = _long_term_resolver_env
        self._write_month_config(config_dir, "month_1", lead=1)
        self._write_month_config(config_dir, "month_2", lead=2)
        self._write_month_config(config_dir, "month_3", lead=3)
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "month_1,month_2,month_3"
        )
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        def forecast_for_lead(lead):
            return {
                **_LONG_FORECAST_RECORD,
                "code": "19999",
                "horizon_value": lead,
                "id": 100 + lead,
            }

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                lead = params.get("horizon_value")
                if lead in (1, 2, 3):
                    return _make_mock_response([forecast_for_lead(lead)])
                return _make_mock_response([])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "month", "19999", pd.DataFrame({"code": ["19999"], "station_labels": ["x"]})
        )

        # month_1 stays on the dedicated "forecasts_all" key.
        assert not data["forecasts_all"].empty
        assert (data["forecasts_all"]["horizon_value"] == 1).all()

        by_mode = data["long_forecasts_by_month_mode"]
        assert set(by_mode) == {"month_2", "month_3"}
        assert not by_mode["month_2"].empty
        assert (by_mode["month_2"]["horizon_value"] == 2).all()
        assert not by_mode["month_3"].empty
        assert (by_mode["month_3"]["horizon_value"] == 3).all()

    def test_by_mode_dict_absent_when_flag_off(self, monkeypatch, _long_term_resolver_env):
        """Flag-OFF golden: the new by-mode dict key must not appear at all
        (dict shape stays byte-identical to pre-M1 behavior)."""
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "month_1,month_2,month_3"
        )

        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "month", "99001", pd.DataFrame({"code": ["99001"], "station_labels": ["x"]})
        )

        assert "long_forecasts_by_month_mode" not in data


# ── _get_data_monthly: M1 P3 review fixes — resolver robustness ───────────


class TestGetDataMonthlyLeadAwareResolverRobustness:
    """Regression tests for the P3 review findings.

    A dashboard READ must never crash just because a deployment's monthly
    config carries only ``operational_month_lead_time`` (the taj shape) and
    no ``operational_issue_day``. The monthly display path needs only the
    lead, so it must resolve via ``operational_lead_for_mode`` (lead-only)
    rather than ``operational_schedule_for_mode`` (which additionally
    requires ``operational_issue_day`` + the config file to exist).
    """

    def _patch_processing(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.processing.add_labels_to_hydrograph",
            lambda df, stations: df,
        )
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def _write_lead_only_config(self, config_dir, mode, lead):
        """Config with ONLY operational_month_lead_time (no issue_day)."""
        (config_dir / f"{mode}.json").write_text(
            json.dumps({"operational_month_lead_time": lead})
        )

    def _write_full_config(self, config_dir, mode, lead, issue_day=25):
        (config_dir / f"{mode}.json").write_text(
            json.dumps({"operational_month_lead_time": lead, "operational_issue_day": issue_day})
        )

    def _all_19999(self):
        return pd.DataFrame({"code": ["19999"], "station_labels": ["x"]})

    # ── #1: month_1 config missing operational_issue_day must not crash ──
    def test_month1_lead_only_config_does_not_crash_flag_on(
        self, monkeypatch, _long_term_resolver_env
    ):
        """#1: primary monthly tile must resolve the lead from a lead-only
        config (no operational_issue_day) instead of raising."""
        config_dir = _long_term_resolver_env
        self._write_lead_only_config(config_dir, "month_1", lead=1)
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 1}

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                if params.get("horizon_value") == 1:
                    return _make_mock_response([forecast])
                return _make_mock_response([])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_19999())

        fa = data["forecasts_all"]
        assert not fa.empty, "lead-only month_1 config must not hide the primary forecast"
        assert (fa["horizon_value"] == 1).all()

    # ── #2: month_0 config missing operational_issue_day must not crash ──
    def test_month0_lead_only_config_does_not_crash_flag_on(
        self, monkeypatch, _long_term_resolver_env
    ):
        """#2: month_0 block must resolve its lead from a lead-only config;
        month_1 still loads and long_forecasts_m0 degrades gracefully."""
        config_dir = _long_term_resolver_env
        self._write_full_config(config_dir, "month_1", lead=1)
        self._write_lead_only_config(config_dir, "month_0", lead=0)
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_0,month_1")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        m1_forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 1, "id": 71}
        m0_forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 0, "id": 70}

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                if params.get("horizon_value") == 1:
                    return _make_mock_response([m1_forecast])
                if params.get("horizon_value") == 0:
                    return _make_mock_response([m0_forecast])
                return _make_mock_response([])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_19999())

        assert not data["forecasts_all"].empty
        assert (data["forecasts_all"]["horizon_value"] == 1).all()
        m0 = data["long_forecasts_m0"]
        assert not m0.empty, "month_0 lead-only config must resolve to lead 0"
        assert (m0["horizon_value"] == 0).all()

    # ── #3: flag-ON always keys merge on horizon_value (month_1 absent) ──
    def test_merge_keys_include_horizon_value_even_without_month1(
        self, monkeypatch, _long_term_resolver_env
    ):
        """#3: with month_1 absent from supported_modes, the primary merge
        must still key on horizon_value so a lead-1 forecast does not fan
        out against (or inherit the skill of) other leads' stats rows."""
        config_dir = _long_term_resolver_env
        self._write_full_config(config_dir, "month_2", lead=2)
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_2")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 1}

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                if params.get("horizon_value") == 1:
                    return _make_mock_response([forecast])
                return _make_mock_response([])
            if "/skill-metric/" in url:
                # Two stats rows for the same period/model, different leads —
                # NEITHER is lead 1.
                return _make_mock_response(
                    [
                        _skill_metric_record_with_lead(4, "GBT", 2, delta=2.0),
                        _skill_metric_record_with_lead(4, "GBT", 3, delta=3.0),
                    ]
                )
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_19999())

        fa = data["forecasts_all"]
        assert len(fa) == 1, (
            "lead-1 forecast fanned out against non-matching-lead stats rows: "
            f"{fa[['code', 'model_short', 'horizon_value']].to_dict('records')}"
        )
        assert pd.isna(fa["delta"].iloc[0]), "no wrong-lead skill metric may attach"

    # ── #4: supported_modes tokens are stripped before membership check ──
    def test_supported_modes_with_spaces_recognizes_month1(
        self, monkeypatch, _long_term_resolver_env
    ):
        """#4: 'quarter, month_1' (space before month_1) must recognize
        month_1 after strip and resolve its (taj-style lead-0) lead."""
        config_dir = _long_term_resolver_env
        self._write_lead_only_config(config_dir, "month_1", lead=0)
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "quarter, month_1"
        )
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        taj_forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 0}

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                if params.get("horizon_value") == 0:
                    return _make_mock_response([taj_forecast])
                return _make_mock_response([])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_19999())

        fa = data["forecasts_all"]
        assert not fa.empty, "spaced 'month_1' token must be recognized after strip"
        assert (fa["horizon_value"] == 0).all()

    # ── #5: by-mode loop skips (not crashes) a mode with an absent file ──
    def test_by_mode_missing_config_file_is_skipped_not_crash(
        self, monkeypatch, _long_term_resolver_env
    ):
        """#5: a supported month_N whose config FILE is absent must be
        skipped with a warning, not raise FileNotFoundError."""
        config_dir = _long_term_resolver_env
        self._write_full_config(config_dir, "month_1", lead=1)
        # month_2 intentionally has NO config file written.
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1,month_2")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        m1_forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 1}

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                if params.get("horizon_value") == 1:
                    return _make_mock_response([m1_forecast])
                return _make_mock_response([])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_19999())

        assert not data["forecasts_all"].empty
        by_mode = data["long_forecasts_by_month_mode"]
        assert "month_2" not in by_mode, "mode with missing config file must be skipped"

    # ── flag-OFF golden: lead-only config path is untouched by the fix ──
    def test_flag_off_lead_only_config_unchanged(
        self, monkeypatch, _long_term_resolver_env
    ):
        """Flag-OFF golden: the legacy path never reads month_1's lead from
        config (hardcodes horizon_value=1), filters stats to lead 1, keeps
        no horizon_value merge key, and emits no by-mode dict."""
        config_dir = _long_term_resolver_env
        self._write_lead_only_config(config_dir, "month_1", lead=0)
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1")
        # SAPPHIRE_SKILL_LEAD_AWARE intentionally unset (default OFF).

        seen_leads = []
        forecast = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 1}

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                seen_leads.append(params.get("horizon_value"))
                if params.get("horizon_value") == 1:
                    return _make_mock_response([forecast])
                return _make_mock_response([])
            if "/skill-metric/" in url:
                return _make_mock_response(
                    [
                        _skill_metric_record_with_lead(4, "GBT", 1, delta=1.0),
                        _skill_metric_record_with_lead(4, "GBT", 2, delta=2.0),
                    ]
                )
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data("month", "19999", self._all_19999())

        # Legacy path hardcodes horizon_value=1 regardless of config lead=0.
        assert 1 in seen_leads
        fa = data["forecasts_all"]
        assert not fa.empty
        # Stats filtered to lead 1 → lead-1 delta attaches, single row.
        assert len(fa) == 1
        assert fa["delta"].iloc[0] == 1.0
        # No by-mode dict under flag OFF.
        assert "long_forecasts_by_month_mode" not in data


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
    @pytest.mark.parametrize(("lead", "expected_name"), [(0, "tajik"), (1, "kyrgyz")])
    def test_default_horizon_value_comes_from_deployment_config(
        self, lead, expected_name, monkeypatch, tmp_path
    ):
        config_dir = tmp_path / expected_name
        config_dir.mkdir()
        (config_dir / "quarter.json").write_text(json.dumps({"operational_month_lead_time": lead}))
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_configuration",
            expected_name,
        )
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes",
            "quarter",
        )
        seen_params = []

        def mock_get(url, **kwargs):
            seen_params.append(kwargs["params"])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        db.get_long_forecasts_quarter(station="99001")

        assert seen_params[0]["horizon_value"] == lead

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
        # C3: an empty pd.DataFrame(columns=[...]) defaults every column to
        # object dtype — `quarter_issue_date` must still be datetime64 on
        # this early-return path, matching the non-empty path's dtype.
        assert pd.api.types.is_datetime64_any_dtype(result["quarter_issue_date"])


class TestGetLongForecastsQuarterLeadAware:
    """M1 P3: under the flag, quarter forecast dedup keys on lead too so
    distinct-lead rows for the same code/model are not silently collapsed."""

    def test_horizon_value_preserved_when_flag_on(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        def mock_get(url, **kwargs):
            return _make_mock_response([_QUARTER_FORECAST_RECORD])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="99001")

        assert "horizon_value" in result.columns
        assert result["horizon_value"].iloc[0] == 1

    def test_distinct_leads_not_collapsed_when_flag_on(self, monkeypatch):
        """Two rows, same code/model, different lead — both must survive."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        lead0 = {**_QUARTER_FORECAST_RECORD, "id": 5, "horizon_value": 0, "date": "2026-03-01"}
        lead1 = {**_QUARTER_FORECAST_RECORD, "id": 6, "horizon_value": 1, "date": "2026-03-02"}

        def mock_get(url, **kwargs):
            return _make_mock_response([lead0, lead1])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="99001")

        assert len(result) == 2, (
            f"Expected both leads to survive, got {len(result)}: "
            f"{result[['code', 'model_short', 'horizon_value']].to_dict('records')}"
        )
        assert set(result["horizon_value"]) == {0, 1}

    def test_flag_off_still_collapses_to_single_latest_row(self, monkeypatch):
        """Flag-OFF golden: distinct leads still collapse to the latest-dated row."""
        lead0 = {**_QUARTER_FORECAST_RECORD, "id": 5, "horizon_value": 0, "date": "2026-03-01"}
        lead1 = {**_QUARTER_FORECAST_RECORD, "id": 6, "horizon_value": 1, "date": "2026-03-02"}

        def mock_get(url, **kwargs):
            return _make_mock_response([lead0, lead1])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="99001")

        assert len(result) == 1
        assert "horizon_value" not in result.columns
        assert str(result["date"].iloc[0].date()) == "2026-03-02"


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

    def test_season_in_year_comes_from_api_horizon_value(self, monkeypatch):
        def mock_get(url, **kwargs):
            assert kwargs["params"]["horizon_value"] == 0
            return _make_mock_response([{**_SEASON_FORECAST_RECORD_19999, "horizon_value": 0}])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_season(station="19999", horizon_value=0)

        assert "season_in_year" in result.columns
        assert result["season_in_year"].iloc[0] == 0

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

        for key in (
            "hydrograph_day_all",
            "hydrograph_pentad_all",
            "rain",
            "temp",
            "snow_data",
            "ml_forecast",
            "linreg_predictor",
            "forecasts_all",
            "forecast_stats",
        ):
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


class TestGetDataQuarterLeadAware:
    """M1 P3: quarter stats merges were period-only; under the flag they
    must key on lead too (hv0/hv1 coexisting for the same target period)."""

    def _patch_processing(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.processing.add_labels_to_hydrograph",
            lambda df, stations: df,
        )
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

    def test_stats_merge_keyed_by_lead_not_just_period(self, monkeypatch):
        """ONE lead-0 forecast row for LR_Base, but the API's skill-metric
        table holds BOTH a lead-0 and a lead-1 stats row for LR_Base/same
        target quarter (e.g. hindcast + operational coexist). Merging on
        period+model alone would fan the single forecast row out into 2
        duplicate rows (one per matching stats row) and could attach the
        WRONG lead's skill metrics. Keying on lead too must pick exactly
        the matching (hv0) row."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        forecast_lead0 = {
            **_QUARTER_FORECAST_RECORD_19999,
            "id": 50,
            "model_type": "LR_Base",
            "horizon_value": 0,
        }
        skill_lead0 = {
            **_skill_metric_record_19999("quarter", 2, "LR_Base", 5.0),
            "horizon_value": 0,
        }
        skill_lead1 = {
            **_skill_metric_record_19999("quarter", 2, "LR_Base", 9.0),
            "horizon_value": 1,
        }

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([forecast_lead0])
            if "/skill-metric/" in url:
                return _make_mock_response([skill_lead0, skill_lead1])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "quarter", "19999", pd.DataFrame({"code": ["19999"], "station_labels": ["x"]})
        )

        fa = data["forecasts_all"]
        assert len(fa) == 1, (
            f"Expected exactly 1 row (no Cartesian duplication across leads), "
            f"got {len(fa)}: {fa[['model_short', 'horizon_value', 'delta']].to_dict('records')}"
        )
        assert fa["horizon_value"].iloc[0] == 0
        assert fa["delta"].iloc[0] == 5.0  # the hv0 stats row, not hv1's 9.0

    def test_flag_off_golden_shows_the_pre_m1_cartesian_merge_baseline(self, monkeypatch):
        """Flag-OFF golden: documents that WITHOUT the flag, the same setup
        as above still fans out into duplicate rows (pre-existing baseline
        behavior — kept byte-identical, not fixed, when the flag is off).
        """
        forecast_lead0 = {
            **_QUARTER_FORECAST_RECORD_19999,
            "id": 50,
            "model_type": "LR_Base",
            "horizon_value": 0,
        }
        skill_lead0 = {
            **_skill_metric_record_19999("quarter", 2, "LR_Base", 5.0),
            "horizon_value": 0,
        }
        skill_lead1 = {
            **_skill_metric_record_19999("quarter", 2, "LR_Base", 9.0),
            "horizon_value": 1,
        }

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([forecast_lead0])
            if "/skill-metric/" in url:
                return _make_mock_response([skill_lead0, skill_lead1])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "quarter", "19999", pd.DataFrame({"code": ["19999"], "station_labels": ["x"]})
        )

        fa = data["forecasts_all"]
        # Pre-M1 baseline: the single lead-0 forecast fans out into 2 rows
        # (one per matching stats row) because horizon_value is not part of
        # the merge key — a duplication that the flag fixes (see the
        # companion flag-ON test above).
        assert len(fa) == 2
        assert set(fa["delta"]) == {5.0, 9.0}


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

        for key in (
            "hydrograph_day_all",
            "hydrograph_pentad_all",
            "rain",
            "temp",
            "snow_data",
            "ml_forecast",
            "linreg_predictor",
            "forecasts_all",
            "forecast_stats",
        ):
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

    def test_all_four_season_leads_retain_their_own_skill(self, monkeypatch):
        """Forecast rows for Jan/Feb/Mar/Apr issues keep per-lead skill metrics."""
        leads = [3, 2, 1, 0]
        forecasts = pd.DataFrame(
            [
                {
                    "code": "19999",
                    "date": pd.Timestamp(f"2026-0{4 - lead}-22"),
                    "Date": pd.Timestamp(f"2026-0{4 - lead}-22"),
                    "year": 2026,
                    "model_short": "LR_Base",
                    "model_long": "Linear regression base",
                    "forecasted_discharge": 300.0 + lead,
                    "flag": 0,
                    "Q5": 270.0,
                    "Q25": 280.0,
                    "Q75": 320.0,
                    "Q95": 330.0,
                    "E[Q]": 300.0,
                    "valid_from": pd.Timestamp("2026-04-01"),
                    "month_in_year": 4,
                    "season_in_year": lead,
                }
                for lead in leads
            ]
        )
        skills = pd.DataFrame(
            [
                {
                    "code": "19999",
                    "season_in_year": lead,
                    "model_short": "LR_Base",
                    "model_long": "Linear regression base",
                    "delta": float(lead) + 0.25,
                    "sdivsigma": float(lead) + 0.5,
                    "mae": float(lead) + 0.75,
                    "accuracy": 90.0 + lead,
                }
                for lead in leads
            ]
        )

        monkeypatch.setattr("src.db.get_long_forecasts_season", lambda station: forecasts)
        monkeypatch.setattr("src.db.get_forecast_stats", lambda horizon, station: skills)
        monkeypatch.setattr("src.db.get_hydrograph_day_all", lambda station: pd.DataFrame())
        monkeypatch.setattr("src.db.get_rain", lambda station: pd.DataFrame())
        monkeypatch.setattr("src.db.get_temp", lambda station: pd.DataFrame())
        monkeypatch.setattr("src.db.get_snow_data", lambda *args, **kwargs: {})
        self._patch_processing(monkeypatch)

        data = db.get_data(
            "season",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        by_lead = data["forecasts_all"].set_index("season_in_year")
        assert sorted(by_lead.index.tolist()) == [0, 1, 2, 3]
        for lead in leads:
            row = by_lead.loc[lead]
            assert row["delta"] == float(lead) + 0.25
            assert row["sdivsigma"] == float(lead) + 0.5
            assert row["mae"] == float(lead) + 0.75
            assert row["accuracy"] == 90.0 + lead

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

    def test_season_summary_table_without_skill_metrics_does_not_crash(self, monkeypatch):
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


# ── Tombstone suppression in get_forecast_stats / get_forecast_stats_all ─────


def _tombstone_skill_record(horizon, horizon_in_year, model_type, **overrides):
    """Build an API skill-metric record that looks like a tombstone (n_pairs=0)."""
    base = _skill_metric_record_19999(horizon, horizon_in_year, model_type, delta=0.0)
    # Overwrite metric fields to NULL/zero to mimic a real tombstone
    base.update(
        {
            "n_pairs": 0,
            "nse": None,
            "mae": None,
            "accuracy": None,
            "sdivsigma": None,
            "delta": None,
            "crps": None,
            "pbias": None,
            "kgelf": None,
            "nse_log": None,
        }
    )
    base.update(overrides)
    return base


class TestGetForecastStatsTombstoneSuppression:
    """Tombstone rows (n_pairs == 0) must not appear in get_forecast_stats output."""

    def test_tombstone_excluded_from_get_forecast_stats(self, monkeypatch):
        """A tombstone row returned by the API is dropped before dedup."""
        records = [
            _skill_metric_record_19999("month", 4, "GBT", 1.0),
            _tombstone_skill_record("month", 4, "LR_Base"),
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "19999")

        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        # Tombstone LR_Base must not be present
        assert "LR_Base" not in result["model_short"].values

    def test_all_tombstones_returns_empty_frame_with_columns(self, monkeypatch):
        """When the API returns only tombstone rows, the result is empty."""
        records = [
            _tombstone_skill_record("month", 4, "GBT"),
            _tombstone_skill_record("month", 4, "LR_Base"),
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "19999")

        assert result.empty
        assert "month_in_year" in result.columns
        assert "model_short" in result.columns

    def test_tombstone_excluded_from_get_forecast_stats_all(self, monkeypatch):
        """get_forecast_stats_all also drops tombstone rows."""
        records = [
            _skill_metric_record_19999("quarter", 2, "LR_Base", 1.0),
            _tombstone_skill_record("quarter", 2, "LR_SM"),
        ]

        def mock_get(url, **kwargs):
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats_all("quarter")

        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "LR_Base"
        assert "LR_SM" not in result["model_short"].values

    def test_tombstone_is_not_the_selected_skill_row_in_month_merge(self, monkeypatch):
        """A tombstone for LR_Base must not land in the month forecasts_all merge."""
        monthly_forecast = {
            **_LONG_FORECAST_RECORD,
            "id": 50,
            "code": "19999",
            "model_type": "GBT",
            "model_type_description": "Gradient Boosted Trees",
            "q": 130.0,
        }
        # API returns one real skill row for GBT and one tombstone for LR_Base
        skills = [
            _skill_metric_record_19999("month", 4, "GBT", 2.0),
            _tombstone_skill_record("month", 4, "LR_Base"),
        ]

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([monthly_forecast])
            if "/skill-metric/" in url:
                return _make_mock_response(skills)
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        monkeypatch.setattr("src.db.processing.add_labels_to_hydrograph", lambda df, s: df)
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

        data = db.get_data(
            "month",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        fs = data["forecast_stats"]
        # The tombstone must not appear in forecast_stats
        assert "LR_Base" not in fs["model_short"].values
        assert "GBT" in fs["model_short"].values

    def test_get_forecast_stats_all_horizon_value_dedup_preserves_distinct_leads(self, monkeypatch):
        """get_forecast_stats_all: two leads for the same (code, month, model) are
        both retained — the dedup key now includes horizon_value.
        """
        records = [
            {**_skill_metric_record_with_lead(3, "GBT", 1, delta=1.0), "code": "19999"},
            {**_skill_metric_record_with_lead(3, "GBT", 2, delta=1.5), "code": "19999"},
        ]

        def mock_get(url, **kwargs):
            limit = kwargs["params"]["limit"]
            assert limit == 1000
            return _make_mock_response(records)

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats_all("month")

        assert len(result) == 2, (
            f"Expected 2 rows (one per lead), got {len(result)}: "
            f"{result[['month_in_year', 'model_short', 'horizon_value']].to_dict('records') if not result.empty else '(empty)'}"
        )
        assert set(result["horizon_value"].unique()) == {1, 2}

    # ── M1 P3: full-lead-key tombstone filtering ───────────────────────────
    #
    # A tombstone must be excluded by its OWN (code, period, horizon_value,
    # model_short) key — never by (code, period, model_short) alone, else a
    # tombstoned lead-2 row could mask (or be masked by) a live lead-1 row
    # for the same period/model and silently "look fine".

    def test_tombstoned_lead_excluded_live_lead_for_same_period_survives(self, monkeypatch):
        """Same period/code/model, two leads: lead-1 live, lead-2 tombstoned.
        Only the live lead-1 row must survive — the tombstone must not mask
        it, and must not itself masquerade as data."""
        live_lead1 = _skill_metric_record_with_lead(4, "GBT", 1, delta=1.0)
        tombstoned_lead2 = _tombstone_skill_record("month", 4, "GBT", horizon_value=2)

        def mock_get(url, **kwargs):
            return _make_mock_response([live_lead1, tombstoned_lead2])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "19999")

        assert len(result) == 1, (
            f"Expected only the live lead-1 row to survive, got: "
            f"{result[['model_short', 'horizon_value', 'n_pairs']].to_dict('records') if not result.empty else '(empty)'}"
        )
        assert result["horizon_value"].iloc[0] == 1
        assert result["delta"].iloc[0] == 1.0

    def test_later_dated_tombstone_does_not_mask_earlier_live_row_of_different_lead(
        self, monkeypatch
    ):
        """Regression: a LATER-dated tombstone for lead-2 must not win a
        keep='last' dedup against an EARLIER live row for lead-1 sharing the
        same period/model — proving tombstone exclusion is keyed on the
        FULL (code, period, horizon_value, model_short) tuple, not merely
        (code, period, model_short)."""
        live_lead1 = {
            **_skill_metric_record_with_lead(4, "GBT", 1, delta=1.0),
            "date": "2026-03-01",
        }
        tombstoned_lead2_later = _tombstone_skill_record(
            "month", 4, "GBT", horizon_value=2, date="2026-03-20"
        )

        def mock_get(url, **kwargs):
            return _make_mock_response([live_lead1, tombstoned_lead2_later])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_forecast_stats("month", "19999")

        assert len(result) == 1
        assert result["horizon_value"].iloc[0] == 1
        assert result["delta"].iloc[0] == 1.0

    def test_tombstoned_lead_in_month_by_mode_merge_has_no_leaked_skill_metrics(
        self, monkeypatch, _long_term_resolver_env
    ):
        """Integration: a tombstoned month_2 skill row must not attach to
        month_2's forecast via a stray cross-lead merge, and must not mask
        month_1's live skill metrics in the primary tile."""
        config_dir = _long_term_resolver_env
        (config_dir / "month_1.json").write_text(
            json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
        )
        (config_dir / "month_2.json").write_text(
            json.dumps({"operational_month_lead_time": 2, "operational_issue_day": 25})
        )
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1,month_2")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        forecast_lead1 = {**_LONG_FORECAST_RECORD, "code": "19999", "horizon_value": 1}
        forecast_lead2 = {
            **_LONG_FORECAST_RECORD,
            "id": 2,
            "code": "19999",
            "horizon_value": 2,
        }
        live_lead1 = _skill_metric_record_with_lead(4, "GBT", 1, delta=1.0)
        tombstoned_lead2 = _tombstone_skill_record("month", 4, "GBT", horizon_value=2)

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url:
                lead = params.get("horizon_value")
                if lead == 1:
                    return _make_mock_response([forecast_lead1])
                if lead == 2:
                    return _make_mock_response([forecast_lead2])
                return _make_mock_response([])
            if "/skill-metric/" in url:
                return _make_mock_response([live_lead1, tombstoned_lead2])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        monkeypatch.setattr("src.db.processing.add_labels_to_hydrograph", lambda df, s: df)
        monkeypatch.setattr(
            "src.db.processing.internationalize_forecast_model_names",
            lambda fn, df, **kw: df,
        )

        data = db.get_data(
            "month",
            "19999",
            pd.DataFrame({"code": ["19999"], "station_labels": ["Test River B"]}),
        )

        fa = data["forecasts_all"]  # month_1, primary tile
        assert not fa.empty
        assert fa["delta"].iloc[0] == 1.0

        month2 = data["long_forecasts_by_month_mode"]["month_2"]
        assert not month2.empty  # the month_2 forecast itself is still visible
        assert pd.isna(month2["delta"].iloc[0])  # but no skill metrics — tombstoned


# ── FD-005: _read_data_paginated / fixed-limit truncation fix ─────────────


# The real postprocessing API's `/forecast/` endpoint defaults `limit` to
# 100 when the query omits it (sapphire/services/postprocessing/app/main.py
# `read_forecast`). The fake below must honour that same contract — a fake
# that instead returns everything when `limit` is missing is more generous
# than the real API and hides any caller that forgot to paginate.
_API_DEFAULT_LIMIT = 100


def _skip_limit_fake(full_df):
    """A fake `_read_data` that serves `full_df` page by page via the
    `skip`/`limit` params, mirroring the real API's pagination contract
    (including `_read_data`'s own "date" → datetime conversion, so callers
    that rely on it — e.g. get_ml_forecast — see realistic dtypes)."""

    def fake(service_type, data_type, params=None):
        params = params or {}
        # .get with defaults (rather than indexing) so this fake also works
        # against a non-paginated caller that passes only "limit" (no
        # "skip") — that path then serves just the first page, which is
        # exactly the truncation this fix targets. A missing "limit" must
        # fall back to the real API's default (100), not to "everything" —
        # otherwise an unpaginated caller would silently see the full
        # result set here while the real API would truncate it.
        skip = params.get("skip", 0)
        limit = params.get("limit", min(_API_DEFAULT_LIMIT, len(full_df)))
        page = full_df.iloc[skip : skip + limit].reset_index(drop=True).copy()
        if "date" in page.columns:
            page["date"] = pd.to_datetime(page["date"])
        return page

    return fake


class TestReadDataPaginated:
    """Unit tests for the `_read_data_paginated` helper in isolation."""

    def test_returns_complete_result_larger_than_page_size(self, monkeypatch):
        """FD-005 regression: a result set spanning multiple pages must be
        returned in full, not truncated to the first page."""
        full_df = pd.DataFrame({"id": range(7)})
        monkeypatch.setattr(db, "_read_data", _skip_limit_fake(full_df))

        result = db._read_data_paginated("preprocessing", "runoff", {}, page_size=3)

        assert list(result["id"]) == list(range(7))

    def test_stops_on_short_final_page_no_infinite_loop_no_dropped_rows(self, monkeypatch):
        """7 rows / page_size 3 → pages of 3, 3, 1. The short (1-row) final
        page must stop the loop without an extra request."""
        full_df = pd.DataFrame({"id": range(7)})
        seen_skips = []

        def fake(service_type, data_type, params=None):
            seen_skips.append(params["skip"])
            return _skip_limit_fake(full_df)(service_type, data_type, params)

        monkeypatch.setattr(db, "_read_data", fake)

        result = db._read_data_paginated("preprocessing", "runoff", {}, page_size=3)

        assert len(result) == 7
        assert seen_skips == [0, 3, 6]

    def test_stops_on_exact_page_multiple_via_empty_next_page(self, monkeypatch):
        """6 rows / page_size 3 → two full pages, then a 3rd request that
        comes back empty. Must stop there (no infinite loop) and keep all
        6 rows (no dropped rows)."""
        full_df = pd.DataFrame({"id": range(6)})
        seen_skips = []

        def fake(service_type, data_type, params=None):
            seen_skips.append(params["skip"])
            return _skip_limit_fake(full_df)(service_type, data_type, params)

        monkeypatch.setattr(db, "_read_data", fake)

        result = db._read_data_paginated("preprocessing", "runoff", {}, page_size=3)

        assert len(result) == 6
        assert seen_skips == [0, 3, 6]

    def test_empty_first_page_returns_empty_dataframe_without_error(self, monkeypatch):
        monkeypatch.setattr(db, "_read_data", lambda *a, **k: pd.DataFrame())

        result = db._read_data_paginated("preprocessing", "runoff", {}, page_size=3)

        assert result.empty

    def test_helper_skip_and_limit_win_over_caller_supplied_values(self, monkeypatch):
        """The helper builds each page's params as
        `{**base_params, "skip": skip, "limit": page_size}` — its own
        computed `skip`/`limit` must win over any caller-supplied keys of
        the same name, not be overridden by them. Pins current (correct)
        behavior; no production change accompanies this test."""
        full_df = pd.DataFrame({"id": range(7)})
        monkeypatch.setattr(db, "_read_data", _skip_limit_fake(full_df))

        result = db._read_data_paginated(
            "preprocessing", "runoff", {"skip": 99, "limit": 2}, page_size=3
        )

        assert list(result["id"]) == list(range(7))

    def test_default_page_size_is_10000(self, monkeypatch):
        """Latency follow-up to FD-005: the default page size must be large
        enough that a typical station result set fits in one or a handful
        of requests, not the ~10 requests a page_size of 1000 caused. This
        pins the default the helper requests when the caller does not pass
        `page_size`."""
        full_df = pd.DataFrame({"id": range(5)})
        seen_limits = []

        def fake(service_type, data_type, params=None):
            seen_limits.append(params["limit"])
            return _skip_limit_fake(full_df)(service_type, data_type, params)

        monkeypatch.setattr(db, "_read_data", fake)

        result = db._read_data_paginated("preprocessing", "runoff", {})

        assert seen_limits == [10000]
        assert len(result) == 5

    def test_progress_guard_stops_on_unresponsive_skip(self, monkeypatch):
        """A server that ignores `skip` and always returns a full page must
        not make the loop run unboundedly. The guard must terminate the
        loop and log a WARNING, rather than de-duplicating (which would
        mask the server defect) or looping forever.

        `db.logger` is stubbed directly (rather than via `caplog`) because
        it is configured with `propagate = False`, which `caplog`'s
        root-logger handler never sees.
        """
        call_count = 0
        warn_calls = []

        def fake(service_type, data_type, params=None):
            nonlocal call_count
            call_count += 1
            # Ignores params["skip"] entirely: always the same full page.
            return pd.DataFrame({"id": [1, 2]})

        def fake_warning(msg, *args, **kwargs):
            warn_calls.append(msg % args if args else msg)

        monkeypatch.setattr(db, "_read_data", fake)
        monkeypatch.setattr(db.logger, "warning", fake_warning)

        result = db._read_data_paginated("preprocessing", "runoff", {}, page_size=2)

        # Must have stopped well short of an unbounded loop.
        assert 0 < call_count <= 2000
        assert not result.empty
        assert warn_calls
        assert any("skip" in w.lower() for w in warn_calls)


def _ml_forecast_rows(n, forecast_date, target_date="2026-03-25", model_type="LR"):
    """Build `n` rows sharing `forecast_date`, but with distinct `target`
    dates (offset by day index from `target_date`) so each row has its own
    identity. Without this, a `.head(1)` truncation applied after
    `get_ml_forecast`'s latest-date filter would still leave one row whose
    `forecast_date` matches the max — passing a count/date-max-only
    assertion trivially even though rows were silently dropped."""
    base = pd.Timestamp(target_date)
    return [
        {
            "code": "19999",
            "date": forecast_date,  # renamed to forecast_date by get_ml_forecast
            "target": (base + pd.Timedelta(days=i)).strftime("%Y-%m-%d"),  # renamed to date
            "model_type": model_type,
            "model_type_description": f"{model_type} description",
            "q05": 1.0,
            "q25": 2.0,
            "q75": 3.0,
            "q95": 4.0,
            "forecasted_discharge": 5.0,
            "flag": 0,
            "composition": "",
        }
        for i in range(n)
    ]


def _read_data_paginated_default_page_size() -> int:
    """The *current* default `page_size` of `db._read_data_paginated`, read
    off the function's signature by parameter NAME rather than hardcoded as
    a literal 1000/10000, and rather than positionally (`__defaults__[-1]`
    is "the last positional default", not "page_size" — adding any later
    keyword parameter with its own default silently changes what that
    position holds).

    FD-005 test-rot history: this test's fixture size used to be hardcoded
    against the default in effect when the test was written (1000). When the
    default was later raised to 10000, a 1200-row fixture — sized to span
    two pages under the *old* default — fit in a single page under the new
    one, and the test kept passing even with the fix reverted. Deriving the
    size from the live default here means a future change to it cannot
    silently disarm this test a second time. Binding by name (via
    `inspect.signature`) means a signature change that removes `page_size`
    raises `KeyError` here — loud failure — instead of silently reading an
    unrelated parameter's default.
    """
    return inspect.signature(db._read_data_paginated).parameters["page_size"].default


class TestGetMlForecastPagination:
    """FD-005: get_ml_forecast must not silently drop the true latest
    forecast_date when the station's row count exceeds one API page.

    Parameterized over an explicit small `page_size` (fast, deterministic,
    and immune to the real default ever changing) and the real, current
    default (introspected — see `_read_data_paginated_default_page_size` —
    never hardcoded). Either way, the fixture is one full page of stale rows
    plus a second page holding the true latest `forecast_date`; the test
    must fail if `get_ml_forecast` stops paginating and instead resolves
    `forecast_date.max()` from a single, truncated page.
    """

    @pytest.mark.parametrize(
        "page_size_override",
        [3, None],
        ids=["explicit-small-page-size", "real-default-page-size"],
    )
    def test_forecast_date_max_reflects_true_latest_not_truncated_slice(
        self, monkeypatch, page_size_override
    ):
        page_size = page_size_override or _read_data_paginated_default_page_size()
        # One full page of stale rows, plus a second page holding the true
        # latest forecast_date — the exact shape of the original FD-005 bug.
        stale_rows = _ml_forecast_rows(page_size, "2025-12-01")
        latest_rows = _ml_forecast_rows(3, "2026-03-20", target_date="2026-03-20")
        full_df = pd.DataFrame(stale_rows + latest_rows)

        monkeypatch.setattr(db, "_read_data", _skip_limit_fake(full_df))
        if page_size_override is not None:
            # Force `_read_data_paginated`'s own default down to the small
            # override for the duration of this test, without touching
            # get_ml_forecast (which never passes page_size explicitly and
            # so always uses whatever this default is). This exercises the
            # real pagination loop at a small, fast page size instead of
            # requiring a fixture larger than the real (10000) default.
            monkeypatch.setattr(
                db._read_data_paginated, "__defaults__", (None, page_size_override)
            )

        result = db.get_ml_forecast("day", "19999")

        assert not result.empty
        assert result["forecast_date"].max() == pd.Timestamp("2026-03-20")
        # Every surviving row must belong to the true latest forecast_date —
        # none of the stale, truncated-page rows should leak through.
        assert (result["forecast_date"] == pd.Timestamp("2026-03-20")).all()
        # FD-005: assert the *complete* set of latest-date rows survives —
        # count AND identity — not merely that some row with the right
        # forecast_date is present. The three latest rows carry distinct
        # target dates (see `_ml_forecast_rows`); a `.head(1)` truncation
        # applied after the latest-date filter would still satisfy both
        # assertions above trivially (one row, whose forecast_date is the
        # max) while silently dropping two of the three rows.
        expected_target_dates = {f"2026-03-{20 + i}" for i in range(3)}
        assert set(result["date"]) == expected_target_dates
        assert len(result) == len(expected_target_dates)


class TestFetchersUsePagination:
    """FD-005: the five sibling fetchers (get_forecasts_all,
    get_forecast_stats, get_long_forecasts, get_long_forecasts_quarter,
    get_long_forecasts_season) must route through `_read_data_paginated`,
    not the fixed-limit `_read_data`, so none of them can silently truncate
    a result set larger than one page."""

    def _fake_paginated(self, calls):
        def fake(service_type, data_type, params=None, page_size=1000):
            calls.append(data_type)
            return pd.DataFrame()

        return fake

    def test_get_forecasts_all_uses_paginated_reads(self, monkeypatch):
        calls = []
        monkeypatch.setattr(db, "_read_data_paginated", self._fake_paginated(calls))

        db.get_forecasts_all("pentad", "19999")

        assert "forecast" in calls
        assert "lr-forecast" in calls

    def test_get_forecasts_all_preserves_all_ml_rows_across_multiple_pages(self, monkeypatch):
        """FD-005: `get_forecasts_all`'s ML result must retain every row of a
        multi-page result set. This exercises the real (unmocked)
        `_read_data_paginated` against a fake `_read_data`, with fixture
        rows carrying distinct `date` identities, and asserts the returned
        set of rows is complete — count AND identity — not merely
        non-empty. A `.head(N)` truncation applied anywhere to the ML frame
        (e.g. after the paginated read) would otherwise pass a weaker
        non-empty/max-date check while silently dropping rows.
        """
        page_size = _read_data_paginated_default_page_size()
        n = page_size + 5  # forces a second page
        dates = pd.date_range("2026-01-01", periods=n, freq="D")
        full_df = pd.DataFrame({"code": ["19999"] * n, "date": dates})

        def fake_read_data(service_type, data_type, params=None):
            if data_type != "forecast":
                return pd.DataFrame()
            params = params or {}
            skip = params.get("skip", 0)
            limit = params.get("limit", len(full_df))
            page = full_df.iloc[skip : skip + limit].reset_index(drop=True).copy()
            page["date"] = pd.to_datetime(page["date"])
            return page

        monkeypatch.setattr(db, "_read_data", fake_read_data)

        result = db.get_forecasts_all("pentad", "19999")

        assert len(result) == n
        assert set(result["date"]) == set(dates)

    def test_get_forecast_stats_uses_paginated_reads(self, monkeypatch):
        calls = []
        monkeypatch.setattr(db, "_read_data_paginated", self._fake_paginated(calls))

        db.get_forecast_stats("pentad", "19999")

        assert calls == ["skill-metric"]

    def test_get_long_forecasts_uses_paginated_reads(self, monkeypatch):
        calls = []
        monkeypatch.setattr(db, "_read_data_paginated", self._fake_paginated(calls))

        db.get_long_forecasts(station="19999")

        assert calls == ["long-forecast"]

    def test_get_long_forecasts_quarter_uses_paginated_reads(self, monkeypatch):
        calls = []
        monkeypatch.setattr(db, "_read_data_paginated", self._fake_paginated(calls))

        db.get_long_forecasts_quarter(station="19999")

        assert calls == ["long-forecast"]

    def test_get_long_forecasts_season_uses_paginated_reads(self, monkeypatch):
        calls = []
        monkeypatch.setattr(db, "_read_data_paginated", self._fake_paginated(calls))

        db.get_long_forecasts_season(station="19999")

        assert calls == ["long-forecast"]


# ── FD-029 P1: year-safe fetch, eligibility cutoff, native LR selection ────
#
# These tests need a FULL operational schedule (lead + issue day), unlike
# the module's autouse `_long_term_resolver_env` fixture (lead only, no
# `operational_issue_day`) — that fixture's shape is required so trunk's
# existing quarter tests keep exercising the degraded path (see item 3 of
# the plan). Tests below that need the real schedule write their own
# `quarter.json` into a fresh config dir via `_configure_quarter_schedule`.


def _configure_quarter_schedule(monkeypatch, tmp_path, lead, issue_day, dirname="quarter_schedule"):
    """Point the resolver at a quarter.json carrying BOTH schedule fields."""
    config_dir = tmp_path / dirname
    config_dir.mkdir()
    (config_dir / "quarter.json").write_text(json.dumps({
        "operational_month_lead_time": lead,
        "operational_issue_day": issue_day,
    }))
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", dirname)
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")


class TestGetLongForecastsQuarterFetchWindow:
    def test_fetch_window_year_safe_at_dec25(self, monkeypatch):
        """Problem 1: on 2026-12-25 the OLD window ({PREV_YEAR}-12-20 ..
        {CUR_YEAR}-12-31, frozen at import) never covers a row dated
        2027-01-01 (kghm Q1's flag-OFF derived/ensemble rows). The new
        window is resolved at CALL time from `today`."""
        seen_params = []

        def mock_get(url, **kwargs):
            seen_params.append(kwargs["params"])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        db.get_long_forecasts_quarter(station="19999", today=date(2026, 12, 25))

        params = seen_params[0]
        assert params["start_date"] <= "2026-12-25"
        assert params["end_date"] >= "2027-01-01"

    def test_lead1_previous_quarter_still_fetched_early_january(self, monkeypatch, tmp_path):
        """R2: the lower bound must cover the station's still-ELIGIBLE
        PREVIOUS calendar quarter too, not just the one containing
        `today`. kghm-shaped (lead 1): on 2027-01-02, with only a Q4 2026
        row dated 2026-09-25 in the backend, the fixed lower bound
        ({today.year-1}-12-01 = 2026-12-01) never covered it, so the card
        would silently disappear once Q1 2027 has no rows of its own yet."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        q4_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 170, "model_type": "GBT",
            "date": "2026-09-25", "valid_from": "2026-10-01", "valid_to": "2026-12-31",
        }

        def mock_get(url, **kwargs):
            # Simulates a real API that filters by the requested window,
            # unlike `_make_mock_response`'s callers elsewhere in this
            # file — the fetch window itself is what's under test here.
            params = kwargs.get("params", {})
            start, end = params.get("start_date"), params.get("end_date")
            row_date = q4_row["date"]
            if (start is not None and row_date < start) or (end is not None and row_date > end):
                return _make_mock_response([])
            return _make_mock_response([q4_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 2))

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 4

    def test_lead2_current_q1_still_fetched_in_january(self, monkeypatch, tmp_path):
        """R2: a lead>=2 config's CURRENT Q1 is issued in November, and
        stays eligible well into January — the lower bound must reach
        back far enough to still fetch it."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=2, issue_day=25)
        q1_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 171, "model_type": "GBT",
            "date": "2026-11-25", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            start, end = params.get("start_date"), params.get("end_date")
            row_date = q1_row["date"]
            if (start is not None and row_date < start) or (end is not None and row_date > end):
                return _make_mock_response([])
            return _make_mock_response([q1_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 5))

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 1

    def test_c1_missed_lt_run_does_not_narrow_below_the_spec_window(self, monkeypatch, tmp_path):
        """C1/W1 regression: a missed LT run for the previous calendar
        quarter must not make the card/bulletin go empty. A
        schedule-derived lower bound sized to reach back only (3 + lead)
        months (the shape before W1) is narrower than the spec's original
        window ({today.year-1}-12-01) for most of the year. kghm lead 1:
        the backend only has a Q2 2026 row (issued 2026-03-25); on
        2026-11-10 (Q4) a (3 + lead)-months-back bound alone would start
        at 2026-06-01, excluding it — the window must never be narrower
        than `min(spec bound, schedule-derived bound)`; W1's own
        schedule-derived bound reaches back (12 + lead) months, which
        alone already covers this case (see the W1 tests below for the
        lead>=4 / lead-0-in-January edges that (12 + lead) still misses
        without the `min` with the spec bound)."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        q2_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 172, "model_type": "GBT",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            start, end = params.get("start_date"), params.get("end_date")
            row_date = q2_row["date"]
            if (start is not None and row_date < start) or (end is not None and row_date > end):
                return _make_mock_response([])
            return _make_mock_response([q2_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 11, 10))

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 2

    def test_c2_degraded_window_uses_resolved_horizon_value_as_lead(self, monkeypatch, tmp_path):
        """C2: in degraded mode (a lead-only quarter.json, no
        operational_issue_day), the fetch window must use the RESOLVED
        horizon_value (here 4, from the same lead-only config) as the
        lead for sizing purposes, not a fixed guess. Early January, the
        previous quarter's row dated exactly 7 months (3 + lead) before
        the current quarter starts must still be returned."""
        config_dir = tmp_path / "lead_only"
        config_dir.mkdir()
        (config_dir / "quarter.json").write_text(
            json.dumps({"operational_month_lead_time": 4})
        )
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "lead_only")
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")

        # Q4 2026 row, dated 2026-06-01 -- exactly 7 months before Q1
        # 2027 (2027-01-01), i.e. 3 + lead(4) months back.
        q4_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 173, "model_type": "GBT",
            "date": "2026-06-01", "valid_from": "2026-10-01", "valid_to": "2026-12-31",
        }

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            start, end = params.get("start_date"), params.get("end_date")
            row_date = q4_row["date"]
            if (start is not None and row_date < start) or (end is not None and row_date > end):
                return _make_mock_response([])
            return _make_mock_response([q4_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 5))

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 4

    def test_w1_lead4_flag_off_row_dated_next_quarter_start_is_fetched(self, monkeypatch, tmp_path):
        """W1: for lead>=4, a flag-OFF row (dated at its own `valid_from`)
        of an eligible target quarter can be dated well into the NEXT
        quarter relative to `today` — the old fixed upper bound
        ({today.year+1}-03-31) missed it. kghm lead 4, issue day 25,
        today 2026-12-26: Q2 2027 (Apr-Jun) is already eligible (issue
        date 2026-12-25), and its flag-OFF GBT row is dated 2027-04-01
        (Q2's own valid_from) — one day past the old upper bound."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=4, issue_day=25)
        q2_2027_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 180, "model_type": "GBT",
            "date": "2027-04-01", "valid_from": "2027-04-01", "valid_to": "2027-06-30",
        }

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            start, end = params.get("start_date"), params.get("end_date")
            row_date = q2_2027_row["date"]
            if (start is not None and row_date < start) or (end is not None and row_date > end):
                return _make_mock_response([])
            return _make_mock_response([q2_2027_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 12, 26))

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 2
        assert result["year"].iloc[0] == 2027

    def test_w1_lead0_early_january_still_reaches_older_eligible_quarter(
        self, monkeypatch, tmp_path
    ):
        """W1: a lead-0 config's issue day (25) can still push a
        (3 + lead)-months-back schedule-derived lower bound past an
        eligible OLDER quarter in early January. tjhm-shaped lead 0,
        issue day 25, today 2027-01-05, Q4 2026 missing entirely: the
        native Q3 2026 row (issued 2026-07-25) must still be reachable."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=0, issue_day=25)
        q3_2026_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 181, "model_type": "GBT",
            "date": "2026-07-25", "valid_from": "2026-07-01", "valid_to": "2026-09-30",
        }

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            start, end = params.get("start_date"), params.get("end_date")
            row_date = q3_2026_row["date"]
            if (start is not None and row_date < start) or (end is not None and row_date > end):
                return _make_mock_response([])
            return _make_mock_response([q3_2026_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 5))

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 3
        assert bool(result["is_native"].iloc[0]) is True

    def test_w2_explicit_horizon_value_widens_window_beyond_schedule_lead(
        self, monkeypatch, tmp_path
    ):
        """W2: the non-degraded half of C2
        (max(schedule.lead_time, resolved_horizon_value or 0)) was
        untested for the case where the RESOLVED horizon_value exceeds
        the schedule's own configured lead. Schedule lead 1; an explicit
        `horizon_value=3` override must widen the window to lead 3's
        reach, not stay narrowed to lead 1's."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        q4_2025_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 182, "model_type": "GBT",
            "date": "2025-11-01", "valid_from": "2025-10-01", "valid_to": "2025-12-31",
        }

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            start, end = params.get("start_date"), params.get("end_date")
            row_date = q4_2025_row["date"]
            if (start is not None and row_date < start) or (end is not None and row_date > end):
                return _make_mock_response([])
            return _make_mock_response([q4_2025_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(
            station="19999", today=date(2027, 1, 5), horizon_value=3
        )

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 4
        assert result["year"].iloc[0] == 2025


class TestGetLongForecastsQuarterCalendarOnly:
    def test_rolling_window_excluded(self, monkeypatch):
        """Problem 2: a rolling Jun-Aug row (issued later than the calendar
        Apr-Jun row) must not win the dedup by virtue of a later date."""
        calendar_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 60, "date": "2026-03-25",
            "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        rolling_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 61, "date": "2026-05-25",
            "valid_from": "2026-06-01", "valid_to": "2026-08-31",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([calendar_row, rolling_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert len(result) == 1
        assert str(result["valid_from"].iloc[0].date()) == "2026-04-01"

    def test_valid_to_mismatch_excluded(self, monkeypatch):
        """R4(a): `valid_from` alone is a clean quarter start (day 1,
        month 4), so only the `valid_to` equality predicate — not the
        day/month checks — can catch a `valid_to` that is one month too
        long (Jul 31 instead of the Q2-correct Jun 30)."""
        bad_valid_to_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 62,
            "valid_from": "2026-04-01", "valid_to": "2026-07-31",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([bad_valid_to_row])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert result.empty


class TestGetLongForecastsQuarterNativeSelection:
    def test_native_row_preferred_over_rewrite(self, monkeypatch, tmp_path):
        """Problem 2: the flag-OFF rewrite (b) is dated LATER than the
        native row (a) but must not win — the caption must read the
        native issue date, not the rewrite's."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        native = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 70, "date": "2026-03-25",
            "valid_from": "2026-04-01", "valid_to": "2026-06-30", "q": 200.0,
        }
        rewrite = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 71, "date": "2026-04-01",
            "valid_from": "2026-04-01", "valid_to": "2026-06-30", "q": 999.0,
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([native, rewrite])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert len(result) == 1
        assert str(result["date"].iloc[0].date()) == "2026-03-25"
        assert result["forecasted_discharge"].iloc[0] == 200.0
        assert bool(result["is_native"].iloc[0]) is True
        # dtype contract (non-degraded): quarter_issue_date must be a
        # real datetime column, not object/float64, in the common case too.
        assert pd.api.types.is_datetime64_any_dtype(result["quarter_issue_date"])

        from dashboard.plot_manager import _format_quarterly_forecast_info
        caption = _format_quarterly_forecast_info(
            lambda s: s, None, None,
            valid_from=result["valid_from"].iloc[0],
            valid_to=result["valid_to"].iloc[0],
            quarter_issue_date=result["quarter_issue_date"].iloc[0],
        )
        assert "25th of March 2026" in caption

    def test_backfilled_older_quarter_does_not_hide_newer_quarter(self, monkeypatch, tmp_path):
        """Problem 2: dedup by issue date alone means a later backfill of
        an OLDER quarter (Q2, dated Jul 2) must not hide the same model's
        NEWER quarter (Q3, dated Jun 25)."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        q3_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 80, "model_type": "GBT",
            "date": "2026-06-25", "valid_from": "2026-07-01", "valid_to": "2026-09-30",
        }
        q2_backfill = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 81, "model_type": "GBT",
            "date": "2026-07-02", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([q2_backfill, q3_row])  # shuffled order

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 7, 10))

        assert set(result["quarter_in_year"]) == {2, 3}
        latest_quarter_row = result.loc[result["valid_from"].idxmax()]
        assert latest_quarter_row["quarter_in_year"] == 3

    def test_id_tie_break_on_equal_date(self, monkeypatch, tmp_path):
        """The `id` drop moved to AFTER the dedup so a same-date tie can be
        broken by the highest API id."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        lower_id = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 200, "model_type": "GBT",
            "date": "2026-03-22", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
            "q": 100.0,
        }
        higher_id = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 201, "model_type": "GBT",
            "date": "2026-03-22", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
            "q": 555.0,
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([lower_id, higher_id])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == 555.0
        assert "id" not in result.columns

    def test_flag_on_native_wins_over_later_legacy(self, monkeypatch, tmp_path):
        """Fresh vs legacy rows, flag ON: a fresh NATIVE row dated at the
        issue date must win over a legacy row dated LATER (at valid_from)."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        fresh_gbt = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 130, "model_type": "GBT",
            "date": "2026-12-25", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
            "q": 111.0,
        }
        legacy_gbt = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 131, "model_type": "GBT",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
            "q": 222.0,
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([legacy_gbt, fresh_gbt])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 5))

        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == 111.0
        assert str(result["date"].iloc[0].date()) == "2026-12-25"
        assert bool(result["is_native"].iloc[0]) is True

    def test_flag_off_fresh_wins_over_earlier_legacy(self, monkeypatch, tmp_path):
        """Fresh vs legacy rows, flag OFF: the fresh row (dated at
        valid_from) is returned over an earlier legacy row."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        fresh_naive = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 140, "model_type": "Naive Mean",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
            "q": 333.0,
        }
        legacy_naive = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 141, "model_type": "Naive Mean",
            "date": "2026-12-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
            "q": 444.0,
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([legacy_naive, fresh_naive])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 5))

        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == 333.0
        assert str(result["date"].iloc[0].date()) == "2027-01-01"

    def test_no_quarter_em_returned(self, monkeypatch, tmp_path):
        """Quarterly EM is retired; old EM rows must never be shown."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        gbt = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 150, "model_type": "GBT",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        naive_mean = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 151, "model_type": "Naive Mean",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        old_em = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 152, "model_type": "EM",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([gbt, naive_mean, old_em])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert "EM" not in set(result["model_short"])
        assert set(result["model_short"]) == {"GBT", "Naive Mean"}

    def test_no_quarter_em_returned_case_insensitive(self, monkeypatch, tmp_path):
        """R4(b) / C4: the EM filter's `.str.upper()` must catch any
        casing of both spellings ('EM' and 'ENSEMBLE_MEAN'), not just an
        exact match — including a bare lowercase/mixed-case 'EM' itself,
        not only the longer 'ENSEMBLE_MEAN' spelling."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        gbt = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 153, "model_type": "GBT",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        lower_ensemble_mean = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 154, "model_type": "ensemble_mean",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        upper_ensemble_mean = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 155, "model_type": "ENSEMBLE_MEAN",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        lower_em = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 156, "model_type": "em",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        mixed_em = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 157, "model_type": "Em",
            "date": "2026-03-25", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response(
                [gbt, lower_ensemble_mean, upper_ensemble_mean, lower_em, mixed_em]
            )

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert set(result["model_short"]) == {"GBT"}


class TestGetLongForecastsQuarterEligibility:
    def test_tjhm_eligibility_hides_not_yet_issued_quarter(self, monkeypatch, tmp_path):
        """tjhm (lead 0, issue day 1): Q4 2026 (fallback-derived, dated
        2026-10-01) must be hidden on 2026-09-26; the native Q3 row
        (issued 2026-07-01) is returned and correctly marked native."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=0, issue_day=1)
        naive_q4 = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 110, "model_type": "Naive Mean",
            "date": "2026-10-01", "valid_from": "2026-10-01", "valid_to": "2026-12-31",
        }
        lr_q3 = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 111, "model_type": "LR_Base",
            "date": "2026-07-01", "valid_from": "2026-07-01", "valid_to": "2026-09-30",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([naive_q4, lr_q3])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 26))

        assert len(result) == 1
        assert result["quarter_in_year"].iloc[0] == 3
        assert bool(result["is_native"].iloc[0]) is True

        from dashboard.plot_manager import _format_quarterly_forecast_info
        caption = _format_quarterly_forecast_info(
            lambda s: s, None, None,
            valid_from=result["valid_from"].iloc[0],
            valid_to=result["valid_to"].iloc[0],
            quarter_issue_date=result["quarter_issue_date"].iloc[0],
        )
        assert "Jul 2026" in caption and "Sep 2026" in caption
        assert "1st of July 2026" in caption

    def test_kghm_dec25_next_year_q1_becomes_eligible(self, monkeypatch, tmp_path):
        """kghm (lead 1, issue day 25): Q1 2027 becomes eligible exactly on
        2026-12-25, although its flag-OFF Skilled Mean row is dated at
        valid_from (2027-01-01)."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        skilled_mean = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 120, "model_type": "Skilled Mean",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }
        lr_base = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 121, "model_type": "LR_Base",
            "date": "2026-12-25", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }
        lr_sm = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 122, "model_type": "LR_SM",
            "date": "2026-12-25", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([skilled_mean, lr_base, lr_sm])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 12, 25))

        assert set(result["model_short"]) == {"Skilled Mean", "LR_Base", "LR_SM"}
        assert (result["quarter_in_year"] == 1).all()
        assert (result["year"] == 2027).all()


class TestGetLongForecastsQuarterIssueDayClamping:
    def test_issue_day_31_clamps_to_month_length_no_exception(self, monkeypatch, tmp_path):
        """A configured issue_day of 31 with lead 1 shifts Q3's issue month
        back to June (30 days) -> June 31 does not exist.
        pd.to_datetime on an unclamped day=31 would raise ValueError,
        crashing the monthly dashboard load / reservoir bulletin instead of
        degrading. The issue day must clamp to the issue month's own
        length (June 30), mirroring
        long_term_forecasting.lt_utils.nearest_scheduled_issue_date."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=31)
        gbt_q3 = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 160, "model_type": "GBT",
            "date": "2026-06-30", "valid_from": "2026-07-01", "valid_to": "2026-09-30",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([gbt_q3])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert len(result) == 1
        assert result["quarter_issue_date"].iloc[0] == pd.Timestamp("2026-06-30")
        assert bool(result["is_native"].iloc[0]) is True

    @pytest.mark.parametrize("bad_issue_day", [0, -1])
    def test_non_positive_issue_day_degrades_instead_of_crashing(
        self, monkeypatch, tmp_path, bad_issue_day
    ):
        """`_require_int_field` (long_term_horizon_resolver.py) only checks
        that operational_issue_day is an int, not that it is a valid
        day-of-month. A misconfigured 0 or negative value must not reach
        the date construction (ValueError, aborting the monthly dashboard
        load / reservoir bulletin) — it must degrade the same way an
        unresolvable schedule does, not invent a day."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=bad_issue_day)
        gbt_q3 = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 161, "model_type": "GBT",
            "date": "2026-06-25", "valid_from": "2026-07-01", "valid_to": "2026-09-30",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([gbt_q3])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 9, 1))

        assert len(result) == 1
        assert not result["is_native"].any()
        assert result["quarter_issue_date"].isna().all()


class TestGetLongForecastsQuarterDegraded:
    def test_degraded_schedule_no_native_preference(self, monkeypatch):
        """No `operational_issue_day` configured (the module's own autouse
        fixture shape) -> degraded: no native preference/LR strictness,
        eligibility falls back to `date <= today`, caption shows 'issue
        date not available'."""
        lr_row = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 95, "model_type": "LR_Base",
            "date": "2026-03-22", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        gbt_q2 = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 96, "model_type": "GBT",
            "date": "2026-03-22", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
        }
        gbt_q3 = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 97, "model_type": "GBT",
            "date": "2026-06-22", "valid_from": "2026-07-01", "valid_to": "2026-09-30",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([lr_row, gbt_q2, gbt_q3])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2026, 5, 1))

        assert len(result) == 2
        assert set(result["model_short"]) == {"LR_Base", "GBT"}
        assert not result["is_native"].any()
        assert result["quarter_issue_date"].isna().all()
        # R3: must be NaT (datetime), not float64 NaN — `_convert_na_to_nan`
        # + `infer_objects()` cannot tell an all-null datetime column from
        # an all-null float column apart on its own.
        assert pd.api.types.is_datetime64_any_dtype(result["quarter_issue_date"])

        from dashboard.plot_manager import _format_quarterly_forecast_info
        caption = _format_quarterly_forecast_info(
            lambda s: s, None, None,
            valid_from=result["valid_from"].iloc[0],
            valid_to=result["valid_to"].iloc[0],
            quarter_issue_date=None,
        )
        assert "issue date not available" in caption

    def test_kghm_fallback_quarter_no_native_lr(self, monkeypatch, tmp_path):
        """Round-2 decision 3: a fallback quarter (no native LR row) shows
        the derived models/ensembles but never a non-native LR row."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=1, issue_day=25)
        lr_rewrite = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 100, "model_type": "LR_Base",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }
        gbt = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 101, "model_type": "GBT",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }
        naive_mean = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 102, "model_type": "Naive Mean",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }
        skilled_mean = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 103, "model_type": "Skilled Mean",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([lr_rewrite, gbt, naive_mean, skilled_mean])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 5))

        assert set(result["model_short"]) == {"GBT", "Naive Mean", "Skilled Mean"}
        assert not result["is_native"].any()

        from dashboard.plot_manager import _format_quarterly_forecast_info
        caption = _format_quarterly_forecast_info(
            lambda s: s, None, None,
            valid_from=result["valid_from"].iloc[0],
            valid_to=result["valid_to"].iloc[0],
            quarter_issue_date=result["quarter_issue_date"].iloc[0],
        )
        assert "Jan 2027" in caption and "Mar 2027" in caption
        assert "25th of December 2026" in caption

    def test_tjhm_fallback_derived_q1_caption_from_schedule(self, monkeypatch, tmp_path):
        """tjhm (lead 0): a fallback-derived Q1 2027 (no native LR row)
        still gets its issue date from the schedule, not 'unavailable'."""
        _configure_quarter_schedule(monkeypatch, tmp_path, lead=0, issue_day=1)
        gbt = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 90, "model_type": "GBT",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }
        naive_mean = {
            **_QUARTER_FORECAST_RECORD_19999, "id": 91, "model_type": "Naive Mean",
            "date": "2027-01-01", "valid_from": "2027-01-01", "valid_to": "2027-03-31",
        }

        def mock_get(url, **kwargs):
            return _make_mock_response([gbt, naive_mean])

        monkeypatch.setattr(requests, "get", mock_get)

        result = db.get_long_forecasts_quarter(station="19999", today=date(2027, 1, 2))

        assert len(result) == 2
        assert not (result["model_short"] == "LR_Base").any()
        assert (result["quarter_issue_date"] == pd.Timestamp("2027-01-01")).all()

        from dashboard.plot_manager import _format_quarterly_forecast_info
        caption = _format_quarterly_forecast_info(
            lambda s: s, None, None,
            valid_from=result["valid_from"].iloc[0],
            valid_to=result["valid_to"].iloc[0],
            quarter_issue_date=result["quarter_issue_date"].iloc[0],
        )
        assert "Jan 2027" in caption and "Mar 2027" in caption
        assert "1st of January 2027" in caption
