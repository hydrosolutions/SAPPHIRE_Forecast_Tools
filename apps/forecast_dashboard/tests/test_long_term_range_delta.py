"""Tests for long-term observation range statistics reader/joins (PP-075 E2).

Covers:
- _range_period_label_or_raise: config validation including calendar bounds
- _read_long_term_hydrograph_stats: API params, date validation, count gate,
  code/horizon_type filtering, duplicate-marks-unavailable, std→delta
- _attach_range_delta: row identity, monthly/quarterly/seasonal mapping,
  valid_from override for monthly, station_code filtering, seasonal valid_to gate
- Integration via _get_data_monthly, _get_data_quarter, _get_data_season

Station code 19999 throughout; no real station codes.
"""

import json
from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests
from src import db

STATION = "19999"
PUB_YEAR = 2026  # end_year=2025 < 2026 → valid with defaults


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _hydrograph_row(horizon, pub_year, horizon_value, std=10.0, count=20):
    """Synthetic hydrograph API row with std and count."""
    if horizon == "month":
        d = date(pub_year, horizon_value, 1).isoformat()
    elif horizon == "quarter":
        m = (horizon_value - 1) * 3 + 1
        d = date(pub_year, m, 1).isoformat()
    elif horizon == "season":
        d = date(pub_year, 4, 1).isoformat()
    else:
        d = date(pub_year, 1, 1).isoformat()
    return {
        "id": horizon_value,
        "horizon_type": horizon,
        "horizon_value": horizon_value,
        "code": STATION,
        "date": d,
        "day_of_year": 1,
        "horizon_in_year": horizon_value,
        "norm": 100.0,
        "previous": 90.0,
        "current": 95.0,
        "q05": None, "q25": None, "q50": None, "q75": None, "q95": None,
        "std": std,
        "count": count,
    }


def _long_forecast_row(code, month, year=2026, model="GBT", horizon_value=1, q=100.0):
    return {
        "id": 1,
        "horizon_type": "month",
        "horizon_value": horizon_value,
        "code": code,
        "date": f"{year}-{month:02d}-22",
        "model_type": model,
        "model_type_description": model,
        "valid_from": f"{year}-{month:02d}-01",
        "valid_to": f"{year}-{month:02d}-28",
        "flag": 0,
        "composition": "",
        "q": q,
        "q_obs": None,
        "q05": 80.0, "q10": 85.0, "q25": 90.0, "q50": 100.0,
        "q75": 110.0, "q90": 115.0, "q95": 120.0,
        "q_xgb": None, "q_lgbm": None, "q_catboost": None, "q_loc": None,
    }


def _quarter_forecast_row(code, quarter_start_month, year=2026, model="GBT", horizon_value=1, q=100.0):
    quarter_end_month = quarter_start_month + 2
    quarter_end_day = 31 if quarter_end_month in (1, 3, 5, 7, 8, 10, 12) else 30
    return {
        "id": 1,
        "horizon_type": "quarter",
        "horizon_value": horizon_value,
        "code": code,
        "date": f"{year}-{quarter_start_month - 1:02d}-25" if quarter_start_month > 1 else f"{year - 1}-12-25",
        "model_type": model,
        "model_type_description": model,
        "valid_from": f"{year}-{quarter_start_month:02d}-01",
        "valid_to": f"{year}-{quarter_end_month:02d}-{quarter_end_day}",
        "flag": 0, "composition": "",
        "q": q,
        "q_obs": None,
        "q05": 80.0, "q10": 85.0, "q25": 90.0, "q50": 100.0,
        "q75": 110.0, "q90": 115.0, "q95": 120.0,
        "q_xgb": None, "q_lgbm": None, "q_catboost": None, "q_loc": None,
    }


def _season_forecast_row(code, year=2026, model="GBT", horizon_value=1, q=100.0):
    return {
        "id": 1,
        "horizon_type": "season",
        "horizon_value": horizon_value,
        "code": code,
        "date": f"{year}-04-01",
        "model_type": model,
        "model_type_description": model,
        "valid_from": f"{year}-04-01",
        "valid_to": f"{year}-09-30",
        "flag": 0, "composition": "",
        "q": q, "q_obs": None,
        "q05": 80.0, "q10": 85.0, "q25": 90.0, "q50": 100.0,
        "q75": 110.0, "q90": 115.0, "q95": 120.0,
        "q_xgb": None, "q_lgbm": None, "q_catboost": None, "q_loc": None,
    }


# ---------------------------------------------------------------------------
# Shared fixture: minimal long-term configs (follows test_db.py pattern)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _lt_config_env(monkeypatch, tmp_path):
    config_dir = tmp_path / "long_term_configs"
    config_dir.mkdir()
    for name, lead in [
        ("month_1", 1), ("month_0", 0),
        ("quarter", 1),
        ("seasonal_january", 3), ("seasonal_april", 0),
    ]:
        (config_dir / f"{name}.json").write_text(
            json.dumps({
                "operational_month_lead_time": lead,
                "operational_issue_day": 1,
            })
        )
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term_configs")
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_supported_modes",
        "month_0,month_1,quarter,seasonal_january,seasonal_april",
    )
    # Clear range env vars so defaults apply; individual tests can override.
    monkeypatch.delenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", raising=False)
    monkeypatch.delenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", raising=False)
    return config_dir


# ---------------------------------------------------------------------------
# Tests: _range_period_label_or_raise
# ---------------------------------------------------------------------------

class TestRangePeriodLabel:
    def test_defaults_produce_correct_label(self):
        assert db._range_period_label_or_raise(2026) == "2000–2025"

    def test_custom_valid_config(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "1990")
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2020")
        assert db._range_period_label_or_raise(2026) == "1990–2020"

    def test_malformed_start_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "abc")
        with pytest.raises(ValueError, match="malformed"):
            db._range_period_label_or_raise(2026)

    def test_malformed_end_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2025.5")
        with pytest.raises(ValueError, match="malformed"):
            db._range_period_label_or_raise(2026)

    def test_reversed_start_end_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "2010")
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2005")
        with pytest.raises(ValueError, match="2010"):
            db._range_period_label_or_raise(2026)

    def test_end_equals_pub_year_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2026")
        with pytest.raises(ValueError, match="pub_year"):
            db._range_period_label_or_raise(2026)

    def test_end_one_less_than_pub_year_valid(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2025")
        label = db._range_period_label_or_raise(2026)
        assert "2025" in label

    def test_end_greater_than_pub_year_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2027")
        with pytest.raises(ValueError):
            db._range_period_label_or_raise(2026)

    def test_year_zero_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "0")
        with pytest.raises(ValueError, match="calendar range"):
            db._range_period_label_or_raise(2026)

    def test_year_10000_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "10000")
        with pytest.raises(ValueError, match="calendar range"):
            db._range_period_label_or_raise(2026)

    def test_negative_year_raises(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "-1")
        with pytest.raises(ValueError):
            db._range_period_label_or_raise(2026)


# ---------------------------------------------------------------------------
# Tests: _is_valid_period_date
# ---------------------------------------------------------------------------

class TestIsValidPeriodDate:
    def test_monthly_first_of_month(self):
        assert db._is_valid_period_date("2026-04-01", "month", 4, 2026)

    def test_monthly_wrong_day(self):
        assert not db._is_valid_period_date("2026-04-15", "month", 4, 2026)

    def test_monthly_wrong_month(self):
        assert not db._is_valid_period_date("2026-05-01", "month", 4, 2026)

    def test_monthly_wrong_year(self):
        assert not db._is_valid_period_date("2025-04-01", "month", 4, 2026)

    def test_quarterly_q2_april(self):
        assert db._is_valid_period_date("2026-04-01", "quarter", 2, 2026)

    def test_quarterly_q1_january(self):
        assert db._is_valid_period_date("2026-01-01", "quarter", 1, 2026)

    def test_quarterly_wrong_month_for_q2(self):
        assert not db._is_valid_period_date("2026-05-01", "quarter", 2, 2026)

    def test_seasonal_april_1(self):
        assert db._is_valid_period_date("2026-04-01", "season", 1, 2026)

    def test_seasonal_wrong_month(self):
        assert not db._is_valid_period_date("2026-05-01", "season", 1, 2026)

    def test_none_date(self):
        assert not db._is_valid_period_date(None, "month", 4, 2026)


# ---------------------------------------------------------------------------
# Tests: _read_long_term_hydrograph_stats
# ---------------------------------------------------------------------------

class TestReadLongTermHydrographStats:
    def test_monthly_all_12_rows_correct_delta(self, monkeypatch):
        rows = [_hydrograph_row("month", PUB_YEAR, m, std=10.0) for m in range(1, 13)]

        def mock_get(url, **kwargs):
            return _make_mock_response(rows)

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert len(result) == 12
        for m in range(1, 13):
            assert result[m] == pytest.approx(0.674 * 10.0)

    def test_pins_api_params(self, monkeypatch):
        seen = []

        def mock_get(url, **kwargs):
            seen.append(kwargs.get("params", {}))
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert seen[0]["horizon"] == "month"
        assert seen[0]["code"] == STATION
        assert seen[0]["start_date"] == f"{PUB_YEAR}-01-01"
        assert seen[0]["end_date"] == f"{PUB_YEAR}-12-31"

    def test_null_std_gives_none_in_lookup(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 4, std=None)

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[4] is None

    def test_zero_std_gives_zero_delta(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 6, std=0.0)

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[6] == 0.0

    def test_negative_std_gives_none(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 3, std=-5.0)

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[3] is None

    def test_empty_response_returns_empty_dict(self, monkeypatch):
        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result == {}

    def test_wrong_year_date_filtered_out(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 4, std=10.0)
        row["date"] = "2025-04-01"  # wrong year

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert 4 not in result

    def test_wrong_station_row_filtered(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 4, std=10.0)
        row["code"] = "other_station"

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert 4 not in result

    def test_wrong_horizon_type_row_filtered(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 4, std=10.0)
        row["horizon_type"] = "quarter"  # wrong horizon type

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert 4 not in result

    def test_duplicate_valid_period_rows_marked_unavailable(self, monkeypatch):
        """Two valid rows for the same period → unavailable (None)."""
        row1 = _hydrograph_row("month", PUB_YEAR, 5, std=10.0, count=20)
        row2 = _hydrograph_row("month", PUB_YEAR, 5, std=20.0, count=20)

        def mock_get(url, **kwargs):
            return _make_mock_response([row1, row2])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[5] is None

    def test_null_then_valid_duplicate_no_resurrection(self, monkeypatch):
        """First row null std, second row valid std → still None (no resurrection)."""
        row1 = _hydrograph_row("month", PUB_YEAR, 5, std=None, count=20)
        row2 = _hydrograph_row("month", PUB_YEAR, 5, std=10.0, count=20)

        def mock_get(url, **kwargs):
            return _make_mock_response([row1, row2])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[5] is None

    def test_shuffled_duplicate_no_resurrection(self, monkeypatch):
        """Reversed row order: still None for the duplicate period."""
        row1 = _hydrograph_row("month", PUB_YEAR, 5, std=10.0, count=20)
        row2 = _hydrograph_row("month", PUB_YEAR, 5, std=20.0, count=20)

        def mock_get(url, **kwargs):
            return _make_mock_response([row2, row1])  # reversed

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[5] is None

    def test_quarterly_lookup_correct_keys(self, monkeypatch):
        rows = [_hydrograph_row("quarter", PUB_YEAR, q, std=5.0) for q in range(1, 5)]

        def mock_get(url, **kwargs):
            return _make_mock_response(rows)

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "quarter", PUB_YEAR)
        assert set(result.keys()) == {1, 2, 3, 4}
        for q in range(1, 5):
            assert result[q] == pytest.approx(0.674 * 5.0)

    def test_seasonal_april1_row_accepted(self, monkeypatch):
        row = _hydrograph_row("season", PUB_YEAR, 1, std=8.0)

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "season", PUB_YEAR)
        assert result[1] == pytest.approx(0.674 * 8.0)

    def test_api_exception_propagates(self, monkeypatch):
        def mock_get(url, **kwargs):
            raise requests.exceptions.ConnectionError("unreachable")

        monkeypatch.setattr(requests, "get", mock_get)
        with pytest.raises(requests.exceptions.ConnectionError):
            db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)

    def test_threshold_12_vs_13_check(self, monkeypatch):
        """count=12 with finite std rejects; count=13 with finite std accepts.

        Default period 2000-2025 = 26 years → min_count = ceil(26/2) = 13.
        std=10.0 on the count=12 row proves the count gate blocks a valid std,
        not a vacuous null-std pass-through.
        """
        row_below = _hydrograph_row("month", PUB_YEAR, 7, std=10.0, count=12)
        row_at = _hydrograph_row("month", PUB_YEAR, 8, std=15.0, count=13)

        def mock_get(url, **kwargs):
            return _make_mock_response([row_below, row_at])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[7] is None
        assert result[8] == pytest.approx(0.674 * 15.0)

    def test_odd_window_ceil_threshold(self, monkeypatch):
        """27-year window (2000-2026, pub_year=2027) → min_count=ceil(27/2)=14.

        count=13 → rejected; count=14 → accepted.
        """
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "2000")
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2026")
        pub_year = 2027
        row_below = _hydrograph_row("month", pub_year, 3, std=10.0, count=13)
        row_at = _hydrograph_row("month", pub_year, 4, std=10.0, count=14)

        def mock_get(url, **kwargs):
            return _make_mock_response([row_below, row_at])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", pub_year)
        assert result[3] is None
        assert result[4] == pytest.approx(0.674 * 10.0)

    def test_null_count_gives_none(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 4, std=10.0, count=None)

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[4] is None

    def test_negative_count_gives_none(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 4, std=10.0, count=-1)

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[4] is None

    def test_noninteger_count_gives_none(self, monkeypatch):
        row = _hydrograph_row("month", PUB_YEAR, 4, std=10.0, count="bad")

        def mock_get(url, **kwargs):
            return _make_mock_response([row])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[4] is None

    def test_fractional_count_gives_none(self, monkeypatch):
        """count=13.5 with finite std must mark the row unavailable.

        int(13.5)==13 passes the old gate; float→floor check rejects it.
        13.0 (same int value) must still be accepted.
        Default period 2000-2025 → min_count=13.
        """
        row_frac = _hydrograph_row("month", PUB_YEAR, 7, std=10.0, count=13.5)
        row_exact = _hydrograph_row("month", PUB_YEAR, 8, std=10.0, count=13.0)

        def mock_get(url, **kwargs):
            return _make_mock_response([row_frac, row_exact])

        monkeypatch.setattr(requests, "get", mock_get)
        result = db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)
        assert result[7] is None, "count=13.5 must be rejected as fractional"
        assert result[8] == pytest.approx(0.674 * 10.0), "count=13.0 must be accepted as 13"

    def test_invalid_config_propagates_from_stats_reader(self, monkeypatch):
        """Invalid range config raises from within the stats reader itself."""
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2026")  # >= pub_year

        def mock_get(url, **kwargs):
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        with pytest.raises(ValueError):
            db._read_long_term_hydrograph_stats(STATION, "month", PUB_YEAR)


# ---------------------------------------------------------------------------
# Tests: _attach_range_delta
# ---------------------------------------------------------------------------

class TestAttachRangeDelta:
    def _monthly_df(self, months=(4, 5, 6), models=("GBT", "LR"), code=STATION):
        rows = []
        for m in months:
            for model in models:
                rows.append({
                    "code": code,
                    "month_in_year": m,
                    "model_short": model,
                    "forecasted_discharge": 100.0,
                    "delta": 5.0,
                    "accuracy": 80.0,
                })
        return pd.DataFrame(rows)

    def test_monthly_attaches_correct_delta(self):
        df = self._monthly_df(months=(4, 5))
        lookup = {4: 3.0, 5: 4.0}
        result = db._attach_range_delta(df, lookup, "month")
        m4_rows = result[result["month_in_year"] == 4]
        m5_rows = result[result["month_in_year"] == 5]
        assert m4_rows["range_delta"].tolist() == pytest.approx([3.0] * len(m4_rows))
        assert m5_rows["range_delta"].tolist() == pytest.approx([4.0] * len(m5_rows))

    def test_monthly_missing_period_gets_nan(self):
        df = self._monthly_df(months=(3, 4))
        lookup = {4: 3.0}  # month 3 not in lookup
        result = db._attach_range_delta(df, lookup, "month")
        assert result[result["month_in_year"] == 3]["range_delta"].isna().all()

    def test_monthly_none_in_lookup_gives_nan(self):
        df = self._monthly_df(months=(6,))
        lookup = {6: None}
        result = db._attach_range_delta(df, lookup, "month")
        assert result["range_delta"].isna().all()

    def test_skill_delta_column_unchanged(self):
        df = self._monthly_df(months=(4,))
        lookup = {4: 2.5}
        result = db._attach_range_delta(df, lookup, "month")
        assert (result["delta"] == 5.0).all()
        assert result["range_delta"].tolist() == pytest.approx([2.5] * len(result))

    def test_no_skill_rows_still_get_range_delta(self):
        df = pd.DataFrame([{
            "code": STATION,
            "month_in_year": 7,
            "model_short": "LR",
            "forecasted_discharge": 100.0,
            # No delta or accuracy columns (no skill)
        }])
        result = db._attach_range_delta(df, {7: 1.5}, "month")
        assert result["range_delta"].iloc[0] == pytest.approx(1.5)

    def test_row_count_preserved(self):
        df = self._monthly_df(months=(1, 2, 3, 4, 5, 6), models=("GBT", "LR", "NE"))
        result = db._attach_range_delta(df, {m: float(m) for m in range(1, 7)}, "month")
        assert len(result) == len(df)

    def test_row_order_preserved(self):
        df = self._monthly_df(months=(6, 1, 3))
        result = db._attach_range_delta(df, {1: 1.0, 3: 3.0, 6: 6.0}, "month")
        assert list(result["month_in_year"]) == list(df["month_in_year"])

    def test_index_preserved(self):
        df = self._monthly_df(months=(4, 5)).reset_index(drop=False)
        df.index = [10, 11, 12, 13]
        result = db._attach_range_delta(df, {4: 2.0, 5: 3.0}, "month")
        assert list(result.index) == [10, 11, 12, 13]

    def test_empty_df_gets_range_delta_column(self):
        df = pd.DataFrame(columns=["code", "month_in_year", "model_short"])
        result = db._attach_range_delta(df, {4: 2.0}, "month")
        assert "range_delta" in result.columns
        assert len(result) == 0

    def test_different_models_same_period_share_value(self):
        df = self._monthly_df(months=(4,), models=("GBT", "LR", "NE"))
        result = db._attach_range_delta(df, {4: 7.0}, "month")
        assert result["range_delta"].tolist() == pytest.approx([7.0, 7.0, 7.0])
        assert len(result) == 3  # no row expansion

    def test_monthly_valid_from_overrides_month_in_year(self):
        """When valid_from is present, its month is used rather than month_in_year."""
        df = pd.DataFrame([{
            "code": STATION,
            "month_in_year": 4,  # stale cached value
            "valid_from": "2026-05-01",  # actual target month
            "model_short": "GBT",
        }])
        result = db._attach_range_delta(df, {4: 4.0, 5: 5.0}, "month")
        # Must use valid_from month (5), not month_in_year (4)
        assert result["range_delta"].iloc[0] == pytest.approx(5.0)

    def test_monthly_falls_back_to_month_in_year_when_no_valid_from(self):
        df = pd.DataFrame([{
            "code": STATION,
            "month_in_year": 3,
            "model_short": "GBT",
        }])
        result = db._attach_range_delta(df, {3: 9.0}, "month")
        assert result["range_delta"].iloc[0] == pytest.approx(9.0)

    def test_mixed_station_other_station_gets_nan(self):
        """When station_code provided, only matching rows get range_delta."""
        df = pd.DataFrame([
            {"code": STATION, "month_in_year": 4, "valid_from": "2026-04-01", "model_short": "GBT"},
            {"code": "other_station", "month_in_year": 4, "valid_from": "2026-04-01", "model_short": "GBT"},
        ])
        result = db._attach_range_delta(df, {4: 3.0}, "month", station_code=STATION)
        assert result.iloc[0]["range_delta"] == pytest.approx(3.0)
        assert pd.isna(result.iloc[1]["range_delta"])

    def test_quarterly_joins_by_quarter_in_year(self):
        df = pd.DataFrame([
            {"code": STATION, "quarter_in_year": 1, "model_short": "GBT", "forecasted_discharge": 100.0},
            {"code": STATION, "quarter_in_year": 2, "model_short": "GBT", "forecasted_discharge": 100.0},
            {"code": STATION, "quarter_in_year": 3, "model_short": "GBT", "forecasted_discharge": 100.0},
        ])
        lookup = {1: 5.0, 2: None, 3: 8.0}
        result = db._attach_range_delta(df, lookup, "quarter")
        assert result.iloc[0]["range_delta"] == pytest.approx(5.0)
        assert pd.isna(result.iloc[1]["range_delta"])
        assert result.iloc[2]["range_delta"] == pytest.approx(8.0)

    def test_quarterly_historical_year_arbitrary_lead(self):
        """Historical-year row with arbitrary lead still gets correct quarter delta."""
        df = pd.DataFrame([{
            "code": STATION,
            "quarter_in_year": 2,
            "model_short": "GBT",
            "horizon_value": 3,
            "year": 2025,
        }])
        result = db._attach_range_delta(df, {2: 4.5}, "quarter")
        assert result["range_delta"].iloc[0] == pytest.approx(4.5)

    def test_seasonal_apr_sep_gets_delta(self):
        df = pd.DataFrame([{
            "code": STATION,
            "season_in_year": 1,
            "model_short": "GBT",
            "forecasted_discharge": 100.0,
            "valid_from": "2026-04-01",
            "valid_to": "2026-09-30",
        }])
        result = db._attach_range_delta(df, {1: 6.0}, "season")
        assert result["range_delta"].iloc[0] == pytest.approx(6.0)

    def test_seasonal_non_apr_sep_gets_nan(self):
        df = pd.DataFrame([{
            "code": STATION,
            "season_in_year": 2,
            "model_short": "GBT",
            "forecasted_discharge": 100.0,
            "valid_from": "2026-10-01",
            "valid_to": "2027-03-31",
        }])
        result = db._attach_range_delta(df, {1: 6.0}, "season")
        assert pd.isna(result["range_delta"].iloc[0])

    def test_seasonal_wrong_valid_to_gets_nan(self):
        df = pd.DataFrame([{
            "code": STATION,
            "season_in_year": 1,
            "model_short": "GBT",
            "forecasted_discharge": 100.0,
            "valid_from": "2026-04-01",
            "valid_to": "2026-08-31",  # wrong end
        }])
        result = db._attach_range_delta(df, {1: 6.0}, "season")
        assert pd.isna(result["range_delta"].iloc[0])

    def test_seasonal_null_in_lookup_gives_nan(self):
        df = pd.DataFrame([{
            "code": STATION,
            "season_in_year": 1,
            "model_short": "GBT",
            "forecasted_discharge": 100.0,
            "valid_from": "2026-04-01",
            "valid_to": "2026-09-30",
        }])
        result = db._attach_range_delta(df, {1: None}, "season")
        assert pd.isna(result["range_delta"].iloc[0])

    def test_seasonal_absent_valid_to_gives_nan(self):
        """Seasonal row with valid_from but no valid_to column → NaN (fallback removed)."""
        df = pd.DataFrame([{
            "code": STATION,
            "season_in_year": 1,
            "model_short": "GBT",
            "valid_from": "2026-04-01",
            # No valid_to column
        }])
        result = db._attach_range_delta(df, {1: 6.0}, "season")
        assert pd.isna(result["range_delta"].iloc[0])

    def test_different_years_models_leads_same_period_share_value(self):
        """Rows targeting the same period (different models/leads) get the same range_delta."""
        df = pd.DataFrame([
            {"code": STATION, "month_in_year": 4, "model_short": "GBT", "horizon_value": 0},
            {"code": STATION, "month_in_year": 4, "model_short": "GBT", "horizon_value": 1},
            {"code": STATION, "month_in_year": 4, "model_short": "LR", "horizon_value": 1},
        ])
        result = db._attach_range_delta(df, {4: 9.9}, "month")
        assert result["range_delta"].tolist() == pytest.approx([9.9, 9.9, 9.9])
        assert len(result) == 3

    def test_december_issued_january_target(self):
        """Dec-issued forecast with month_in_year=1 (January target) gets January delta."""
        df = pd.DataFrame([{
            "code": STATION,
            "month_in_year": 1,
            "model_short": "GBT",
            "forecasted_discharge": 50.0,
        }])
        result = db._attach_range_delta(df, {1: 11.0}, "month")
        assert result["range_delta"].iloc[0] == pytest.approx(11.0)

    def test_seasonal_other_window_lead_does_not_confuse_period(self):
        """season_in_year is a forecast lead, NOT the season identity — do not join by it."""
        df = pd.DataFrame([{
            "code": STATION,
            "season_in_year": 3,  # lead=3, NOT season-3
            "model_short": "GBT",
            "forecasted_discharge": 100.0,
            "valid_from": "2026-04-01",
            "valid_to": "2026-09-30",
        }])
        # The Apr-Sep window should still receive the range_delta regardless of season_in_year
        result = db._attach_range_delta(df, {1: 7.5}, "season")
        assert result["range_delta"].iloc[0] == pytest.approx(7.5)


# ---------------------------------------------------------------------------
# Integration: _get_data_monthly
# ---------------------------------------------------------------------------

class TestGetDataMonthlyRange:
    """_get_data_monthly attaches range_delta and sets range_reference_period."""

    def _make_dispatcher(self, month_stats_rows, long_forecast_rows=None, skill_rows=None):
        """Return a requests.get mock dispatching by URL+params."""
        long_forecast_rows = long_forecast_rows or [_long_forecast_row(STATION, 4)]
        skill_rows = skill_rows or []

        def _dispatch(url, **kwargs):
            params = kwargs.get("params", {})
            if "hydrograph" in url:
                horizon = params.get("horizon", "")
                if horizon == "month":
                    return _make_mock_response(month_stats_rows)
                if horizon == "quarter":
                    return _make_mock_response([])
                return _make_mock_response([])
            if "long-forecast" in url:
                return _make_mock_response(long_forecast_rows)
            if "skill-metric" in url:
                return _make_mock_response(skill_rows)
            # rain, temp, snow, day hydrograph
            return _make_mock_response([])

        return _dispatch

    def test_forecasts_all_has_range_delta_column(self, monkeypatch):
        stats_rows = [_hydrograph_row("month", PUB_YEAR, 4, std=10.0)]
        forecast_rows = [_long_forecast_row(STATION, 4)]
        monkeypatch.setattr(requests, "get", self._make_dispatcher(stats_rows, forecast_rows))
        data = db._get_data_monthly(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=date(PUB_YEAR, 4, 15),
        )
        assert "range_delta" in data["forecasts_all"].columns

    def test_range_reference_period_in_data(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        data = db._get_data_monthly(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=date(PUB_YEAR, 4, 15),
        )
        assert data["range_reference_period"] == "2000–2025"

    def test_no_snow_ref_date_gives_none_period_and_nan_delta(self, monkeypatch):
        stats_rows = [_hydrograph_row("month", PUB_YEAR, 4, std=10.0)]
        monkeypatch.setattr(requests, "get", self._make_dispatcher(stats_rows))
        data = db._get_data_monthly(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=None,
        )
        assert data["range_reference_period"] is None
        fa = data["forecasts_all"]
        if not fa.empty and "range_delta" in fa.columns:
            assert fa["range_delta"].isna().all()

    def test_invalid_config_propagates_error(self, monkeypatch):
        """Invalid range config raises ValueError through _get_data_monthly."""
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2026")  # invalid: >= pub_year
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        with pytest.raises(ValueError):
            db._get_data_monthly(
                STATION, pd.DataFrame(), lambda df: df, lambda df: df,
                snow_ref_date=date(PUB_YEAR, 4, 15),
            )

    def test_failed_stats_read_propagates_error(self, monkeypatch):
        """API failure in hydrograph stats read propagates through _get_data_monthly."""
        def _fail(url, **kwargs):
            if "hydrograph" in url and kwargs.get("params", {}).get("horizon") == "month":
                raise requests.exceptions.ConnectionError("fail")
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", _fail)
        with pytest.raises(requests.exceptions.ConnectionError):
            db._get_data_monthly(
                STATION, pd.DataFrame(), lambda df: df, lambda df: df,
                snow_ref_date=date(PUB_YEAR, 4, 15),
            )

    def test_skill_delta_retained_alongside_range_delta(self, monkeypatch):
        """Skill δ=5.5, accuracy=85 and range_delta=6.74 coexist on the same row;
        native quantiles Q25/Q75 are unchanged."""
        stats_rows = [_hydrograph_row("month", PUB_YEAR, 4, std=10.0)]
        forecast_rows = [_long_forecast_row(STATION, 4)]
        skill_rows = [{
            "id": 1,
            "horizon_type": "month",
            "horizon_value": 1,
            "code": STATION,
            "date": f"{PUB_YEAR}-01-01",
            "horizon_in_year": 4,
            "model_type": "GBT",
            "model_type_description": "GBT",
            "nse": 0.75,
            "accuracy": 85.0,
            "delta": 5.5,
            "n_pairs": 10,
            "sdivsigma": 0.6,
        }]
        monkeypatch.setattr(requests, "get", self._make_dispatcher(stats_rows, forecast_rows, skill_rows))
        data = db._get_data_monthly(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=date(PUB_YEAR, 4, 15),
        )
        fa = data["forecasts_all"]
        assert not fa.empty, "forecasts_all must not be empty"
        gbt_rows = fa[fa["model_short"] == "GBT"]
        assert not gbt_rows.empty, "GBT row must be present in forecasts_all"
        row = gbt_rows.iloc[0]
        # Skill columns attached by merge
        assert row["delta"] == pytest.approx(5.5), "skill delta must be 5.5"
        assert row["accuracy"] == pytest.approx(85.0), "accuracy must be 85.0"
        # Range statistics column attached by _attach_range_delta (0.674 * std=10)
        assert row["range_delta"] == pytest.approx(6.74), "range_delta must be 0.674 * 10.0"
        # Native forecast quantiles unchanged by either join
        assert row["Q25"] == pytest.approx(90.0)
        assert row["Q75"] == pytest.approx(110.0)


# ---------------------------------------------------------------------------
# Integration: _get_data_quarter
# ---------------------------------------------------------------------------

class TestGetDataQuarterRange:
    def _make_dispatcher(self, quarter_stats_rows, forecast_rows=None, skill_rows=None):
        forecast_rows = forecast_rows or [_quarter_forecast_row(STATION, 4)]
        skill_rows = skill_rows or []

        def _dispatch(url, **kwargs):
            params = kwargs.get("params", {})
            if "hydrograph" in url and params.get("horizon") == "quarter":
                return _make_mock_response(quarter_stats_rows)
            if "long-forecast" in url:
                return _make_mock_response(forecast_rows)
            if "skill-metric" in url:
                return _make_mock_response(skill_rows)
            return _make_mock_response([])

        return _dispatch

    def test_forecasts_all_has_range_delta(self, monkeypatch):
        rows = [_hydrograph_row("quarter", PUB_YEAR, 2, std=5.0)]
        monkeypatch.setattr(requests, "get", self._make_dispatcher(rows))
        data = db._get_data_quarter(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=date(PUB_YEAR, 4, 15),
        )
        assert "range_delta" in data["forecasts_all"].columns

    def test_range_reference_period_set(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        data = db._get_data_quarter(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=date(PUB_YEAR, 4, 15),
        )
        assert data["range_reference_period"] == "2000–2025"

    def test_no_snow_ref_date_gives_none(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        data = db._get_data_quarter(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=None,
        )
        assert data["range_reference_period"] is None

    def test_invalid_config_propagates_error(self, monkeypatch):
        """Invalid range config raises ValueError through _get_data_quarter."""
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2026")
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        with pytest.raises(ValueError):
            db._get_data_quarter(
                STATION, pd.DataFrame(), lambda df: df, lambda df: df,
                snow_ref_date=date(PUB_YEAR, 4, 15),
            )

    def test_quarter_validation_non_calendar_quarter_filtered(self, monkeypatch):
        """A quarter row with a wrong date (not Q-start) is not included in lookup."""
        row = _hydrograph_row("quarter", PUB_YEAR, 2, std=5.0)
        row["date"] = f"{PUB_YEAR}-05-01"  # May 1 is not Q2 start

        def _dispatch(url, **kwargs):
            if "hydrograph" in url:
                return _make_mock_response([row])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", _dispatch)
        result = db._read_long_term_hydrograph_stats(STATION, "quarter", PUB_YEAR)
        assert 2 not in result


# ---------------------------------------------------------------------------
# Integration: _get_data_season
# ---------------------------------------------------------------------------

class TestGetDataSeasonRange:
    def _make_dispatcher(self, season_stats_rows, forecast_rows=None, skill_rows=None):
        forecast_rows = forecast_rows or [_season_forecast_row(STATION)]
        skill_rows = skill_rows or []

        def _dispatch(url, **kwargs):
            params = kwargs.get("params", {})
            if "hydrograph" in url and params.get("horizon") == "season":
                return _make_mock_response(season_stats_rows)
            if "long-forecast" in url:
                return _make_mock_response(forecast_rows)
            if "skill-metric" in url:
                return _make_mock_response(skill_rows)
            return _make_mock_response([])

        return _dispatch

    def test_forecasts_all_has_range_delta(self, monkeypatch):
        rows = [_hydrograph_row("season", PUB_YEAR, 1, std=8.0)]
        monkeypatch.setattr(requests, "get", self._make_dispatcher(rows))
        data = db._get_data_season(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=date(PUB_YEAR, 4, 15),
        )
        assert "range_delta" in data["forecasts_all"].columns

    def test_range_reference_period_set(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        data = db._get_data_season(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=date(PUB_YEAR, 4, 15),
        )
        assert data["range_reference_period"] == "2000–2025"

    def test_no_snow_ref_date_gives_none(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        data = db._get_data_season(
            STATION, pd.DataFrame(), lambda df: df, lambda df: df,
            snow_ref_date=None,
        )
        assert data["range_reference_period"] is None

    def test_invalid_config_propagates_error(self, monkeypatch):
        """Invalid range config raises ValueError through _get_data_season."""
        monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2026")
        monkeypatch.setattr(requests, "get", self._make_dispatcher([]))
        with pytest.raises(ValueError):
            db._get_data_season(
                STATION, pd.DataFrame(), lambda df: df, lambda df: df,
                snow_ref_date=date(PUB_YEAR, 4, 15),
            )

    def test_seasonal_other_window_lead_does_not_confuse_period(self, monkeypatch):
        """season_in_year is a forecast lead, NOT the season identity — do not join by it."""
        df = pd.DataFrame([{
            "code": STATION,
            "season_in_year": 3,  # lead=3, NOT season-3
            "model_short": "GBT",
            "forecasted_discharge": 100.0,
            "valid_from": "2026-04-01",
            "valid_to": "2026-09-30",
        }])
        # The Apr-Sep window should still receive the range_delta regardless of season_in_year
        result = db._attach_range_delta(df, {1: 7.5}, "season")
        assert result["range_delta"].iloc[0] == pytest.approx(7.5)
