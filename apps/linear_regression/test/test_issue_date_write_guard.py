"""Contract tests for the exact-issue-date writer guard added to
write_linreg_pentad_forecast_data and write_linreg_decad_forecast_data.

Scope: prevent stale/wrong-day rows from being published under an explicit
forecast_date.  Legacy forecast_date=None behavior is covered in
test_feb20_diagnostic.py and is not re-tested here beyond parity checks.

Synthetic station codes only (no real codes).  All API calls use a recording
fake; no live network or database connections.
"""

import datetime as dt
import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
os.environ.setdefault("SAPPHIRE_TEST_ENV", "True")

import forecast_library as fl

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _pentad_row(
    code,
    date_str,
    forecast=13.0,
    predictor=12.0,
    pentad_in_year=11,
    pentad_in_month=5,
    issue_date=True,
):
    """Build a single-row pentad DataFrame suitable for the writer."""
    row = {
        "code": code,
        "date": pd.to_datetime(date_str),
        "discharge": 50.0,
        "discharge_avg": 11.0,
        "issue_date": issue_date,
        "pentad_in_year": pentad_in_year,
        "pentad_in_month": pentad_in_month,
        "q_mean": 14.0,
        "q_std_sigma": 1.5,
        "delta": 1.0,
        "slope": 0.5,
        "intercept": 5.0,
    }
    if forecast is _ABSENT:
        pass
    else:
        row["forecasted_discharge"] = forecast
    if predictor is _ABSENT:
        pass
    else:
        row["predictor"] = predictor
    return pd.DataFrame([row])


def _decad_row(
    code,
    date_str,
    forecast=13.0,
    predictor=12.0,
    decad_in_year=6,
    decad_in_month=3,
    issue_date=True,
):
    """Build a single-row decad DataFrame suitable for the writer."""
    row = {
        "code": code,
        "date": pd.to_datetime(date_str),
        "discharge": 50.0,
        "discharge_avg": 11.0,
        "issue_date": issue_date,
        "decad_in_year": decad_in_year,
        "decad_in_month": decad_in_month,
        "q_mean": 14.0,
        "q_std_sigma": 1.5,
        "delta": 1.0,
        "slope": 0.5,
        "intercept": 5.0,
    }
    if forecast is _ABSENT:
        pass
    else:
        row["forecasted_discharge"] = forecast
    if predictor is _ABSENT:
        pass
    else:
        row["predictor"] = predictor
    return pd.DataFrame([row])


class _ABSENT:
    """Sentinel: column should be absent from the DataFrame."""


def _make_pentad_df(*rows):
    return pd.concat(list(rows), ignore_index=True)


def _make_decad_df(*rows):
    return pd.concat(list(rows), ignore_index=True)


# ---------------------------------------------------------------------------
# Recording fake API client
# ---------------------------------------------------------------------------


class _RecordingClient:
    """Fake sapphire postprocessing client that records write calls."""

    def __init__(self):
        self.calls = []  # list of record-lists passed to write_lr_forecasts
        # Simulated stored values: key=(code, date, horizon_type) -> record dict
        self._store = {}

    def readiness_check(self):
        return True

    def write_lr_forecasts(self, records):
        self.calls.append(list(records))
        for r in records:
            key = (r.get("code"), r.get("date"), r.get("horizon_type"))
            self._store[key] = r
        return len(records)

    def get_stored(self, code, date_str, horizon_type):
        return self._store.get((code, date_str, horizon_type))


# ---------------------------------------------------------------------------
# Environment fixture — uses monkeypatch to avoid os.environ leaks
# ---------------------------------------------------------------------------


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Set up output-path env vars pointing at a temp directory."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_analysis_pentad_file", "pentad.csv")
    monkeypatch.setenv("ieasyforecast_analysis_decad_file", "decad.csv")
    yield tmp_path


def _pentad_path(env):
    return os.path.join(str(env), "pentad.csv")


def _pentad_latest_path(env):
    return os.path.join(str(env), "pentad_latest.csv")


def _decad_path(env):
    return os.path.join(str(env), "decad.csv")


def _decad_latest_path(env):
    return os.path.join(str(env), "decad_latest.csv")


# ---------------------------------------------------------------------------
# Helper: call writer with mocked API
# ---------------------------------------------------------------------------


def _call_pentad(data, forecast_date, api_data=None, recording_client=None):
    """Call write_linreg_pentad_forecast_data with a fake API layer."""
    if recording_client is None:
        recording_client = _RecordingClient()

    def _fake_get_client():
        return recording_client

    with patch.object(fl, "_get_postprocessing_client", side_effect=_fake_get_client):
        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            with patch.object(fl, "SAPPHIRE_API_AVAILABLE", True):
                return fl.write_linreg_pentad_forecast_data(
                    data, api_data=api_data, forecast_date=forecast_date
                ), recording_client


def _call_decad(data, forecast_date, api_data=None, recording_client=None):
    """Call write_linreg_decad_forecast_data with a fake API layer."""
    if recording_client is None:
        recording_client = _RecordingClient()

    def _fake_get_client():
        return recording_client

    with patch.object(fl, "_get_postprocessing_client", side_effect=_fake_get_client):
        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            with patch.object(fl, "SAPPHIRE_API_AVAILABLE", True):
                return fl.write_linreg_decad_forecast_data(
                    data, api_data=api_data, forecast_date=forecast_date
                ), recording_client


# ===========================================================================
# 1. Mixed current / stale stations
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestMixedCurrentStale:
    """Station A has the exact issue date; station B has last year's date."""

    FD = dt.date(2026, 2, 20)
    CURRENT_DATE = "2026-02-20"
    STALE_DATE = "2025-02-20"

    def _build(self, horizon):
        if horizon == "pentad":
            a = _pentad_row("19901", self.CURRENT_DATE, predictor=12.0, forecast=13.0)
            b = _pentad_row("19902", self.STALE_DATE, predictor=9.0, forecast=9.5)
            return _make_pentad_df(a, b)
        else:
            a = _decad_row("19901", self.CURRENT_DATE, predictor=12.0, forecast=13.0)
            b = _decad_row("19902", self.STALE_DATE, predictor=9.0, forecast=9.5)
            return _make_decad_df(a, b)

    def test_api_serialized_contains_current_only(self, env, horizon):
        """API payload must contain only station A (current) with its original predictor."""
        data = self._build(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(data, self.FD)
        else:
            ret, client = _call_decad(data, self.FD)

        assert ret is True, "expected True (successful write)"
        assert len(client.calls) == 1
        written_codes = {r["code"] for r in client.calls[0]}
        assert written_codes == {"19901"}, f"only current station expected, got {written_codes}"
        written_pred = client.calls[0][0]["predictor"]
        assert written_pred == pytest.approx(12.0, abs=0.01), (
            f"predictor should be original 12.0, got {written_pred}"
        )

    def test_csv_contains_current_station_only(self, env, horizon):
        """CSV must not contain a stale B/current-date row."""
        data = self._build(horizon)
        if horizon == "pentad":
            _call_pentad(data, self.FD)
            csv_path = _pentad_path(env)
        else:
            _call_decad(data, self.FD)
            csv_path = _decad_path(env)

        result = pd.read_csv(csv_path)
        written_codes = set(result["code"].astype(str))
        assert "19902" not in written_codes, (
            f"stale station 19902 must not appear in CSV, got codes: {written_codes}"
        )
        assert "19901" in written_codes


# ===========================================================================
# 2. All stale / same-year wrong day — sentinel check
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestAllStaleReturnNone:
    """When no rows match the exact issue date, return None without writing."""

    FD = dt.date(2026, 2, 20)

    def _build_stale(self, horizon):
        if horizon == "pentad":
            return _make_pentad_df(
                _pentad_row("19901", "2025-02-20"),
                _pentad_row("19902", "2025-02-20"),
            )
        else:
            return _make_decad_df(
                _decad_row("19901", "2025-02-20"),
                _decad_row("19902", "2025-02-20"),
            )

    def _build_wrong_day(self, horizon):
        # Same year but different calendar day
        if horizon == "pentad":
            return _make_pentad_df(
                _pentad_row("19901", "2026-02-19"),
                _pentad_row("19902", "2026-02-21"),
            )
        else:
            return _make_decad_df(
                _decad_row("19901", "2026-02-19"),
                _decad_row("19902", "2026-02-21"),
            )

    def test_all_stale_returns_none(self, env, horizon):
        sentinel = os.path.join(str(env), "sentinel.txt")
        with open(sentinel, "w") as f:
            f.write("untouched")

        data = self._build_stale(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(data, self.FD)
            csv_path = _pentad_path(env)
            latest_path = _pentad_latest_path(env)
        else:
            ret, client = _call_decad(data, self.FD)
            csv_path = _decad_path(env)
            latest_path = _decad_latest_path(env)

        assert ret is None, f"expected None for all-stale input, got {ret}"
        assert not os.path.exists(csv_path), "main CSV must not be created"
        assert not os.path.exists(latest_path), "_latest CSV must not be created"
        assert len(client.calls) == 0, "API must not be called"
        with open(sentinel) as f:
            assert f.read() == "untouched"

    def test_wrong_day_same_year_returns_none(self, env, horizon):
        data = self._build_wrong_day(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(data, self.FD)
            csv_path = _pentad_path(env)
        else:
            ret, client = _call_decad(data, self.FD)
            csv_path = _decad_path(env)

        assert ret is None, f"expected None for wrong-day input, got {ret}"
        assert not os.path.exists(csv_path)
        assert len(client.calls) == 0


# ===========================================================================
# 3. Unsorted history — current row selected before tail, not stale last input
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestUnsortedHistoryCurrentSelected:
    """Input order: [current row, historical row]. Current must be selected even though it
    is NOT last in input order — proving that date filtering precedes tail."""

    FD = dt.date(2026, 2, 20)

    def _build(self, horizon):
        if horizon == "pentad":
            historical = _pentad_row("19901", "2025-02-20", predictor=99.0, forecast=99.0)
            current = _pentad_row("19901", "2026-02-20", predictor=12.0, forecast=13.0)
            # current first — a tail-before-filter implementation would select historical
            return _make_pentad_df(current, historical)
        else:
            historical = _decad_row("19901", "2025-02-20", predictor=99.0, forecast=99.0)
            current = _decad_row("19901", "2026-02-20", predictor=12.0, forecast=13.0)
            # current first — a tail-before-filter implementation would select historical
            return _make_decad_df(current, historical)

    def test_current_row_selected(self, env, horizon):
        data = self._build(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(data, self.FD)
        else:
            ret, client = _call_decad(data, self.FD)

        assert ret is True
        assert len(client.calls) == 1
        r = client.calls[0][0]
        assert r["predictor"] == pytest.approx(12.0, abs=0.01), (
            f"current predictor 12.0 expected, got {r['predictor']}"
        )
        assert r["forecasted_discharge"] == pytest.approx(13.0, abs=0.01)


# ===========================================================================
# 4. Numeric / NULL eligibility
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestNumericNullEligibility:
    """
    Eligibility rules:
    - NULL forecast + NULL predictor  → eligible
    - NULL forecast + finite predictor → eligible
    - finite forecast + finite predictor → eligible
    - finite forecast + NULL predictor → ineligible
    - inf forecast → ineligible
    - bool forecast → ineligible
    - numpy.bool_ forecast → ineligible
    - numeric string forecast → ineligible
    - inf predictor (with finite forecast) → ineligible
    - finite zero forecast + finite predictor → eligible
    - absent forecasted_discharge col → treated as NULL → eligible
    - absent predictor col + NULL forecast → eligible
    - absent predictor col + finite forecast → ineligible
    """

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"

    def _call(self, env, horizon, data):
        if horizon == "pentad":
            return _call_pentad(data, self.FD)
        else:
            return _call_decad(data, self.FD)

    def _row(self, horizon, code, forecast, predictor):
        if horizon == "pentad":
            return _pentad_row(code, self.DATE, forecast=forecast, predictor=predictor)
        else:
            return _decad_row(code, self.DATE, forecast=forecast, predictor=predictor)

    def test_null_forecast_null_predictor_eligible(self, env, horizon):
        """NULL forecast + NULL predictor: row written as API NULL."""
        data = self._row(horizon, "19901", forecast=float("nan"), predictor=float("nan"))
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1
        r = client.calls[0][0]
        assert r["forecasted_discharge"] is None, (
            f"NaN forecast should serialize as None, got {r['forecasted_discharge']}"
        )
        assert r["predictor"] is None

    def test_null_forecast_finite_predictor_eligible(self, env, horizon):
        """NULL forecast + finite predictor: row written."""
        data = self._row(horizon, "19901", forecast=float("nan"), predictor=12.0)
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1

    def test_finite_forecast_finite_predictor_eligible(self, env, horizon):
        """Standard finite forecast + predictor: row written."""
        data = self._row(horizon, "19901", forecast=13.0, predictor=12.0)
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1

    def test_finite_forecast_null_predictor_ineligible(self, env, horizon):
        """Finite forecast + NULL predictor: row rejected (no write)."""
        data = self._row(horizon, "19901", forecast=13.0, predictor=float("nan"))
        ret, client = self._call(env, horizon, data)
        assert ret is None, f"expected None (all rows ineligible), got {ret}"
        assert len(client.calls) == 0

    def test_inf_forecast_ineligible(self, env, horizon):
        """Infinity in forecast: row rejected."""
        data = self._row(horizon, "19901", forecast=float("inf"), predictor=12.0)
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_neg_inf_forecast_ineligible(self, env, horizon):
        """Negative infinity in forecast: row rejected."""
        data = self._row(horizon, "19901", forecast=float("-inf"), predictor=12.0)
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_inf_predictor_with_finite_forecast_ineligible(self, env, horizon):
        """Infinity in predictor with finite forecast: row rejected."""
        data = self._row(horizon, "19901", forecast=13.0, predictor=float("inf"))
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_zero_forecast_eligible(self, env, horizon):
        """Finite zero is a valid forecast value."""
        data = self._row(horizon, "19901", forecast=0.0, predictor=12.0)
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1
        assert client.calls[0][0]["forecasted_discharge"] == pytest.approx(0.0)

    def test_bool_forecast_ineligible(self, env, horizon):
        """Python bool is not a supported numeric type for forecast."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE)
        else:
            data = _decad_row("19901", self.DATE)
        data = data.copy()
        data["forecasted_discharge"] = True
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_numpy_bool_forecast_ineligible(self, env, horizon):
        """numpy.bool_ in forecast field: row rejected."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE)
        else:
            data = _decad_row("19901", self.DATE)
        data = data.copy()
        data["forecasted_discharge"] = np.bool_(True)
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_numpy_bool_predictor_ineligible(self, env, horizon):
        """numpy.bool_ in predictor with finite forecast: row rejected."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=13.0)
        else:
            data = _decad_row("19901", self.DATE, forecast=13.0)
        data = data.copy()
        data["predictor"] = np.bool_(True)
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_numeric_string_forecast_ineligible(self, env, horizon):
        """Numeric string in forecast field: row rejected (no string-to-number coercion)."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE)
        else:
            data = _decad_row("19901", self.DATE)
        data = data.copy()
        data["forecasted_discharge"] = "12.5"
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_numeric_string_predictor_ineligible(self, env, horizon):
        """Numeric string in predictor with finite forecast: row rejected."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=13.0)
        else:
            data = _decad_row("19901", self.DATE, forecast=13.0)
        data = data.copy()
        data["predictor"] = "12.5"
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_absent_forecasted_discharge_column_eligible(self, env, horizon):
        """Absent forecasted_discharge column is treated as NULL forecast."""
        data = self._row(horizon, "19901", forecast=_ABSENT, predictor=12.0)
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1
        assert client.calls[0][0]["forecasted_discharge"] is None

    def test_absent_predictor_null_forecast_eligible(self, env, horizon):
        """Absent predictor column + NULL forecast: eligible."""
        data = self._row(horizon, "19901", forecast=float("nan"), predictor=_ABSENT)
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1

    def test_absent_predictor_finite_forecast_ineligible(self, env, horizon):
        """Absent predictor column + finite forecast: ineligible."""
        data = self._row(horizon, "19901", forecast=13.0, predictor=_ABSENT)
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_complex64_nonzero_imaginary_forecast_ineligible(self, env, horizon):
        """np.complex64 with nonzero imaginary part in forecast: row rejected."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=12.0, predictor=12.0)
        else:
            data = _decad_row("19901", self.DATE, forecast=12.0, predictor=12.0)
        data = data.copy()
        data["forecasted_discharge"] = np.complex64(3.0 + 1j)
        ret, client = self._call(env, horizon, data)
        assert ret is None, f"complex64 nonzero imaginary forecast must be rejected, got {ret}"
        assert len(client.calls) == 0

    def test_complex64_nonzero_imaginary_predictor_ineligible(self, env, horizon):
        """np.complex64 with nonzero imaginary part in predictor with finite forecast: row rejected."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=13.0, predictor=12.0)
        else:
            data = _decad_row("19901", self.DATE, forecast=13.0, predictor=12.0)
        data = data.copy()
        data["predictor"] = np.complex64(3.0 + 1j)
        ret, client = self._call(env, horizon, data)
        assert ret is None, f"complex64 nonzero imaginary predictor must be rejected, got {ret}"
        assert len(client.calls) == 0

    def test_complex_nan_forecast_ineligible(self, env, horizon):
        """np.complex64 NaN in forecast: row rejected (not treated as numeric null)."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=12.0, predictor=12.0)
        else:
            data = _decad_row("19901", self.DATE, forecast=12.0, predictor=12.0)
        data = data.copy()
        data["forecasted_discharge"] = np.complex64(float("nan") + 0j)
        ret, client = self._call(env, horizon, data)
        assert ret is None, f"complex NaN forecast must be rejected, got {ret}"
        assert len(client.calls) == 0

    def test_complex_nan_predictor_ineligible(self, env, horizon):
        """np.complex64 NaN in predictor with finite forecast: row rejected."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=13.0, predictor=12.0)
        else:
            data = _decad_row("19901", self.DATE, forecast=13.0, predictor=12.0)
        data = data.copy()
        data["predictor"] = np.complex64(float("nan") + 0j)
        ret, client = self._call(env, horizon, data)
        assert ret is None, f"complex NaN predictor must be rejected, got {ret}"
        assert len(client.calls) == 0


# ===========================================================================
# 5. All-NULL batch writes and NULL upsert clears stored number
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestAllNullBatch:
    """All-NULL forecasts must write successfully (not become an all-stale None)."""

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"

    def _build(self, horizon):
        if horizon == "pentad":
            a = _pentad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
            b = _pentad_row("19902", self.DATE, forecast=float("nan"), predictor=float("nan"))
            return _make_pentad_df(a, b)
        else:
            a = _decad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
            b = _decad_row("19902", self.DATE, forecast=float("nan"), predictor=float("nan"))
            return _make_decad_df(a, b)

    def test_all_null_writes_true(self, env, horizon):
        data = self._build(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(data, self.FD)
            csv_path = _pentad_path(env)
        else:
            ret, client = _call_decad(data, self.FD)
            csv_path = _decad_path(env)

        assert ret is True, f"all-NULL batch should return True, got {ret}"
        assert len(client.calls) == 1, "API must be called once"
        assert os.path.exists(csv_path)

    def test_null_update_clears_stored_number(self, env, horizon):
        """A NULL write on an existing key overwrites the prior numeric value."""
        client = _RecordingClient()
        ht = "pentad" if horizon == "pentad" else "decade"
        client._store[("19901", "2026-02-20", ht)] = {
            "code": "19901",
            "date": "2026-02-20",
            "horizon_type": ht,
            "forecasted_discharge": 99.0,
            "predictor": 88.0,
        }

        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
            ret, _ = _call_pentad(data, self.FD, recording_client=client)
        else:
            data = _decad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
            ret, _ = _call_decad(data, self.FD, recording_client=client)

        assert ret is True
        stored = client.get_stored("19901", "2026-02-20", ht)
        assert stored is not None
        assert stored["forecasted_discharge"] is None, (
            f"NULL update must overwrite 99.0, got {stored['forecasted_discharge']}"
        )
        assert stored["predictor"] is None


# ===========================================================================
# 6. Current row duplicates: valid/invalid ordering
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestCurrentDuplicateOrdering:
    """Multiple rows with the same code and exact issue date."""

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"

    def test_valid_then_valid_keeps_last(self, env, horizon):
        """Two valid rows for same code: keep-last selects second row."""
        if horizon == "pentad":
            r1 = _pentad_row("19901", self.DATE, predictor=10.0, forecast=11.0)
            r2 = _pentad_row("19901", self.DATE, predictor=12.0, forecast=13.0)
            data = _make_pentad_df(r1, r2)
            ret, client = _call_pentad(data, self.FD)
        else:
            r1 = _decad_row("19901", self.DATE, predictor=10.0, forecast=11.0)
            r2 = _decad_row("19901", self.DATE, predictor=12.0, forecast=13.0)
            data = _make_decad_df(r1, r2)
            ret, client = _call_decad(data, self.FD)

        assert ret is True
        assert len(client.calls[0]) == 1
        assert client.calls[0][0]["predictor"] == pytest.approx(12.0, abs=0.01)

    def test_valid_then_invalid_does_not_fallback(self, env, horizon):
        """Valid row followed by invalid row: invalid selected, no fallback to earlier valid."""
        if horizon == "pentad":
            r1 = _pentad_row("19901", self.DATE, predictor=10.0, forecast=11.0)
            r2 = _pentad_row("19901", self.DATE, predictor=float("nan"), forecast=99.0)
            data = _make_pentad_df(r1, r2)
            ret, client = _call_pentad(data, self.FD)
        else:
            r1 = _decad_row("19901", self.DATE, predictor=10.0, forecast=11.0)
            r2 = _decad_row("19901", self.DATE, predictor=float("nan"), forecast=99.0)
            data = _make_decad_df(r1, r2)
            ret, client = _call_decad(data, self.FD)

        assert ret is None, "latest duplicate is invalid → no eligible rows → None"
        assert len(client.calls) == 0

    def test_invalid_then_valid_keeps_last(self, env, horizon):
        """Invalid row followed by valid row: valid row (last) is used."""
        if horizon == "pentad":
            r1 = _pentad_row("19901", self.DATE, predictor=float("nan"), forecast=99.0)
            r2 = _pentad_row("19901", self.DATE, predictor=12.0, forecast=13.0)
            data = _make_pentad_df(r1, r2)
            ret, client = _call_pentad(data, self.FD)
        else:
            r1 = _decad_row("19901", self.DATE, predictor=float("nan"), forecast=99.0)
            r2 = _decad_row("19901", self.DATE, predictor=12.0, forecast=13.0)
            data = _make_decad_df(r1, r2)
            ret, client = _call_decad(data, self.FD)

        assert ret is True
        assert client.calls[0][0]["predictor"] == pytest.approx(12.0, abs=0.01)

    def test_null_forecast_latest_stays_eligible(self, env, horizon):
        """Latest duplicate has NULL forecast: must remain eligible (not resurrect earlier finite)."""
        if horizon == "pentad":
            r1 = _pentad_row("19901", self.DATE, predictor=10.0, forecast=11.0)
            r2 = _pentad_row("19901", self.DATE, predictor=float("nan"), forecast=float("nan"))
            data = _make_pentad_df(r1, r2)
            ret, client = _call_pentad(data, self.FD)
        else:
            r1 = _decad_row("19901", self.DATE, predictor=10.0, forecast=11.0)
            r2 = _decad_row("19901", self.DATE, predictor=float("nan"), forecast=float("nan"))
            data = _make_decad_df(r1, r2)
            ret, client = _call_decad(data, self.FD)

        assert ret is True, "latest NULL forecast is eligible"
        r = client.calls[0][0]
        assert r["forecasted_discharge"] is None, "must write NULL, not resurrect earlier 11.0"
        assert r["predictor"] is None


# ===========================================================================
# 7. Row date representation validation
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestRowDateValidation:
    """Row date values are validated strictly; timezone-aware and non-midnight rejected."""

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"

    def _base_row(self, horizon):
        if horizon == "pentad":
            return _pentad_row("19901", self.DATE)
        else:
            return _decad_row("19901", self.DATE)

    def _call(self, env, horizon, data):
        if horizon == "pentad":
            return _call_pentad(data, self.FD)
        else:
            return _call_decad(data, self.FD)

    def _set_date(self, data, val):
        d = data.copy()
        d["date"] = pd.Series([val], dtype=object, index=d.index)
        return d

    def test_python_date_valid(self, env, horizon):
        """Python date object as row date: valid and accepted."""
        data = self._set_date(self._base_row(horizon), dt.date(2026, 2, 20))
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1

    def test_midnight_naive_timestamp_valid(self, env, horizon):
        """Midnight naive pd.Timestamp as row date: valid."""
        data = self._set_date(self._base_row(horizon), pd.Timestamp("2026-02-20 00:00:00"))
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1

    def test_canonical_string_date_valid(self, env, horizon):
        """Strict YYYY-MM-DD string as row date: valid."""
        data = self._set_date(self._base_row(horizon), "2026-02-20")
        ret, client = self._call(env, horizon, data)
        assert ret is True
        assert len(client.calls) == 1

    def test_tz_aware_timestamp_rejected(self, env, horizon):
        """Timezone-aware timestamp row date: row rejected."""
        data = self._set_date(
            self._base_row(horizon), pd.Timestamp("2026-02-20 00:00:00", tz="UTC")
        )
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_nonmidnight_timestamp_rejected(self, env, horizon):
        """Non-midnight row date (same calendar day): row rejected."""
        data = self._set_date(self._base_row(horizon), pd.Timestamp("2026-02-20 12:00:00"))
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_microsecond_timestamp_rejected(self, env, horizon):
        """Row date with non-zero microsecond: row rejected."""
        data = self._set_date(self._base_row(horizon), pd.Timestamp("2026-02-20 00:00:00.000001"))
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_nanosecond_timestamp_rejected(self, env, horizon):
        """Row date with non-zero nanosecond: row rejected."""
        ts_ns = pd.Timestamp("2026-02-20 00:00:00") + pd.Timedelta(nanoseconds=1)
        data = self._set_date(self._base_row(horizon), ts_ns)
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_noncanonical_string_date_rejected(self, env, horizon):
        """Non-canonical date string: row rejected (no fuzzy parsing)."""
        data = self._set_date(self._base_row(horizon), "02/20/2026")
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_nat_row_rejected(self, env, horizon):
        """NaT row date: row rejected."""
        data = self._set_date(self._base_row(horizon), pd.NaT)
        ret, client = self._call(env, horizon, data)
        assert ret is None
        assert len(client.calls) == 0

    def test_mixed_valid_invalid_rows_only_valid_written(self, env, horizon):
        """Station A valid midnight date; B tz-aware date. Only A reaches API."""
        if horizon == "pentad":
            a = _pentad_row("19901", self.DATE)
            b = _pentad_row("19902", self.DATE)
            data = _make_pentad_df(a, b)
        else:
            a = _decad_row("19901", self.DATE)
            b = _decad_row("19902", self.DATE)
            data = _make_decad_df(a, b)

        date_vals = list(data["date"])
        date_vals[1] = pd.Timestamp("2026-02-20 00:00:00", tz="UTC")
        data = data.copy()
        data["date"] = pd.Series(date_vals, dtype=object, index=data.index)

        ret, client = self._call(env, horizon, data)
        assert ret is True
        written_codes = {r["code"] for r in client.calls[0]}
        assert written_codes == {"19901"}, f"only valid-date station expected, got {written_codes}"


# ===========================================================================
# 8. API override (custom api_data) asymmetric cases
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestOverrideFiltering:
    """nonempty api_data with forecast_date: date + eligibility applied."""

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"
    STALE = "2025-02-20"

    def _main_current(self, horizon):
        if horizon == "pentad":
            return _pentad_row("19901", self.DATE)
        else:
            return _decad_row("19901", self.DATE)

    def _override_stale(self, horizon):
        """Override row with stale date: ineligible."""
        if horizon == "pentad":
            return _pentad_row("19902", self.STALE, predictor=9.0, forecast=9.5)
        else:
            return _decad_row("19902", self.STALE, predictor=9.0, forecast=9.5)

    def _override_current(self, horizon):
        """Override row with exact issue date: eligible."""
        if horizon == "pentad":
            return _pentad_row("19902", self.DATE, predictor=9.0, forecast=9.5)
        else:
            return _decad_row("19902", self.DATE, predictor=9.0, forecast=9.5)

    def test_stale_override_no_api_call_but_csv_written(self, env, horizon):
        """Main eligible + all-stale override: no API call, CSV written, return True."""
        main = self._main_current(horizon)
        override = self._override_stale(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(main, self.FD, api_data=override)
            csv_path = _pentad_path(env)
        else:
            ret, client = _call_decad(main, self.FD, api_data=override)
            csv_path = _decad_path(env)

        assert ret is True, f"eligible main + rejected override → True, got {ret}"
        assert len(client.calls) == 0, "API must not be called when override is all-rejected"
        assert os.path.exists(csv_path), "CSV must still be written"

    def test_current_override_api_called(self, env, horizon):
        """Main eligible + current override: API called with override rows."""
        main = self._main_current(horizon)
        override = self._override_current(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(main, self.FD, api_data=override)
        else:
            ret, client = _call_decad(main, self.FD, api_data=override)

        assert ret is True
        assert len(client.calls) == 1
        written_codes = {r["code"] for r in client.calls[0]}
        assert "19902" in written_codes

    def test_override_independent_station_allowed(self, env, horizon):
        """Override station not in main is allowed."""
        main = self._main_current(horizon)
        if horizon == "pentad":
            override = _pentad_row("19903", self.DATE, predictor=7.0, forecast=7.5)
            ret, client = _call_pentad(main, self.FD, api_data=override)
        else:
            override = _decad_row("19903", self.DATE, predictor=7.0, forecast=7.5)
            ret, client = _call_decad(main, self.FD, api_data=override)

        assert ret is True
        assert len(client.calls) == 1
        written_codes = {r["code"] for r in client.calls[0]}
        assert "19903" in written_codes

    def test_override_duplicate_rows_preserved(self, env, horizon):
        """Duplicate rows in override are preserved (no dedup applied)."""
        main = self._main_current(horizon)
        if horizon == "pentad":
            row = _pentad_row("19902", self.DATE, predictor=9.0, forecast=9.5)
            override = _make_pentad_df(row, row.copy())
            ret, client = _call_pentad(main, self.FD, api_data=override)
        else:
            row = _decad_row("19902", self.DATE, predictor=9.0, forecast=9.5)
            override = _make_decad_df(row, row.copy())
            ret, client = _call_decad(main, self.FD, api_data=override)

        assert ret is True
        assert len(client.calls[0]) == 2, (
            f"both duplicate override rows must be passed, got {len(client.calls[0])}"
        )

    def test_override_precision_unrounded(self, env, horizon):
        """Override values are passed as-is (no rounding applied)."""
        main = self._main_current(horizon)
        precise_pred = 9.123456789
        precise_fc = 10.987654321
        if horizon == "pentad":
            override = _pentad_row("19902", self.DATE, predictor=precise_pred, forecast=precise_fc)
            ret, client = _call_pentad(main, self.FD, api_data=override)
        else:
            override = _decad_row("19902", self.DATE, predictor=precise_pred, forecast=precise_fc)
            ret, client = _call_decad(main, self.FD, api_data=override)

        assert ret is True
        r = client.calls[0][0]
        assert r["predictor"] == pytest.approx(precise_pred, rel=1e-6)
        assert r["forecasted_discharge"] == pytest.approx(precise_fc, rel=1e-6)

    def test_empty_override_skips_api(self, env, horizon):
        """Explicit empty DataFrame override disables API write."""
        main = self._main_current(horizon)
        if horizon == "pentad":
            ret, client = _call_pentad(main, self.FD, api_data=pd.DataFrame())
            csv_path = _pentad_path(env)
        else:
            ret, client = _call_decad(main, self.FD, api_data=pd.DataFrame())
            csv_path = _decad_path(env)

        assert ret is True
        assert len(client.calls) == 0, "empty override must skip API"
        assert os.path.exists(csv_path), "CSV must still be written"

    def test_no_eligible_main_returns_none_even_with_valid_override(self, env, horizon):
        """No eligible main rows: return None even when override is valid."""
        if horizon == "pentad":
            main = _pentad_row("19901", self.STALE)
            override = _pentad_row("19902", self.DATE, predictor=9.0, forecast=9.5)
            ret, client = _call_pentad(main, self.FD, api_data=override)
            csv_path = _pentad_path(env)
        else:
            main = _decad_row("19901", self.STALE)
            override = _decad_row("19902", self.DATE, predictor=9.0, forecast=9.5)
            ret, client = _call_decad(main, self.FD, api_data=override)
            csv_path = _decad_path(env)

        assert ret is None, "no eligible main rows → None before any writes"
        assert len(client.calls) == 0
        assert not os.path.exists(csv_path)

    def test_malformed_override_missing_date_column(self, env, horizon):
        """Override missing 'date' column: ValueError caught by API error policy."""
        main = self._main_current(horizon)
        # Override has no date column — structurally malformed
        override_no_date = pd.DataFrame(
            {
                "code": ["19902"],
                "predictor": [9.0],
                "forecasted_discharge": [9.5],
                "issue_date": [True],
            }
        )
        if horizon == "pentad":
            override_no_date["pentad_in_year"] = 11
            override_no_date["pentad_in_month"] = 5
        else:
            override_no_date["decad_in_year"] = 6
            override_no_date["decad_in_month"] = 3

        # In "warn" mode the error policy logs and sets api_ok=False without re-raising
        with patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "warn"}):
            if horizon == "pentad":
                ret, client = _call_pentad(main, self.FD, api_data=override_no_date)
                csv_path = _pentad_path(env)
            else:
                ret, client = _call_decad(main, self.FD, api_data=override_no_date)
                csv_path = _decad_path(env)

        assert ret is False, "malformed override raises → caught by error policy → api_ok=False"
        assert len(client.calls) == 0, "API write must not have been called"
        assert os.path.exists(csv_path), "CSV still written from eligible main rows"


# ===========================================================================
# 9. API serialization boundary — date and predictor origin verified
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestAPISerializationBoundary:
    """Verify exact serialized values reaching the API client."""

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"

    def test_serialized_date_equals_forecast_date(self, env, horizon):
        """The date field in the API record must equal the exact forecast date."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, predictor=12.345, forecast=13.456)
            ret, client = _call_pentad(data, self.FD)
        else:
            data = _decad_row("19901", self.DATE, predictor=12.345, forecast=13.456)
            ret, client = _call_decad(data, self.FD)

        assert ret is True
        r = client.calls[0][0]
        assert r["date"] == "2026-02-20", f"expected date '2026-02-20', got {r['date']!r}"

    def test_serialized_predictor_from_source_row(self, env, horizon):
        """The predictor in the API record must come from the source row unchanged."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, predictor=12.345, forecast=13.456)
            ret, client = _call_pentad(data, self.FD)
        else:
            data = _decad_row("19901", self.DATE, predictor=12.345, forecast=13.456)
            ret, client = _call_decad(data, self.FD)

        assert ret is True
        r = client.calls[0][0]
        # Main frame is rounded to 3 dp, so 12.345 stays 12.345
        assert r["predictor"] == pytest.approx(12.345, rel=1e-3)

    def test_null_predictor_serialized_as_none(self, env, horizon):
        """NaN predictor in a NULL-forecast row must serialize as None."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
            ret, client = _call_pentad(data, self.FD)
        else:
            data = _decad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
            ret, client = _call_decad(data, self.FD)

        assert ret is True
        r = client.calls[0][0]
        assert r["predictor"] is None, f"NaN predictor must be None, got {r['predictor']}"
        assert r["forecasted_discharge"] is None


# ===========================================================================
# 10. API error handling — exception vs False return parity
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestAPIErrorHandling:
    """Raised exceptions set api_ok=False; a False return value is ignored."""

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"

    def _data(self, horizon):
        if horizon == "pentad":
            return _pentad_row("19901", self.DATE)
        else:
            return _decad_row("19901", self.DATE)

    def test_api_exception_returns_false(self, env, horizon):
        """Exception from _write_lr_forecast_to_api → api_ok=False → return False."""
        data = self._data(horizon)
        with patch.object(fl, "_write_lr_forecast_to_api", side_effect=RuntimeError("sim fail")):
            with patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "warn"}):
                if horizon == "pentad":
                    ret = fl.write_linreg_pentad_forecast_data(data, forecast_date=self.FD)
                else:
                    ret = fl.write_linreg_decad_forecast_data(data, forecast_date=self.FD)

        assert ret is False, f"raised exception must set api_ok=False → return False, got {ret}"

    def test_helper_false_return_treated_as_success(self, env, horizon):
        """_write_lr_forecast_to_api returning False does not set api_ok=False."""
        data = self._data(horizon)
        with patch.object(fl, "_write_lr_forecast_to_api", return_value=False):
            with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
                if horizon == "pentad":
                    ret = fl.write_linreg_pentad_forecast_data(data, forecast_date=self.FD)
                else:
                    ret = fl.write_linreg_decad_forecast_data(data, forecast_date=self.FD)

        assert ret is True, "False return from helper must not change api_ok; expected True"


# ===========================================================================
# 11. Legacy forecast_date=None parity
# ===========================================================================


class TestLegacyParity:
    """forecast_date=None behavior is unchanged."""

    @patch.object(fl, "_write_lr_forecast_to_api")
    def test_legacy_pentad_mixed_year_writes(self, mock_api, tmp_path, monkeypatch):
        """Without forecast_date, genuinely mixed year still writes (legacy behavior)."""
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
        monkeypatch.setenv("ieasyforecast_analysis_pentad_file", "pentad.csv")

        data = pd.concat(
            [
                _pentad_row("19901", "2025-02-20"),
                _pentad_row("19902", "2024-01-10"),  # genuinely different year
            ],
            ignore_index=True,
        )
        fl.write_linreg_pentad_forecast_data(data)

        assert os.path.exists(os.path.join(str(tmp_path), "pentad.csv"))
        mock_api.assert_called_once()

    @patch.object(fl, "_write_lr_forecast_to_api")
    def test_legacy_decad_mixed_year_writes(self, mock_api, tmp_path, monkeypatch):
        """Without forecast_date, decad genuinely mixed year still writes."""
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
        monkeypatch.setenv("ieasyforecast_analysis_decad_file", "decad.csv")

        data = pd.concat(
            [
                _decad_row("19901", "2025-02-20"),
                _decad_row("19902", "2024-01-10"),  # genuinely different year
            ],
            ignore_index=True,
        )
        fl.write_linreg_decad_forecast_data(data)

        assert os.path.exists(os.path.join(str(tmp_path), "decad.csv"))
        mock_api.assert_called_once()


# ===========================================================================
# 12. Legacy consistency check runs after warned exception
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestLegacyConsistencyAfterWarn:
    """Legacy path (forecast_date=None): consistency check runs even when the API
    write raises and is caught in warn mode."""

    def test_consistency_check_runs_after_warned_exception(self, env, horizon, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        monkeypatch.setenv("SAPPHIRE_API_FAILURE_MODE", "warn")

        if horizon == "pentad":
            monkeypatch.setenv("ieasyforecast_analysis_pentad_file", "pentad.csv")
            data = _pentad_row("19901", "2026-02-20")
        else:
            monkeypatch.setenv("ieasyforecast_analysis_decad_file", "decad.csv")
            data = _decad_row("19901", "2026-02-20")

        with patch.object(
            fl, "_write_lr_forecast_to_api", side_effect=RuntimeError("simulated failure")
        ):
            with patch.object(
                fl, "_verify_write_consistency", return_value=(True, "ok")
            ) as mock_check:
                if horizon == "pentad":
                    fl.write_linreg_pentad_forecast_data(data)
                else:
                    fl.write_linreg_decad_forecast_data(data)

        (
            mock_check.assert_called_once(),
            ("consistency check must run even when API write raised a warned exception"),
        )


# ===========================================================================
# 13. Two-write lifecycle: NULL second write clears prior numeric value in CSV
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestTwoWriteLifecycle:
    """First write uses a Timestamp date + numeric values. Second write uses a
    canonical object-string date + NULL values for the same station/date key.
    Both CSV files must hold exactly one row for the station after the second
    write, and that row must be NULL (not the prior numeric value)."""

    FD = dt.date(2026, 2, 20)

    def _first_data(self, horizon):
        """Row with pd.Timestamp date and finite forecast/predictor."""
        if horizon == "pentad":
            row = _pentad_row("19901", "2026-02-20", forecast=99.0, predictor=88.0)
        else:
            row = _decad_row("19901", "2026-02-20", forecast=99.0, predictor=88.0)
        row = row.copy()
        row["date"] = pd.Series(
            [pd.Timestamp("2026-02-20 00:00:00")], dtype=object, index=row.index
        )
        return row

    def _second_data(self, horizon):
        """Same station/date key with canonical string date and NULL values."""
        if horizon == "pentad":
            row = _pentad_row("19901", "2026-02-20", forecast=float("nan"), predictor=float("nan"))
        else:
            row = _decad_row("19901", "2026-02-20", forecast=float("nan"), predictor=float("nan"))
        row = row.copy()
        row["date"] = pd.Series(["2026-02-20"], dtype=object, index=row.index)
        return row

    def test_null_second_write_clears_csv_number(self, env, horizon):
        """After two writes, both CSV files must have one row with NULL values."""
        client = _RecordingClient()

        # First write: Timestamp date, numeric values
        if horizon == "pentad":
            _call_pentad(self._first_data(horizon), self.FD, recording_client=client)
            csv_path = _pentad_path(env)
            latest_path = _pentad_latest_path(env)
        else:
            _call_decad(self._first_data(horizon), self.FD, recording_client=client)
            csv_path = _decad_path(env)
            latest_path = _decad_latest_path(env)

        df_first = pd.read_csv(csv_path)
        first_fc = df_first.loc[df_first["code"].astype(str) == "19901", "forecasted_discharge"]
        assert float(first_fc.iloc[0]) == pytest.approx(99.0), (
            f"first write must record 99.0, got {first_fc.iloc[0]}"
        )

        # Second write: object string date, NULL values — uses existing-file branch
        if horizon == "pentad":
            ret, _ = _call_pentad(self._second_data(horizon), self.FD, recording_client=client)
        else:
            ret, _ = _call_decad(self._second_data(horizon), self.FD, recording_client=client)

        assert ret is True

        # Main CSV: exactly one row for this station, forecasted_discharge NULL
        df_main = pd.read_csv(csv_path)
        rows = df_main[df_main["code"].astype(str) == "19901"]
        assert len(rows) == 1, f"expected one row for 19901 in main CSV, got {len(rows)}"
        assert pd.isna(rows["forecasted_discharge"].iloc[0]), (
            f"main CSV must show NULL after second write, got {rows['forecasted_discharge'].iloc[0]}"
        )

        # Latest CSV: exactly one row for this station, forecasted_discharge NULL
        df_latest = pd.read_csv(latest_path)
        rows_l = df_latest[df_latest["code"].astype(str) == "19901"]
        assert len(rows_l) == 1, f"expected one row for 19901 in latest CSV, got {len(rows_l)}"
        assert pd.isna(rows_l["forecasted_discharge"].iloc[0]), (
            f"latest CSV must show NULL after second write, "
            f"got {rows_l['forecasted_discharge'].iloc[0]}"
        )

        # API second call delivered NULL
        assert len(client.calls) == 2
        second_call_records = client.calls[1]
        assert len(second_call_records) == 1
        assert second_call_records[0]["forecasted_discharge"] is None


# ===========================================================================
# 14. Container NaN rejection: [NaN] is not a supported scalar NULL
# ===========================================================================


@pytest.mark.parametrize("horizon", ["pentad", "decad"])
class TestContainerNaNRejected:
    """One-element NaN list in a numeric cell must be rejected (not treated as NULL scalar).
    Scalar None, NaN, and pd.NA positives must remain accepted."""

    FD = dt.date(2026, 2, 20)
    DATE = "2026-02-20"

    def _call(self, env, horizon, data):
        if horizon == "pentad":
            return _call_pentad(data, self.FD)
        else:
            return _call_decad(data, self.FD)

    def test_list_nan_forecast_rejected(self, env, horizon):
        """[float('nan')] in forecasted_discharge is not a supported NULL scalar."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        else:
            data = _decad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        data = data.copy()
        data["forecasted_discharge"] = data["forecasted_discharge"].astype(object)
        data.at[data.index[0], "forecasted_discharge"] = [float("nan")]
        assert isinstance(data.at[data.index[0], "forecasted_discharge"], list), (
            "fixture cell must be a list before calling writer"
        )
        ret, client = self._call(env, horizon, data)
        assert ret is None, f"[NaN] container in forecast must be rejected, got {ret}"
        assert len(client.calls) == 0
        if horizon == "pentad":
            assert not os.path.exists(_pentad_path(env))
            assert not os.path.exists(_pentad_latest_path(env))
        else:
            assert not os.path.exists(_decad_path(env))
            assert not os.path.exists(_decad_latest_path(env))

    def test_list_nan_predictor_rejected(self, env, horizon):
        """[float('nan')] in predictor is not a supported NULL scalar."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        else:
            data = _decad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        data = data.copy()
        data["predictor"] = data["predictor"].astype(object)
        data.at[data.index[0], "predictor"] = [float("nan")]
        assert isinstance(data.at[data.index[0], "predictor"], list), (
            "fixture cell must be a list before calling writer"
        )
        ret, client = self._call(env, horizon, data)
        assert ret is None, f"[NaN] container in predictor must be rejected, got {ret}"
        assert len(client.calls) == 0
        if horizon == "pentad":
            assert not os.path.exists(_pentad_path(env))
            assert not os.path.exists(_pentad_latest_path(env))
        else:
            assert not os.path.exists(_decad_path(env))
            assert not os.path.exists(_decad_latest_path(env))

    def test_scalar_none_forecast_still_accepted(self, env, horizon):
        """Scalar None in forecasted_discharge is still a valid NULL."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        else:
            data = _decad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        data = data.copy()
        data["forecasted_discharge"] = data["forecasted_discharge"].astype(object)
        data.at[data.index[0], "forecasted_discharge"] = None
        ret, client = self._call(env, horizon, data)
        assert ret is True, f"scalar None forecast must remain eligible, got {ret}"
        assert len(client.calls) == 1

    def test_scalar_pd_na_forecast_still_accepted(self, env, horizon):
        """pd.NA in forecasted_discharge is still a valid NULL."""
        if horizon == "pentad":
            data = _pentad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        else:
            data = _decad_row("19901", self.DATE, forecast=float("nan"), predictor=float("nan"))
        data = data.copy()
        data["forecasted_discharge"] = data["forecasted_discharge"].astype(object)
        data.at[data.index[0], "forecasted_discharge"] = pd.NA
        ret, client = self._call(env, horizon, data)
        assert ret is True, f"pd.NA forecast must remain eligible, got {ret}"
        assert len(client.calls) == 1
