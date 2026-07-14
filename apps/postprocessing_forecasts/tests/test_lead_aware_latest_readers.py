"""Tests for M1 P5a flag-gated wiring of `select_operational_issuances`

into the OPERATIONAL "latest" long-forecast readers
(`read_latest_monthly_forecasts`, `read_latest_seasonal_forecasts`).

An adversarial review found these operational read paths were NOT made
lead-aware (unlike `read_latest_quarterly_forecasts`, which was), so a
same-target non-operational reissue/backfill row could leak into the
latest operational output. These tests lock:

- flag ON: a same-target non-operational reissue (wrong issue day) is
  EXCLUDED, the configured operational issuance survives, and distinct
  configured leads for the same target survive as separate rows;
- flag ON: a config-resolution failure (mode missing
  operational_issue_day) PROPAGATES (fail loud) rather than silently
  falling back to an unfiltered read;
- flag OFF: byte-identical to the pre-fix behavior (no selection --
  the reissue row is retained).

The `_normalize_monthly_forecasts` NULL-horizon_value coercion (FIX #6)
was intentionally NOT changed; see
`TestNullHorizonValueUnderSelection` for the documented current
behavior and the coupling rationale.
"""

import datetime as dt
import json
import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import data_reader

CODE = "19999"


def _write_mode_config(config_dir, mode, lead, issue_day=None):
    payload = {"operational_month_lead_time": lead}
    if issue_day is not None:
        payload["operational_issue_day"] = issue_day
    (config_dir / f"{mode}.json").write_text(json.dumps(payload))


def _set_long_term_env(monkeypatch, tmp_path, modes):
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", ",".join(modes))


def _month_row(code, year, month, issue_date, model="LR_Base", horizon_value=99, q50=100.0):
    valid_from = f"{year}-{month:02d}-01"
    return {
        "horizon_type": "month",
        "horizon_value": horizon_value,
        "code": code,
        "date": issue_date,
        "model_type": model,
        "valid_from": valid_from,
        "valid_to": valid_from,
        "q50": q50,
        "q05": q50 - 30.0,
        "q10": q50 - 25.0,
        "q25": q50 - 15.0,
        "q75": q50 + 15.0,
        "q90": q50 + 25.0,
        "q95": q50 + 30.0,
        "id": 1,
        "model_type_description": model,
    }


def _season_row(code, valid_from, issue_date, model="LR_Base", horizon_value=0, q50=100.0):
    return {
        "horizon_type": "season",
        "horizon_value": horizon_value,
        "code": code,
        "date": issue_date,
        "model_type": model,
        "valid_from": valid_from,
        "valid_to": valid_from,
        "q50": q50,
        "q05": q50 - 30.0,
        "q10": q50 - 25.0,
        "q25": q50 - 15.0,
        "q75": q50 + 15.0,
        "q90": q50 + 25.0,
        "q95": q50 + 30.0,
        "id": 1,
        "model_type_description": model,
    }


# ---------------------------------------------------------------------------
# FIX #3 -- read_latest_monthly_forecasts
# ---------------------------------------------------------------------------


class TestLatestMonthlyFlagOn:
    """(a) Flag ON: non-operational reissue excluded, operational

    issuance survives, and two configured leads for the same target month
    survive as distinct per-lead rows.
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["month_0", "month_1"])
        _write_mode_config(config_dir, "month_0", lead=0, issue_day=25)
        _write_mode_config(config_dir, "month_1", lead=1, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def _rows(self):
        # Target month 2024-06 (valid_from 2024-06-01).
        return [
            # month_1 operational: issued 2024-05-25, lead 1, issue day 25.
            _month_row(CODE, 2024, 6, "2024-05-25", horizon_value=99, q50=111.0),
            # month_0 operational: issued 2024-06-25, lead 0, issue day 25.
            _month_row(CODE, 2024, 6, "2024-06-25", horizon_value=99, q50=222.0),
            # non-operational reissue: issued 2024-05-10, lead 1 BUT wrong
            # issue day (10) -> must be excluded.
            _month_row(CODE, 2024, 6, "2024-05-10", horizon_value=99, q50=999.0),
        ]

    def test_reissue_excluded_operational_survives_per_lead_distinct(self):
        rows = self._rows()

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            return pd.DataFrame(rows)

        with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api) as mock_api:
            result = data_reader.read_latest_monthly_forecasts(
                [CODE], forecast_date=dt.date(2024, 6, 30)
            )

        # Window expanded backward by the max configured lead (>= 1 year).
        assert mock_api.call_args.args[1] <= 2024 - 1

        # The non-operational reissue (q50=999) is gone.
        assert 999.0 not in set(result["q50"].astype(float))
        # Both operational leads survive as distinct rows.
        assert len(result) == 2
        assert set(result["horizon_value"].astype(int)) == {0, 1}
        assert set(result["q50"].astype(float)) == {111.0, 222.0}
        # All rows are the latest target (2024, 6).
        assert set(result["year"].astype(int)) == {2024}
        assert set(result["month"].astype(int)) == {6}


class TestLatestMonthlyFailLoud:
    """(c) Flag ON: a month_N config missing operational_issue_day makes

    schedule resolution raise -- this MUST propagate (no silent unfiltered
    fallback), and the API must not be read.
    """

    @pytest.fixture(autouse=True)
    def _broken_config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
        _write_mode_config(config_dir, "month_1", lead=1)  # no issue_day
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_raises_and_does_not_read_api(self):
        from long_term_horizon_resolver import LongTermHorizonResolverError

        with patch("src.data_reader._read_long_forecasts_api") as mock_api:
            with pytest.raises(LongTermHorizonResolverError, match="operational_issue_day"):
                data_reader.read_latest_monthly_forecasts(
                    [CODE], forecast_date=dt.date(2024, 6, 30)
                )
        mock_api.assert_not_called()


class TestLatestMonthlyFlagOff:
    """(d) Flag OFF: byte-identical to pre-fix -- no operational selection,

    so the non-operational reissue is RETAINED alongside the operational
    rows for the latest target month.
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        # Config present but must be IGNORED under flag OFF.
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["month_0", "month_1"])
        _write_mode_config(config_dir, "month_0", lead=0, issue_day=25)
        _write_mode_config(config_dir, "month_1", lead=1, issue_day=25)
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

    def test_no_selection_reissue_retained_and_window_not_expanded(self):
        rows = [
            _month_row(CODE, 2024, 6, "2024-05-25", horizon_value=1, q50=111.0),
            _month_row(CODE, 2024, 6, "2024-06-25", horizon_value=0, q50=222.0),
            _month_row(CODE, 2024, 6, "2024-05-10", horizon_value=1, q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            return pd.DataFrame(rows)

        with (
            patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api) as mock_api,
            patch.object(data_reader, "_operational_schedules_for_horizon_type") as mock_resolve,
        ):
            result = data_reader.read_latest_monthly_forecasts(
                [CODE], forecast_date=dt.date(2024, 6, 30)
            )

        # Flag OFF must not resolve schedules nor expand the read window.
        mock_resolve.assert_not_called()
        assert mock_api.call_args.args[1] == 2024  # start_year unchanged
        # All three rows for the latest target month are retained (pre-fix
        # behavior); the reissue (q50=999) is NOT excluded.
        assert len(result) == 3
        assert 999.0 in set(result["q50"].astype(float))


class TestNullHorizonValueUnderSelection:
    """(e) FIX #6 NOT changed: `_normalize_monthly_forecasts` still coerces

    a NULL horizon_value to sentinel 0 under flag ON. This is harmless for
    the selector-fed readers because `select_operational_issuances`
    DERIVES the lead from (valid_from, date) and OVERWRITES horizon_value
    -- so a legacy-NULL input row is emitted with its correct DERIVED lead,
    never a silent 0.

    Coupling note: `_normalize_monthly_forecasts` is also used by the
    combined/gap-detection path (`_normalize_monthly_combined_forecasts`),
    which is NOT routed through the selector and whose downstream consumers
    rely on an int horizon_value (see the explicit comment in
    `_normalize_monthly_combined_forecasts`). Dropping the coercion at the
    shared helper would change that non-selector path under flag ON;
    since it yields no observable change for the selector readers (the
    selector overwrites), FIX #6 was intentionally left in place.
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
        _write_mode_config(config_dir, "month_1", lead=1, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_null_horizon_value_row_gets_derived_lead_not_zero(self):
        row = _month_row(CODE, 2024, 6, "2024-05-25", horizon_value=None, q50=111.0)

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            return pd.DataFrame([row])

        with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api):
            result = data_reader.read_latest_monthly_forecasts(
                [CODE], forecast_date=dt.date(2024, 6, 30)
            )

        assert len(result) == 1
        # Derived lead 1 -- NOT the sentinel 0 the NULL was coerced to.
        assert int(result.iloc[0]["horizon_value"]) == 1


# ---------------------------------------------------------------------------
# FIX #4 -- read_latest_seasonal_forecasts
# ---------------------------------------------------------------------------


class TestLatestSeasonalFlagOn:
    """(b) Flag ON: a non-operational April reissue (wrong issue day) is

    excluded; the configured operational April issuance survives.
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        _write_mode_config(config_dir, "seasonal_april", lead=0, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_reissue_excluded_operational_survives(self):
        rows = [
            # Operational April issuance: lead 0, issue day 25.
            _season_row(CODE, "2024-04-01", "2024-04-25", q50=111.0),
            # Non-operational April reissue: lead 0 but wrong issue day 10.
            _season_row(CODE, "2024-04-01", "2024-04-10", q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="season", horizon_value=None):
            return pd.DataFrame(rows)

        with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api):
            result = data_reader.read_latest_seasonal_forecasts(
                [CODE], forecast_date=dt.date(2024, 4, 30)
            )

        assert len(result) == 1
        assert float(result.iloc[0]["q50"]) == 111.0
        assert 999.0 not in set(result["q50"].astype(float))
        # Derived lead written into both lead columns.
        assert int(result.iloc[0]["season_in_year"]) == 0
        assert int(result.iloc[0]["horizon_value"]) == 0


class TestLatestSeasonalFailLoud:
    """(c) Flag ON: a seasonal config missing operational_issue_day makes

    schedule resolution raise -- must propagate, API not read.
    """

    @pytest.fixture(autouse=True)
    def _broken_config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        _write_mode_config(config_dir, "seasonal_april", lead=0)  # no issue_day
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_raises_and_does_not_read_api(self):
        from long_term_horizon_resolver import LongTermHorizonResolverError

        with patch("src.data_reader._read_long_forecasts_api") as mock_api:
            with pytest.raises(LongTermHorizonResolverError, match="operational_issue_day"):
                data_reader.read_latest_seasonal_forecasts(
                    [CODE], forecast_date=dt.date(2024, 4, 30)
                )
        mock_api.assert_not_called()


class TestLatestSeasonalFlagOff:
    """(d) Flag OFF: byte-identical to pre-fix -- no operational selection,

    so the non-operational April reissue is RETAINED.
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        _write_mode_config(config_dir, "seasonal_april", lead=0, issue_day=25)
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

    def test_no_selection_reissue_retained(self):
        rows = [
            _season_row(CODE, "2024-04-01", "2024-04-25", q50=111.0),
            _season_row(CODE, "2024-04-01", "2024-04-10", q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="season", horizon_value=None):
            return pd.DataFrame(rows)

        with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api) as mock_api:
            result = data_reader.read_latest_seasonal_forecasts(
                [CODE], forecast_date=dt.date(2024, 4, 30)
            )

        # start_year for the 200-day lookback from 2024-04-30 is 2023;
        # flag OFF must NOT expand it further.
        assert mock_api.call_args.args[1] == 2023
        assert len(result) == 2
        assert 999.0 in set(result["q50"].astype(float))
